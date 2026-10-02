#!/usr/bin/env python3
"""
Bulk-repair daily history rows that were written with a broken `timestamp`.

THE BUG
-------
The old sync stamped every row's timestamp at 00:00:00 UTC of the New York date:

    date = 2026-10-01   timestamp = 2026-10-01T00:00:00Z

00:00 UTC is 20:00 ET the PREVIOUS day, so that timestamp resolves to the
PREVIOUS New York trading day - a full day behind its own `date`.

THE REPAIR
----------
Rewrite each row's timestamp to <date>T<anchor>Z, where the anchor defaults to
21:30 UTC (17:30 New York) - the time the corrected pipeline captures the close.
The invariant restored is:

    ny_day(timestamp) == date

Nothing else is touched. Prices are left alone unless you pass --set-prices,
because for May-Oct 2026 (fully inside daylight saving) both cron runs landed
after the 17:00 ET close, so the captured values are sound.

USAGE
-----
Dry run (default, writes nothing):

    python scripts/repair_history_timestamps.py
    python scripts/repair_history_timestamps.py --csv firestore_export.csv

Apply to history.json:

    python scripts/repair_history_timestamps.py --apply

Repair Firestore as well (needs FIREBASE_SERVICE_ACCOUNT set):

    python scripts/repair_history_timestamps.py --apply --firestore

Limit the range:

    python scripts/repair_history_timestamps.py --apply --from 2026-05-01 --to 2026-10-01
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Column aliases. `Close` appears in plain price exports, so it maps to gold.
GOLD_NAMES = ("gold_oz", "gold", "goldperounceusd", "xau", "close",
              "adj close", "price", "value", "last")
SILVER_NAMES = ("silver_oz", "silver", "silverperounceusd", "xag",
                "silver close", "silverprice")

_DATE_FORMATS = (
    ("%Y-%m-%d", "mdy"), ("%Y-%m-%d %H:%M", "mdy"),
    ("%Y-%m-%dT%H:%M:%S", "mdy"), ("%Y-%m-%d %H:%M:%S", "mdy"),
    ("%m/%d/%Y %H:%M", "mdy"), ("%m/%d/%Y %H:%M:%S", "mdy"),
    ("%m/%d/%Y", "mdy"), ("%m/%d/%y %H:%M", "mdy"),
    ("%d/%m/%Y %H:%M", "dmy"), ("%d/%m/%Y %H:%M:%S", "dmy"),
    ("%d/%m/%Y", "dmy"), ("%d/%m/%y %H:%M", "dmy"),
)


def detect_date_order(values, forced=None) -> str:
    """
    Decide month/day vs day/month from unambiguous values.

    A component greater than 12 can only be a DAY, never a month:
        9/30/2026  -> 30 > 12, so it must be month/day  (September 30)
        25/12/2026 -> 25 > 12, so it must be day/month  (25 December)
    """
    if forced in ("mdy", "dmy"):
        return forced
    mdy = dmy = 0
    for value in values:
        match = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{2,4})", str(value or ""))
        if not match:
            continue
        first, second = int(match.group(1)), int(match.group(2))
        if second > 12 >= first:
            mdy += 1
        elif first > 12 >= second:
            dmy += 1
    return "dmy" if dmy > mdy else "mdy"


def parse_date(value, order="mdy"):
    """Parse a date cell into YYYY-MM-DD, tolerating a trailing time part."""
    text = re.sub(r"\s+\d{1,2}:\d{2}(:\d{2})?(\.\d+)?\s*$", "", str(value or "").strip())
    if not text:
        return None
    text = re.sub(r"\.\d+$", "", text)
    for fmt, fmt_order in _DATE_FORMATS:
        if fmt_order != order:
            continue
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    for fmt, _ in _DATE_FORMATS:            # fall back to any unambiguous match
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except ValueError:
        return None


def read_csv_records(path: str):
    """
    Read a CSV into dicts, skipping any junk rows above the real header.

    Excel exports often carry a title line ("XAUUSD Historical Data,") before
    the header, which defeats a naive DictReader.
    """
    path = path.strip().strip('"').strip("'")     # tolerate a quoted path
    with open(path, newline="", encoding="utf-8-sig") as fh:
        raw_rows = [row for row in csv.reader(fh)
                    if any((cell or "").strip() for cell in row)]

    header_index = None
    for index, row in enumerate(raw_rows):
        if any(_normalise_key(cell) in ("date", "datetime", "day", "time")
               for cell in row):
            header_index = index
            break
    if header_index is None:
        raise SystemExit(
            f"No `date` column found in {path}. Header row was: {raw_rows[0] if raw_rows else '(empty)'}"
        )

    header = [(cell or "").strip() for cell in raw_rows[header_index]]
    records = []
    for row in raw_rows[header_index + 1:]:
        record = {header[i]: row[i] for i in range(min(len(header), len(row)))}
        if any((cell or "").strip() for cell in row):
            records.append(record)
    return records

NY = None  # populated in main(); avoids a hard dependency at import time


def ny_day(ts_ms: float) -> str:
    """New York trading day of an epoch-millisecond timestamp."""
    from zoneinfo import ZoneInfo

    return (
        datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        .astimezone(ZoneInfo("America/New_York"))
        .strftime("%Y-%m-%d")
    )


def anchor_ms(date_str: str, anchor_utc: str) -> int:
    """Epoch ms for <date_str>T<anchor_utc>Z."""
    hh, mm = anchor_utc.split(":")
    dt = datetime.strptime(f"{date_str}T{hh}:{mm}:00", "%Y-%m-%dT%H:%M:%S")
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


# --- tolerant field lookup ---------------------------------------------------
# Firestore CSV exports vary: flat columns, ".doubleValue" suffixes, or the old
# "fieldexport" layout. Normalise aggressively instead of demanding one shape.
def _normalise_key(key: str) -> str:
    """
    Strip Firestore export decorations so column names compare cleanly.

    "fieldexport.date.stringValue" -> "date"
    "fieldexport.gold_oz.doubleValue" -> "goldoz"
    "Date" -> "date", "gold_oz" -> "goldoz"
    """
    k = str(key).strip().lower()

    # Type decorations hang off the END: .stringValue, _doubleValue, ...
    for suffix in (".doublevalue", ".stringvalue", ".timestampvalue",
                   ".integervalue", ".booleanvalue", ".referencevalue",
                   "_doublevalue", "_stringvalue", "_timestampvalue",
                   "_integervalue", "_booleanvalue"):
        if k.endswith(suffix):
            k = k[: -len(suffix)]
            break

    # Container names hang off the START: fieldexport., fields., ...
    for prefix in ("fieldexport.", "fieldexport_", "fields.", "fields_"):
        if k.startswith(prefix):
            k = k[len(prefix):]
            break

    return k.replace("_", "").replace(".", "")


def pick(row: dict, *names: str):
    """Find a value by tolerant column name, then fall back to suffix matching."""
    wanted = {_normalise_key(n) for n in names}
    for raw_key, value in row.items():
        if value in (None, "") or _normalise_key(raw_key) not in wanted:
            continue
        return value
    for raw_key, value in row.items():          # e.g. "export.date.stringValue"
        if value in (None, ""):
            continue
        normalised = _normalise_key(raw_key)
        if any(normalised.endswith(w) for w in wanted):
            return value
    return None


def to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_ts(value):
    """
    Coerce a timestamp to epoch milliseconds.

    Firestore CSV exports write ISO-8601 ("2026-09-30T00:00:00Z"), sometimes
    nanoseconds; history.json uses epoch milliseconds. Accept all of them so a
    CSV feed does not report every row as "unreadable".
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)

    if isinstance(value, (int, float)):
        num = float(value)
        if num > 1e14:                       # nanoseconds -> milliseconds
            num /= 1e6
        return int(num)

    text = str(value).strip()
    try:
        num = float(text)
    except ValueError:
        num = None
    if num is not None:
        if num > 1e14:                       # nanoseconds -> milliseconds
            num /= 1e6
        return int(num)

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def extract_csv_rows(path: str, want_gold: bool, want_silver: bool,
                     gold_column=None, silver_column=None, forced_order=None):
    records = read_csv_records(path)
    order = detect_date_order([pick(r, "date", "datetime", "day") for r in records],
                              forced_order)
    out = []
    for record in records:
        date = parse_date(pick(record, "date", "datetime", "day"), order)
        if not date:
            continue
        row = {"date": date, "ts": parse_ts(pick(record, "ts", "timestamp"))}
        if want_gold:
            value = pick(record, gold_column) if gold_column else pick(record, *GOLD_NAMES)
            row["gold_oz"] = to_float(value)
        if want_silver:
            value = (pick(record, silver_column) if silver_column
                     else pick(record, *SILVER_NAMES))
            row["silver_oz"] = to_float(value)
        out.append(row)
    return out


def load_rows(source: str, csv_path, csv_gold=None, csv_silver=None,
              gold_column=None, silver_column=None, forced_order=None):
    """
    Build the repair feed from CSV(s) and/or history.json.

    Three shapes are supported:
      --csv FILE        one file holding both metals
      --csv-gold FILE   gold prices (column `Close`, `gold_oz`, ...)
      --csv-silver FILE silver prices, merged onto the same dates
    """
    if csv_path or csv_gold or csv_silver:
        merged = {}

        def absorb(rows):
            for row in rows:
                slot = merged.setdefault(row["date"], {"date": row["date"], "ts": None})
                for key in ("gold_oz", "silver_oz"):
                    if row.get(key) is not None:
                        slot[key] = row[key]
                if row.get("ts") is not None:
                    slot["ts"] = row["ts"]

        if csv_path:
            absorb(extract_csv_rows(csv_path, True, True, gold_column,
                                    silver_column, forced_order))
        if csv_gold:
            absorb(extract_csv_rows(csv_gold, True, False, gold_column,
                                    None, forced_order))
        if csv_silver:
            absorb(extract_csv_rows(csv_silver, False, True, None,
                                     silver_column, forced_order))

        if not merged:
            raise SystemExit("No dated rows could be read from the CSV input(s).")
        return list(merged.values())

    path = Path(source)
    if not path.exists():
        raise SystemExit(f"history.json not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("history.json must contain a JSON array")
    return data


def re_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except (TypeError, ValueError):
        return False


def in_range(date_str: str, start: str | None, end: str | None) -> bool:
    if start and date_str < start:
        return False
    if end and date_str > end:
        return False
    return True


def is_weekend(date_str: str) -> bool:
    return datetime.strptime(date_str, "%Y-%m-%d").weekday() >= 5


def plan(rows, anchor_utc, start, end, set_prices, include_weekends=False):
    """Return (plan_entries, stats). plan_entries are dicts ready to write."""
    entries = []
    stats = {"total": 0, "in_range": 0, "ts_fixed": 0, "ts_ok": 0,
             "ts_unparseable": 0, "prices_changed": 0, "weekend_skipped": 0}

    for row in rows:
        date = str(row.get("date") or "").strip()
        if not re_date(date):
            continue
        stats["total"] += 1
        if not in_range(date, start, end):
            continue
        stats["in_range"] += 1
        # history.json stores trading days only. A vendor feed that also carries
        # Sunday candles must not inject them, or the file loses its convention.
        if not include_weekends and is_weekend(date):
            stats["weekend_skipped"] += 1
            continue

        entry = {"date": date}

        # --- timestamp ---
        new_ts = anchor_ms(date, anchor_utc)
        old_ts = parse_ts(row.get("ts"))
        if old_ts is None:
            old_ts = parse_ts(row.get("timestamp"))
        if old_ts is None:
            stats["ts_unparseable"] += 1
        elif ny_day(old_ts) == date:
            stats["ts_ok"] += 1
        else:
            stats["ts_fixed"] += 1
        entry["ts"] = new_ts
        entry["_old_ts"] = int(old_ts) if old_ts is not None else None

        # --- prices (only touched on request) ---
        gold = to_float(row.get("gold_oz"))
        silver = to_float(row.get("silver_oz"))
        if set_prices:
            if gold is not None:
                entry["gold_oz"] = round(gold, 4)
                if to_float(row.get("gold_oz")) != entry["gold_oz"]:
                    stats["prices_changed"] += 1
            if silver is not None:
                entry["silver_oz"] = round(silver, 4)
        entry["_src_gold"] = gold
        entry["_src_silver"] = silver

        entries.append(entry)

    entries.sort(key=lambda e: e["date"])
    return entries, stats


def apply_to_history_json(out_path: str, entries, set_prices: bool) -> int:
    """Rewrite history.json (rows outside the range are preserved untouched)."""
    path = Path(out_path)
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    by_date = {str(r.get("date", "")).strip(): r for r in data
               if isinstance(r, dict) and r.get("date")}

    changed = 0
    for entry in entries:
        row = by_date.setdefault(entry["date"], {"date": entry["date"]})
        # Compare only the canonical field. `ts` is a legacy alias that gets
        # popped below, so including it here would report a change every run.
        if row.get("timestamp") != entry["ts"]:
            changed += 1
        row["timestamp"] = entry["ts"]
        row.pop("ts", None)          # keep one canonical field name
        if set_prices and entry["_src_gold"] is not None:
            row["gold_oz"] = entry["gold_oz"]
        if set_prices and entry["_src_silver"] is not None:
            row["silver_oz"] = entry["silver_oz"]
        row.setdefault("source", "daily-sync")

    merged = sorted(by_date.values(), key=lambda r: r.get("date", ""))
    path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    return changed
def apply_to_firestore(entries, set_prices: bool, batch_size: int = 400) -> int:
    """Bulk-update the `ts` field (and prices only if explicitly requested)."""
    service_account = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "").strip()
    if not service_account:
        raise SystemExit("FIREBASE_SERVICE_ACCOUNT is not set - cannot reach Firestore")

    from google.cloud import firestore
    from google.oauth2 import service_account as sa

    raw = service_account.strip()
    if raw.startswith("{"):
        info = json.loads(raw)
        creds = sa.Credentials.from_service_account_info(info)
        db = firestore.Client(project=info.get("project_id"), credentials=creds)
    else:
        creds = sa.Credentials.from_service_account_file(raw)
        db = firestore.Client(credentials=creds)

    collection_name = os.environ.get("FIRESTORE_COLLECTION", "metals_daily_usd")
    coll = db.collection(collection_name)
    updated = 0

    for start in range(0, len(entries), batch_size):
        chunk = entries[start:start + batch_size]
        batch = db.batch()
        for entry in chunk:
            payload = {
                "ts": datetime.fromtimestamp(entry["ts"] / 1000, tz=timezone.utc),
            }
            if set_prices:
                if entry["_src_gold"] is not None:
                    payload["gold_oz"] = entry["gold_oz"]
                if entry["_src_silver"] is not None:
                    payload["silver_oz"] = entry["silver_oz"]
            batch.set(coll.document(entry["date"]), payload, merge=True)
            updated += 1
        batch.commit()
        done = min(start + batch_size, len(entries))
        print(f"  committed batch {start // batch_size + 1} ({done}/{len(entries)} docs)")

    return updated


def report(entries, stats, args, current=None):
    current = current or {}
    print("=" * 74)
    print("DRY RUN - nothing written" if not args.apply else "APPLYING")
    print("=" * 74)
    print(f"rows with a valid date      : {stats['total']}")
    print(f"rows in the requested range : {stats['in_range']}")
    print(f"  timestamp already correct : {stats['ts_ok']}")
    print(f"  timestamp one day behind  : {stats['ts_fixed']}   <- will be repaired")
    print(f"  timestamp unreadable      : {stats['ts_unparseable']}")
    if stats.get("weekend_skipped"):
        print(f"  weekend rows skipped     : {stats['weekend_skipped']} "
              f"(use --include-weekends to keep them)")
    print(f"  prices                    : "
          f"{'overwritten from source' if args.set_prices else 'untouched'}")
    if args.set_prices:
        print(f"  price values to be set    : {stats['prices_changed']}")
    print(f"anchor                      : <date>T{args.anchor}Z")

    if entries:
        added = [e["date"] for e in entries if e["date"] not in current]
        if added:
            print(f"  rows to be ADDED (in source, missing here): {len(added)}"
                  f"  e.g. {', '.join(added[:5])}")

    if not entries:
        return

    def sample(seq, head=4, tail=1):
        if len(seq) <= head + tail + 1:
            return list(seq)
        return list(seq[:head]) + [None] + list(seq[-tail:])

    print("\nsample of what changes:")
    for entry in sample(entries):
        if entry is None:
            print(f"  {'...':<12}")
            continue
        old = entry["_old_ts"]
        old_s = (datetime.fromtimestamp(old / 1000, tz=timezone.utc)
                 .strftime("%Y-%m-%d %H:%MZ") if old else "n/a")
        new_s = datetime.fromtimestamp(entry["ts"] / 1000, tz=timezone.utc)\
            .strftime("%Y-%m-%d %H:%MZ")
        print(f"  {entry['date']:<12} ts {old_s} -> {new_s}")

    if args.set_prices:
        print("\nPRICE CHANGES (current history.json value -> new value from source):")
        shown = 0
        for entry in entries:
            prev = current.get(entry["date"])
            if not prev:
                continue
            for field, label in (("gold_oz", "gold "), ("silver_oz", "silver")):
                new_v = entry.get(field)
                old_v = to_float(prev.get(field))
                if new_v is None:
                    continue
                if old_v is None or abs(old_v - new_v) < 1e-9:
                    continue
                if shown < 12:
                    delta = new_v - old_v
                    print(f"  {entry['date']}  {label} "
                          f"{old_v:>10.4f} -> {new_v:>10.4f}  "
                          f"({delta:+.4f}, {delta / old_v * 100:+.2f}%)")
                    shown += 1
        if shown == 0:
            print("  (no price differences in the sampled range)")
        elif shown >= 12:
            print(f"  ... and more. Re-run without --set-prices to skip this table.")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Bulk-repair daily history rows with a broken timestamp.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--source", default="history.json", help="history.json path")
    ap.add_argument("--csv", help="CSV holding both metals")
    ap.add_argument("--csv-gold", help="CSV with gold prices only (column `Close` etc.)")
    ap.add_argument("--csv-silver", help="CSV with silver prices only")
    ap.add_argument("--gold-column", help="override the gold column name in the CSV")
    ap.add_argument("--silver-column", help="override the silver column name in the CSV")
    ap.add_argument("--date-order", choices=("mdy", "dmy"),
                    help="override ambiguous slash dates (default: auto-detect)")
    ap.add_argument("--apply", action="store_true", help="actually write (default: dry run)")
    ap.add_argument("--firestore", action="store_true", help="also bulk-update Firestore")
    ap.add_argument("--from", dest="start", help="first date, YYYY-MM-DD")
    ap.add_argument("--to", dest="end", help="last date, YYYY-MM-DD")
    ap.add_argument("--anchor", default="21:30",
                    help="UTC time-of-day to stamp, HH:MM (default 21:30 = 17:30 NY)")
    ap.add_argument("--set-prices", action="store_true",
                    help="also overwrite gold_oz/silver_oz from the source")
    ap.add_argument("--include-weekends", action="store_true",
                    help="keep Sat/Sun rows from the source (off by default)")
    args = ap.parse_args()

    rows = load_rows(args.source, args.csv, args.csv_gold, args.csv_silver,
                     args.gold_column, args.silver_column, args.date_order)
    if not rows:
        print("No dated rows found.")
        return 1

    missing_gold = sum(1 for r in rows if r.get("gold_oz") is None)
    missing_silver = sum(1 for r in rows if r.get("silver_oz") is None)
    if missing_gold or missing_silver:
        print(f"WARNING: source has no value for gold on {missing_gold} row(s) "
              f"and silver on {missing_silver} row(s). Those fields will be left "
              f"as they are. Pass --csv-silver to supply silver prices.\n")

    # Current on-disk values, so the dry run can show a real before/after diff.
    current = {}
    hist_path = Path(args.source)
    if hist_path.exists():
        for row in json.loads(hist_path.read_text(encoding="utf-8")):
            if isinstance(row, dict) and row.get("date"):
                current[str(row["date"]).strip()] = row

    entries, stats = plan(rows, args.anchor, args.start, args.end, args.set_prices,
                         args.include_weekends)
    report(entries, stats, args, current)

    if not args.apply:
        print("\nNothing written. Re-run with --apply to make these changes.")
        return 0

    changed = apply_to_history_json(args.source, entries, args.set_prices)
    print(f"\nhistory.json: {changed} rows rewritten -> {args.source}")

    if args.firestore:
        n = apply_to_firestore(entries, args.set_prices)
        print(f"Firestore: {n} documents updated (ts only, prices "
              f"{'included' if args.set_prices else 'untouched'})")
    else:
        print("Firestore: not touched (pass --firestore to include it)")

    return 0


if __name__ == "__main__":
    sys.exit(main())