#!/usr/bin/env python3
"""
Record the New York trading-day gold/silver close into history.json + Firestore.

COPY THIS FILE to `.github/scripts/update_history_json.py` in the
`peshwan/gold-history` repository. That is the repo the Android app reads
history.json from (see staticHistoryUrl in services/priceService.ts), and it is
where this script actually runs.

Why the close-hour guard exists
-------------------------------
The workflow fires at TWO UTC times so that one of them lands inside the New York
close hour in either DST state:

    EDT (UTC-4):  21:30 UTC = 17:30 NY   (the 22:30 UTC run is 18:30 NY)
    EST (UTC-5):  22:30 UTC = 17:30 NY   (the 21:30 UTC run is 16:30 NY)

Without a guard BOTH runs write. In winter the 21:30 UTC run captures a
MID-SESSION price (16:30 NY, market still open) and stamps it as the day's
close, then overwrites it an hour later - so the close you see depends on when
you happen to look. Two mechanisms prevent that:

    1. hour >= 17 guard       -> rejects the winter 21:30 UTC run
    2. already-recorded check -> rejects the second run of the day

Together they guarantee exactly ONE close per NY session, always at or after
17:00 ET. Neither mechanism is sufficient alone.

`ts` is the REAL observation time. Stamping it at midnight UTC of the NY date
claims the price was seen ~21.5 hours before it actually was, and resolves back
to the PREVIOUS New York trading day.

Env vars:
- METALS_GOLD_URL / METALS_SILVER_URL  (defaults: gold-api.com XAU / XAG)
- METALS_API_KEY / API_AUTH_HEADER     (optional)
- HISTORY_JSON_PATH                   (default: history.json)
- HISTORY_RETENTION_YEARS             (default: 17)
- FIRESTORE_COLLECTION                (default: metals_daily_usd)
- FIREBASE_SERVICE_ACCOUNT            (path to json, or the json itself)
- SYNC_FORCE_TODAY                    (1/true: rewrite today even if recorded)
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from zoneinfo import ZoneInfo

import requests
from google.cloud import firestore
from google.oauth2 import service_account as sa

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

# The NY spot session closes at 17:00 ET. Anything earlier is mid-session.
NY_CLOSE_HOUR = 17


def _extract_number(data: Dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = data.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def fetch_spot_price(url: str, headers: Dict[str, str], timeout_s: int = 20) -> float:
    response = requests.get(url, headers=headers, timeout=timeout_s)
    response.raise_for_status()
    data = response.json()
    price = _extract_number(data, ["price", "xau", "xag", "gold", "silver", "value"])
    if price is None:
        raise ValueError(f"Could not parse price from {url}. Response keys: {list(data.keys())}")
    return price


def force_today() -> bool:
    """Escape hatch for workflow_dispatch: rewrite today even if already stored."""
    return os.environ.get("SYNC_FORCE_TODAY", "").strip().lower() in ("1", "true", "yes")


def get_market_snapshot_info() -> tuple[bool, str, str]:
    """
    Decide whether "now" is a valid moment to record today's New York close.

    Returns (should_sync, quote_date, reason_when_not_syncing). quote_date is the
    New York trading day and stays the Firestore document id.
    """
    ny_now = datetime.now(NY)
    quote_date = ny_now.strftime("%Y-%m-%d")

    if ny_now.weekday() > 4:
        return False, quote_date, f"weekend, no NY session on {quote_date}"
    if ny_now.hour < NY_CLOSE_HOUR:
        return False, quote_date, (
            f"now {ny_now:%H:%M} NY is before the {NY_CLOSE_HOUR}:00 ET close"
        )

    return True, quote_date, ""


def subtract_years(dt: datetime, years: int) -> datetime:
    try:
        return dt.replace(year=dt.year - years)
    except ValueError:
        return dt.replace(year=dt.year - years, day=28)


def get_cutoff_date(quote_date: str, retention_years: int) -> str:
    quote_dt = datetime.strptime(quote_date, "%Y-%m-%d").replace(tzinfo=UTC)
    return subtract_years(quote_dt, retention_years).strftime("%Y-%m-%d")


def get_firestore_client(service_account_str: str) -> firestore.Client:
    raw = service_account_str.strip()
    if raw.startswith("{"):
        cert_dict = json.loads(raw)
        creds = sa.Credentials.from_service_account_info(cert_dict)
        project_id = cert_dict.get("project_id")
        return firestore.Client(project=project_id, credentials=creds)
    else:
        creds = sa.Credentials.from_service_account_file(raw)
        return firestore.Client(credentials=creds)
def upsert_firestore(
    quote_date: str,
    observed_at: datetime,
    gold_price: float,
    silver_price: float,
) -> str | None:
    """
    Upsert the close into Firestore.

    Returns None when the write happened, or a human-readable reason when it was
    intentionally skipped (missing credentials / already recorded today).
    """
    service_account = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "").strip()
    if not service_account:
        return "No FIREBASE_SERVICE_ACCOUNT set; skipping Firestore"

    db = get_firestore_client(service_account)
    collection = os.environ.get("FIRESTORE_COLLECTION", "metals_daily_usd")
    doc_ref = db.collection(collection).document(quote_date)

    # Second run of the day must not rewrite the close captured by the first.
    existing = doc_ref.get()
    if existing.exists and not force_today():
        data = existing.to_dict() or {}
        return (
            f"{quote_date} already recorded in '{collection}' "
            f"(gold_oz={data.get('gold_oz')}) - keeping that close. "
            f"Set SYNC_FORCE_TODAY=1 to overwrite."
        )

    payload = {
        "date": quote_date,
        # Real observation time, not midnight UTC of the NY date (which resolves
        # back to the PREVIOUS New York trading day).
        "ts": observed_at,
        "gold_oz": round(gold_price, 4),
        "silver_oz": round(silver_price, 4),
        "source": "daily-sync",
        "updated_at": firestore.SERVER_TIMESTAMP,
    }

    doc_ref.set(payload, merge=True)
    print(f"Upserted {quote_date} into Firestore collection '{collection}'")
    return None


def cleanup_firestore_old_history(quote_date: str, retention_years: int) -> None:
    if retention_years <= 0:
        return

    service_account = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "").strip()
    if not service_account:
        return

    db = get_firestore_client(service_account)
    collection = os.environ.get("FIRESTORE_COLLECTION", "metals_daily_usd")
    cutoff_date = get_cutoff_date(quote_date, retention_years)
    deleted = 0

    # Hard iteration cap so a failing delete cannot spin forever.
    max_batches = 200
    for _ in range(max_batches):
        old_docs = list(
            db.collection(collection)
            .where("date", "<", cutoff_date)
            .limit(450)
            .stream()
        )
        if not old_docs:
            break

        batch = db.batch()
        for doc in old_docs:
            batch.delete(doc.reference)
            deleted += 1
        batch.commit()
    else:
        print(
            f"WARNING: stopped after {max_batches} delete batches; "
            f"re-run the workflow to continue pruning"
        )

    if deleted:
        print(f"Deleted {deleted} old Firestore docs before {cutoff_date}")
def main() -> None:
    gold_url = os.environ.get("METALS_GOLD_URL", "https://api.gold-api.com/price/XAU")
    silver_url = os.environ.get("METALS_SILVER_URL", "https://api.gold-api.com/price/XAG")
    api_key = os.environ.get("METALS_API_KEY", "").strip()
    auth_header = os.environ.get("API_AUTH_HEADER", "X-API-Key")
    history_path = Path(os.environ.get("HISTORY_JSON_PATH", "history.json"))
    retention_years = int(os.environ.get("HISTORY_RETENTION_YEARS", "17"))

    should_sync, quote_date, reason = get_market_snapshot_info()
    if not should_sync:
        print(f"Skip close capture for {quote_date}: {reason}")
        return

    # REAL observation time - see the module docstring.
    observed_at = datetime.now(UTC)
    ts = int(observed_at.timestamp() * 1000)

    headers = {"Accept": "application/json"}
    if api_key:
        headers[auth_header] = api_key

    gold_price = fetch_spot_price(gold_url, headers)
    silver_price = fetch_spot_price(silver_url, headers)
    print(
        f"NY close for {quote_date}: gold={gold_price} silver={silver_price} "
        f"(observed {observed_at:%Y-%m-%d %H:%M} UTC)"
    )

    if history_path.exists():
        data = json.loads(history_path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("history.json must contain a JSON array")
    else:
        data = []

    by_date: Dict[str, Dict[str, Any]] = {}
    for row in data:
        if not isinstance(row, dict):
            continue
        d = str(row.get("date", "")).strip()
        if d:
            by_date[d] = row

    # One close per NY session. The second cron of the day must not rewrite the
    # value captured by the first.
    existing = by_date.get(quote_date)
    if existing is not None and not force_today():
        print(
            f"{quote_date} already recorded in {history_path} "
            f"(gold_oz={existing.get('gold_oz')}, silver_oz={existing.get('silver_oz')}) "
            f"- keeping that close. Set SYNC_FORCE_TODAY=1 to overwrite."
        )
    else:
        if existing is not None:
            print(
                f"WARNING SYNC_FORCE_TODAY: rewriting {quote_date} "
                f"{existing.get('gold_oz')} -> {round(gold_price, 4)}"
            )

        by_date[quote_date] = {
            "date": quote_date,
            "timestamp": ts,
            "gold_oz": round(gold_price, 4),
            "silver_oz": round(silver_price, 4),
            "source": "daily-sync",
        }

        cutoff_date = get_cutoff_date(quote_date, retention_years)
        merged = sorted(
            [row for row in by_date.values() if str(row.get("date", "")) >= cutoff_date],
            key=lambda r: r.get("date", ""),
        )

        history_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Updated {history_path} with {quote_date}")
        print(f"Kept {len(merged)} history rows from {cutoff_date} onward")

    # Firestore is best-effort. history.json is already written by this point, so
    # a Firestore outage must not fail the run and skip the git commit.
    try:
        skip_reason = upsert_firestore(quote_date, observed_at, gold_price, silver_price)
        if skip_reason:
            print(skip_reason)
        cleanup_firestore_old_history(quote_date, retention_years)
    except Exception as e:
        print(f"Firestore sync warning: {e}")


if __name__ == "__main__":
    main()