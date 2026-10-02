"""
End-to-end test of the deployable gold-history sync script.

Runs the REAL main() with the network and Firestore mocked out, to prove:
  1. the first run of the day writes exactly one row
  2. the second run of the day does NOT overwrite that close
  3. SYNC_FORCE_TODAY=1 does overwrite it
  4. `timestamp` is the real observation time, not midnight UTC of the NY date
  5. a weekend / pre-close run writes nothing at all
"""

import importlib.util
import json
import os
import sys
import tempfile
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

SCRIPT = "scripts/gold_history/update_history_json.py"
NY = ZoneInfo("America/New_York")
UTC = timezone.utc

failures = 0


def check(label, actual, expected):
    global failures
    ok = actual == expected
    if not ok:
        failures += 1
    print(f"{'PASS' if ok else 'FAIL'}  {label}\n        got={actual!r}\n        want={expected!r}")


# ---- stub the network + Firestore so main() runs offline -------------------
class FakeResponse:
    def __init__(self, price):
        self._price = price

    def raise_for_status(self):
        pass

    def json(self):
        return {"price": self._price}


PRICE = {"gold": 4099.5, "silver": 47.25}
FETCHES = []


def fake_get(url, headers=None, timeout=None):
    FETCHES.append(url)
    return FakeResponse(PRICE["gold"] if "XAU" in url else PRICE["silver"])


try:  # pragma: no cover
    import requests
    requests.get = fake_get
except ImportError:  # pragma: no cover
    m = types.ModuleType("requests")
    m.__path__ = []
    m.get = fake_get
    sys.modules["requests"] = m

if "google.cloud.firestore" not in sys.modules:
    fs_mod = types.ModuleType("google.cloud.firestore")
    fs_mod.SERVER_TIMESTAMP = "SERVER_TIMESTAMP"
    sys.modules["google.cloud.firestore"] = fs_mod
    import google.cloud
    google.cloud.firestore = fs_mod

if "google.oauth2.service_account" not in sys.modules:
    sa_mod = types.ModuleType("google.oauth2.service_account")
    sa_mod.Credentials = object
    sys.modules["google.oauth2.service_account"] = sa_mod
    import google.oauth2
    google.oauth2.service_account = sa_mod


def load():
    spec = importlib.util.spec_from_file_location("gh_sync", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    class FakeDatetime(datetime):
        _now = None

        @classmethod
        def now(cls, tz=None):
            return cls._now.astimezone(tz) if tz is not None else cls._now

    mod.datetime = FakeDatetime
    return mod, FakeDatetime


def run(mod, dt_class, utc_iso, tmp, extra_env=None):
    """Invoke the real main() with a pinned clock and an isolated history.json."""
    dt_class._now = datetime.fromisoformat(utc_iso).replace(tzinfo=UTC)
    hist = Path(tmp) / "history.json"
    env = {
        "HISTORY_JSON_PATH": str(hist),
        "HISTORY_RETENTION_YEARS": "17",
    }
    # No FIREBASE_SERVICE_ACCOUNT -> Firestore is skipped cleanly.
    env.update(extra_env or {})
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        FETCHES.clear()
        mod.main()
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return json.loads(hist.read_text()) if hist.exists() else []


with tempfile.TemporaryDirectory() as tmp:
    mod, DT = load()

    print("--- run 1: first run of the session writes the close ---")
    # Thursday 1 Oct 2026, 17:30 NY = 21:30 UTC (EDT)
    rows = run(mod, DT, "2026-10-01T21:30:00", tmp)
    check("one row written", len(rows), 1)
    check("row date is the NY trading day", rows[0]["date"], "2026-10-01")
    check("gold price captured", rows[0]["gold_oz"], PRICE["gold"])
    check("silver price captured", rows[0]["silver_oz"], PRICE["silver"])
    check("both endpoints were polled", len(FETCHES), 2)
    first_ts = rows[0]["timestamp"]
    check(
        "timestamp is the real observation time",
        datetime.fromtimestamp(first_ts / 1000, tz=UTC),
        datetime(2026, 10, 1, 21, 30, tzinfo=UTC),
    )
    check(
        "timestamp resolves to the correct NY day",
        datetime.fromtimestamp(first_ts / 1000, tz=UTC).astimezone(NY).strftime("%Y-%m-%d"),
        "2026-10-01",
    )

    print("\n--- run 2: second cron of the day must NOT overwrite ---")
    PRICE["gold"] = 9999.0  # pretend the market moved after the close
    rows = run(mod, DT, "2026-10-01T22:30:00", tmp)
    check("still one row", len(rows), 1)
    check("close preserved, not overwritten", rows[0]["gold_oz"], 4099.5)  # not 9999.0
    check("timestamp preserved too", rows[0]["timestamp"], first_ts)

    print("\n--- run 3: SYNC_FORCE_TODAY=1 overrides ---")
    rows = run(mod, DT, "2026-10-01T22:30:00", tmp, {"SYNC_FORCE_TODAY": "1"})
    check("force rewrote the close", rows[0]["gold_oz"], 9999.0)
    check("still one row", len(rows), 1)
    PRICE["gold"] = 4099.5

    print("\n--- run 4: next weekday adds a new row, both kept ---")
    rows = run(mod, DT, "2026-10-02T21:30:00", tmp)
    check("two rows now", len(rows), 2)
    check("sorted by date", [r["date"] for r in rows], ["2026-10-01", "2026-10-02"])

    print("\n--- run 5: pre-close and weekend runs write nothing ---")
    rows = run(mod, DT, "2026-10-05T16:30:00", tmp)  # Mon 13:30 NY
    check("pre-close: no extra row", len(rows), 2)
    rows = run(mod, DT, "2026-10-03T21:30:00", tmp)  # Saturday
    check("weekend: no extra row", len(rows), 2)
    check("no price fetched on skipped runs", len(FETCHES), 0)

print(f"\n{'ALL CHECKS PASSED' if failures == 0 else str(failures) + ' CHECK(S) FAILED'}")
sys.exit(0 if failures == 0 else 1)
