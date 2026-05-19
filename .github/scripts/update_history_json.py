#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from zoneinfo import ZoneInfo

import firebase_admin
import requests
from firebase_admin import credentials, firestore


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


def get_market_snapshot_info() -> tuple[bool, str]:
    ny_now = datetime.now(ZoneInfo("America/New_York"))
    weekday = ny_now.weekday()
    return weekday in (0, 1, 2, 3, 4) and ny_now.hour == 17, ny_now.strftime("%Y-%m-%d")


def upsert_firestore(quote_date: str, gold_price: float, silver_price: float) -> None:
    service_account = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "").strip()
    if not service_account:
        print("No FIREBASE_SERVICE_ACCOUNT set; skipping Firestore")
        return

    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account)
        firebase_admin.initialize_app(cred)

    collection = os.environ.get("FIRESTORE_COLLECTION", "metals_daily_usd")
    db = firestore.client()

    payload = {
        "date": quote_date,
        "ts": datetime.strptime(quote_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("UTC")),
        "gold_oz": round(gold_price, 4),
        "silver_oz": round(silver_price, 4),
        "source": "daily-sync",
        "updated_at": firestore.SERVER_TIMESTAMP,
    }

    db.collection(collection).document(quote_date).set(payload, merge=True)
    print(f"Upserted {quote_date} into {collection}")


def main() -> None:
    gold_url = os.environ.get("METALS_GOLD_URL", "https://api.gold-api.com/price/XAU")
    silver_url = os.environ.get("METALS_SILVER_URL", "https://api.gold-api.com/price/XAG")
    api_key = os.environ.get("METALS_API_KEY", "").strip()
    auth_header = os.environ.get("API_AUTH_HEADER", "X-API-Key")
    history_path = Path(os.environ.get("HISTORY_JSON_PATH", "history.json"))

    should_sync, quote_date = get_market_snapshot_info()
    if not should_sync:
        print(f"Outside NY close capture window. Skip update for {quote_date}.")
        return

    headers = {"Accept": "application/json"}
    if api_key:
        headers[auth_header] = api_key

    ts = int(datetime.strptime(quote_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("UTC")).timestamp() * 1000)

    gold_price = fetch_spot_price(gold_url, headers)
    silver_price = fetch_spot_price(silver_url, headers)

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

    by_date[quote_date] = {
        "date": quote_date,
        "timestamp": ts,
        "gold_oz": round(gold_price, 4),
        "silver_oz": round(silver_price, 4),
        "source": "daily-sync",
    }

    merged = sorted(by_date.values(), key=lambda r: r.get("date", ""))
    history_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Updated {history_path} with {quote_date}")
    upsert_firestore(quote_date, gold_price, silver_price)


if __name__ == "__main__":
    main()
