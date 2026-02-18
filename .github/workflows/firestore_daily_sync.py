#!/usr/bin/env python3
"""
Daily upsert of today's USD metal prices into Firestore.

Required env vars:
- FIREBASE_SERVICE_ACCOUNT: path to service account json

Optional env vars:
- FIRESTORE_COLLECTION (default: metals_daily_usd)
- METALS_GOLD_URL (default: https://api.gold-api.com/price/XAU)
- METALS_SILVER_URL (default: https://api.gold-api.com/price/XAG)
- METALS_API_URL (optional combined endpoint that returns both values)
- METALS_API_KEY (optional)
- API_AUTH_HEADER (default: X-API-Key)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
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


def parse_payload(data: Dict[str, Any]) -> dict:
    # Supports flexible payload keys from different providers.
    gold = _extract_number(data, ["gold_oz", "gold", "xau", "price"])
    silver = _extract_number(data, ["silver_oz", "silver", "xag"])
    quote_date = data.get("date")

    if gold is None or silver is None:
        raise ValueError("API payload missing gold/silver values")

    if not quote_date:
        quote_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return {
        "date": quote_date,
        "ts": datetime.strptime(quote_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
        "gold_oz": gold,
        "silver_oz": silver,
        "gold_high": firestore.DELETE_FIELD,
        "gold_low": firestore.DELETE_FIELD,
        "source": "daily-sync",
        "updated_at": firestore.SERVER_TIMESTAMP,
    }


def fetch_spot_price(url: str, headers: Dict[str, str], timeout_s: int = 20) -> float:
    response = requests.get(url, headers=headers, timeout=timeout_s)
    response.raise_for_status()
    data = response.json()
    price = _extract_number(data, ["price", "xau", "xag", "gold", "silver", "value"])
    if price is None:
        raise ValueError(f"Could not parse price from {url}. Response keys: {list(data.keys())}")
    return price


def get_market_snapshot_info() -> tuple[bool, str]:
    """
    Returns:
      should_sync: False on weekend/market-closed window
      quote_date: YYYY-MM-DD in New York market time
    """
    ny_now = datetime.now(ZoneInfo("America/New_York"))
    weekday = ny_now.weekday()  # Mon=0 ... Sun=6
    hour = ny_now.hour

    # Market closed: Fri 17:00+ through Sun 18:00 NY time
    if (weekday == 4 and hour >= 17) or weekday == 5 or (weekday == 6 and hour < 18):
        return False, ny_now.strftime("%Y-%m-%d")

    # Daily maintenance window: 17:00-17:59 NY time (Mon-Thu)
    if weekday in (0, 1, 2, 3) and hour == 17:
        return False, ny_now.strftime("%Y-%m-%d")

    return True, ny_now.strftime("%Y-%m-%d")


def main() -> None:
    service_account = Path(os.environ["FIREBASE_SERVICE_ACCOUNT"])
    api_url = os.environ.get("METALS_API_URL", "").strip()
    gold_url = os.environ.get("METALS_GOLD_URL", "https://api.gold-api.com/price/XAU")
    silver_url = os.environ.get("METALS_SILVER_URL", "https://api.gold-api.com/price/XAG")
    api_key = os.environ.get("METALS_API_KEY", "").strip()
    collection = os.environ.get("FIRESTORE_COLLECTION", "metals_daily_usd")
    auth_header = os.environ.get("API_AUTH_HEADER", "X-API-Key")

    if not service_account.exists():
        raise FileNotFoundError(f"Service account not found: {service_account}")

    if not firebase_admin._apps:
        cred = credentials.Certificate(str(service_account))
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    headers = {"Accept": "application/json"}
    if api_key:
        headers[auth_header] = api_key

    should_sync, quote_date = get_market_snapshot_info()
    if not should_sync:
        print(f"Market closed/maintenance in NY. Skip sync for {quote_date}.")
        return

    if api_url:
        response = requests.get(api_url, headers=headers, timeout=20)
        response.raise_for_status()
        payload = parse_payload(response.json())
        payload["date"] = quote_date
        payload["ts"] = datetime.strptime(quote_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        gold_price = fetch_spot_price(gold_url, headers)
        silver_price = fetch_spot_price(silver_url, headers)
        payload = {
            "date": quote_date,
            "ts": datetime.strptime(quote_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
            "gold_oz": gold_price,
            "silver_oz": silver_price,
            "gold_high": firestore.DELETE_FIELD,
            "gold_low": firestore.DELETE_FIELD,
            "source": "daily-sync",
            "updated_at": firestore.SERVER_TIMESTAMP,
        }

    db.collection(collection).document(payload["date"]).set(payload, merge=True)
    print(f"Upserted {payload['date']} into {collection}")


if __name__ == "__main__":
    main()
