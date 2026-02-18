#!/usr/bin/env python3
"""
Update static history.json with today's gold/silver USD close.

Env vars:
- METALS_GOLD_URL (default: https://api.gold-api.com/price/XAU)
- METALS_SILVER_URL (default: https://api.gold-api.com/price/XAG)
- METALS_API_KEY (optional)
- API_AUTH_HEADER (default: X-API-Key)
- HISTORY_JSON_PATH (default: history.json)
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from zoneinfo import ZoneInfo

import requests


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


def get_market_date_ny() -> str:
    ny_now = datetime.now(ZoneInfo("America/New_York"))
    return ny_now.strftime("%Y-%m-%d")


def main() -> None:
    gold_url = os.environ.get("METALS_GOLD_URL", "https://api.gold-api.com/price/XAU")
    silver_url = os.environ.get("METALS_SILVER_URL", "https://api.gold-api.com/price/XAG")
    api_key = os.environ.get("METALS_API_KEY", "").strip()
    auth_header = os.environ.get("API_AUTH_HEADER", "X-API-Key")
    history_path = Path(os.environ.get("HISTORY_JSON_PATH", "history.json"))

    headers = {"Accept": "application/json"}
    if api_key:
        headers[auth_header] = api_key

    quote_date = get_market_date_ny()
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


if __name__ == "__main__":
    main()

