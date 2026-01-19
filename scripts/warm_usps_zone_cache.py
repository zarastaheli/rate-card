#!/usr/bin/env python3
import argparse
import json
import re
import time
from io import BytesIO
from pathlib import Path
import urllib.request

import pandas as pd
import subprocess

DEFAULT_CACHE = Path(__file__).resolve().parents[1] / "runs" / "usps_zone_cache.json"


def zip3_from_zip(value):
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) < 3:
        return None
    return digits[:3].zfill(3)


def load_cache(path):
    if not path.exists():
        return {}
    try:
        with path.open("r") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_cache(path, cache):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(cache, f)


def fetch_zone_chart(origin_zip3):
    url = (
        "https://postcalc.usps.com/DomesticZoneChart/GetZoneChart"
        f"?ZIPCode3Digit={origin_zip3}&ExcelFormat=1"
    )
    content = None
    try:
        request_obj = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(request_obj, timeout=30) as response:
            content = response.read()
    except Exception:
        content = None
    if content is None:
        try:
            result = subprocess.run(
                ["curl", "-L", "-A", "Mozilla/5.0", url],
                capture_output=True,
                check=False
            )
            if result.returncode == 0 and result.stdout:
                content = result.stdout
        except Exception:
            content = None
    if content is None:
        try:
            result = subprocess.run(
                [
                    "curl",
                    "--doh-url",
                    "https://dns.google/dns-query",
                    "-L",
                    "-A",
                    "Mozilla/5.0",
                    url
                ],
                capture_output=True,
                check=False
            )
            if result.returncode == 0 and result.stdout:
                content = result.stdout
        except Exception:
            content = None
    if content is None:
        return {}

    df = None
    try:
        df = pd.read_excel(BytesIO(content), dtype=str)
    except Exception:
        try:
            df = pd.read_csv(BytesIO(content), dtype=str)
        except Exception:
            df = None
    if df is None:
        try:
            html_text = content.decode("utf-8", errors="ignore")
            html_tables = pd.read_html(html_text)
            if html_tables:
                df = html_tables[0].astype(str)
        except Exception:
            df = None
    if df is None or df.empty:
        return {}

    df.columns = [str(c).strip() for c in df.columns]
    zone_col = None
    zip_col = None
    for col in df.columns:
        col_lower = col.lower()
        if zone_col is None and "zone" in col_lower:
            zone_col = col
        if zip_col is None and ("dest" in col_lower or "zip" in col_lower):
            zip_col = col
    if zip_col is None:
        zip_col = df.columns[0]
    if zone_col is None and len(df.columns) > 1:
        zone_col = df.columns[1]

    mapping = {}
    for _, row in df.iterrows():
        dest_raw = row.get(zip_col)
        zone_raw = row.get(zone_col) if zone_col else None
        dest_zip3 = zip3_from_zip(dest_raw)
        if not dest_zip3:
            continue
        zone_match = re.search(r"\d+", str(zone_raw or "").strip())
        if not zone_match:
            continue
        mapping[dest_zip3] = zone_match.group()
    return mapping


def iter_zip3(start, end):
    for value in range(start, end + 1):
        yield f"{value:03d}"


def main():
    parser = argparse.ArgumentParser(description="Warm USPS zone chart cache.")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=999)
    parser.add_argument("--sleep", type=float, default=0.25)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    args = parser.parse_args()

    cache = load_cache(args.cache)
    total = args.end - args.start + 1
    completed = 0

    for zip3 in iter_zip3(args.start, args.end):
        if zip3 in cache and isinstance(cache.get(zip3), dict) and cache[zip3]:
            completed += 1
            continue
        try:
            mapping = fetch_zone_chart(zip3)
        except Exception:
            mapping = {}
        if mapping:
            cache[zip3] = mapping
            save_cache(args.cache, cache)
        completed += 1
        if args.sleep:
            time.sleep(args.sleep)
        if completed % 50 == 0:
            print(f"{completed}/{total} origin ZIP3 cached.")

    save_cache(args.cache, cache)
    print(f"Done. Cached {len(cache)} origin ZIP3 charts at {args.cache}.")


if __name__ == "__main__":
    main()
