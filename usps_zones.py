import pandas as pd
import requests
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional


def get_zip_code_zones(zip_code_prefix: str, shipping_date: Optional[str] = None) -> Dict:
    """Fetch USPS zone chart data for a three-digit prefix."""
    base_url = "https://postcalc.usps.com/DomesticZoneChart/GetZoneChart"
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=1, i',
        'referer': 'https://postcalc.usps.com/domesticzonechart',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
        ),
        'x-requested-with': 'XMLHttpRequest'
    }

    def _request_for_date(target_date: str) -> Dict:
        params = {'zipCode3Digit': zip_code_prefix, 'shippingDate': target_date}
        response = requests.get(base_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    request_date = shipping_date or datetime.now().strftime('%m/%d/%Y')
    data = _request_for_date(request_date)
    error = str(data.get('ShippingDateError') or '')
    if error:
        match = re.search(r'between (\d{2}/\d{2}/\d{4}) and (\d{2}/\d{2}/\d{4})', error)
        if match:
            data = _request_for_date(match.group(1))
    return data


def build_zip_zone_dataframe(prefixes: Iterable[int]) -> pd.DataFrame:
    """Compile USPS zone data for the given list of prefixes."""
    frames = []
    for prefix in prefixes:
        zip_prefix = f"{prefix:03d}"
        payload = get_zip_code_zones(zip_prefix)
        for column_idx in range(4):
            column_df = pd.DataFrame(payload.get(f"Column{column_idx}", []))
            if column_df.empty:
                continue
            column_df['ZipCodePrefix'] = zip_prefix
            frames.append(column_df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def save_zip_zone_csv(output_path: Path, prefixes: Optional[Iterable[int]] = None) -> None:
    """Save the USPS zone lookup table to CSV."""
    if prefixes is None:
        prefixes = range(5, 1000)
    df = build_zip_zone_dataframe(prefixes)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    save_zip_zone_csv(Path("zip_code_zones_new.csv"))
