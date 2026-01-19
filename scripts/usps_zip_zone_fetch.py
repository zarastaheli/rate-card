import pandas as pd
import requests


def get_zip_code_zones(zip_code_prefix: str) -> dict:
    url = (
        "https://postcalc.usps.com/DomesticZoneChart/GetZoneChart"
        f"?zipCode3Digit={zip_code_prefix}&shippingDate=05%2F05%2F2025&_=1733413695736"
    )
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
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/130.0.0.0 Safari/537.36'
        ),
        'x-requested-with': 'XMLHttpRequest'
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def main():
    df_list = []

    for i in range(5, 1000):
        zip_code_prefix = ('00' + str(i))[-3:]
        print(zip_code_prefix)
        result = get_zip_code_zones(zip_code_prefix)
        for col_idx in range(0, 4):
            df = pd.DataFrame(result[f'Column{col_idx}'])
            df['ZipCodePrefix'] = zip_code_prefix
            df_list.append(df)

    final_df = pd.concat(df_list)
    final_df.to_csv('zip_code_zones_new.csv', index=False)


if __name__ == "__main__":
    main()
