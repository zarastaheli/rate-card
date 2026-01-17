import os
import csv
import json
import uuid
import shutil
import subprocess
import re
import math
import zipfile
import tempfile
import threading
import time
from functools import lru_cache
from datetime import datetime, timedelta, timezone
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_file, session
import pandas as pd
import numpy as np
import openpyxl
from openpyxl.worksheet.formula import ArrayFormula
from openpyxl.utils.cell import range_boundaries, column_index_from_string
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['UPLOAD_FOLDER'] = 'runs'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

dashboard_jobs = {}
dashboard_jobs_lock = threading.Lock()
summary_jobs = {}
summary_jobs_lock = threading.Lock()
carrier_details_jobs = {}
carrier_details_jobs_lock = threading.Lock()

# Thresholds for size/weight classification.
SMALL_MAX_VOLUME = 1728
MEDIUM_MAX_VOLUME = 5000
AMAZON_DAILY_MIN = 150
UNIUNI_WORKDAY_MIN = 300
DEFAULT_WORKING_DAYS_PER_YEAR = 261
WEIGHT_CLASS_BREAKS = [
    (1, '<1'),
    (5, '1-5'),
    (10, '5-10'),
    (float('inf'), '10+')
]

COUNTRY_NAME_TO_CODE = {
    'UNITED STATES': 'US',
    'UNITED STATES OF AMERICA': 'US',
    'USA': 'US',
    'U.S.': 'US',
    'U.S.A.': 'US',
    'US': 'US',
    'CANADA': 'CA',
    'CA': 'CA',
    'MEXICO': 'MX',
    'MEX': 'MX',
    'MX': 'MX',
    'UNITED KINGDOM': 'GB',
    'GREAT BRITAIN': 'GB',
    'ENGLAND': 'GB',
    'UK': 'GB',
    'GB': 'GB',
    'GERMANY': 'DE',
    'DE': 'DE',
    'FRANCE': 'FR',
    'FR': 'FR'
}

CODE_TO_COUNTRY_NAME = {
    'US': 'United States',
    'CA': 'Canada',
    'MX': 'Mexico',
    'GB': 'United Kingdom',
    'DE': 'Germany',
    'FR': 'France'
}

# Ensure runs directory exists
Path(app.config['UPLOAD_FOLDER']).mkdir(exist_ok=True)

# Standard field mappings
STANDARD_FIELDS = {
    'required': [
        'Order Number',
        'Order Date',
        'Zip',
        'Weight',
        'Shipping Carrier',
        'Shipping Service',
        'Package Height',
        'Package Width',
        'Package Length',
        'Zone'
    ],
    'optional': ['Label Cost']
}

def required_fields_for_structure(structure):
    required = ['Shipping Carrier', 'Shipping Service']
    if structure == 'zip':
        required.append('Zip')
    elif structure == 'zone':
        required.append('Zone')
    return required

# Service level normalization
SERVICE_LEVELS = [
    'UPSÂ® Ground',
    'DHL Parcel International Direct - DDU',
    'DHL SM Parcel Expedited',
    'USPS Ground Advantage',
    'UPS 2nd Day AirÂ®',
    'DHL SM Parcel Expedited Max'
]

REDO_CARRIERS = [
    "UniUni",
    "USPS Market",
    "UPS Ground",
    "UPS Ground Saver",
    "FedEx",
    "Amazon"
]

REDO_FORCED_ON = [
    "USPS Market",
    "UPS Ground",
    "UPS Ground Saver"
]
MERCHANT_CARRIERS = ['USPS', 'UPS', 'Amazon', 'FedEx', 'DHL', 'UniUni']
DASHBOARD_CARRIERS = ['UniUni', 'USPS Market', 'UPS Ground', 'UPS Ground Saver', 'FedEx', 'Amazon']
FAST_DASHBOARD_METRICS = True
WEIGHT_BUCKETS = [i / 16 for i in range(1, 16)] + list(range(1, 21))

def strip_after_dash(value):
    if value is None:
        return ""
    text = str(value)
    for sep in (' - ', ' – ', ' — ', '-', '–', '—'):
        if sep in text:
            return text.split(sep, 1)[0].strip()
    return text

BASE_DIR = Path(__file__).resolve().parent
AMAZON_ZIP_PATH = BASE_DIR / 'Amazon Zip list  - Zip Code List.csv'
AMAZON_ZIPS = None
UNIUNI_ZIP_PATH = BASE_DIR / 'UniUni Qualified Zips.txt'
UNIUNI_ZIPS = None

def _load_amazon_zips():
    global AMAZON_ZIPS
    if AMAZON_ZIPS is not None:
        return AMAZON_ZIPS
    zips = set()
    if AMAZON_ZIP_PATH.exists():
        try:
            with AMAZON_ZIP_PATH.open(newline='', encoding='utf-8', errors='ignore') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    raw = row.get('Zip Code') or ''
                    digits = re.sub(r'\D', '', str(raw))
                    if not digits:
                        continue
                    if len(digits) < 5:
                        digits = digits.zfill(5)
                    else:
                        digits = digits[:5]
                    zips.add(digits)
        except Exception:
            zips = set()
    AMAZON_ZIPS = zips
    return AMAZON_ZIPS

def _load_uniuni_zips():
    global UNIUNI_ZIPS
    if UNIUNI_ZIPS is not None:
        return UNIUNI_ZIPS
    zip3 = set()
    zip5 = set()
    if UNIUNI_ZIP_PATH.exists():
        try:
            with UNIUNI_ZIP_PATH.open('r', encoding='utf-8-sig', errors='ignore') as f:
                for line in f:
                    digits = re.sub(r'\D', '', line.strip())
                    if len(digits) == 3:
                        zip3.add(digits)
                    elif len(digits) == 5:
                        zip5.add(digits)
        except Exception:
            zip3 = set()
            zip5 = set()
    UNIUNI_ZIPS = {'zip3': zip3, 'zip5': zip5}
    return UNIUNI_ZIPS

def get_working_days_per_year():
    raw = os.getenv('WORKING_DAYS_PER_YEAR', str(DEFAULT_WORKING_DAYS_PER_YEAR))
    try:
        value = int(raw)
    except Exception:
        value = DEFAULT_WORKING_DAYS_PER_YEAR
    return value if value > 0 else DEFAULT_WORKING_DAYS_PER_YEAR

def is_amazon_eligible(origin_zip):
    digits = re.sub(r'\D', '', str(origin_zip or ''))
    if not digits:
        return False
    amazon_zips = _load_amazon_zips()
    if len(digits) < 5:
        return any(zip_code.startswith(digits) for zip_code in amazon_zips)
    digits = digits[:5]
    return digits in amazon_zips

def is_uniuni_zip_eligible(origin_zip):
    digits = re.sub(r'\D', '', str(origin_zip or ''))
    if not digits:
        return False
    uniuni_zips = _load_uniuni_zips()
    zip3 = uniuni_zips.get('zip3', set())
    zip5 = uniuni_zips.get('zip5', set())
    if len(digits) >= 5:
        first5 = digits[:5]
        if first5 in zip5:
            return True
        return first5[:3] in zip3
    if len(digits) >= 3:
        return digits[:3] in zip3
    return False

def _parse_annual_orders(value):
    if value is None or value == '':
        return None
    try:
        return float(value)
    except Exception:
        return None

def compute_eligibility(origin_zip, annual_orders, working_days_per_year=None, mapping_config=None):
    zip_eligible_amazon = is_amazon_eligible(origin_zip)
    zip_eligible_uniuni = is_uniuni_zip_eligible(origin_zip)

    if mapping_config:
        uniuni_override = (
            mapping_config.get('uniuni_eligible')
            or mapping_config.get('uniuni_qualified')
            or mapping_config.get('uniuni')
        )
        if isinstance(uniuni_override, bool):
            zip_eligible_uniuni = uniuni_override
        elif uniuni_override is not None:
            zip_eligible_uniuni = str(uniuni_override).strip().lower() in ('1', 'true', 'yes', 'y')

    annual_orders_value = _parse_annual_orders(annual_orders)
    if annual_orders_value is None:
        amazon_volume_avg = 0
        uniuni_volume_avg = 0
    else:
        amazon_volume_avg = annual_orders_value / 365
        days = working_days_per_year or get_working_days_per_year()
        uniuni_volume_avg = annual_orders_value / days

    amazon_volume_eligible = amazon_volume_avg >= AMAZON_DAILY_MIN
    uniuni_volume_eligible = uniuni_volume_avg >= UNIUNI_WORKDAY_MIN

    amazon_eligible_final = zip_eligible_amazon and amazon_volume_eligible
    uniuni_eligible_final = zip_eligible_uniuni and uniuni_volume_eligible

    return {
        'zip_eligible_amazon': zip_eligible_amazon,
        'zip_eligible_uniuni': zip_eligible_uniuni,
        'amazon_volume_avg': amazon_volume_avg,
        'amazon_volume_eligible': amazon_volume_eligible,
        'uniuni_volume_avg': uniuni_volume_avg,
        'uniuni_volume_eligible': uniuni_volume_eligible,
        'amazon_eligible_final': amazon_eligible_final,
        'uniuni_eligible_final': uniuni_eligible_final,
        'working_days_per_year': working_days_per_year or get_working_days_per_year()
    }

def _parse_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {'true', '1', 'yes', 'y'}

def _parse_number(value):
    try:
        return float(value)
    except Exception:
        return 0.0

def _saas_tier_name(annual_orders):
    try:
        orders = float(annual_orders)
    except Exception:
        orders = 0
    if orders <= 0:
        return ''
    if orders < 5000:
        return 'Starter Tier'
    if orders < 15000:
        return 'Growth Tier'
    if orders < 35000:
        return 'Pro Tier'
    if orders < 50000:
        return 'Scale Tier'
    return 'Enterprise Tier'

def _effective_orders(annual_orders, comment_sold, ebay, live_selling):
    adjusted = annual_orders
    if comment_sold:
        adjusted *= 0.6
    if ebay:
        adjusted *= 0.6 if live_selling else 0.95
    return adjusted

def default_included_services(services):
    if not services:
        return []
    exclude_tokens = {'PRIORITY', 'NEXT', '2ND', 'SECOND'}
    international_tokens = {'INTERNATIONAL', 'INTL'}
    included = []
    for service in services:
        normalized = normalize_service_name(service)
        normalized_compact = normalized.replace(' ', '')
        if any(token in normalized for token in international_tokens):
            continue
        is_two_day = any(token in normalized for token in exclude_tokens) or any(
            token in normalized_compact for token in ('2DAY', '2NDDAY', 'SECONDDAY')
        )
        if is_two_day:
            if 'DHL' in normalized and 'EXPEDITED' in normalized:
                included.append(service)
                continue
            continue
        included.append(service)
    return included

def normalize_service_name(service):
    """Normalize service name for matching"""
    if not service:
        return ""
    cleaned = strip_after_dash(str(service)).replace('Â', '').replace('®', '')
    # Remove punctuation and symbols, collapse whitespace, uppercase
    normalized = re.sub(r'[^\w\s]', '', cleaned)
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized.upper().strip()

def clean_shipping_service(service):
    """Normalize shipping service for cleaned column output."""
    if not service:
        return ""
    cleaned = strip_after_dash(str(service)).replace('Â', '').replace('®', '')
    cleaned = re.sub(r'[^\w\s]', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.upper().strip()

def extract_zip5(value):
    if value is None:
        return None
    zip_match = re.search(r'\d{5}', str(value))
    if zip_match:
        return int(zip_match.group())
    return None

def extract_origin_zip(value):
    if value is None:
        return None
    digits = re.sub(r'\D', '', str(value))
    if not digits:
        return None
    if len(digits) >= 5:
        return int(digits[:5])
    return int(digits)

def to_float(value):
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return float(value)
    except Exception:
        return None

def normalize_country_code(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    raw = str(value).strip()
    if len(raw) == 2 and raw.isalpha():
        return raw.upper()
    lookup = COUNTRY_NAME_TO_CODE.get(raw.upper())
    return lookup or ""

def normalize_country_name(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    raw = str(value).strip()
    if len(raw) == 2 and raw.isalpha():
        return CODE_TO_COUNTRY_NAME.get(raw.upper(), "")
    return raw

def calculate_shipping_priority(cleaned_service):
    if not cleaned_service:
        return ""
    service = cleaned_service.upper()
    if 'GROUND' in service:
        return 'GROUND'
    if '2ND DAY' in service or '2 DAY' in service or '2DAY' in service:
        return 'AIR'
    if 'EXPEDITED' in service:
        return 'EXPEDITED'
    return 'OTHER'

def classify_package_size(volume):
    if volume is None:
        return ""
    if volume < SMALL_MAX_VOLUME:
        return 'SMALL'
    if volume < MEDIUM_MAX_VOLUME:
        return 'MEDIUM'
    return 'LARGE'

def classify_weight(weight_lbs):
    if weight_lbs is None:
        return ""
    for threshold, label in WEIGHT_CLASS_BREAKS:
        if weight_lbs < threshold:
            return label
    return ""

def infer_redo_carrier(carrier_value, service_value):
    """Infer Redo carrier bucket from carrier/service text."""
    combined = f"{carrier_value or ''} {service_value or ''}"
    text = normalize_service_name(combined)
    if not text:
        return None

    if 'USPS' in text or 'POSTAL' in text or 'GROUND ADVANTAGE' in text or 'USPS MARKET' in text:
        return 'USPS Market'
    if 'UPS GROUND SAVER' in text or 'UPS SAVER' in text:
        return 'UPS Ground Saver'
    if 'UPS' in text and 'GROUND' in text:
        return 'UPS Ground'
    if 'UNIUNI' in text or 'UNI UNI' in text:
        return 'UniUni'
    if 'FEDEX' in text:
        return 'FedEx'
    if 'AMAZON' in text:
        return 'Amazon'
    return None

def extract_invoice_services(raw_df, mapping_config):
    mapping_value = mapping_config.get('mapping', {}).get('Shipping Service')
    normalized_cols = {
        re.sub(r'\W+', '', str(c).strip().lower()): c for c in raw_df.columns
    }
    candidates = []
    for norm, original in normalized_cols.items():
        score = 0
        if norm == 'shippingservice':
            score += 100
        if 'shipping' in norm and 'service' in norm:
            score += 60
        if mapping_value:
            mapping_norm = re.sub(r'\W+', '', str(mapping_value).strip().lower())
            if norm == mapping_norm:
                score += 80
        if score:
            candidates.append((score, original))
    if mapping_value and mapping_value in raw_df.columns:
        candidates.append((90, mapping_value))
    if not candidates:
        return []
    candidates.sort(key=lambda x: x[0], reverse=True)
    for _, service_col in candidates:
        if service_col not in raw_df.columns:
            continue
        services = raw_df[service_col].dropna().astype(str).tolist()
        if any(s.strip() for s in services):
            return services
    return []

def detect_redo_carriers(raw_df, mapping_config):
    carrier_col = mapping_config.get('mapping', {}).get('Shipping Carrier')
    service_col = mapping_config.get('mapping', {}).get('Shipping Service')
    detected = set()
    for _, row in raw_df.iterrows():
        carrier_val = row.get(carrier_col) if carrier_col in raw_df.columns else None
        service_val = row.get(service_col) if service_col in raw_df.columns else None
        inferred = infer_redo_carrier(carrier_val, service_val)
        if inferred:
            detected.add(inferred)
    ordered = [c for c in REDO_CARRIERS if c in detected]
    return ordered

def available_merchant_carriers(raw_df, mapping_config):
    carrier_col = mapping_config.get('mapping', {}).get('Shipping Carrier')
    detected = set()
    for _, row in raw_df.iterrows():
        carrier_val = row.get(carrier_col) if carrier_col in raw_df.columns else None
        normalized = normalize_merchant_carrier(carrier_val)
        if normalized:
            detected.add(normalized)
    display_map = {
        'USPS': 'USPS',
        'UPS': 'UPS',
        'FEDEX': 'FedEx',
        'AMAZON': 'Amazon',
        'DHL': 'DHL',
        'UNIUNI': 'UniUni'
    }
    ordered = []
    for carrier in MERCHANT_CARRIERS:
        key = normalize_merchant_carrier(carrier)
        if key in detected:
            ordered.append(display_map.get(key, carrier))
    return ordered

def available_merchant_services(raw_df, mapping_config):
    services = extract_invoice_services(raw_df, mapping_config)
    if not services:
        return []
    normalized_invoice = {normalize_service_name(s) for s in services if s}
    canonical_map = {normalize_service_name(s): s for s in SERVICE_LEVELS}
    available = [canonical_map[n] for n in canonical_map if n in normalized_invoice]
    if available:
        return available
    # Fallback: return unique services from invoice when no canonical match exists.
    seen = set()
    ordered = []
    for service in services:
        if not service:
            continue
        cleaned = str(service).replace('Â', '').replace('®', '').strip()
        cleaned = re.sub(r'\s+', ' ', cleaned)
        display = cleaned.upper().strip()
        norm = re.sub(r'[^A-Z0-9]+', '', display)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        ordered.append(display)
    return ordered

def normalize_redo_label(label):
    if not label:
        return ""
    text = re.sub(r'\([^)]*\)', '', strip_after_dash(str(label)))
    text = text.replace('Â', '').replace('®', '')
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.upper().strip()

def normalize_merchant_carrier(value):
    text = normalize_redo_label(value)
    if not text:
        return ""
    if 'USPS' in text or 'POSTAL' in text:
        return 'USPS'
    if 'UPS' in text:
        return 'UPS'
    if 'FEDEX' in text:
        return 'FEDEX'
    if 'AMAZON' in text:
        return 'AMAZON'
    if 'DHL' in text:
        return 'DHL'
    if 'UNIUNI' in text or 'UNI UNI' in text:
        return 'UNIUNI'
    return ""

def _dashboard_selected_from_redo(selected_redo):
    selected = set()
    if not selected_redo:
        return selected
    if 'UniUni' in selected_redo:
        selected.add('UniUni')
    if 'USPS Market' in selected_redo:
        selected.add('USPS Market')
    if 'UPS Ground' in selected_redo:
        selected.add('UPS Ground')
    if 'UPS Ground Saver' in selected_redo:
        selected.add('UPS Ground Saver')
    if 'Amazon' in selected_redo:
        selected.add('Amazon')
    if 'FedEx' in selected_redo:
        selected.add('FedEx')
    if 'DHL' in selected_redo:
        selected.add('DHL')
    return selected

def _redo_selection_from_dashboard(selected_dashboard):
    selected = set()
    if not selected_dashboard:
        return selected
    if 'UniUni' in selected_dashboard:
        selected.add('UNIUNI')
    if 'USPS Market' in selected_dashboard:
        selected.add('USPS MARKET')
    if 'UPS Ground' in selected_dashboard:
        selected.add('UPS GROUND')
    if 'UPS Ground Saver' in selected_dashboard:
        selected.add('UPS GROUND SAVER')
    if 'Amazon' in selected_dashboard:
        selected.add('AMAZON')
    if 'FedEx' in selected_dashboard:
        selected.add('FEDEX')
    if 'DHL' in selected_dashboard:
        selected.add('DHL')
    return selected

def _read_summary_metrics(ws):
    labels = [
        'Est. Merchant Annual Savings',
        'Est. Redo Deal Size',
        'Spread Available',
        '% Orders We Could Win',
        '% Orders Won W/ Spread'
    ]
    values = {}
    for row in ws.iter_rows():
        for cell in row:
            if cell.value in labels:
                value_cell = ws.cell(cell.row, cell.column + 1)
                values[cell.value] = value_cell.value
    return values

def _apply_redo_selection(ws, selected_dashboard):
    header_row_idx, label_col, use_col = _find_pricing_section(ws, 'Redo Carriers')
    if header_row_idx is None:
        return
    selection = _redo_selection_from_dashboard(selected_dashboard)
    stop_titles = {'MERCHANT CARRIERS', 'MERCHANT CARRIER', 'MERCHANT SERVICE LEVELS'}
    for row_idx, label_val in _iter_section_rows(ws, header_row_idx + 1, label_col, stop_titles):
        normalized = normalize_redo_label(label_val)
        target_cell = ws.cell(row_idx, use_col)
        if 'FIRST MILE' in normalized:
            target_cell.value = 'No'
            continue
        target_cell.value = 'Yes' if normalized in selection else 'No'

RATE_TABLE_COLUMNS = {
    'UPS Ground': ('N', 'U'),
    'FedEx': ('X', 'AE'),
    'UniUni': ('AH', 'AO'),
    'Amazon': ('BC', 'BJ'),
    'USPS Market': ('BO', 'BV'),
    'UPS Ground Saver': ('CI', 'CP')
}
CARRIER_PRIORITY = [
    'UPS Ground',
    'FedEx',
    'UniUni',
    'Amazon',
    'USPS Market',
    'UPS Ground Saver'
]

@lru_cache(maxsize=4)
def _get_pricing_controls(template_path_str):
    path = Path(template_path_str)
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb['Pricing & Summary']
    controls = {
        'k2': ws['K2'].value,
        'g2': ws['G2'].value,
        'c2': ws['C2'].value,
        'c19': ws['C19'].value or 0,
        'c20': ws['C20'].value or 0,
        'c22': ws['C22'].value or 0,
        'c23': ws['C23'].value or 0,
        'c25': ws['C25'].value or 0,
        'c26': ws['C26'].value or 0
    }
    wb.close()
    return controls

@lru_cache(maxsize=4)
def _load_rate_tables(template_path_str):
    path = Path(template_path_str)
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb['Redo Rate Cards']
    tables = {}
    for carrier, (start_col, end_col) in RATE_TABLE_COLUMNS.items():
        start_idx = column_index_from_string(start_col)
        end_idx = column_index_from_string(end_col)
        rates = {}
        for row in range(145, 210):
            zone_rates = {}
            for zone, col_idx in enumerate(range(start_idx, end_idx + 1), start=1):
                value = ws.cell(row, col_idx).value
                if value is None:
                    zone_rates[zone] = None
                else:
                    try:
                        zone_rates[zone] = float(value)
                    except Exception:
                        zone_rates[zone] = None
            rates[row] = zone_rates
        tables[carrier] = rates
    wb.close()
    return tables

def _rate_row_for_bucket(weight_bucket):
    if weight_bucket is None:
        return None
    if weight_bucket < 1:
        oz = int(round(weight_bucket * 16))
        if oz <= 0:
            return None
        return 144 + oz
    lbs = int(round(weight_bucket))
    if lbs <= 0:
        return None
    return 159 + lbs

def _compute_first_mile_weight(weight_oz, weight_lbs):
    weight_oz = pd.to_numeric(weight_oz, errors='coerce')
    weight_lbs = pd.to_numeric(weight_lbs, errors='coerce')
    output = pd.Series(np.nan, index=weight_oz.index)
    oz_mask = weight_oz.notna()
    if oz_mask.any():
        oz = weight_oz[oz_mask]
        output.loc[oz_mask] = np.where(
            oz < 16,
            np.ceil(oz).astype(int) / 16,
            np.ceil(oz / 16).astype(int)
        )
    lbs_mask = ~oz_mask & weight_lbs.notna()
    if lbs_mask.any():
        lbs = weight_lbs[lbs_mask]
        output.loc[lbs_mask] = np.where(
            lbs < 1,
            np.ceil(lbs * 16).astype(int) / 16,
            np.ceil(lbs).astype(int)
        )
    return output.round(4)

def _coerce_float(value):
    if value is None or value == '':
        return None
    try:
        return float(value)
    except Exception:
        return None

def _normalize_percent(value):
    number = _coerce_float(value)
    if number is None:
        return None
    if 1 < number <= 100:
        return number / 100
    return number

def _read_carrier_details_from_ws(ws):
    details = {}
    for row in range(35, 41):
        carrier = ws.cell(row, 2).value
        if not carrier:
            continue
        spread = _coerce_float(ws.cell(row, 4).value)
        orders_won_pct = _normalize_percent(ws.cell(row, 3).value)
        key = normalize_redo_label(carrier)
        details[key] = {
            'carrier': str(carrier).strip(),
            'spread': spread,
            'orders_won_pct': orders_won_pct
        }
    return details

@lru_cache(maxsize=16)
def _load_carrier_details_cached(rate_card_path_str, source_mtime):
    path = Path(rate_card_path_str)
    if not path.exists():
        return {}
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    if 'Pricing & Summary' not in wb.sheetnames:
        wb.close()
        return {}
    ws = wb['Pricing & Summary']
    details = _read_carrier_details_from_ws(ws)
    wb.close()
    return details

def _load_carrier_details(rate_card_path, source_mtime=None):
    if not rate_card_path:
        return {}
    try:
        mtime = source_mtime or int(rate_card_path.stat().st_mtime)
    except Exception:
        mtime = 0
    return _load_carrier_details_cached(str(rate_card_path), mtime)

def _load_carrier_details_for_selection(job_dir, selected_dashboard, mapping_config, profile_dir=None):
    return _calculate_carrier_details_fast(job_dir, selected_dashboard, mapping_config)

def _carrier_details_cache_path(job_dir):
    return Path(job_dir) / 'carrier_details.json'

def _carrier_details_job_key(job_dir, source_mtime, selection_key):
    return f"{Path(job_dir).name}:{source_mtime}:{selection_key}"

def _read_carrier_details_cache(job_dir, source_mtime, selection_key):
    cache_path = _carrier_details_cache_path(job_dir)
    if not cache_path.exists():
        return None
    try:
        with open(cache_path, 'r') as f:
            cache = json.load(f)
        if cache.get('source_mtime') != source_mtime:
            return None
        entries = cache.get('entries', {})
        return entries.get(selection_key)
    except Exception:
        return None

def _write_carrier_details_cache(job_dir, source_mtime, selection_key, details):
    cache_path = _carrier_details_cache_path(job_dir)
    if not isinstance(details, dict):
        details = {}
    payload = {'source_mtime': source_mtime, 'updated_at': datetime.now(timezone.utc).isoformat(), 'entries': {}}
    if cache_path.exists():
        try:
            with open(cache_path, 'r') as f:
                existing = json.load(f)
            if existing.get('source_mtime') == source_mtime:
                payload = existing
        except Exception:
            payload = {'source_mtime': source_mtime, 'updated_at': datetime.now(timezone.utc).isoformat(), 'entries': {}}
    payload['source_mtime'] = source_mtime
    payload['updated_at'] = datetime.now(timezone.utc).isoformat()
    payload.setdefault('entries', {})
    payload['entries'][selection_key] = details
    with open(cache_path, 'w') as f:
        json.dump(payload, f)

def _build_carrier_details_cache(job_dir, source_mtime, selection_key, selected_dashboard, mapping_config, job_key):
    try:
        details = _calculate_carrier_details_fast(job_dir, selected_dashboard, mapping_config)
        if not isinstance(details, dict):
            details = {}
        _write_carrier_details_cache(job_dir, source_mtime, selection_key, details)
    finally:
        with carrier_details_jobs_lock:
            carrier_details_jobs.pop(job_key, None)

def _start_carrier_details_cache(job_dir, source_mtime, selection_key, selected_dashboard, mapping_config):
    cached = _read_carrier_details_cache(job_dir, source_mtime, selection_key)
    if cached is not None:
        return cached, False
    details = _calculate_carrier_details_fast(job_dir, selected_dashboard, mapping_config)
    if not isinstance(details, dict):
        details = {}
    _write_carrier_details_cache(job_dir, source_mtime, selection_key, details)
    return details, False

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            temp_input = tmp_dir_path / source_path.name
            shutil.copy2(source_path, temp_input)

            wb = openpyxl.load_workbook(temp_input, data_only=False)
            if 'Pricing & Summary' not in wb.sheetnames:
                wb.close()
                return {}
            ws = wb['Pricing & Summary']
            _apply_redo_selection(ws, selected_dashboard)
            pct_off, dollar_off = _usps_market_discount_values(mapping_config)
            ws['C19'] = pct_off
            ws['C20'] = dollar_off
            wb.save(temp_input)
            wb.close()

            recalculated_path = _recalc_workbook(temp_input, tmp_dir_path, profile_dir=profile_dir)
            result_wb = openpyxl.load_workbook(recalculated_path, data_only=True, read_only=True)
            result_ws = result_wb['Pricing & Summary']
            details = _read_carrier_details_from_ws(result_ws)
            result_wb.close()
            return details
    except Exception:
        try:
            source_mtime = int(source_path.stat().st_mtime)
        except Exception:
            source_mtime = 0
        return _load_carrier_details(source_path, source_mtime)

def _mode_or_min(series):
    series = series.dropna()
    if series.empty:
        return None
    counts = series.value_counts()
    if not counts.empty:
        return float(counts.index[0])
    return float(series.min())

def _calculate_metrics_fast(job_dir, selected_dashboard, mapping_config):
    normalized_csv = job_dir / 'normalized.csv'
    if not normalized_csv.exists():
        return {}
    normalized_df = pd.read_csv(normalized_csv)
    if normalized_df.empty:
        return {}

    template_path = Path('#New Template - Rate Card.xlsx')
    if not template_path.exists():
        template_path = Path('Rate Card Template.xlsx')
    rate_tables = _load_rate_tables(str(template_path))
    controls = dict(_get_pricing_controls(str(template_path)))
    pct_off, dollar_off = _usps_market_discount_values(mapping_config)
    controls['c19'] = pct_off
    controls['c20'] = dollar_off

    merchant_pricing = {'excluded_carriers': [], 'included_services': []}
    pricing_file = job_dir / 'merchant_pricing.json'
    if pricing_file.exists():
        with open(pricing_file, 'r') as f:
            merchant_pricing = json.load(f)
    excluded_carriers = merchant_pricing.get('excluded_carriers', [])
    included_services = merchant_pricing.get('included_services', [])
    if not included_services:
        available_services = _unique_cleaned_services(normalized_df)
        included_services = default_included_services(available_services)

    normalized_selected = {normalize_service_name(s) for s in included_services}
    normalized_excluded = {normalize_merchant_carrier(c) for c in excluded_carriers}

    service_series = normalized_df.get('CLEANED_SHIPPING_SERVICE')
    if service_series is None:
        service_series = normalized_df.get('Shipping Service')
    carrier_series = normalized_df.get('Shipping Carrier')
    if service_series is None:
        service_series = pd.Series([""] * len(normalized_df))
    if carrier_series is None:
        carrier_series = pd.Series([""] * len(normalized_df))

    service_norm = service_series.fillna("").astype(str).apply(normalize_service_name)
    carrier_norm = carrier_series.fillna("").astype(str).apply(normalize_merchant_carrier)
    carrier_allowed = ~carrier_norm.isin(normalized_excluded)
    qualified = service_norm.isin(normalized_selected) & carrier_allowed

    weight_oz = normalized_df.get('WEIGHT_IN_OZ')
    if weight_oz is None:
        weight_oz = normalized_df.get('Weight')
    if weight_oz is None:
        weight_oz = pd.Series([np.nan] * len(normalized_df))
    weight_lbs = normalized_df.get('WEIGHT_IN_LBS')
    if weight_lbs is None:
        weight_lbs = pd.Series([np.nan] * len(normalized_df))

    weight_bucket = _compute_first_mile_weight(weight_oz, weight_lbs)
    zone_series = normalized_df.get('Zone')
    if zone_series is None:
        zone_series = normalized_df.get('ZONE')
    if zone_series is None:
        zone_series = pd.Series([np.nan] * len(normalized_df))
    zone = pd.to_numeric(zone_series, errors='coerce').astype('Int64')

    label_cost = normalized_df.get('Label Cost')
    if label_cost is None:
        label_cost = normalized_df.get('LABEL_COST')
    if label_cost is None:
        label_cost = pd.Series([np.nan] * len(normalized_df))
    label_cost = pd.to_numeric(label_cost, errors='coerce')

    work_df = pd.DataFrame({
        'zone': zone,
        'weight_bucket': weight_bucket,
        'label_cost': label_cost,
        'qualified': qualified
    })
    work_df = work_df[work_df['zone'].between(1, 8)]
    work_df = work_df[work_df['weight_bucket'].isin(WEIGHT_BUCKETS)]
    if work_df.empty:
        return {}

    count_all = work_df.groupby(['zone', 'weight_bucket']).size()
    qualified_df = work_df[work_df['qualified']]
    count_qualified = qualified_df.groupby(['zone', 'weight_bucket']).size()

    merchant_rate = None
    if controls['k2'] == 'USPS Market Rates':
        merchant_rate = {}
        usps_rates = rate_tables.get('USPS Market', {})
        for (zone_val, weight_val), count_val in count_all.items():
            row_idx = _rate_row_for_bucket(weight_val)
            rate = None
            if row_idx and row_idx in usps_rates:
                rate = usps_rates[row_idx].get(int(zone_val))
            if rate is not None:
                merchant_rate[(zone_val, weight_val)] = rate
    else:
        if controls['g2'] == 'Minimum Rates':
            merchant_rate = qualified_df.groupby(['zone', 'weight_bucket'])['label_cost'].min()
        else:
            merchant_rate = qualified_df.groupby(['zone', 'weight_bucket'])['label_cost'].apply(_mode_or_min)

    total_qualified = 0
    for key, count_val in count_qualified.items():
        if count_val > 0:
            total_qualified += count_val
    if total_qualified <= 0:
        return {}

    selected_carriers = [c for c in selected_dashboard if c in rate_tables]
    if not selected_carriers:
        return {}

    savings_all = 0.0
    savings_won = 0.0
    spread_all = 0.0
    spread_won = 0.0
    winable_count = 0.0
    won_count = 0.0
    usps_won_count = 0.0
    ups_won_count = 0.0

    c19 = float(controls['c19'] or 0)
    c20 = float(controls['c20'] or 0)
    usps_rates = rate_tables.get('USPS Market', {})

    for (zone_val, weight_val), count_val in count_all.items():
        count_q = count_qualified.get((zone_val, weight_val), 0)
        if count_q <= 0:
            continue
        merchant = merchant_rate.get((zone_val, weight_val))
        if merchant is None or (isinstance(merchant, float) and math.isnan(merchant)):
            continue
        row_idx = _rate_row_for_bucket(weight_val)
        if not row_idx:
            continue
        redo_rates = {}
        for carrier in selected_carriers:
            rate = rate_tables.get(carrier, {}).get(row_idx, {}).get(int(zone_val))
            if rate is not None:
                redo_rates[carrier] = rate
        if not redo_rates:
            continue
        min_rate = min(redo_rates.values())
        winning_carrier = None
        for carrier in CARRIER_PRIORITY:
            rate = redo_rates.get(carrier)
            if rate is not None and abs(rate - min_rate) < 1e-9:
                winning_carrier = carrier
                break
        if winning_carrier is None:
            winning_carrier = min(redo_rates, key=redo_rates.get)

        redo_rate = min_rate
        usps_market_rate = None
        if row_idx and row_idx in usps_rates:
            usps_market_rate = usps_rates[row_idx].get(int(zone_val))
        if winning_carrier in {'USPS Market', 'UPS Ground', 'UPS Ground Saver'}:
            rate_offered = redo_rate
        else:
            base_rate = merchant
            if winning_carrier in {'UniUni', 'Amazon'} and usps_market_rate is not None:
                base_rate = usps_market_rate
            if base_rate is None or (isinstance(base_rate, float) and math.isnan(base_rate)):
                continue
            if c19 > 0:
                rate_offered = max(redo_rate, base_rate * (1 - c19))
            else:
                rate_offered = max(redo_rate, base_rate - c20)

        savings = merchant - rate_offered
        base_savings = merchant - redo_rate
        spread = rate_offered - redo_rate

        savings_all += savings * count_val
        spread_all += spread * count_val
        if savings >= 0:
            savings_won += savings * count_val
            spread_won += spread * count_val
            won_count += count_val
            if winning_carrier == 'USPS Market':
                usps_won_count += count_val
            if winning_carrier in {'UPS Ground', 'UPS Ground Saver'}:
                ups_won_count += count_val
        if base_savings >= 0:
            winable_count += count_val

    orders_in_analysis = total_qualified
    annual_orders = None
    try:
        annual_orders = int(float(mapping_config.get('annual_orders'))) if mapping_config.get('annual_orders') else None
    except Exception:
        annual_orders = None
    scale_factor = 1.0
    if annual_orders and orders_in_analysis:
        scale_factor = orders_in_analysis / annual_orders

    if controls['c2'] == 'All Orders':
        est_savings = savings_all
        est_redo_deal = spread_all
    else:
        est_savings = savings_won / scale_factor if scale_factor else savings_won
        est_redo_deal = spread_won / scale_factor if scale_factor else spread_won

    annual_orders_value = annual_orders or orders_in_analysis
    usps_won_pct = usps_won_count / total_qualified if total_qualified else 0
    ups_won_pct = ups_won_count / total_qualified if total_qualified else 0
    avg_qualified_label_cost = (
        float(qualified_df['label_cost'].mean())
        if not qualified_df.empty and qualified_df['label_cost'].notna().any()
        else 0.0
    )
    selected_set = set(selected_dashboard or [])
    if selected_set and selected_set.issubset({'USPS Market'}):
        est_redo_deal = 0.20 * annual_orders_value * usps_won_pct
    elif selected_set and selected_set.issubset({'UPS Ground', 'UPS Ground Saver'}):
        est_redo_deal = avg_qualified_label_cost * 0.11 * annual_orders_value * ups_won_pct

    spread_available = est_savings + est_redo_deal
    orders_winable = winable_count / total_qualified if total_qualified else 0
    orders_won = won_count / total_qualified if total_qualified else 0

    return {
        'Est. Merchant Annual Savings': est_savings,
        'Est. Redo Deal Size': est_redo_deal,
        'Spread Available': spread_available,
        '% Orders We Could Win': orders_winable,
        '% Orders Won W/ Spread': orders_won
    }

def _calculate_carrier_details_fast(job_dir, selected_dashboard, mapping_config):
    normalized_csv = Path(job_dir) / 'normalized.csv'
    if not normalized_csv.exists():
        return {}
    normalized_df = pd.read_csv(normalized_csv)
    if normalized_df.empty:
        return {}

    template_path = Path('#New Template - Rate Card.xlsx')
    if not template_path.exists():
        template_path = Path('Rate Card Template.xlsx')
    rate_tables = _load_rate_tables(str(template_path))
    controls = dict(_get_pricing_controls(str(template_path)))
    pct_off, dollar_off = _usps_market_discount_values(mapping_config)
    controls['c19'] = pct_off
    controls['c20'] = dollar_off

    merchant_pricing = {'excluded_carriers': [], 'included_services': []}
    pricing_file = Path(job_dir) / 'merchant_pricing.json'
    if pricing_file.exists():
        with open(pricing_file, 'r') as f:
            merchant_pricing = json.load(f)
    excluded_carriers = merchant_pricing.get('excluded_carriers', [])
    included_services = merchant_pricing.get('included_services', [])
    if not included_services:
        available_services = _unique_cleaned_services(normalized_df)
        included_services = default_included_services(available_services)

    normalized_selected = {normalize_service_name(s) for s in included_services}
    normalized_excluded = {normalize_merchant_carrier(c) for c in excluded_carriers}

    service_series = normalized_df.get('CLEANED_SHIPPING_SERVICE')
    if service_series is None:
        service_series = normalized_df.get('Shipping Service')
    carrier_series = normalized_df.get('Shipping Carrier')
    if service_series is None:
        service_series = pd.Series([""] * len(normalized_df))
    if carrier_series is None:
        carrier_series = pd.Series([""] * len(normalized_df))

    service_norm = service_series.fillna("").astype(str).apply(normalize_service_name)
    carrier_norm = carrier_series.fillna("").astype(str).apply(normalize_merchant_carrier)
    carrier_allowed = ~carrier_norm.isin(normalized_excluded)
    qualified = service_norm.isin(normalized_selected) & carrier_allowed

    weight_oz = normalized_df.get('WEIGHT_IN_OZ')
    if weight_oz is None:
        weight_oz = normalized_df.get('Weight')
    if weight_oz is None:
        weight_oz = pd.Series([np.nan] * len(normalized_df))
    weight_lbs = normalized_df.get('WEIGHT_IN_LBS')
    if weight_lbs is None:
        weight_lbs = pd.Series([np.nan] * len(normalized_df))

    weight_bucket = _compute_first_mile_weight(weight_oz, weight_lbs)
    zone_series = normalized_df.get('Zone')
    if zone_series is None:
        zone_series = normalized_df.get('ZONE')
    if zone_series is None:
        zone_series = pd.Series([np.nan] * len(normalized_df))
    zone = pd.to_numeric(zone_series, errors='coerce').astype('Int64')

    label_cost = normalized_df.get('Label Cost')
    if label_cost is None:
        label_cost = normalized_df.get('LABEL_COST')
    if label_cost is None:
        label_cost = pd.Series([np.nan] * len(normalized_df))
    label_cost = pd.to_numeric(label_cost, errors='coerce')

    work_df = pd.DataFrame({
        'zone': zone,
        'weight_bucket': weight_bucket,
        'label_cost': label_cost,
        'qualified': qualified
    })
    work_df = work_df[work_df['zone'].between(1, 8)]
    work_df = work_df[work_df['weight_bucket'].isin(WEIGHT_BUCKETS)]
    if work_df.empty:
        return {}

    count_all = work_df.groupby(['zone', 'weight_bucket']).size()
    qualified_df = work_df[work_df['qualified']]
    count_qualified = qualified_df.groupby(['zone', 'weight_bucket']).size()

    merchant_rate = None
    if controls['k2'] == 'USPS Market Rates':
        merchant_rate = {}
        usps_rates = rate_tables.get('USPS Market', {})
        for (zone_val, weight_val), count_val in count_all.items():
            row_idx = _rate_row_for_bucket(weight_val)
            rate = None
            if row_idx and row_idx in usps_rates:
                rate = usps_rates[row_idx].get(int(zone_val))
            if rate is not None:
                merchant_rate[(zone_val, weight_val)] = rate
    else:
        if controls['g2'] == 'Minimum Rates':
            merchant_rate = qualified_df.groupby(['zone', 'weight_bucket'])['label_cost'].min()
        else:
            merchant_rate = qualified_df.groupby(['zone', 'weight_bucket'])['label_cost'].apply(_mode_or_min)

    total_qualified = 0
    for key, count_val in count_qualified.items():
        if count_val > 0:
            total_qualified += count_val
    if total_qualified <= 0:
        return {}

    selected_carriers = [c for c in (selected_dashboard or []) if c in rate_tables]
    if not selected_carriers:
        return {}

    won_counts = {carrier: 0.0 for carrier in DASHBOARD_CARRIERS}
    spread_sums = {carrier: 0.0 for carrier in DASHBOARD_CARRIERS}

    c19 = float(controls['c19'] or 0)
    c20 = float(controls['c20'] or 0)
    usps_rates = rate_tables.get('USPS Market', {})

    for (zone_val, weight_val), count_val in count_all.items():
        count_q = count_qualified.get((zone_val, weight_val), 0)
        if count_q <= 0:
            continue
        merchant = merchant_rate.get((zone_val, weight_val))
        if merchant is None or (isinstance(merchant, float) and math.isnan(merchant)):
            continue
        row_idx = _rate_row_for_bucket(weight_val)
        if not row_idx:
            continue
        redo_rates = {}
        for carrier in selected_carriers:
            rate = rate_tables.get(carrier, {}).get(row_idx, {}).get(int(zone_val))
            if rate is not None:
                redo_rates[carrier] = rate
        if not redo_rates:
            continue
        min_rate = min(redo_rates.values())
        winning_carrier = None
        for carrier in CARRIER_PRIORITY:
            rate = redo_rates.get(carrier)
            if rate is not None and abs(rate - min_rate) < 1e-9:
                winning_carrier = carrier
                break
        if winning_carrier is None:
            winning_carrier = min(redo_rates, key=redo_rates.get)

        redo_rate = min_rate
        usps_market_rate = None
        if row_idx and row_idx in usps_rates:
            usps_market_rate = usps_rates[row_idx].get(int(zone_val))
        if winning_carrier in {'USPS Market', 'UPS Ground', 'UPS Ground Saver'}:
            rate_offered = redo_rate
        else:
            base_rate = merchant
            if winning_carrier in {'UniUni', 'Amazon'} and usps_market_rate is not None:
                base_rate = usps_market_rate
            if base_rate is None or (isinstance(base_rate, float) and math.isnan(base_rate)):
                continue
            if c19 > 0:
                rate_offered = max(redo_rate, base_rate * (1 - c19))
            else:
                rate_offered = max(redo_rate, base_rate - c20)

        spread = rate_offered - redo_rate
        won_counts[winning_carrier] += count_q
        spread_sums[winning_carrier] += spread * count_q

    details = {}
    for carrier in DASHBOARD_CARRIERS:
        won_count = won_counts.get(carrier, 0.0)
        orders_pct = won_count / total_qualified if total_qualified else 0.0
        spread_total = spread_sums.get(carrier, 0.0)
        spread_avg = spread_total / won_count if won_count else 0.0
        details[normalize_redo_label(carrier)] = {
            'carrier': carrier,
            'spread': spread_avg,
            'spread_total': spread_total,
            'orders_won_pct': orders_pct
        }
    return details

def _recalc_workbook(input_path, output_dir, profile_dir=None):
    soffice = shutil.which('soffice')
    if not soffice:
        candidate = Path('/Applications/LibreOffice.app/Contents/MacOS/soffice')
        if candidate.exists():
            soffice = str(candidate)
    if not soffice:
        raise RuntimeError('LibreOffice (soffice) not found')
    cmd = [
        soffice,
        '--headless',
        '--invisible',
        '--nologo',
        '--nofirststartwizard',
        '--norestore',
        '--nocrashreport'
    ]
    if profile_dir:
        cmd.append(f"--env:UserInstallation={Path(profile_dir).resolve().as_uri()}")
    cmd.extend(['--convert-to', 'xlsx', '--outdir', str(output_dir), str(input_path)])
    subprocess.run(
        cmd,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    candidates = list(Path(output_dir).glob('*.xlsx'))
    if not candidates:
        raise RuntimeError('LibreOffice did not produce an output file')
    return max(candidates, key=lambda p: p.stat().st_mtime)

def _recalc_workbooks(input_paths, output_dir, profile_dir=None):
    if not input_paths:
        return {}
    soffice = shutil.which('soffice')
    if not soffice:
        candidate = Path('/Applications/LibreOffice.app/Contents/MacOS/soffice')
        if candidate.exists():
            soffice = str(candidate)
    if not soffice:
        raise RuntimeError('LibreOffice (soffice) not found')
    cmd = [
        soffice,
        '--headless',
        '--invisible',
        '--nologo',
        '--nofirststartwizard',
        '--norestore',
        '--nocrashreport'
    ]
    if profile_dir:
        cmd.append(f"--env:UserInstallation={Path(profile_dir).resolve().as_uri()}")
    cmd.extend(['--convert-to', 'xlsx', '--outdir', str(output_dir)])
    cmd.extend([str(p) for p in input_paths])
    subprocess.run(
        cmd,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    outputs = list(Path(output_dir).glob('*.xlsx'))
    if not outputs:
        raise RuntimeError('LibreOffice did not produce output files')
    by_stem = {}
    for path in outputs:
        by_stem.setdefault(path.stem, []).append(path)
    mapping = {}
    for input_path in input_paths:
        stem = Path(input_path).stem
        candidates = by_stem.get(stem, [])
        if not candidates:
            continue
        mapping[input_path] = max(candidates, key=lambda p: p.stat().st_mtime)
    if len(mapping) != len(input_paths):
        missing = [p for p in input_paths if p not in mapping]
        raise RuntimeError(f'LibreOffice did not produce output for: {missing}')
    return mapping

def _calculate_metrics(job_dir, selected_dashboard, profile_dir=None):
    if FAST_DASHBOARD_METRICS:
        mapping_file = job_dir / 'mapping.json'
        mapping_config = {}
        if mapping_file.exists():
            with open(mapping_file, 'r') as f:
                mapping_config = json.load(f)
        try:
            return _calculate_metrics_fast(job_dir, selected_dashboard, mapping_config)
        except Exception:
            pass

    rate_card_files = list(job_dir.glob('* - Rate Card.xlsx'))
    if not rate_card_files:
        raise FileNotFoundError('Rate card not found')
    source_path = rate_card_files[0]
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        temp_input = tmp_dir_path / source_path.name
        shutil.copy2(source_path, temp_input)

        wb = openpyxl.load_workbook(temp_input, data_only=False)
        if 'Pricing & Summary' not in wb.sheetnames:
            wb.close()
            raise ValueError('Pricing & Summary sheet not found')
        ws = wb['Pricing & Summary']
        _apply_redo_selection(ws, selected_dashboard)
        wb.save(temp_input)
        wb.close()

        recalculated_path = _recalc_workbook(temp_input, tmp_dir_path, profile_dir=profile_dir)
        result_wb = openpyxl.load_workbook(recalculated_path, data_only=True, read_only=True)
        result_ws = result_wb['Pricing & Summary']
        metrics = _read_summary_metrics(result_ws)
        result_wb.close()
    return metrics

def _calculate_metrics_batch(job_dir, selections, profile_dir=None):
    if FAST_DASHBOARD_METRICS:
        mapping_file = job_dir / 'mapping.json'
        mapping_config = {}
        if mapping_file.exists():
            with open(mapping_file, 'r') as f:
                mapping_config = json.load(f)
        results = {}
        for key, selected_dashboard in selections.items():
            results[key] = _calculate_metrics_fast(job_dir, selected_dashboard, mapping_config)
        return results

    rate_card_files = list(job_dir.glob('* - Rate Card.xlsx'))
    if not rate_card_files:
        raise FileNotFoundError('Rate card not found')
    source_path = rate_card_files[0]
    results = {}
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        input_paths = []
        key_to_input = {}
        for key, selected_dashboard in selections.items():
            safe_key = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(key)).strip('_') or 'selection'
            temp_input = tmp_dir_path / f"{safe_key}-{source_path.name}"
            shutil.copy2(source_path, temp_input)

            wb = openpyxl.load_workbook(temp_input, data_only=False)
            if 'Pricing & Summary' not in wb.sheetnames:
                wb.close()
                raise ValueError('Pricing & Summary sheet not found')
            ws = wb['Pricing & Summary']
            _apply_redo_selection(ws, selected_dashboard)
            wb.save(temp_input)
            wb.close()

            input_paths.append(temp_input)
            key_to_input[key] = temp_input

        output_map = _recalc_workbooks(input_paths, tmp_dir_path, profile_dir=profile_dir)
        for key, input_path in key_to_input.items():
            output_path = output_map.get(input_path)
            if not output_path:
                results[key] = {}
                continue
            result_wb = openpyxl.load_workbook(output_path, data_only=True, read_only=True)
            result_ws = result_wb['Pricing & Summary']
            results[key] = _read_summary_metrics(result_ws)
            result_wb.close()
    return results

def _get_lo_profile(job_dir, suffix=None):
    base_name = Path(job_dir).name
    if suffix:
        safe_suffix = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(suffix)).strip('_')
        if safe_suffix:
            base_name = f"{base_name}-{safe_suffix}"
    profile_dir = (Path(tempfile.gettempdir()) / f"lo-profile-{base_name}").resolve()
    lock_path = profile_dir / 'lock'
    if lock_path.exists():
        try:
            age = time.time() - lock_path.stat().st_mtime
        except Exception:
            age = None
        if age is None or age > 120:
            shutil.rmtree(profile_dir, ignore_errors=True)
    profile_dir.mkdir(parents=True, exist_ok=True)
    return profile_dir

def _cache_path_for_job(job_dir):
    return job_dir / 'dashboard_breakdown.json'

def _summary_cache_path(job_dir):
    return job_dir / 'dashboard_summary.json'

def _selection_cache_key(selected_dashboard):
    return '|'.join(sorted(selected_dashboard))

def _summary_job_key(job_dir, source_mtime, selection_key):
    return f"{job_dir.name}:{source_mtime}:{selection_key}"

def _read_summary_cache(job_dir, source_mtime, selection_key):
    cache_path = _summary_cache_path(job_dir)
    if not cache_path.exists():
        return None
    try:
        with open(cache_path, 'r') as f:
            cache = json.load(f)
        if cache.get('source_mtime') != source_mtime:
            return None
        entries = cache.get('entries', {})
        return entries.get(selection_key)
    except Exception:
        return None

def _write_summary_cache(job_dir, source_mtime, selection_key, metrics):
    cache_path = _summary_cache_path(job_dir)
    payload = {'source_mtime': source_mtime, 'updated_at': datetime.now(timezone.utc).isoformat(), 'entries': {}}
    if cache_path.exists():
        try:
            with open(cache_path, 'r') as f:
                existing = json.load(f)
            if existing.get('source_mtime') == source_mtime:
                payload = existing
        except Exception:
            payload = {'source_mtime': source_mtime, 'updated_at': datetime.now(timezone.utc).isoformat(), 'entries': {}}
    payload['source_mtime'] = source_mtime
    payload['updated_at'] = datetime.now(timezone.utc).isoformat()
    payload.setdefault('entries', {})
    payload['entries'][selection_key] = metrics
    with open(cache_path, 'w') as f:
        json.dump(payload, f)

def _read_breakdown_cache(job_dir, source_mtime):
    cache_path = _cache_path_for_job(job_dir)
    if not cache_path.exists():
        return None, False
    try:
        with open(cache_path, 'r') as f:
            cache = json.load(f)
        if cache.get('source_mtime') != source_mtime:
            return None, False
        return cache.get('per_carrier', []), bool(cache.get('complete', False))
    except Exception:
        return None, False

def _write_breakdown_cache(job_dir, source_mtime, per_carrier, complete=True):
    cache_path = _cache_path_for_job(job_dir)
    payload = {
        'source_mtime': source_mtime,
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'complete': complete,
        'per_carrier': per_carrier
    }
    with open(cache_path, 'w') as f:
        json.dump(payload, f)

def _build_breakdown_cache(job_dir, source_mtime, job_key, selected_dashboard=None, selection_key=None, available_carriers=None):
    try:
        carriers = available_carriers or list(DASHBOARD_CARRIERS)
        selections = {carrier: [carrier] for carrier in carriers}
        if selected_dashboard and selection_key:
            selections['__overall__'] = list(selected_dashboard)
        profile_dir = _get_lo_profile(job_dir, suffix='breakdown')
        try:
            metrics_map = _calculate_metrics_batch(job_dir, selections, profile_dir)
        except subprocess.CalledProcessError:
            metrics_map = _calculate_metrics_batch(job_dir, selections, profile_dir=None)
        except Exception:
            metrics_map = {}

        if selected_dashboard and selection_key:
            overall_metrics = metrics_map.get('__overall__', {})
            _write_summary_cache(job_dir, source_mtime, selection_key, overall_metrics)

        per_carrier = []
        for carrier in carriers:
            metrics = metrics_map.get(carrier, {})
            per_carrier.append({'carrier': carrier, 'metrics': metrics})
            _write_breakdown_cache(job_dir, source_mtime, per_carrier, complete=False)
        _write_breakdown_cache(job_dir, source_mtime, per_carrier, complete=True)
    finally:
        with dashboard_jobs_lock:
            dashboard_jobs.pop(job_key, None)

def _build_summary_cache(job_dir, source_mtime, selection_key, selected_dashboard, job_key):
    try:
        profile_dir = _get_lo_profile(job_dir)
        try:
            metrics = _calculate_metrics(job_dir, selected_dashboard, profile_dir)
        except subprocess.CalledProcessError:
            metrics = _calculate_metrics(job_dir, selected_dashboard, profile_dir=None)
        _write_summary_cache(job_dir, source_mtime, selection_key, metrics)
        _start_breakdown_cache(job_dir, source_mtime)
    finally:
        with summary_jobs_lock:
            summary_jobs.pop(job_key, None)

def _start_breakdown_cache(job_dir, source_mtime, selected_dashboard=None, selection_key=None, available_carriers=None):
    cached, complete = _read_breakdown_cache(job_dir, source_mtime)
    if cached is not None:
        return cached, not complete
    job_key = f"{job_dir.name}:{source_mtime}"
    with dashboard_jobs_lock:
        if job_key not in dashboard_jobs:
            thread = threading.Thread(
                target=_build_breakdown_cache,
                args=(job_dir, source_mtime, job_key, selected_dashboard, selection_key, available_carriers),
                daemon=True
            )
            dashboard_jobs[job_key] = thread
            thread.start()
    return None, True

def _start_summary_cache(job_dir, source_mtime, selected_dashboard):
    selection_key = _selection_cache_key(selected_dashboard)
    cached = _read_summary_cache(job_dir, source_mtime, selection_key)
    if cached is not None:
        return cached, False
    if selected_dashboard and len(selected_dashboard) == 1:
        breakdown_cached, _ = _read_breakdown_cache(job_dir, source_mtime)
        if breakdown_cached:
            carrier = selected_dashboard[0]
            for entry in breakdown_cached:
                if entry.get('carrier') == carrier:
                    return entry.get('metrics', {}), False
    job_key = _summary_job_key(job_dir, source_mtime, selection_key)
    with summary_jobs_lock:
        if job_key not in summary_jobs:
            thread = threading.Thread(
                target=_build_summary_cache,
                args=(job_dir, source_mtime, selection_key, selected_dashboard, job_key),
                daemon=True
            )
            summary_jobs[job_key] = thread
            thread.start()
    return None, True

def _find_pricing_section(ws, section_title):
    title_cell = None
    for row in ws.iter_rows():
        for cell in row:
            if cell.value and str(cell.value).strip() == section_title:
                title_cell = cell
                break
        if title_cell:
            break
    if not title_cell:
        return None, None, None

    header_row_idx = title_cell.row
    label_col = title_cell.column
    use_col = None

    for cell in ws[header_row_idx]:
        if cell.value and str(cell.value).strip() == 'Use in Pricing':
            use_col = cell.column
            break

    if use_col is None:
        for cell in ws[header_row_idx + 1]:
            if cell.value and str(cell.value).strip() == 'Use in Pricing':
                use_col = cell.column
                header_row_idx = header_row_idx + 1
                break

    if use_col is None:
        return None, None, None

    return header_row_idx, label_col, use_col

def _iter_section_rows(ws, start_row, label_col, stop_titles):
    row_idx = start_row
    while True:
        label_val = ws.cell(row_idx, label_col).value
        normalized = normalize_redo_label(label_val)
        if not normalized:
            break
        if normalized in stop_titles:
            break
        yield row_idx, label_val
        row_idx += 1

def _scan_section_rows(ws, section_title, stop_titles):
    header_row_idx, label_col, use_col = _find_pricing_section(ws, section_title)
    if header_row_idx is None:
        return None, None, None, []
    rows = []
    row_idx = header_row_idx + 1
    while True:
        label_val = ws.cell(row_idx, label_col).value
        normalized = normalize_redo_label(label_val)
        if not normalized or normalized in stop_titles:
            break
        rows.append((row_idx, label_val))
        row_idx += 1
    return header_row_idx, label_col, use_col, rows

def update_pricing_summary_redo_carriers(ws, selected_redo_carriers):
    """Update Use in Pricing for Redo Carriers section."""
    selected = set(selected_redo_carriers or [])
    canonical_map = {normalize_redo_label(c): c for c in REDO_CARRIERS}

    header_row_idx, label_col, use_col = _find_pricing_section(ws, 'Redo Carriers')
    if header_row_idx is None:
        return

    stop_titles = {'MERCHANT CARRIERS', 'MERCHANT CARRIER', 'MERCHANT SERVICE LEVELS'}
    for row_idx, label_val in _iter_section_rows(ws, header_row_idx + 1, label_col, stop_titles):
        normalized = normalize_redo_label(label_val)
        canonical = canonical_map.get(normalized)

        target_cell = ws.cell(row_idx, use_col)
        target_cell.value = 'Yes' if canonical in selected else 'No'

def update_pricing_summary_merchant_carriers(ws, excluded_carriers):
    """Update Use in Pricing for Merchant Carriers section."""
    excluded = {normalize_merchant_carrier(c) for c in (excluded_carriers or [])}

    header_row_idx, label_col, use_col = _find_pricing_section(ws, 'Merchant Carriers')
    if header_row_idx is None:
        return

    stop_titles = {'MERCHANT SERVICE LEVELS', 'REDO CARRIERS'}
    for row_idx, label_val in _iter_section_rows(ws, header_row_idx + 1, label_col, stop_titles):
        label_norm = normalize_redo_label(label_val)
        target_cell = ws.cell(row_idx, use_col)
        if 'FIRST MILE' in label_norm:
            target_cell.value = 'No'
            continue
        carrier = normalize_merchant_carrier(label_val)
        if not carrier:
            continue
        target_cell.value = 'No' if carrier in excluded else 'Yes'

def _unique_cleaned_services(normalized_df):
    if normalized_df is None or 'CLEANED_SHIPPING_SERVICE' not in normalized_df.columns:
        return []
    seen = set()
    ordered = []
    for value in normalized_df['CLEANED_SHIPPING_SERVICE']:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        text = str(value).strip()
        if not text:
            continue
        norm = normalize_service_name(text)
        if norm not in seen:
            seen.add(norm)
            ordered.append(text)
    return ordered

def update_pricing_summary_merchant_service_levels(ws, selected_services, normalized_df=None):
    """Update Use in Pricing for Merchant Service Levels section."""
    selected_normalized = {normalize_service_name(s) for s in (selected_services or [])}

    stop_titles = {'REDO CARRIERS', 'MERCHANT CARRIERS'}
    header_row_idx, label_col, use_col, rows = _scan_section_rows(
        ws, 'Merchant Service Levels', stop_titles
    )
    if header_row_idx is None:
        return
    services = _unique_cleaned_services(normalized_df)
    if not services:
        services = SERVICE_LEVELS

    row_idx = header_row_idx + 1
    for service in services:
        ws.cell(row_idx, label_col, service)
        normalized = normalize_service_name(service)
        ws.cell(row_idx, use_col, 'Yes' if normalized in selected_normalized else 'No')
        row_idx += 1

    while True:
        cell = ws.cell(row_idx, label_col)
        normalized = normalize_redo_label(cell.value)
        if not normalized or normalized in stop_titles:
            break
        cell.value = None
        ws.cell(row_idx, use_col, None)
        row_idx += 1

def detect_structure(csv_path):
    """Detect if invoice is zone-based or zip-based"""
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = [h.lower() for h in reader.fieldnames or []]
        # Check for zone column (case-insensitive, allow variants)
        zone_keywords = ['zone', 'shipment - zone', 'shipment-zone']
        has_zone = any(any(kw in h for h in headers) for kw in zone_keywords)
        return 'zone' if has_zone else 'zip'

def _detect_weight_unit_from_text(text):
    if not text:
        return None
    cleaned = re.sub(r'[^a-z0-9\s]', ' ', str(text).lower())
    patterns = {
        'oz': re.compile(r'\b(oz|ounce|ounces)\b'),
        'lb': re.compile(r'\b(lb|lbs|pound|pounds)\b'),
        'kg': re.compile(r'\b(kg|kilogram|kilograms)\b')
    }
    for unit, pattern in patterns.items():
        if pattern.search(cleaned):
            return unit
    return None

def detect_weight_unit_from_values(series, sample_size=50):
    if series is None:
        return None
    sample = series.dropna().astype(str).head(sample_size)
    counts = {'oz': 0, 'lb': 0, 'kg': 0}
    for value in sample:
        unit = _detect_weight_unit_from_text(value)
        if unit:
            counts[unit] += 1
    if not any(counts.values()):
        return None
    return max(counts, key=counts.get)

def detect_weight_unit_fallback(df):
    unit_columns = [
        col for col in df.columns
        if 'unit' in col.lower() or 'uom' in col.lower()
    ]
    for col in unit_columns:
        unit = detect_weight_unit_from_values(df[col])
        if unit:
            return unit
    return None

def _annual_orders_missing(mapping_config):
    if not mapping_config:
        return True
    raw_value = mapping_config.get('annual_orders')
    if raw_value is None or str(raw_value).strip() == '':
        return True
    try:
        return float(raw_value) <= 0
    except Exception:
        return True

def _usps_market_discount_values(mapping_config):
    pct = None
    dollar = None
    if mapping_config:
        pct = mapping_config.get('usps_market_pct_off')
        dollar = mapping_config.get('usps_market_dollar_off')
    try:
        pct = float(pct) if pct is not None else 0.05
    except Exception:
        pct = 0.05
    if pct > 1:
        pct = pct / 100
    try:
        dollar = float(dollar) if dollar is not None else 0.0
    except Exception:
        dollar = 0.0
    return pct, dollar

def _avg_label_cost_from_job(job_dir):
    normalized_csv = Path(job_dir) / 'normalized.csv'
    if not normalized_csv.exists():
        return None
    try:
        df = pd.read_csv(normalized_csv)
    except Exception:
        return None
    if df.empty:
        return None
    series = None
    if 'Label Cost' in df.columns:
        series = df['Label Cost']
    elif 'LABEL_COST' in df.columns:
        series = df['LABEL_COST']
    if series is None:
        return None
    numeric = pd.to_numeric(series, errors='coerce')
    if numeric.notna().any():
        return float(numeric.mean())
    return None

def _carrier_distribution(job_dir, mapping_config, available_carriers):
    normalized_csv = Path(job_dir) / 'normalized.csv'
    if not normalized_csv.exists():
        return {carrier: 0 for carrier in available_carriers}
    try:
        df = pd.read_csv(normalized_csv)
    except Exception:
        return {carrier: 0 for carrier in available_carriers}
    if df.empty:
        return {carrier: 0 for carrier in available_carriers}
    carrier_series = df['Shipping Carrier'] if 'Shipping Carrier' in df.columns else pd.Series([""] * len(df))
    service_series = df['Shipping Service'] if 'Shipping Service' in df.columns else pd.Series([""] * len(df))
    carrier_series = carrier_series.fillna("").astype(str)
    service_series = service_series.fillna("").astype(str)
    counts = {carrier: 0 for carrier in available_carriers}
    total = len(df)
    for carrier_val, service_val in zip(carrier_series, service_series):
        inferred = infer_redo_carrier(carrier_val, service_val)
        if inferred in counts:
            counts[inferred] += 1
    if total <= 0:
        return {carrier: 0 for carrier in available_carriers}
    return {carrier: counts.get(carrier, 0) / total for carrier in available_carriers}

def suggest_mapping(invoice_columns, standard_field):
    """Suggest best matching column for a standard field"""
    invoice_lower = [c.lower() for c in invoice_columns]
    field_lower = standard_field.lower()
    
    def _is_bad_label_cost(col_text):
        bad_tokens = ('insurance', 'labelcreatedate', 'create date', 'createdate', 'shipdate', 'date')
        return any(token in col_text for token in bad_tokens)

    # Exact match
    for i, col in enumerate(invoice_lower):
        if field_lower in col or col in field_lower:
            if standard_field == 'Label Cost' and _is_bad_label_cost(col):
                continue
            return invoice_columns[i]
    
    # Partial matches
    keywords = {
        'Order Number': ['order', 'number', 'order_number'],
        'Order Date': ['date', 'shipped', 'order_date'],
        'Zip': ['zip', 'postal', 'postal_code'],
        'Weight': ['weight', 'oz', 'ounces', 'lb', 'lbs', 'pound', 'pounds', 'kg', 'kilogram', 'kilograms'],
        'Shipping Carrier': ['carrier'],
        'Shipping Service': ['service', 'shipping_service', 'shippingservice'],
        'Package Height': ['height'],
        'Package Width': ['width'],
        'Package Length': ['length'],
        'Label Cost': ['cost', 'shipping_rate', 'rate', 'label', 'carrier fee', 'carrier_fee', 'fee']
    }
    
    if standard_field in keywords:
        if standard_field == 'Label Cost':
            candidates = []
            for i, col in enumerate(invoice_lower):
                if _is_bad_label_cost(col):
                    continue
                score = 0
                if 'carrier fee' in col or 'carrier_fee' in col:
                    score += 100
                if 'fee' in col:
                    score += 30
                if 'rate' in col or 'shipping_rate' in col:
                    score += 20
                if 'cost' in col or 'label' in col:
                    score += 10
                if score:
                    candidates.append((score, i))
            if candidates:
                candidates.sort(reverse=True)
                return invoice_columns[candidates[0][1]]
        for keyword in keywords[standard_field]:
            for i, col in enumerate(invoice_lower):
                if keyword in col:
                    if standard_field == 'Label Cost' and _is_bad_label_cost(col):
                        continue
                    return invoice_columns[i]
    
    return None

def clean_old_runs():
    """Remove runs older than 24 hours"""
    runs_dir = Path(app.config['UPLOAD_FOLDER'])
    if not runs_dir.exists():
        return
    
    cutoff = datetime.now() - timedelta(hours=24)
    for run_dir in runs_dir.iterdir():
        if run_dir.is_dir():
            try:
                mtime = datetime.fromtimestamp(run_dir.stat().st_mtime)
                if mtime < cutoff:
                    shutil.rmtree(run_dir)
            except Exception:
                pass

ADMIN_LOG_PATH = BASE_DIR / 'admin_log.xlsx'
DEAL_SIZING_HEADERS = [
    'Merchant Name',
    'SE/AE On Deal',
    'Annual Orders',
    '% USPS',
    '% FedEx',
    '% UPS',
    '% Amazon',
    '% UniUni',
    'UPS Average Label Cost',
    'FedEx Actual Spread',
    'Amazon Actual Spread',
    'UniUni Actual Spread',
    'Carrier Spread',
    'Adjusted Annual Orders',
    'Attach Rate',
    'SaaS Fee',
    'SaaS Tier',
    'Per Label Fee',
    '% Of Orders With Fee',
    'CommentSold',
    'Ebay',
    'Ebay Live Selling',
    'Printing Today',
    'Total Deal Size'
]
RATE_CARD_HEADERS = [
    'Timestamp',
    'Job ID',
    'Flow Type',
    'Merchant Name',
    'Merchant ID',
    'Existing Customer',
    'Origin ZIP',
    'Annual Orders',
    'Structure',
    'Zone Column',
    'Mapping JSON',
    'Merchant Pricing JSON',
    'Redo Carriers JSON',
    'USPS Market % Off',
    'USPS Market $ Off'
]

def _ensure_admin_log():
    if not ADMIN_LOG_PATH.exists():
        wb = openpyxl.Workbook()
        default_sheet = wb.active
        wb.remove(default_sheet)
        ws = wb.create_sheet('Deal sizing')
        ws.append(DEAL_SIZING_HEADERS)
        ws = wb.create_sheet('Rate card + deal sizing')
        ws.append(RATE_CARD_HEADERS)
        wb.save(ADMIN_LOG_PATH)
        return

    wb = openpyxl.load_workbook(ADMIN_LOG_PATH)
    if 'Deal sizing' not in wb.sheetnames:
        ws = wb.create_sheet('Deal sizing')
        ws.append(DEAL_SIZING_HEADERS)
    else:
        ws = wb['Deal sizing']
        for idx, header in enumerate(DEAL_SIZING_HEADERS, start=1):
            ws.cell(row=1, column=idx, value=header)
    if 'Rate card + deal sizing' not in wb.sheetnames:
        ws = wb.create_sheet('Rate card + deal sizing')
        ws.append(RATE_CARD_HEADERS)
    else:
        ws = wb['Rate card + deal sizing']
        for idx, header in enumerate(RATE_CARD_HEADERS, start=1):
            ws.cell(row=1, column=idx, value=header)
    wb.save(ADMIN_LOG_PATH)

def _upsert_admin_row(ws, job_id, row_values):
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        if str(row[1].value) == str(job_id):
            for idx, value in enumerate(row_values, start=1):
                row[idx - 1].value = value
            return
    ws.append(row_values)

def log_admin_entry(job_id, mapping_config, merchant_pricing, redo_config):
    _ensure_admin_log()
    flow_type = mapping_config.get('flow_type', 'rate_card_plus_deal_sizing') if mapping_config else ''
    if flow_type == 'deal_sizing':
        return
    sheet_name = 'Deal sizing' if flow_type == 'deal_sizing' else 'Rate card + deal sizing'
    pct_off, dollar_off = _usps_market_discount_values(mapping_config)
    row = [
        datetime.now(timezone.utc).isoformat(),
        job_id,
        flow_type,
        mapping_config.get('merchant_name', '') if mapping_config else '',
        mapping_config.get('merchant_id', '') if mapping_config else '',
        bool(mapping_config.get('existing_customer')) if mapping_config else False,
        mapping_config.get('origin_zip', '') if mapping_config else '',
        mapping_config.get('annual_orders', '') if mapping_config else '',
        mapping_config.get('structure', '') if mapping_config else '',
        mapping_config.get('zone_column', '') if mapping_config else '',
        json.dumps(mapping_config.get('mapping', {})) if mapping_config else '{}',
        json.dumps(merchant_pricing or {}),
        json.dumps(redo_config or {}),
        pct_off,
        dollar_off
    ]
    wb = openpyxl.load_workbook(ADMIN_LOG_PATH)
    ws = wb[sheet_name]
    _upsert_admin_row(ws, job_id, row)
    wb.save(ADMIN_LOG_PATH)

@app.route('/')
def index():
    clean_old_runs()
    return render_template('entry.html')

@app.route('/upload')
def upload_page():
    clean_old_runs()
    return render_template('screen1.html')

@app.route('/deal-sizing')
def deal_sizing_page():
    return render_template('deal_sizing.html')

@app.route('/admin')
def admin_page():
    _ensure_admin_log()
    wb = openpyxl.load_workbook(ADMIN_LOG_PATH, data_only=True)
    deal_ws = wb['Deal sizing'] if 'Deal sizing' in wb.sheetnames else None
    rate_ws = wb['Rate card + deal sizing'] if 'Rate card + deal sizing' in wb.sheetnames else None

    def _sheet_data(ws):
        if ws is None:
            return {'headers': [], 'rows': []}
        headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
        rows = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            rows.append(list(row))
        return {'headers': headers, 'rows': rows}

    deal_data = _sheet_data(deal_ws)
    rate_data = _sheet_data(rate_ws)

    def _format_currency(value):
        try:
            number = float(value)
        except Exception:
            return ''
        return f"${number:,.0f}"

    def _format_percent(value):
        try:
            number = float(value)
        except Exception:
            return ''
        if number <= 1:
            number *= 100
        return f"{number:.0f}%"

    def _format_number(value):
        try:
            number = float(value)
        except Exception:
            return ''
        return f"{number:,.0f}"

    def _format_bool(value):
        return 'Yes' if value else 'No'

    def _parse_bool(value):
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {'true', '1', 'yes', 'y'}

    def _parse_number(value):
        try:
            return float(value)
        except Exception:
            return 0.0

    def _saas_tier_name(annual_orders):
        try:
            orders = float(annual_orders)
        except Exception:
            orders = 0
        if orders <= 0:
            return ''
        if orders < 5000:
            return 'Starter Tier'
        if orders < 15000:
            return 'Growth Tier'
        if orders < 35000:
            return 'Pro Tier'
        if orders < 50000:
            return 'Scale Tier'
        return 'Enterprise Tier'

    def _summarize_list(values):
        items = [str(value).strip() for value in (values or []) if value]
        return ', '.join(items)

    def _effective_orders(annual_orders, comment_sold, ebay, live_selling):
        adjusted = annual_orders
        if comment_sold:
            adjusted *= 0.6
        if ebay:
            adjusted *= 0.6 if live_selling else 0.95
        return adjusted

    def _deal_spread_from_details(details, effective_orders):
        total = 0.0
        for detail in (details or []):
            spread = _parse_number(detail.get('spread'))
            pct = detail.get('orders_won_pct')
            pct = _parse_number(pct)
            if pct > 1:
                pct /= 100
            total += spread * pct * effective_orders
        return total

    def _reorder_with_total(data, download_label='Download Rate Card'):
        headers = list(data.get('headers') or [])
        rows = list(data.get('rows') or [])
        if not headers:
            return data
        total_label = 'Total Deal Size'
        total_index = None
        if 'Total Size' in headers:
            total_index = headers.index('Total Size')
        elif total_label in headers:
            total_index = headers.index(total_label)
        if total_index is not None:
            headers = headers[:total_index] + headers[total_index + 1:] + [total_label]
            updated = []
            for row in rows:
                total_value = row[total_index] if total_index < len(row) else ''
                new_row = []
                for header in headers:
                    if header == total_label:
                        new_row.append(total_value)
                        continue
                    try:
                        idx = data['headers'].index(header)
                    except ValueError:
                        new_row.append('')
                        continue
                    new_row.append(row[idx] if idx < len(row) else '')
                updated.append(new_row)
            data['headers'] = headers
            data['rows'] = updated
        else:
            if total_label not in headers:
                headers.append(total_label)
                rows = [list(row) + [''] for row in rows]
                data['headers'] = headers
                data['rows'] = rows
        if download_label in headers and headers[-1] != total_label:
            download_idx = headers.index(download_label)
            headers.pop(download_idx)
            headers.insert(len(headers) - 1, download_label)
            updated = []
            for row in data.get('rows', []):
                values = list(row)
                value = values.pop(download_idx) if download_idx < len(values) else ''
                values.insert(len(values) - 1, value)
                updated.append(values)
            data['headers'] = headers
            data['rows'] = updated
        return data

    def _build_admin_groups(headers, sheet_type):
        def group_label(header):
            header = str(header or '')
            if sheet_type == 'deal':
                if header in ('Merchant Name', 'SE/AE On Deal', 'Annual Orders'):
                    return 'Merchant Info'
                if header.startswith('% '):
                    return 'Carrier Mix'
                if header in (
                    'UPS Average Label Cost',
                    'FedEx Actual Spread',
                    'Amazon Actual Spread',
                    'UniUni Actual Spread'
                ):
                    return 'Carrier Inputs'
                if header in (
                    'Carrier Spread',
                    'Adjusted Annual Orders',
                    'Attach Rate',
                    'SaaS Fee',
                    'SaaS Tier',
                    'Per Label Fee',
                    '% Of Orders With Fee',
                    'CommentSold',
                    'Ebay',
                    'Ebay Live Selling',
                    'Printing Today'
                ):
                    return 'Deal Sizing Inputs'
                if header == 'Total Deal Size':
                    return 'Deal Sizes'
                return 'Other'
            if sheet_type == 'rate':
                if header in (
                    'Timestamp',
                    'Merchant Name',
                    'Existing Customer',
                    'Merchant ID',
                    'SE/AE On Deal',
                    'Origin ZIP',
                    'Annual Orders',
                    'Structure',
                    'Raw Invoice File',
                    'Amazon Eligible',
                    'UniUni Eligible'
                ):
                    return 'Merchant Info'
                if header.endswith('Mapped To'):
                    return 'Mappings'
                if header in ('Merchant Carriers', 'Merchant Service Levels'):
                    return 'Merchant Pricing'
                if header == 'Redo Carriers':
                    return 'Redo Carriers'
                if header in (
                    'Est. Merchant Annual Savings',
                    'Total Spread Available',
                    '% Orders We Could Win',
                    '% Orders Won W/ Spread'
                ):
                    return 'Dashboard Figures'
                if header.endswith('Breakdown'):
                    return 'Carrier Breakdown'
                if header in ('% Off USPS Market Rate', '$ Off USPS Market Rate'):
                    return 'Pricing Inputs'
                if header in (
                    'Est. Redo Deal Size (Popup)',
                    'Carrier Spread',
                    'Adjusted Annual Orders',
                    'Attach Rate',
                    'File Attached',
                    'SaaS Fee',
                    'Per Label Fee',
                    '% Of Orders With Fee',
                    'Annual Orders (Deal Sizing)',
                    'Average Label Cost',
                    'CommentSold',
                    'Ebay',
                    'Live Selling',
                    'Printing Today'
                ):
                    return 'Deal Sizing Inputs'
                if header in ('Download Rate Card', 'View File In Sharepoint'):
                    return 'Files'
                return 'Other'
            return 'Other'

        groups = []
        if not headers:
            return groups
        current = group_label(headers[0])
        start = 0
        for idx, header in enumerate(headers):
            label = group_label(header)
            if label != current:
                groups.append({'label': current, 'span': idx - start})
                current = label
                start = idx
        groups.append({'label': current, 'span': len(headers) - start})
        return groups

    def _format_breakdown(metrics):
        if not metrics:
            return ''
        savings = _format_currency(metrics.get('Est. Merchant Annual Savings'))
        deal_size = _format_currency(metrics.get('Est. Redo Deal Size'))
        spread = _format_currency(metrics.get('Spread Available'))
        could_win = _format_percent(metrics.get('% Orders We Could Win'))
        won_spread = _format_percent(metrics.get('% Orders Won W/ Spread'))
        return f"Savings: {savings} | Deal Size: {deal_size} | Total Spread: {spread} | % We Could Win: {could_win} | % Won W/ Spread: {won_spread}"

    def _available_carriers_for_job(job_dir, mapping_config, excluded):
        raw_csv = job_dir / 'raw_invoice.csv'
        if raw_csv.exists() and mapping_config:
            try:
                raw_df = pd.read_csv(raw_csv)
                available = available_merchant_carriers(raw_df, mapping_config)
                if available:
                    return [c for c in available if c not in excluded]
            except Exception:
                pass
        return [c for c in MERCHANT_CARRIERS if c not in excluded]

    def _deal_inputs_from_mapping(mapping_config):
        inputs = mapping_config.get('deal_sizing_inputs') if mapping_config else None
        return inputs if isinstance(inputs, dict) else {}

    def _build_deal_sizing_view(data):
        headers = data.get('headers') or []
        rows = data.get('rows') or []
        header_index = {str(header): idx for idx, header in enumerate(headers)}

        def value_for(row, *keys):
            for key in keys:
                idx = header_index.get(key)
                if idx is not None and idx < len(row):
                    return row[idx]
            return ''

        target_headers = [
            'Merchant Name',
            'SE/AE On Deal',
            'Annual Orders',
            '% USPS',
            '% FedEx',
            '% UPS',
            '% Amazon',
            '% UniUni',
            'UPS Average Label Cost',
            'FedEx Actual Spread',
            'Amazon Actual Spread',
            'UniUni Actual Spread',
            'Carrier Spread',
            'Adjusted Annual Orders',
            'Attach Rate',
            'SaaS Fee',
            'SaaS Tier',
            'Per Label Fee',
            '% Of Orders With Fee',
            'CommentSold',
            'Ebay',
            'Ebay Live Selling',
            'Printing Today',
            'Total Deal Size'
        ]

        updated_rows = []
        for row in rows:
            merchant_name = value_for(row, 'Merchant Name', 'Merchant')
            se_ae_on_deal = value_for(row, 'SE/AE On Deal')
            annual_orders = value_for(row, 'Annual Orders')
            pct_usps = value_for(row, '% USPS')
            pct_fedex = value_for(row, '% FedEx')
            pct_ups = value_for(row, '% UPS')
            pct_amazon = value_for(row, '% Amazon')
            pct_uniuni = value_for(row, '% UniUni')
            ups_avg_label = value_for(row, 'UPS Average Label Cost')
            fedex_spread = value_for(row, 'FedEx Actual Spread')
            amazon_spread = value_for(row, 'Amazon Actual Spread')
            uniuni_spread = value_for(row, 'UniUni Actual Spread')
            attach_rate = value_for(row, 'Attach Rate')
            saas_fee = value_for(row, 'SaaS Fee', 'Monthly SaaS')
            per_label_fee = value_for(row, 'Per Label Fee')
            fee_order_pct = value_for(row, '% Of Orders With Fee')
            comment_sold = value_for(row, 'CommentSold', 'CommentSold?')
            ebay = value_for(row, 'Ebay')
            live_selling = value_for(row, 'Ebay Live Selling', 'Live Selling')
            printing = value_for(row, 'Printing Today', 'Printing?')
            total_deal_size = value_for(row, 'Total Deal Size', 'Total Size')

            usps_size = _parse_number(value_for(row, 'USPS Size'))
            fedex_size = _parse_number(value_for(row, 'FedEx Size'))
            ups_size = _parse_number(value_for(row, 'UPS Size'))
            amazon_size = _parse_number(value_for(row, 'Amazon Size'))
            uniuni_size = _parse_number(value_for(row, 'UniUni Size'))
            carrier_spread = usps_size + fedex_size + ups_size + amazon_size + uniuni_size
            carrier_spread_value = carrier_spread if carrier_spread else value_for(row, 'Carrier Spread')

            annual_orders_value = _parse_number(annual_orders)
            comment_sold_flag = _parse_bool(comment_sold)
            ebay_flag = _parse_bool(ebay)
            live_flag = _parse_bool(live_selling)
            adjusted_orders = _effective_orders(annual_orders_value, comment_sold_flag, ebay_flag, live_flag)
            adjusted_orders_value = adjusted_orders if adjusted_orders else value_for(row, 'Adjusted Annual Orders')

            updated_rows.append([
                merchant_name,
                se_ae_on_deal,
                annual_orders,
                pct_usps,
                pct_fedex,
                pct_ups,
                pct_amazon,
                pct_uniuni,
                ups_avg_label,
                fedex_spread,
                amazon_spread,
                uniuni_spread,
                carrier_spread_value,
                adjusted_orders_value,
                attach_rate,
                saas_fee,
                _saas_tier_name(annual_orders_value),
                per_label_fee,
                fee_order_pct,
                _format_bool(comment_sold_flag) if comment_sold != '' else '',
                _format_bool(ebay_flag) if ebay != '' else '',
                _format_bool(live_flag) if live_selling != '' else '',
                _format_bool(_parse_bool(printing)) if printing != '' else '',
                total_deal_size
            ])

        return {'headers': target_headers, 'rows': updated_rows}
    if rate_data.get('headers'):
        headers = rate_data['headers']
        header_index = {str(header): idx for idx, header in enumerate(headers)}
        job_idx = header_index.get('Job ID')
        ts_idx = header_index.get('Timestamp')
        merchant_name_idx = header_index.get('Merchant Name')
        merchant_id_idx = header_index.get('Merchant ID')
        existing_idx = header_index.get('Existing Customer')
        origin_idx = header_index.get('Origin ZIP')
        annual_idx = header_index.get('Annual Orders')
        structure_idx = header_index.get('Structure')
        mapping_idx = header_index.get('Mapping JSON')
        pricing_idx = header_index.get('Merchant Pricing JSON')
        redo_idx = header_index.get('Redo Carriers JSON')
        pct_off_idx = header_index.get('USPS Market % Off')
        dollar_off_idx = header_index.get('USPS Market $ Off')

        standard_fields = STANDARD_FIELDS['required'] + STANDARD_FIELDS['optional']
        mapping_headers = [f'{field} Mapped To' for field in standard_fields]
        breakdown_headers = [f'{carrier} Breakdown' for carrier in DASHBOARD_CARRIERS]

        rate_headers = [
            'Timestamp',
            'Merchant Name',
            'Existing Customer',
            'Merchant ID',
            'SE/AE On Deal',
            'Origin ZIP',
            'Annual Orders',
            'Structure',
            'Raw Invoice File',
            'Amazon Eligible',
            'UniUni Eligible'
        ] + mapping_headers + [
            'Merchant Carriers',
            'Merchant Service Levels',
            'Redo Carriers',
            'Est. Merchant Annual Savings',
            'Total Spread Available',
            '% Orders We Could Win',
            '% Orders Won W/ Spread'
        ] + breakdown_headers + [
            '% Off USPS Market Rate',
            '$ Off USPS Market Rate',
            'Est. Redo Deal Size (Popup)',
            'Carrier Spread',
            'Adjusted Annual Orders',
            'Attach Rate',
            'File Attached',
            'SaaS Fee',
            'Per Label Fee',
            '% Of Orders With Fee',
            'Annual Orders (Deal Sizing)',
            'Average Label Cost',
            'CommentSold',
            'Ebay',
            'Live Selling',
            'Printing Today',
            'Download Rate Card',
            'View File In Sharepoint'
        ]

        rate_rows = []
        for row in rate_data.get('rows', []):
            job_id = row[job_idx] if job_idx is not None and job_idx < len(row) else ''
            timestamp = row[ts_idx] if ts_idx is not None and ts_idx < len(row) else ''
            job_dir = Path(app.config['UPLOAD_FOLDER']) / str(job_id)
            mapping_config = {}
            merchant_pricing = {}
            redo_config = {}
            if job_id and job_dir.exists():
                mapping_file = job_dir / 'mapping.json'
                if mapping_file.exists():
                    try:
                        with open(mapping_file, 'r') as f:
                            mapping_config = json.load(f)
                    except Exception:
                        mapping_config = {}
                pricing_file = job_dir / 'merchant_pricing.json'
                if pricing_file.exists():
                    try:
                        with open(pricing_file, 'r') as f:
                            merchant_pricing = json.load(f)
                    except Exception:
                        merchant_pricing = {}
                redo_file = job_dir / 'redo_carriers.json'
                if redo_file.exists():
                    try:
                        with open(redo_file, 'r') as f:
                            redo_config = json.load(f)
                    except Exception:
                        redo_config = {}

            mapping_json = row[mapping_idx] if mapping_idx is not None and mapping_idx < len(row) else None
            pricing_json = row[pricing_idx] if pricing_idx is not None and pricing_idx < len(row) else None
            redo_json = row[redo_idx] if redo_idx is not None and redo_idx < len(row) else None
            if not mapping_config and mapping_json:
                try:
                    mapping_config = json.loads(mapping_json)
                except Exception:
                    mapping_config = {}
            if not merchant_pricing and pricing_json:
                try:
                    merchant_pricing = json.loads(pricing_json)
                except Exception:
                    merchant_pricing = {}
            if not redo_config and redo_json:
                try:
                    redo_config = json.loads(redo_json)
                except Exception:
                    redo_config = {}

            merchant_name = mapping_config.get('merchant_name') or (
                row[merchant_name_idx] if merchant_name_idx is not None and merchant_name_idx < len(row) else ''
            )
            merchant_id = mapping_config.get('merchant_id') or (
                row[merchant_id_idx] if merchant_id_idx is not None and merchant_id_idx < len(row) else ''
            )
            existing_customer = mapping_config.get('existing_customer')
            if existing_customer is None and existing_idx is not None and existing_idx < len(row):
                existing_customer = row[existing_idx]
            origin_zip = mapping_config.get('origin_zip') or (
                row[origin_idx] if origin_idx is not None and origin_idx < len(row) else ''
            )
            annual_orders = mapping_config.get('annual_orders') or (
                row[annual_idx] if annual_idx is not None and annual_idx < len(row) else ''
            )
            structure = mapping_config.get('structure') or (
                row[structure_idx] if structure_idx is not None and structure_idx < len(row) else ''
            )
            se_ae_on_deal = mapping_config.get('se_ae_on_deal', '')
            raw_invoice_file = mapping_config.get('raw_invoice_file', '')
            raw_invoice_cell = {
                'href': f'/download/{job_id}/raw-invoice',
                'label': raw_invoice_file or 'Download'
            } if job_id else ''

            eligibility = None
            try:
                eligibility = compute_eligibility(origin_zip, annual_orders, mapping_config=mapping_config)
            except Exception:
                eligibility = None
            amazon_eligible = mapping_config.get('amazon_eligible')
            uniuni_eligible = mapping_config.get('uniuni_eligible')
            if amazon_eligible is None and eligibility:
                amazon_eligible = eligibility.get('amazon_eligible_final')
            if uniuni_eligible is None and eligibility:
                uniuni_eligible = eligibility.get('uniuni_eligible_final')

            mapped_values = mapping_config.get('mapping', {}) if mapping_config else {}
            mapped_columns = [mapped_values.get(field, '') for field in standard_fields]

            excluded_carriers = merchant_pricing.get('excluded_carriers', []) if merchant_pricing else []
            merchant_carriers = _summarize_list(_available_carriers_for_job(job_dir, mapping_config, excluded_carriers)) if job_id else ''
            merchant_services = _summarize_list(merchant_pricing.get('included_services', []) if merchant_pricing else [])
            redo_carriers = _summarize_list(redo_config.get('selected_carriers', []) if redo_config else [])

            pct_off = row[pct_off_idx] if pct_off_idx is not None and pct_off_idx < len(row) else ''
            dollar_off = row[dollar_off_idx] if dollar_off_idx is not None and dollar_off_idx < len(row) else ''

            selected_carriers = redo_config.get('selected_carriers', []) if redo_config else []
            selected_dashboard = [c for c in DASHBOARD_CARRIERS if c in selected_carriers] or list(DASHBOARD_CARRIERS)

            summary_metrics = {}
            breakdown_metrics = {}
            if job_id and job_dir.exists():
                rate_cards = list(job_dir.glob('* - Rate Card.xlsx'))
                if rate_cards:
                    try:
                        summary_metrics = _calculate_metrics(job_dir, selected_dashboard)
                    except Exception:
                        summary_metrics = {}
                    try:
                        selections = {carrier: [carrier] for carrier in DASHBOARD_CARRIERS}
                        breakdown_metrics = _calculate_metrics_batch(job_dir, selections)
                    except Exception:
                        breakdown_metrics = {}

            breakdown_cells = []
            for carrier in DASHBOARD_CARRIERS:
                breakdown_cells.append(_format_breakdown(breakdown_metrics.get(carrier, {})))

            deal_inputs = _deal_inputs_from_mapping(mapping_config)
            deal_annual_orders = _parse_number(deal_inputs.get('annual_orders') or 0)
            deal_avg_label_cost = _parse_number(deal_inputs.get('avg_label_cost') or 0)
            deal_per_label_fee = _parse_number(deal_inputs.get('per_label_fee') or 0)
            deal_fee_order_pct = _parse_number(deal_inputs.get('fee_order_pct') or 0)
            if deal_fee_order_pct > 1:
                deal_fee_order_pct /= 100
            deal_attach_rate = _parse_number(deal_inputs.get('attach_rate') or 0)
            deal_saas_fee = _parse_number(deal_inputs.get('saas_fee') or 0)
            deal_comment_sold = _parse_bool(deal_inputs.get('comment_sold'))
            deal_ebay = _parse_bool(deal_inputs.get('ebay'))
            deal_live_selling = _parse_bool(deal_inputs.get('live_selling'))
            deal_printing = _parse_bool(deal_inputs.get('printing'))
            deal_attach_upload = deal_inputs.get('attach_upload_name')

            adjusted_orders = _effective_orders(deal_annual_orders, deal_comment_sold, deal_ebay, deal_live_selling)
            effective_orders = adjusted_orders * (deal_attach_rate / 100) if deal_attach_rate else adjusted_orders
            carrier_spread_total = 0.0
            if job_id and job_dir.exists() and selected_dashboard:
                try:
                    details = _calculate_carrier_details_fast(job_dir, selected_dashboard, mapping_config)
                    carrier_spread_total = _deal_spread_from_details(list(details.values()), effective_orders)
                except Exception:
                    carrier_spread_total = 0.0
            label_fee_total = deal_per_label_fee * deal_fee_order_pct * deal_annual_orders
            popup_deal_size = carrier_spread_total + deal_saas_fee + label_fee_total

            file_attached = 'Yes' if (deal_attach_rate > 85 and deal_attach_upload) else ('No' if deal_attach_rate > 85 else '')

            download_cell = {
                'href': f'/download/{job_id}/rate-card',
                'label': 'Download'
            } if job_id else ''
            sharepoint_cell = {
                'href': 'https://redotechinc.sharepoint.com/sites/Redo/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FRedo%2FShared%20Documents%2FAll%20Rate%20Cards%2FUpdating&viewid=90e9a3fe%2Df7a4%2D42c6%2Da353%2D200c53d2e4f3&view=7',
                'label': 'View'
            }

            rate_rows.append([
                timestamp,
                merchant_name,
                _format_bool(_parse_bool(existing_customer)),
                merchant_id,
                se_ae_on_deal,
                origin_zip,
                annual_orders,
                structure,
                raw_invoice_cell,
                _format_bool(_parse_bool(amazon_eligible)) if amazon_eligible is not None else '',
                _format_bool(_parse_bool(uniuni_eligible)) if uniuni_eligible is not None else '',
                *mapped_columns,
                merchant_carriers,
                merchant_services,
                redo_carriers,
                _format_currency(summary_metrics.get('Est. Merchant Annual Savings')),
                _format_currency(summary_metrics.get('Spread Available')),
                _format_percent(summary_metrics.get('% Orders We Could Win')),
                _format_percent(summary_metrics.get('% Orders Won W/ Spread')),
                *breakdown_cells,
                pct_off,
                dollar_off,
                _format_currency(popup_deal_size),
                _format_currency(carrier_spread_total),
                _format_number(adjusted_orders),
                _format_percent(deal_attach_rate),
                file_attached,
                _format_currency(deal_saas_fee),
                _format_currency(deal_per_label_fee),
                _format_percent(deal_fee_order_pct),
                _format_number(deal_annual_orders),
                _format_currency(deal_avg_label_cost),
                _format_bool(deal_comment_sold),
                _format_bool(deal_ebay),
                _format_bool(deal_live_selling),
                _format_bool(deal_printing),
                download_cell,
                sharepoint_cell
            ])

        rate_data = {'headers': rate_headers, 'rows': rate_rows}
        rate_data['groups'] = _build_admin_groups(rate_data.get('headers') or [], 'rate')

    deal_data = _build_deal_sizing_view(deal_data)
    deal_data['groups'] = _build_admin_groups(deal_data.get('headers') or [], 'deal')
    wb.close()
    return render_template('admin.html', deal_data=deal_data, rate_data=rate_data)

@app.route('/admin/download')
def admin_download():
    _ensure_admin_log()
    return send_file(ADMIN_LOG_PATH, as_attachment=True)

@app.route('/api/admin/clear', methods=['POST'])
def admin_clear():
    payload = request.get_json(silent=True) or {}
    sheet_key = (payload.get('sheet') or '').strip().lower()
    sheet_name = 'Deal sizing' if sheet_key == 'deal' else 'Rate card + deal sizing'
    _ensure_admin_log()
    wb = openpyxl.load_workbook(ADMIN_LOG_PATH)
    if sheet_name not in wb.sheetnames:
        wb.close()
        return jsonify({'error': 'Sheet not found'}), 404
    ws = wb[sheet_name]
    if ws.max_row and ws.max_row > 1:
        ws.delete_rows(2, ws.max_row)
    wb.save(ADMIN_LOG_PATH)
    wb.close()
    return jsonify({'success': True})

@app.route('/api/deal-sizing-inputs/<job_id>', methods=['POST'])
def save_deal_sizing_inputs(job_id):
    payload = request.get_json(silent=True) or {}
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    if not job_dir.exists():
        return jsonify({'error': 'Job not found'}), 404
    mapping_file = job_dir / 'mapping.json'
    if not mapping_file.exists():
        return jsonify({'error': 'Mapping not found'}), 404
    try:
        with open(mapping_file, 'r') as f:
            mapping_config = json.load(f)
    except Exception:
        mapping_config = {}
    mapping_config['deal_sizing_inputs'] = payload
    with open(mapping_file, 'w') as f:
        json.dump(mapping_config, f)
    return jsonify({'success': True})

@app.route('/api/deal-sizing-standalone', methods=['POST'])
def deal_sizing_standalone():
    try:
        payload = request.get_json(silent=True) or {}
        merchant = (payload.get('merchant') or '').strip()
        annual_orders = payload.get('annual_orders')
        if not merchant:
            return jsonify({'error': 'Merchant is required'}), 400
        try:
            annual_orders_value = float(annual_orders) if annual_orders not in (None, '') else 0
        except Exception:
            annual_orders_value = 0
        if annual_orders_value <= 0:
            return jsonify({'error': 'Annual orders is required'}), 400

        _ensure_admin_log()
        wb = openpyxl.load_workbook(ADMIN_LOG_PATH)
        ws = wb['Deal sizing']
        def _num(value):
            try:
                return float(value)
            except Exception:
                return 0.0

        pct_usps = payload.get('pct_usps', '')
        pct_fedex = payload.get('pct_fedex', '')
        pct_ups = payload.get('pct_ups', '')
        pct_amazon = payload.get('pct_amazon', '')
        pct_uniuni = payload.get('pct_uniuni', '')
        ups_avg_label = payload.get('ups_avg_label_cost', '')
        fedex_spread = payload.get('fedex_actual_spread', '')
        amazon_spread = payload.get('amazon_actual_spread', '')
        uniuni_spread = payload.get('uniuni_actual_spread', '')
        attach_rate = payload.get('attach_rate', '')
        per_label_fee = payload.get('per_label_fee', '')
        fee_order_pct = payload.get('fee_order_pct', '')
        comment_sold = payload.get('commentsold', False)
        ebay = payload.get('ebay', False)
        live_selling = payload.get('live_selling', False)
        printing = payload.get('printing', False)
        saas_fee = payload.get('monthly_saas', '')
        usps_size = _num(payload.get('usps_size', 0))
        fedex_size = _num(payload.get('fedex_size', 0))
        ups_size = _num(payload.get('ups_size', 0))
        amazon_size = _num(payload.get('amazon_size', 0))
        uniuni_size = _num(payload.get('uniuni_size', 0))
        carrier_spread = usps_size + fedex_size + ups_size + amazon_size + uniuni_size
        total_size = payload.get('total_size', '')
        adjusted_orders = _effective_orders(annual_orders_value, _parse_bool(comment_sold), _parse_bool(ebay), _parse_bool(live_selling))
        saas_tier = _saas_tier_name(annual_orders_value)

        row = [
            merchant,
            payload.get('se_ae_on_deal', ''),
            annual_orders_value,
            pct_usps,
            pct_fedex,
            pct_ups,
            pct_amazon,
            pct_uniuni,
            ups_avg_label,
            fedex_spread,
            amazon_spread,
            uniuni_spread,
            carrier_spread,
            adjusted_orders,
            attach_rate,
            saas_fee,
            saas_tier,
            per_label_fee,
            fee_order_pct,
            comment_sold,
            ebay,
            live_selling,
            printing,
            total_size
        ]
        ws.append(row)
        wb.save(ADMIN_LOG_PATH)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/mapping')
def mapping_page():
    """Render mapping page"""
    job_id = request.args.get('job_id')
    if not job_id:
        return render_template('screen1.html'), 400
    
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    if not job_dir.exists():
        return render_template('screen1.html'), 404
    
    # Load CSV to get columns
    raw_csv_path = job_dir / 'raw_invoice.csv'
    if not raw_csv_path.exists():
        return render_template('screen1.html'), 404
    
    structure = detect_structure(raw_csv_path)
    df = pd.read_csv(raw_csv_path, nrows=1)
    df_sample = pd.read_csv(raw_csv_path, nrows=200)
    columns = list(df.columns)
    
    # Load existing mapping if available
    mapping_file = job_dir / 'mapping.json'
    suggested_mapping = {}
    if mapping_file.exists():
        with open(mapping_file, 'r') as f:
            config = json.load(f)
            suggested_mapping = config.get('mapping', {})
    
    # Suggest mappings
    suggestions = {}
    all_fields = STANDARD_FIELDS['required'] + STANDARD_FIELDS['optional']
    required_fields = required_fields_for_structure(structure)
    optional_fields = [f for f in all_fields if f not in required_fields]
    for field in all_fields:
        suggested = suggest_mapping(columns, field)
        if suggested:
            suggestions[field] = suggested
    
    weight_unit_by_column = {}
    for col in columns:
        unit = _detect_weight_unit_from_text(col)
        if unit:
            weight_unit_by_column[col] = unit
    weight_unit_fallback = detect_weight_unit_fallback(df_sample)

    return render_template('screen2.html', 
                         job_id=job_id,
                         columns=columns,
                         suggestions=suggestions,
                         standard_fields={'required': required_fields, 'optional': optional_fields},
                         weight_unit_by_column=weight_unit_by_column,
                         weight_unit_fallback=weight_unit_fallback)

@app.route('/service-levels')
def service_levels_page():
    """Render service levels selection page"""
    job_id = request.args.get('job_id')
    if not job_id:
        return render_template('screen1.html'), 400
    
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    if not job_dir.exists():
        return render_template('screen1.html'), 404
    
    mapping_file = job_dir / 'mapping.json'
    if not mapping_file.exists():
        return render_template('screen1.html'), 404

    with open(mapping_file, 'r') as f:
        mapping_config = json.load(f)

    raw_df = pd.read_csv(job_dir / 'raw_invoice.csv')
    available_services = available_merchant_services(raw_df, mapping_config)

    selected_services = []
    service_file = job_dir / 'service_levels.json'
    if service_file.exists():
        with open(service_file, 'r') as f:
            config = json.load(f)
            selected_services = config.get('selected_services', [])

    return render_template('service_levels.html',
                          job_id=job_id,
                          service_levels=available_services,
                          selected_services=selected_services)

@app.route('/merchant-pricing')
def merchant_pricing_page():
    """Render merchant pricing selection page"""
    job_id = request.args.get('job_id')
    if not job_id:
        return "Missing job_id", 400
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    if not job_dir.exists():
        return "Job not found", 404
    return render_template('merchant_pricing.html', job_id=job_id)

@app.route('/redo-carriers')
def redo_carriers_page():
    """Render redo carrier selection page"""
    job_id = request.args.get('job_id')
    if not job_id:
        return render_template('screen1.html'), 400

    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    if not job_dir.exists():
        return render_template('screen1.html'), 404

    return render_template('redo_carriers.html', job_id=job_id)

@app.route('/loading')
def loading_page():
    """Render loading page"""
    job_id = request.args.get('job_id') or request.args.get('token')
    if not job_id:
        return render_template('screen1.html'), 400
    
    return render_template('loading.html', token=job_id)

@app.route('/ready')
def ready_page():
    """Render rate card ready page"""
    job_id = request.args.get('job_id')
    if not job_id:
        return render_template('screen1.html'), 400
    
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    if not job_dir.exists():
        return render_template('screen1.html'), 404
    
    # Load merchant name
    merchant_name = 'Merchant'
    mapping_file = job_dir / 'mapping.json'
    if mapping_file.exists():
        with open(mapping_file, 'r') as f:
            config = json.load(f)
            merchant_name = config.get('merchant_name', 'Merchant')
    
    return render_template('screen3.html', job_id=job_id, merchant_name=merchant_name)

@app.route('/dashboard')
def dashboard_page():
    """Render summary dashboard page"""
    job_id = request.args.get('job_id')
    if not job_id:
        return render_template('screen1.html'), 400
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    if not job_dir.exists():
        return render_template('screen1.html'), 404
    merchant_name = 'Merchant'
    mapping_file = job_dir / 'mapping.json'
    if mapping_file.exists():
        with open(mapping_file, 'r') as f:
            config = json.load(f)
            merchant_name = config.get('merchant_name', 'Merchant')
    return render_template('dashboard.html', job_id=job_id, merchant_name=merchant_name)

@app.route('/api/upload', methods=['POST'])
def upload():
    """Handle file upload and detect structure"""
    try:
        # Accept multiple field names
        file = request.files.get('invoice') or request.files.get('invoice_file') or request.files.get('invoice_csv')
        if not file or file.filename == '':
            return jsonify({'error': 'No file uploaded'}), 400
        
        # Create job directory
        job_id = str(uuid.uuid4())
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        
        # Save uploaded file
        filename = secure_filename(file.filename)
        ext = Path(filename).suffix.lower()
        raw_csv_path = job_dir / 'raw_invoice.csv'
        if ext == '.xlsx':
            raw_xlsx_path = job_dir / 'raw_invoice.xlsx'
            file.save(raw_xlsx_path)
            try:
                sheets = pd.read_excel(raw_xlsx_path, sheet_name=None, dtype=str)
            except Exception as e:
                return jsonify({'error': f'Failed to read XLSX: {str(e)}'}), 400
            if not sheets:
                return jsonify({'error': 'XLSX file has no readable sheets'}), 400

            def score_sheet(df):
                columns = [str(c).strip().lower() for c in df.columns if c is not None]
                if not columns:
                    return 0
                keywords = ('service', 'carrier', 'shipping', 'order', 'zip', 'postal', 'weight', 'zone')
                keyword_hits = sum(1 for c in columns if any(k in c for k in keywords))
                non_empty_cols = sum(1 for c in columns if c)
                return keyword_hits * 10 + non_empty_cols

            df_upload = max(sheets.values(), key=score_sheet)
            df_upload.columns = [
                str(c).strip() if c is not None else ''
                for c in df_upload.columns
            ]
            def _ensure_shipping_service_column(frame):
                normalized = {
                    re.sub(r'\W+', '', str(c).strip().lower()): c for c in frame.columns
                }
                if 'shippingservice' in normalized:
                    return frame
                if frame.shape[1] > 28:
                    cols = list(frame.columns)
                    cols[28] = 'ShippingService'
                    frame.columns = cols
                return frame

            df_upload = _ensure_shipping_service_column(df_upload)
            normalized_cols = {
                re.sub(r'\W+', '', str(c).strip().lower()): c for c in df_upload.columns
            }
            if 'shippingservice' not in normalized_cols and 'shipping_service' not in normalized_cols:
                raw_sheet_name = None
                for name, sheet_df in sheets.items():
                    if sheet_df is df_upload:
                        raw_sheet_name = name
                        break
                if raw_sheet_name is None:
                    raw_sheet_name = list(sheets.keys())[0]
                df_raw = pd.read_excel(raw_xlsx_path, sheet_name=raw_sheet_name, header=None, dtype=str)
                header_row = None
                for idx in range(min(20, len(df_raw))):
                    row_values = df_raw.iloc[idx].fillna('').astype(str)
                    normalized_row = [
                        re.sub(r'\W+', '', val.strip().lower()) for val in row_values
                    ]
                    if any('shippingservice' in val or ('shipping' in val and 'service' in val) for val in normalized_row):
                        header_row = idx
                        break
                if header_row is not None:
                    header_values = df_raw.iloc[header_row].fillna('').astype(str).tolist()
                    df_upload = df_raw.iloc[header_row + 1:].copy()
                    df_upload.columns = [str(c).strip() for c in header_values]
                    df_upload = df_upload.loc[:, df_upload.columns != '']
                    df_upload = _ensure_shipping_service_column(df_upload)
            df_upload.to_csv(raw_csv_path, index=False)
        else:
            file.save(raw_csv_path)
        
        # Detect structure
        structure = detect_structure(raw_csv_path)
        
        # Read CSV to get columns and suggest merchant name
        df = pd.read_csv(raw_csv_path, nrows=5)
        columns = list(df.columns)
        
        # Merchant name is manual; do not auto-suggest.
        merchant_name_suggestion = None
        
        return jsonify({
            'job_id': job_id,
            'detected_structure': structure,
            'columns': columns,
            'merchant_name_suggestion': merchant_name_suggestion or '',
            'requires_origin_zip': structure == 'zip'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/mapping', methods=['POST'])
def mapping():
    """Save mapping configuration"""
    try:
        data = request.json
        job_id = data.get('job_id')
        merchant_name = data.get('merchant_name', '')
        merchant_id = data.get('merchant_id', '')
        existing_customer = data.get('existing_customer', False)
        se_ae_on_deal = data.get('se_ae_on_deal', '')
        origin_zip = data.get('origin_zip', '')
        annual_orders = data.get('annual_orders', '')
        raw_invoice_file = data.get('raw_invoice_file', '')
        amazon_eligible = data.get('amazon_eligible')
        uniuni_eligible = data.get('uniuni_eligible')
        mapping_config = data.get('mapping', {})
        
        if not job_id:
            return jsonify({'error': 'job_id required'}), 400
        
        if existing_customer and not merchant_id:
            return jsonify({'error': 'Merchant ID required for existing customers'}), 400
        
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404

        weight_column = mapping_config.get('Weight')
        weight_unit = (mapping_config.get('Weight Unit') or '').lower()
        if weight_column and not weight_unit:
            return jsonify({'error': 'Weight unit required when Weight is mapped'}), 400
        if weight_unit and weight_unit not in {'oz', 'lb', 'kg'}:
            return jsonify({'error': 'Invalid weight unit'}), 400
        
        # Validate required mappings
        structure = data.get('structure', 'zone')
        required_fields = required_fields_for_structure(structure)
        missing = [f for f in required_fields if f not in mapping_config or not mapping_config[f]]
        
        if missing:
            return jsonify({
                'error': 'Missing required field mappings',
                'missing': missing
            }), 400
        
        # Normalize and save CSV
        raw_csv_path = job_dir / 'raw_invoice.csv'
        df = pd.read_csv(raw_csv_path)
        
        # Find zone column name for later use
        zone_col_name = None
        structure = data.get('structure', 'zone')
        if structure == 'zone':
            for col in df.columns:
                if 'zone' in col.lower():
                    zone_col_name = col
                    break
        
        # Save mapping config
        config = {
            'merchant_name': merchant_name,
            'merchant_id': merchant_id,
            'existing_customer': existing_customer,
            'se_ae_on_deal': se_ae_on_deal,
            'origin_zip': origin_zip,
            'annual_orders': annual_orders,
            'raw_invoice_file': raw_invoice_file,
            'amazon_eligible': amazon_eligible,
            'uniuni_eligible': uniuni_eligible,
            'flow_type': data.get('flow_type', 'rate_card_plus_deal_sizing'),
            'mapping': mapping_config,
            'structure': structure,
            'zone_column': zone_col_name
        }
        
        # Save config
        with open(job_dir / 'mapping.json', 'w') as f:
            json.dump(config, f)
        
        # Apply mapping
        normalized_data = {}
        for std_field, invoice_col in mapping_config.items():
            if invoice_col and invoice_col in df.columns:
                normalized_data[std_field] = df[invoice_col]
        
        # Zone will be included if mapped by user
        # If zone-based and zone column exists but wasn't mapped, we'll handle it in generation
        
        # Create normalized DataFrame
        normalized_df = pd.DataFrame(normalized_data)

        # Derived/computed fields
        country_series = None
        for col in df.columns:
            if 'country' in str(col).lower():
                country_series = df[col]
                break

        if country_series is not None:
            country_str = country_series.fillna("").astype(str).str.strip()
            country_upper = country_str.str.upper()
            is_two_letter = country_upper.str.len().eq(2) & country_upper.str.isalpha()

            mapped_codes = country_upper.map(COUNTRY_NAME_TO_CODE).fillna("")
            normalized_df['TWO_LETTER_COUNTRY_CODE'] = np.where(
                is_two_letter, country_upper, mapped_codes
            )
            normalized_df['FULL_COUNTRY_NAME'] = np.where(
                is_two_letter,
                country_upper.map(CODE_TO_COUNTRY_NAME).fillna(""),
                country_str
            )
        else:
            normalized_df['TWO_LETTER_COUNTRY_CODE'] = ""
            normalized_df['FULL_COUNTRY_NAME'] = ""

        two_letter = normalized_df['TWO_LETTER_COUNTRY_CODE'].fillna("").astype(str)
        full_name = normalized_df['FULL_COUNTRY_NAME'].fillna("").astype(str)
        mapped_from_name = full_name.str.upper().map(COUNTRY_NAME_TO_CODE).fillna("")
        zip_series = normalized_df['Zip'] if 'Zip' in normalized_df.columns else pd.Series([""] * len(normalized_df))
        zip_match = zip_series.fillna("").astype(str).str.extract(r'(\d{5})', expand=False)
        has_zip = zip_match.notna()

        calculated_code = two_letter.mask(two_letter.eq(""), mapped_from_name)
        calculated_code = calculated_code.mask(calculated_code.eq(""), np.where(has_zip, "US", ""))
        normalized_df['CALCULATED_TWO_LETTER_COUNTRY_CODE'] = calculated_code

        shipping_service_series = (
            normalized_df['Shipping Service']
            if 'Shipping Service' in normalized_df.columns
            else pd.Series([""] * len(normalized_df))
        )
        cleaned_service = shipping_service_series.fillna("").astype(str)
        cleaned_service = cleaned_service.str.replace('Â', '', regex=False).str.replace('®', '', regex=False)
        cleaned_service = cleaned_service.str.split(r'\s*[-–—]\s*', n=1, expand=True)[0]
        cleaned_service = cleaned_service.str.replace(r'[^\w\s]', ' ', regex=True)
        cleaned_service = cleaned_service.str.replace(r'\s+', ' ', regex=True).str.upper().str.strip()
        normalized_df['CLEANED_SHIPPING_SERVICE'] = cleaned_service

        priority = pd.Series([""] * len(normalized_df))
        non_empty = cleaned_service.ne("")
        priority = priority.mask(non_empty & cleaned_service.str.contains('GROUND', regex=False), 'GROUND')
        air_mask = non_empty & cleaned_service.str.contains(r'2ND DAY|2 DAY|2DAY', regex=True)
        priority = priority.mask(priority.eq("") & air_mask, 'AIR')
        exp_mask = non_empty & cleaned_service.str.contains('EXPEDITED', regex=False)
        priority = priority.mask(priority.eq("") & exp_mask, 'EXPEDITED')
        priority = priority.mask((priority.eq("")) & non_empty, 'OTHER')
        normalized_df['SHIPPING_PRIORITY'] = priority

        weight_series = None
        if 'Weight' in normalized_df.columns:
            weight_series = pd.to_numeric(normalized_df['Weight'], errors='coerce')
        else:
            weight_series = pd.Series([None] * len(normalized_df))

        if weight_unit == 'oz':
            normalized_df['WEIGHT_IN_OZ'] = weight_series.round(4)
            normalized_df['WEIGHT_IN_LBS'] = (weight_series / 16).round(4)
        elif weight_unit == 'lb':
            normalized_df['WEIGHT_IN_LBS'] = weight_series.round(4)
            normalized_df['WEIGHT_IN_OZ'] = (weight_series * 16).round(4)
        elif weight_unit == 'kg':
            normalized_df['WEIGHT_IN_LBS'] = (weight_series * 2.2046226218).round(4)
            normalized_df['WEIGHT_IN_OZ'] = (weight_series * 35.27396195).round(4)
        else:
            normalized_df['WEIGHT_IN_LBS'] = pd.Series([None] * len(normalized_df))
            normalized_df['WEIGHT_IN_OZ'] = pd.Series([None] * len(normalized_df))

        def _numeric_series(series_name):
            if series_name in normalized_df.columns:
                return pd.to_numeric(normalized_df[series_name], errors='coerce')
            return pd.Series([None] * len(normalized_df))

        length = _numeric_series('Package Length')
        width = _numeric_series('Package Width')
        height = _numeric_series('Package Height')
        volume = length * width * height
        normalized_df['PACKAGE_DIMENSION_VOLUME'] = volume

        size_bins = [-float('inf'), SMALL_MAX_VOLUME, MEDIUM_MAX_VOLUME, float('inf')]
        size_labels = ['SMALL', 'MEDIUM', 'LARGE']
        size_class = pd.cut(volume, bins=size_bins, labels=size_labels, right=False)
        normalized_df['PACKAGE_SIZE_STATUS'] = size_class.where(volume.notna(), "")

        weight_lbs = normalized_df['WEIGHT_IN_LBS']
        weight_bins = [-float('inf'), 1, 5, 10, float('inf')]
        weight_labels = ['<1', '1-5', '5-10', '10+']
        weight_class = pd.cut(weight_lbs, bins=weight_bins, labels=weight_labels, right=False)
        normalized_df['WEIGHT_CLASSIFICATION'] = weight_class.where(weight_lbs.notna(), "")

        origin_zip_value = extract_origin_zip(origin_zip)
        normalized_df['ORIGIN_ZIP_CODE'] = [origin_zip_value] * len(normalized_df)
        
        # Save normalized CSV
        normalized_csv_path = job_dir / 'normalized.csv'
        normalized_df.to_csv(normalized_csv_path, index=False)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/service-levels', methods=['POST'])
def service_levels():
    """Save selected service levels"""
    try:
        data = request.json
        job_id = data.get('job_id')
        selected_services = data.get('selected_services', [])
        
        if not job_id:
            return jsonify({'error': 'job_id required'}), 400
        
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404
        
        # Save service levels
        with open(job_dir / 'service_levels.json', 'w') as f:
            json.dump({'selected_services': selected_services}, f)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/service-levels/<job_id>', methods=['GET'])
def get_service_levels(job_id):
    """Return available merchant service levels for a job"""
    try:
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404

        mapping_file = job_dir / 'mapping.json'
        if not mapping_file.exists():
            return jsonify({'error': 'Mapping not found'}), 404

        with open(mapping_file, 'r') as f:
            mapping_config = json.load(f)

        raw_df = pd.read_csv(job_dir / 'raw_invoice.csv')
        available_services = available_merchant_services(raw_df, mapping_config)

        return jsonify({'available_services': available_services})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/amazon-eligibility', methods=['POST'])
def amazon_eligibility():
    """Return Amazon eligibility based on origin ZIP."""
    try:
        data = request.json or {}
        origin_zip = data.get('origin_zip', '')
        annual_orders = data.get('annual_orders')
        eligibility = compute_eligibility(origin_zip, annual_orders)
        payload = {
            'zip_eligible': eligibility['zip_eligible_amazon'],
            'amazon_volume_avg': eligibility['amazon_volume_avg'],
            'amazon_volume_eligible': eligibility['amazon_volume_eligible'],
            'uniuni_volume_avg': eligibility['uniuni_volume_avg'],
            'uniuni_volume_eligible': eligibility['uniuni_volume_eligible'],
            'amazon_eligible_final': eligibility['amazon_eligible_final'],
            'uniuni_eligible_final': eligibility['uniuni_eligible_final'],
            'eligible': eligibility['amazon_eligible_final']
        }
        return jsonify(payload)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/uniuni-eligibility', methods=['POST'])
def uniuni_eligibility():
    """Return UniUni eligibility based on origin ZIP."""
    try:
        data = request.json or {}
        origin_zip = data.get('origin_zip', '')
        annual_orders = data.get('annual_orders')
        eligibility = compute_eligibility(origin_zip, annual_orders)
        payload = {
            'zip_eligible': eligibility['zip_eligible_uniuni'],
            'amazon_volume_avg': eligibility['amazon_volume_avg'],
            'amazon_volume_eligible': eligibility['amazon_volume_eligible'],
            'uniuni_volume_avg': eligibility['uniuni_volume_avg'],
            'uniuni_volume_eligible': eligibility['uniuni_volume_eligible'],
            'amazon_eligible_final': eligibility['amazon_eligible_final'],
            'uniuni_eligible_final': eligibility['uniuni_eligible_final'],
            'eligible': eligibility['uniuni_eligible_final']
        }
        return jsonify(payload)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/merchant-pricing/<job_id>', methods=['GET', 'POST'])
def merchant_pricing(job_id):
    """Get or save merchant pricing selections."""
    try:
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404

        pricing_file = job_dir / 'merchant_pricing.json'

        if request.method == 'POST':
            data = request.json or {}
            excluded = data.get('excluded_carriers', [])
            included = data.get('included_services', [])
            if not isinstance(excluded, list) or not isinstance(included, list):
                return jsonify({'error': 'Invalid payload'}), 400
            payload = {
                'excluded_carriers': excluded,
                'included_services': included
            }
            with open(pricing_file, 'w') as f:
                json.dump(payload, f)
            return jsonify({'success': True})

        saved = {'excluded_carriers': [], 'included_services': []}
        has_saved = False
        if pricing_file.exists():
            with open(pricing_file, 'r') as f:
                saved = json.load(f)
            has_saved = True

        mapping_file = job_dir / 'mapping.json'
        if not mapping_file.exists():
            return jsonify({'error': 'Mapping not found'}), 404

        with open(mapping_file, 'r') as f:
            mapping_config = json.load(f)

        raw_df = pd.read_csv(job_dir / 'raw_invoice.csv')
        available_services = available_merchant_services(raw_df, mapping_config)
        available_carriers = available_merchant_carriers(raw_df, mapping_config)

        excluded = saved.get('excluded_carriers', [])
        included_services = saved.get('included_services', [])
        if not has_saved and not included_services:
            included_services = default_included_services(available_services)

        eligibility = compute_eligibility(
            mapping_config.get('origin_zip'),
            mapping_config.get('annual_orders'),
            mapping_config=mapping_config
        )
        if not eligibility['amazon_eligible_final'] and 'Amazon' not in excluded:
            excluded.append('Amazon')
        if not eligibility['uniuni_eligible_final'] and 'UniUni' not in excluded:
            excluded.append('UniUni')

        if available_carriers:
            excluded = [c for c in excluded if c in available_carriers]

        return jsonify({
            'carriers': available_carriers,
            'service_levels': available_services,
            'excluded_carriers': excluded,
            'included_services': included_services
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/redo-carriers/<job_id>', methods=['GET', 'POST'])
def redo_carriers(job_id):
    """Get or save redo carrier selections"""
    try:
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404

        mapping_file = job_dir / 'mapping.json'
        if not mapping_file.exists():
            return jsonify({'error': 'Mapping not found'}), 404

        with open(mapping_file, 'r') as f:
            mapping_config = json.load(f)

        eligibility = compute_eligibility(
            mapping_config.get('origin_zip'),
            mapping_config.get('annual_orders'),
            mapping_config=mapping_config
        )

        if request.method == 'POST':
            data = request.json or {}
            selected = data.get('selected_carriers', [])
            selected = [c for c in selected if c in REDO_CARRIERS]
            if not eligibility['amazon_eligible_final']:
                selected = [c for c in selected if c != 'Amazon']
            if not eligibility['uniuni_eligible_final']:
                selected = [c for c in selected if c != 'UniUni']

            with open(job_dir / 'redo_carriers.json', 'w') as f:
                json.dump({'selected_carriers': selected}, f)
            return jsonify({'success': True})

        available = list(REDO_CARRIERS)
        selected = list(REDO_FORCED_ON)
        if eligibility['amazon_eligible_final'] and 'Amazon' not in selected:
            selected.append('Amazon')
        if eligibility['uniuni_eligible_final'] and 'UniUni' not in selected:
            selected.append('UniUni')

        redo_file = job_dir / 'redo_carriers.json'
        if redo_file.exists():
            with open(redo_file, 'r') as f:
                saved = json.load(f)
                selected = saved.get('selected_carriers', list(REDO_FORCED_ON))
            for forced in REDO_FORCED_ON:
                if forced not in selected:
                    selected.append(forced)
            if eligibility['amazon_eligible_final'] and 'Amazon' not in selected:
                selected.append('Amazon')
            if eligibility['uniuni_eligible_final'] and 'UniUni' not in selected:
                selected.append('UniUni')

        if not eligibility['amazon_eligible_final']:
            selected = [c for c in selected if c != 'Amazon']
        if not eligibility['uniuni_eligible_final']:
            selected = [c for c in selected if c != 'UniUni']
        if not eligibility['amazon_eligible_final']:
            available = [c for c in available if c != 'Amazon']
        if not eligibility['uniuni_eligible_final']:
            available = [c for c in available if c != 'UniUni']

        return jsonify({
            'detected_carriers': available,
            'selected_carriers': selected,
            'default_selected': list(REDO_FORCED_ON)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/generate', methods=['POST'])
def generate():
    """Start rate card generation"""
    try:
        data = request.json
        job_id = data.get('job_id')
        
        if not job_id:
            return jsonify({'error': 'job_id required'}), 400
        
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404
        
        # Load configs
        with open(job_dir / 'mapping.json', 'r') as f:
            mapping_config = json.load(f)
        
        merchant_pricing = {'excluded_carriers': [], 'included_services': []}
        pricing_file = job_dir / 'merchant_pricing.json'
        if pricing_file.exists():
            with open(pricing_file, 'r') as f:
                merchant_pricing = json.load(f)
        
        # Initialize progress
        progress_file = job_dir / 'progress.json'
        estimated_seconds = None
        normalized_csv = job_dir / 'normalized.csv'
        if normalized_csv.exists():
            try:
                with open(normalized_csv, newline='') as f:
                    rows = sum(1 for _ in f) - 1
                rows = max(rows, 0)
                estimated_seconds = max(5, min(180, int(rows * 0.05)))
            except Exception:
                estimated_seconds = None

        progress_payload = {'started_at': datetime.now(timezone.utc).isoformat()}
        if estimated_seconds is not None:
            progress_payload['eta_seconds'] = estimated_seconds

        with open(progress_file, 'w') as f:
            json.dump(progress_payload, f)
        
        def run_generation():
            try:
                generate_rate_card(job_dir, mapping_config, merchant_pricing)
                rate_card_files = list(job_dir.glob('* - Rate Card.xlsx'))
                if rate_card_files:
                    source_mtime = int(rate_card_files[0].stat().st_mtime)
                    redo_selected = []
                    redo_file = job_dir / 'redo_carriers.json'
                    if redo_file.exists():
                        with open(redo_file, 'r') as f:
                            redo_config = json.load(f)
                            redo_selected = redo_config.get('selected_carriers', [])
                    else:
                        redo_selected = list(REDO_FORCED_ON)
                        eligibility = compute_eligibility(
                            mapping_config.get('origin_zip'),
                            mapping_config.get('annual_orders'),
                            mapping_config=mapping_config
                        )
                        if eligibility['amazon_eligible_final']:
                            redo_selected.append('Amazon')
                        if eligibility['uniuni_eligible_final']:
                            redo_selected.append('UniUni')
                    selected_set = _dashboard_selected_from_redo(redo_selected)
                    selected_dashboard = [c for c in DASHBOARD_CARRIERS if c in selected_set]
                    if selected_dashboard:
                        _start_summary_cache(job_dir, source_mtime, selected_dashboard)
            except Exception as e:
                write_error(job_dir, f'Generation failed: {str(e)}')

        thread = threading.Thread(target=run_generation, daemon=True)
        thread.start()
        return jsonify({'success': True, 'status': 'started'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def write_progress(job_dir, step, value=True):
    """Write progress update"""
    progress_file = job_dir / 'progress.json'
    progress = {}
    if progress_file.exists():
        with open(progress_file, 'r') as f:
            progress = json.load(f)
    progress[step] = value
    with open(progress_file, 'w') as f:
        json.dump(progress, f)

def write_error(job_dir, message):
    """Write error to progress file."""
    progress_file = job_dir / 'progress.json'
    progress = {}
    if progress_file.exists():
        with open(progress_file, 'r') as f:
            progress = json.load(f)
    progress['error'] = message
    with open(progress_file, 'w') as f:
        json.dump(progress, f)

def generate_rate_card(job_dir, mapping_config, merchant_pricing):
    """Generate the rate card Excel file"""
    # Save output path first
    merchant_name = mapping_config.get('merchant_name', 'Merchant')
    output_filename = f"{merchant_name} - Rate Card.xlsx"
    output_path = job_dir / output_filename
    
    # Step 1: Normalize data
    write_progress(job_dir, 'normalize', True)
    
    # Copy template to output location first to preserve file structure
    template_path = Path('#New Template - Rate Card.xlsx')
    if not template_path.exists():
        template_path = Path('Rate Card Template.xlsx')
    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")
    
    # Load template directly - openpyxl will preserve structure when saving
    # Don't use keep_vba as it can cause corruption with openpyxl
    wb = openpyxl.load_workbook(template_path, keep_vba=False, data_only=False)
    if 'Raw Data' not in wb.sheetnames:
        raise ValueError("Template must contain 'Raw Data' sheet")
    ws = wb['Raw Data']
    
    headers = [cell.value for cell in ws[1]]
    header_to_col = {}
    for idx, header in enumerate(headers):
        if header is None:
            continue
        header_str = str(header).strip()
        if header_str:
            header_to_col[header_str] = idx + 1
    
    redo_config = {}
    redo_file = job_dir / 'redo_carriers.json'
    if redo_file.exists():
        with open(redo_file, 'r') as f:
            redo_config = json.load(f)

    # Load original raw CSV to get zone if needed
    raw_df = pd.read_csv(job_dir / 'raw_invoice.csv')
    
    # Load normalized data
    normalized_df = pd.read_csv(job_dir / 'normalized.csv')

    if not normalized_df.empty:
        debug_cols = [
            'WEIGHT_IN_LBS',
            'TWO_LETTER_COUNTRY_CODE',
            'CALCULATED_TWO_LETTER_COUNTRY_CODE',
            'FULL_COUNTRY_NAME',
            'CLEANED_SHIPPING_SERVICE',
            'SHIPPING_PRIORITY',
            'PACKAGE_DIMENSION_VOLUME',
            'PACKAGE_SIZE_STATUS',
            'WEIGHT_CLASSIFICATION',
            'ORIGIN_ZIP_CODE'
        ]
        sample = {}
        for col in debug_cols:
            if col in normalized_df.columns:
                sample[col] = normalized_df.iloc[0].get(col)
        if sample:
            app.logger.info("Computed columns sample: %s", sample)
    
    # Step 2: Write into template
    write_progress(job_dir, 'write_template', True)
    
    # Find zone column in original CSV if zone-based
    zone_col = mapping_config.get('zone_column')
    if not zone_col and mapping_config.get('structure') == 'zone':
        for col in raw_df.columns:
            if 'zone' in col.lower():
                zone_col = col
                break
    
    # Map standard fields to Excel columns
    field_to_excel = {
        'Order Number': 'ORDER_NUMBER',
        'Order Date': 'DATE',
        'Zip': 'DESTINATION_ZIP_CODE',
        'Weight': 'WEIGHT_IN_OZ',
        'WEIGHT_IN_LBS': 'WEIGHT_IN_LBS',
        'Shipping Carrier': 'SHIPPING_CARRIER',
        'CLEANED_SHIPPING_SERVICE': 'CLEANED_SHIPPING_SERVICE',
        'Package Height': 'PACKAGE_HEIGHT',
        'Package Width': 'PACKAGE_WIDTH',
        'Package Length': 'PACKAGE_LENGTH',
        'PACKAGE_DIMENSION_VOLUME': 'PACKAGE_DIMENSION_VOLUME',
        'ORIGIN_ZIP_CODE': 'ORIGIN_ZIP_CODE',
        'Shipping Service': 'SHIPPING_SERVICE',
        'Label Cost': 'LABEL_COST',
        'Zone': 'ZONE'
    }

    required_headers = {
        'ORDER_NUMBER',
        'DATE',
        'DESTINATION_ZIP_CODE',
        'WEIGHT_IN_OZ',
        'WEIGHT_IN_LBS',
        'SHIPPING_CARRIER',
        'CLEANED_SHIPPING_SERVICE',
        'SHIPPING_SERVICE',
        'LABEL_COST',
        'MERCHANT_ID',
        'ZONE',
        'QUALIFIED'
    }
    missing_headers = [h for h in required_headers if h not in header_to_col]
    if missing_headers:
        raise ValueError(f"Template missing required headers: {', '.join(sorted(missing_headers))}")
    
    # Get merchant pricing selections
    excluded_carriers = merchant_pricing.get('excluded_carriers', [])
    included_services = merchant_pricing.get('included_services', [])
    normalized_selected = {normalize_service_name(s) for s in included_services}
    normalized_excluded = {normalize_merchant_carrier(c) for c in excluded_carriers}
    
    # Identify formula columns (AI-AN, 1-indexed)
    formula_cols = set(range(35, 41))

    # Find starting row (skip header row)
    start_row = 2
    table_min_col = None
    table_max_col = None
    table_max_row = None
    if ws.tables:
        table = next(iter(ws.tables.values()))
        min_col, min_row, max_col, max_row = range_boundaries(table.ref)
        start_row = min_row + 1
        table_min_col = min_col
        table_max_col = max_col
        table_max_row = max_row

    if table_min_col is not None and table_max_col is not None:
        for col_idx in range(table_min_col, table_max_col + 1):
            cell_value = ws.cell(start_row, col_idx).value
            if cell_value and str(cell_value).startswith('='):
                formula_cols.add(col_idx)
    
    column_data = {
        field: normalized_df[field].tolist()
        for field in field_to_excel.keys()
        if field in normalized_df.columns
    }
    shipping_service_data = normalized_df['Shipping Service'].tolist() if 'Shipping Service' in normalized_df.columns else None
    shipping_carrier_data = normalized_df['Shipping Carrier'].tolist() if 'Shipping Carrier' in normalized_df.columns else None

    write_cols = set()
    write_fields = []
    for std_field, excel_col in field_to_excel.items():
        col_idx = header_to_col.get(excel_col)
        if col_idx:
            write_cols.add(col_idx)
            if std_field in column_data:
                write_fields.append((std_field, excel_col, col_idx))
    if 'QUALIFIED' in header_to_col:
        write_cols.add(header_to_col['QUALIFIED'])
    if 'MERCHANT_ID' in header_to_col:
        write_cols.add(header_to_col['MERCHANT_ID'])
    origin_zip_value = extract_origin_zip(mapping_config.get('origin_zip'))
    if 'ORIGIN_ZIP_CODE' in header_to_col:
        write_cols.add(header_to_col['ORIGIN_ZIP_CODE'])

    if table_max_row and write_cols:
        clear_end_row = start_row + len(normalized_df) - 1
        if table_max_row:
            clear_end_row = min(clear_end_row, table_max_row)
        for row_idx in range(start_row, clear_end_row + 1):
            for col_idx in write_cols:
                if col_idx in formula_cols:
                    continue
                cell = ws.cell(row_idx, col_idx)
                if cell.value and str(cell.value).startswith('='):
                    continue
                cell.value = None

    numeric_cols = {
        'WEIGHT_IN_OZ',
        'WEIGHT_IN_LBS',
        'PACKAGE_HEIGHT',
        'PACKAGE_WIDTH',
        'PACKAGE_LENGTH',
        'PACKAGE_DIMENSION_VOLUME',
        'LABEL_COST'
    }

    # Write data starting from row 2
    for idx in range(len(normalized_df)):
        excel_row = start_row + idx
        
        # Write mapped fields
        for std_field, excel_col, col_idx in write_fields:
            # Only write if not a formula column
            if col_idx in formula_cols:
                continue
            cell = ws.cell(excel_row, col_idx)
            if cell.value and str(cell.value).startswith('='):
                continue
            value = column_data[std_field][idx]
            # Handle NaN values
            if pd.isna(value):
                value = None
            if std_field == 'ORIGIN_ZIP_CODE' and value is None and origin_zip_value is not None:
                value = origin_zip_value
            else:
                # Format dates
                if std_field == 'Order Date' and excel_col == 'DATE':
                    try:
                        # Try parsing with pandas
                        if isinstance(value, str):
                            value = pd.to_datetime(value)
                        if hasattr(value, 'to_pydatetime'):
                            value = value.to_pydatetime()
                    except:
                        pass
                # Extract zip code (first 5 digits if longer)
                elif std_field == 'Zip' and excel_col == 'DESTINATION_ZIP_CODE':
                    zip_str = str(value).strip()
                    # Extract first 5 digits
                    zip_match = re.search(r'\d{5}', zip_str)
                    if zip_match:
                        value = int(zip_match.group())
                    else:
                        value = None
                # Ensure numeric types for numeric columns
                elif excel_col in numeric_cols:
                    try:
                        value = float(value) if value else None
                    except:
                        value = None
            
            # Only write non-None values to avoid breaking Excel structure
            if value is not None:
                ws.cell(excel_row, col_idx, value)
        
        # Zone is now handled through the mapping like other fields
        # But if it's zone-based and zone wasn't mapped, try to get it from raw CSV
        if 'Zone' not in normalized_df.columns and mapping_config.get('structure') == 'zone' and zone_col:
            if 'ZONE' in header_to_col:
                col_idx = header_to_col['ZONE']
                if col_idx not in formula_cols:
                    zone_value = raw_df.iloc[idx][zone_col] if idx < len(raw_df) else None
                    if pd.notna(zone_value):
                        try:
                            zone_value = int(float(zone_value))
                            ws.cell(excel_row, col_idx, zone_value)
                        except:
                            pass
        
        # Write MERCHANT_ID if provided
        if mapping_config.get('merchant_id') and 'MERCHANT_ID' in header_to_col:
            col_idx = header_to_col['MERCHANT_ID']
            if col_idx not in formula_cols:
                ws.cell(excel_row, col_idx, mapping_config['merchant_id'])
        
        # Write QUALIFIED based on service matching
        if 'QUALIFIED' in header_to_col:
            col_idx = header_to_col['QUALIFIED']
            if col_idx not in formula_cols:
                shipping_service = str(shipping_service_data[idx]) if shipping_service_data is not None else ''
                normalized_service = normalize_service_name(shipping_service)
                carrier_value = shipping_carrier_data[idx] if shipping_carrier_data is not None else ''
                carrier_normalized = normalize_merchant_carrier(carrier_value)
                carrier_allowed = not carrier_normalized or carrier_normalized not in normalized_excluded
                is_qualified = carrier_allowed and normalized_service in normalized_selected
                ws.cell(excel_row, col_idx, is_qualified)

    # Update Pricing & Summary redo carrier selections
    if 'Pricing & Summary' in wb.sheetnames:
        selected_redo = redo_config.get('selected_carriers', [])
        selected_services = merchant_pricing.get('included_services', [])
        excluded_carriers = merchant_pricing.get('excluded_carriers', [])
        summary_ws = wb['Pricing & Summary']
        # Populate Annual Orders to avoid blank Deal Info calculations.
        annual_orders_value = mapping_config.get('annual_orders')
        try:
            annual_orders_value = int(float(annual_orders_value)) if annual_orders_value else None
        except Exception:
            annual_orders_value = None
        summary_ws['C9'] = annual_orders_value if annual_orders_value is not None else 13968
        if origin_zip_value is not None:
            summary_ws['C29'] = origin_zip_value
        pct_off, dollar_off = _usps_market_discount_values(mapping_config)
        summary_ws['C19'] = pct_off
        summary_ws['C20'] = dollar_off
        update_pricing_summary_redo_carriers(summary_ws, selected_redo)
        update_pricing_summary_merchant_carriers(summary_ws, excluded_carriers)
        update_pricing_summary_merchant_service_levels(
            summary_ws, selected_services, normalized_df
        )

    # Ensure formula columns remain formulas (copy from row 2 if exists)
    last_data_row = start_row + len(normalized_df) - 1
    if last_data_row >= start_row:
        for col_idx in formula_cols:
            source_cell = ws.cell(2, col_idx)
            if source_cell.value and str(source_cell.value).startswith('='):
                formula = source_cell.value
                for row_idx in range(start_row, last_data_row + 1):
                    target_cell = ws.cell(row_idx, col_idx)
                    if not target_cell.value or not str(target_cell.value).startswith('='):
                        target_cell.value = formula
    
    # Step 3: Save and finalize
    write_progress(job_dir, 'finalize', True)
    wb.calculation.fullCalcOnLoad = True
    wb.calculation.calcMode = "auto"
    if hasattr(wb, "_calcChain"):
        wb._calcChain = None
    
    # Save the workbook atomically
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, dir=job_dir, suffix='.xlsx') as tmp:
            temp_file = Path(tmp.name)
        wb.save(temp_file)
        if not zipfile.is_zipfile(temp_file):
            raise Exception("Generated file is not a valid XLSX archive")
        os.replace(temp_file, output_path)
    except Exception as e:
        if temp_file and temp_file.exists():
            try:
                temp_file.unlink()
            except Exception:
                pass
        raise Exception(f"Failed to save Excel file: {str(e)}")
    finally:
        wb.close()
    try:
        log_admin_entry(job_dir.name, mapping_config, merchant_pricing, redo_config)
    except Exception:
        pass

    return output_path

@app.route('/api/status/<job_id>')
def status(job_id=None):
    """Get job status and file links"""
    if not job_id:
        return jsonify({'error': 'Job ID required'}), 400
    
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    if not job_dir.exists():
        return jsonify({'error': 'Job not found'}), 404
    
    # Check progress file
    progress_file = job_dir / 'progress.json'
    progress = {}
    if progress_file.exists():
        with open(progress_file, 'r') as f:
            progress = json.load(f)
    
    # Surface generation error if present
    if 'error' in progress:
        return jsonify({
            'ready': False,
            'error': progress['error'],
            'progress': progress
        })

    # Check if generation is complete
    rate_card_files = list(job_dir.glob('* - Rate Card.xlsx'))
    is_complete = len(rate_card_files) > 0
    
    if is_complete:
        # Load merchant name for redirect
        merchant_name = 'Merchant'
        mapping_file = job_dir / 'mapping.json'
        if mapping_file.exists():
            with open(mapping_file, 'r') as f:
                config = json.load(f)
                merchant_name = config.get('merchant_name', 'Merchant')
        return jsonify({
            'ready': True,
            'redirect_url': f'/dashboard?job_id={job_id}',
            'progress': progress
        })
    
    eta_remaining = None
    if progress.get('started_at') and progress.get('eta_seconds'):
        try:
            started_at = datetime.fromisoformat(progress['started_at'])
            elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
            eta_remaining = max(0, int(progress['eta_seconds'] - elapsed))
        except Exception:
            eta_remaining = None

    return jsonify({
        'ready': False,
        'progress': progress,
        'eta_seconds_remaining': eta_remaining
    })

@app.route('/api/dashboard/<job_id>', methods=['GET', 'POST'])
def dashboard_data(job_id):
    """Return dashboard metrics for overall and per-carrier selections."""
    try:
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404
        mapping_file = job_dir / 'mapping.json'
        mapping_config = {}
        if mapping_file.exists():
            with open(mapping_file, 'r') as f:
                mapping_config = json.load(f)
        annual_orders_missing = _annual_orders_missing(mapping_config)
        pct_off, dollar_off = _usps_market_discount_values(mapping_config)
        rate_card_files = list(job_dir.glob('* - Rate Card.xlsx'))
        if not rate_card_files:
            return jsonify({'error': 'Rate card not found'}), 404
        source_mtime = int(rate_card_files[0].stat().st_mtime)
        refresh = request.args.get('refresh') == '1'
        if refresh:
            summary_cache = _summary_cache_path(job_dir)
            breakdown_cache = _cache_path_for_job(job_dir)
            carrier_details_cache = _carrier_details_cache_path(job_dir)
            if summary_cache.exists():
                summary_cache.unlink()
            if breakdown_cache.exists():
                breakdown_cache.unlink()
            if carrier_details_cache.exists():
                carrier_details_cache.unlink()
            job_prefix = f"{job_dir.name}:"
            with summary_jobs_lock:
                for key in list(summary_jobs.keys()):
                    if key.startswith(job_prefix):
                        summary_jobs.pop(key, None)
            with dashboard_jobs_lock:
                for key in list(dashboard_jobs.keys()):
                    if key.startswith(job_prefix):
                        dashboard_jobs.pop(key, None)

        eligibility = None
        if mapping_config:
            eligibility = compute_eligibility(
                mapping_config.get('origin_zip'),
                mapping_config.get('annual_orders'),
                mapping_config=mapping_config
            )

        available_carriers = list(DASHBOARD_CARRIERS)
        if eligibility and not eligibility['amazon_eligible_final']:
            available_carriers = [c for c in available_carriers if c != 'Amazon']
        if eligibility and not eligibility['uniuni_eligible_final']:
            available_carriers = [c for c in available_carriers if c != 'UniUni']

        redo_selected = []
        redo_file = job_dir / 'redo_carriers.json'
        if redo_file.exists():
            with open(redo_file, 'r') as f:
                redo_config = json.load(f)
                redo_selected = redo_config.get('selected_carriers', [])
        else:
            redo_selected = list(REDO_FORCED_ON)
            if eligibility and eligibility['amazon_eligible_final']:
                redo_selected.append('Amazon')
            if eligibility and eligibility['uniuni_eligible_final']:
                redo_selected.append('UniUni')

        selected_set = _dashboard_selected_from_redo(redo_selected)
        selected_dashboard = [c for c in available_carriers if c in selected_set]
        show_usps_market_discount = bool(
            (eligibility and (eligibility['amazon_eligible_final'] or eligibility['uniuni_eligible_final']))
            or ('Amazon' in redo_selected or 'UniUni' in redo_selected)
        )
        carrier_percentages = _carrier_distribution(job_dir, mapping_config, available_carriers)
        if request.method == 'POST':
            data = request.json or {}
            incoming = data.get('selected_carriers', [])
            if isinstance(incoming, list) and incoming:
                selected_dashboard = [c for c in available_carriers if c in incoming]

        per_carrier = []
        include_per_carrier = request.args.get('per_carrier') == '1'
        if request.method == 'GET' and include_per_carrier:
            selection_key = _selection_cache_key(selected_dashboard)
            selections = {carrier: [carrier] for carrier in available_carriers}
            metrics_map = _calculate_metrics_batch(job_dir, selections)
            per_carrier = [
                {'carrier': carrier, 'metrics': metrics_map.get(carrier, {})}
                for carrier in available_carriers
            ]
            overall_metrics = _calculate_metrics(job_dir, selected_dashboard)
            _write_summary_cache(job_dir, source_mtime, selection_key, overall_metrics)
            summary_pending = False
            pending = False
            per_carrier_count = len(per_carrier)
            if annual_orders_missing:
                overall_metrics = {**overall_metrics}
                for key in ('Est. Merchant Annual Savings', 'Est. Redo Deal Size', 'Spread Available'):
                    overall_metrics.pop(key, None)
                for entry in per_carrier:
                    metrics = entry.get('metrics') or {}
                    for key in ('Est. Merchant Annual Savings', 'Est. Redo Deal Size', 'Spread Available'):
                        metrics.pop(key, None)
                    entry['metrics'] = metrics
            return jsonify({
                'selected_carriers': selected_dashboard,
                'available_carriers': available_carriers,
                'overall': overall_metrics,
                'per_carrier': per_carrier,
                'pending': False,
                'summary_pending': summary_pending,
                'per_carrier_count': per_carrier_count,
                'annual_orders_missing': annual_orders_missing,
                'show_usps_market_discount': show_usps_market_discount,
                'usps_market_pct_off': pct_off,
                'usps_market_dollar_off': dollar_off,
                'carrier_percentages': carrier_percentages,
                'per_carrier_total': len(available_carriers)
            })

        if refresh:
            overall_metrics = _calculate_metrics(job_dir, selected_dashboard)
            summary_pending = False
        else:
            overall_metrics, summary_pending = _start_summary_cache(job_dir, source_mtime, selected_dashboard)
        if overall_metrics is None and not summary_pending:
            overall_metrics = {}
        if annual_orders_missing:
            overall_metrics = {**(overall_metrics or {})}
            for key in ('Est. Merchant Annual Savings', 'Est. Redo Deal Size', 'Spread Available'):
                overall_metrics.pop(key, None)

        return jsonify({
            'selected_carriers': selected_dashboard,
            'available_carriers': available_carriers,
            'overall': overall_metrics,
            'per_carrier': [
                entry for entry in (per_carrier or [])
                if entry.get('carrier') in available_carriers
            ],
            'pending': False,
            'summary_pending': summary_pending,
            'annual_orders_missing': annual_orders_missing,
            'show_usps_market_discount': show_usps_market_discount,
            'usps_market_pct_off': pct_off,
            'usps_market_dollar_off': dollar_off,
            'carrier_percentages': carrier_percentages
        })
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download/<job_id>/rate-card')
def download_rate_card(job_id):
    """Download generated rate card"""
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    if not job_dir.exists():
        return jsonify({'error': 'Job not found'}), 404
    
    rate_card_files = list(job_dir.glob('* - Rate Card.xlsx'))
    if not rate_card_files:
        return jsonify({'error': 'Rate card not found'}), 404
    
    return send_file(rate_card_files[0], as_attachment=True)

@app.route('/api/annual-orders/<job_id>', methods=['POST'])
def update_annual_orders(job_id):
    """Update annual orders and regenerate rate card."""
    try:
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404
        mapping_file = job_dir / 'mapping.json'
        if not mapping_file.exists():
            return jsonify({'error': 'Mapping not found'}), 404
        data = request.json or {}
        annual_orders = data.get('annual_orders')
        try:
            annual_orders_value = int(float(annual_orders))
        except Exception:
            return jsonify({'error': 'Invalid annual orders'}), 400
        if annual_orders_value <= 0:
            return jsonify({'error': 'Annual orders must be greater than 0'}), 400

        with open(mapping_file, 'r') as f:
            mapping_config = json.load(f)
        mapping_config['annual_orders'] = annual_orders_value
        with open(mapping_file, 'w') as f:
            json.dump(mapping_config, f)

        merchant_pricing = {'excluded_carriers': [], 'included_services': []}
        pricing_file = job_dir / 'merchant_pricing.json'
        if pricing_file.exists():
            with open(pricing_file, 'r') as f:
                merchant_pricing = json.load(f)

        generate_rate_card(job_dir, mapping_config, merchant_pricing)

        summary_cache = _summary_cache_path(job_dir)
        breakdown_cache = _cache_path_for_job(job_dir)
        carrier_details_cache = _carrier_details_cache_path(job_dir)
        if summary_cache.exists():
            summary_cache.unlink()
        if breakdown_cache.exists():
            breakdown_cache.unlink()
        if carrier_details_cache.exists():
            carrier_details_cache.unlink()

        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/usps-market-discount/<job_id>', methods=['POST'])
def update_usps_market_discount(job_id):
    """Update USPS Market discount settings and regenerate rate card."""
    try:
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404
        mapping_file = job_dir / 'mapping.json'
        if not mapping_file.exists():
            return jsonify({'error': 'Mapping not found'}), 404
        data = request.json or {}
        pct_off = data.get('pct_off')
        dollar_off = data.get('dollar_off')
        try:
            pct_off = float(pct_off)
            dollar_off = float(dollar_off)
        except Exception:
            return jsonify({'error': 'Invalid discount values'}), 400
        if pct_off > 1:
            pct_off = pct_off / 100
        if pct_off < 0 or dollar_off < 0:
            return jsonify({'error': 'Discount values must be non-negative'}), 400

        with open(mapping_file, 'r') as f:
            mapping_config = json.load(f)
        mapping_config['usps_market_pct_off'] = pct_off
        mapping_config['usps_market_dollar_off'] = dollar_off
        with open(mapping_file, 'w') as f:
            json.dump(mapping_config, f)

        merchant_pricing = {'excluded_carriers': [], 'included_services': []}
        pricing_file = job_dir / 'merchant_pricing.json'
        if pricing_file.exists():
            with open(pricing_file, 'r') as f:
                merchant_pricing = json.load(f)

        generate_rate_card(job_dir, mapping_config, merchant_pricing)

        summary_cache = _summary_cache_path(job_dir)
        breakdown_cache = _cache_path_for_job(job_dir)
        carrier_details_cache = _carrier_details_cache_path(job_dir)
        if summary_cache.exists():
            summary_cache.unlink()
        if breakdown_cache.exists():
            breakdown_cache.unlink()
        if carrier_details_cache.exists():
            carrier_details_cache.unlink()
        job_prefix = f"{job_dir.name}:"
        with summary_jobs_lock:
            for key in list(summary_jobs.keys()):
                if key.startswith(job_prefix):
                    summary_jobs.pop(key, None)
        with dashboard_jobs_lock:
            for key in list(dashboard_jobs.keys()):
                if key.startswith(job_prefix):
                    dashboard_jobs.pop(key, None)

        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/deal-sizing/<job_id>', methods=['GET', 'POST'])
def deal_sizing_data(job_id):
    """Return inputs needed for deal sizing."""
    try:
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404
        mapping_file = job_dir / 'mapping.json'
        if not mapping_file.exists():
            return jsonify({'error': 'Mapping not found'}), 404
        with open(mapping_file, 'r') as f:
            mapping_config = json.load(f)
        selected_carriers = []
        if request.method == 'POST':
            payload = request.get_json(silent=True) or {}
            selected_carriers = payload.get('selected_carriers', [])
        else:
            raw_selected = request.args.get('selected_carriers')
            if raw_selected:
                selected_carriers = [c.strip() for c in raw_selected.split(',') if c.strip()]
        if not isinstance(selected_carriers, list):
            selected_carriers = []
        selected_carriers = [c for c in selected_carriers if c in DASHBOARD_CARRIERS]
        annual_orders = mapping_config.get('annual_orders')
        avg_label_cost = _avg_label_cost_from_job(job_dir)
        carrier_details = []
        carrier_details_pending = False
        rate_card_files = list(job_dir.glob('* - Rate Card.xlsx'))
        if rate_card_files:
            try:
                source_mtime = int(rate_card_files[0].stat().st_mtime)
            except Exception:
                source_mtime = 0
            selection_key = _selection_cache_key(selected_carriers)
            detail_map, carrier_details_pending = _start_carrier_details_cache(
                job_dir,
                source_mtime,
                selection_key,
                selected_carriers,
                mapping_config
            )
            if detail_map is None:
                detail_map = _load_carrier_details(rate_card_files[0], source_mtime) or {}
            if not isinstance(detail_map, dict):
                detail_map = {}
            carrier_details = list(detail_map.values())
        return jsonify({
            'annual_orders': annual_orders,
            'avg_label_cost': avg_label_cost,
            'carrier_details': carrier_details,
            'carrier_details_pending': carrier_details_pending
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download/<job_id>/raw-invoice')
def download_raw_invoice(job_id):
    """Download original raw invoice"""
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    raw_csv = job_dir / 'raw_invoice.csv'
    
    if not raw_csv.exists():
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(raw_csv, as_attachment=True, download_name='raw_invoice.csv')

@app.route('/download/<job_id>/normalized')
def download_normalized(job_id):
    """Download normalized CSV"""
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    normalized_csv = job_dir / 'normalized.csv'
    
    if not normalized_csv.exists():
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(normalized_csv, as_attachment=True, download_name='normalized.csv')

if __name__ == '__main__':
    app.run(debug=True, port=5000, threaded=True, use_reloader=False)
