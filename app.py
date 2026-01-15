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
from datetime import datetime, timedelta
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
    controls = _get_pricing_controls(str(template_path))

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

    total_count = 0
    for key, count_val in count_all.items():
        if count_qualified.get(key, 0) > 0:
            total_count += count_val
    if total_count <= 0:
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
    c22 = float(controls['c22'] or 0)
    c23 = float(controls['c23'] or 0)
    c25 = float(controls['c25'] or 0)
    c26 = float(controls['c26'] or 0)

    for (zone_val, weight_val), count_val in count_all.items():
        if count_qualified.get((zone_val, weight_val), 0) <= 0:
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
        if winning_carrier == 'USPS Market':
            rate_offered = redo_rate
        elif c19 > 0 or c20 > 0:
            if c19 == 0:
                rate_offered = max(redo_rate, merchant - c20)
            else:
                rate_offered = max(redo_rate, merchant * (1 - c19))
        elif c22 > 0 or c23 > 0:
            if c22 == 0:
                rate_offered = redo_rate + c23
            else:
                rate_offered = redo_rate * (1 + c22)
        else:
            if c26 == 0:
                rate_offered = redo_rate * (c25 + 1)
            else:
                rate_offered = redo_rate + c26

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

    orders_in_analysis = total_count
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
    usps_won_pct = usps_won_count / total_count if total_count else 0
    ups_won_pct = ups_won_count / total_count if total_count else 0
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
    orders_winable = winable_count / total_count if total_count else 0
    orders_won = won_count / total_count if total_count else 0

    return {
        'Est. Merchant Annual Savings': est_savings,
        'Est. Redo Deal Size': est_redo_deal,
        'Spread Available': spread_available,
        '% Orders We Could Win': orders_winable,
        '% Orders Won W/ Spread': orders_won
    }

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
    payload = {'source_mtime': source_mtime, 'updated_at': datetime.utcnow().isoformat(), 'entries': {}}
    if cache_path.exists():
        try:
            with open(cache_path, 'r') as f:
                existing = json.load(f)
            if existing.get('source_mtime') == source_mtime:
                payload = existing
        except Exception:
            payload = {'source_mtime': source_mtime, 'updated_at': datetime.utcnow().isoformat(), 'entries': {}}
    payload['source_mtime'] = source_mtime
    payload['updated_at'] = datetime.utcnow().isoformat()
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
        'updated_at': datetime.utcnow().isoformat(),
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

@app.route('/')
def index():
    clean_old_runs()
    return render_template('screen1.html')

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
        origin_zip = data.get('origin_zip', '')
        annual_orders = data.get('annual_orders', '')
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
            'origin_zip': origin_zip,
            'annual_orders': annual_orders,
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

        progress_payload = {'started_at': datetime.utcnow().isoformat()}
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
            elapsed = (datetime.utcnow() - started_at).total_seconds()
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
        rate_card_files = list(job_dir.glob('* - Rate Card.xlsx'))
        if not rate_card_files:
            return jsonify({'error': 'Rate card not found'}), 404
        source_mtime = int(rate_card_files[0].stat().st_mtime)

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
        if request.method == 'POST':
            data = request.json or {}
            incoming = data.get('selected_carriers', [])
            if isinstance(incoming, list) and incoming:
                selected_dashboard = [c for c in available_carriers if c in incoming]

        per_carrier = []
        include_per_carrier = request.args.get('per_carrier') == '1'
        if request.method == 'GET' and include_per_carrier:
            selection_key = _selection_cache_key(selected_dashboard)
            overall_metrics = _read_summary_cache(job_dir, source_mtime, selection_key) or {}
            summary_pending = not bool(overall_metrics)
            summary_job_key = _summary_job_key(job_dir, source_mtime, selection_key)
            with summary_jobs_lock:
                summary_running = summary_job_key in summary_jobs

            cached, pending = _start_breakdown_cache(
                job_dir,
                source_mtime,
                selected_dashboard if summary_pending and not summary_running else None,
                selection_key if summary_pending and not summary_running else None,
                available_carriers
            )
            per_carrier = cached or []
            if per_carrier:
                per_carrier = [
                    entry for entry in per_carrier
                    if entry.get('carrier') in available_carriers
                ]
            per_carrier_count = len(per_carrier)
            return jsonify({
                'selected_carriers': selected_dashboard,
                'available_carriers': available_carriers,
                'overall': overall_metrics,
                'per_carrier': per_carrier,
                'pending': pending,
                'summary_pending': summary_pending,
                'per_carrier_count': per_carrier_count,
                'per_carrier_total': len(available_carriers)
            })

        overall_metrics, summary_pending = _start_summary_cache(job_dir, source_mtime, selected_dashboard)
        if overall_metrics is None and not summary_pending:
            overall_metrics = {}

        return jsonify({
            'selected_carriers': selected_dashboard,
            'available_carriers': available_carriers,
            'overall': overall_metrics,
            'per_carrier': [
                entry for entry in (per_carrier or [])
                if entry.get('carrier') in available_carriers
            ],
            'pending': False,
            'summary_pending': summary_pending
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
