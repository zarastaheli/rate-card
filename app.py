import os
import csv
import json
import uuid
import shutil
import re
import zipfile
import tempfile
import threading
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_file, session
import pandas as pd
import openpyxl
from openpyxl.utils.cell import range_boundaries
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['UPLOAD_FOLDER'] = 'runs'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

# Thresholds for size/weight classification.
SMALL_MAX_VOLUME = 1728
MEDIUM_MAX_VOLUME = 5000
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
        'Weight (oz)',
        'Shipping Carrier',
        'Shipping Service',
        'Package Height',
        'Package Width',
        'Package Length',
        'Zone'
    ],
    'optional': ['Label Cost']
}

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
    'First Mile 2-5 Days',
    'USPS Market',
    'UPS Ground',
    'UPS Ground Saver',
    'FedEx',
    'Amazon',
    'First Mile 1-3 Days',
    'First Mile 3-8 Days',
    'DHL'
]

REDO_FORCED_ON = ['USPS Market', 'UPS Ground', 'UPS Ground Saver']

def normalize_service_name(service):
    """Normalize service name for matching"""
    if not service:
        return ""
    cleaned = str(service).replace('Â', '').replace('®', '')
    # Remove punctuation and symbols, collapse whitespace, uppercase
    normalized = re.sub(r'[^\w\s]', '', cleaned)
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized.upper().strip()

def clean_shipping_service(service):
    """Normalize shipping service for cleaned column output."""
    if not service:
        return ""
    cleaned = str(service).replace('Â', '').replace('®', '')
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
    if 'FEDEX' in text:
        return 'FedEx'
    if 'AMAZON' in text:
        return 'Amazon'
    if 'DHL' in text:
        return 'DHL'
    if 'FIRST MILE' in text:
        if '1 3' in text or '1-3' in text or '1 TO 3' in text:
            return 'First Mile 1-3 Days'
        if '3 8' in text or '3-8' in text or '3 TO 8' in text:
            return 'First Mile 3-8 Days'
        if '2 5' in text or '2-5' in text or '2 TO 5' in text:
            return 'First Mile 2-5 Days'
        return 'First Mile 2-5 Days'
    return None

def extract_invoice_services(raw_df, mapping_config):
    service_col = mapping_config.get('mapping', {}).get('Shipping Service')
    if not service_col or service_col not in raw_df.columns:
        return []
    services = raw_df[service_col].dropna().astype(str).tolist()
    return services

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

def available_merchant_services(raw_df, mapping_config):
    services = extract_invoice_services(raw_df, mapping_config)
    normalized_invoice = {normalize_service_name(s) for s in services if s}
    canonical_map = {normalize_service_name(s): s for s in SERVICE_LEVELS}
    available = [canonical_map[n] for n in canonical_map if n in normalized_invoice]
    return available

def normalize_redo_label(label):
    if not label:
        return ""
    text = re.sub(r'\([^)]*\)', '', str(label))
    text = text.replace('Â', '').replace('®', '')
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.upper().strip()

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
    forced_on = set(REDO_FORCED_ON)
    selected = set(selected_redo_carriers or [])
    selected |= forced_on
    canonical_map = {normalize_redo_label(c): c for c in REDO_CARRIERS}

    header_row_idx, label_col, use_col = _find_pricing_section(ws, 'Redo Carriers')
    if header_row_idx is None:
        return

    stop_titles = {'MERCHANT CARRIERS', 'MERCHANT CARRIER', 'MERCHANT SERVICE LEVELS'}
    for row_idx, label_val in _iter_section_rows(ws, header_row_idx + 1, label_col, stop_titles):
        normalized = normalize_redo_label(label_val)
        canonical = None
        if normalized.startswith('UPS GROUND SAVER'):
            canonical = 'UPS Ground Saver'
        elif normalized.startswith('UPS GROUND'):
            canonical = 'UPS Ground'
        elif normalized.startswith('USPS MARKET'):
            canonical = 'USPS Market'
        else:
            canonical = canonical_map.get(normalized)

        target_cell = ws.cell(row_idx, use_col)
        target_cell.value = 'Yes' if canonical in selected else 'No'

def update_pricing_summary_merchant_carriers(ws, selected_redo_carriers):
    """Update Use in Pricing for Merchant Carriers section."""
    forced_on = set(REDO_FORCED_ON)
    selected = set(selected_redo_carriers or [])
    selected |= forced_on

    carrier_on = set()
    if 'USPS Market' in selected:
        carrier_on.add('USPS')
    if 'UPS Ground' in selected or 'UPS Ground Saver' in selected:
        carrier_on.add('UPS')
    if 'DHL' in selected:
        carrier_on.add('DHL')
    if 'Amazon' in selected:
        carrier_on.add('AMAZON')
    if 'FedEx' in selected:
        carrier_on.add('FEDEX')
    if any(c.startswith('First Mile') for c in selected):
        carrier_on.add('FIRST MILE')

    header_row_idx, label_col, use_col = _find_pricing_section(ws, 'Merchant Carriers')
    if header_row_idx is None:
        return

    stop_titles = {'MERCHANT SERVICE LEVELS', 'REDO CARRIERS'}
    for row_idx, label_val in _iter_section_rows(ws, header_row_idx + 1, label_col, stop_titles):
        normalized = normalize_redo_label(label_val)
        target_cell = ws.cell(row_idx, use_col)
        target_cell.value = 'Yes' if normalized in carrier_on else 'No'

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
    canonical_map = {normalize_service_name(s): s for s in SERVICE_LEVELS}

    stop_titles = {'REDO CARRIERS', 'MERCHANT CARRIERS'}
    header_row_idx, label_col, use_col, rows = _scan_section_rows(
        ws, 'Merchant Service Levels', stop_titles
    )
    if header_row_idx is None:
        return
    existing = {normalize_service_name(label): row for row, label in rows}

    label_cell = ws.cell(header_row_idx + 1, label_col)
    if isinstance(label_cell.value, str) and label_cell.value.startswith('='):
        unique_services = _unique_cleaned_services(normalized_df)
        for idx, service in enumerate(unique_services):
            row_idx = header_row_idx + 1 + idx
            target_cell = ws.cell(row_idx, use_col)
            normalized = normalize_service_name(service)
            target_cell.value = 'Yes' if normalized in selected_normalized else 'No'
        return

    for normalized, row_idx in existing.items():
        target_cell = ws.cell(row_idx, use_col)
        target_cell.value = 'Yes' if normalized in selected_normalized else 'No'

    missing = [name for name in canonical_map if name not in existing]
    if missing:
        app.logger.warning("Merchant Service Levels missing in template: %s", missing)

def detect_structure(csv_path):
    """Detect if invoice is zone-based or zip-based"""
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = [h.lower() for h in reader.fieldnames or []]
        # Check for zone column (case-insensitive, allow variants)
        zone_keywords = ['zone', 'shipment - zone', 'shipment-zone']
        has_zone = any(any(kw in h for h in headers) for kw in zone_keywords)
        return 'zone' if has_zone else 'zip'

def suggest_mapping(invoice_columns, standard_field):
    """Suggest best matching column for a standard field"""
    invoice_lower = [c.lower() for c in invoice_columns]
    field_lower = standard_field.lower()
    
    # Exact match
    for i, col in enumerate(invoice_lower):
        if field_lower in col or col in field_lower:
            return invoice_columns[i]
    
    # Partial matches
    keywords = {
        'Order Number': ['order', 'number', 'order_number'],
        'Order Date': ['date', 'shipped', 'order_date'],
        'Zip': ['zip', 'postal', 'postal_code'],
        'Weight (oz)': ['weight', 'oz', 'ounces'],
        'Shipping Carrier': ['carrier'],
        'Shipping Service': ['service', 'shipping_service'],
        'Package Height': ['height'],
        'Package Width': ['width'],
        'Package Length': ['length'],
        'Label Cost': ['cost', 'shipping_rate', 'rate', 'label']
    }
    
    if standard_field in keywords:
        for keyword in keywords[standard_field]:
            for i, col in enumerate(invoice_lower):
                if keyword in col:
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
    
    df = pd.read_csv(raw_csv_path, nrows=1)
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
    for field in STANDARD_FIELDS['required'] + STANDARD_FIELDS['optional']:
        suggested = suggest_mapping(columns, field)
        if suggested:
            suggestions[field] = suggested
    
    return render_template('screen2.html', 
                         job_id=job_id,
                         columns=columns,
                         suggestions=suggestions,
                         standard_fields=STANDARD_FIELDS)

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
        raw_csv_path = job_dir / 'raw_invoice.csv'
        file.save(raw_csv_path)
        
        # Detect structure
        structure = detect_structure(raw_csv_path)
        
        # Read CSV to get columns and suggest merchant name
        df = pd.read_csv(raw_csv_path, nrows=5)
        columns = list(df.columns)
        
        # Try to suggest merchant name from first row
        merchant_name_suggestion = None
        if len(df) > 0:
            # Look for common merchant name columns
            for col in ['Market Store Name', 'StoreName', 'Merchant Name', 'Store Name']:
                if col in df.columns and df[col].iloc[0]:
                    merchant_name_suggestion = str(df[col].iloc[0]).strip()
                    break
        
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
        mapping_config = data.get('mapping', {})
        
        if not job_id:
            return jsonify({'error': 'job_id required'}), 400
        
        if existing_customer and not merchant_id:
            return jsonify({'error': 'Merchant ID required for existing customers'}), 400
        
        job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
        if not job_dir.exists():
            return jsonify({'error': 'Job not found'}), 404
        
        # Validate required mappings
        required_fields = STANDARD_FIELDS['required']
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
            normalized_df['TWO_LETTER_COUNTRY_CODE'] = country_series.apply(normalize_country_code)
            normalized_df['FULL_COUNTRY_NAME'] = country_series.apply(normalize_country_name)
        else:
            normalized_df['TWO_LETTER_COUNTRY_CODE'] = ""
            normalized_df['FULL_COUNTRY_NAME'] = ""

        def safe_cell(value):
            if value is None:
                return ""
            if isinstance(value, float) and pd.isna(value):
                return ""
            return str(value).strip()

        def calc_country_code(row):
            code = safe_cell(row.get('TWO_LETTER_COUNTRY_CODE'))
            if code:
                return code
            name = safe_cell(row.get('FULL_COUNTRY_NAME'))
            if name:
                derived = normalize_country_code(name)
                return derived
            dest_zip = row.get('Zip')
            if extract_zip5(dest_zip):
                return 'US'
            return ""

        normalized_df['CALCULATED_TWO_LETTER_COUNTRY_CODE'] = normalized_df.apply(calc_country_code, axis=1)

        shipping_service_series = (
            normalized_df['Shipping Service']
            if 'Shipping Service' in normalized_df.columns
            else pd.Series([""] * len(normalized_df))
        )
        normalized_df['CLEANED_SHIPPING_SERVICE'] = shipping_service_series.apply(clean_shipping_service)
        normalized_df['SHIPPING_PRIORITY'] = normalized_df['CLEANED_SHIPPING_SERVICE'].apply(calculate_shipping_priority)

        def ounces_to_lbs(value):
            weight_oz = to_float(value)
            if weight_oz is None:
                return None
            return round(weight_oz / 16, 4)

        if 'Weight (oz)' in normalized_df.columns:
            normalized_df['WEIGHT_IN_LBS'] = normalized_df['Weight (oz)'].apply(ounces_to_lbs)
        else:
            normalized_df['WEIGHT_IN_LBS'] = None

        def calc_volume(row):
            length = to_float(row.get('Package Length'))
            width = to_float(row.get('Package Width'))
            height = to_float(row.get('Package Height'))
            if length is None or width is None or height is None:
                return None
            return length * width * height

        normalized_df['PACKAGE_DIMENSION_VOLUME'] = normalized_df.apply(calc_volume, axis=1)
        normalized_df['PACKAGE_SIZE_STATUS'] = normalized_df['PACKAGE_DIMENSION_VOLUME'].apply(classify_package_size)
        normalized_df['WEIGHT_CLASSIFICATION'] = normalized_df['WEIGHT_IN_LBS'].apply(classify_weight)

        origin_zip_value = ""
        if structure == 'zip':
            origin_zip_value = extract_zip5(origin_zip)
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

@app.route('/api/redo-carriers/<job_id>', methods=['GET', 'POST'])
def redo_carriers(job_id):
    """Get or save redo carrier selections"""
    job_dir = Path(app.config['UPLOAD_FOLDER']) / job_id
    if not job_dir.exists():
        return jsonify({'error': 'Job not found'}), 404

    if request.method == 'POST':
        data = request.json or {}
        selected = data.get('selected_carriers', [])
        selected = [c for c in selected if c in REDO_CARRIERS]
        for forced in REDO_FORCED_ON:
            if forced not in selected:
                selected.append(forced)
        with open(job_dir / 'redo_carriers.json', 'w') as f:
            json.dump({'selected_carriers': selected}, f)
        return jsonify({'success': True})

    mapping_file = job_dir / 'mapping.json'
    if not mapping_file.exists():
        return jsonify({'error': 'Mapping not found'}), 404

    with open(mapping_file, 'r') as f:
        mapping_config = json.load(f)

    raw_df = pd.read_csv(job_dir / 'raw_invoice.csv')
    detected = detect_redo_carriers(raw_df, mapping_config)
    selectable = [c for c in detected if c not in REDO_FORCED_ON]

    return jsonify({
        'detected_carriers': detected,
        'forced_on': REDO_FORCED_ON,
        'selectable_carriers': selectable,
        'default_selected': REDO_FORCED_ON
    })

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
        
        with open(job_dir / 'service_levels.json', 'r') as f:
            service_config = json.load(f)
        
        # Initialize progress
        progress_file = job_dir / 'progress.json'
        with open(progress_file, 'w') as f:
            json.dump({}, f)
        
        def run_generation():
            try:
                generate_rate_card(job_dir, mapping_config, service_config)
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

def generate_rate_card(job_dir, mapping_config, service_config):
    """Generate the rate card Excel file"""
    # Save output path first
    merchant_name = mapping_config.get('merchant_name', 'Merchant')
    output_filename = f"{merchant_name} - Rate Card.xlsx"
    output_path = job_dir / output_filename
    
    # Step 1: Normalize data
    write_progress(job_dir, 'normalize', True)
    
    # Copy template to output location first to preserve file structure
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
        'Weight (oz)': 'WEIGHT_IN_OZ',
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
    
    # Get selected services (normalized)
    selected_services = service_config.get('selected_services', [])
    normalized_selected = [normalize_service_name(s) for s in selected_services]
    
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
    
    write_cols = set()
    for excel_col in field_to_excel.values():
        if excel_col in header_to_col:
            write_cols.add(header_to_col[excel_col])
    if 'QUALIFIED' in header_to_col:
        write_cols.add(header_to_col['QUALIFIED'])
    if 'MERCHANT_ID' in header_to_col:
        write_cols.add(header_to_col['MERCHANT_ID'])
    origin_zip_value = ""
    if mapping_config.get('structure') == 'zip':
        origin_zip_value = extract_zip5(mapping_config.get('origin_zip'))
    if origin_zip_value and 'ORIGIN_ZIP_CODE' in header_to_col:
        write_cols.add(header_to_col['ORIGIN_ZIP_CODE'])

    if table_max_row and write_cols:
        for row_idx in range(start_row, table_max_row + 1):
            for col_idx in write_cols:
                if col_idx in formula_cols:
                    continue
                cell = ws.cell(row_idx, col_idx)
                if cell.value and str(cell.value).startswith('='):
                    continue
                cell.value = None

    # Write data starting from row 2
    for idx, row in normalized_df.iterrows():
        excel_row = start_row + idx
        
        # Write mapped fields
        for std_field, excel_col in field_to_excel.items():
            if std_field in normalized_df.columns and excel_col in header_to_col:
                col_idx = header_to_col[excel_col]
                # Only write if not a formula column
                if col_idx not in formula_cols:
                    cell = ws.cell(excel_row, col_idx)
                    if cell.value and str(cell.value).startswith('='):
                        continue
                    if std_field == 'ORIGIN_ZIP_CODE' and not origin_zip_value:
                        continue
                    value = row[std_field]
                    # Handle NaN values
                    if pd.isna(value):
                        value = None
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
                        elif excel_col in [
                            'WEIGHT_IN_OZ',
                            'WEIGHT_IN_LBS',
                            'PACKAGE_HEIGHT',
                            'PACKAGE_WIDTH',
                            'PACKAGE_LENGTH',
                            'PACKAGE_DIMENSION_VOLUME',
                            'LABEL_COST'
                        ]:
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
                shipping_service = str(row.get('Shipping Service', ''))
                normalized_service = normalize_service_name(shipping_service)
                is_qualified = normalized_service in normalized_selected
                ws.cell(excel_row, col_idx, is_qualified)

    # Update Pricing & Summary redo carrier selections
    if 'Pricing & Summary' in wb.sheetnames:
        selected_redo = redo_config.get('selected_carriers', [])
        selected_services = service_config.get('selected_services', [])
        summary_ws = wb['Pricing & Summary']
        update_pricing_summary_redo_carriers(summary_ws, selected_redo)
        update_pricing_summary_merchant_carriers(summary_ws, selected_redo)
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
            'redirect_url': f'/ready?job_id={job_id}',
            'progress': progress
        })
    
    return jsonify({
        'ready': False,
        'progress': progress
    })

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
