# Self-Serve Rate Card

## Overview
A Flask-based web application for shipping rate card analysis. The app processes shipping invoice data and rate card templates to provide carrier rate analysis and recommendations.

## Project Structure
```
.
├── app.py                      # Main Flask application (5000+ lines)
├── usps_zones.py              # USPS zone lookup utilities
├── requirements.txt           # Python dependencies
├── templates/                 # Jinja2 HTML templates
│   ├── entry.html            # Entry portal
│   ├── screen1.html          # Upload flow
│   └── screen2.html          # Mapping flow
├── static/                   # Static assets
│   ├── redo-logo.png         # Brand logo
│   └── styles.css            # Styling
├── scripts/                  # Utility scripts
├── tests/                    # Test suite
│   └── test_app.py          # Pytest tests
└── runs/                     # Job artifacts (auto-created)
```

## Running the Application
- The app runs on port 5000
- Run with: `gunicorn app:app --workers 4 --timeout 120 --bind 0.0.0.0:5000 --preload --log-level info`
- The Replit workflow launches the same command so the server runs in production-ready mode. Use `python app.py` only for quick local validation; production deployments should rely on Gunicorn.
- Keep the preview open for a minute after deploy to keep workers warm instead of cold-starting
- Open in browser: Navigate to the preview URL

## Key Dependencies
- Flask 3.0.0 - Web framework
- pandas - Data processing
- openpyxl - Excel file handling
- Werkzeug - WSGI utilities

## Dashboard Caching Architecture
The dashboard uses 100% Python calculations for instant loading with no external dependencies.

### Performance
- **Cached load**: ~0.1 seconds (JSON read)
- **Fresh calculation**: ~0.2 seconds (rate tables preloaded at startup)
- **No external deps**: No LibreOffice or Excel automation required

### Startup Optimization
At app startup, the following are preloaded into memory:
- Template workbook (BytesIO buffer)
- Rate tables (parsed from Excel once)
- Pricing controls (parsed from Excel once)

This eliminates the 40+ second Excel parsing delay on first dashboard load.

### Cache Files (per job in `runs/<job_id>/`)
- `dashboard_breakdown.json` - Pre-computed per-carrier metrics
- `dashboard_summary.json` - Summary metrics by carrier selection

### Cache Flow
1. **On Generate**: Python computes all metrics instantly (~0.2 seconds)
2. **Cache Write**: Results saved to JSON files
3. **On Dashboard Load**: Fast JSON read, instant display

### Key Functions
- `_precompute_dashboard_metrics()` - Orchestrates metric calculation
- `_calculate_all_carriers_batch()` - Batch calculation for all carriers in one pass
- `_calculate_summary_from_context()` - Summary metrics using pre-loaded context

### Technical Notes
- Pure Python implementation - no LibreOffice, Excel automation, or external APIs
- Calculations replicate Excel formula logic using pandas
- Rate tables and pricing controls are cached with file mtime checks for freshness

## Phase Tracking & Progress API
The app tracks timestamps for each major processing phase to enable accurate ETA calculations.

### Phase Flow
1. **upload** - File upload complete
2. **mapping** - Field mapping saved
3. **generation_start** - Excel generation initiated
4. **qualification** - Data normalization/qualification
5. **write_template** - Writing data to Excel template
6. **saving** - Saving Excel file
7. **excel_complete** - Generation finished

### Status API
`GET /api/status/<job_id>` returns:
- `phase_timestamps` - ISO timestamps for each completed phase
- `phase_durations` - Seconds between each phase
- `elapsed_seconds` - Total time since job started
- `eta_seconds_remaining` - Estimated time to completion

### Gunicorn Configuration
- Uses `--preload` flag to parse templates/rate tables once before forking workers
- 4 workers share preloaded data in memory
- Startup logs show `[PRELOAD]` messages for each cached resource
- Rate tables and pricing controls stay in memory rather than re-reading Excel
- Generation time: ~50-52 seconds (24s template parse + 25s workbook save)

### Dashboard Carrier Toggle Behavior
- **Summary**: Recalculates based on selected carriers
- **Breakdown**: Shows ALL carriers, hides deselected ones (no recalculation)
- Carrier toggling is fast - only summary API call, breakdown re-renders locally

## Carrier Eligibility

### Eligibility Requirements
Eligibility requires BOTH ZIP code whitelist AND volume thresholds:

### UniUni Eligibility
- ZIP code must be in UniUni service area whitelist AND
- Volume: >= 300 orders per workday (annual_orders / working_days_per_year)
- Default working days per year: 260 (configurable via WORKING_DAYS_PER_YEAR env var)
- Threshold: 78,000+ annual orders
- Explicit overrides in mapping_config take precedence if set

### Amazon Eligibility
- ZIP code must be in Amazon service area whitelist AND
- Volume: >= 150 orders per day (annual_orders / 365)
- Threshold: 54,750+ annual orders
- Explicit overrides in mapping_config take precedence if set

### Eligibility Sync
- When annual orders update, explicit overrides are cleared and eligibility recalculates
- Amazon/UniUni are automatically added/removed from redo carriers and merchant pricing
- No DHL carrier support (removed from all carrier lists)

## Notes
- The template file `#New Template - Rate Card.xlsx` must be in the project root
- Job artifacts are stored in `./runs/<job_id>/`
- Old runs (>24 hours) are automatically cleaned up
