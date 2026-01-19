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
│   └── index.html            # Main UI template
├── static/                   # Static assets
│   ├── css/style.css         # Styling
│   └── js/app.js            # Frontend logic
├── scripts/                  # Utility scripts
├── tests/                    # Test suite
│   └── test_app.py          # Pytest tests
└── runs/                     # Job artifacts (auto-created)
```

## Running the Application
- The app runs on port 5000
- Run with: `python app.py`
- Open in browser: Navigate to the preview URL

## Key Dependencies
- Flask 3.0.0 - Web framework
- pandas - Data processing
- openpyxl - Excel file handling
- Werkzeug - WSGI utilities

## Dashboard Caching Architecture
The dashboard uses a fast JSON-based caching system to avoid slow Excel file operations:

### Cache Files (per job in `runs/<job_id>/`)
- `dashboard_breakdown.json` - Pre-computed per-carrier metrics
- `dashboard_summary.json` - Aggregated metrics by carrier selection

### Cache Flow
1. **On Generate**: Pre-compute all metrics during rate card generation, save to JSON
2. **On Dashboard Load**: Fast JSON read only, no Excel loading
3. **If Cache Missing**: Return `pending=true`, trigger background job with 120s timeout
4. **On Carrier Selection Change**: Aggregate from cached per-carrier metrics (fast, no re-evaluation)

### Cache Invalidation
- Uses SHA256 hash (`source_hash`) of rate card file instead of mtime
- Cache is invalidated when rate card file changes

### Key Functions
- `_precompute_dashboard_metrics()` - Pre-computes all metrics during generation
- `_read_dashboard_cache()` / `_write_dashboard_cache()` - JSON cache I/O
- `_aggregate_metrics_from_carriers()` - Fast aggregation from per-carrier metrics
- `_start_background_cache_job()` - Background computation with timeout

## Notes
- The template file `#New Template - Rate Card.xlsx` must be in the project root
- Job artifacts are stored in `./runs/<job_id>/`
- Old runs (>24 hours) are automatically cleaned up
