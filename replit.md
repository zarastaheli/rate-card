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
The dashboard uses fast Python calculations for instant loading:

### Computation Strategy
- **All metrics**: Computed via Python (instant, ~0.1 second)
- **Accuracy**: Within ~10% of Excel formula values
- **Speed priority**: Dashboard loads instantly instead of waiting 2+ minutes

### Cache Files (per job in `runs/<job_id>/`)
- `dashboard_breakdown.json` - Pre-computed per-carrier metrics
- `dashboard_summary.json` - Summary metrics by carrier selection

### Cache Flow
1. **On Generate**: Python computes all metrics (~1 second total)
2. **On Dashboard Load**: Fast JSON read only, instant display
3. **If Cache Missing**: Return `pending=true`, trigger background computation

### Key Functions
- `_precompute_dashboard_metrics()` - Python-based fast calculation
- `_calculate_metrics_fast()` - Core metric calculation using pandas
- `_read_dashboard_cache()` / `_write_dashboard_cache()` - JSON cache I/O

### Technical Notes
- LibreOffice recalculation code exists but is disabled for speed
- Power Automate integration exists but requires Microsoft 365 premium license

## Notes
- The template file `#New Template - Rate Card.xlsx` must be in the project root
- Job artifacts are stored in `./runs/<job_id>/`
- Old runs (>24 hours) are automatically cleaned up
