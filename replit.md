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
The dashboard uses progressive loading: fast Python estimates first, then accurate Excel values.

### Computation Strategy
- **Instant load**: Python calculations (~0.1 second) for immediate display
- **Background refinement**: LibreOffice recalculates Excel formulas (~2 min, async)
- **Auto-update**: Cache silently updated with exact values when LibreOffice completes
- **Result**: Users see estimates immediately, accurate values on next visit

### Cache Files (per job in `runs/<job_id>/`)
- `dashboard_breakdown.json` - Pre-computed per-carrier metrics
- `dashboard_summary.json` - Summary metrics by carrier selection

### Cache Flow
1. **On Generate**: Python computes all metrics instantly (~1 second)
2. **Background**: LibreOffice refinement starts in async thread
3. **On Dashboard Load**: Fast JSON read, instant display
4. **When LibreOffice Done**: Cache updated with accurate Excel values

### Key Functions
- `_precompute_dashboard_metrics()` - Fast Python + background LibreOffice
- `_calculate_metrics_fast()` - Core metric calculation using pandas
- `_recalculate_excel_with_libreoffice()` - Accurate formula evaluation
- `_read_metrics_from_excel_cells()` - Read summary from Excel cells C5-C12

### Technical Notes
- Python estimates are within ~10% of Excel formula values
- LibreOffice runs in background thread, doesn't block user
- Power Automate integration exists but requires Microsoft 365 premium license

## Notes
- The template file `#New Template - Rate Card.xlsx` must be in the project root
- Job artifacts are stored in `./runs/<job_id>/`
- Old runs (>24 hours) are automatically cleaned up
