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
The dashboard uses a hybrid approach for accuracy and speed:

### Hybrid Computation Strategy
- **Summary metrics**: Computed via LibreOffice recalculation (Excel-accurate, ~2 min one-time)
- **Per-carrier metrics**: Computed via Python (fast, responsive UI)
- **Result**: Summary shows exact Excel values ($11,454.81 spread, 45.61% winnable)

### Cache Files (per job in `runs/<job_id>/`)
- `dashboard_breakdown.json` - Pre-computed per-carrier metrics (Python)
- `dashboard_summary.json` - Summary metrics by carrier selection (LibreOffice)

### Cache Flow
1. **On Generate**: LibreOffice recalculates Excel file (~2 min)
2. **Hash Computed**: SHA256 hash computed AFTER LibreOffice modifies file
3. **Metrics Cached**: Summary from Excel cells, per-carrier from Python
4. **On Dashboard Load**: Fast JSON read only, instant display
5. **If Cache Missing**: Return `pending=true`, trigger background job

### Cache Invalidation
- Uses SHA256 hash (`source_hash`) computed AFTER LibreOffice recalculation
- Hash must be computed after file modification to avoid false invalidation

### Key Functions
- `_precompute_dashboard_metrics()` - Hybrid: LibreOffice summary + Python per-carrier
- `_recalculate_excel_with_libreoffice()` - LibreOffice recalc for accurate formulas
- `_read_metrics_from_excel_cells()` - Read summary from Excel cells C5-C12
- `_read_dashboard_cache()` / `_write_dashboard_cache()` - JSON cache I/O

### Technical Notes
- openpyxl cannot evaluate Excel formulas (writes them but can't compute)
- LibreOffice is required to evaluate complex Excel functions like XLOOKUP
- Power Automate integration exists but requires Microsoft 365 premium license

## Notes
- The template file `#New Template - Rate Card.xlsx` must be in the project root
- Job artifacts are stored in `./runs/<job_id>/`
- Old runs (>24 hours) are automatically cleaned up
