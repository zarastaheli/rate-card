# Changes Summary

## What Changed

This project was built from scratch to implement a complete rate card generation system with the following components:

### Backend (Flask)
- **File Upload API** (`/api/upload`): Accepts CSV files via multiple field names (invoice, invoice_file, invoice_csv)
- **Structure Detection**: Automatically detects zone-based vs zip-based invoices
- **Field Mapping API** (`/api/mapping`): Handles field mapping with validation
- **Service Levels API** (`/api/service-levels`): Stores selected service levels
- **Generation API** (`/api/generate`): Generates Excel rate cards from template
- **Download Endpoints**: Provides downloads for rate card, raw invoice, and normalized CSV
- **Auto-cleanup**: Removes job artifacts older than 24 hours

### Frontend
- **4-Step UI Flow**: Complete multi-step interface matching design specifications
- **File Upload**: Drag-and-drop with visual feedback
- **Field Mapping**: Interactive mapping table with suggestions and validation
- **Service Level Selection**: Searchable service list with checkboxes
- **Progress Tracking**: Loading screen with step-by-step progress indicators
- **Download Page**: Final page with download links and supporting files

### Excel Generation
- **Template Preservation**: Preserves all formula columns (24-29) in the template
- **Data Mapping**: Maps normalized data to Excel columns by header name
- **QUALIFIED Column**: Writes TRUE/FALSE based on service level matching with normalization
- **Zone Handling**: Properly handles zone-based and zip-based structures
- **Date Formatting**: Converts dates to Excel datetime format
- **Zip Code Extraction**: Extracts 5-digit zip codes from various formats

### Service Normalization
- Removes punctuation and special characters (including ® and Â)
- Collapses whitespace
- Converts to uppercase
- Ensures consistent matching across different encodings

### Testing
- Structure detection tests (zone vs zip)
- File upload field name acceptance tests
- Service normalization tests
- Excel formula preservation tests
- QUALIFIED column writing tests

## Why These Changes

1. **Template-Based Generation**: Uses the provided Excel template to ensure consistency with existing rate cards
2. **Formula Preservation**: Critical for maintaining Excel calculations - formulas must remain intact
3. **Service Normalization**: Handles encoding issues (Â® vs ®) that occur in real-world data
4. **Zone vs Zip Detection**: Supports both invoice structures automatically
5. **Field Mapping**: Flexible mapping system handles various invoice column naming conventions
6. **Progress Tracking**: Provides user feedback during long-running operations
7. **Auto-cleanup**: Prevents disk space issues from accumulating job artifacts

## Files Created

- `app.py`: Main Flask application
- `requirements.txt`: Python dependencies
- `templates/index.html`: Main UI template
- `static/css/style.css`: Styling
- `static/js/app.js`: Frontend JavaScript
- `tests/test_app.py`: Test suite
- `README.md`: Documentation
- `.gitignore`: Git ignore rules
- `CHANGES.md`: This file

## Files Deleted

No files were deleted as this was a new project. The `.gitignore` file prevents future accumulation of:
- `__pycache__/` directories
- `*.pyc` files
- `.DS_Store` files
- `*.log` files
- `*.bak` files
- `runs/` directory contents (job artifacts)

These are safe to ignore as they are:
- Python bytecode (auto-generated)
- OS metadata files
- Temporary log files
- Backup files
- Runtime-generated job directories

