# Self-Serve Rate Card Generator

A Flask application for generating rate cards from raw carrier invoice CSV files.

## Features

- **4-Step UI Flow**: Upload → Mapping → Service Levels → Generate
- **Zone-based and Zip-based Support**: Automatically detects invoice structure
- **Field Mapping**: Intelligent column mapping with suggestions
- **Service Level Selection**: Select services to qualify against
- **Excel Generation**: Generates rate cards using template, preserving formulas
- **Progress Tracking**: Real-time progress updates during generation

## Local Setup

### Prerequisites

- Python 3.8 or higher
- pip

### Install and run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Windows (PowerShell):

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Run tests (if present):

```bash
pytest
```

The application will be available at `http://localhost:5000`

## Usage

1. **Upload Raw Invoice**: Upload a CSV file containing carrier invoice data
2. **Map Fields**: Map invoice columns to standard fields (required fields must be mapped)
3. **Select Service Levels**: Choose which shipping services to qualify against
4. **Generate Rate Card**: The system generates an Excel rate card matching the template

## Testing

Run tests with pytest:

```bash
pytest
```

With coverage:

```bash
pytest --cov=. --cov-report=html
```

## Project Structure

```
.
├── app.py                 # Flask application
├── requirements.txt       # Python dependencies
├── templates/             # HTML templates
│   └── index.html
├── static/                # Static assets
│   ├── css/
│   │   └── style.css
│   └── js/
│       └── app.js
├── runs/                  # Job directories (auto-created)
└── tests/                 # Test files
```

## API Endpoints

- `POST /api/upload` - Upload invoice CSV file
- `POST /api/mapping` - Save field mappings
- `POST /api/service-levels` - Save selected service levels
- `POST /api/generate` - Generate rate card
- `GET /api/status/<job_id>` - Get job status
- `GET /download/<job_id>/rate-card` - Download generated rate card
- `GET /download/<job_id>/raw-invoice` - Download original invoice
- `GET /download/<job_id>/normalized` - Download normalized CSV

## Notes

- Job artifacts are stored in `./runs/<job_id>/`
- Old runs (>24 hours) are automatically cleaned up
- Excel formulas in columns 24-29 are preserved during generation
- Service names are normalized for matching (removes punctuation, collapses whitespace)

## License

MIT
