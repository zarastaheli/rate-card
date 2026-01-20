# Quick Setup Guide

## One-Command Setup

```bash
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt && python app.py
```

On Windows:
```bash
python -m venv venv && venv\Scripts\activate && pip install -r requirements.txt && python app.py
```

## Step-by-Step

1. **Create virtual environment:**
   ```bash
   python3 -m venv venv
   ```

2. **Activate virtual environment:**
   ```bash
   source venv/bin/activate  # macOS/Linux
   # OR
   venv\Scripts\activate  # Windows
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the application:**
   ```bash
   python app.py
   ```

5. **Open in browser:**
   Navigate to `http://localhost:5000`

## Running Tests

```bash
pytest tests/test_app.py -v
```

## Project Structure

```
.
├── app.py                      # Main Flask application
├── requirements.txt            # Python dependencies
├── README.md                   # Full documentation
├── CHANGES.md                  # What changed and why
├── templates/
│   ├── entry.html             # Entry portal
│   ├── screen1.html           # Upload flow
│   └── screen2.html           # Mapping flow
├── static/
│   ├── redo-logo.png          # Brand logo
│   └── styles.css             # Styling
├── tests/
│   └── test_app.py            # Test suite
└── runs/                       # Job artifacts (auto-created)
```

## Notes

- The template file `New Template - Rate Card.xlsx` must be in the project root
- Job artifacts are stored in `./runs/<job_id>/`
- Old runs (>24 hours) are automatically cleaned up
- The app runs on port 5000 by default
