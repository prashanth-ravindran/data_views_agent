# Data Views Agent

Streamlit demo for querying large multi-sheet Excel workbooks with a Gemini-driven planning step and SQLite-backed filtering.

## Local setup
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Build the Maharashtra office manifest
```bash
.venv/bin/python scripts/build_office_manifest.py
```

## Generate synthetic data
```bash
.venv/bin/python scripts/generate_synthetic_data.py --output artifacts/generated/maharashtra_demo.xlsx --rows 300000 --seed 7
```

## Run the demo
```bash
.venv/bin/streamlit run app.py
```
