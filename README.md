# Presto → Databricks SQL Converter

Opinionated, scriptable converter that turns Presto SQL into Databricks SQL (DBSQL). Includes an interactive Streamlit app, batch processing, and safeguards for tricky constructs (EXECUTE/USING, PREPARE, aliases, regexp quirks).

## Features

🔹 1. Interactive Conversion
- Convert individual queries in real time via Streamlit.
- See three buckets: Converted, Already Compatible, Errors.
- Download converted/compatible/error outputs.

🔹 2. Batch Jobs
- Bulk convert `.sql`/`.txt` files (or zip bundles) via the UI.
- Safe statement splitting; each query classified as converted / compatible / errored.
- Outputs per input set: `*_converted.sql`, `*_compatible.sql`, `*_errors.sql`.

🔹 3. Function Compatibility
- Uses `sqlglot` to map most Presto functions, date/time literals, and intervals to DBSQL.
- Helper layer adds targeted fixes (regexp_replace, alias/identifier normalization, wrapper unwrapping).
- For any unmapped Presto-specific function, add a small rule in `src/utils/helper_functions.py` or a custom `sqlglot` transform.

## 🚀 Quick Start

### Web App DEployment

1. Clone into Databricks Git Folder
Clone this repository into a Databricks Git-enabled workspace folder.

2. Deploy the app
In the repo root there is a notebook called app_deployer.
Open it in Databricks and run all cells. This will automatically:

Install dependencies
Deploy the Streamlit app into your workspace
Make the app available for immediate use

### Local System Deployment

1. Clone & install
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

2. Run the Streamlit app
```bash
source .venv/bin/activate
streamlit run app.py
```
- Tab “Convert text”: paste SQL, view/download outputs.
- Tab “Batch convert file”: upload .sql/.txt or .zip; get zipped outputs.


## Project Structure
```
├── app.py                 # Streamlit app (interactive + batch)
├── converter.py           # Core conversion pipeline
├── requirements.txt       # Dependencies (sqlglot, streamlit, etc.)
├── src
│   ├── notebooks/
│   │   └── presto_to_dbsql.py   # Notebook wrapper for conversion
│   └── utils/
│       └── helper_functions.py  # Pre/post-parse helpers and fixes
├── test/                  # Unit tests
└── README.md
```

## Tips / Limitations
- If a query fails, inspect `errors.sql` for the cleaned candidate emitted by `sqlglot`.
- Vendor-specific functions may need a small rule in `helper_functions.py`.
- If you must keep an outer wrapper (e.g., `EXECUTE IMMEDIATE`), add a post-step to re-wrap the converted inner SELECT.
