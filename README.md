# ML Stock Dashboard

Python and Streamlit project for a stock-analysis workflow:

- Google Sheets / SheetsFinance ingestion helpers
- cleaning and diagnostics
- feature transformation
- linear baseline and XGBoost models
- Streamlit dashboards for model results, individual stocks, financial ratios, and portfolio suggestions

## Safety Note

This repository intentionally excludes local/private files:

- Google service-account JSON credentials
- `.env`
- local Excel workbooks
- generated model output folders
- cached Google Sheet data
- virtual environments and IDE files

The dashboard expects output files to exist locally after running the pipeline, or after you provide your own data files in the expected folders.

## Setup

Create a virtual environment and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Running The Main Dashboard

```powershell
streamlit run streamlit_dashboard.py
```

The dashboard reads files from folders such as:

```text
5_output/
4_ready_for_analysis/
macro_verdict.txt
```

If those files are missing, some dashboard sections will be empty.

## Pipeline

Typical local pipeline order:

```text
sync_sheetsfinance_outputs.py
cleaning_script.py
diagnostics.py
XGBoost - feature - tranformation.py
linear_baseline.py
XGBoost - main code.py
```

`run_ml_pipeline.bat` runs the main local workflow.

## Google Sheets / SheetsFinance

The SheetsFinance scripts expect a Google service-account credential file locally, but this file is not included in the repository.

Create your own credential file and place it where the scripts expect it, or update the path in the scripts:

```text
0_ingestion/stock-ingestion-494417-17cbf0e7891b.json
```

Do not commit real credential files.

## Look-Ahead Bias

See:

```text
lookahead_bias_model_plan.txt
```

The intended clean modeling design is:

```text
2024 fundamentals -> returns_2025
2025 fundamentals -> predicted_returns_2026
```

## Public Demo Data

This public repo does not include the full local dataset. To make the dashboard fully runnable for other users, add a small sanitized sample dataset in the same folder structure used by the dashboard.
