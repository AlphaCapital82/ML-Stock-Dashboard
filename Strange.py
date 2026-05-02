import pandas as pd
import yfinance as yf
from pathlib import Path
import time

INPUT_FILE = "tickers.txt"
OUTPUT_FILE = "fundamental_growth_2025.csv"

tickers = [
    line.strip().upper()
    for line in Path(INPUT_FILE).read_text().splitlines()
    if line.strip()
]

def get_value(statement: pd.DataFrame, possible_rows, year: int):
    """
    Finds value from yfinance income statement for a given fiscal year.
    Columns are usually fiscal period end dates.
    """
    if statement is None or statement.empty:
        return None

    year_cols = [col for col in statement.columns if pd.to_datetime(col).year == year]

    if not year_cols:
        return None

    col = year_cols[0]

    for row in possible_rows:
        if row in statement.index:
            value = statement.loc[row, col]
            if pd.notna(value):
                return float(value)

    return None

results = []

for i, ticker in enumerate(tickers, start=1):
    print(f"{i}/{len(tickers)}: {ticker}")

    try:
        t = yf.Ticker(ticker)

        income = t.get_income_stmt(freq="yearly", pretty=False)

        revenue_2025 = get_value(income, ["Total Revenue", "TotalRevenue"], 2025)
        revenue_2024 = get_value(income, ["Total Revenue", "TotalRevenue"], 2024)

        ebit_2025 = get_value(income, ["EBIT", "Operating Income", "OperatingIncome"], 2025)
        ebit_2024 = get_value(income, ["EBIT", "Operating Income", "OperatingIncome"], 2024)

        revenue_growth_2025 = None
        ebit_growth_2025_raw = None
        ebit_growth_2025_clean = None

        if revenue_2025 is not None and revenue_2024 not in [None, 0]:
            revenue_growth_2025 = revenue_2025 / revenue_2024 - 1

        if ebit_2025 is not None and ebit_2024 not in [None, 0]:
            ebit_growth_2025_raw = ebit_2025 / ebit_2024 - 1

            # Cleaner factor-model version:
            # Only use EBIT growth when the base year EBIT is positive.
            if ebit_2024 > 0:
                ebit_growth_2025_clean = ebit_growth_2025_raw

        status = "OK"

        if revenue_2025 is None or revenue_2024 is None:
            status = "Missing revenue data"
        elif ebit_2025 is None or ebit_2024 is None:
            status = "Missing EBIT/operating income data"
        elif ebit_2024 <= 0:
            status = "EBIT base <= 0; raw EBIT growth may be misleading"

        results.append({
            "ticker": ticker,
            "revenue_2024": revenue_2024,
            "revenue_2025": revenue_2025,
            "revenue_growth_2025": revenue_growth_2025,
            "revenue_growth_2025_pct": None if revenue_growth_2025 is None else revenue_growth_2025 * 100,
            "ebit_2024": ebit_2024,
            "ebit_2025": ebit_2025,
            "ebit_growth_2025_raw": ebit_growth_2025_raw,
            "ebit_growth_2025_raw_pct": None if ebit_growth_2025_raw is None else ebit_growth_2025_raw * 100,
            "ebit_growth_2025_clean": ebit_growth_2025_clean,
            "ebit_growth_2025_clean_pct": None if ebit_growth_2025_clean is None else ebit_growth_2025_clean * 100,
            "status": status
        })

    except Exception as e:
        results.append({
            "ticker": ticker,
            "revenue_2024": None,
            "revenue_2025": None,
            "revenue_growth_2025": None,
            "revenue_growth_2025_pct": None,
            "ebit_2024": None,
            "ebit_2025": None,
            "ebit_growth_2025_raw": None,
            "ebit_growth_2025_raw_pct": None,
            "ebit_growth_2025_clean": None,
            "ebit_growth_2025_clean_pct": None,
            "status": f"Error: {e}"
        })

    time.sleep(0.5)

df = pd.DataFrame(results)
df.to_csv(OUTPUT_FILE, index=False)

print(f"Done. Saved to {OUTPUT_FILE}")