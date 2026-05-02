import os
import time
import random
from datetime import date, timedelta
from pathlib import Path
import pandas as pd
from pytrends.request import TrendReq

INPUT_FILE = Path("0_needs_processing") / "tickerlist.xlsx"

END_DATE = date.today().isoformat()
START_DATE = (date.today() - timedelta(days=730)).isoformat()
TIMEFRAME = f"{START_DATE} {END_DATE}"

GEO = ""  # "" = WORLD. Use "US" or "NO" if you want a country filter.

OUT_CSV = Path("0_needs_processing") / "google_trends_progress.csv"
OUTPUT_COLUMN = "google_trends"


def read_universe(path: str) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path, encoding="utf-8-sig", sep=None, engine="python", on_bad_lines="warn")
    df.columns = [c.replace("\ufeff", "").strip() for c in df.columns]
    normalized_columns = {c.strip().casefold(): c for c in df.columns}

    rename_map = {}
    if "ticker" in normalized_columns and "TICKERS" not in df.columns:
        rename_map[normalized_columns["ticker"]] = "TICKERS"
    if "tickers" in normalized_columns and "TICKERS" not in df.columns:
        rename_map[normalized_columns["tickers"]] = "TICKERS"
    if "name" in normalized_columns and "NAME" not in df.columns:
        rename_map[normalized_columns["name"]] = "NAME"
    if "term_final" in normalized_columns and "TERM_FINAL" not in df.columns:
        rename_map[normalized_columns["term_final"]] = "TERM_FINAL"
    if rename_map:
        df = df.rename(columns=rename_map)

    if "TICKERS" not in df.columns:
        raise ValueError(f"Missing column TICKERS. Found: {list(df.columns)}")

    if "TERM_FINAL" not in df.columns:
        df["TERM_FINAL"] = df["NAME"] if "NAME" in df.columns else df["TICKERS"]
    if "NAME" not in df.columns:
        df["NAME"] = ""

    df["TICKERS"] = df["TICKERS"].astype(str).str.strip()
    df["TERM_FINAL"] = df["TERM_FINAL"].astype(str).str.strip()
    df["NAME"] = df["NAME"].astype(str).str.strip()

    df = df[df["TICKERS"] != ""].drop_duplicates(subset=["TICKERS"], keep="first").copy()
    return df


def load_done_tickers(out_csv: str) -> set[str]:
    if not os.path.exists(out_csv):
        return set()
    try:
        prev = pd.read_csv(out_csv, encoding="utf-8-sig")
        if "TICKERS" not in prev.columns:
            return set()
        return set(prev["TICKERS"].astype(str).str.strip().tolist())
    except Exception:
        return set()


def trailing_one_year_growth(series: pd.Series) -> tuple[float | None, float | None, float | None]:
    series = pd.to_numeric(series, errors="coerce").dropna()
    if series.empty:
        return None, None, None

    latest_date = series.index.max()
    recent_start = latest_date - pd.DateOffset(years=1)
    prior_start = latest_date - pd.DateOffset(years=2)

    recent_avg = series[(series.index > recent_start) & (series.index <= latest_date)].mean()
    prior_avg = series[(series.index > prior_start) & (series.index <= recent_start)].mean()

    if pd.isna(recent_avg) or pd.isna(prior_avg) or prior_avg == 0:
        return None, None if pd.isna(recent_avg) else float(recent_avg), None if pd.isna(prior_avg) else float(prior_avg)

    growth = (recent_avg - prior_avg) / prior_avg
    return float(growth), float(recent_avg), float(prior_avg)


def append_rows_to_csv(rows: list[dict], out_csv: str):
    if not rows:
        return
    df = pd.DataFrame(rows)
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    write_header = not os.path.exists(out_csv)
    df.to_csv(out_csv, mode="a", header=write_header, index=False, encoding="utf-8-sig")


def merge_progress_into_workbook(workbook_path: Path, progress_csv: Path) -> int:
    if not progress_csv.exists():
        return 0

    workbook_df = pd.read_excel(workbook_path)
    workbook_df.columns = [c.replace("\ufeff", "").strip() for c in workbook_df.columns]
    ticker_column = next((c for c in workbook_df.columns if c.casefold() == "ticker"), None)
    if ticker_column is None:
        raise ValueError(f"Missing ticker column in {workbook_path}. Found: {list(workbook_df.columns)}")

    progress_df = pd.read_csv(progress_csv, encoding="utf-8-sig")
    if "TICKERS" not in progress_df.columns or OUTPUT_COLUMN not in progress_df.columns:
        return 0

    progress_df = progress_df.drop_duplicates(subset=["TICKERS"], keep="last")
    trends_by_ticker = progress_df.set_index("TICKERS")[OUTPUT_COLUMN]

    workbook_df[OUTPUT_COLUMN] = workbook_df[ticker_column].astype(str).str.strip().map(trends_by_ticker)
    workbook_df.to_excel(workbook_path, index=False)
    return int(workbook_df[OUTPUT_COLUMN].notna().sum())


def main():
    universe = read_universe(INPUT_FILE)
    done = load_done_tickers(OUT_CSV)

    items = universe.to_dict("records")
    items = [r for r in items if r["TICKERS"] not in done]

    total_all = len(universe)
    remaining = len(items)
    print(f"Total tickers: {total_all}. Remaining (not in output yet): {remaining}.")

    pytrends = TrendReq(hl="en-US", tz=0)

    batch_size = 5
    cooldown_s = 45.0
    cooldown_min = 20.0
    cooldown_max = 180.0
    success_streak = 0

    idx = 0
    while idx < len(items):
        batch = items[idx: idx + batch_size]

        terms = [r.get("TERM_FINAL", "").strip() for r in batch]
        terms = [t for t in terms if t]

        if not terms:
            idx += batch_size
            continue

        try:
            pytrends.build_payload(terms, timeframe=TIMEFRAME, geo=GEO)
            ts = pytrends.interest_over_time()

            out_rows = []
            for r in batch:
                ticker = r["TICKERS"]
                name = r.get("NAME", "")
                term = r.get("TERM_FINAL", "").strip()

                if ts is None or ts.empty or term not in ts.columns:
                    growth, recent_avg, prior_avg = None, None, None
                else:
                    growth, recent_avg, prior_avg = trailing_one_year_growth(ts[term])

                out_rows.append({
                    "TICKERS": ticker,
                    "NAME": name,
                    "TERM_FINAL": term,
                    OUTPUT_COLUMN: growth,
                    "GOOGLE_TRENDS_RECENT_AVG": recent_avg,
                    "GOOGLE_TRENDS_PRIOR_AVG": prior_avg,
                    "WINDOW_START": START_DATE,
                    "WINDOW_END": END_DATE,
                    "GEO": GEO if GEO else "WORLD"
                })

                done.add(ticker)

            append_rows_to_csv(out_rows, OUT_CSV)
            populated = merge_progress_into_workbook(INPUT_FILE, OUT_CSV)

            idx += batch_size
            success_streak += 1

            processed = len(done)
            print(
                f"Processed {processed}/{total_all} | {OUTPUT_COLUMN} populated={populated} | "
                f"batch={batch_size} | cooldown={cooldown_s:.1f}s"
            )

            if success_streak >= 6 and batch_size < 5:
                batch_size += 1
                success_streak = 0

            cooldown_s = max(cooldown_min, cooldown_s * 0.90)

            time.sleep(cooldown_s + random.uniform(0.0, 1.5))

        except Exception as e:
            msg = str(e)

            if "429" in msg:
                success_streak = 0

                if batch_size > 1:
                    batch_size -= 1

                cooldown_s = min(cooldown_max, cooldown_s * 1.5)
                print(f"429 rate limit. New batch={batch_size}. New cooldown={cooldown_s:.1f}s. Retrying same batch.")
                time.sleep(cooldown_s + random.uniform(0.0, 2.0))
                continue

            print(f"Non-429 error at index {idx}. Skipping this batch. Error: {e}")
            idx += batch_size
            time.sleep(cooldown_s)

    if not os.path.exists(OUT_CSV):
        print("No Google Trends rows were written.")
        return

    populated = merge_progress_into_workbook(INPUT_FILE, OUT_CSV)
    print(f"Updated {INPUT_FILE}: {OUTPUT_COLUMN} populated for {populated} rows.")


if __name__ == "__main__":
    main()
