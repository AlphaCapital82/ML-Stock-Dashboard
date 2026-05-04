import os
import time
import random
import re
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
REFRESH_AFTER_DAYS = 30
START_AT_TICKER = "SIEB"
COMPANY_SUFFIX_PATTERN = re.compile(
    r"\b("
    r"incorporated|inc|corporation|corp|company|co|limited|ltd|plc|"
    r"ordinary shares|common stock|class a|class b|s\.a\.|sa|n\.v\.|nv|ag|se|asa|as|ab|oyj|"
    r"holdings|holding|group"
    r")\.?\b",
    flags=re.IGNORECASE,
)


def clean_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def clean_company_name(value) -> str:
    text = clean_text(value)
    if not text:
        return ""
    text = re.sub(r"\([^)]*\)", " ", text)
    text = text.replace("&", " and ")
    text = COMPANY_SUFFIX_PATTERN.sub(" ", text)
    text = re.sub(r"[^A-Za-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_ticker_for_search(value) -> str:
    text = clean_text(value).upper()
    if not text:
        return ""
    return text.split(".")[0].split("-")[0].strip()


def trend_term_candidates(row: dict) -> list[str]:
    candidates = [
        clean_company_name(row.get("NAME", "")),
        clean_text(row.get("TERM_FINAL", "")),
        clean_ticker_for_search(row.get("TICKERS", "")),
        clean_text(row.get("TICKERS", "")),
    ]
    out = []
    seen = set()
    for candidate in candidates:
        if candidate and candidate.casefold() not in seen:
            out.append(candidate)
            seen.add(candidate.casefold())
    return out


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

    df["TICKERS"] = df["TICKERS"].map(clean_text)
    df["TERM_FINAL"] = df["TERM_FINAL"].map(clean_text)
    df["NAME"] = df["NAME"].map(clean_text)
    df.loc[df["TERM_FINAL"] == "", "TERM_FINAL"] = df.loc[df["TERM_FINAL"] == "", "NAME"]
    df.loc[df["TERM_FINAL"] == "", "TERM_FINAL"] = df.loc[df["TERM_FINAL"] == "", "TICKERS"]

    df = df[df["TICKERS"] != ""].drop_duplicates(subset=["TICKERS"], keep="first").copy()
    return df


def load_done_tickers(out_csv: str) -> set[str]:
    if not os.path.exists(out_csv):
        return set()


def load_latest_progress(out_csv: Path) -> pd.DataFrame:
    if not out_csv.exists():
        return pd.DataFrame(columns=["TICKERS", OUTPUT_COLUMN, "WINDOW_END"])
    try:
        progress = pd.read_csv(out_csv, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame(columns=["TICKERS", OUTPUT_COLUMN, "WINDOW_END"])
    if "TICKERS" not in progress.columns:
        return pd.DataFrame(columns=["TICKERS", OUTPUT_COLUMN, "WINDOW_END"])

    progress = progress.copy()
    progress["TICKERS"] = progress["TICKERS"].map(clean_text)
    if OUTPUT_COLUMN in progress.columns:
        progress[OUTPUT_COLUMN] = pd.to_numeric(progress[OUTPUT_COLUMN], errors="coerce")
    else:
        progress[OUTPUT_COLUMN] = pd.NA
    if "WINDOW_END" in progress.columns:
        progress["WINDOW_END_DATE"] = pd.to_datetime(progress["WINDOW_END"], errors="coerce").dt.date
    else:
        progress["WINDOW_END"] = ""
        progress["WINDOW_END_DATE"] = pd.NaT

    progress = progress[progress["TICKERS"] != ""]
    progress = progress.drop_duplicates(subset=["TICKERS"], keep="last")
    return progress


def classify_refresh_status(universe: pd.DataFrame, progress: pd.DataFrame) -> pd.DataFrame:
    out = universe.copy()
    if OUTPUT_COLUMN not in out.columns:
        out[OUTPUT_COLUMN] = pd.NA

    progress_cols = ["TICKERS", OUTPUT_COLUMN, "WINDOW_END", "WINDOW_END_DATE"]
    available_progress_cols = [c for c in progress_cols if c in progress.columns]
    latest = progress[available_progress_cols].rename(
        columns={
            OUTPUT_COLUMN: "progress_google_trends",
            "WINDOW_END": "progress_window_end",
            "WINDOW_END_DATE": "progress_window_end_date",
        }
    )
    out = out.merge(latest, on="TICKERS", how="left")

    cutoff_date = date.today() - timedelta(days=REFRESH_AFTER_DAYS)
    workbook_value = pd.to_numeric(out[OUTPUT_COLUMN], errors="coerce")
    progress_value = pd.to_numeric(out.get("progress_google_trends"), errors="coerce")
    out["has_google_trends_value"] = workbook_value.notna() | progress_value.notna()
    out["is_stale_google_trends"] = out["has_google_trends_value"] & (
        out["progress_window_end_date"].isna() | (out["progress_window_end_date"] < cutoff_date)
    )
    out["needs_google_trends"] = (~out["has_google_trends_value"]) | out["is_stale_google_trends"]
    return out
    try:
        prev = pd.read_csv(out_csv, encoding="utf-8-sig")
        if "TICKERS" not in prev.columns:
            return set()
        prev["TICKERS"] = prev["TICKERS"].map(clean_text)
        if OUTPUT_COLUMN in prev.columns:
            prev = prev[pd.to_numeric(prev[OUTPUT_COLUMN], errors="coerce").notna()]
        return set(prev["TICKERS"].tolist())
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

    progress_df["TICKERS"] = progress_df["TICKERS"].map(clean_text)
    valid_tickers = set(workbook_df[ticker_column].map(clean_text))
    progress_df = progress_df[progress_df["TICKERS"].isin(valid_tickers)]
    progress_df = progress_df.drop_duplicates(subset=["TICKERS"], keep="last")
    progress_values = pd.to_numeric(progress_df[OUTPUT_COLUMN], errors="coerce")
    progress_df = progress_df[progress_values.notna()].copy()
    progress_df[OUTPUT_COLUMN] = progress_values[progress_values.notna()]
    trends_by_ticker = progress_df.set_index("TICKERS")[OUTPUT_COLUMN]

    existing = workbook_df[OUTPUT_COLUMN] if OUTPUT_COLUMN in workbook_df.columns else pd.Series(index=workbook_df.index, dtype="float64")
    merged = workbook_df[ticker_column].map(clean_text).map(trends_by_ticker)
    workbook_df[OUTPUT_COLUMN] = merged.combine_first(existing)
    workbook_df.to_excel(workbook_path, index=False)
    return int(workbook_df[OUTPUT_COLUMN].notna().sum())


def main():
    universe = read_universe(INPUT_FILE)
    total_workbook_rows = len(universe)
    progress = load_latest_progress(OUT_CSV)
    status = classify_refresh_status(universe, progress)

    current_values = int((status["has_google_trends_value"] & ~status["is_stale_google_trends"]).sum())
    stale_values = int(status["is_stale_google_trends"].sum())
    missing_values = int((~status["has_google_trends_value"]).sum())
    universe = status[status["needs_google_trends"]].copy()

    done = set(
        status.loc[
            status["has_google_trends_value"] & ~status["is_stale_google_trends"],
            "TICKERS",
        ].tolist()
    )

    items = universe.to_dict("records")
    items = [r for r in items if r["TICKERS"] not in done]
    if START_AT_TICKER:
        start_at = START_AT_TICKER.strip().upper()
        start_index = next((i for i, r in enumerate(items) if clean_text(r.get("TICKERS", "")).upper() == start_at), None)
        if start_index is None:
            print(f"START_AT_TICKER={START_AT_TICKER!r} was not found in the remaining request list. Starting at first remaining ticker.")
        else:
            skipped = start_index
            items = items[start_index:]
            print(f"Starting at {START_AT_TICKER}; skipped {skipped} earlier remaining tickers for this run.")

    total_all = len(universe)
    remaining = len(items)
    print(
        f"Workbook tickers: {total_workbook_rows}. Current values: {current_values}. "
        f"Missing values: {missing_values}. Stale values older than {REFRESH_AFTER_DAYS} days: {stale_values}. "
        f"Remaining requests: {remaining}."
    )

    pytrends = TrendReq(hl="en-US", tz=0)

    batch_size = 5
    cooldown_s = 45.0
    cooldown_min = 20.0
    cooldown_max = 180.0
    success_streak = 0

    idx = 0
    while idx < len(items):
        batch = items[idx: idx + batch_size]

        terms_by_ticker = {}
        terms = []
        seen_terms = set()
        for r in batch:
            candidates = trend_term_candidates(r)
            if not candidates:
                continue
            terms_by_ticker[r["TICKERS"]] = candidates
            for term in candidates:
                if term.casefold() not in seen_terms:
                    terms.append(term)
                    seen_terms.add(term.casefold())
                if len(terms) >= 5:
                    break
            if len(terms) >= 5:
                break

        if not terms:
            idx += batch_size
            continue

        try:
            pytrends.build_payload(terms, timeframe=TIMEFRAME, geo=GEO)
            ts = pytrends.interest_over_time()

            out_rows = []
            new_values = 0
            no_data_tickers = []
            for r in batch:
                ticker = r["TICKERS"]
                name = clean_text(r.get("NAME", ""))
                term = ""
                growth, recent_avg, prior_avg = None, None, None
                if ts is not None and not ts.empty:
                    for candidate in terms_by_ticker.get(ticker, []):
                        if candidate not in ts.columns:
                            continue
                        candidate_growth, candidate_recent_avg, candidate_prior_avg = trailing_one_year_growth(ts[candidate])
                        if candidate_growth is not None:
                            term = candidate
                            growth, recent_avg, prior_avg = candidate_growth, candidate_recent_avg, candidate_prior_avg
                            break
                    if not term and terms_by_ticker.get(ticker):
                        term = terms_by_ticker[ticker][0]
                elif terms_by_ticker.get(ticker):
                    term = terms_by_ticker[ticker][0]
                if growth is None:
                    no_data_tickers.append(ticker)
                else:
                    new_values += 1

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

                if growth is not None:
                    done.add(ticker)

            append_rows_to_csv(out_rows, OUT_CSV)
            populated = merge_progress_into_workbook(INPUT_FILE, OUT_CSV)

            idx += batch_size
            success_streak += 1

            processed = len(done)
            batch_tickers = ", ".join(clean_text(r.get("TICKERS", "")) for r in batch)
            print(
                f"Batch [{batch_tickers}] | new_values={new_values} | no_data={len(no_data_tickers)} | "
                f"current_progress_values={processed} | refresh/missing_set={total_all} | "
                f"{OUTPUT_COLUMN}_populated={populated} | batch={batch_size} | cooldown={cooldown_s:.1f}s"
            )
            if no_data_tickers:
                print(f"No usable Google Trends value: {', '.join(no_data_tickers)}")

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
