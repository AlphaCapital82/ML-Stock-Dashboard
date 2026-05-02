import argparse
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import yfinance as yf
from dateutil.relativedelta import relativedelta


def read_tickers(input_path: Path) -> pd.DataFrame:
    if input_path.suffix.lower() in [".xlsx", ".xlsm", ".xls"]:
        df = pd.read_excel(input_path)
    elif input_path.suffix.lower() in [".csv"]:
        df = pd.read_csv(input_path)
    elif input_path.suffix.lower() in [".txt"]:
        df = pd.read_csv(input_path, header=None, names=["ticker"])
    else:
        raise ValueError(f"Unsupported file type: {input_path.suffix}")

    ticker_col = None
    for col in df.columns:
        if str(col).strip().lower() in ["ticker", "tickers", "symbol"]:
            ticker_col = col
            break

    if ticker_col is None:
        ticker_col = df.columns[0]

    df = df.copy()
    df.rename(columns={ticker_col: "ticker"}, inplace=True)
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df = df[df["ticker"].notna() & (df["ticker"] != "") & (df["ticker"].str.lower() != "nan")]
    df["ticker"] = df["ticker"].astype(str)

    return df


def calculate_3m_change_for_batch(tickers, target_date, start_buffer_date):
    results = {}

    try:
        data = yf.download(
            tickers=tickers,
            start=start_buffer_date.strftime("%Y-%m-%d"),
            end=(datetime.today() + relativedelta(days=1)).strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=True,
            group_by="ticker",
            threads=True,
            progress=False,
        )
    except Exception as e:
        for ticker in tickers:
            results[ticker] = {
                "price_change_3m": None,
                "price_3m_start": None,
                "latest_price": None,
                "start_date_used": None,
                "latest_date_used": None,
                "price_change_3m_status": f"download_error: {e}",
            }
        return results

    for ticker in tickers:
        try:
            if len(tickers) == 1:
                ticker_data = data.copy()
            else:
                if ticker not in data.columns.get_level_values(0):
                    raise ValueError("ticker_not_returned_by_yfinance")
                ticker_data = data[ticker].copy()

            if "Close" not in ticker_data.columns:
                raise ValueError("missing_close_column")

            close = ticker_data["Close"].dropna()

            if close.empty:
                raise ValueError("no_price_data")

            latest_date = close.index.max()
            latest_price = float(close.loc[latest_date])

            close_after_target = close[close.index >= pd.Timestamp(target_date)]

            if close_after_target.empty:
                raise ValueError("no_price_on_or_after_3m_date")

            start_date = close_after_target.index.min()
            start_price = float(close_after_target.loc[start_date])

            if start_price == 0:
                raise ValueError("zero_start_price")

            price_change_3m = (latest_price / start_price) - 1

            results[ticker] = {
                "price_change_3m": price_change_3m,
                "price_3m_start": start_price,
                "latest_price": latest_price,
                "start_date_used": start_date.date().isoformat(),
                "latest_date_used": latest_date.date().isoformat(),
                "price_change_3m_status": "ok",
            }

        except Exception as e:
            results[ticker] = {
                "price_change_3m": None,
                "price_3m_start": None,
                "latest_price": None,
                "start_date_used": None,
                "latest_date_used": None,
                "price_change_3m_status": str(e),
            }

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="tickers.txt", help="Input file: tickers.txt, tickerlist.xlsx, or csv")
    parser.add_argument("--output", default="tickerlist_with_3m_price_change.xlsx", help="Output Excel file")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of tickers per yfinance batch")
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between batches")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    df = read_tickers(input_path)
    tickers = df["ticker"].dropna().astype(str).str.strip().unique().tolist()
    if not tickers:
        raise ValueError(f"No tickers found in {input_path.resolve()}")

    today = datetime.today()
    target_date = today - relativedelta(months=3)
    start_buffer_date = today - relativedelta(months=5)

    all_results = {}

    print(f"Tickers: {len(tickers)}")
    print(f"Target 3-month date: {target_date.date().isoformat()}")
    print(f"Using start buffer date: {start_buffer_date.date().isoformat()}")

    for i in range(0, len(tickers), args.batch_size):
        batch = tickers[i:i + args.batch_size]
        print(f"Processing batch {i // args.batch_size + 1}: {len(batch)} tickers")

        batch_results = calculate_3m_change_for_batch(
            tickers=batch,
            target_date=target_date,
            start_buffer_date=start_buffer_date,
        )

        all_results.update(batch_results)
        time.sleep(args.sleep)

    result_df = pd.DataFrame.from_dict(all_results, orient="index").reset_index()
    result_df.rename(columns={"index": "ticker"}, inplace=True)
    result_df["ticker"] = result_df["ticker"].astype(str)
    df["ticker"] = df["ticker"].astype(str)

    output_df = df.merge(result_df, on="ticker", how="left")

    output_df.to_excel(output_path, index=False)

    print(f"Done. Saved to: {output_path.resolve()}")
    print("Columns added:")
    print("- price_change_3m")
    print("- price_3m_start")
    print("- latest_price")
    print("- start_date_used")
    print("- latest_date_used")
    print("- price_change_3m_status")


if __name__ == "__main__":
    main()
