from __future__ import annotations

import argparse
import json
import math
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from openpyxl import Workbook


GOOGLE_CREDENTIALS_PATH = Path("0_ingestion") / "stock-ingestion-494417-17cbf0e7891b.json"
EXPORT_WORKBOOK_PATH = Path("0_needs_processing") / "tickerlist.xlsx"
PROGRESS_PATH = Path("0_ingestion") / "valuation_gap_progress.json"
FAILED_TICKERS_PATH = Path("0_ingestion") / "failed_tickers.json"

DEFAULT_SPREADSHEET_ID = "1FBnRzytDx-5uNRmK4Qqagt8CsnKRCkiI2WUiyZMfOIs"
DEFAULT_SOURCE_WORKSHEET = "tickers"
DEFAULT_OUTPUT_WORKSHEET = "tickers_output"
DEFAULT_WORKSHEET_NAME = "Sheet1"
DEFAULT_DCF_WORKSHEET = "dcf"
DEFAULT_VALUATION_GAP_HEADER = "valuation_gap"
DEFAULT_DCF_INPUT_CELL = "A2"
DEFAULT_DCF_OUTPUT_CELL = "B21"
DEFAULT_DCF_POLL_SECONDS = 4.0
DEFAULT_DCF_TIMEOUT_SECONDS = 90.0
DEFAULT_DCF_MIN_WAIT_SECONDS = 8.0
DEFAULT_DCF_STABLE_READS = 2
TICKER_HEADER = "Ticker"

SHEET_ERROR_TOKENS = (
    "#ERROR!",
    "#VALUE!",
    "#DIV/0!",
    "#N/A",
    "#REF!",
    "#NAME?",
    "#NUM!",
    "#NULL!",
    "#FEIL!",
    "#VERDI!",
    "#I/T",
    "ERROR:",
    "OOPS",
    "LASTER INN",
    "LOADING",
    "FUNCTION TREND",
    "FUNCTION DIVIDE",
)


def column_letter(column_number: int) -> str:
    if column_number <= 0:
        return ""
    letters: list[str] = []
    while column_number > 0:
        column_number, remainder = divmod(column_number - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def normalize_header_name(value: Any) -> str:
    return str(value).strip().casefold()


def find_header_index(headers: list[str], required_header: str, worksheet_name: str) -> int:
    normalized_headers = [normalize_header_name(value) for value in headers]
    normalized_required = normalize_header_name(required_header)
    if normalized_required not in normalized_headers:
        raise ValueError(f"Required header {required_header!r} not found in worksheet {worksheet_name!r}.")
    return normalized_headers.index(normalized_required)


def is_bad_sheet_text(value: str) -> bool:
    upper_text = value.strip().upper()
    return any(token in upper_text for token in SHEET_ERROR_TOKENS)


def coerce_valid_number(value: Any) -> float | None:
    if value in ("", None):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text or is_bad_sheet_text(text):
        return None

    had_percent = "%" in text
    text = text.replace("\u2212", "-")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[^0-9,\.\-+eE]", "", text)
    if not text:
        return None

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        number = float(text)
    except ValueError:
        return None

    if had_percent:
        number /= 100
    return number if math.isfinite(number) else None


def short_sheet_value(value: Any, max_length: int = 80) -> str:
    if value is None:
        return "blank"
    text = str(value).replace("\n", " ").strip()
    if not text:
        return "blank"
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text


def import_google_client():
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "Google Sheets support requires 'google-auth' and 'google-api-python-client'."
        ) from exc
    return Credentials, build


def load_google_service(credentials_path: Path):
    if not credentials_path.exists():
        raise FileNotFoundError(f"Google service-account JSON not found: {credentials_path}")
    credentials_cls, build = import_google_client()
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = credentials_cls.from_service_account_file(str(credentials_path), scopes=scopes)
    return build("sheets", "v4", credentials=credentials)


def get_google_header_cells(service, spreadsheet_id: str, sheet_name: str) -> dict[str, str]:
    response = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!1:1").execute()
    rows = response.get("values", [])
    if not rows:
        raise ValueError(f"Worksheet {sheet_name!r} does not contain a header row.")
    header_cells: dict[str, str] = {}
    for index, value in enumerate(rows[0], start=1):
        if value not in (None, ""):
            header_cells[str(value).strip()] = column_letter(index)
    return header_cells


def get_google_last_row(service, spreadsheet_id: str, sheet_name: str, ticker_column: str) -> int:
    response = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!{ticker_column}:{ticker_column}",
    ).execute()
    return len(response.get("values", []))


def get_google_values(service, spreadsheet_id: str, sheet_name: str) -> list[list[Any]]:
    response = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute()
    return response.get("values", [])


def get_google_range_values(service, spreadsheet_id: str, range_name: str, value_render_option: str = "UNFORMATTED_VALUE") -> list[list[Any]]:
    response = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueRenderOption=value_render_option,
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute()
    return response.get("values", [])


def get_google_single_value(service, spreadsheet_id: str, range_name: str, value_render_option: str = "UNFORMATTED_VALUE") -> Any:
    values = get_google_range_values(service, spreadsheet_id, range_name, value_render_option=value_render_option)
    if values and values[0]:
        return values[0][0]
    return ""


def update_google_range_values(
    service,
    spreadsheet_id: str,
    range_name: str,
    values: list[list[str | int | float]],
    value_input_option: str = "RAW",
) -> None:
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption=value_input_option,
        body={"values": values},
    ).execute()


def normalize_rows(values: list[list[Any]]) -> list[list[Any]]:
    if not values:
        return []
    max_columns = max(len(row) for row in values)
    return [list(row) + [""] * (max_columns - len(row)) for row in values]


def export_google_sheet_to_excel(service, spreadsheet_id: str, output_sheet_name: str, export_path: Path) -> tuple[int, int]:
    values = normalize_rows(get_google_values(service, spreadsheet_id, output_sheet_name))
    if not values:
        raise ValueError(f"Worksheet {output_sheet_name!r} does not contain data.")
    export_path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    ws = workbook.active
    ws.title = DEFAULT_WORKSHEET_NAME
    for row in values:
        ws.append(row)
    workbook.save(export_path)
    return len(values), len(values[0]) if values else 0


def load_progress(progress_path: Path) -> dict[str, object]:
    if not progress_path.exists():
        return {}
    with progress_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_progress(progress_path: Path, source_sheet_name: str, last_completed_row: int, last_completed_ticker: str) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source_worksheet": source_sheet_name,
        "last_completed_row": last_completed_row,
        "last_completed_ticker": last_completed_ticker,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    with progress_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def clear_progress(progress_path: Path) -> None:
    if progress_path.exists():
        progress_path.unlink()


def wait_for_numeric_output(
    service,
    spreadsheet_id: str,
    range_name: str,
    poll_seconds: float,
    timeout_seconds: float,
    min_wait_seconds: float,
    stable_reads_required: int,
) -> tuple[float | None, Any]:
    start_time = time.time()
    last_number: float | None = None
    stable_reads = 0
    latest_raw: Any = ""
    stable_reads_required = max(1, stable_reads_required)
    poll_seconds = max(0.5, poll_seconds)

    while time.time() - start_time < timeout_seconds:
        elapsed = time.time() - start_time
        try:
            raw_output = get_google_single_value(service, spreadsheet_id, range_name, value_render_option="UNFORMATTED_VALUE")
        except Exception as exc:
            raw_output = f"Google read error: {exc}"

        latest_raw = raw_output
        number = coerce_valid_number(raw_output)

        if elapsed >= min_wait_seconds and number is not None:
            if last_number is not None and abs(number - last_number) < 1e-12:
                stable_reads += 1
            else:
                last_number = number
                stable_reads = 1
            if stable_reads >= stable_reads_required:
                return number, raw_output
        elif number is None:
            last_number = None
            stable_reads = 0

        time.sleep(poll_seconds)

    return None, latest_raw


def sync_metric_output_column(
    service,
    spreadsheet_id: str,
    source_sheet_name: str,
    output_sheet_name: str,
    header_name: str,
) -> int:
    source_values = get_google_values(service, spreadsheet_id, source_sheet_name)
    if not source_values:
        raise ValueError(f"Worksheet {source_sheet_name!r} does not contain data.")
    source_headers = [str(value).strip() for value in source_values[0]]
    source_idx = find_header_index(source_headers, header_name, source_sheet_name)
    output_header_cells = get_google_header_cells(service, spreadsheet_id, output_sheet_name)
    if header_name not in output_header_cells:
        raise ValueError(f"Column {header_name!r} not found in output worksheet {output_sheet_name!r}.")
    output_column = output_header_cells[header_name]

    values: list[list[str | float]] = []
    copied_rows = 0
    for row in source_values[1:]:
        padded_row = list(row) + [""] * max(0, len(source_headers) - len(row))
        raw_value = padded_row[source_idx]
        number = coerce_valid_number(raw_value)
        values.append([raw_value if number is not None else ""])
        copied_rows += 1

    if values:
        update_google_range_values(service, spreadsheet_id, f"{output_sheet_name}!{output_column}2:{output_column}{copied_rows + 1}", values)
    return copied_rows


def refresh_valuation_gap_from_dcf(
    service,
    spreadsheet_id: str,
    tickers_sheet_name: str,
    output_sheet_name: str,
    dcf_sheet_name: str,
    valuation_gap_header: str,
    dcf_input_cell: str,
    dcf_output_cell: str,
    poll_seconds: float,
    timeout_seconds: float,
    min_wait_seconds: float,
    stable_reads_required: int,
    progress_path: Path,
    resume_progress: bool,
    batch_size: int = 50,
    export_path: Path = None,
) -> tuple[int, list[str]]:
    source_values = get_google_values(service, spreadsheet_id, tickers_sheet_name)
    if not source_values:
        raise ValueError(f"Worksheet {tickers_sheet_name!r} does not contain data.")
    source_headers = [str(value).strip() for value in source_values[0]]
    ticker_idx = find_header_index(source_headers, TICKER_HEADER, tickers_sheet_name)
    valuation_gap_idx = find_header_index(source_headers, valuation_gap_header, tickers_sheet_name)
    valuation_gap_column = column_letter(valuation_gap_idx + 1)

    # Get output worksheet column mapping
    output_header_cells = get_google_header_cells(service, spreadsheet_id, output_sheet_name)
    if valuation_gap_header not in output_header_cells:
        raise ValueError(f"Column {valuation_gap_header!r} not found in output worksheet {output_sheet_name!r}.")
    output_column = output_header_cells[valuation_gap_header]

    resume_row = 2
    if resume_progress:
        progress = load_progress(progress_path)
        if progress.get("source_worksheet") == tickers_sheet_name:
            try:
                stored_row = int(progress.get("last_completed_row", 1) or 1)
            except (TypeError, ValueError):
                stored_row = 1
            resume_row = max(2, stored_row + 1)
    else:
        clear_progress(progress_path)

    last_row = len(source_values)
    print(
        f"Starting {valuation_gap_header}: worksheet={tickers_sheet_name}, rows {resume_row}-{last_row}, output={output_sheet_name}",
        flush=True,
    )

    failures: list[str] = []
    refreshed_count = 0
    batch_data = []

    for row_number, row in enumerate(source_values[1:], start=2):
        if row_number < resume_row:
            continue

        padded_row = list(row) + [""] * max(0, len(source_headers) - len(row))
        ticker = str(padded_row[ticker_idx]).strip()
        if not ticker:
            break

        print(f"Row {row_number}: refreshing {ticker}", flush=True)
        update_google_range_values(service, spreadsheet_id, f"{dcf_sheet_name}!{dcf_input_cell}", [[ticker]])
        number, raw_output = wait_for_numeric_output(
            service,
            spreadsheet_id,
            f"{dcf_sheet_name}!{dcf_output_cell}",
            poll_seconds,
            timeout_seconds,
            min_wait_seconds,
            stable_reads_required,
        )

        if number is None:
            failures.append(f"{ticker}: {short_sheet_value(raw_output)}")
            output_value: str | float = ""
        else:
            output_value = number

        # Write to both worksheets immediately
        update_google_range_values(service, spreadsheet_id, f"{tickers_sheet_name}!{valuation_gap_column}{row_number}", [[output_value]])
        update_google_range_values(service, spreadsheet_id, f"{output_sheet_name}!{output_column}{row_number}", [[output_value]])

        refreshed_count += 1
        save_progress(progress_path, tickers_sheet_name, row_number, ticker)

        # Batch Excel export every batch_size writes
        if export_path and refreshed_count % batch_size == 0:
            try:
                exported_rows, _ = export_google_sheet_to_excel(service, spreadsheet_id, output_sheet_name, export_path)
                print(f"Batch export: {exported_rows} rows written to {export_path}")
            except Exception as e:
                print(f"Batch export failed: {e}")

    # Final export if export_path provided
    if export_path and refreshed_count > 0:
        try:
            exported_rows, _ = export_google_sheet_to_excel(service, spreadsheet_id, output_sheet_name, export_path)
            print(f"Final export: {exported_rows} rows written to {export_path}")
        except Exception as e:
            print(f"Final export failed: {e}")

    clear_progress(progress_path)
    return refreshed_count, failures


def save_failed_tickers(failures: list[str], failed_tickers_path: Path) -> None:
    """Save failed tickers to a file for later reprocessing."""
    failed_tickers_path.parent.mkdir(parents=True, exist_ok=True)
    failed_tickers = []
    for failure in failures:
        # Extract ticker from failure string (format: "TICKER: error_message")
        if ": " in failure:
            ticker = failure.split(": ")[0].strip()
            failed_tickers.append(ticker)

    with failed_tickers_path.open("w", encoding="utf-8") as handle:
        json.dump({
            "failed_tickers": failed_tickers,
            "total_failures": len(failed_tickers),
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z"
        }, handle, indent=2)


def load_failed_tickers(failed_tickers_path: Path) -> list[str]:
    """Load failed tickers from file for reprocessing."""
    if not failed_tickers_path.exists():
        return []

    with failed_tickers_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
        return data.get("failed_tickers", [])


def reprocess_failed_tickers(
    service,
    spreadsheet_id: str,
    tickers_sheet_name: str,
    output_sheet_name: str,
    dcf_sheet_name: str,
    valuation_gap_header: str,
    dcf_input_cell: str,
    dcf_output_cell: str,
    poll_seconds: float,
    timeout_seconds: float,
    min_wait_seconds: float,
    stable_reads_required: int,
    failed_tickers_path: Path,
    export_path: Path = None,
) -> tuple[int, list[str]]:
    """Reprocess tickers that previously failed."""
    failed_tickers = load_failed_tickers(failed_tickers_path)
    if not failed_tickers:
        print("No failed tickers found to reprocess.")
        return 0, []

    # Get worksheet data and mappings
    source_values = get_google_values(service, spreadsheet_id, tickers_sheet_name)
    if not source_values:
        raise ValueError(f"Worksheet {tickers_sheet_name!r} does not contain data.")

    source_headers = [str(value).strip() for value in source_values[0]]
    ticker_idx = find_header_index(source_headers, TICKER_HEADER, tickers_sheet_name)
    valuation_gap_idx = find_header_index(source_headers, valuation_gap_header, tickers_sheet_name)
    valuation_gap_column = column_letter(valuation_gap_idx + 1)

    output_header_cells = get_google_header_cells(service, spreadsheet_id, output_sheet_name)
    if valuation_gap_header not in output_header_cells:
        raise ValueError(f"Column {valuation_gap_header!r} not found in output worksheet {output_sheet_name!r}.")
    output_column = output_header_cells[valuation_gap_header]

    # Create ticker to row mapping
    ticker_to_row = {}
    for row_number, row in enumerate(source_values[1:], start=2):
        padded_row = list(row) + [""] * max(0, len(source_headers) - len(row))
        ticker = str(padded_row[ticker_idx]).strip()
        if ticker:
            ticker_to_row[ticker] = row_number

    reprocessed_count = 0
    remaining_failures = []

    for ticker in failed_tickers:
        if ticker not in ticker_to_row:
            remaining_failures.append(f"{ticker}: ticker not found in worksheet")
            continue

        row_number = ticker_to_row[ticker]

        update_google_range_values(service, spreadsheet_id, f"{dcf_sheet_name}!{dcf_input_cell}", [[ticker]])
        number, raw_output = wait_for_numeric_output(
            service,
            spreadsheet_id,
            f"{dcf_sheet_name}!{dcf_output_cell}",
            poll_seconds,
            timeout_seconds,
            min_wait_seconds,
            stable_reads_required,
        )

        if number is None:
            remaining_failures.append(f"{ticker}: {short_sheet_value(raw_output)}")
            output_value: str | float = ""
        else:
            output_value = number

        # Write to both worksheets
        update_google_range_values(service, spreadsheet_id, f"{tickers_sheet_name}!{valuation_gap_column}{row_number}", [[output_value]])
        update_google_range_values(service, spreadsheet_id, f"{output_sheet_name}!{output_column}{row_number}", [[output_value]])

        reprocessed_count += 1
        print(f"Reprocessed {ticker}: {'SUCCESS' if number is not None else 'FAILED'}")

    # Update failed tickers file with remaining failures
    if remaining_failures:
        save_failed_tickers(remaining_failures, failed_tickers_path)
    else:
        # All succeeded, remove the file
        if failed_tickers_path.exists():
            failed_tickers_path.unlink()

    # Export final results if requested
    if export_path:
        try:
            exported_rows, _ = export_google_sheet_to_excel(service, spreadsheet_id, output_sheet_name, export_path)
            print(f"Reprocess export: {exported_rows} rows written to {export_path}")
        except Exception as e:
            print(f"Reprocess export failed: {e}")

    return reprocessed_count, remaining_failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh valuation_gap through the DCF tab, sync it to tickers_output, and export tickerlist.xlsx.")
    parser.add_argument("--sheet-id", default=os.getenv("GOOGLE_SHEET_ID", DEFAULT_SPREADSHEET_ID))
    parser.add_argument("--credentials", default=str(GOOGLE_CREDENTIALS_PATH))
    parser.add_argument("--source-worksheet", default=os.getenv("INGESTION_SOURCE_WORKSHEET", DEFAULT_SOURCE_WORKSHEET))
    parser.add_argument("--output-worksheet", default=os.getenv("INGESTION_OUTPUT_WORKSHEET", DEFAULT_OUTPUT_WORKSHEET))
    parser.add_argument("--export-path", default=str(EXPORT_WORKBOOK_PATH))
    parser.add_argument("--dcf-worksheet", default=os.getenv("INGESTION_DCF_WORKSHEET", DEFAULT_DCF_WORKSHEET))
    parser.add_argument("--valuation-gap-header", default=os.getenv("INGESTION_VALUATION_GAP_HEADER", DEFAULT_VALUATION_GAP_HEADER))
    parser.add_argument("--dcf-input-cell", default=os.getenv("INGESTION_DCF_INPUT_CELL", DEFAULT_DCF_INPUT_CELL))
    parser.add_argument("--dcf-output-cell", default=os.getenv("INGESTION_DCF_OUTPUT_CELL", DEFAULT_DCF_OUTPUT_CELL))
    parser.add_argument("--poll-seconds", type=float, default=float(os.getenv("INGESTION_DCF_POLL_SECONDS", str(DEFAULT_DCF_POLL_SECONDS))))
    parser.add_argument("--timeout-seconds", type=float, default=float(os.getenv("INGESTION_DCF_TIMEOUT_SECONDS", str(DEFAULT_DCF_TIMEOUT_SECONDS))))
    parser.add_argument("--min-wait-seconds", type=float, default=float(os.getenv("INGESTION_DCF_MIN_WAIT_SECONDS", str(DEFAULT_DCF_MIN_WAIT_SECONDS))))
    parser.add_argument("--stable-reads", type=int, default=int(os.getenv("INGESTION_DCF_STABLE_READS", str(DEFAULT_DCF_STABLE_READS))))
    parser.add_argument("--progress-path", default=str(PROGRESS_PATH))
    parser.add_argument("--no-resume-progress", action="store_true")
    parser.add_argument("--reprocess-failed", action="store_true")
    parser.add_argument("--failed-tickers-path", default=str(FAILED_TICKERS_PATH))
    parser.add_argument("--batch-size", type=int, default=50)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(f"Using Google Sheet ID: {args.sheet_id}", flush=True)
    print(f"Loading Google credentials: {args.credentials}", flush=True)
    service = load_google_service(Path(args.credentials))
    print("Google Sheets service loaded.", flush=True)
    print(f"Reading worksheets: {args.source_worksheet}, {args.output_worksheet}", flush=True)

    if args.reprocess_failed:
        # Reprocess failed tickers mode
        reprocessed_count, remaining_failures = reprocess_failed_tickers(
            service=service,
            spreadsheet_id=args.sheet_id,
            tickers_sheet_name=args.source_worksheet,
            output_sheet_name=args.output_worksheet,
            dcf_sheet_name=args.dcf_worksheet,
            valuation_gap_header=args.valuation_gap_header,
            dcf_input_cell=args.dcf_input_cell,
            dcf_output_cell=args.dcf_output_cell,
            poll_seconds=args.poll_seconds,
            timeout_seconds=args.timeout_seconds,
            min_wait_seconds=args.min_wait_seconds,
            stable_reads_required=args.stable_reads,
            failed_tickers_path=Path(args.failed_tickers_path),
            export_path=Path(args.export_path),
        )
        print(f"Reprocessed tickers: {reprocessed_count}")
        if remaining_failures:
            print("Remaining failures:")
            for failure in remaining_failures:
                print(f"- {failure}")
        return

    # Normal processing mode
    refreshed_rows, failures = refresh_valuation_gap_from_dcf(
        service=service,
        spreadsheet_id=args.sheet_id,
        tickers_sheet_name=args.source_worksheet,
        output_sheet_name=args.output_worksheet,
        dcf_sheet_name=args.dcf_worksheet,
        valuation_gap_header=args.valuation_gap_header,
        dcf_input_cell=args.dcf_input_cell,
        dcf_output_cell=args.dcf_output_cell,
        poll_seconds=args.poll_seconds,
        timeout_seconds=args.timeout_seconds,
        min_wait_seconds=args.min_wait_seconds,
        stable_reads_required=args.stable_reads,
        progress_path=Path(args.progress_path),
        resume_progress=not args.no_resume_progress,
        batch_size=args.batch_size,
        export_path=Path(args.export_path),
    )

    # Save failed tickers for later reprocessing
    if failures:
        save_failed_tickers(failures, Path(args.failed_tickers_path))
        print(f"Saved {len(failures)} failed tickers to {args.failed_tickers_path}")

    print(f"Source worksheet updated: {args.source_worksheet}")
    print(f"Output worksheet updated: {args.output_worksheet}")
    print(f"Rows refreshed in tickers: {refreshed_rows}")
    print(f"Export path: {args.export_path}")
    if failures:
        print("Rows that did not resolve to a valid DCF value:")
        for failure in failures:
            print(f"- {failure}")
        print(f"\nTo reprocess failed tickers, run: python {__file__} --reprocess-failed")


if __name__ == "__main__":
    main()
