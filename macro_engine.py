# === macro_verdict.py ===

import pandas as pd
from fredapi import Fred
import yfinance as yf
import smtplib
import sys
import html
import re
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
import os
from pathlib import Path
from dotenv import load_dotenv

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# === Load environment variables ===
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
sender_email = (os.getenv("EMAIL_SENDER") or "").strip()
receiver_email = (os.getenv("EMAIL_RECEIVER") or "").strip()
email_password = (os.getenv("EMAIL_PASSWORD") or "").strip().replace(" ", "")
enable_toast = os.getenv("ENABLE_TOAST", "").strip().lower() in {"1", "true", "yes", "on"}
missing_email_vars = [
    name
    for name, value in {
        "EMAIL_SENDER": sender_email,
        "EMAIL_RECEIVER": receiver_email,
        "EMAIL_PASSWORD": email_password,
    }.items()
    if not value
]
print("✅ Email loaded:", not missing_email_vars)
if missing_email_vars:
    print("⚠️ Missing email environment variables:", ", ".join(missing_email_vars))

ISM_BASE_URL = "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports"
MONTH_SLUGS = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]


def recent_month_slugs(today, lookback_months=6):
    year = today.year
    month_index = today.month - 1
    for offset in range(lookback_months):
        index = month_index - offset
        report_year = year + index // 12
        slug = MONTH_SLUGS[index % 12]
        label = f"{slug.title()} {report_year}"
        yield slug, label


def fetch_url_text(url, params=None):
    response = requests.get(
        url,
        params=params,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
            )
        },
        timeout=20,
    )
    response.raise_for_status()
    return response.text


def extract_ism_pmi(page_text, label):
    text = html.unescape(re.sub(r"<[^>]+>", " ", page_text))
    text = re.sub(r"\s+", " ", text)
    pattern = rf"{re.escape(label)}\s*(?:®|\(R\))?\s*(?:at|registered)\s*([0-9]+(?:\.[0-9]+)?)\s*(?:percent|%)"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if match:
        return float(match.group(1))
    return None


def find_prnewswire_ism_release(month_label, indicator_label):
    query = f"{month_label} ISM {indicator_label} Report"
    page_text = fetch_url_text(
        "https://www.prnewswire.com/search/news/",
        params={"keyword": query},
    )
    slug_label = indicator_label.lower().replace(" ", "-")
    links = re.findall(r'href="([^"]+)"', page_text, flags=re.IGNORECASE)
    for link in links:
        if slug_label not in link.lower() or "ism" not in link.lower():
            continue
        if link.startswith("/"):
            return f"https://www.prnewswire.com{link}"
        if link.startswith("https://www.prnewswire.com/"):
            return link
    return None


def get_latest_ism_pmi(report_path, indicator_label):
    today = datetime.datetime.today()
    errors = []
    for month_slug, month_label in recent_month_slugs(today):
        candidate_urls = [f"{ISM_BASE_URL}/{report_path}/{month_slug}/"]
        try:
            prnewswire_url = find_prnewswire_ism_release(month_label, indicator_label)
        except Exception as exc:
            prnewswire_url = None
            errors.append(f"{month_label}: PR Newswire search failed: {exc}")
        if prnewswire_url:
            candidate_urls.append(prnewswire_url)

        for url in candidate_urls:
            try:
                page_text = fetch_url_text(url)
            except Exception as exc:
                errors.append(f"{month_label}: {exc}")
                continue
            value = extract_ism_pmi(page_text, indicator_label)
            if value is not None:
                print(f"ISM {indicator_label} loaded from {url}: {value:.1f} ({month_label})")
                return value
            errors.append(f"{month_label}: value not found at {url}")
    raise RuntimeError(
        f"Could not fetch ISM {indicator_label}. Tried recent ISM report pages. "
        f"Last errors: {'; '.join(errors[-3:])}"
    )


# === FRED Setup ===
fred = Fred(api_key="b300bb17176490feb3bdc9f571eb9712")
try:
    cpi_series = fred.get_series('CPIAUCSL').dropna()
    cpi_yoy = ((cpi_series.iloc[-1] / cpi_series.iloc[-13]) - 1) * 100
    unemp = float(fred.get_series('UNRATE').dropna().iloc[-1])
    fed_rate = float(fred.get_series('FEDFUNDS').dropna().iloc[-1])
    yield_curve = float(fred.get_series('T10Y2Y').dropna().iloc[-1])
    lei = float(fred.get_series('USSLIND').dropna().iloc[-1])
    nfci = float(fred.get_series('NFCI').dropna().iloc[-1])
except Exception as e:
    print("❌ FRED data error:", e)
    exit()

# === Yahoo Finance: VIX ===
try:
    vix_price = yf.Ticker("^VIX").history(period="5d")['Close'].dropna().iloc[-1]
except Exception as e:
    print("⚠️ VIX fetch failed:", e)
    vix_price = 20.0

# === ISM PMI data ===
try:
    ism_services = get_latest_ism_pmi("services", "Services PMI")
    ism_manu = get_latest_ism_pmi("pmi", "Manufacturing PMI")
except Exception as e:
    print("ISM data error:", e)
    exit()

# === Indicator scoring ===
def get_tilt(name, val):
    if name == "CPI YoY":
        return 1 if 1.5 <= val <= 3.0 else 0 if 3.0 < val <= 4.0 else -1
    elif name == "Unemployment Rate":
        return 1 if val < 4.5 else 0 if val <= 5.5 else -1
    elif name == "Fed Funds Rate":
        return 1 if val < 4.5 else 0 if val <= 5.5 else -1
    elif name == "Yield Curve (10Y–2Y)":
        return 1 if val > 0.25 else 0 if -0.25 <= val <= 0.25 else -1
    elif name == "VIX":
        return 1 if val < 18 else 0 if val <= 23 else -1
    elif name == "ISM Services PMI":
        return 1 if val > 50.2 else 0 if val >= 49.8 else -1
    elif name == "ISM Manufacturing PMI":
        return 1 if val > 50.2 else 0 if val >= 49.8 else -1
    elif name == "LEI":
        return 1 if val > 0.5 else 0 if val >= -0.5 else -1
    elif name == "NFCI":
        return 1 if val < -0.25 else 0 if val <= 0.25 else -1

def tilt_to_label(tilt):
    return {1: "Bullish", 0: "Neutral", -1: "Bearish"}[tilt]

# === Values and tilt evaluations ===
data = {
    "CPI YoY": cpi_yoy,
    "Unemployment Rate": unemp,
    "Fed Funds Rate": fed_rate,
    "Yield Curve (10Y–2Y)": yield_curve,
    "VIX": vix_price,
    "ISM Services PMI": ism_services,
    "ISM Manufacturing PMI": ism_manu,
    "LEI": lei,
    "NFCI": nfci
}

scored_data = {k: {"value": v, "tilt": get_tilt(k, v)} for k, v in data.items()}
tilt_total = sum(v["tilt"] for v in scored_data.values())
confidence = round((abs(tilt_total) / 9) * 100, 1)

if tilt_total >= 7:
    final_verdict = "Strongly Bullish"
elif tilt_total >= 4:
    final_verdict = "Bullish"
elif tilt_total >= 1:
    final_verdict = "Cautiously Bullish"
elif tilt_total == 0:
    final_verdict = "Neutral"
elif tilt_total <= -1 and tilt_total >= -3:
    final_verdict = "Cautiously Bearish"
elif tilt_total <= -4 and tilt_total >= -6:
    final_verdict = "Bearish"
else:
    final_verdict = "Strongly Bearish"

# === Summary Message ===
symbols = {'Bullish': '✅', 'Neutral': '⚖️', 'Bearish': '❌'}
today = datetime.datetime.today().strftime('%Y-%m-%d')
lines = [
    f"📅 {today}",
    f"📈 Macro Verdict: {final_verdict} (Confidence: {confidence}%)\n",
    "🔍 Indicator Summary:\n"
]
for k, v in scored_data.items():
    label = tilt_to_label(v["tilt"])
    emoji = symbols[label]
    lines.append(f"{emoji} {k}: {label} ({v['value']:.2f})")
lines.append(f"\n➡️ Total Bullish Tilt: {tilt_total} (out of 9)")
lines.append(f"➡️ Final Market Stance: {final_verdict}")
summary = "\n".join(lines)
print(summary)

# === Toast Notification ===
if enable_toast:
    try:
        from win10toast import ToastNotifier

        ToastNotifier().show_toast(
            f"Macro Verdict: {final_verdict} ({confidence}%)",
            f"CPI: {cpi_yoy:.2f}%, Yield Curve: {yield_curve:.2f}%, VIX: {vix_price:.2f}",
            duration=10,
            threaded=False
        )
    except Exception as e:
        print("⚠️ Toast error:", e)

# === Email Sending ===
if missing_email_vars:
    print("⚠️ Email skipped because credentials are not configured.")
else:
    message = MIMEMultipart("alternative")
    message["Subject"] = f"Weekly Macro Verdict: {final_verdict}"
    message["From"] = sender_email
    message["To"] = receiver_email
    message.attach(MIMEText(summary, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, email_password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        print("✅ Email sent.")
    except smtplib.SMTPAuthenticationError:
        print("❌ Email failed: Gmail rejected EMAIL_SENDER/EMAIL_PASSWORD.")
        print("Use a Gmail app password in .env, not your normal Google account password.")
    except Exception as e:
        print("❌ Email failed:", e)

# === Save to file ===
try:
    with open("macro_verdict.txt", "w", encoding="utf-8") as f:
        f.write(summary)
    print("📄 Verdict saved.")
except Exception as e:
    print("❌ Failed to write file:", e)
