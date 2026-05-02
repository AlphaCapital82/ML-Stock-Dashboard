# === macro_verdict.py ===

import pandas as pd
from fredapi import Fred
import yfinance as yf
import smtplib
import sys
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

# === Static or scraped PMI data ===
ism_services = 50.80
ism_manu = 49.50

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
