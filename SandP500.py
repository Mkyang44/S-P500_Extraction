import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import yfinance as yf

# Suppress parser warning
from bs4 import XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# HTTP headers for SEC
HEADERS = {
    "User-Agent": "MichaelResearchBot/1.0 (mkyang334@gmail.com)",
    "Accept-Encoding": "gzip, deflate"
}

EDGAR_SEARCH = "https://www.sec.gov/cgi-bin/browse-edgar"
EDGAR_ARCHIVES = "https://www.sec.gov"

# Load full S&P 500 list (DataHub CSV)
print("🌐 Fetching full S&P 500 list...")
sp500_csv_url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
sp500_df = pd.read_csv(sp500_csv_url)
sp500_df['Symbol'] = sp500_df['Symbol'].str.upper()

# Download SEC ticker → CIK mapping
print("🌐 Downloading SEC ticker to CIK mapping...")
sec_tickers_url = "https://www.sec.gov/files/company_tickers.json"
resp = requests.get(sec_tickers_url, headers=HEADERS)
resp.raise_for_status()
ticker_cik_map = {item['ticker'].upper(): str(item['cik_str']).zfill(10) for item in resp.json().values()}

sp500_df['CIK'] = sp500_df['Symbol'].map(ticker_cik_map)
sp500_df = sp500_df.dropna(subset=['CIK'])

# Fetch latest trading volume for each ticker using yfinance
print("⏳ Fetching latest trading volume for all tickers (this may take a few minutes)...")
volumes = []
for ticker in sp500_df['Symbol']:
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d")
        volume = hist['Volume'].iloc[0] if not hist.empty else 0
    except Exception:
        volume = 0
    volumes.append(volume)
    time.sleep(0.1)

sp500_df['Volume'] = volumes

# Select top 150 most active tickers by volume
top150_active = sp500_df.sort_values('Volume', ascending=False).head(150).reset_index(drop=True)

print(f"✅ Selected top 150 active companies by volume.")

# SEC 10-K filing scraping functions

def get_latest_10k_url_html(cik):
    params = {
        "CIK": cik,
        "type": "10-K",
        "count": "10",
        "action": "getcompany"
    }
    r = requests.get(EDGAR_SEARCH, headers=HEADERS, params=params)
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="tableFile2")
    if not table:
        return None

    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if len(cells) >= 4 and "10-K" in cells[0].text:
            href = cells[1].find("a")["href"]
            detail_url = EDGAR_ARCHIVES + href
            return get_filing_full_text_url(detail_url)
    return None

def get_filing_full_text_url(detail_url):
    r = requests.get(detail_url, headers=HEADERS)
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="tableFile", summary="Document Format Files")
    if not table:
        return None

    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if len(cells) >= 4 and "10-K" in cells[3].text.upper():
            href = cells[2].find("a")["href"]
            return EDGAR_ARCHIVES + href

    first_link = table.find("a", href=True)
    return EDGAR_ARCHIVES + first_link["href"] if first_link else None

def download_10k_text(filing_url):
    r = requests.get(filing_url, headers=HEADERS)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "lxml")
    return soup.get_text(separator=" ", strip=True)

def extract_international_revenue(text):
    matches = re.findall(
        r"(?i)(Revenue|Net Sales).*?(International|Outside U\.S\.|Foreign).*?\$?[\d,.]+",
        text
    )
    return [" ".join(match) for match in matches]

def extract_total_revenue(text):
    matches = re.findall(
        r"(?i)(Total\s+Revenue|Revenue|Net\s+Sales)(?!.*International).*?\$?[\d,.]+",
        text
    )
    return matches

# Process each top 150 active company

results = []

for idx, row in top150_active.iterrows():
    ticker, cik = row["Symbol"], row["CIK"]
    print(f"\n🔍 Processing {ticker} (CIK: {cik}) [{idx+1}/150]...")
    filing_url = get_latest_10k_url_html(cik)
    if not filing_url:
        print(f"[!] No 10-K filing found for {ticker}")
        results.append({
            "Ticker": ticker,
            "Type": "Filing",
            "Extract": "No 10-K filing found",
            "File": None
        })
        continue

    print(f"📄 Downloading filing: {filing_url}")
    text = download_10k_text(filing_url)
    if not text:
        print(f"[!] Failed to download 10-K text for {ticker}")
        results.append({
            "Ticker": ticker,
            "Type": "Filing",
            "Extract": "Failed to download 10-K text",
            "File": filing_url
        })
        continue

    intl_lines = extract_international_revenue(text)
    total_lines = extract_total_revenue(text)

    if intl_lines:
        for line in intl_lines:
            results.append({
                "Ticker": ticker,
                "Type": "International Revenue",
                "Extract": line,
                "File": filing_url
            })
    else:
        results.append({
            "Ticker": ticker,
            "Type": "International Revenue",
            "Extract": "No match found",
            "File": filing_url
        })

    if total_lines:
        for line in total_lines:
            results.append({
                "Ticker": ticker,
                "Type": "Total Revenue",
                "Extract": line,
                "File": filing_url
            })
    else:
        results.append({
            "Ticker": ticker,
            "Type": "Total Revenue",
            "Extract": "No match found",
            "File": filing_url
        })

    time.sleep(2)  # SEC rate limiting

# Save results
df = pd.DataFrame(results)
output_file = "revenue_data_top150_active.csv"
df.to_csv(output_file, index=False)
print(f"\n✅ Done! Results saved to: {output_file}")
