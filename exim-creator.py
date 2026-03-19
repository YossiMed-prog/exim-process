import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

start_time = time.time()

# Configuration
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}
MAX_THREADS = 10  # Reduced slightly to improve stability during retries
BASE_URL = "https://www.transfermarkt.com"


def get_links_with_retry(url, selector, pattern, retries=3):
    """Fetch a page with automatic retries on timeouts or throttling."""
    for attempt in range(retries):
        try:
            # Increased timeout to 20 seconds to handle slow server responses
            response = requests.get(url, headers=HEADERS, timeout=20)

            # Handle Throttling (Too Many Requests)
            if response.status_code == 429:
                wait_time = (attempt + 1) * 10
                print(f"\n⚠️ Throttled (429) on {url}. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue

            if response.status_code != 200:
                return []

            soup = BeautifulSoup(response.content, "html.parser")
            links = [a['href'] for a in soup.select(f"{selector} a") if a.has_attr('href')]
            return [BASE_URL + l for l in links if pattern in l]

        except (requests.exceptions.Timeout, requests.exceptions.RequestException):
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 3
                print(f"\r🔄 Timeout on {url}. Retry {attempt + 1}/{retries} in {wait_time}s...", end="")
                time.sleep(wait_time)
            else:
                print(f"\n❌ Failed to load {url} after {retries} attempts.")
                return []
    return []


# --- Step 1: Country Discovery ---
try:
    # Reading initial URLs from your exim.txt file
    with open("C:/Rtest/exim.txt", "r") as f:
        start_urls = [line.strip() for line in f if line.strip()]
except FileNotFoundError:
    print("Error: Could not find exim.txt at C:/Rtest/")
    start_urls = []

print(f"Fetching countries from {len(start_urls)} sources...")
country_urls = []
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    # Extracting country links containing 'land_id'
    results = executor.map(lambda u: get_links_with_retry(u, ".zentriert", "land_id"), start_urls)
    for res in results:
        country_urls.extend(res)

country_urls = list(set(country_urls))
print(f"Found {len(country_urls)} unique country URLs.")

# --- Step 2: Player Discovery ---
print("Fetching player profile links...")
player_urls = []
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    # Extracting player profile links using the '.hauptlink' selector
    results = executor.map(lambda u: get_links_with_retry(u, ".hauptlink", "/profil/spieler/"), country_urls)
    for i, res in enumerate(results, 1):
        player_urls.extend(res)
        print(f"\rProgress: [{i}/{len(country_urls)}] countries processed", end="")

# --- Step 3: Clean and Save ---
player_urls = list(set(player_urls))
if player_urls:
    player_urls.pop()  # Removes last element per your R logic

# Saving output to pl.txt
with open("pl.txt", "w", encoding="utf-8") as f:
    for url in player_urls:
        f.write(f"{url}\n")

print(f"\n✅ Final count: {len(player_urls)} player URLs saved to pl.txt.")
