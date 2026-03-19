import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

start_time = time.time()

# Load and clean URLs from pl.txt
def load_urls(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return [
            line.strip().replace('\u200b', '').replace('\r', '').replace('\n', '')
            for line in f if line.strip()
        ]

pl = load_urls("pl.txt")
##pl1 = load_urls("pl1.txt")
##pl = [url for url in pl if url not in pl1]


# Realistic browser headers to avoid 403 errors
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive"
}

# Function to extract player information
def extract_player_info(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=1)
        if response.status_code != 200:
            return [url, None, None, None]

        soup = BeautifulSoup(response.text, 'html.parser')

        # Player name
        name_block = soup.select_one(".data-header__headline-wrapper")
        name_lines = name_block.get_text("\n", strip=True).split("\n") if name_block else []
        player_name = name_lines[1] if len(name_lines) > 1 else None

        # Current team
        team_block = soup.select_one(".data-header__club")
        team_lines = team_block.get_text("\n", strip=True).split("\n") if team_block else []
        current_team = team_lines[0]

        # Date joined
        content_blocks = soup.select(".data-header__content")
        d = content_blocks[1].get_text(strip=True) if len(content_blocks) > 1 else None


        return [url, player_name, current_team, d]
    except Exception as e:
        print(f"Error parsing {url}: {e}")
        return [url, None, None, None]

# Initialize the DataFrame
exim_table = pd.DataFrame([extract_player_info(pl[0])], columns=["player", "name", "team", "joined"])

# Add empty rows
for i in range(1, len(pl)):
# for i in range(1, 20):
    exim_table.loc[i] = [pl[i], None, None, None]

k1 = 0

MAX_THREADS = 40


def run_parallel_pass(url_list, description="initial"):
    results = []
    total = len(url_list)
    print(f"\n🚀 Starting {description} parallel scrape of {total} URLs...")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for i, result in enumerate(executor.map(extract_player_info, url_list), 1):
            results.append(result)
            print(f"\rProgress: [{i}/{total}] {(i / total) * 100:.1f}%", end="")
    return results


# --- PASS 1: The main bulk ---
first_pass_results = run_parallel_pass(pl, "first")

# Create temporary DataFrame to find failures
df_temp = pd.DataFrame(first_pass_results, columns=["player", "name", "team", "joined"])

# --- PASS 2: The Retry ---
# Identify URLs where 'team' is None (failed requests)
failed_indices = df_temp[df_temp["team"].isna()].index.tolist()
failed_urls = [pl[i] for i in failed_indices]

attempt_count = 1

while len(failed_urls) > 0:
    print(f"\n\n⚠️ Attempt {attempt_count}: {len(failed_urls)} items still missing. Retrying...")

    # Wait to let the server "cool down" - increase wait time each attempt
    wait_time = 1
    print(f"Waiting {wait_time}s before next parallel pass...")
    time.sleep(wait_time)

    # Run a parallel pass only for the failed URLs
    retry_results = run_parallel_pass(failed_urls, f"retry-attempt-{attempt_count}")

    # Update the main DataFrame with any successful new data
    for idx, new_data in zip(failed_indices, retry_results):
        if new_data and new_data[2] is not None:  # Check if 'team' was actually found
            df_temp.iloc[idx] = new_data

    # Re-evaluate: how many are still missing?
    failed_indices = df_temp[df_temp["team"].isna()].index.tolist()
    failed_urls = [pl[i] for i in failed_indices]
    attempt_count += 1

exim_table = df_temp
print("\nAll passes complete.")

#
# # Retry mechanism
# while exim_table["team"].isna().sum() > 0:
#     t = exim_table[exim_table["team"].isna()].index.tolist()
#     k1 += 1
#     k = 0
#     m = sum(t[:3]) if len(t) >= 3 else sum(t)
#
#     for i in t:
#         l1 = exim_table["team"].isna().sum()
#         print(f"Retrying index {i}, Remaining NAs: {l1}")
#         url = pl[i]
#
#         try:
#             response = requests.get(url, headers=HEADERS, timeout=3)
#             if response.status_code == 200:
#                 exim_table.loc[i] = extract_player_info(url)
#                 k = 0
#             else:
#                 k += 1
#         except Exception as e:
#             k += 1
#             print(f"Exception on index {i} ({k}): {e}")
#
#
#         # time.sleep(random.uniform(1, 2))  # Small randomized pause
#         # time.sleep(4)
#
#     current_na_indices = exim_table[exim_table["team"].isna()].index.tolist()
#     print(f"End of loop: {k1}")
#     if k1 == 10:
#         break
#     if len(current_na_indices) >= 3 and m == sum(current_na_indices[:3]):
#         time.sleep(5)
#     else:
#         time.sleep(1)
#
# Convert joined column to date safely
def parse_date_safe(datestr):
    try:
        # Parse from dd.mm.yyyy format
        dt = datetime.strptime(datestr, "%d.%m.%Y")
        return dt.strftime("%d/%m/%Y")  # Keep final output as dd/mm/yyyy
    except Exception:
        return None

# exim_table["joined"] = exim_table["joined"].apply(parse_date_safe)
exim_table["joined"] = pd.to_datetime(exim_table["joined"], format="%d/%m/%Y", errors="coerce")

exim_table = exim_table.sort_values(by="joined", ascending=False).reset_index(drop=True)

# Save output
exim_table.to_csv("exim_table.txt", sep="\t", index=False)

end_time = time.time()
total_seconds = end_time - start_time

# Format as minutes and seconds
mins, secs = divmod(total_seconds, 60)
print(f"\n⏱️ Total runtime: {int(mins)} minutes {int(secs)} seconds")
print(datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S'))
