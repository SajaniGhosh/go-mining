import requests
import csv
import os
import time

# --- Configuration ---

# The API endpoint for searching repositories
API_URL = "https://api.github.com/search/repositories"

# Output CSV file
OUTPUT_FILE = "go_filtered_repositories.csv"

# GitHub hard-limits search results to 1,000 per query.
# To get 10,000, we perform multiple searches using star-count ranges.
TARGET_TOTAL = 10000
RESULTS_PER_PAGE = 100
PAGES_PER_QUERY = 10  # 10 pages * 100 per page = 1,000 limit

# --- Authentication ---

TOKEN = os.environ.get('GITHUB_TOKEN')

if not TOKEN:
    print("Warning: GITHUB_TOKEN environment variable not set.")
    headers = {"Accept": "application/vnd.github.v3+json"}
else:
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {TOKEN}"
    }

# --- Main Script ---

def fetch_repositories():
    """
    Fetches up to 10,000 Go repositories by iterating through star ranges
    to bypass the GitHub API's 1,000-result limit per query.
    """
    csv_headers = [
        "name", "full_name", "url", "description", "stars",
        "forks", "open_issues", "created_at", "updated_at", "license_name"
    ]
    
    total_saved = 0
    # We start with a very high number of stars and work our way down
    current_max_stars = 500000 
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(csv_headers)
        
        print(f"Targeting {TARGET_TOTAL} repositories...")

        while total_saved < TARGET_TOTAL:
            print(f"\nSearching for repos with stars <= {current_max_stars}...")
            
            last_star_count = current_max_stars
            
            for page in range(1, PAGES_PER_QUERY + 1):
                # Query logic: language is Go, stars are within our current window
                query = f"language:go stars:<={current_max_stars}"
                params = {
                    "q": query,
                    "sort": "stars",
                    "order": "desc",
                    "per_page": RESULTS_PER_PAGE,
                    "page": page
                }
                
                try:
                    response = requests.get(API_URL, headers=headers, params=params)
                    
                    if response.status_code == 403:
                        print("Rate limit hit. Sleeping for 60 seconds...")
                        time.sleep(60)
                        continue
                    elif response.status_code != 200:
                        print(f"Error {response.status_code}: {response.text}")
                        break

                    data = response.json()
                    items = data.get('items', [])
                    
                    if not items:
                        break
                    
                    for repo in items:
                        if total_saved >= TARGET_TOTAL:
                            break
                            
                        stars = repo.get('stargazers_count', 0)
                        writer.writerow([
                            repo.get('name'),
                            repo.get('full_name'),
                            repo.get('html_url'),
                            str(repo.get('description', '')).replace("\n", " "),
                            stars,
                            repo.get('forks_count'),
                            repo.get('open_issues_count'),
                            repo.get('created_at'),
                            repo.get('updated_at'),
                            (repo.get('license') or {}).get('name', 'N/A')
                        ])
                        total_saved += 1
                        # Track the lowest star count in this batch to move the window
                        last_star_count = stars

                    print(f"Progress: {total_saved}/{TARGET_TOTAL} (Current batch lowest stars: {last_star_count})")
                    
                    if total_saved >= TARGET_TOTAL:
                        break
                    
                    # Short pause to be kind to the API
                    time.sleep(2)

                except Exception as e:
                    print(f"Request failed: {e}")
                    break
            
            # Move the star window down. 
            # If the last star count we saw is the same as the current max, 
            # we subtract 1 to ensure we make progress and don't fetch duplicates.
            if last_star_count >= current_max_stars:
                current_max_stars -= 1
            else:
                current_max_stars = last_star_count

            if current_max_stars <= 0:
                print("Reached 0 stars. No more repositories to fetch.")
                break

    print(f"\nDone. Saved {total_saved} repositories to {OUTPUT_FILE}")

if __name__ == "__main__":
    fetch_repositories()
