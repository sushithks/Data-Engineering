import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
import random

def extract_links(url):
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",

    ]
    headers = {'User-Agent': random.choice(USER_AGENTS)}  # Randomly choose a user-agent


    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract and resolve links
        links = [urljoin(url, a["href"]) for a in soup.find_all("a", href=True)]
        return links

    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []

website_url = "https://ca.news.yahoo.com/"  # Change this to your target site

extracted_links = extract_links(website_url)

if extracted_links:
    df = pd.DataFrame(extracted_links, columns=["Links"])
    df.to_csv("extracted_links.csv", index=False)
    print("Links saved to extracted_links.csv")
else:
    print("No links found.")
