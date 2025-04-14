import time
import pandas as pd
from selenium import webdriver

driver = webdriver.Chrome()


def fetch_yahoo_finance(links_csv):
    # Read links from CSV
    df = pd.read_csv(links_csv)

    # Ensure column name is correct
    if "Data" not in df.columns:
        print("CSV does not have a 'Links' column.")
        return

    links = df["Links"].tolist()

    for index, url in enumerate(links, start=1):
        print(f"Processing {index}/{len(links)}: {url}")

        try:
            driver.get(url)
            time.sleep(10)

            # Actual task
            perform_task()

            print(f"Task completed for {url}\n")

        except Exception as e:
            print(f"Error processing {url}: {e}")

    driver.quit()
    print("All links processed!")


def perform_task():
    # Call the JS from here.
    print("Performing the required task...")

fetch_yahoo_finance("extracted_links.csv")
