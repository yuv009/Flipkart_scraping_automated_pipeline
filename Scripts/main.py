#main.py

import os
# Import the functions from your other scripts
from scraper_module import scrape_reviews_for_url
from cleaning import clean_samsung_reviews # Assuming your cleaning file is named cleaning.py

def run_pipeline():
    """
    The main function to orchestrate the scraping and cleaning workflow.
    """
    print("--- Flipkart Review Pipeline ---")
    print("Please paste the Flipkart product URLs you want to scrape.")
    print("Enter each URL on a new line. Type 'done' when you are finished.")

    urls_to_scrape = []
    while True:
        line = input("> ")
        if line.lower() == 'done':
            break
        if line.strip().startswith("http"):
            urls_to_scrape.append(line.strip())
        else:
            print("Invalid input. Please enter a valid URL or 'done'.")

    if not urls_to_scrape:
        print("No URLs provided. Exiting.")
        return

    print(f"\nFound {len(urls_to_scrape)} URLs. Starting scraping process...")
    
    raw_data_folder = "raw_reviews"
    raw_csv_paths = []
    
    for url in urls_to_scrape:
        print(f"\n--- Scraping URL: {url[:60]}... ---")
        # The scraper function now saves the file into the 'raw_reviews' folder
        file_path = scrape_reviews_for_url(url, raw_data_folder)
        if file_path:
            raw_csv_paths.append(file_path)
        else:
            print(f"Warning: Scraping failed for URL: {url}")
    
    if not raw_csv_paths:
        print("\nScraping finished, but no data files were created. Exiting.")
        return

    print(f"\nScraping complete. Collected {len(raw_csv_paths)} raw data file(s).")

    # 3. Clean each of the scraped files
    print("\n--- Starting cleaning process ---")
    cleaned_output_folder = "cleaned_reviews"

    for raw_path in raw_csv_paths:
        print(f"\n--- Cleaning file: {os.path.basename(raw_path)} ---")
        try:
            # Your cleaning function takes the raw file path and saves a new cleaned file
            clean_samsung_reviews(csv_path=raw_path, output_folder=cleaned_output_folder)
        except Exception as e:
            print(f"Error cleaning file {raw_path}: {e}")

    print("\n--- Pipeline Finished ---")
    print(f"All raw data is in the '{raw_data_folder}' folder.")
    print(f"All cleaned data is in the '{cleaned_output_folder}' folder.")

# This makes the script runnable from the command line
if __name__ == "__main__":
    run_pipeline()