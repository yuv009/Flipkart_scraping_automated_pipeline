from __future__ import annotations
import os
import pendulum
import pandas as pd
from airflow.decorators import dag, task

# Import the refactored functions
from scraper_module import scrape_reviews_for_url
from cleaning import clean_raw_review_data

# Define folder paths for the data volume
RAW_DATA_FOLDER = "/opt/airflow/data/raw"
CLEANED_DATA_FOLDER = "/opt/airflow/data/cleaned"
FINAL_DATA_FOLDER = "/opt/airflow/data/final"


@dag(
    dag_id="flipkart_sentiment_pipeline_v3",
    start_date=pendulum.datetime(2025, 7, 25, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    tags=["sentiment_analysis", "scraping"],
    doc_md="""
    ### Refactored Flipkart Review Analysis Pipeline (v3)
    A clean, three-stage pipeline:
    1.  **Scrape Task**: Scrapes raw review data from a URL.
    2.  **Clean Task**: Performs comprehensive cleaning (prices, dates, emojis).
    3.  **Extract Reviews Task**: Creates a final CSV with only the review text for analysis.
    """
)
def flipkart_sentiment_pipeline_v3():
    
    @task
    def scrape_task(url: str) -> str | None:
        """Task to scrape reviews. Returns the path to the raw file."""
        print(f"Starting scrape for URL: {url}")
        file_path = scrape_reviews_for_url(phone_url=url, output_folder=RAW_DATA_FOLDER)
        return file_path

    @task
    def clean_task(raw_file_path: str) -> str | None:
        """Task to run the main cleaning script and return the cleaned file path."""
        if not raw_file_path:
            print("Scraping failed, skipping cleaning.")
            return None
        
        cleaned_path = clean_raw_review_data(csv_path=raw_file_path, output_folder=CLEANED_DATA_FOLDER)
        return cleaned_path

    @task
    def extract_reviews_only_task(cleaned_file_path: str) -> str | None:
        """Task to create a final data layer with only the review text."""
        if not cleaned_file_path:
            print("Cleaning failed, skipping final extraction.")
            return None
            
        print(f"Extracting reviews from: {cleaned_file_path}")
        df = pd.read_csv(cleaned_file_path)
        
        # Ensure 'Review' column exists
        if 'Review' not in df.columns:
            print("Error: 'Review' column not found in cleaned file.")
            return None
            
        # Create the output path in the 'final' folder
        input_filename = os.path.basename(cleaned_file_path)
        final_filename = input_filename.replace('_CLEANED.csv', '_reviews_only.csv')
        output_path = os.path.join(FINAL_DATA_FOLDER, final_filename)
        os.makedirs(FINAL_DATA_FOLDER, exist_ok=True)
        
        # Save just the 'Review' column
        df[['Review']].to_csv(output_path, index=False)
        print(f"Saved final review-only data to: {output_path}")
        return output_path

    # --- Define the Workflow ---
    flipkart_url = "https://www.flipkart.com/apple-iphone-15-black-128-gb/p/itme5f540d559560"

    # Stage 1: Scrape
    scraped_path = scrape_task(url=flipkart_url)
    
    # Stage 2: Clean
    cleaned_path = clean_task(raw_file_path=scraped_path)
    
    # Stage 3: Extract
    extract_reviews_only_task(cleaned_file_path=cleaned_path)


flipkart_sentiment_pipeline_v3()