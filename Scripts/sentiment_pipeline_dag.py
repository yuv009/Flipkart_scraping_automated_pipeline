# dags/sentiment_pipeline_dag.py

from __future__ import annotations
import os
import pendulum
from airflow.decorators import dag, task

# Import functions from all three of your scripts
from scraper_module import scrape_reviews_for_url
from cleaning import clean_samsung_reviews
from emoji_cleaner import clean_emoji_from_reviews

# Define folder paths for use inside the Airflow containers
RAW_DATA_FOLDER = "/opt/airflow/data/raw"
CLEANED_DATA_FOLDER = "/opt/airflow/data/cleaned"
FINAL_DATA_FOLDER = "/opt/airflow/data/final"


@dag(
    dag_id="flipkart_sentiment_pipeline_v2",
    start_date=pendulum.datetime(2025, 7, 24, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    tags=["sentiment_analysis", "scraping"],
    doc_md="""
    ### Flipkart Review Analysis Pipeline (v2)
    A robust three-stage pipeline:
    1.  **Scrape Task**: Scrapes raw review data.
    2.  **Initial Clean Task**: Performs comprehensive cleaning (prices, dates, regex-based emoji removal).
    3.  **Final Emoji Clean Task**: Runs a second, library-based emoji removal for maximum thoroughness.
    """
)
def flipkart_sentiment_pipeline_v2():
    
    @task
    def scrape_task(url: str) -> str | None:
        """Task to scrape reviews. Returns the path to the raw file."""
        print(f"Starting scrape for URL: {url}")
        file_path = scrape_reviews_for_url(phone_url=url, output_folder=RAW_DATA_FOLDER)
        return file_path

    @task
    def initial_clean_task(raw_file_path: str) -> str | None:
        """
        Task to run the main cleaning script.
        Returns the path to the intermediate cleaned file.
        """
        if not raw_file_path:
            print("Scraping failed, skipping initial cleaning.")
            return None
            
        print(f"Starting initial cleaning for file: {raw_file_path}")
        clean_samsung_reviews(csv_path=raw_file_path, output_folder=CLEANED_DATA_FOLDER)
        
        # Construct the output path to pass to the next task
        input_filename = os.path.basename(raw_file_path)
        clean_filename = input_filename.replace('.csv', '_CLEANED.csv')
        output_path = os.path.join(CLEANED_DATA_FOLDER, clean_filename)
        return output_path

    @task
    def final_emoji_clean_task(cleaned_file_path: str):
        """Task to run the final, dedicated emoji cleaner."""
        if not cleaned_file_path:
            print("Initial cleaning failed, skipping final emoji cleaning.")
            return

        print(f"Starting final emoji cleanup for: {cleaned_file_path}")
        # The function saves the output in the final_reviews folder
        clean_emoji_from_reviews(input_path=cleaned_file_path, output_path=FINAL_DATA_FOLDER)

    # --- Define the Workflow ---
    flipkart_url = "https://www.flipkart.com/apple-iphone-15-black-128-gb/p/itme5f540d559560"

    # Stage 1: Scrape
    scraped_path = scrape_task(url=flipkart_url)
    
    # Stage 2: Initial Clean
    cleaned_path = initial_clean_task(raw_file_path=scraped_path)
    
    # Stage 3: Final Emoji Clean
    final_emoji_clean_task(cleaned_file_path=cleaned_path)


flipkart_sentiment_pipeline_v2()