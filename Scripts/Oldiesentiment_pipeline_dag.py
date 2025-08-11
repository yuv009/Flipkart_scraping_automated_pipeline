from __future__ import annotations
import os
import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.models.dagrun import DagRun

# Import the refactored functions
from scraper_module import scrape_reviews_for_url
from cleaning import clean_raw_review_data

# Define folder paths for the data volume
RAW_DATA_FOLDER = "/opt/airflow/data/raw"
CLEANED_DATA_FOLDER = "/opt/airflow/data/cleaned"
FINAL_DATA_FOLDER = "/opt/airflow/data/final"


@dag(
    dag_id="flipkart_sentiment_pipeline_v8_branching",
    start_date=pendulum.datetime(2025, 7, 26, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    tags=["sentiment_analysis", "scraping"],
    doc_md="""
    ### Final Branching Pipeline (v8)
    Uses a Branching Operator to dynamically choose between parallel and sequential execution based on the trigger configuration.
    """
)
def flipkart_sentiment_pipeline_v8_branching():
    
    # Task to get the list of URLs from the config
    @task
    def get_urls_to_scrape(**context) -> list:
        """Gets the list of URLs from the DAG run config."""
        dag_run: DagRun = context["dag_run"]
        conf = dag_run.conf or {}
        urls = conf.get("urls", ["https://www.flipkart.com/apple-iphone-15-black-128-gb/p/itme5f540d559560"])
        return urls

    # The actual task definitions are the same
    @task
    def scrape_task(url: str) -> str | None:
        return scrape_reviews_for_url(phone_url=url, output_folder=RAW_DATA_FOLDER)

    @task
    def clean_task(raw_file_path: str) -> str | None:
        if not raw_file_path: return None
        return clean_raw_review_data(csv_path=raw_file_path, output_folder=CLEANED_DATA_FOLDER)

    @task
    def extract_reviews_only_task(cleaned_file_path: str):
        if not cleaned_file_path: return
        df = pd.read_csv(cleaned_file_path)
        if 'Review' not in df.columns: return
        
        input_filename = os.path.basename(cleaned_file_path)
        final_filename = input_filename.replace('_CLEANED.csv', '_reviews_only.csv')
        output_path = os.path.join(FINAL_DATA_FOLDER, final_filename)
        os.makedirs(FINAL_DATA_FOLDER, exist_ok=True)
        
        df[['Review']].to_csv(output_path, index=False)
        print(f"Saved final review-only data to: {output_path}")

    # NEW: A special branching task
    @task.branch
    def branch_on_mode(**context) -> str:
        """
        Decides which path to take based on the 'mode' config.
        It must return the task_id of the next task to run.
        """
        dag_run: DagRun = context["dag_run"]
        conf = dag_run.conf or {}
        mode = conf.get("mode", "sequential")
        
        if mode == "parallel":
            return "run_pipeline_parallel"
        else:
            return "run_pipeline_sequential"

    # NEW: A task that defines the parallel workflow
    @task
    def run_pipeline_parallel(urls: list):
        # The .expand() calls now happen inside this task
        scraped_paths = scrape_task.expand(url=urls)
        cleaned_paths = clean_task.expand(raw_file_path=scraped_paths)
        extract_reviews_only_task.expand(cleaned_file_path=cleaned_paths)

    # NEW: A task that defines the sequential workflow
    @task
    def run_pipeline_sequential(urls: list):
        # The for-loop now happens inside this task
        for url in urls:
            scraped_path = scrape_task(url=url)
            cleaned_path = clean_task(raw_file_path=scraped_path)
            extract_reviews_only_task(cleaned_file_path=cleaned_path)

    # --- Define the final workflow ---
    urls_to_process = get_urls_to_scrape()
    branch_task = branch_on_mode()
    
    # Connect the branching task to the two possible paths
    parallel_path = run_pipeline_parallel(urls_to_process)
    sequential_path = run_pipeline_sequential(urls_to_process)
    
    branch_task >> [parallel_path, sequential_path]


flipkart_sentiment_pipeline_v8_branching()