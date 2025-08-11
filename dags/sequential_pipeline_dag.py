# dags/sequential_pipeline_dag.py
from __future__ import annotations
import pendulum
from airflow.decorators import dag, task
from airflow.models.dagrun import DagRun
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scraper_module import scrape_reviews_for_url
from cleaning import clean_raw_review_data
from db_module import write_to_postgres

RAW_DATA_FOLDER = "/opt/airflow/data/raw"
CLEANED_DATA_FOLDER = "/opt/airflow/data/cleaned"
TABLE_NAME = "flipkart_reviewer_data"

@dag(
    dag_id="sequential_scraping_pipeline",
    start_date=pendulum.datetime(2025, 7, 26, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    tags=["sentiment_analysis", "sequential", "database"],
)
def sequential_pipeline():

    create_table_task = PostgresOperator(
        task_id="create_reviewer_data_table_seq",
        postgres_conn_id="postgres_default",
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            "Phone_Name" TEXT, "Price" FLOAT(53), "Review_ID" TEXT,
            "Author" TEXT, "Location" TEXT, "Rating" BIGINT, "Title" TEXT,
            "Review" TEXT, "Month" TEXT, "Year" FLOAT(53)
        );
        """
    )

    @task
    def get_urls_from_config(**context) -> list:
        dag_run: DagRun = context["dag_run"]
        unique_urls = list(set(dag_run.conf.get("urls", [])))
        print(f"Processing {len(unique_urls)} unique URLs.")
        return unique_urls

    @task
    def process_urls_sequentially(urls: list):
        if not urls:
            print("No URLs provided to process.")
            return

        for url in urls:
            print(f"--- Starting sequential processing for: {url} ---")
            raw_path = scrape_reviews_for_url(phone_url=url, output_folder=RAW_DATA_FOLDER)
            if not raw_path: continue
            
            cleaned_path = clean_raw_review_data(csv_path=raw_path, output_folder=CLEANED_DATA_FOLDER)
            if not cleaned_path: continue
            
            write_to_postgres(file_path=cleaned_path, table_name=TABLE_NAME)

    urls_to_process = get_urls_from_config()
    
    create_table_task >> urls_to_process >> process_urls_sequentially(urls=urls_to_process)

sequential_pipeline()