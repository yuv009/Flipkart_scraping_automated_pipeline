# dags/parallel_pipeline_dag.py
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
    dag_id="parallel_scraping_pipeline",
    start_date=pendulum.datetime(2025, 7, 26, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    max_active_tasks=3,
    tags=["sentiment_analysis", "parallel", "database"],
)
def parallel_pipeline():
    
    create_table_task = PostgresOperator(
        task_id="create_reviewer_data_table",
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
    def get_urls(**context) -> list:
        dag_run: DagRun = context["dag_run"]
        unique_urls = list(set(dag_run.conf.get("urls", [])))
        print(f"Processing {len(unique_urls)} unique URLs.")
        return unique_urls

    @task
    def scrape_task(url: str):
        return scrape_reviews_for_url(phone_url=url, output_folder=RAW_DATA_FOLDER)

    @task
    def clean_task(raw_file_path: str):
        if not raw_file_path: return None
        return clean_raw_review_data(csv_path=raw_file_path, output_folder=CLEANED_DATA_FOLDER)

    @task
    def load_to_postgres(cleaned_file_path: str):
        if not cleaned_file_path: return
        write_to_postgres(file_path=cleaned_file_path, table_name=TABLE_NAME)

    urls_to_process = get_urls()
    scraped_paths = scrape_task.expand(url=urls_to_process)
    cleaned_paths = clean_task.expand(raw_file_path=scraped_paths)
    load_tasks = load_to_postgres.expand(cleaned_file_path=cleaned_paths)
    
    create_table_task >> urls_to_process >> scraped_paths >> cleaned_paths >> load_tasks

parallel_pipeline()