# dags/db_module.py

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def get_db_engine() -> Engine:
    """Creates and returns a SQLAlchemy engine connected to the database."""
    conn_string = "postgresql+psycopg2://shopinion:yuvraj%4012345@flipkart-pipeline-db.postgres.database.azure.com/postgres"
    return create_engine(conn_string)

def write_to_postgres(file_path: str, table_name: str):
    """
    Reads a CSV file, filters out reviews that already exist in the target table
    based on 'Review_ID', and appends only the new reviews.
    """
    if not file_path:
        print("No file path provided. Skipping database write.")
        return

    print(f"Preparing to write {file_path} to PostgreSQL table '{table_name}'...")
    engine = get_db_engine()
    df_new_reviews = pd.read_csv(file_path)

    if 'Review_ID' not in df_new_reviews.columns:
        print("❌ 'Review_ID' column not found in the CSV. Cannot perform deduplication.")
        return

    try:
        with engine.connect() as connection:
            # Step 1: Fetch existing Review_IDs from the database for fast checking.
            # The "text()" function is used to ensure the table name is correctly interpreted.
            query = text(f'SELECT DISTINCT "Review_ID" FROM {table_name}')
            existing_ids = pd.read_sql(query, connection)
            existing_id_set = set(existing_ids['Review_ID'])
            print(f"Found {len(existing_id_set)} existing review IDs in the database.")

            # Step 2: Filter the new dataframe to exclude existing Review_IDs.
            initial_rows = len(df_new_reviews)
            df_to_insert = df_new_reviews[~df_new_reviews['Review_ID'].isin(existing_id_set)]
            new_rows_count = len(df_to_insert)
            print(f"Filtered {initial_rows} new reviews down to {new_rows_count} unique reviews to be inserted.")

            # Step 3: Only write to the database if there is new data to insert.
            if new_rows_count > 0:
                df_to_insert.to_sql(table_name, engine, if_exists='append', index=False)
                print(f"✅ Successfully appended {new_rows_count} new rows to table '{table_name}'.")
            else:
                print("✅ No new reviews to add. The database is already up-to-date.")

    except Exception as e:
        # This handles the case where the table doesn't exist yet on the very first run.
        if "does not exist" in str(e):
            print(f"Table '{table_name}' does not exist. Writing all {len(df_new_reviews)} new reviews.")
            df_new_reviews.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"✅ Successfully created table '{table_name}' and inserted {len(df_new_reviews)} rows.")
        else:
            print(f"❌ An error occurred during database write: {e}")
            raise