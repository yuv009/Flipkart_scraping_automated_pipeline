import streamlit as st
import requests
import json

# --- Configuration ---
AIRFLOW_URL = "http://localhost:8081"

# NEW: Define the two DAG IDs
PARALLEL_DAG_ID = "parallel_scraping_pipeline"
SEQUENTIAL_DAG_ID = "sequential_scraping_pipeline"

AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"

def trigger_airflow_dag(mode: str, urls: list):
    # NEW: Choose the DAG ID based on the selected mode
    dag_id = PARALLEL_DAG_ID if mode == "parallel" else SEQUENTIAL_DAG_ID
    
    api_url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
    
    headers = {"Content-Type": "application/json"}
    data = {"conf": {"urls": urls}}

    try:
        response = requests.post(
            api_url,
            headers=headers,
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            data=json.dumps(data)
        )
        response.raise_for_status()
        st.success(f"Successfully triggered DAG '{dag_id}'!")
        st.json(response.json())
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to trigger DAG. Error: {e}")
        if e.response: st.error(f"Response Body: {e.response.text}")

# --- Streamlit UI ---
st.set_page_config(page_title="Airflow DAG Trigger", layout="wide")
st.title("🚀 Airflow Scraping Pipeline Trigger")
st.markdown("Select an execution mode and provide the Flipkart product URLs to scrape.")

with st.form("trigger_form"):
    col1, col2 = st.columns(2)
    with col1:
        urls_input = st.text_area("Product URLs (one per line)", height=200, placeholder="https://www.flipkart.com/product-1...")
    with col2:
        execution_mode = st.radio("Scraping Mode", ("parallel", "sequential"), index=0)

    submitted = st.form_submit_button("▶️ Trigger Pipeline")

    if submitted:
        urls_list = [url.strip() for url in urls_input.split('\n') if url.strip()]
        if not urls_list:
            st.warning("Please provide at least one URL.")
        else:
            with st.spinner(f"Requesting Airflow to start the '{execution_mode}' pipeline..."):
                trigger_airflow_dag(mode=execution_mode, urls=urls_list)