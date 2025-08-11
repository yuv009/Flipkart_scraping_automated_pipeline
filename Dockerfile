# Use the official Airflow image as a base
FROM apache/airflow:2.9.3

# Switch to the root user to install system packages
USER root

# Install Chromium (the browser) and its driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Install your Python libraries, now including selenium-stealth
RUN pip install --no-cache-dir \
    selenium \
    pandas \
    beautifulsoup4 \
    emoji \
    selenium-stealth