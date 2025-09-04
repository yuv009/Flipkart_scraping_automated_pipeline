# Flipkart Scraping Automated Pipeline

This repository contains an automated pipeline for scraping data from Flipkart. The project is designed to efficiently collect product information, handle data cleaning, and store the results for further analysis.

## Features

- Scrapes product details from Flipkart (title, price, ratings, etc.)
- Automated pipeline for triggered (Streamlit UI) data collection
- Data cleaning and preprocessing
- Stores cleaned data in Azure Postgres
- Error handling and logging for reliability

## Getting Started

### Prerequisites

- Python 3.7+
- Docker (for running Airflow)
- Chrome WebDriver (for Selenium scraping)
- Pip (Python package manager)
- [Optional] Jupyter Notebook for exploratory analysis

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yuv009/Flipkart_scraping_automated_pipeline.git
   cd Flipkart_scraping_automated_pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Requirements

Install these Python dependencies (see `requirements.txt`):

- pandas
- numpy
- sqlalchemy
- psycopg2-binary
- requests
- beautifulsoup4
- selenium
- selenium-stealth
- pendulum
- apache-airflow
- apache-airflow-providers-postgres
- streamlit
- python-dotenv
- emoji
- regex

**System dependencies:**
- Docker (for Airflow orchestration)
- Chrome WebDriver (for Selenium scraping)

> For the full list, see [`requirements.txt`](https://github.com/yuv009/Flipkart_scraping_automated_pipeline/search?q=requirements).

### Usage

Edit the configuration file (if present) or update script parameters as needed.

Run the main scraping script:
```bash
python scrape_flipkart.py
```

Output data will be saved in the specified output directory.

## Project Structure

```
Flipkart_scraping_automated_pipeline/
├── scrape_flipkart.py
├── data/
│   └── output/
├── requirements.txt
├── utils/
│   └── helper_functions.py
└── README.md
```

- `scrape_flipkart.py`: Main script for scraping Flipkart
- `data/output/`: Directory for storing scraped and cleaned data
- `requirements.txt`: List of Python dependencies
- `utils/`: Utility scripts, helper functions
- `README.md`: Project documentation

## Contributing

Feel free to open issues or submit pull requests for improvements or bug fixes.

## License

[MIT License](LICENSE)

## Contact

For questions or feedback, contact [yuv009](https://github.com/yuv009).