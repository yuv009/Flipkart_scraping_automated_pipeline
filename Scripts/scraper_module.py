# scraper_module.py

import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import re
import os

# This function is unchanged
def clean_filename(filename):
    """Clean filename for saving"""
    return re.sub(r'[<>:"/\\|?*]', '', filename).replace(' ', '_')[:100]

# This function is unchanged
def get_phone_info(driver):
    """Extract phone name and price"""
    phone_name = "Unknown_Phone"
    price = "N/A"
    try:
        name_element = driver.find_element(By.CSS_SELECTOR, '.VU-ZEz')
        phone_name = name_element.text.strip()
    except NoSuchElementException:
        try:
            name_element = driver.find_element(By.CSS_SELECTOR, 'span.B_NuCI')
            phone_name = name_element.text.strip()
        except NoSuchElementException: pass
    try:
        price_element = driver.find_element(By.CSS_SELECTOR, '.Nx9bqj.CxhGGd')
        price = price_element.text.strip()
    except NoSuchElementException:
        try:
            price_element = driver.find_element(By.CSS_SELECTOR, '._30jeq3._16Jk6d')
            price = price_element.text.strip()
        except NoSuchElementException: pass
    return phone_name, price

# UPDATED FUNCTION
def scrape_reviews_for_url(phone_url: str, output_folder: str) -> str | None:
    """
    Scrapes all reviews for a single Flipkart URL, saves them to a CSV in a
    specified folder, and returns the full file path.
    """
    # --- SETUP ---
    print("Initializing WebDriver...")
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # ADDED: These arguments are crucial for running in a Docker container
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=ChromeService(), options=chrome_options) # MODIFIED: Removed WebDriverManager
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'})

    try:
        driver.get(phone_url)
        time.sleep(3)
        phone_name, price = get_phone_info(driver)
        print(f"Extracted - Phone: {phone_name}, Price: {price}")
        driver.find_element(By.PARTIAL_LINK_TEXT, "All").click()
        time.sleep(3)
        
        data = []
        page = 1
        while True:
            print(f"Scraping Page {page} for {phone_name}...")
            time.sleep(2)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            containers = soup.find_all('div', class_='col EPCmJX Ma1fCG')
            
            for container in containers:
                rating = container.find('div', class_='XQDdHH')
                title = container.find('p', class_='z9E0IG')
                body = container.find('div', class_='ZmyHeo')
                author_block = container.find('div', class_='gHqwa8')
                if body:
                    read_more = body.find('span', class_='wTYmpv')
                    if read_more: read_more.decompose()
                if author_block:
                    author = author_block.find('p', class_='_2NsDsF AwS1CA')
                    location = author_block.find('p', class_='MztJPv')
                    age_candidates = author_block.find_all('p', class_='_2NsDsF')
                    age = next((p.get_text(strip=True) for p in age_candidates if 'AwS1CA' not in p.get('class', [])), "No Age")
                    data.append({
                        'Phone_Name': phone_name, 'Price': price,
                        'Review_ID': location.get('id', 'No ID') if location else 'No ID',
                        'Author': author.get_text(strip=True) if author else 'No Author',
                        'Location': location.find_all('span')[-1].get_text(strip=True).replace(',', '') if location and location.find_all('span') else 'No Location',
                        'Rating': rating.text.strip() if rating else 'No Rating',
                        'Title': title.get_text(strip=True) if title else 'No Title',
                        'Review': body.get_text(strip=True) if body else 'No Review',
                        'Age': age
                    })
            try:
                next_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[.//span[text()='Next']]")))
                next_button.click()
                page += 1
            except (TimeoutException, StaleElementReferenceException):
                break
        
        if data:
            os.makedirs(output_folder, exist_ok=True)
            filename = f'{clean_filename(phone_name)}_reviews.csv'
            full_path = os.path.join(output_folder, filename)
            pd.DataFrame(data).to_csv(full_path, index=False, encoding='utf-8')
            print(f"Saved {len(data)} reviews to {full_path}")
            return full_path
            
    except Exception as e:
        print(f"An error occurred while scraping {phone_url}: {e}")
        return None
    finally:
        driver.quit()
    return None