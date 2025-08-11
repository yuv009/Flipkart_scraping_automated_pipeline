# dags/scraper_module.py

import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException
from selenium_stealth import stealth
import re
import os


def clean_filename(filename):
    """Cleans a filename to be safe for saving."""
    return re.sub(r'[<>:"/\\|?*]', '', filename).replace(' ', '_')[:100]


def get_phone_info(driver):
    """Extracts phone name and price from the page."""
    name, price = "Unknown_Phone", "N/A"
    for selector in ['.VU-ZEz', 'span.B_NuCI']:
        try:
            name = driver.find_element(By.CSS_SELECTOR, selector).text.strip()
            break
        except NoSuchElementException:
            continue
    for selector in ['.Nx9bqj.CxhGGd', '._30jeq3._16Jk6d']:
        try:
            price = driver.find_element(By.CSS_SELECTOR, selector).text.strip()
            break
        except NoSuchElementException:
            continue
    return name, price


def scrape_reviews_for_url(phone_url: str, output_folder: str) -> str | None:
    """
    Scrapes all reviews for a single Flipkart URL using stealth techniques,
    saves them to a CSV, and returns the file path.
    Stops scraping only when "Next" button disappears. Continues even if page has no reviews.
    """
    print("Initializing Stealth WebDriver...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Comment out this line for visible browser window
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(service=ChromeService(), options=options)

    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
            )

    # Manual stealth JS overrides
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        window.chrome = {runtime: {}};
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        '''
    })

    try:
        driver.get(phone_url)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.VU-ZEz'))
        )
        time.sleep(2)
        phone_name, price = get_phone_info(driver)
        print(f"Extracted - Phone: {phone_name}, Price: {price}")

        try:
            driver.find_element(By.PARTIAL_LINK_TEXT, "All").click()
            time.sleep(5)
        except NoSuchElementException:
            print("Could not find 'All' reviews link. Assuming already on reviews page.")

        data = []
        page = 1
        max_retries = 2

        while True:
            print(f"Scraping Page {page} for {phone_name}...")

            # Robust waiting for either reviews or Next button, with retry on failure
            retries = 0
            while retries < max_retries:
                try:
                    WebDriverWait(driver, 25).until(
                        EC.any_of(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.col.EPCmJX.Ma1fCG')),
                            EC.presence_of_element_located((By.XPATH, "//a[.//span[text()='Next']]"))
                        )
                    )
                    break
                except TimeoutException:
                    retries += 1
                    print(f"Retry {retries} waiting for reviews or next button...")
                    time.sleep(10)
            else:
                print("Timeout waiting for reviews or next button. Ending scrape.")
                break

            soup = BeautifulSoup(driver.page_source, "html.parser")
            containers = soup.find_all("div", class_="col EPCmJX Ma1fCG")

            if containers:
                for container in containers:
                    rating = container.find("div", class_="XQDdHH")
                    title = container.find("p", class_="z9E0IG")
                    body = container.find("div", class_="ZmyHeo")
                    author_block = container.find("div", class_="gHqwa8")

                    if body:
                        read_more = body.find("span", class_="wTYmpv")
                        if read_more:
                            read_more.decompose()

                    if author_block:
                        author = author_block.find("p", class_="_2NsDsF AwS1CA")
                        location = author_block.find("p", class_="MztJPv")
                        age_candidates = author_block.find_all("p", class_="_2NsDsF")
                        age = next((p.get_text(strip=True) for p in age_candidates if "AwS1CA" not in p.get("class", [])), "No Age")

                        data.append({
                            "Phone_Name": phone_name,
                            "Price": price,
                            "Review_ID": location.get("id", "No ID") if location else "No ID",
                            "Author": author.get_text(strip=True) if author else "No Author",
                            "Location": location.find_all("span")[-1].get_text(strip=True).replace(",", "") if location and location.find_all("span") else "No Location",
                            "Rating": rating.text.strip() if rating else "No Rating",
                            "Title": title.get_text(strip=True) if title else "No Title",
                            "Review": body.get_text(strip=True) if body else "No Review",
                            "Age": age
                        })
            else:
                print(f"No reviews found on page {page}.")

            # Scroll to bottom to trigger lazy loading
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

            try:
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[.//span[text()='Next']]"))
                )
                review_elements = driver.find_elements(By.CSS_SELECTOR, "div.col.EPCmJX.Ma1fCG")
                first_review_el = review_elements[0] if review_elements else None

                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", next_button)

                if first_review_el:
                    WebDriverWait(driver, 15).until(EC.staleness_of(first_review_el))
                else:
                    time.sleep(3)

                page += 1
            except TimeoutException:
                print("No 'Next' button found. Scraping complete.")
                break

        if data:
            os.makedirs(output_folder, exist_ok=True)
            filename = f"{clean_filename(phone_name)}_reviews.csv"
            full_path = os.path.join(output_folder, filename)
            pd.DataFrame(data).to_csv(full_path, index=False, encoding="utf-8")
            print(f"Saved {len(data)} reviews to {full_path}")
            return full_path
        else:
            print("No reviews scraped.")
            return None

    except Exception as e:
        print(f"An error occurred while scraping {phone_url}: {e}")
        return None

    finally:
        driver.quit()
