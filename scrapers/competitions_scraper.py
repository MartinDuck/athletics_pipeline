import logging
import re
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import sys
from google.cloud import bigquery 
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

COMPETITION_GROUPS = { # These IDs are based on the World Athletics website's structure and may need to be updated if the site changes
        "Olympic Games": 5,
        "World Athletics Championships": 6,
        "World Athletics Indoor Championships": 12
    }

def get_competition_ids(url: str) -> list:
    logging.info(f"Starting Selenium scraper for: {url}")
    
    options = Options()
    options.add_argument('--headless') 
    
    driver = webdriver.Firefox(options=options)
    
    try:
        driver.get(url)

        logging.info("Waiting for content to load...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/competition/calendar-results')]"))
        )
       
        html = driver.page_source
        
    except Exception as e:
        logging.error(f"Selenium failed to load page elements: {e}")
        return 
    finally:
        driver.quit() 

    logging.info("Parsing HTML...")
    soup = BeautifulSoup(html, 'html.parser')
   
    rows = soup.find_all('tr')
    competitions = []
    for row in rows:
        link = row.find('a', href=re.compile(r'^/competition/calendar-results/results.*'))
        if link:
            href = link.get('href')
            id = re.search(r'\d+', href)
            columns = row.find_all('td')[:3] 

            if len(columns) == 3 and id:

                competition_date = columns[0].get_text(strip=True).replace('–', '-') # Standardize date format by replacing en dash with hyphen

                competition_name = columns[1].get_text(strip=True)

                competition_location = columns[2].get_text(strip=True)
                location_split = re.search(r"^(.*)\s\(([A-Z]{3})\)$", competition_location) # Attempt to split location into venue and country code
        
                competition_venue = competition_location
                competition_country = None

                if location_split:
                    competition_venue = location_split.group(1)
                    competition_country = location_split.group(2)

                is_indoor = 'indoor' in competition_name.lower()

            competitions.append((id.group(), competition_date, competition_name, competition_venue, competition_country, is_indoor))

    if not competitions:
        logging.warning("No competitions found.")
        return
    
    return competitions

if __name__ == "__main__":
    
    project_id = os.getenv("PROJECT_ID")
    client = bigquery.Client(project=project_id)

    table_id = os.getenv("COMPETITIONS_TABLE")

    for group_name, group_id in COMPETITION_GROUPS.items():
        target_url = f'https://worldathletics.org/competition/calendar-results?competitionGroupId={group_id}'
        competitions = get_competition_ids(target_url)

        if competitions:
            rows_to_insert = []
            for comp in competitions:
                row = {
                    "Competition_ID": int(comp[0]),
                    "Competition_Date": comp[1],
                    "Competition_Name": comp[2],
                    "Venue": comp[3],
                    "Country": comp[4],
                    "Is_Indoor": comp[5]
                }
                rows_to_insert.append(row)

            errors = client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                logging.error(f"BigQuery insertion errors for {group_name}: {errors}")
            else:
                logging.info(f"Successfully inserted {len(rows_to_insert)} rows for {group_name}.")