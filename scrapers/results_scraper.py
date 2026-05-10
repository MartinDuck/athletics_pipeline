import requests
from bs4 import BeautifulSoup
import logging
import time
from google.cloud import bigquery
import re 
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd 

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_competition_ids(client: bigquery.Client) -> list[dict[str, str]]:
    """
    Fetch competition IDs and names from the BigQuery table.
    """
    query = f"""
        SELECT Competition_ID, Competition_Name
        FROM `{os.getenv('COMPETITIONS_TABLE')}`
    """
    try:
        query_job = client.query(query)
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return []

    competitions = []
    for row in query_job:
        competitions.append({
            "id": row["Competition_ID"],
            "name": row["Competition_Name"],
        })
    return competitions


def scrape_event_ids(competition_id: str) -> dict[str, str]:
    """
    Fetch event IDs and names for a given competition ID 
    """
    url = f'https://worldathletics.org/competition/calendar-results/results/{competition_id}?eventId='
    events = {}
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        event_select = soup.find('select', {'name': 'event-select'})
        ids_soup = event_select.find_all('option', attrs={'value': re.compile(r'\d+')})
        
        for id in ids_soup:
            events[id['value']] = id.text
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
    except Exception as e:
        logging.error(f"Error parsing {url}: {e}")
    
    return events


def scrape_event(session: requests.Session, base_url: str, comp_id: str, event_id: str, event_name: str) -> list[dict[str, str]]:
    """
    Scrape event results for a given event ID and return a list of dictionaries with the data.
    """
    event_url = f"{base_url}?eventId={event_id}"
    scraped_data = []
    
    try:
        response = session.get(event_url, timeout=10)
        response.raise_for_status() 
        
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('tr', attrs={'role': 'row'}, limit=9) # Header + top 8 results
        
        for row in rows[1:]: # Skip header row
            cells = row.find_all('td')
            if len(cells) >= 5:

                Place = cells[0].text.strip('. ') # Remove trailing dot and spaces from place
                if Place == '-':
                    Place = None

                raw_dob = cells[2].text.strip()
                clean_dob = None

                if raw_dob:
                    try:
                        parsed_date = datetime.strptime(raw_dob, "%d %b %Y")  # Parse date in format "DD MMM YYYY"
                        clean_dob = parsed_date.strftime("%Y-%m-%d")          # Convert to ISO format "YYYY-MM-DD"
                    except ValueError:
                        clean_dob = None

                row_dict = {
                    "Competition_ID": int(comp_id),
                    "Event_ID": int(event_id),
                    "Event_Name": event_name,
                    "Place": int(Place) if Place else None,
                    "Athlete_Name": None,
                    "Birth_date": clean_dob,
                    "Nationality": cells[3].text.strip(),
                    "Mark": cells[4].text.strip()
                }

                if 'relay' in event_name.lower(): # Split relay results into individual records for each athlete
                    athlete_links = cells[1].find_all('a') 
                    
                    if athlete_links:
                        for link in athlete_links:
                            clean_name = link.text.strip(', ') 
                            row_dict["Athlete_Name"] = clean_name
                            scraped_data.append(row_dict.copy())

                else:
                    row_dict["Athlete_Name"] = cells[1].text.strip()
                    scraped_data.append(row_dict)
                
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error scraping {event_url}: {e}")
    except Exception as e:
        logging.error(f"Parsing error on {event_url}: {e}")
        
    return scraped_data


def extract_all_events(comp_ids: list[dict[str, str]]) -> list[dict]:
    """Handles the session setup and loops through competitions to extract raw data."""
    all_events_data = []
    
    with requests.Session() as session:
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0'
        })

        for comp in comp_ids:
            comp_id = comp["id"]
            base_url = f'https://worldathletics.org/competition/calendar-results/results/{comp_id}'
            
            logging.info(f"Processing competition: {comp['name']}")
            
            event_ids = scrape_event_ids(comp_id)
            for event_id, event_name in event_ids.items():
                logging.info(f"-> Scraping event: {event_name}")
                event_data = scrape_event(session, base_url, comp_id, event_id, event_name)
            
                if event_data:
                    all_events_data.extend(event_data)
            
                time.sleep(0.5)
                
    return all_events_data


def transform_to_dataframe(raw_data: list[dict]) -> pd.DataFrame:
    """Converts raw list of dictionaries to a DataFrame and cleans it."""
    logging.info("Transforming raw data...")
    df = pd.DataFrame(raw_data)
    
    initial_count = len(df)
    df.drop_duplicates(inplace=True)
    
    if len(df) < initial_count:
        logging.info(f"Dropped {initial_count - len(df)} duplicate rows.")
        
    return df

def load_to_bigquery(client: bigquery.Client, df: pd.DataFrame, table_id: str) -> None:
    """Executes a batch load job to BigQuery."""
    logging.info(f"Initiating BigQuery Load Job for {len(df)} rows to {table_id}...")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", 
    )

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  
        logging.info("Load job completed successfully.")
    except Exception as e:
        logging.error(f"Failed to load DataFrame to BigQuery: {e}")

def main():
    logging.info("Starting World Athletics to BigQuery Pipeline...")
    
    project_id = os.getenv("PROJECT_ID")
    table_id = os.getenv("RESULTS_TABLE")
    client = bigquery.Client(project=project_id)
    
    comp_ids = get_competition_ids(client)
    if not comp_ids:
        logging.error("Missing competition IDs. Exiting.")
        return

    raw_data = extract_all_events(comp_ids)
    
    if not raw_data:
        logging.warning("No data scraped. Pipeline stopping.")
        return

    clean_df = transform_to_dataframe(raw_data)
    
    load_to_bigquery(client, clean_df, table_id)
                
    logging.info("Pipeline finished successfully.")

if __name__ == "__main__":
    main()
