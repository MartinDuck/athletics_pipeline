import requests
from bs4 import BeautifulSoup
import logging
import time
from google.cloud import bigquery
import re 
from datetime import datetime
from dotenv import load_dotenv
import os

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

def scrape_event(session: requests.Session, base_url: str, comp_id: str, event_id: str, event_name: str) -> list:

    """
    Scrape event results for a given event ID and return a list of dictionaries with the data.
    """

    event_url = f"{base_url}?eventId={event_id}"
    scraped_data = []
    
    try:
        response = session.get(event_url, timeout=10)
        response.raise_for_status() 
        
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('tr', attrs={'role': 'row'}, limit=9) #Header + top 8 results
        
        for row in rows[1:]: #Skip header row
            cells = row.find_all('td')
            if len(cells) >= 5:

                Place = cells[0].text.strip('. ') #Remove trailing dot and spaces from place
                if Place == '-':
                    Place = None

                raw_dob = cells[2].text.strip()

                if raw_dob:
                    try:
                        parsed_date = datetime.strptime(raw_dob, "%d %b %Y")  #Parse date in format "DD MMM YYYY"
                        clean_dob = parsed_date.strftime("%Y-%m-%d")           
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

                if 'relay' in event_name.lower(): #Split relay results into individual records for each athlete
                    names = cells[1].text.strip().split(',')

                    for name in names:
                        row_dict["Athlete_Name"] = name.strip()
                        scraped_data.append(row_dict.copy())
                else:
                    row_dict["Athlete_Name"] = cells[1].text.strip()
                    scraped_data.append(row_dict)
                
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error scraping {event_url}: {e}")
    except Exception as e:
        logging.error(f"Parsing error on {event_url}: {e}")
        
    return scraped_data

def main():
    logging.info("Starting World Athletics to BigQuery Pipeline...")
    
    project_id = os.getenv("PROJECT_ID")
    client = bigquery.Client(project=project_id)
    
    table_id = os.getenv("RESULTS_TABLE")
    
    comp_ids = get_competition_ids(client)

    event_ids = []
    
    if not comp_ids:
        logging.error("Missing competition IDs. Exiting.")
        return

    with requests.Session() as session:
        for comp in comp_ids:
            comp_id = comp["id"]

            comp_name = comp["name"]
            base_url = f'https://worldathletics.org/competition/calendar-results/results/{comp_id}'
            
            logging.info(f"Processing competition: {comp_name}")
            
            event_ids = scrape_event_ids(comp_id)
            for event_id, event_name in event_ids.items():
                logging.info(f"-> Scraping event: {event_name}")
                event_data = scrape_event(session, base_url, comp_id, event_id, event_name)
            
                if event_data:
                    errors = client.insert_rows_json(table_id, event_data)
                    if errors == []:
                        logging.info(f"Successfully loaded {len(event_data)} rows..")
                    else:
                        logging.error(f"Failed to load rows: {errors}")
                
                time.sleep(0.5) 
                    
    logging.info("Pipeline finished successfully.")

if __name__ == "__main__":
    main()