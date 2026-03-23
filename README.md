# World Athletics Cloud Data Pipeline & BI Dashboard

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Google Cloud Platform](https://img.shields.io/badge/GCP-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=google-bigquery&logoColor=white)
![Looker Studio](https://img.shields.io/badge/Looker_Studio-4285F4?style=for-the-badge&logo=looker&logoColor=white)

## Project Overview
This project is an end-to-end **Cloud Data Engineering** pipeline designed to extract, transform, and visualize track and field results from the World Athletics website.

The goal was to build a highly resilient, cloud-native ELT (Extract, Load, Transform) architecture that handles messy, real-world track and field data (e.g., varying measurement units, status codes like DQ/DNF, and multi-athlete relay events) and models it for downstream Business Intelligence.

The scraper specifically extracts top 8 results from chosen competition groups. In this case, i chose three: Olympic Games, World Athletics Championships and World Athletics Indoor Championships. 


## Architecture & Tech Stack
* **Extraction (Web Scraping):** `Python` (Selenium, BeautifulSoup, Requests)
* **Data Warehouse:** `Google BigQuery`
* **Data Modeling:** Kimball Star Schema (DDL & SQL)
* **Business Intelligence:** `Looker Studio`

## Detailed Pipeline Workflow

### Stage 1: Website Analysis & Setup
Before writing the extraction logic, I analyzed the DOM structure of the World Athletics (WA) website to identify how their internal APIs and URL parameters functioned. I discovered that competitions are categorized by internal Group IDs. I mapped these into a configuration dictionary to drive the automated scraping loop.

# Configuration dictionary mapping major championships to their WA Group IDs
COMPETITION_GROUPS = {
    "Olympic Games": 5,
    "World Athletics Championships": 6,
    "World Athletics Indoor Championships": 12
}

### Stage 2: Building the Dimension Table (dim_competitions)
The first phase of the ELT process focuses on extracting metadata for every finished competition within the target groups.
1. **Extraction:** The script navigates the calendar pages and extracts the raw HTML table rows.
2. **Transformation (In-flight):** I utilized Regular Expressions to parse concatenated location strings into separate Venue and Country dimensions, and generated an Is_Indoor boolean flag based on the competition name.
3. **Loading:** The cleaned data is streamed directly into the BigQuery dim_competitions table.

### Stage 3: The Cloud-Native Results Scraper (fact_results)
1. **Querying the Cloud:** The script connects to BigQuery and runs a SELECT statement to fetch the Competition_IDs loaded in Stage 2.
2. **Dynamic Event Parsing:** For each competition, it requests the specific WA results URL, parses the HTML <select> dropdown menu, and extracts every Event_ID (eg., 100m, Long Jump).
3. **Granular Extraction:** It iterates through every event, scraping the Top 8 results.
4. **Handling Edge Cases and initial transformations:** If the script detects a "Relay" event, it explodes the concatenated string of athletes into four distinct rows to ensure accurate medal counts per athlete in the downstream dashboard. It also strips '.' out of 'Place' column to ensure safe INT parsing and converts birth date format to YYYY-MM-DD.

### The ELT Pipeline Flow:
1. **Extract:** A Python scraper dynamically extracts hierarchical competition and event data.
2. **Load :** Semi-structured data is streamed directly into BigQuery using `Application Default Credentials`. To ensure fault tolerance and prevent pipeline crashes, polymorphic fields (like finishing marks and varying date formats) are ingested as raw `STRING` types.
3. **Transform:** * Python logic explodes concatenated "Relay Team" strings into individual athlete rows.
    * BigQuery SQL is used to enforce data types, handle NULLs, and model the data into a Star Schema.
4. **Serve:** Looker Studio connects directly to the BigQuery tables to serve interactive analytics.
