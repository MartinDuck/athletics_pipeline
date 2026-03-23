# World Athletics Cloud Data Pipeline & BI Dashboard

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Google Cloud Platform](https://img.shields.io/badge/GCP-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=google-bigquery&logoColor=white)
![Looker Studio](https://img.shields.io/badge/Looker_Studio-4285F4?style=for-the-badge&logo=looker&logoColor=white)

## Project Overview
This project is an end-to-end **Cloud Data Engineering** pipeline designed to extract, transform, and visualize track and field results from the World Athletics database. 

The goal was to build a highly resilient, cloud-native ELT (Extract, Load, Transform) architecture that handles messy, real-world sports data (e.g., varying measurement units, status codes like DQ/DNF, and multi-athlete relay events) and models it for downstream Business Intelligence.

## Architecture & Tech Stack
* **Extraction (Web Scraping):** `Python` (Selenium, BeautifulSoup, Requests)
* **Data Warehouse:** `Google BigQuery`
* **Data Modeling:** Kimball Star Schema (DDL & SQL)
* **Business Intelligence:** `Looker Studio`

### The ELT Pipeline Flow:
1. **Extract:** A Python scraper dynamically extracts hierarchical competition and event data.
2. **Load (Bronze/Staging):** Semi-structured data is streamed directly into BigQuery using `Application Default Credentials`. To ensure fault tolerance and prevent pipeline crashes, polymorphic fields (like finishing marks and varying date formats) are ingested as raw `STRING` types.
3. **Transform (Silver/Gold):** * Python logic explodes concatenated "Relay Team" strings into individual athlete rows.
    * BigQuery SQL (`SAFE_CAST`, `TRUNCATE`) is used to enforce data types, handle NULLs, and model the data into a Star Schema.
4. **Serve:** Looker Studio connects directly to the BigQuery tables to serve interactive analytics.
