import os
import json
import time
import asyncio
import logging
import pymysql
import pandas as pd
import aiohttp
import nest_asyncio
import streamlit as st
from rich.progress import Progress
from dotenv import load_dotenv

# Selenium and WebDriver Manager for Edge
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager


# Allow nested event loops (useful in Streamlit/Jupyter)
nest_asyncio.apply()

# Set page config to wide layout
st.set_page_config(page_title="WA Athlete ID Finder", layout="wide")

# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

############################################################
# PART 1: Get athlete names from MySQL database
############################################################

@st.cache_data(show_spinner=True)
def load_athlete_names():
    # Get database connection details from environment variables
    DB_HOST = os.environ.get("DB_HOST", "sportsdb-sports-database-for-web-scrapes.g.aivencloud.com")
    DB_PORT = int(os.environ.get("DB_PORT", 16439))
    DB_USER = os.environ.get("DB_USER", "avnadmin")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    DB_NAME = os.environ.get("DB_NAME", "defaultdb")

    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    query = """
    SELECT DISTINCT Competitor_Name, IAAF_ID, Gender 
    FROM WA_competition_results 
    WHERE Nationality = %s
    """
    cursor.execute(query, ("QAT",))
    results = cursor.fetchall()
    df_QAT = pd.DataFrame(results, columns=['Competitor_Name', 'IAAF_ID', 'Gender'])
    cursor.close()
    conn.close()

    athlete_names = df_QAT['Competitor_Name'].tolist()
    logging.info(f"Loaded {len(athlete_names)} athlete names from the database.")
    return athlete_names

############################################################
# PART 2: Use Selenium to get GraphQL endpoint URL and API key
############################################################

@st.cache_data(show_spinner=True)
def get_api_details():

    # Configure Chrome options for headless mode
    chrome_options = ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')

    # Set performance logging via ChromeOptions capabilities
    chrome_options.set_capability("goog:loggingPrefs", {'performance': 'ALL'})

    # Initialize Chrome WebDriver using ChromeDriverManager
    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()),
        options=chrome_options
    )

    selenium_url = 'https://worldathletics.org/competition/calendar-results'
    driver.get(selenium_url)
    logging.info("Loading calendar-results page...")
    time.sleep(10)  # Adjust as needed for full page load

    logs = driver.get_log('performance')
    request_url = None
    api_key = None
    for log in logs:
        try:
            log_json = json.loads(log['message'])['message']
            if log_json.get('method') == 'Network.requestWillBeSent':
                request = log_json['params'].get('request', {})
                if request.get('method') == 'POST' and 'graphql' in request.get('url', ''):
                    headers_req = request.get('headers', {})
                    possible_api_key = headers_req.get('x-api-key')
                    if possible_api_key:
                        request_url = request['url']
                        api_key = possible_api_key
                        logging.info(f"Extracted request_url: {request_url}")
                        logging.info(f"Extracted x-api-key: {api_key}")
                        break
        except Exception as e:
            logging.warning(f"Error processing log: {e}")
    driver.quit()

    if not request_url or not api_key:
        logging.error("Could not extract API key or request URL from Selenium logs.")
        st.error("Error extracting API details from World Athletics. Please try again later.")
        st.stop()
    return request_url, api_key

############################################################
# PART 3: Asynchronous GraphQL queries for competitor info
############################################################

# GraphQL query for searching competitors
graphql_query = """query SearchCompetitors($query: String, $gender: GenderType, $disciplineCode: String, $environment: String, $countryCode: String) {
  searchCompetitors(query: $query, gender: $gender, disciplineCode: $disciplineCode, environment: $environment, countryCode: $countryCode) {
    aaAthleteId
    familyName
    givenName
    birthDate
    disciplines
    iaafId
    gender
    country
    urlSlug
    __typename
  }
}"""

async def fetch_competitor_info(session, athlete_name, request_url, headers, semaphore):
    payload = {
        "operationName": "SearchCompetitors",
        "query": graphql_query,
        "variables": {"countryCode": "QAT", "query": athlete_name}
    }
    async with semaphore:
        try:
            async with session.post(request_url, json=payload, headers=headers, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    competitors = data.get("data", {}).get("searchCompetitors", [])
                    if competitors:
                        first_competitor = competitors[0]
                        return {
                            "athlete_name": athlete_name,
                            "aaAthleteId": first_competitor.get("aaAthleteId"),
                            "familyName": first_competitor.get("familyName"),
                            "givenName": first_competitor.get("givenName"),
                            "birthDate": first_competitor.get("birthDate"),
                            "disciplines": first_competitor.get("disciplines"),
                            "iaafId": first_competitor.get("iaafId"),
                            "gender": first_competitor.get("gender"),
                            "country": first_competitor.get("country"),
                            "urlSlug": "https://worldathletics.org/athletes/" + first_competitor.get("urlSlug", "")
                        }
                    else:
                        logging.info(f"No competitor found for {athlete_name}")
                        return None
                else:
                    logging.error(f"Error fetching {athlete_name}: HTTP {response.status}")
                    return None
        except Exception as e:
            logging.error(f"Exception fetching {athlete_name}: {e}")
            return None

async def fetch_all_competitors(athlete_names, request_url, headers):
    results = []
    semaphore = asyncio.Semaphore(10)  # Adjust concurrency as needed
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_competitor_info(session, name, request_url, headers, semaphore) for name in athlete_names]
        with Progress() as progress:
            task_progress = progress.add_task("[cyan]Fetching competitor info...", total=len(tasks))
            for future in asyncio.as_completed(tasks):
                result = await future
                if result:
                    results.append(result)
                progress.advance(task_progress)
    return results

@st.cache_data(show_spinner=True)
def load_competitor_data(athlete_names, request_url, api_key):
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key
    }
    competitor_results = asyncio.run(fetch_all_competitors(athlete_names, request_url, headers))
    df = pd.DataFrame(competitor_results,
                      columns=["athlete_name", "aaAthleteId", "urlSlug",
                               "birthDate", "disciplines"])
    return df

############################################################
# PART 4: Streamlit App Layout
############################################################

def main_app():
    st.title("World Athletics ID Finder")
    st.write("This app helps you find the World Athletics ID for a given athlete from Qatar.")
    st.info("Loading competitor data. This may take 30 seconds to 1 minute on the first load...")

    # Get athlete names from DB
    athlete_names = load_athlete_names()

    # Get API details using Selenium
    request_url, api_key = get_api_details()

    # Load competitor data (cached)
    df_competitors = load_competitor_data(athlete_names, request_url, api_key)

    st.success("Data loaded successfully!")

    # Sidebar filters
    st.sidebar.header("Filter Competitors")
    search_name = st.sidebar.text_input("Search Athlete Name")
    search_event = st.sidebar.text_input("Search Event (Discipline)")

    # Filter DataFrame based on sidebar input
    filtered_df = df_competitors.copy()
    if search_name:
        filtered_df = filtered_df[filtered_df["athlete_name"].str.contains(search_name, case=False, na=False)]
    if search_event:
        filtered_df = filtered_df[filtered_df["disciplines"].str.contains(search_event, case=False, na=False)]

    st.write("### Competitor Data")
    st.dataframe(filtered_df, use_container_width=True)

if __name__ == "__main__":
    main_app()
