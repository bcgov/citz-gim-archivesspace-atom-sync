import os
import time
import json
import logging
import ssl
import requests

ATOM_API_TOKEN = os.environ["ATOM_API_TOKEN"]
HEADERS = {"REST-API-KEY": ATOM_API_TOKEN}
BASE = os.getenv("ATOM_API_URL", "https://search-bcarchives.royalbcmuseum.bc.ca/api").rstrip("/")
QUERY = os.getenv("ATOM_INFORMATION_OBJECTS_QUERY", "sq0=GR*&sf0=referenceCode&levels=197")

# Create an SSL context using the provided atom.crt file
ssl_context = ssl.create_default_context()
cert_path = os.path.join(os.path.dirname(__file__), "..", "atom.crt")
ssl_context.load_verify_locations(cert_path)

# 24 hours (288 attempts at 5 minutes each)
MAX_RETRIES = 288

def fetch_atom_detail(slug: str) -> dict:
    url = f"{BASE}/informationobjects/{slug}"
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            response = requests.get(url, headers=HEADERS, verify=cert_path)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            attempts += 1
            logging.error("Attempt %d: Failed to fetch details for slug '%s': %s", attempts, slug, e)
            if attempts < MAX_RETRIES:
                time.sleep(300)  # Pause for 5 minutes before retrying
    return {}  # Return an empty dictionary after MAX_RETRIES failed attempts

def fetch_slugs(skip: int, limit: int):
    url = f"{BASE}/informationobjects?{QUERY}&limit={limit}&skip={skip}"
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            response = requests.get(url, headers=HEADERS, verify=cert_path)
            response.raise_for_status()
            data = response.json()
            return data["results"], data.get("total", 0)  # total defaults to 0 if not provided
        except requests.exceptions.RequestException as e:
            attempts += 1
            logging.error("Attempt %d: Failed to fetch slugs: %s", attempts, e)
            if attempts < MAX_RETRIES:
                time.sleep(300)  # Pause for 5 minutes before retrying
    return [], 0  # Return empty results and total 0 after MAX_RETRIES failed attempts
