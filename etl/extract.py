"""
extract.py
----------
Pulls raw load shedding schedule data from the EskomSePush API
and saves it as timestamped JSON files in data/raw/.
"""

import os
import json
import logging
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ESP_API_KEY")
RAW_DIR = "data/raw"
BASE_URL = "https://developer.sepush.co.za/business/2.0"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_headers() -> dict:
    return {"Token": API_KEY}


def fetch_status() -> dict:
    """Fetch current national load shedding stage."""
    url = f"{BASE_URL}/status"
    response = requests.get(url, headers=get_headers(), timeout=10)
    response.raise_for_status()
    return response.json()


def fetch_area_schedule(area_id: str) -> dict:
    """Fetch the schedule for a specific area by area ID."""
    url = f"{BASE_URL}/area"
    params = {"id": area_id}
    response = requests.get(url, headers=get_headers(), params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def save_raw(data: dict, filename: str) -> str:
    """Save raw JSON response to data/raw/ with a timestamp prefix."""
    os.makedirs(RAW_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(RAW_DIR, f"{timestamp}_{filename}.json")
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved raw data to {filepath}")
    return filepath


def run():
    """Main extract entry point."""
    logger.info("Starting extraction...")

    # Fetch national status
    status = fetch_status()
    save_raw(status, "status")

    # TODO (Phase 2): add your target area IDs here
    # area_ids = ["eskde-10-bellvillecpt", ...]
    # for area_id in area_ids:
    #     schedule = fetch_area_schedule(area_id)
    #     save_raw(schedule, f"area_{area_id}")

    logger.info("Extraction complete.")


if __name__ == "__main__":
    run()
