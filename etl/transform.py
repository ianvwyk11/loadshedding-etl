"""
transform.py
------------
Reads raw JSON files from data/raw/, cleans and normalises the data,
and writes structured CSVs to data/processed/.
"""

import os
import json
import logging
from datetime import datetime

import pandas as pd

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_latest_raw(prefix: str) -> dict:
    """Load the most recently saved raw JSON file matching a prefix."""
    files = sorted([
        f for f in os.listdir(RAW_DIR)
        if f.endswith(".json") and prefix in f
    ])
    if not files:
        raise FileNotFoundError(f"No raw files found with prefix '{prefix}' in {RAW_DIR}")
    filepath = os.path.join(RAW_DIR, files[-1])
    logger.info(f"Loading raw file: {filepath}")
    with open(filepath) as f:
        return json.load(f)


def transform_status(raw: dict) -> pd.DataFrame:
    """Transform the national status response into a flat DataFrame."""
    records = []
    status = raw.get("status", {})
    for region, detail in status.items():
        records.append({
            "region": region,
            "stage": detail.get("stage", 0),
            "stage_updated": detail.get("stage_updated"),
            "next_stages": str(detail.get("next_stages", [])),
            "extracted_at": datetime.utcnow().isoformat(),
        })
    df = pd.DataFrame(records)
    df["stage"] = pd.to_numeric(df["stage"], errors="coerce").fillna(0).astype(int)
    df["stage_updated"] = pd.to_datetime(df["stage_updated"], errors="coerce", utc=True)
    return df


def save_processed(df: pd.DataFrame, filename: str) -> str:
    """Save a processed DataFrame to data/processed/ as CSV."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(PROCESSED_DIR, f"{timestamp}_{filename}.csv")
    df.to_csv(filepath, index=False)
    logger.info(f"Saved processed data to {filepath} ({len(df)} rows)")
    return filepath


def run():
    """Main transform entry point."""
    logger.info("Starting transformation...")

    raw_status = load_latest_raw("status")
    df_status = transform_status(raw_status)
    save_processed(df_status, "status")

    logger.info("Transformation complete.")


if __name__ == "__main__":
    run()
