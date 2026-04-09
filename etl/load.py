"""
load.py
-------
Loads processed CSV data into a SQLite database using a star schema.
"""

import os
import logging
import argparse

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PROCESSED_DIR = "data/processed"
DB_PATH = os.getenv("DB_PATH", "data/loadshedding.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_engine():
    return create_engine(f"sqlite:///{DB_PATH}")


def init_db():
    """Create tables from SQL schema file."""
    engine = get_engine()
    sql_path = os.path.join("sql", "create_tables.sql")
    with open(sql_path) as f:
        schema_sql = f.read()
    with engine.connect() as conn:
        for statement in schema_sql.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
        conn.commit()
    logger.info(f"Database initialised at {DB_PATH}")


def load_latest_processed(prefix: str) -> pd.DataFrame:
    """Load the most recently saved processed CSV matching a prefix."""
    files = sorted([
        f for f in os.listdir(PROCESSED_DIR)
        if f.endswith(".csv") and prefix in f
    ])
    if not files:
        raise FileNotFoundError(f"No processed files found with prefix '{prefix}'")
    filepath = os.path.join(PROCESSED_DIR, files[-1])
    logger.info(f"Loading processed file: {filepath}")
    return pd.read_csv(filepath)


def load_status(engine):
    """Load status data into fact_outages table."""
    df = load_latest_processed("status")
    df.to_sql("fact_status_snapshot", engine, if_exists="append", index=False)
    logger.info(f"Loaded {len(df)} rows into fact_status_snapshot")


def run():
    """Main load entry point."""
    logger.info("Starting load...")
    engine = get_engine()
    load_status(engine)
    logger.info("Load complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true", help="Initialise the database schema")
    args = parser.parse_args()

    if args.init:
        init_db()
    else:
        run()
