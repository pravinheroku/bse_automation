# /home/pravin/Development/bse_scraper/init_historical_db.py

import sqlite3
from pathlib import Path
import logging

# Configure basic logging for this standalone script
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DB_NAME = "historical_announcements.db"
DB_PATH = Path(DB_NAME)


def initialize_database():
    """
    Creates and initializes the historical announcements database and table.
    This script is safe to run multiple times.
    """
    if DB_PATH.exists():
        logging.warning(f"Database '{DB_NAME}' already exists. Verifying schema.")

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        logging.info(
            f"Creating 'announcements' table in {DB_NAME} if it doesn't exist..."
        )

        # We store dates as TEXT in ISO format (YYYY-MM-DD) for easy sorting and reading
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS announcements (
                news_id TEXT PRIMARY KEY,
                scrip_code TEXT NOT NULL,
                company_name TEXT NOT NULL,
                announcement_date TEXT NOT NULL,
                pdf_url TEXT NOT NULL UNIQUE,
                summary_json TEXT
            )
        """
        )

        logging.info("Creating indexes for faster queries...")
        # Index for quickly finding the last announcement for a company
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_scrip_code_date 
            ON announcements (scrip_code, announcement_date)
        """
        )

        conn.commit()
        conn.close()
        logging.info(f"✅ Database '{DB_NAME}' is ready.")

    except sqlite3.Error as e:
        logging.error(f"❌ Database error: {e}")


if __name__ == "__main__":
    initialize_database()
