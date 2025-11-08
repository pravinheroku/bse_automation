# core/historical_db_handler.py

import sqlite3
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import json

HISTORICAL_DB_FILE = "historical_announcements.db"

logger = logging.getLogger(__name__)


class HistoricalDBHandler:
    """
    A dedicated handler for interacting with the historical_announcements.db.
    It primarily provides lookup and update capabilities for the live scraper.
    """

    def __init__(self, db_path=HISTORICAL_DB_FILE):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            logger.error(f"FATAL: Historical database not found at '{self.db_path}'!")
            raise FileNotFoundError(f"Historical database not found at {self.db_path}")
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Makes fetching rows as dicts easy
        logger.info("🔗 Connected to historical database.")

    def get_latest_announcement_for_scrip(
        self, scrip_code: str, current_ann_date: str
    ) -> Optional[Dict[str, Any]]:
        """
        Finds the most recent announcement for a given scrip_code that occurred
        BEFORE the provided announcement date.

        Args:
            scrip_code: The company's scrip code.
            current_ann_date: The date of the new announcement (YYYY-MM-DD).

        Returns:
            A dictionary representing the database row, or None if not found.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT news_id, scrip_code, company_name, announcement_date, pdf_url, summary_json
                FROM announcements
                WHERE scrip_code = ? AND announcement_date < ?
                ORDER BY announcement_date DESC
                LIMIT 1
                """,
                (scrip_code, current_ann_date),
            )
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            logger.error(f"Failed to query historical DB for {scrip_code}: {e}")
            return None

    def update_summary(self, news_id: str, summary_data: dict) -> None:
        """
        Updates a historical record with its newly generated summary JSON.
        This is used by the Just-in-Time summarization process.
        """
        summary_str = json.dumps(summary_data, indent=2)
        try:
            with self.conn:  # Use context manager for automatic commit/rollback
                self.conn.execute(
                    "UPDATE announcements SET summary_json = ? WHERE news_id = ?",
                    (summary_str, news_id),
                )
            logger.info(f"💾 Updated historical summary for {news_id}.")
        except sqlite3.Error as e:
            logger.error(f"Failed to update historical summary for {news_id}: {e}")

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Historical database connection closed.")
