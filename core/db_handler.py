# core/db_handler.py
import sqlite3
from pathlib import Path
import json

DB_FILE = "database.db"


class DBHandler:
    def __init__(self, db_path=DB_FILE):
        """Initializes the database connection and creates/updates the table."""
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self._create_table()

    def _create_table(self):
        """
        Creates the 'announcements' table or adds new columns if they don't exist.
        Status can be: 'DOWNLOADED', 'PROCESSED', 'ERROR_PROCESSING'
        """
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS announcements (
                news_id TEXT PRIMARY KEY,
                scrip_code TEXT,
                company_name TEXT,
                download_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'DOWNLOADED',
                summary_json TEXT
            )
        """
        )

        try:
            self.cursor.execute(
                "ALTER TABLE announcements ADD COLUMN status TEXT DEFAULT 'DOWNLOADED'"
            )
        except sqlite3.OperationalError:
            pass
        try:
            self.cursor.execute(
                "ALTER TABLE announcements ADD COLUMN summary_json TEXT"
            )
        except sqlite3.OperationalError:
            pass
        self.conn.commit()

    def is_processed(self, news_id: str) -> bool:
        """Checks if a given NEWSID has already been downloaded."""
        self.cursor.execute("SELECT 1 FROM announcements WHERE news_id = ?", (news_id,))
        return self.cursor.fetchone() is not None

    def needs_summarization(self, news_id: str) -> bool:
        """Checks if a downloaded item still needs to be summarized."""
        self.cursor.execute(
            "SELECT 1 FROM announcements WHERE news_id = ? AND status = 'DOWNLOADED'",
            (news_id,),
        )
        return self.cursor.fetchone() is not None

    def add_new_announcement(self, news_id: str, scrip_code: str, company_name: str):
        """Adds a new NEWSID to the database with 'DOWNLOADED' status."""
        try:
            self.cursor.execute(
                "INSERT INTO announcements (news_id, scrip_code, company_name, status) VALUES (?, ?, ?, 'DOWNLOADED')",
                (news_id, scrip_code, company_name),
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass

    def update_summary(
        self, news_id: str, summary_data: dict, status: str = "PROCESSED"
    ):
        """Updates an announcement with the summary JSON and new status."""
        summary_str = json.dumps(summary_data, indent=2)
        self.cursor.execute(
            "UPDATE announcements SET summary_json = ?, status = ? WHERE news_id = ?",
            (summary_str, status, news_id),
        )
        self.conn.commit()

    def close(self):
        """Closes the database connection."""
        self.conn.close()
