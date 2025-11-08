# /home/pravin/Development/bse_scraper/test_single.py

from core.scraper import BSEScraper
import asyncio
import logging
from datetime import datetime
from pathlib import Path
import os
import sys  # <--- IMPORT SYS
import traceback  # <--- IMPORT TRACEBACK


# --- NEW: GLOBAL EXCEPTION HANDLER ---
def handle_exception(exc_type, exc_value, exc_traceback):
    """Logs unhandled exceptions to the root logger."""
    logger = logging.getLogger()
    if logger.handlers:
        # Format the exception traceback
        tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        tb_text = "".join(tb_lines)
        logger.critical(f"Unhandled exception:\n{tb_text}")
    # Also call the default excepthook to print to stderr
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


def setup_logging():
    """Configures the root logger for the application run."""
    run_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_dir = Path("logs") / f"SINGLE_TEST-{run_timestamp}"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # --- IMPORTANT: Clear any existing handlers ---
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Console handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # File handler
    file_handler = logging.FileHandler(log_dir / "run.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # --- Control third-party library verbosity ---
    # Quieten down the noisy google libraries
    logging.getLogger("google.api_core").setLevel(logging.WARNING)
    logging.getLogger("google.auth.transport.requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("google_genai.types").setLevel(logging.ERROR)

    return log_dir


async def main():
    log_path = setup_logging()
    # --- NEW: SET THE GLOBAL EXCEPTION HOOK ---
    sys.excepthook = handle_exception

    # use the root logger
    logger = logging.getLogger(__name__)

    logger.info("🧪 --- SINGLE PDF TEST ---")
    logger.info(f"Full logs for this run are in: {log_path}")

    url = os.getenv("SINGLE_TEST_PDF_URL")
    company = os.getenv("SINGLE_TEST_COMPANY_NAME", "Unknown")
    scrip = os.getenv("SINGLE_TEST_SCRIP_CODE", "000000")

    if not url:
        logger.error("❌ SINGLE_TEST_PDF_URL not set in .env")
        return

    mock = [
        {
            "NEWSID": "SINGLE_TEST_ID",
            "SLONGNAME": company,
            "SCRIP_CD": scrip,
            "PDF_URL_OVERRIDE": url,
        }
    ]

    scraper = BSEScraper(test_mode=False)

    tasks = await scraper.run(announcements_override=mock)
    if tasks:
        await scraper.run_all_notifications_sequentially(tasks)

    scraper.db.close()
    logger.info("✅ Single test complete.")


if __name__ == "__main__":
    asyncio.run(main())
