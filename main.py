# /home/pravin/Development/bse_scraper/main.py

import time
import asyncio
import logging
from datetime import datetime
from pathlib import Path
import sys  # <--- IMPORTED SYS
import traceback  # <--- IMPORTED TRACEBACK
from core.scraper import BSEScraper


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
    """Configures the root logger for the entire application run."""
    run_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_dir = Path("logs") / f"LIVE-{run_timestamp}"
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
    # Quieten down noisy google and other libraries
    logging.getLogger("google.api_core").setLevel(logging.WARNING)
    logging.getLogger("google.auth.transport.requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("google_genai.types").setLevel(logging.ERROR)

    return log_dir


async def run_single_poll():
    """
    Encapsulates the logic for a single polling run.
    It relies on the logger that was already configured by the main() function.
    """
    # Get the logger for this specific module. It will inherit the root setup.
    logger = logging.getLogger(__name__)
    logger.info("--- Starting new poll cycle ---")

    try:
        # Each poll gets a fresh scraper instance.
        # The scraper will automatically use the pre-configured root logger.
        scraper = BSEScraper(test_mode=False)

        notification_tasks = await scraper.run()

        if notification_tasks:
            await scraper.run_all_notifications_sequentially(notification_tasks)

        scraper.db.close()
        logger.info("--- Poll cycle complete ---")

    except Exception as e:
        logger.error(
            f"An unexpected error occurred during the poll: {e}", exc_info=True
        )


def main():
    """
    The main entry point for the long-running scraper.
    Sets up logging once, then enters an infinite polling loop.
    """
    polling_interval_seconds = 60

    # 1. Set up logging for the entire lifetime of this script run.
    log_path = setup_logging()

    # --- NEW: SET THE GLOBAL EXCEPTION HOOK ---
    sys.excepthook = handle_exception

    # Now we get the logger for the main module.
    logger = logging.getLogger(__name__)

    logger.info("🚀 Starting BSE Real-Time Scraper...")
    logger.info(f"Full logs for this run are in: {log_path}")
    logger.info(f"Polling interval set to {polling_interval_seconds} seconds.")

    while True:
        try:
            # asyncio.run() creates and closes a new event loop for each poll.

            asyncio.run(run_single_poll())

            logger.info(
                f"Waiting for {polling_interval_seconds} seconds before next run..."
            )
            time.sleep(polling_interval_seconds)

        except KeyboardInterrupt:
            logger.info("\n🛑 Scraper stopped by user.")
            break
        except Exception as e:
            # This will catch critical errors in the main loop or asyncio.run itself.
            logger.critical(
                f"A critical error occurred in the main loop: {e}", exc_info=True
            )
            logger.info("Restarting loop in 60 seconds...")
            time.sleep(60)


if __name__ == "__main__":
    main()
