# main.py

import time
import asyncio
import logging
from datetime import datetime
from pathlib import Path
import sys 
import traceback  
from core.scraper import BSEScraper



def handle_exception(exc_type, exc_value, exc_traceback):
    """Logs unhandled exceptions to the root logger."""
    logger = logging.getLogger()
    if logger.handlers:
        
        tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        tb_text = "".join(tb_lines)
        logger.critical(f"Unhandled exception:\n{tb_text}")
    
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


def setup_logging():
    """Configures the root logger for the entire application run."""
    run_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_dir = Path("logs") / f"LIVE-{run_timestamp}"
    log_dir.mkdir(parents=True, exist_ok=True)

    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    

    if logger.hasHandlers():
        logger.handlers.clear()

    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    
    file_handler = logging.FileHandler(log_dir / "run.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # --- Control third-party library verbosity ---
    
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
    
    logger = logging.getLogger(__name__)
    logger.info("--- Starting new poll cycle ---")

    try:
        
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

    
    log_path = setup_logging()

    
    sys.excepthook = handle_exception

    
    logger = logging.getLogger(__name__)

    logger.info("🚀 Starting BSE Real-Time Scraper...")
    logger.info(f"Full logs for this run are in: {log_path}")
    logger.info(f"Polling interval set to {polling_interval_seconds} seconds.")

    while True:
        try:
            

            asyncio.run(run_single_poll())

            logger.info(
                f"Waiting for {polling_interval_seconds} seconds before next run..."
            )
            time.sleep(polling_interval_seconds)

        except KeyboardInterrupt:
            logger.info("\n🛑 Scraper stopped by user.")
            break
        except Exception as e:
            
            logger.critical(
                f"A critical error occurred in the main loop: {e}", exc_info=True
            )
            logger.info("Restarting loop in 60 seconds...")
            time.sleep(60)


if __name__ == "__main__":
    main()
