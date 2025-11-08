# /workspaces/bse_auto/historical_backfill.py

import asyncio
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from dateutil.relativedelta import relativedelta
import requests
from lxml import etree
import aiohttp
from asyncio import Queue, Event

# --- Configuration ---
HISTORICAL_DB = "historical_announcements.db"
LOG_DIR = Path("logs") / "historical_backfill"
LOG_DIR.mkdir(parents=True, exist_ok=True)
MONTHS_TO_BACKFILL = 24
# --- Concurrency Control ---
MAX_CONCURRENT_WORKERS = 3  # REDUCED from 4 to be kinder
THROTTLE_SECONDS = 5.0  # Time to pause all workers when server complains

# --- Logging Setup ---
# ... (logging setup is unchanged) ...
run_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
log_file = LOG_DIR / f"backfill_run-{run_timestamp}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)


# --- Scraper Logic ---
# ... (HEADERS, API_URL, XBRL_URL are unchanged) ...
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}
API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
XBRL_URL = "https://www.bseindia.com/Msource/90D/CorpXbrlGen.aspx"


# --- START OF FIX ---

# A global event to signal all workers to slow down
throttle_event = Event()


async def fetch_pdf_url_async(session, news_id, scrip_code, company_name):
    """Asynchronously fetches the PDF URL with retries and better logging."""
    params = {"Bsenewid": news_id, "Scripcode": scrip_code}
    for attempt in range(5):
        # If the throttle is on, wait for it to be released
        if throttle_event.is_set():
            logger.warning(
                f"Throttling is active. Worker for {company_name} pausing..."
            )
            await asyncio.sleep(THROTTLE_SECONDS)
            throttle_event.clear()  # Release the brake

        try:
            async with session.get(
                XBRL_URL, params=params, headers=HEADERS, timeout=20
            ) as response:
                response.raise_for_status()
                content = await response.read()

                # We only try to parse XML *after* a successful request
                root = etree.fromstring(
                    content
                )  # This is where XMLSyntaxError can happen

                if attempt > 0:
                    logger.info(
                        f"✅ SUCCESS on attempt {attempt+1}/5 for {company_name} ({news_id})"
                    )

                for elem in root.getiterator():
                    if "}" in elem.tag:
                        elem.tag = elem.tag.split("}", 1)[1]
                pdf_url_element = root.find(".//AttachmentURL")
                if pdf_url_element is not None and pdf_url_element.text:
                    return pdf_url_element.text

                logger.warning(
                    f"AttachmentURL not found for {company_name} ({news_id})"
                )
                return None  # Successful request, but no URL found

        except (aiohttp.ClientError, asyncio.TimeoutError, etree.XMLSyntaxError) as e:
            wait_time = 2**attempt
            logger.warning(
                f"Attempt {attempt+1}/5 for {company_name} ({news_id}) failed: {type(e).__name__}. Retrying in {wait_time}s..."
            )

            # If it's a syntax error, it means the server is angry. Hit the brakes.
            if isinstance(e, etree.XMLSyntaxError):
                throttle_event.set()  # Press the brake pedal

            await asyncio.sleep(wait_time)

    logger.error(
        f"❌ All 5 retries failed for {company_name} ({news_id}). Giving up on this item."
    )
    return None


def save_to_db_threaded(db_path: str, item: dict, pdf_url: str):
    """This function is executed in a separate thread and creates its own connection."""
    # (This function is correct and remains unchanged)
    try:
        date_string = item.get("DissemDT", "")
        dt_object = datetime.fromisoformat(date_string)
        ann_date = dt_object.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        logger.warning(
            f"Invalid date format '{date_string}' for {item.get('NEWSID')}. Skipping DB insert."
        )
        return

    with sqlite3.connect(db_path) as conn:
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO announcements (news_id, scrip_code, company_name, announcement_date, pdf_url)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    item.get("NEWSID"),
                    str(item.get("SCRIP_CD")),
                    item.get("SLONGNAME", "N/A").strip(),
                    ann_date,
                    pdf_url,
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            pass
        except Exception as e:
            logger.error(f"DB Insert failed for {item.get('NEWSID')}: {e}")


async def worker(name: str, queue: Queue, db_path: str):
    """The worker task that processes items from the queue."""
    # (This function is correct and remains unchanged, but I'm including it for completeness)
    async with aiohttp.ClientSession() as session:
        while True:
            item = await queue.get()
            if item is None:
                break

            pdf_url = await fetch_pdf_url_async(
                session,
                item.get("NEWSID"),
                item.get("SCRIP_CD"),
                item.get("SLONGNAME", "N/A").strip(),
            )

            if pdf_url:
                await asyncio.to_thread(save_to_db_threaded, db_path, item, pdf_url)
                logger.info(f"[{name}] Stored: {item.get('SLONGNAME', 'N/A').strip()}")

            queue.task_done()


# The function fetch_announcements_for_period is also correct and remains unchanged.
def fetch_announcements_for_period(session, from_date, to_date):
    params = {
        "pageno": 1,
        "strCat": "Company Update",
        "strPrevDate": from_date,
        "strScrip": "",
        "strSearch": "P",
        "strToDate": to_date,
        "strType": "C",
        "subcategory": "Earnings Call Transcript",
    }
    all_announcements = []
    logger.info(f"Fetching announcement list for {from_date} -> {to_date}...")
    try:
        response = session.get(API_URL, headers=HEADERS, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(
            f"FATAL: API request failed for the first page. Cannot proceed for this period. Error: {e}"
        )
        return []

    first_page_data = data.get("Table", [])
    if not first_page_data:
        return []

    all_announcements.extend(first_page_data)

    try:
        total_records = int(data.get("Table1", [{}])[0].get("ROWCNT", 0))
        records_per_page = len(first_page_data)
        total_pages = (total_records + records_per_page - 1) // records_per_page
    except (IndexError, TypeError, ValueError):
        total_pages = -1

    if total_pages != -1:
        logger.info(
            f"Total records for period: {total_records} | Total pages: {total_pages}"
        )

    current_page = 2
    while True:
        if total_pages != -1 and current_page > total_pages:
            break
        params["pageno"] = current_page
        logger.info(
            f"  -> Fetching page {current_page}/{total_pages if total_pages != -1 else '?'}"
        )
        try:
            response = session.get(API_URL, headers=HEADERS, params=params, timeout=60)
            response.raise_for_status()
            page_data = response.json().get("Table", [])

            if not page_data:
                break

            all_announcements.extend(page_data)
            current_page += 1
            time.sleep(1)
        except (requests.RequestException, ValueError) as e:
            logger.error(
                f"API request failed on page {current_page}. Stopping. Error: {e}"
            )
            break

    logger.info(
        f"Finished fetching a total of {len(all_announcements)} announcements for the period."
    )
    return all_announcements


# The main function also remains unchanged in its logic.
async def main():
    logger.info("🚀 Starting RESILIENT historical backfill process...")
    logger.info("👷‍♂️ Using %s concurrent workers.", MAX_CONCURRENT_WORKERS)
    logger.info("🗓️ Fetching data in 7-day chunks for maximum reliability.")

    conn_read_only = sqlite3.connect(HISTORICAL_DB)

    end_date = datetime.now()

    total_weeks_to_process = (MONTHS_TO_BACKFILL * 4) + 4

    with requests.Session() as list_fetch_session:
        for i in range(total_weeks_to_process):
            chunk_end_date = end_date - timedelta(days=i * 7)
            chunk_start_date = chunk_end_date - timedelta(days=6)

            from_date_str = chunk_start_date.strftime("%Y%m%d")
            to_date_str = chunk_end_date.strftime("%Y%m%d")

            logger.info(
                f"\n--- Processing period: {from_date_str} to {to_date_str} ---"
            )

            announcements = fetch_announcements_for_period(
                list_fetch_session, from_date_str, to_date_str
            )
            if not announcements:
                continue

            cursor = conn_read_only.cursor()
            news_ids_in_db = {
                row[0] for row in cursor.execute("SELECT news_id FROM announcements")
            }
            items_to_process = [
                item
                for item in announcements
                if item.get("NEWSID") not in news_ids_in_db
            ]

            if not items_to_process:
                logger.info(
                    "All announcements for this period are already in the database."
                )
                continue

            logger.info(
                f"Found {len(items_to_process)} new announcements to process in this chunk."
            )

            queue = Queue()
            for item in items_to_process:
                await queue.put(item)

            workers = [
                asyncio.create_task(worker(f"Worker-{w+1}", queue, HISTORICAL_DB))
                for w in range(MAX_CONCURRENT_WORKERS)
            ]

            await queue.join()

            for _ in range(MAX_CONCURRENT_WORKERS):
                await queue.put(None)

            await asyncio.gather(*workers)

            logger.info(
                f"--- Chunk complete. Processed {len(items_to_process)} items. ---"
            )

    conn_read_only.close()
    logger.info("✅ Historical backfill finished.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n🛑 Backfill stopped by user.")
