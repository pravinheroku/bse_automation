# core/scraper.py
import requests
from lxml import etree
from datetime import datetime, timedelta
from pathlib import Path
import time
import os
from dotenv import load_dotenv
import json
import logging
import asyncio
from urllib.parse import urlparse
from typing import Callable, Awaitable, Optional, Dict, Any, Tuple

from .db_handler import DBHandler
from .processor import PDFProcessor
from .summarizer import GeminiSummarizer
from .notifier import TelegramNotifier
from .historical_db_handler import HistoricalDBHandler

load_dotenv()


class BSEScraper:
    def __init__(self, test_mode=False):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Referer": "https://www.bseindia.com/",
            "Origin": "https://www.bseindia.com",
        }
        self.api_url = (
            "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
        )
        self.xbrl_base_url = "https://www.bseindia.com/Msource/90D/CorpXbrlGen.aspx"
        self.download_path = Path("downloads")
        self.download_path.mkdir(exist_ok=True)
        self.test_mode = test_mode
        self.logger = logging.getLogger(__name__)
        log_dir_path = Path("logs")
        log_dir_path.mkdir(exist_ok=True)
        self.url_log_file = log_dir_path / "pdf_urls.log"
        self.db = DBHandler()
        try:
            self.historical_db = HistoricalDBHandler()
        except FileNotFoundError:
            self.logger.critical(
                "Historical DB not found. Comparison features will be disabled."
            )
            self.historical_db = None
        self.pdf_processor = PDFProcessor()
        self.summarizer = GeminiSummarizer()
        self.notifier = TelegramNotifier()
        self.start_date = os.getenv("START_DATE")
        self.end_date = os.getenv("END_DATE")
        self.max_items = int(os.getenv("MAX_ITEMS_TO_PROCESS", 0))
        if not (self.start_date and self.end_date):
            self.lookback_hours = int(os.getenv("LOOKBACK_HOURS", 24))
            self.logger.info(
                f"🔧 Config: Real-time mode. Lookback period set to {self.lookback_hours} hours."
            )
        else:
            self.logger.info(
                f"🔧 Config: Test/Backfill mode. Fetching from {self.start_date} to {self.end_date}."
            )
        if self.max_items > 0:
            self.logger.info(
                f"🔧 Config: Limiting this run to a maximum of {self.max_items} new items."
            )
        if self.test_mode:
            self.logger.warning(
                "--- SCRAPER RUNNING IN TEST MODE: PDF downloads & Summarization are DISABLED. ---"
            )

    def close_connections(self):
        """Closes all database connections gracefully."""
        self.logger.info("Closing database connections...")
        if self.db:
            self.db.close()
        if self.historical_db:
            self.historical_db.close()

    def _make_resilient_request(
        self,
        method: str,
        url: str,
        retries: int = 5,
        backoff_factor: float = 2.0,
        **kwargs,
    ) -> Optional[requests.Response]:
        """A robust, centralized synchronous request handler with exponential backoff."""
        session = requests.Session()
        for attempt in range(retries):
            try:
                response = session.request(method, url, **kwargs)
                response.raise_for_status()
                if attempt > 0:
                    self.logger.info(
                        f"✅ Successfully completed request to {url} on attempt {attempt + 1}/{retries}."
                    )
                return response
            except requests.exceptions.RequestException as e:
                wait_time = backoff_factor * (2**attempt)
                self.logger.warning(
                    f"Request to {url} failed (Attempt {attempt + 1}/{retries}): {type(e).__name__}. Retrying in {wait_time:.2f}s..."
                )
                if attempt + 1 == retries:
                    break
                time.sleep(wait_time)
        self.logger.error(
            f"❌ All {retries} retries failed for request to {url}. Giving up."
        )
        return None

    def _get_api_params(self):
        """Prepares parameters for the API call based on the mode."""
        if self.start_date and self.end_date:
            from_date_str, to_date_str = self.start_date, self.end_date
        else:
            to_date = datetime.now()
            from_date = to_date - timedelta(hours=self.lookback_hours)
            from_date_str, to_date_str = (
                from_date.strftime("%Y%m%d"),
                to_date.strftime("%Y%m%d"),
            )
        return {
            "pageno": 1,
            "strCat": "Company Update",
            "strPrevDate": from_date_str,
            "strScrip": "",
            "strSearch": "P",
            "strToDate": to_date_str,
            "strType": "C",
            "subcategory": "Earnings Call Transcript",
        }

    def _make_api_request(self, params, retries=3, backoff_factor=5):
        """A resilient method to make an API request with retries."""
        for attempt in range(retries):
            try:
                response = requests.get(
                    self.api_url, headers=self.headers, params=params, timeout=60
                )
                response.raise_for_status()
                return response.json()
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                self.logger.warning(
                    f"Attempt {attempt + 1}/{retries} failed for page {params.get('pageno', 1)}: {e}"
                )
                if attempt + 1 == retries:
                    self.logger.error(
                        f"All {retries} retries failed for page {params.get('pageno', 1)}. Giving up."
                    )
                    return None
                wait_time = backoff_factor * (2**attempt)
                self.logger.info(f"Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)

    def fetch_announcements(self):
        """Fetches 'Earnings Call Transcript' announcements with smarter pagination and retries."""
        all_announcements = []
        params = self._get_api_params()
        self.logger.info("📡 Initial fetch to get total count...")
        initial_data = self._make_api_request(params)
        if not initial_data:
            return []
        total_records = initial_data.get("Table1", [{}])[0].get("ROWCNT", 0)
        if total_records == 0:
            self.logger.info("✔️ No records found for this period.")
            return []
        self.logger.info(f"✔️ Total records to fetch: {total_records}")
        records_this_page = initial_data.get("Table", [])
        all_announcements.extend(records_this_page)
        records_per_page = len(records_this_page)
        if records_per_page == 0:
            return []
        total_pages = (total_records + records_per_page - 1) // records_per_page
        for page_no in range(2, total_pages + 1):
            self.logger.info(f"📡 Fetching page {page_no}/{total_pages}...")
            params["pageno"] = page_no
            page_data = self._make_api_request(params)
            if not page_data or not page_data.get("Table"):
                self.logger.error(
                    f"Failed to retrieve data for page {page_no}. Stopping pagination."
                )
                break
            all_announcements.extend(page_data.get("Table", []))
        self.logger.info(
            f"✔️ Fetched a total of {len(all_announcements)} announcements."
        )
        return all_announcements

    def get_pdf_url_from_xbrl(self, news_id, scrip_code):
        params = {"Bsenewid": news_id, "Scripcode": scrip_code}
        response = self._make_resilient_request(
            "GET", self.xbrl_base_url, params=params, headers=self.headers, timeout=15
        )
        if not response:
            self.logger.error(
                f"❌ Failed to fetch XBRL for NEWSID {news_id} after all retries."
            )
            return None
        try:
            root = etree.fromstring(response.content)
            for elem in root.getiterator():
                if not hasattr(elem.tag, "find"):
                    continue
                i = elem.tag.find("}")
                if i >= 0:
                    elem.tag = elem.tag[i + 1 :]
            pdf_url_element = root.find(".//AttachmentURL")
            if pdf_url_element is not None and pdf_url_element.text:
                return pdf_url_element.text
            self.logger.warning(f"⚠️ Could not find AttachmentURL for NEWSID {news_id}")
            return None
        except etree.XMLSyntaxError as e:
            self.logger.error(
                f"❌ Error parsing valid XBRL response for NEWSID {news_id}: {e}"
            )
            return None

    def download_pdf(
        self, pdf_url: str, scrip_code: str, company_name: str, news_id: str
    ) -> Path | None:
        if self.test_mode:
            log_entry = f"{datetime.now().isoformat()} | {company_name} | {pdf_url}\n"
            with open(self.url_log_file, "a") as f:
                f.write(log_entry)
            self.logger.info(f"📝 Logged URL for {company_name}")
            return None
        if pdf_url.startswith("file://"):
            try:
                local_path = Path(urlparse(pdf_url).path)
                if local_path.exists():
                    self.logger.info(f"✅ Accessed local test PDF: {local_path}")
                    return local_path
                else:
                    self.logger.error(f"❌ Local test PDF not found at {local_path}")
                    return None
            except Exception as e:
                self.logger.error(f"❌ Failed to handle local file URI {pdf_url}: {e}")
                return None
        else:
            self.logger.info(f"⬇️ Downloading PDF for {company_name} from {pdf_url}")
            response = self._make_resilient_request(
                "GET", pdf_url, headers=self.headers, timeout=60, stream=True
            )
            if not response:
                self.logger.error(
                    f"❌ Failed to download PDF for NEWSID {news_id} after all retries."
                )
                return None
            try:
                safe_name = "".join(
                    [c for c in company_name if c.isalnum() or c.isspace()]
                ).rstrip()
                filename = f"{scrip_code}_{safe_name}_{news_id[:8]}.pdf"
                filepath = self.download_path / filename
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                self.logger.info(f"💾 Saved PDF to {filepath}")
                return filepath
            except Exception as e:
                self.logger.error(
                    f"❌ Failed to save downloaded PDF for NEWSID {news_id} to disk: {e}"
                )
                return None

    async def _get_historical_summary_for_comparison(
        self, scrip_code: str, current_ann_date: str
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """
        Finds the most recent prior announcement and performs "Just-in-Time" summarization.
        - Processes historical media links.
        - Aborts if the historical document is a non-media web link.
        - Returns both the summary and a context dictionary with relevant URLs.
        """
        if not self.historical_db:
            return None
        self.logger.info(
            f"🔄 [{scrip_code}] Searching for previous announcement before {current_ann_date}..."
        )
        prev_ann = self.historical_db.get_latest_announcement_for_scrip(
            scrip_code, current_ann_date
        )
        if not prev_ann:
            self.logger.info(
                f"[{scrip_code}] No previous announcements found in historical DB."
            )
            return None
        self.logger.info(
            f"[{scrip_code}] Found previous announcement: {prev_ann['news_id']} from {prev_ann['announcement_date']}"
        )
        comparison_context = {"comparison_pdf_url": prev_ann.get("pdf_url")}
        if prev_ann.get("summary_json"):
            try:
                summary = json.loads(prev_ann["summary_json"])
                if summary.get("type") == "summary":
                    self.logger.info(
                        f"[{scrip_code}] Previous summary already cached. Using it."
                    )
                    return summary, comparison_context
                else:
                    self.logger.warning(
                        f"[{scrip_code}] Cached historical record is not a valid summary. Skipping."
                    )
                    return None
            except json.JSONDecodeError:
                self.logger.warning(
                    f"[{scrip_code}] Failed to parse cached summary. Will regenerate."
                )
        self.logger.info(
            f"[{scrip_code}] JIT Summarization required for {prev_ann['news_id']}..."
        )
        pdf_path = None
        try:
            pdf_path = self.download_pdf(
                prev_ann["pdf_url"],
                str(scrip_code),
                prev_ann["company_name"],
                prev_ann["news_id"],
            )
            if not pdf_path or not pdf_path.exists():
                self.logger.error(
                    f"[{scrip_code}] JIT Failed: Could not download historical PDF."
                )
                return None
            content_data = self.pdf_processor.process_pdf(pdf_path)
            summary_json = await self.summarizer.summarize(
                content_data,
                prev_ann["company_name"],
                prev_ann["pdf_url"],
                previous_summary=None,
                is_historical_jit=True,
            )
            if summary_json and summary_json.get("type") == "web_link":
                self.logger.warning(
                    f"[{scrip_code}] JIT Aborted: Historical PDF {prev_ann['news_id']} is an external web link. Ignoring for comparison."
                )
                self.historical_db.update_summary(prev_ann["news_id"], summary_json)
                return None
            if summary_json and summary_json.get("type") == "summary":
                if media_links := summary_json.get("links", []):
                    comparison_context["comparison_media_url"] = media_links[0].get(
                        "url"
                    )
                self.historical_db.update_summary(prev_ann["news_id"], summary_json)
                self.logger.info(
                    f"[{scrip_code}] JIT Success: Generated and saved historical summary."
                )
                return summary_json, comparison_context
            else:
                self.logger.warning(
                    f"[{scrip_code}] JIT summary generation failed. Cannot use for comparison."
                )
                if summary_json:
                    self.historical_db.update_summary(prev_ann["news_id"], summary_json)
                return None
        except Exception as e:
            self.logger.error(
                f"[{scrip_code}] JIT summarization for {prev_ann['news_id']} failed with exception: {e}",
                exc_info=True,
            )
            return None
        finally:
            if pdf_path and pdf_path.exists():
                try:
                    pdf_path.unlink()
                except OSError as e:
                    self.logger.warning(
                        f"Could not delete JIT temporary file {pdf_path}: {e}"
                    )

    async def process_and_summarize(
        self,
        pdf_path: Path,
        news_id: str,
        company_name: str,
        pdf_url: str,
        previous_summary: Optional[Dict] = None,
        comparison_context: Optional[Dict] = None,
    ):
        if self.test_mode:
            self.logger.info("Summarization skipped in test mode.")
            return None
        if not self.db.needs_summarization(news_id):
            self.logger.info(
                f"🔵 Item {news_id} already processed/summarized. Skipping."
            )
            return None

        self.logger.info(f"⚙️ Processing PDF for {company_name} ({news_id})...")
        content_data = self.pdf_processor.process_pdf(pdf_path)

        summary_data = await self.summarizer.summarize(
            content_data, company_name, pdf_url, previous_summary
        )

        if not summary_data:
            self.logger.error(
                f"Summarization returned no data for {news_id}. Marking as error."
            )
            summary_data = self.summarizer._create_error_json(
                "summarizer_failure", "Summarizer returned None", company_name, pdf_url
            )

        if summary_data and comparison_context:
            summary_data.update(comparison_context)

        status = (
            "PROCESSED" if summary_data.get("type") != "error" else "ERROR_PROCESSING"
        )
        self.db.update_summary(news_id, summary_data, status)
        self.logger.info(f"💾 Updated database for {news_id} with status: {status}")

        if status == "PROCESSED":
            if summary_data.get("type") == "summary":
                return lambda: self.notifier.notify_summary(summary_data)
            elif summary_data.get("type") == "web_link":
                if previous_summary:
                    self.logger.info(
                        f"[{company_name}] New item is web link, but historical context found. Sending detailed notification."
                    )
                    return lambda: self.notifier.notify_weblink_with_context(
                        summary_data, previous_summary
                    )
                else:
                    return lambda: self.notifier.notify_weblink(summary_data)
        elif summary_data.get("type") == "error":
            return lambda: self.notifier.notify_error(summary_data)
        return None

    async def run_all_notifications_sequentially(
        self, tasks: list[Callable[[], Awaitable[None]]]
    ) -> None:
        """Runs notification tasks one by one with a 2-second delay between each."""
        if not tasks:
            return
        total = len(tasks)
        self.logger.info(
            "--- Sending %s notifications sequentially (1 per 2 s) ---", total
        )
        for idx, task_factory in enumerate(tasks, 1):
            self.logger.info("  -> notification %s/%s", idx, total)
            await task_factory()
            if idx < total:
                await asyncio.sleep(2)
        self.logger.info("--- All notifications sent successfully ---")

    async def run(self, announcements_override=None) -> list:
        self.logger.info("--- Starting BSE Scraper Run ---")
        announcements = (
            announcements_override
            if announcements_override is not None
            else self.fetch_announcements()
        )
        if not announcements:
            self.logger.info("--- No announcements found. Ending run. ---")
            self.close_connections()
            return []
        new_items_processed = 0
        notification_tasks: list[Callable[[], Awaitable[None]]] = []
        for item in reversed(announcements):
            if self.max_items > 0 and new_items_processed >= self.max_items:
                self.logger.warning(
                    f"🛑 Reached processing limit of {self.max_items}. Halting run."
                )
                break
            news_id = item.get("NEWSID")
            scrip_code = str(item.get("SCRIP_CD"))
            name = item.get("SLONGNAME", "N/A").strip()
            try:
                dissem_dt_str = item.get("DissemDT", "")
                announcement_date = datetime.fromisoformat(dissem_dt_str).strftime(
                    "%Y-%m-%d"
                )
            except (ValueError, TypeError):
                self.logger.warning(
                    f"Could not parse date for {news_id}. Cannot perform historical lookup."
                )
                announcement_date = datetime.now().strftime("%Y-%m-%d")
            if not news_id:
                continue
            if self.db.is_processed(news_id) and not self.db.needs_summarization(
                news_id
            ):
                continue
            pdf_path = None
            pdf_url = item.get("PDF_URL_OVERRIDE")
            if not self.db.is_processed(news_id):
                new_items_processed += 1
                self.logger.info(
                    f"✨ New item found for {name} ({news_id}) [Item {new_items_processed}/{self.max_items if self.max_items > 0 else '∞'}]"
                )
                if not pdf_url:
                    pdf_url = self.get_pdf_url_from_xbrl(news_id, scrip_code)
                if pdf_url:
                    pdf_path = self.download_pdf(
                        pdf_url, str(scrip_code), name, news_id
                    )
                    if pdf_path:
                        self.db.add_new_announcement(news_id, str(scrip_code), name)
                    elif self.test_mode or item.get("is_test"):
                        self.db.add_new_announcement(news_id, str(scrip_code), name)
            if pdf_path or (
                self.db.is_processed(news_id) and self.db.needs_summarization(news_id)
            ):
                if not pdf_path:
                    safe_name = "".join(
                        [c for c in name if c.isalnum() or c.isspace()]
                    ).rstrip()
                    filename = f"{scrip_code}_{safe_name}_{news_id[:8]}.pdf"
                    pdf_path = self.download_path / filename
                if pdf_path.exists():
                    if not pdf_url:
                        pdf_url = self.get_pdf_url_from_xbrl(news_id, scrip_code) or ""
                    previous_summary = None
                    comparison_context = None
                    historical_result = (
                        await self._get_historical_summary_for_comparison(
                            scrip_code, announcement_date
                        )
                    )
                    if historical_result:
                        previous_summary, comparison_context = historical_result
                    notification_task_factory = await self.process_and_summarize(
                        pdf_path,
                        news_id,
                        name,
                        pdf_url,
                        previous_summary,
                        comparison_context,
                    )
                    if notification_task_factory:
                        notification_tasks.append(notification_task_factory)
                else:
                    self.logger.warning(
                        f"⚠️ PDF for {name} ({news_id}) not found, cannot process."
                    )
        if notification_tasks:
            self.logger.info(
                f"📦 Scraper run produced {len(notification_tasks)} notification tasks to be sent."
            )
        if new_items_processed == 0:
            self.logger.info(
                "✔️ No *new* announcements to download. All items up-to-date."
            )
        else:
            action = "logged URLs for" if self.test_mode else "processed"
            self.logger.info(
                f"✨ Run complete. Found and {action} {new_items_processed} new announcements."
            )
        self.logger.info("--- BSE Scraper Run Finished ---")
        self.close_connections()
        return notification_tasks
