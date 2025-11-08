
---

# BSE Auto-Summarizer: Project Documentation

## 1. Project Overview

Welcome to the BSE Auto-Summarizer project!

This is an automated, standalone system designed to monitor the BSE India website for corporate announcements, specifically focusing on "Earnings Call Transcripts". Its core purpose is to fetch these announcements, intelligently analyze their content, generate high-quality, AI-powered summaries, and deliver these insights in real-time via Telegram notifications.

The system is built to be robust, resilient, and to operate continuously, providing timely financial intelligence without manual intervention.

## 2. Key Features

it's an end-to-end intelligence pipeline.

*   **Automated Real-Time Monitoring**: The system continuously polls the BSE API in `main.py`, ensuring that new announcements are picked up and processed within minutes of their release.
*   **Intelligent Content Analysis**: It doesn't just download files. The `PDFProcessor` applies a critical business rule: small documents (<= 3 pages) are treated as pointers to external media/web links, while large documents are treated as full-text transcripts. This avoids wasting resources on non-transcript files.
*   **Advanced AI Summarization (Gemini)**: The system uses Google's Gemini-Flash model to read entire transcripts or listen to audio/video files. It generates a structured, concise, and insightful summary that mimics the analysis of a seasoned financial expert.
*   **Rich Historical Context**: The system maintains a separate, two-year `historical_announcements.db`. When a new announcement for a company is found, the system automatically finds the *previous* earnings call, performs a "Just-in-Time" summary if needed, and feeds it to the AI. This allows for powerful comparative analysis in the final summary ("Did they meet their previous goals?", "How have risks evolved?").
*   **Robust Error Handling & Logging**: Network requests are wrapped in an exponential backoff retry mechanism. Every run (live, backfill, or test) generates a detailed, timestamped log file in the `/logs` directory, making debugging and monitoring straightforward.
*   **Dual-Channel Telegram Notifications**: Notifications are intelligently routed. High-level AI summaries are sent to one channel (for stakeholders/management), while technical details like web links and processing errors are sent to another (for developers/system monitoring).

## 3. The Intelligent Pipeline: How It Works

The system follows a clear, logical flow for each announcement it discovers:

1.  **Fetch Announcement List**: The `BSEScraper` hits the official BSE API to get a list of recent "Earnings Call Transcript" announcements within a specified time window.
2.  **Filter for New Items**: Each announcement `NEWSID` is checked against the local `database.db`. If it's already been processed, it's ignored.
3.  **Retrieve PDF URL**: For a new item, the scraper navigates to a secondary BSE page (the XBRL source) to find the actual URL of the announcement PDF.
4.  **Download & Store**: The PDF is downloaded into the `/downloads` directory. A record is added to `database.db` with a status of `DOWNLOADED`.
5.  **Process Content**: The downloaded PDF is passed to the `PDFProcessor`.
    *   **If Large (> 3 pages)**: The full text is extracted. The result is `{'type': 'text', 'content': '...'}`.
    *   **If Small (<= 3 pages)**: The text is scanned for URLs. The result is `{'type': 'link', 'links': [...]}`.
    *   **If Error**: An error object is created.
6.  **Find Historical Context**: The scraper queries `historical_announcements.db` for the most recent announcement from the *same company* that occurred *before* the current one.
    *   If a previous announcement is found and it doesn't have a summary, a "Just-in-Time" (JIT) summarization is performed on it and the result is cached in the historical DB. This ensures the context is ready for the next step.
7.  **Summarize with AI**: The processed content (text or media link) and the historical summary (if found) are sent to the `GeminiSummarizer`. The AI follows a strict prompt to generate a structured JSON output, including a direct comparison to the previous call.
8.  **Update Database**: The generated JSON summary is saved to the `summary_json` column in `database.db` for the corresponding `NEWSID`, and the status is updated to `PROCESSED` or `ERROR_PROCESSING`.
9.  **Notify**: Based on the result, the `TelegramNotifier` is called:
    *   **Full Summary**: A formatted message is sent to the "Summaries" channel.
    *   **Web Link**: A message with the discovered link(s) is sent to the "Links & Errors" channel.
    *   **Error**: A formatted error message is sent to the "Links & Errors" channel.

## 4. Codebase Breakdown

### Core Logic (`/core`)

This directory contains the modular building blocks of the system.

*   `scraper.py` **(Most Important)**
    *   This is the orchestrator. It manages the entire pipeline from fetching data to triggering notifications.
    *   Handles API communication, pagination, and data filtering.
    *   Coordinates with all other core modules (`DBHandler`, `PDFProcessor`, `GeminiSummarizer`, `HistoricalDBHandler`, `TelegramNotifier`).
    *   Contains the logic for historical context lookup and JIT summarization.

*   `summarizer.py` **(Very Important)**
    *   Handles all interactions with the Google Gemini API.
    *   Contains the structured prompts that guide the AI to produce consistent, high-quality JSON output.
    *   Manages media file downloads, uploads to the Gemini service, and cleanup.
    *   Includes a robust retry mechanism for API calls to handle transient network issues.

*   `processor.py`
    *   Responsible for the initial analysis of a downloaded PDF.
    *   Implements the critical "size-first" business logic to determine if a PDF is a transcript or just a link container.
    *   Extracts text and URLs from the PDFs.

*   `notifier.py`
    *   Formats and sends all messages to the configured Telegram channels.
    *   Uses a simple, plain-text sending method to avoid MarkdownV2 parsing errors from the AI's output.
    *   Separates different types of notifications (summaries, links, errors) to their respective channels.

*   `db_handler.py`
    *   Manages the main operational database (`database.db`).
    *   Keeps track of which announcements have been downloaded and which have been summarized.
    *   Stores the final JSON output from the summarizer.

*   `historical_db_handler.py`
    *   Manages the long-term historical database (`historical_announcements.db`).
    *   Primarily used for read-only lookups to find previous announcements for comparison.
    *   Includes a function to update a historical record with a JIT summary.

### Executable Scripts

These are the entry points for running the system in different modes.

*   `main.py`: The primary script for **live, continuous monitoring**. It runs in an infinite loop, polling for new data every 60 seconds. This is the script you will deploy for production.
*   `backfill.py`: Use this for processing **historical data**. It runs once based on the `START_DATE` and `END_DATE` in the `.env` file and then exits.
*   `test_single.py`: A utility script for **testing and debugging**. It processes a single PDF URL defined in the `.env` file, allowing for quick iteration on the summarization and notification logic.
*   `historical_backfill.py`: A specialized, high-concurrency script used to build the initial `historical_announcements.db`. It fetches up to 2 years of announcement metadata. **This should only be run once during initial setup.**
*   `init_historical_db.py`: A helper script to create and initialize the `historical_announcements.db` file with the correct schema and indexes.

## 5. Setup & Installation

### Prerequisites

*   **OS**: A Linux-based environment is strongly recommended (e.g., Ubuntu, Debian, or WSL on Windows).
*   **Python**: Python 3.9 or higher.
*   **Git**: For cloning the repository.

### Installation Steps

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/pravinheroku/bse_automation
    cd bse_auto
    ```

2.  **Create and activate a Python virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install the required packages:**
    
    ```bash
    pip install -r requirements.txt
    ```

### Creating the `.env` Configuration File

This is the **most critical step**. The system will not run without it. Create a file named `.env` in the root of the project directory and paste the following content into it.

```ini
# --- Scraper Configuration ---
# For live mode (main.py), this is the number of hours to look back on each run.
LOOKBACK_HOURS=24

# --- FOR BACKFILLING (backfill.py) ---
# Format is YYYYMMDD. Set these to run a historical backfill.
START_DATE=20240101
END_DATE=20240102
# Limit the number of *new* items processed in a single run. 0 = unlimited.
MAX_ITEMS_TO_PROCESS=5

# --- Gemini API Configuration ---
# Your Google AI Studio API key
GEMINI_API_KEY="YOUR_GEMINI_API_KEY_HERE"

# --- Telegram Notifications ---
# The token for your Telegram Bot from BotFather
TELEGRAM_BOT_TOKEN="YOUR_TELEGRAM_BOT_TOKEN_HERE"
# The Chat ID for sending AI-generated summaries (for stakeholders)
TELEGRAM_CHAT_ID_SUMMARIES="TARGET_CHAT_ID_FOR_SUMMARIES"
# The Chat ID for sending web links and error alerts (for developers)
TELEGRAM_CHAT_ID_LINKS="DEVELOPER_MONITORING_CHAT_ID"

# --- FOR SINGLE PDF TESTING (test_single.py) ---
SINGLE_TEST_PDF_URL=https://www.bseindia.com/stockinfo/AnnPdfOpen.aspx?Pname=some_pdf_guid.pdf
SINGLE_TEST_COMPANY_NAME=Example Company Ltd
SINGLE_TEST_SCRIP_CODE=500000
```

**Fill in the following values:**

*   `GEMINI_API_KEY`: Your API key from Google AI Studio.
*   `TELEGRAM_BOT_TOKEN`: The token for the Telegram bot you created.
*   `TELEGRAM_CHAT_ID_SUMMARIES`: The chat ID where the final summaries should be sent.

> **IMPORTANT NOTE:** Please **do not change** the value for `TELEGRAM_CHAT_ID_LINKS`. It is currently configured to send critical system alerts and debugging information to the original developer's channel. This is vital for ongoing system health monitoring and support.

## 6. How to Use the System

### Initializing the Historical Database

 The historical_announcements.db is already given so no need to run the `historical_backfill.py`.


### Mode 1: Live Monitoring (`main.py`)

This is the standard production mode. It will run forever, polling for new announcements every minute.

*   **To run:**
    ```bash
    python main.py
    ```
*   **To stop:** Press `Ctrl+C`.

### Mode 2: Historical Backfill (`backfill.py`)

Use this to process data from a specific historical date range.

1.  **Configure `.env`**:
    *   Set `START_DATE` and `END_DATE` to the desired range (e.g., `20240501` to `20240531`).
    *   Set `MAX_ITEMS_TO_PROCESS` if you want to limit the run (e.g., to `10` for a test).
2.  **Run the script:**
    ```bash
    python backfill.py
    ```
    The script will process all announcements in that range and then exit automatically.

### Mode 3: Testing a Single PDF (`test_single.py`)

Use this to debug the processing and summarization for one specific announcement.

1.  **Configure `.env`**:
    *   Set `SINGLE_TEST_PDF_URL` to the direct URL of the PDF you want to test.
    *   Set `SINGLE_TEST_COMPANY_NAME` and `SINGLE_TEST_SCRIP_CODE`.
2.  **Run the script:**
    ```bash
    python test_single.py
    ```
    The script will download and process only that single file and then exit.

## 7. System Maintenance & Monitoring

The primary tool for monitoring the system's health is the `/logs` directory.

*   Every time you run `main.py`, `backfill.py`, or `test_single.py`, a new sub-directory is created (e.g., `logs/LIVE-20240521-143000`).
*   Inside this directory, a `run.log` file contains a detailed, step-by-step record of everything the system did during that run.
*   If you encounter any issues, this log file is the first place to look for error messages and clues. The error notifications sent to the developer Telegram channel also provide real-time alerts for any failures.
