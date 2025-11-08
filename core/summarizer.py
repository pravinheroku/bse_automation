# core/summarizer.py
import os
import json
import logging
import requests
from pathlib import Path
import time
import mimetypes
import random
from json import JSONDecodeError
from typing import Optional, Dict, Callable

import google.generativeai as genai
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger()

TARGET_CHAR_LIMIT = 2800


def _gemini_call_with_retry(call_fn: Callable, *, desc="gemini_call", max_attempts=3):
    """
    Universal retry wrapper for ANY Gemini API call (text or media).
    call_fn()  ->  response object with .text attribute
    """
    for attempt in range(1, max_attempts + 1):
        # A small delay to avoid overwhelming the API

        time.sleep(1 + attempt)
        try:
            resp = call_fn()

            if not hasattr(resp, "text") or not resp.text or not resp.text.strip():
                raise ValueError("Empty or invalid response object from Gemini API")

            cleaned = (
                resp.text.strip().removeprefix("```json").removesuffix("```").strip()
            )
            return json.loads(cleaned)
        except (JSONDecodeError, ValueError) as exc:
            logger.warning(
                f"⚠️  {desc} failed (attempt {attempt}/{max_attempts}): {exc}"
            )
            if attempt == max_attempts:
                break
            sleep_time = 2**attempt + random.uniform(0, 1)  # exp-backoff + jitter
            logger.info(f"Retrying in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
        except Exception as exc:  # Catch-all so the main pipeline survives
            logger.exception(f"🔥 Unexpected fatal error in {desc}")
            break

    logger.error(f"❌ {desc} exhausted all retries – returning fallback error JSON")
    return None


class GeminiSummarizer:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in .env file.")

        genai.configure(api_key=self.api_key)

        self.model = genai.GenerativeModel("gemini-flash-latest")
        logger.info(f"🧠 GeminiSummarizer initialized with model: gemini-flash-latest")

        self.media_cache_path = Path("media_cache")
        self.media_cache_path.mkdir(exist_ok=True)

    def _extract_company_name_from_text(self, text: str) -> str:
        """Fallback to extract company name if it's missing."""
        prompt = """
        You are a financial document analyst. Extract the primary **company name** from the following text.
        Return ONLY the company name as a plain string. If unsure, return "Unknown Company".
        """
        try:
            resp = self.model.generate_content([prompt + "\n\n" + text[:4000]])
            return resp.text.strip() or "Unknown Company"
        except Exception as e:
            logger.warning(f"⚠️ Failed to extract company name via AI: {e}")
            return "Unknown Company"

    def _generate_historical_summary_prompt(self, company_name: str) -> str:
        """Generates a prompt to create a concise summary of a historical document."""
        return f"""
You are an expert financial analyst AI. Your task is to create a concise summary of the provided historical earnings call transcript for '{company_name}'. This summary will be used for comparison against a future call.

**Instructions:**
1.  **Focus on Key Outcomes:** Extract the main financial results, strategic goals stated at the time, and any major risks discussed.
2.  **Be Concise:** The entire output must be a single, valid JSON object and should be well under {TARGET_CHAR_LIMIT} characters.

**Output Format (Strict JSON):**
{{
    "company_name": "{company_name}",
    "type": "summary",
    "executive_summary": "A 2-3 sentence summary of the call's key outcome.",
    "key_financials": ["Metric 1", "Metric 2"],
    "strategic_outlook": ["Goal 1", "Promise 2"],
    "risks_and_concerns": ["Risk 1 mentioned"]
}}
"""

    def _generate_structured_prompt(
        self, company_name: str, previous_summary: Optional[Dict] = None
    ) -> str:

        TARGET_CHAR_LIMIT = 2800

        if previous_summary:
            previous_summary_str = json.dumps(previous_summary, indent=2)
            historical_comparison_instruction = f"""
8.  **Previous Call Comparison (Max 400 chars):** You have been provided with the JSON summary of the previous earnings call. Your primary task is to compare the CURRENT call to the PREVIOUS one. **The value for this key MUST be a single, well-formatted string, NOT a JSON object.** Summarize the comparison, addressing:
    - Did management execute on their previously stated goals (from `strategic_outlook`)?
    - How have the financials changed?
    - Have previous risks been mitigated or have new ones emerged?
    - Conclude if the company's position has improved, weakened, or remained stable.

Here is the JSON from the previous call:
```json
{previous_summary_str}
```"""
        else:
            historical_comparison_instruction = "8.  **Previous Call Comparison:** No previous call data was provided for comparison. State this explicitly as a string."

        return f"""
You are an expert financial analyst AI. Your analysis is concise, data-driven, and rivals a seasoned human analyst. Analyze the earnings call transcript for '{company_name}'.

**CRITICAL OUTPUT CONSTRAINTS:**
1.  **JSON ONLY:** You MUST return a single, valid JSON object. No text, notes, or explanations before or after the JSON.
2.  **STRICT CHARACTER LIMIT:** The final, stringified JSON output MUST be under **{TARGET_CHAR_LIMIT} characters**. This is a hard limit. Be extremely concise, adhering to the per-field limits below.
3.  **STRINGS, NOT OBJECTS:** All values in the final JSON must be strings or lists of strings. **Do NOT use nested JSON objects.**
4.  **RAW TEXT ONLY:** The string values you generate MUST be clean, raw text. **Do NOT include any Markdown formatting (like *, _, `), escape characters (like \\n, \\), or HTML tags.** The application will handle all formatting.

**Analysis Instructions (Adhere to character limits):**
1.  **Executive Summary (Max 350 chars):** 2-3 sentences.
2.  **Key Takeaway (Max 200 chars):** A single, high-conviction sentence.
3.  **Key Financials:** A list of short strings (max 70 chars each).
4.  **Strategic Outlook:** A list of short strings (max 100 chars each).
5.  **Risks & Concerns:** A list of strings. **For each risk, format it as a single string: "Risk Description (Mitigation: Stated Mitigation Strategy)".**
6.  **Management Tone (Max 150 chars):** A single string.
7.  **Key Q&A Highlights:** A list of short strings (max 150 chars each).
{historical_comparison_instruction}

**Sentiment Options (choose ONE):**
- Strongly Bullish | Moderately Bullish | Neutral | Cautious/Bearish | Strongly Bearish

**Output Format (Example with STRICT formatting):**
{{
    "company_name": "{company_name}",
    "type": "summary",
    "executive_summary": "Share India delivered robust sequential results driven by MTF expansion and secured Board approval for $50 million in FCCBs to fuel future growth and lower capital costs.",
    "key_takeaway": "The primary takeaway is that management has successfully de-risked its aggressive MTF growth strategy via the strategic $50M FCCB approval, securing its main profit engine.",
    "sentiment": "Strongly Bullish",
    "management_tone": "Overwhelmingly confident and bullish, backed by strong execution on their MTF targets and a proactive approach to financing (FCCB approval).",
    "key_financials": [
        "Revenue: ₹X Crores (Up Y% QoQ)",
        "PAT: ₹Z Crores (Up A% QoQ)",
        "PAT Margin improved to 27%.",
        "Declared dividend of ₹2 per share."
    ],
    "strategic_outlook": [
        "Accelerating Credit (MTF) with a target of INR 1,000 Crores by Dec 2027.",
        "Entering Wealth Management (PMS/AIF) as a major diversification."
    ],
    "risks_and_concerns": [
        "Competitive Yield Compression in the MTF business (Mitigation: Planned $50M FCCB issuance to lower the cost of capital).",
        "New Venture Integration and Execution Risk for the PMS/AIF and Debt Capital verticals (Mitigation: Hiring specialized teams)."
    ],
    "key_qa_highlights": [
        "Q: How will you compete on MTF yields? A: The FCCB will lower our cost of funds, allowing us to remain competitive while protecting margins."
    ],
    "comparison_with_previous_call": "The company's position has significantly improved. Management successfully executed on their promise to grow the MTF book and directly addressed the previous 'Capital Deployment Risk' by securing the FCCB approval. The new risk of yield compression is already being actively mitigated."
}}
"""

    async def _summarize_media_from_url(
        self,
        media_url: str,
        company_name: str,
        original_pdf_url: str,
        previous_summary: Optional[Dict] = None,
        is_historical_jit: bool = False,
        desc_prefix: str = "",
    ) -> dict:
        """Downloads, processes, and summarizes a media file using the appropriate prompt."""
        filepath = None
        media_file = None
        try:
            logger.info(f"⬇️ Downloading media for summarization from {media_url}")
            random_suffix = "".join(
                random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=6)
            )
            filename = f"{Path(media_url).stem}_{random_suffix}{Path(media_url).suffix}"

            if media_url.startswith("file://"):
                filepath = Path(media_url[7:])
                if not filepath.exists():
                    raise FileNotFoundError(f"Local test file not found: {filepath}")
                logger.info(f"📎 Using local test file: {filepath}")
            else:
                filepath = self.media_cache_path / filename
                response = requests.get(
                    media_url, timeout=120, stream=True, verify=False
                )
                response.raise_for_status()
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

            logger.info(f"🧠 Uploading '{filename}' to Gemini...")
            media_file = genai.upload_file(path=filepath)
            while media_file.state.name == "PROCESSING":
                time.sleep(2)
                media_file = genai.get_file(name=media_file.name)
            if media_file.state.name == "FAILED":
                raise Exception(f"Gemini file processing failed: {media_file.state}")

            logger.info(
                f"🗣️ {desc_prefix}Generating structured summary from media for '{company_name}'..."
            )
            if is_historical_jit:
                prompt = self._generate_historical_summary_prompt(company_name)
            else:
                prompt = self._generate_structured_prompt(
                    company_name, previous_summary
                )

            def _call():
                return self.model.generate_content([prompt, media_file])

            summary_json = _gemini_call_with_retry(
                _call, desc=f"{desc_prefix}media summary for {company_name}"
            )

            if summary_json is None:
                return self._create_error_json(
                    "gemini_media_error",
                    "Gemini media summarisation failed after retries",
                    company_name,
                    original_pdf_url,
                )

            logger.info(
                f"✅ Successfully generated summary from media for '{company_name}'."
            )
            summary_json["links"] = [{"url": media_url, "link_type": "media"}]
            summary_json["original_pdf_url"] = original_pdf_url
            return summary_json

        except Exception as e:
            logger.error(
                f"❌ Media summarization failed for '{company_name}' (URL: {media_url}): {e}",
                exc_info=True,
            )
            error_json = self._create_error_json(
                "media_summarization_error", str(e), company_name, original_pdf_url
            )
            error_json["links"] = [{"url": media_url, "link_type": "media"}]
            return error_json
        finally:
            if filepath and filepath.exists() and not media_url.startswith("file://"):
                try:
                    filepath.unlink()
                except OSError as e:
                    logger.warning(f"Could not delete temp media file {filepath}: {e}")
            if media_file:
                try:
                    genai.delete_file(name=media_file.name)
                except Exception as e:
                    logger.warning(
                        f"Could not delete Gemini file {media_file.name}: {e}"
                    )

    async def summarize(
        self,
        content_data: dict,
        company_name: str,
        original_pdf_url: str,
        previous_summary: Optional[Dict] = None,
        is_historical_jit: bool = False,
    ) -> dict:
        """
        Orchestrates summarization for both new and historical (JIT) content.
        - If is_historical_jit is True, it generates a simpler summary for comparison purposes.
        - It correctly handles text, media links, and web links for both cases.
        """
        if not company_name or company_name.strip().lower() in [
            "n/a",
            "unknown",
            "unknown company",
        ]:
            logger.info("Company name is missing, attempting to extract from text...")
            company_name = self._extract_company_name_from_text(
                content_data.get("content", "")
            )
            logger.info(f"Extracted company name: '{company_name}'")

        content_type = content_data.get("type")
        log_prefix = "[JIT] " if is_historical_jit else ""
        desc_prefix = "[JIT] " if is_historical_jit else ""

        if content_type == "text":
            if is_historical_jit:
                prompt = self._generate_historical_summary_prompt(company_name)
            else:
                prompt = self._generate_structured_prompt(
                    company_name, previous_summary
                )

            logger.info(
                f"🧠 {log_prefix}Sending text for '{company_name}' to Gemini for structured analysis..."
            )

            def _call():
                return self.model.generate_content([prompt, content_data["content"]])

            summary_json = _gemini_call_with_retry(
                _call, desc=f"{desc_prefix}text summary for {company_name}"
            )

            if summary_json is None:
                return self._create_error_json(
                    "gemini_text_error",
                    "Gemini text summarisation failed after retries",
                    company_name,
                    original_pdf_url,
                )

            logger.info(
                f"✅ {log_prefix}Successfully generated structured summary for '{company_name}'."
            )
            summary_json["links"] = []
            summary_json["original_pdf_url"] = original_pdf_url
            return summary_json

        elif content_type == "link":
            links = content_data.get("links", [])
            web_links = [link for link in links if link.get("link_type") == "web"]
            media_links = [link for link in links if link.get("link_type") == "media"]

            if media_links:
                logger.info(
                    f"{log_prefix}Detected media link for '{company_name}'. Initiating media summarization."
                )
                media_url = media_links[0]["url"]
                return await self._summarize_media_from_url(
                    media_url,
                    company_name,
                    original_pdf_url,
                    previous_summary,
                    is_historical_jit,
                    desc_prefix,
                )

            elif web_links:
                logger.info(
                    f"🔗 PDF for '{company_name}' contained external web links. Recording link(s)."
                )
                return {
                    "company_name": company_name,
                    "type": "web_link",
                    "links": web_links,
                    "original_pdf_url": original_pdf_url,
                }

            logger.warning(
                f"⚠️ PDF for '{company_name}' has no actionable media/web links."
            )
            return self._create_error_json(
                "no_actionable_content",
                "Small PDF with no actionable links",
                company_name,
                original_pdf_url,
            )

        else:
            error_message = content_data.get("message", "Unknown processing error")
            logger.error(
                f"❗️ Cannot summarize due to processing error for '{company_name}': {error_message}"
            )
            return self._create_error_json(
                "processing_error",
                error_message,
                company_name,
                original_pdf_url,
            )

    def _create_error_json(
        self, error_type: str, message: str, company_name: str, pdf_url: str
    ) -> dict:
        """A standardized helper to create error objects."""
        return {
            "company_name": company_name,
            "type": "error",
            "error_type": error_type,
            "message": message,
            "original_pdf_url": pdf_url,
        }
