# core/processor.py
from PyPDF2 import PdfReader
import re
from pathlib import Path
import logging

logger = logging.getLogger()  


class PDFProcessor:
    """
    Handles the extraction of content from PDF files.
    Identifies if the PDF contains a full transcript or just external links.
    """

    def __init__(self):
        
        self.url_pattern = re.compile(
            r'(?:https?://|www\.|file://)[^\s<>"]+|https?://[^\s<>"]+'
        )
        self.media_pattern = re.compile(r"\.(mp3|mp4|wav|m4a|pdf)\b", re.IGNORECASE)

    def _stitch_broken_urls(self, text: str) -> str:
        """
        A simple pre-processor to fix URLs broken over newlines.
        """
        return text.replace("\n", "")

    def process_pdf(self, pdf_path: Path) -> dict:
        """
        Processes a PDF based on the critical business rule: SIZE FIRST.
        1. Small documents (<= 3 pages) are treated as potential LINK POINTERS.
        2. Large documents (> 3 pages) are treated as FULL TEXT TRANSCRIPTS, and any links within them are ignored.
        """
        try:
            reader = PdfReader(pdf_path)
            page_count = len(reader.pages)

            full_text = ""
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"

            char_count = len(full_text)

            # RULE 1: If it's a LARGE document, it IS the transcript. End of story. IGNORE any links inside.
            if page_count > 3:
                logger.info(
                    f"📄 Large PDF ({page_count} pages) detected. Processing as FULL TEXT. Any internal links will be ignored."
                )
                return {"type": "text", "content": full_text}

            # RULE 2: If it's a SMALL document, it MUST be a pointer. Now, we find the link.
            if char_count > 10:

                # Clean the text to handle links broken across multiple lines.
                text_for_url_finding = self._stitch_broken_urls(full_text)
                urls = self.url_pattern.findall(text_for_url_finding)

                if urls:
                    extracted_links = []
                    for url in urls:
                        cleaned_url = url.rstrip(".,;)")
                        link_type = (
                            "media" if self.media_pattern.search(cleaned_url) else "web"
                        )
                        extracted_links.append(
                            {"url": cleaned_url, "link_type": link_type}
                        )

                    logger.info(
                        f"🔗 Small PDF ({page_count} pages) is a LINK POINTER. Extracted {len(extracted_links)} URL(s)."
                    )
                    return {"type": "link", "links": extracted_links}

            # RULE 3: If it's small and has no links, or is just empty, it's useless.
            logger.warning(
                f"📄 Small PDF ({page_count} pages, {char_count} chars) has no actionable links or content. Skipping."
            )
            return {
                "type": "error",
                "message": "Small PDF with insufficient content or no links found",
            }

        except Exception as e:
            logger.error(
                f"❌ Failed to process PDF {pdf_path.name}: {e}", exc_info=True
            )
            return {"type": "error", "message": str(e)}
