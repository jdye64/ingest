from __future__ import annotations

from pathlib import Path

from bs4 import BeautifulSoup
from pypdf import PdfReader


class UnsupportedDocumentError(ValueError):
    pass


def count_pages(path: Path) -> int:
    """Return page count for throughput metrics (PDF page count, else 1)."""
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        try:
            return max(1, len(PdfReader(str(path)).pages))
        except Exception:
            return 1
    return 1


def parse_file(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".txt", ".md", ".markdown"}:
        return path.read_text(encoding="utf-8", errors="replace")
    if suffix in {".html", ".htm"}:
        raw = path.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(raw, "lxml")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        return "\n".join(line.strip() for line in text.splitlines() if line.strip())
    if suffix == ".pdf":
        reader = PdfReader(str(path))
        parts: list[str] = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            if page_text.strip():
                parts.append(page_text.strip())
        return "\n\n".join(parts)
    raise UnsupportedDocumentError(f"Unsupported file type: {suffix}")
