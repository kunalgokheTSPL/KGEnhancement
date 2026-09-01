"""Template-driven document extractor."""

from __future__ import annotations

import json
import logging
import os
import random
import re
import hashlib
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError

from p0.utils import fs
from p0.utils.doc_fields import resolve_field_descriptions
from p0.utils.progress import emit as _emit_progress, emit_file as _emit_file_outcome
from p0.utils.tabular import read_tables

_COPY_CHUNK = 1024 * 1024


def _iter_files_streaming(
    folder: str,
    allowed_exts: set[str],
    log: logging.Logger,
) -> Iterator[tuple[str, str, bool]]:
    """Yield (filename, path, is_remote) for each supported file under *folder*."""
    if fs.is_s3(folder):
        sfs = fs.get_fs()
        prefix = fs.strip_scheme(folder)
        try:
            remote_paths = sorted(sfs.ls(prefix, detail=False))
        except Exception as exc:
            log.warning("[docs] could not list RustFS prefix %s: %s", folder, exc)
            return
        for remote in remote_paths:
            fname = remote.split("/")[-1]
            if not fname:
                continue
            if os.path.splitext(fname)[1].lower() not in allowed_exts:
                continue
            yield fname, remote, True
        return

    if not os.path.isdir(folder):
        return
    for fname in sorted(os.listdir(folder)):
        fpath = os.path.join(folder, fname)
        if not os.path.isfile(fpath):
            continue
        if os.path.splitext(fname)[1].lower() not in allowed_exts:
            continue
        yield fname, fpath, False


@contextmanager
def _readable_copy(path: str, is_remote: bool, fname: str) -> Iterator[str]:
    """Yield a local path to read, downloading and removing a temp copy when remote."""
    if not is_remote:
        yield path
        return
    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix="docs_", suffix=os.path.splitext(fname)[1].lower()
    )
    os.close(tmp_fd)
    try:
        with fs.get_fs().open(path, "rb") as rfh, open(tmp_path, "wb") as wfh:
            shutil.copyfileobj(rfh, wfh, _COPY_CHUNK)
        if os.path.getsize(tmp_path) == 0:
            raise RuntimeError(f"downloaded copy of {fname} is empty")
        yield tmp_path
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


logger = logging.getLogger(__name__)


_DEFAULT_MODEL = "us.anthropic.claude-sonnet-5"
_DEFAULT_REGION = "us-east-1"

_EXTRACT_TOOL = "record_entries"

SUPPORTED_EXTS = {".pdf", ".docx", ".xlsx", ".xlsb", ".xls", ".csv"}

TABULAR_EXTS = {".xlsx", ".xlsb", ".xls", ".csv"}

_MAX_TOKENS = max(1024, int(os.environ.get("DOC_EXTRACT_MAX_TOKENS", "8192")))
_CHUNK_CHARS = max(2000, int(os.environ.get("DOC_EXTRACT_CHUNK_CHARS", "12000")))
_CHUNK_OVERLAP = max(0, int(os.environ.get("DOC_EXTRACT_CHUNK_OVERLAP", "800")))

_MAX_WORKERS = max(1, int(os.environ.get("DOC_EXTRACT_WORKERS", "8")))
_LLM_CONCURRENCY = max(1, int(os.environ.get("DOC_EXTRACT_LLM_CONCURRENCY", "8")))
_LLM_RETRIES = max(0, int(os.environ.get("DOC_EXTRACT_LLM_RETRIES", "4")))
_RETRY_BASE_SECONDS = float(os.environ.get("DOC_EXTRACT_RETRY_BASE_SECONDS", "1"))
_RETRY_CAP_SECONDS = float(os.environ.get("DOC_EXTRACT_RETRY_CAP_SECONDS", "30"))

_BOTOCORE_CFG = Config(
    read_timeout=300,
    connect_timeout=300,
    retries={"max_attempts": 5, "mode": "adaptive"},
    max_pool_connections=max(10, _LLM_CONCURRENCY * 2),
)

_FATAL_BEDROCK_CODES = (
    "ValidationException",
    "AccessDeniedException",
    "UnrecognizedClientException",
)

_THROTTLE_CODES = (
    "ThrottlingException",
    "TooManyRequestsException",
    "ServiceUnavailableException",
    "ModelTimeoutException",
    "InternalServerException",
)


def _bedrock_region(model_id: str) -> str:
    """Region to call, taken from the model id when it is a region-bearing ARN."""
    if model_id.startswith("arn:aws:bedrock:"):
        parts = model_id.split(":")
        if len(parts) >= 4 and parts[3]:
            return parts[3]
    return os.environ.get("BEDROCK_REGION", _DEFAULT_REGION)


def _bedrock_client(model_id: str = _DEFAULT_MODEL) -> Any:
    """Return a boto3 bedrock-runtime client using env credentials."""
    return boto3.client(
        "bedrock-runtime",
        region_name=_bedrock_region(model_id),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        config=_BOTOCORE_CFG,
    )




def _extract_text_from_docx(file_path: str) -> str:
    """Extract ALL text from DOCX preserving table structure (same as other extractors)."""
    try:
        from docx import Document

        doc = Document(file_path)
        content: list[str] = []
        for element in doc.element.body:
            if element.tag.endswith("}p"):
                texts = [
                    n.text for n in element.iter() if n.text and n.tag.endswith("}t")
                ]
                para = "".join(texts)
                if para.strip():
                    content.append(para)
            elif element.tag.endswith("}tbl"):
                for tbl in doc.tables:
                    if tbl._element is element:
                        for row in tbl.rows:
                            cells = [c.text.strip() for c in row.cells]
                            content.append(" | ".join(cells))
                        content.append("")
                        break
        text = "\n".join(content)
        logger.info(
            "  Extracted %d chars from DOCX: %s", len(text), os.path.basename(file_path)
        )
        return text
    except Exception as exc:
        logger.error(
            "  DOCX text extraction failed (%s): %s", os.path.basename(file_path), exc
        )
        return ""


def _extract_pdf_pages(file_path: str) -> list[str]:
    """Return per-page text strings from a PDF."""
    filename = os.path.basename(file_path)
    try:
        import fitz

        doc = fitz.open(file_path)
        pages = [doc[i].get_text() for i in range(len(doc))]
        logger.info("  PDF has %d pages: %s", len(pages), filename)

        if any(p.strip() for p in pages):
            return pages

        logger.info(
            "  PDF appears to be scanned (0 text chars) — attempting OCR for %s",
            filename,
        )
        return _ocr_pdf_pages(doc, filename)

    except Exception as exc:
        logger.error("  PDF text extraction failed (%s): %s", filename, exc)
        return []


def _ocr_pdf_pages(doc: "fitz.Document", filename: str) -> list[str]:
    """Run pytesseract OCR on each page image of a PyMuPDF document."""
    try:
        import fitz
        import pytesseract
        from PIL import Image
        import io
    except ImportError:
        logger.warning(
            "  pytesseract/Pillow not installed — OCR skipped for %s", filename
        )
        return []

    pages: list[str] = []
    total = len(doc)
    for i in range(total):
        try:
            mat = doc[i].get_pixmap(matrix=fitz.Matrix(200 / 72, 200 / 72))
            img = Image.open(io.BytesIO(mat.tobytes("png")))
            text = pytesseract.image_to_string(img, lang="eng")
            pages.append(text)
        except Exception as exc:
            logger.warning("  OCR failed on page %d of %s: %s", i + 1, filename, exc)
            pages.append("")

    chars = sum(len(p) for p in pages)
    logger.info(
        "  OCR complete for %s: %d pages, %d total chars", filename, total, chars
    )
    return pages


@dataclass
class _Chunk:
    """One unit of text sent to the model, with the pages it came from."""

    index: int
    text: str
    page_start: int
    page_end: int

    def pages_label(self) -> str:
        """Human-readable page range, empty when the source has no pagination."""
        if self.page_start <= 0:
            return ""
        if self.page_start == self.page_end:
            return str(self.page_start)
        return f"{self.page_start}-{self.page_end}"


def _split_oversized(text: str, limit: int, overlap: int) -> list[str]:
    """Cut text that cannot fit in one chunk, overlapping so nothing falls in a seam."""
    if len(text) <= limit:
        return [text]
    step = max(1, limit - overlap)
    return [text[i : i + limit] for i in range(0, len(text), step)]


def _chunk_pages(
    pages: list[str], limit: int = _CHUNK_CHARS, overlap: int = _CHUNK_OVERLAP
) -> list[_Chunk]:
    """Pack pages into chunks under the char limit, keeping page provenance."""
    chunks: list[_Chunk] = []
    buf: list[str] = []
    buf_len = 0
    first_page = 1

    def flush(last_page: int) -> None:
        nonlocal buf, buf_len
        body = "\n".join(buf).strip()
        if body:
            chunks.append(_Chunk(len(chunks), body, first_page, last_page))
        buf, buf_len = [], 0

    for page_no, page in enumerate(pages, start=1):
        page = (page or "").strip()
        if not page:
            continue
        if len(page) > limit:
            flush(page_no - 1 if buf else page_no)
            for part in _split_oversized(page, limit, overlap):
                chunks.append(_Chunk(len(chunks), part, page_no, page_no))
            first_page = page_no + 1
            continue
        if buf and buf_len + len(page) > limit:
            flush(page_no - 1)
            first_page = page_no
        if not buf:
            first_page = page_no
        buf.append(page)
        buf_len += len(page) + 1

    if buf:
        flush(len(pages))
    return chunks


def _chunk_plain_text(
    text: str, limit: int = _CHUNK_CHARS, overlap: int = _CHUNK_OVERLAP
) -> list[_Chunk]:
    """Chunk unpaginated text, overlapping so a value split across a seam survives."""
    parts = _split_oversized(text.strip(), limit, overlap)
    return [_Chunk(i, p, 0, 0) for i, p in enumerate(parts) if p.strip()]




class _ExtractionFailed(RuntimeError):
    """The model returned something this extractor cannot read as entries."""


def _field_description(col: str, field_descs: dict[str, str] | None) -> str:
    """What to tell the model one field means, falling back to the field name."""
    desc = (field_descs or {}).get(col, "").strip()
    if desc:
        return f'{col} — {desc}. Verbatim as written, or "" when the excerpt does not state it.'
    return f'Verbatim {col} as written, or "" when the excerpt does not state it.'


def _extract_tool(
    src_cols: list[str], field_descs: dict[str, str] | None = None
) -> dict[str, Any]:
    """The single tool the model must call, one string property per requested field."""
    return {
        "name": _EXTRACT_TOOL,
        "description": "Record every occurrence stated in the excerpt.",
        "input_schema": {
            "type": "object",
            "properties": {
                "entries": {
                    "type": "array",
                    "description": "One object per distinct occurrence found.",
                    "items": {
                        "type": "object",
                        "properties": {
                            c: {
                                "type": "string",
                                "description": _field_description(c, field_descs),
                            }
                            for c in src_cols
                        },
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["entries"],
        },
    }


def _build_prompt(
    src_cols: list[str], doc_type: str, field_descs: dict[str, str] | None = None
) -> str:
    """Build the extraction prompt for the given source_columns."""
    fields_list = "\n".join(
        f"  - {c} — {(field_descs or {}).get(c, '').strip()}"
        if (field_descs or {}).get(c, "").strip()
        else f"  - {c}"
        for c in src_cols
    )
    return f"""You are reading one excerpt of a {doc_type} document from an industrial plant.

Every record you produce is joined to plant equipment and to records from other documents and systems. A record whose subject cannot be identified is discarded, and an invented value corrupts everything joined to it.

Record these fields for each occurrence:
{fields_list}

What makes a record usable:
- One subject per entry. An entry naming two pieces of equipment, or two separate facts about one, joins to neither.
- When a heading or column covers several units, record one entry per unit and repeat the values they share.
- Identify the subject by its tag exactly as printed. Never prepend the equipment noun, expand an abbreviation, pad or strip digits, or keep a combined range: "P-101" stays "P-101", never "Pump P-101", never "P-101A/B".
- Carry down the context the excerpt establishes. A section heading, table caption, title block or column header names the subject of the rows beneath it; apply it to each of them.
- Copy values verbatim. Tags, numbers, dates, units and quantities keep the exact characters used.
- Never infer, expand, normalise or complete a value the excerpt does not state outright.
- Leave a field "" when the excerpt does not state it. An empty field is correct and expected; a guessed one is not.
- Boilerplate is not an occurrence: page numbers, revision blocks, distribution lists and legal notices are skipped.
- This excerpt is part of a longer document. Record only what it states, and record nothing when it states nothing relevant.

Call {_EXTRACT_TOOL} exactly once with everything you found.

Excerpt:
{{text}}"""


def _parse_text_entries(content: str) -> list[dict[str, str]] | None:
    """Read entries out of a plain-text reply, for a model that ignored the tool."""
    cleaned = re.sub(r"^```[a-z]*\n?", "", content.strip()).rstrip("`").strip()
    if not cleaned:
        return None
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        entries = parsed.get("entries")
        return entries if isinstance(entries, list) else [parsed]
    return parsed if isinstance(parsed, list) else None


def _call_llm(
    client: Any, text: str, prompt_template: str, model_id: str, tool: dict[str, Any]
) -> list[dict[str, str]]:
    """Call Bedrock and return the entries the model recorded for this chunk."""
    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": _MAX_TOKENS,
        "tools": [tool],
        "tool_choice": {"type": "tool", "name": _EXTRACT_TOOL},
        "messages": [
            {"role": "user", "content": prompt_template.replace("{text}", text)}
        ],
    }

    response = client.invoke_model(
        modelId=model_id,
        body=json.dumps(request_body),
        contentType="application/json",
        accept="application/json",
    )
    payload = json.loads(response["body"].read())

    if payload.get("stop_reason") == "max_tokens":
        raise _ExtractionFailed(
            f"reply was cut off at the {_MAX_TOKENS}-token limit, so entries are incomplete"
        )

    blocks = payload.get("content") or []
    for block in blocks:
        if block.get("type") == "tool_use" and block.get("name") == _EXTRACT_TOOL:
            entries = (block.get("input") or {}).get("entries")
            if not isinstance(entries, list):
                raise _ExtractionFailed("tool call carried no entries list")
            return [e for e in entries if isinstance(e, dict)]

    joined = "\n".join(b.get("text", "") for b in blocks if b.get("type") == "text")
    entries = _parse_text_entries(joined)
    if entries is None:
        raise _ExtractionFailed("reply was neither a tool call nor readable JSON")
    return [e for e in entries if isinstance(e, dict)]


def _is_throttle(exc: ClientError) -> bool:
    """Whether Bedrock asked us to slow down rather than refusing the call."""
    return exc.response.get("Error", {}).get("Code", "") in _THROTTLE_CODES


def _retry_delay(attempt: int) -> float:
    """Exponential backoff with full jitter, so retries do not resynchronise."""
    ceiling = min(_RETRY_CAP_SECONDS, _RETRY_BASE_SECONDS * (2**attempt))
    return random.uniform(0.0, max(0.0, ceiling))


def _call_llm_with_retry(
    client: Any,
    text: str,
    prompt_template: str,
    model_id: str,
    tool: dict[str, Any],
    log: logging.Logger,
    label: str,
) -> list[dict[str, str]]:
    """Call the model, backing off past botocore's own retries while throttled."""
    for attempt in range(_LLM_RETRIES + 1):
        try:
            return _call_llm(client, text, prompt_template, model_id, tool)
        except ClientError as exc:
            if not _is_throttle(exc) or attempt == _LLM_RETRIES:
                raise
            delay = _retry_delay(attempt)
            log.warning(
                "  throttled on %s, retrying in %.1fs (attempt %d of %d)",
                label,
                delay,
                attempt + 1,
                _LLM_RETRIES,
            )
            time.sleep(delay)
    raise _ExtractionFailed(f"retries exhausted for {label}")




def _make_doc_id(filename: str, doc_type: str) -> str:
    """Stable per-FILE document id. Every row extracted from the same"""
    raw = f"{filename}|{doc_type}"
    h = hashlib.sha1(raw.encode()).hexdigest()[:12]
    prefix = os.path.splitext(filename)[0][:20].replace(" ", "_")
    return f"DOC-{prefix}-{h}".upper()


def _make_record_id(filename: str, doc_type: str, row_idx: int | str) -> str:
    """Per-ROW record id. Unique per extracted row within a document, so the"""
    raw = f"{filename}|{doc_type}|row{row_idx}"
    h = hashlib.sha1(raw.encode()).hexdigest()[:12]
    prefix = os.path.splitext(filename)[0][:16].replace(" ", "_")
    return f"REC-{prefix}-{h}".upper()


def _fields_fingerprint(src_cols: list[str]) -> str:
    """Short, stable fingerprint of the extraction field SET."""
    norm = sorted(str(c).strip().lower() for c in (src_cols or []) if str(c).strip())
    raw = "|".join(norm)
    return hashlib.sha1(raw.encode()).hexdigest()[:8]


def _ledger_key(safe_key: str, src_cols: list[str]) -> str:
    """Dedup-ledger key = subtype + field-set fingerprint."""
    return f"{safe_key}@{_fields_fingerprint(src_cols)}"


def _entities_to_pipe(entities: dict[str, str]) -> str:
    """Convert {field: value, ...} to 'Field: Value | Field: Value' pipe-separated string."""
    parts = [
        f"{k}: {v}"
        for k, v in entities.items()
        if v and str(v).strip().lower() not in ("", "nan", "none")
    ]
    return " | ".join(parts)


def _build_file_reference(type_dir: str, filename: str) -> str:
    """Return a stable pointer back to the source file in object storage."""
    if not filename:
        return ""
    if fs.is_s3(type_dir):
        return f"{type_dir.rstrip('/')}/{filename}"
    return f"file://{os.path.abspath(os.path.join(type_dir, filename))}"


def _extract_from_tabular(
    file_path: str,
    src_cols: list[str],
    doc_type: str,
    filename: str,
    log: logging.Logger,
    file_reference: str = "",
    doc_type_key: str = "",
) -> list[dict[str, Any]]:
    """Handle Excel/CSV: read every sheet row-by-row and pull declared source_columns."""
    try:
        tables = read_tables(file_path, src_cols)
    except Exception as exc:
        log.error("  Could not read tabular file %s: %s", filename, exc)
        return []

    if not tables:
        log.warning("  No sheet carrying data in %s", filename)
        return []

    rows: list[dict[str, Any]] = []
    for ordinal, (df_file, sheet, header_row) in enumerate(tables):
        if sheet or header_row:
            log.info(
                "  %s: reading sheet %r from row %d", filename, sheet, header_row + 1
            )

        for idx, row in df_file.iterrows():
            entities: dict[str, str] = {}
            for col in src_cols:
                val = str(row.get(col, "")).strip() if col in df_file.columns else ""
                entities[col] = val

            equip_tag = ""
            for eq_col in (
                "Equipment_Tag",
                "Equipment Tag",
                "Equipment Name",
                "Tag No.",
                "Tag Name",
                "Tag",
                "Equipment ID",
                "equipment_no",
            ):
                val = (
                    str(row.get(eq_col, "")).strip()
                    if eq_col in df_file.columns
                    else ""
                )
                if val and val.lower() not in ("nan", "none"):
                    equip_tag = val
                    break

            rows.append(
                {
                    "document_id": _make_doc_id(filename, doc_type),
                    "record_id": _make_record_id(
                        filename, doc_type, f"{sheet}!{idx}" if sheet else idx
                    ),
                    "title": os.path.splitext(filename)[0],
                    "document_type": doc_type,
                    "document_type_key": doc_type_key
                    or doc_type,
                    "equipment_tag": equip_tag,
                    "equipment_label": equip_tag,
                    "equipment_id": "",
                    "description": doc_type,
                    "source_file": filename,
                    "file_reference": file_reference,
                    "extracted_entities": _entities_to_pipe(entities),
                    "chunk_index": ordinal,
                    "page_start": 0,
                    "page_end": 0,
                    "source_pages": sheet[:40],
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
            )
    log.info(
        "  Tabular extract: %d rows from %d sheet(s) in %s",
        len(rows),
        len(tables),
        filename,
    )
    return rows


@dataclass
class _Resume:
    """What a previous run left unfinished for one file."""

    chunks: int
    failed: list[int]


@dataclass
class _DocResult:
    """One file's rows, plus the chunk state a later run needs to resume it."""

    rows: list[dict[str, Any]]
    chunk_count: int = 0
    failed: list[int] = field(default_factory=list)


def _extract_from_document(
    file_path: str,
    src_cols: list[str],
    doc_type: str,
    filename: str,
    client: Any,
    model_id: str,
    log: logging.Logger,
    file_reference: str = "",
    doc_type_key: str = "",
    llm_pool: ThreadPoolExecutor | None = None,
    resume: _Resume | None = None,
    field_descs: dict[str, str] | None = None,
) -> _DocResult:
    """Handle PDF/DOCX: chunk text → LLM per chunk → merge → one row per chunk entry."""
    ext = os.path.splitext(file_path)[1].lower()
    prompt_template = _build_prompt(src_cols, doc_type, field_descs)
    tool = _extract_tool(src_cols, field_descs)

    if ext == ".pdf":
        pages = _extract_pdf_pages(file_path)
        if not pages:
            log.warning("  No text extracted from PDF: %s", filename)
            return _DocResult([])
        chunks = _chunk_pages(pages)
        log.info(
            "  PDF: %d pages → %d chunks for %s", len(pages), len(chunks), filename
        )
    elif ext == ".docx":
        text = _extract_text_from_docx(file_path)
        if not text:
            log.warning("  No text extracted from DOCX: %s", filename)
            return _DocResult([])
        chunks = _chunk_plain_text(text)
    else:
        log.warning("  Unsupported format for LLM extraction: %s", filename)
        return _DocResult([])

    selected = chunks
    if resume is not None:
        if resume.chunks == len(chunks):
            lost = set(resume.failed)
            selected = [c for c in chunks if c.index in lost]
            log.info(
                "  Resuming %s: re-running %d of %d chunk(s)",
                filename,
                len(selected),
                len(chunks),
            )
        else:
            log.warning(
                "  %s now splits into %d chunk(s) but the ledger recorded %d — re-extracting the whole file",
                filename,
                len(chunks),
                resume.chunks,
            )

    def run_chunk(chunk: _Chunk) -> list[dict[str, str]]:
        """Extract one chunk, announcing it as the call goes out."""
        log.info(
            "  LLM extracting chunk %d/%d from %s",
            chunk.index + 1,
            len(chunks),
            filename,
            extra={"progress": True},
        )
        return _call_llm_with_retry(
            client,
            chunk.text,
            prompt_template,
            model_id,
            tool,
            log,
            f"chunk {chunk.index + 1} of {filename}",
        )

    pending: list[Any] = []
    if llm_pool is not None and len(selected) > 1:
        pending = [llm_pool.submit(run_chunk, c) for c in selected]

    chunk_results: list[tuple[_Chunk, list[dict[str, str]]]] = []
    failed_chunks: list[int] = []
    for position, chunk in enumerate(selected):
        try:
            results = pending[position].result() if pending else run_chunk(chunk)
            kept = []
            for res in results:
                filtered = {c: str(res.get(c, "")).strip() for c in src_cols}
                if any(filtered.values()):
                    kept.append(filtered)
            if kept:
                chunk_results.append((chunk, kept))
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in _FATAL_BEDROCK_CODES:
                for fut in pending[position + 1 :]:
                    fut.cancel()
                log.error(
                    "  Fatal AWS Bedrock Error (%s). Aborting file %s: %s",
                    code,
                    filename,
                    exc,
                )
                raise
            failed_chunks.append(chunk.index)
            log.error(
                "  AWS API Error in chunk %d for %s: %s", chunk.index + 1, filename, exc
            )
        except Exception as exc:
            failed_chunks.append(chunk.index)
            log.error(
                "  LLM chunk %d failed for %s: %s", chunk.index + 1, filename, exc
            )

    if failed_chunks:
        log.error(
            "  %s: %d of %d chunk(s) lost - %s",
            filename,
            len(failed_chunks),
            len(chunks),
            ", ".join(str(i + 1) for i in failed_chunks[:20]),
        )

    if not chunk_results:
        log.error("  All LLM chunks failed or returned empty for %s", filename)
        return _DocResult([], len(chunks), sorted(failed_chunks))

    rows = []
    for chunk, merged_entries in chunk_results:
        for position, merged in enumerate(merged_entries):
            equip_tag = ""
            for eq_col in (
                "Equipment_Tag",
                "Equipment Tag",
                "Equipment Name",
                "Tag No.",
                "Tag Name",
                "Tag",
                "Equipment ID",
                "equipment_no",
            ):
                val = merged.get(eq_col, "").strip()
                if val and val.lower() not in ("nan", "none"):
                    equip_tag = val
                    break

            rows.append(
                {
                    "document_id": _make_doc_id(filename, doc_type),
                    "record_id": _make_record_id(
                        filename, doc_type, f"{chunk.index}.{position}"
                    ),
                    "title": os.path.splitext(filename)[0],
                    "document_type": doc_type,
                    "document_type_key": doc_type_key
                    or doc_type,
                    "equipment_tag": equip_tag,
                    "equipment_label": equip_tag,
                    "equipment_id": "",
                    "description": doc_type,
                    "source_file": filename,
                    "file_reference": file_reference,
                    "extracted_entities": _entities_to_pipe(merged),
                    "chunk_index": chunk.index,
                    "page_start": chunk.page_start,
                    "page_end": chunk.page_end,
                    "source_pages": chunk.pages_label(),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
            )
    return _DocResult(rows, len(chunks), sorted(failed_chunks))




class _ExtractionAborted(Exception):
    """Raised for tasks skipped after a fatal credential or model error."""


@dataclass
class _DocTask:
    """One document's extraction, planned sequentially and run independently."""

    order: int
    type_key: str
    doc_label: str
    ledger_key: str
    src_cols: list[str]
    fname: str
    fpath: str
    file_ref: str
    is_tabular: bool
    is_remote: bool = False
    restored: list[dict[str, Any]] = field(default_factory=list)
    resume: _Resume | None = None
    field_descs: dict[str, str] = field(default_factory=dict)


def _run_doc_task(
    task: _DocTask,
    get_client,
    model_id: str,
    log: logging.Logger,
    abort: threading.Event,
    llm_pool: ThreadPoolExecutor | None = None,
) -> _DocResult:
    """Extract one document; the unit of parallelism, sharing no mutable state."""
    if abort.is_set():
        raise _ExtractionAborted(task.fname)
    with _readable_copy(task.fpath, task.is_remote, task.fname) as local_path:
        if task.is_tabular:
            return _DocResult(
                _extract_from_tabular(
                    local_path,
                    task.src_cols,
                    task.doc_label,
                    task.fname,
                    log,
                    file_reference=task.file_ref,
                    doc_type_key=task.type_key,
                ),
                1,
            )
        return _extract_from_document(
            local_path,
            task.src_cols,
            task.doc_label,
            task.fname,
            get_client(),
            model_id,
            log,
            file_reference=task.file_ref,
            doc_type_key=task.type_key,
            llm_pool=llm_pool,
            resume=task.resume,
            field_descs=task.field_descs,
        )


def _ledger_entry(chunk_count: int, failed: list[int]) -> dict[str, Any]:
    """One file's ledger record: how many chunks it had and which are still missing."""
    return {"chunks": int(chunk_count or 0), "failed": sorted({int(i) for i in failed})}


def _row_chunk_index(row: dict[str, Any]) -> int:
    """Chunk a stored row came from; rows written before chunking carry none."""
    try:
        return int(row.get("chunk_index") or 0)
    except (TypeError, ValueError):
        return 0


def _load_done_ledger(path: str | None) -> dict[str, dict[str, dict[str, Any]]]:
    """Return {ledger_key: {filename: chunk state}}, reading the older list format too."""
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return {}
        ledger: dict[str, dict[str, dict[str, Any]]] = {}
        for key, value in data.items():
            if isinstance(value, list):
                ledger[key] = {str(f): _ledger_entry(0, []) for f in value}
            elif isinstance(value, dict):
                ledger[key] = {
                    str(f): _ledger_entry(
                        state.get("chunks") or 0, list(state.get("failed") or [])
                    )
                    for f, state in value.items()
                    if isinstance(state, dict)
                }
        return ledger
    except Exception:
        return {}


def _save_done_ledger(
    path: str | None, ledger: dict[str, dict[str, dict[str, Any]]]
) -> None:
    """Persist the ledger. Best-effort — don't propagate errors."""
    if not path:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        clean = {
            key: {name: files[name] for name in sorted(files)}
            for key, files in ledger.items()
            if files
        }
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(clean, fh, indent=2)
    except Exception as exc:
        logger.warning("Could not persist docs dedup ledger: %s", exc)


def process_user_documents(
    staging_dir: str,
    doc_types_cfg: dict[str, dict],
    log: logging.Logger | None = None,
    files_filter: set[str] | None = None,
    dedup_ledger_path: str | None = None,
    processed_out: str | None = None,
) -> pd.DataFrame:
    """Process all user-uploaded documents under staging_dir/{doc_type}/."""
    if log is None:
        log = logger

    model_id = os.environ.get("BEDROCK_MODEL_ID", _DEFAULT_MODEL)

    _client: Any = None
    _client_lock = threading.Lock()

    def _get_client() -> Any:
        nonlocal _client
        with _client_lock:
            if _client is None:
                _client = _bedrock_client(model_id)
                log.info(
                    "  Bedrock client initialised (region=%s, model=%s)",
                    _bedrock_region(model_id),
                    model_id,
                )
            return _client

    all_rows: list[dict[str, Any]] = []
    tasks: list[_DocTask] = []

    done_ledger = _load_done_ledger(dedup_ledger_path)
    if done_ledger and log:
        _total_done = sum(len(v) for v in done_ledger.values())
        log.info(
            "  Dedup ledger: %d previously processed file(s) across %d type(s)",
            _total_done,
            len(done_ledger),
        )

    is_s3_staging = fs.is_s3(staging_dir)

    for type_key, type_cfg in doc_types_cfg.items():
        safe_key = re.sub(r"[^a-z0-9_-]", "_", type_key.lower().strip())

        type_dir = (
            fs.path_join(staging_dir, safe_key)
            if is_s3_staging
            else os.path.join(staging_dir, safe_key)
        )
        if is_s3_staging:
            sfs = fs.get_fs()
            prefix = fs.strip_scheme(type_dir)
            try:
                _entries = sfs.ls(prefix, detail=False)
            except Exception:
                _entries = []
            if not _entries:
                alt = fs.path_join(staging_dir, type_key)
                try:
                    if sfs.ls(fs.strip_scheme(alt), detail=False):
                        type_dir = alt
                    else:
                        log.debug(
                            "  No RustFS folder for doc type '%s' (tried %s)",
                            type_key,
                            type_dir,
                        )
                        continue
                except Exception:
                    log.debug(
                        "  No RustFS folder for doc type '%s' (tried %s)",
                        type_key,
                        type_dir,
                    )
                    continue
        else:
            if not os.path.isdir(type_dir):
                type_dir = os.path.join(staging_dir, type_key)
                if not os.path.isdir(type_dir):
                    log.debug(
                        "  No staging dir for doc type '%s' (tried %s)",
                        type_key,
                        type_dir,
                    )
                    continue

        doc_label = type_cfg.get("document_type", type_key.upper())
        src_cols: list[str] = type_cfg.get("source_columns", [])

        if not src_cols:
            log.warning(
                "  No source_columns declared for doc type '%s' — skipping", type_key
            )
            continue

        field_descs = resolve_field_descriptions(type_key, type_cfg)
        described = [c for c in src_cols if field_descs.get(c)]

        log.info(
            "  [%s] streaming from: %s  source_columns=%s",
            doc_label,
            type_dir,
            src_cols,
        )
        if described:
            log.info(
                "  [%s] %d of %d field(s) carry a description into the prompt",
                doc_label,
                len(described),
                len(src_cols),
            )
        else:
            log.warning(
                "  [%s] no field descriptions resolved — the model sees bare field names",
                doc_label,
            )

        ledger_key = _ledger_key(safe_key, src_cols)
        ledger_files = done_ledger.get(ledger_key, {})
        if ledger_files:
            log.info(
                "  [%s] dedup: %d file(s) recorded for this field-set",
                doc_label,
                len(ledger_files),
            )
        else:
            log.info(
                "  [%s] no prior runs for this field-set (key=%s) — extracting fresh",
                doc_label,
                ledger_key,
            )

        _historical_by_file: dict[str, list[dict]] | None = None

        def _load_historical() -> dict[str, list[dict]]:
            nonlocal _historical_by_file
            if _historical_by_file is not None:
                return _historical_by_file
            out: dict[str, list[dict]] = {}
            if not processed_out:
                _historical_by_file = out
                return out
            try:
                pq_path = (
                    fs.path_join(processed_out, safe_key, "docs_documents.parquet")
                    if fs.is_s3(processed_out)
                    else os.path.join(processed_out, safe_key, "docs_documents.parquet")
                )
                if fs.exists(pq_path):
                    df_prev = fs.read_parquet(pq_path)
                    for row in df_prev.to_dict(orient="records"):
                        sf = str(row.get("source_file", "")).strip()
                        if sf:
                            out.setdefault(sf, []).append(row)
            except Exception as exc:
                log.warning(
                    "  [%s] could not load historical parquet (%s) — dedup-skipped files won't be restored",
                    doc_label,
                    exc,
                )
            _historical_by_file = out
            return out

        file_count = 0
        skipped_already = 0
        skipped_filter = 0
        restored_count = 0
        resumed_count = 0
        for fname, fpath, is_remote in _iter_files_streaming(
            type_dir, SUPPORTED_EXTS, log
        ):
            if files_filter is not None and fname not in files_filter:
                skipped_filter += 1
                continue
            restored: list[dict[str, Any]] = []
            resume: _Resume | None = None
            entry = ledger_files.get(fname)
            if entry is not None:
                historical = _load_historical().get(fname, [])
                lost = set(entry.get("failed") or [])
                if not historical:
                    log.info(
                        "  [%s] ledger hit but no historical rows for %s — re-extracting",
                        doc_label,
                        fname,
                    )
                elif not lost:
                    restored = historical
                    skipped_already += 1
                    restored_count += len(historical)
                    log.info(
                        "  [%s] dedup hit — restored %d historical row(s) for %s (no re-extraction)",
                        doc_label,
                        len(historical),
                        fname,
                    )
                else:
                    restored = [
                        r for r in historical if _row_chunk_index(r) not in lost
                    ]
                    resume = _Resume(int(entry.get("chunks") or 0), sorted(lost))
                    resumed_count += 1
                    restored_count += len(restored)
                    log.warning(
                        "  [%s] %s lost %d chunk(s) last run — kept %d row(s), re-running only the lost chunk(s)",
                        doc_label,
                        fname,
                        len(lost),
                        len(restored),
                    )

            if resume is not None or not restored:
                file_count += 1

            tasks.append(
                _DocTask(
                    order=len(tasks),
                    type_key=type_key,
                    doc_label=doc_label,
                    ledger_key=ledger_key,
                    src_cols=src_cols,
                    fname=fname,
                    fpath=fpath,
                    file_ref=_build_file_reference(type_dir, fname),
                    is_tabular=os.path.splitext(fname)[1].lower() in TABULAR_EXTS,
                    is_remote=is_remote,
                    restored=restored,
                    resume=resume,
                    field_descs=field_descs,
                )
            )

        log.info(
            "  [%s] planned %d new file(s)  skipped_done=%d  resumed=%d  skipped_filter=%d  restored_rows=%d",
            doc_label,
            file_count,
            skipped_already,
            resumed_count,
            skipped_filter,
            restored_count,
        )

    pending = [t for t in tasks if t.resume is not None or not t.restored]
    results: dict[int, list[dict[str, Any]]] = {
        t.order: t.restored for t in tasks if t.restored and t.resume is None
    }
    workers = min(_MAX_WORKERS, len(pending)) or 1
    log.info(
        "  extracting %d document(s) across %d type(s) on %d worker(s)",
        len(pending),
        len(doc_types_cfg),
        workers,
    )
    _emit_progress(
        "extract",
        label="Extracting documents",
        status="running",
        items_done=0,
        items_total=len(pending),
    )

    done_count = 0
    fatal: BaseException | None = None
    abort = threading.Event()

    if pending:
        with ThreadPoolExecutor(max_workers=_LLM_CONCURRENCY) as llm_pool, (
            ThreadPoolExecutor(max_workers=workers)
        ) as pool:
            futures = {
                pool.submit(
                    _run_doc_task, t, _get_client, model_id, log, abort, llm_pool
                ): t
                for t in pending
            }
            for fut in as_completed(futures):
                task = futures[fut]
                done_count += 1
                try:
                    result = fut.result()
                except _ExtractionAborted:
                    continue
                except Exception as exc:
                    code = ""
                    if isinstance(exc, ClientError):
                        code = exc.response.get("Error", {}).get("Code", "")
                    log.error(
                        "  [%s] extraction failed for %s: %s",
                        task.doc_label,
                        task.fname,
                        exc,
                    )
                    _emit_file_outcome(task.fname, "failed", error=str(exc))
                    if code in _FATAL_BEDROCK_CODES and fatal is None:
                        fatal = exc
                        abort.set()
                    continue

                rows = task.restored + result.rows
                rows.sort(key=_row_chunk_index)
                results[task.order] = rows
                if result.failed:
                    _emit_file_outcome(
                        task.fname,
                        "partial",
                        rows=len(rows),
                        error=f"{len(result.failed)} of {result.chunk_count} chunk(s) failed; a re-run retries only those",
                    )
                else:
                    _emit_file_outcome(task.fname, "processed", rows=len(rows))
                done_ledger.setdefault(task.ledger_key, {})[task.fname] = _ledger_entry(
                    result.chunk_count, result.failed
                )
                _save_done_ledger(dedup_ledger_path, done_ledger)
                _emit_progress(
                    "extract",
                    label="Extracting documents",
                    status="running",
                    items_done=done_count,
                    items_total=len(pending),
                    current_item=task.fname,
                )

    if fatal is not None:
        _save_done_ledger(dedup_ledger_path, done_ledger)
        raise fatal

    for task in tasks:
        all_rows.extend(results.get(task.order, []))

    _save_done_ledger(dedup_ledger_path, done_ledger)

    SCHEMA_COLS = [
        "document_id",
        "record_id",
        "title",
        "document_type",
        "document_type_key",
        "equipment_tag",
        "equipment_label",
        "equipment_id",
        "description",
        "source_file",
        "file_reference",
        "extracted_entities",
        "chunk_index",
        "page_start",
        "page_end",
        "source_pages",
        "ingested_at",
    ]
    if not all_rows:
        log.warning("  process_user_documents: no rows extracted from %s", staging_dir)
        return pd.DataFrame(columns=SCHEMA_COLS)

    df = pd.DataFrame(all_rows)
    for col in SCHEMA_COLS:
        if col not in df.columns:
            df[col] = ""
    df = df[SCHEMA_COLS]
    log.info(
        "  process_user_documents: %d total rows from %d doc type(s)",
        len(df),
        len(doc_types_cfg),
    )
    return df



if __name__ == "__main__":
    import argparse
    import yaml

    _project_root = Path(__file__).resolve().parents[3]
    _default_staging = str(
        _project_root / "p0" / "data" / "staging" / "documents"
    )
    _default_config = str(_project_root / "p0" / "config")

    parser = argparse.ArgumentParser(
        description="Generic user-defined document extractor (template-based LLM extraction)."
    )
    parser.add_argument(
        "--staging_dir",
        default=_default_staging,
        help="Root staging dir containing {doc_type}/ subdirectories",
    )
    parser.add_argument(
        "--config_dir",
        default=_default_config,
        help="Config directory containing user_config.yaml",
    )
    parser.add_argument(
        "--output_dir",
        default=_default_staging,
        help="Output directory for the extracted xlsx",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    )

    user_cfg_path = os.path.join(args.config_dir, "user_config.yaml")
    with open(user_cfg_path, encoding="utf-8") as f:
        user_cfg = yaml.safe_load(f) or {}
    doc_types_cfg = (user_cfg.get("document_processing") or {}).get(
        "document_types"
    ) or {}

    if not doc_types_cfg:
        print("No document_types defined in user_config.yaml — nothing to do.")
        raise SystemExit(0)

    df = process_user_documents(args.staging_dir, doc_types_cfg)
    if df.empty:
        print("No documents extracted.")
        raise SystemExit(0)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"user_documents_{timestamp}.xlsx"
    df.to_excel(output_path, index=False)
    print(f"\nExtracted {len(df)} rows → {output_path}")
