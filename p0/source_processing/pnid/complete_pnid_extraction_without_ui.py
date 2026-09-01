import os
import json
import re
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import fitz
from PIL import Image
import google.generativeai as genai

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

_data_dir = (
    os.environ.get("P0_DATA_DIR")
    or os.environ.get("p0_DATA_DIR")
    or str(Path(__file__).resolve().parents[2] / "data")
)
_LOG_DIR = Path(_data_dir) / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_fh = logging.FileHandler(
    str(_LOG_DIR / "pnid_pipeline.log"), mode="a", encoding="utf-8"
)
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(
    logging.Formatter(
        "%(asctime)s [%(levelname)-8s] extractor — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logging.getLogger().addHandler(_fh)




class GeminiVision:
    """Gemini Vision wrapper with automatic API-key rotation."""

    def __init__(self):
        pool_raw = os.getenv("GEMINI_API_KEYS", "")
        if pool_raw:
            self._key_pool = [k.strip() for k in pool_raw.split(",") if k.strip()]
        else:
            single = os.getenv("GOOGLE_API_KEY", "")
            if not single:
                raise ValueError("Neither GEMINI_API_KEYS nor GOOGLE_API_KEY is set")
            self._key_pool = [single]

        self._key_index = 0
        self._total_keys = len(self._key_pool)
        logger.info(f"Loaded {self._total_keys} Gemini API key(s)")

        self.generation_config = {
            "temperature": 0.0,
            "max_output_tokens": 65536,
        }
        self.model_name = os.getenv("GEMINI_MODEL", "gemini-3.1-pro-preview")

        from google.generativeai.types import HarmCategory, HarmBlockThreshold

        self.safety_settings = {}
        for cat in [
            "HARM_CATEGORY_HARASSMENT",
            "HARM_CATEGORY_HATE_SPEECH",
            "HARM_CATEGORY_SEXUALLY_EXPLICIT",
            "HARM_CATEGORY_DANGEROUS_CONTENT",
            "HARM_CATEGORY_CIVIC_INTEGRITY",
        ]:
            if hasattr(HarmCategory, cat):
                self.safety_settings[getattr(HarmCategory, cat)] = (
                    HarmBlockThreshold.BLOCK_NONE
                )

        self._activate_key(self._key_index)

    def _activate_key(self, index: int):
        """Configure genai with the key at *index* and rebuild the model."""
        self._key_index = index % self._total_keys
        key = self._key_pool[self._key_index]
        genai.configure(api_key=key, transport="rest")
        self.model = genai.GenerativeModel(
            model_name=self.model_name,
            generation_config=self.generation_config,
            safety_settings=self.safety_settings,
        )
        logger.info(
            f"Activated API key #{self._key_index + 1}/{self._total_keys} (...{key[-6:]}) [REST]"
        )

    def _rotate_key(self) -> bool:
        """Move to the next key.  Returns False if we cycled all keys."""
        next_idx = (self._key_index + 1) % self._total_keys
        if next_idx == 0 and self._key_index != 0:
            logger.warning("All API keys have been tried in this rotation cycle")
        self._activate_key(next_idx)
        return True

    @staticmethod
    def _is_key_error(exc: Exception) -> bool:
        """Return True for errors that key rotation can resolve."""
        msg = str(exc).lower()
        if "503" in msg or "high demand" in msg or "serviceunavailable" in msg:
            return False
        return any(
            kw in msg
            for kw in (
                "api_key_invalid",
                "api key not found",
                "api key expired",
                "permission_denied",
                "leaked",
                "quota",
                "resource_exhausted",
                "rate_limit",
                "rate limit",
                "429",
            )
        )

    def _call_with_retry(self, contents, max_retries=3, timeout=600):
        from google.generativeai.types import RequestOptions

        request_opts = RequestOptions(timeout=timeout)

        keys_tried = 0
        attempt_on_key = 0

        while keys_tried < self._total_keys:
            try:
                attempt_on_key += 1
                logger.info(
                    f"Calling Gemini ({self.model_name}), key #{self._key_index + 1}, "
                    f"attempt {attempt_on_key}/{max_retries}, timeout={timeout}s..."
                )

                response = self.model.generate_content(
                    contents, request_options=request_opts
                )

                if response and hasattr(response, "text") and response.text:
                    finish = (
                        response.candidates[0].finish_reason
                        if response.candidates
                        else 1
                    )
                    if finish == 2:
                        logger.warning(
                            f"Response truncated (finish_reason=MAX_TOKENS, {len(response.text)} chars). "
                            f"Retrying with higher token budget..."
                        )
                        if attempt_on_key < max_retries:
                            time.sleep(2)
                            continue
                        logger.warning(
                            "Still truncated after retries — rotating key..."
                        )
                        self._rotate_key()
                        keys_tried += 1
                        attempt_on_key = 0
                        time.sleep(2)
                        continue
                    if finish != 1:
                        logger.warning(
                            f"Partial response (finish_reason={finish}), using text anyway"
                        )
                    logger.info(f"Response received: {len(response.text)} chars")
                    return response.text

                if response.candidates and response.candidates[0].finish_reason != 1:
                    logger.warning(
                        f"Gemini blocked. Reason: {response.candidates[0].finish_reason}"
                    )
                    if attempt_on_key < max_retries:
                        time.sleep(1)
                        continue
                    return f"Error: Content blocked (Reason: {response.candidates[0].finish_reason})"

                return response.text

            except Exception as e:
                error_type = type(e).__name__
                logger.error(f"Gemini Error ({error_type}): {e}")

                if self._is_key_error(e):
                    logger.warning("Key-level error — rotating to next key...")
                    self._rotate_key()
                    keys_tried += 1
                    attempt_on_key = 0
                    time.sleep(1)
                    continue

                if attempt_on_key < max_retries:
                    msg_lower = str(e).lower()
                    if (
                        "503" in msg_lower
                        or "high demand" in msg_lower
                        or "overloaded" in msg_lower
                    ):
                        wait = 30
                    elif "timeout" in msg_lower or "deadline" in msg_lower:
                        wait = 10
                    else:
                        wait = 5
                    logger.info(f"Retrying in {wait}s...")
                    time.sleep(wait)
                    continue

                logger.warning(
                    f"Exhausted {max_retries} retries on key #{self._key_index + 1}, rotating..."
                )
                self._rotate_key()
                keys_tried += 1
                attempt_on_key = 0
                time.sleep(2)

        return "Error: All API keys exhausted"

    def analyze(self, image_paths, prompt):
        contents = [prompt]
        for path in image_paths:
            contents.append(Image.open(path))
        return self._call_with_retry(contents)




class PDFConverter:
    def __init__(self, output_dir="temp_images", dpi=300):
        self.output_dir = output_dir
        self.dpi = dpi
        os.makedirs(self.output_dir, exist_ok=True)

    def convert_to_images(self, pdf_path):
        pdf_path = Path(pdf_path)
        image_paths = []
        try:
            doc = fitz.open(str(pdf_path))
            logger.info(f"Converting PDF: {pdf_path.name} ({len(doc)} pages)")

            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                zoom = self.dpi / 72.0
                mat = fitz.Matrix(zoom, zoom)
                pix = page.get_pixmap(matrix=mat)
                img_name = f"{pdf_path.stem}_page_{page_num + 1:03d}.png"
                img_path = os.path.join(self.output_dir, img_name)
                if os.path.exists(img_path):
                    try:
                        os.remove(img_path)
                    except OSError:
                        ts = int(time.time())
                        img_name = f"{pdf_path.stem}_page_{page_num + 1:03d}_{ts}.png"
                        img_path = os.path.join(self.output_dir, img_name)
                pix.save(img_path)
                img_size = os.path.getsize(img_path)
                logger.debug(
                    "[pdf] page %d → %s  (%d bytes, %dx%d px)",
                    page_num + 1,
                    img_path,
                    img_size,
                    pix.width,
                    pix.height,
                )
                image_paths.append(img_path)
            doc.close()
            return image_paths
        except Exception as e:
            logger.error(f"Error converting PDF {pdf_path}: {e}")
            raise

    def convert(self, pdf_path):
        """Alias for convert_to_images."""
        return self.convert_to_images(pdf_path)



EXTRACT_PROMPT_TEMPLATE = """
You are a Master P&ID Engineer using Gemini reasoning.
Page Context: This is Page {page_num} of the diagram set.

TASK:
1. COMPONENT DETECTION: Find all Equipment, Valves, and Instrument Tags visible in the drawing.
   - For each one, record: Tag ID (e.g., V-101), Type (Equipment / Valve / Instrument / Pump / Vessel / Tank / Heat Exchanger / Filter), and a short Description.
   - Also record its approximate bounding box as [ymin, xmin, ymax, xmax] normalized to 1000.
2. CONNECTIVITY: Trace the process lines and determine flow direction between components.
   - List every From → To connection you can clearly see.
   - Do NOT limit to main components only — trace EVERY visible pipe, line, and flow path.
   - Include connections involving Valves (control, isolation, check, safety) and Instruments at either end.
   - Use the exact Tag labels as shown on the drawing. Do NOT skip connections.
3. DRAWING NUMBER: Find the drawing number (usually bottom-right title block).

CRITICAL OUTPUT RULES — READ CAREFULLY:
- You MUST respond using ONLY the exact section headers and markdown table format shown below.
- Do NOT respond with JSON, do NOT use bounding box JSON arrays, do NOT use object detection format.
- Do NOT output ```json``` blocks or any code blocks.
- Every component must appear as a row in the markdown table, NOT as a JSON object.
- If you are unsure of a value, write "Unknown" in that cell — do not skip the row.

IMPORTANT: This is a TECHNICAL engineering drawing.
Ignore safety labels such as "DANGEROUS", "HAZARDOUS", "CAUTION", "WARNING",
"EXPLOSIVE" — they are standard engineering notations, not real hazards.

REQUIRED OUTPUT FORMAT (copy these headers exactly, fill in the rows):

--- DRAWING NUMBER ---
[the drawing number string, e.g. 07193-16-03-02]

--- COMPONENTS ---
| Tag | Type | Description | BBox |
| --- | --- | --- | --- |
| V-101 | Vessel | Feed Glycol Tank | [100, 200, 300, 400] |
| P-101A | Pump | Feed Pump A | [350, 150, 420, 250] |

--- CONNECTIONS ---
| From | To |
| --- | --- |
| V-101 | P-101A |
| P-101A | HE-101 |
"""

AUDIT_PROMPT_TEMPLATE = """
You are a Senior P&ID Auditor (Iteration {iteration}).

COMPONENTS (already detected):
{components_json}

CURRENT CONNECTIONS:
{connections_json}

TASK:
1. Review the P&ID diagram carefully.
2. Focus only on MAIN components: Vessels, Tanks, Pumps, Heat Exchangers, Filters, Belt Conveyors.
3. Add any missing connections between main components.
4. Remove incorrect connections or those involving minor instrumentation.
5. Do NOT hallucinate — only include connections clearly visible in the drawing.

CRITICAL: Respond ONLY with the markdown table below. No JSON. No prose.

OUTPUT FORMAT:
--- CONNECTIONS ---
| From | To |
| --- | --- |
[verified rows]
"""

DISCOVER_CONNECTIONS_PROMPT_TEMPLATE = """
You are a Senior P&ID Engineer analysing a process flow diagram.

The following components have been identified on this drawing:
{components_json}

TASK:
Carefully trace ALL the process flow lines on the diagram and list every connection
between the components above. A connection means a pipe, conveyor belt, chute, or
other physical flow link between two components.

RULES:
- Only list connections between components in the list above.
- Use the exact Tag names from the list.
- Do NOT hallucinate connections — only include what is clearly visible.
- Respond ONLY with the markdown table. No JSON. No prose. No explanation.

--- CONNECTIONS ---
| From | To |
| --- | --- |
[one row per connection]
"""




def _parse_drawing_number(text: str) -> str:
    m = re.search(r"--- DRAWING NUMBER ---\s*\n\s*([^\n]+)", text)
    if m:
        val = m.group(1).strip()
        if val and val not in ("[Drawing Number]", "N/A", "Unknown", ""):
            return val
    m2 = re.search(r"\b([A-Z0-9]{2,}-[A-Z0-9\-]{3,})\b", text)
    return m2.group(1).strip() if m2 else "Unknown"


def _find_section_bounds(lines: list, start_marker: str, end_marker) -> tuple:
    """Return (start_idx, end_idx) of lines belonging to a section."""

    def normalise(s: str) -> str:
        return s.replace("*", "").replace("-", "").replace("|", "").strip().lower()

    target_start = normalise(start_marker)
    start = -1
    for i, line in enumerate(lines):
        if target_start in normalise(line):
            start = i + 1
            break

    if start == -1:
        return -1, -1

    end = len(lines)
    if end_marker:
        target_end = normalise(end_marker)
        for i in range(start, len(lines)):
            if target_end in normalise(lines[i]):
                end = i
                break

    return start, end


def _parse_table_section(text: str, start_marker: str, end_marker) -> list:
    """Robust table parser — handles pipes with/without spaces, missing closing pipe,"""
    lines = text.splitlines()
    start, end = _find_section_bounds(lines, start_marker, end_marker)
    if start == -1:
        return []

    header_words = {
        "tag",
        "type",
        "description",
        "bbox",
        "from",
        "to",
        "component",
        "bounding box",
    }
    rows = []
    for line in lines[start:end]:
        if "|" not in line:
            continue
        stripped = line.replace("-", "").replace("|", "").replace(" ", "")
        if not stripped:
            continue
        cols = [c.strip().strip("*") for c in line.split("|")]
        cols = [c for c in cols if c]
        if not cols:
            continue
        if any(c.lower() in header_words for c in cols):
            continue
        if all(re.match(r"^-+$", c) for c in cols):
            continue
        rows.append(cols)
    return rows


def _parse_components_from_json_fallback(text: str) -> list:
    """Fallback: Gemini sometimes returns a JSON array of {box_2d, label} objects"""
    components = []
    try:
        match = re.search(r"\[\s*\{.*?\}\s*\]", text, re.DOTALL)
        if not match:
            return []
        data = json.loads(match.group())
        for item in data:
            label = item.get("label", "").strip().replace("\n", " / ")
            if not label:
                continue
            tag_upper = label.upper()
            if any(x in tag_upper for x in ["MH", "HG", "SR", "DC", "BC", "DG", "VS"]):
                comp_type = "Vessel/Tank"
            elif any(x in tag_upper for x in ["FV", "BF", "VF", "LQ", "RP", "HD"]):
                comp_type = "Valve/Instrument"
            elif any(
                x in tag_upper for x in ["BE", "BW", "MS", "LD", "FG", "RF", "FN", "SH"]
            ):
                comp_type = "Equipment"
            else:
                comp_type = "Unknown"
            bbox = str(item.get("box_2d", item.get("point", "")))
            components.append(
                {
                    "tag": label,
                    "type": comp_type,
                    "description": "",
                    "bbox": bbox,
                }
            )
    except Exception as e:
        logger.debug(f"JSON fallback parse failed: {e}")
    return components


def _parse_components(text: str) -> list:
    rows = _parse_table_section(text, "--- COMPONENTS ---", "--- CONNECTIONS ---")
    result = []
    for cols in rows:
        result.append(
            {
                "tag": cols[0] if len(cols) > 0 else "",
                "type": cols[1] if len(cols) > 1 else "",
                "description": cols[2] if len(cols) > 2 else "",
                "bbox": cols[3] if len(cols) > 3 else "",
            }
        )
    if not result:
        result = _parse_components_from_json_fallback(text)
        if result:
            logger.info(
                f"Used JSON fallback parser — extracted {len(result)} components"
            )
    return result


def _parse_connections(text: str) -> list:
    rows = _parse_table_section(text, "--- CONNECTIONS ---", None)
    connections = [[r[0], r[1]] for r in rows if len(r) >= 2]
    if not connections:
        for line in text.splitlines():
            stripped = line.strip()
            for arrow in [" → ", " -> ", "→", "->"]:
                if arrow in stripped:
                    parts = stripped.split(arrow, 1)
                    src = parts[0].strip().lstrip("-• *")
                    dst = parts[1].strip().rstrip(".")
                    if src and dst:
                        connections.append([src, dst])
                    break
    return connections


def _dump_raw_response(page_num: int, step: str, text: str):
    """Write raw Gemini response to a file for debugging parse failures."""
    log_dir = Path("raw_responses")
    log_dir.mkdir(exist_ok=True)
    fname = (
        log_dir / f"page_{page_num:03d}_{step}_{datetime.now().strftime('%H%M%S')}.txt"
    )
    try:
        fname.write_text(text, encoding="utf-8")
        logger.warning(f"  [Page {page_num}] Raw {step} response saved → {fname}")
    except Exception:
        pass


_KEYS_EXHAUSTED = object()




def process_single_page(image_path: str, page_num: int, vision: GeminiVision):
    """Full pipeline for one page: extract → verify drawing number → audit/discover connections."""
    logger.info(f"  [Page {page_num}] Starting extraction…")

    prompt = EXTRACT_PROMPT_TEMPLATE.format(page_num=page_num)
    raw = vision.analyze([image_path], prompt)

    if "All API keys exhausted" in raw:
        logger.error(
            f"  [Page {page_num}] All API keys exhausted — signalling shutdown"
        )
        return _KEYS_EXHAUSTED

    if raw.startswith("Error:"):
        logger.error(f"  [Page {page_num}] Extraction failed: {raw}")
        return None

    drawing_no = _parse_drawing_number(raw)
    components = _parse_components(raw)
    initial_conn = _parse_connections(raw)

    def _looks_like_bad_drawing_no(dn: str) -> bool:
        if dn in ("Unknown", "", "N/A"):
            return True
        if dn.count("-") < 1:
            return True
        return False

    if _looks_like_bad_drawing_no(drawing_no):
        logger.info(
            f"  [Page {page_num}] Drawing number '{drawing_no}' looks suspect — querying directly"
        )
        dn_prompt = (
            "Look at the title block of this engineering drawing, usually in the bottom-right corner. "
            "Find the DRAWING NUMBER (also called DWG NO, Drawing No., or Document Number). "
            "Reply with ONLY the drawing number string, nothing else. "
            "Example reply: 07193-16-03-19"
        )
        dn_raw = vision.analyze([image_path], dn_prompt)
        if (
            dn_raw
            and not dn_raw.startswith("Error:")
            and "All API keys exhausted" not in dn_raw
        ):
            candidate = dn_raw.strip().split()[0]
            if candidate and len(candidate) > 3:
                drawing_no = candidate
                logger.info(
                    f"  [Page {page_num}] Drawing number resolved to: {drawing_no}"
                )

    if not components and not initial_conn:
        logger.warning(
            f"  [Page {page_num}] Parser found 0 components AND 0 connections — dumping raw response"
        )
        _dump_raw_response(page_num, "extract", raw)
    elif not components:
        logger.warning(
            f"  [Page {page_num}] Parser found 0 components ({len(initial_conn)} connections) — dumping raw response"
        )
        _dump_raw_response(page_num, "extract", raw)

    logger.info(
        f"  [Page {page_num}] Extracted {len(components)} components, "
        f"{len(initial_conn)} initial connections (drawing: {drawing_no})"
    )

    if not components and not initial_conn:
        logger.warning(f"  [Page {page_num}] Skipping audit — nothing to audit")
        return {
            "page_num": page_num,
            "drawing_no": drawing_no,
            "components": components,
            "connections": initial_conn,
        }

    if components and not initial_conn:
        logger.info(
            f"  [Page {page_num}] Using DISCOVER prompt (components found, connections empty)"
        )
        audit_prompt = DISCOVER_CONNECTIONS_PROMPT_TEMPLATE.format(
            components_json=json.dumps([c["tag"] for c in components], indent=2),
        )
    else:
        audit_prompt = AUDIT_PROMPT_TEMPLATE.format(
            iteration=1,
            components_json=json.dumps(components, indent=2),
            connections_json=json.dumps(initial_conn, indent=2),
        )

    audit_raw = vision.analyze([image_path], audit_prompt)

    if "All API keys exhausted" in audit_raw:
        logger.warning(
            f"  [Page {page_num}] Keys exhausted during audit — keeping initial connections"
        )
        refined_conn = initial_conn
    elif audit_raw.startswith("Error:"):
        logger.warning(
            f"  [Page {page_num}] Audit/Discover failed — keeping initial connections"
        )
        refined_conn = initial_conn
    else:
        refined_conn = _parse_connections(audit_raw)
        if not components and refined_conn:
            logger.warning(
                f"  [Page {page_num}] Audit produced {len(refined_conn)} connections "
                f"but 0 components — discarding to avoid hallucinations"
            )
            refined_conn = initial_conn

    logger.info(
        f"  [Page {page_num}] ✅ Done — {len(refined_conn)} connections after audit"
    )

    return {
        "page_num": page_num,
        "drawing_no": drawing_no,
        "components": components,
        "connections": refined_conn,
    }




def _save_partial(
    output_path, input_name, all_components, all_connections, done_files=None
):
    """Save current results to a consolidated xlsx (overwrites previous partial save)."""
    if not all_components and not all_connections:
        return None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_name = f"{input_name}_consolidated_{timestamp}.xlsx"
    excel_file = output_path / excel_name
    with pd.ExcelWriter(excel_file) as writer:
        pd.DataFrame(all_components).to_excel(
            writer, sheet_name="Components", index=False
        )
        pd.DataFrame(all_connections).to_excel(
            writer, sheet_name="Connections", index=False
        )
    logger.info(
        f"Saved results ({len(all_components)} comps, {len(all_connections)} conns) → {excel_file}"
    )
    if done_files:
        done_path = output_path / "done_files.json"
        with open(done_path, "w") as f:
            json.dump(sorted(done_files), f)
    return excel_file


def _load_progress(output_path):
    """Load an existing consolidated xlsx so we can resume from where we left off."""
    existing = sorted(output_path.glob("*_consolidated_*.xlsx"))
    if not existing:
        done_files_path = output_path / "done_files.json"
        done_files = set()
        if done_files_path.exists():
            try:
                done_files = set(json.load(open(done_files_path)))
            except Exception:
                pass
        return [], [], set(), done_files
    latest = existing[-1]
    logger.info(f"Found existing results: {latest.name} — loading for resume...")
    comps, conns, done_pages = [], [], set()
    try:
        xls = pd.ExcelFile(latest)
        if "Components" in xls.sheet_names:
            df_c = pd.read_excel(xls, sheet_name="Components")
            comps = df_c.to_dict("records")
        if "Connections" in xls.sheet_names:
            df_n = pd.read_excel(xls, sheet_name="Connections")
            conns = df_n.to_dict("records")
        for r in comps:
            dn = str(r.get("Drawing Num", "")).strip()
            if dn and dn != "Unknown":
                done_pages.add(dn)
    except Exception as e:
        logger.warning(f"Could not parse existing xlsx for resume: {e}")
    done_files = set()
    done_files_path = output_path / "done_files.json"
    if done_files_path.exists():
        try:
            done_files = set(json.load(open(done_files_path)))
        except Exception:
            pass
    try:
        latest.unlink()
    except OSError:
        pass
    logger.info(
        f"Resumed: {len(comps)} components, {len(conns)} connections, {len(done_files)} files done"
    )
    return comps, conns, done_pages, done_files




def process_folder(input_folder, output_folder, max_workers=10):
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    output_path.mkdir(exist_ok=True)

    temp_img_dir = os.path.join(output_folder, "temp_images")
    converter = PDFConverter(output_dir=temp_img_dir)
    vision = GeminiVision()

    pdf_files = sorted(input_path.glob("*.pdf"))
    if not pdf_files:
        logger.warning(f"No PDF files found in {input_folder}")
        return

    all_components, all_connections, done_drawings, done_files = _load_progress(
        output_path
    )
    save_interval = 3
    pdfs_since_save = 0

    for pdf_idx, pdf_file in enumerate(pdf_files, 1):
        if pdf_file.name in done_files:
            logger.info(
                f"[{pdf_idx}/{len(pdf_files)}] SKIP {pdf_file.name} (already processed)"
            )
            continue

        logger.info(f"[{pdf_idx}/{len(pdf_files)}] Processing: {pdf_file.name}")

        pdf_success = False
        for attempt in range(1, 4):
            if pdf_success:
                break

            try:
                logger.info(f"Attempt {attempt} for {pdf_file.name}...")
                image_paths = converter.convert_to_images(pdf_file)
                total_pages = len(image_paths)
                logger.info(
                    f"Launching {total_pages} parallel extractions (max_workers={max_workers})…"
                )

                page_results: dict = {}
                keys_exhausted = False

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_page = {
                        executor.submit(process_single_page, img_path, i + 1, vision): i
                        + 1
                        for i, img_path in enumerate(image_paths)
                    }
                    for future in as_completed(future_to_page):
                        page_num = future_to_page[future]
                        try:
                            result = future.result()
                            if result is _KEYS_EXHAUSTED:
                                keys_exhausted = True
                                break
                            if result:
                                page_results[page_num] = result
                                logger.info(f"Page {page_num} collected ✓")
                            else:
                                logger.warning(f"Page {page_num} returned no data")
                        except Exception as exc:
                            logger.error(f"Page {page_num} raised: {exc}")

                if keys_exhausted:
                    logger.error(
                        "All API keys exhausted — saving progress and exiting."
                    )
                    for page_num in sorted(page_results):
                        r = page_results[page_num]
                        for c in r["components"]:
                            all_components.append(
                                {
                                    "Page": page_num,
                                    "Drawing Num": r["drawing_no"],
                                    "source_pdf": pdf_file.name,
                                    "Tag": c.get("tag", ""),
                                    "Type": c.get("type", ""),
                                    "Description": c.get("description", ""),
                                }
                            )
                        for conn in r["connections"]:
                            all_connections.append(
                                {
                                    "Page": page_num,
                                    "Drawing Num": r["drawing_no"],
                                    "source_pdf": pdf_file.name,
                                    "From": conn[0] if len(conn) > 0 else "",
                                    "To": conn[1] if len(conn) > 1 else "",
                                }
                            )
                    if page_results:
                        logger.info(
                            "Saved partial data from %d completed page(s): %d comps, %d conns",
                            len(page_results),
                            len(all_components),
                            len(all_connections),
                        )
                    _save_partial(
                        output_path,
                        input_path.name,
                        all_components,
                        all_connections,
                        done_files,
                    )
                    return

                if not page_results:
                    logger.warning(f"Attempt {attempt}: no data from any page")
                    if attempt < 3:
                        time.sleep(5)
                    continue

                current_pdf_components = []
                current_pdf_connections = []

                for page_num in sorted(page_results):
                    r = page_results[page_num]
                    for c in r["components"]:
                        current_pdf_components.append(
                            {
                                "Page": page_num,
                                "Drawing Num": r["drawing_no"],
                                "source_pdf": pdf_file.name,
                                "Tag": c.get("tag", ""),
                                "Type": c.get("type", ""),
                                "Description": c.get("description", ""),
                            }
                        )
                    for conn in r["connections"]:
                        current_pdf_connections.append(
                            {
                                "Page": page_num,
                                "Drawing Num": r["drawing_no"],
                                "source_pdf": pdf_file.name,
                                "From": conn[0] if len(conn) > 0 else "",
                                "To": conn[1] if len(conn) > 1 else "",
                            }
                        )

                if current_pdf_components or current_pdf_connections:
                    all_components.extend(current_pdf_components)
                    all_connections.extend(current_pdf_connections)
                    pdf_success = True
                    done_files.add(pdf_file.name)
                    pdfs_since_save += 1
                    logger.info(
                        f"Successfully processed {pdf_file.name} (attempt {attempt}): "
                        f"{len(current_pdf_components)} comps, {len(current_pdf_connections)} conns"
                    )
                    try:
                        pdf_file.unlink()
                        logger.debug(
                            f"Removed local temp PDF after extraction: {pdf_file.name}"
                        )
                    except OSError as _oe:
                        logger.debug(
                            f"Could not remove temp PDF {pdf_file.name}: {_oe}"
                        )
                else:
                    logger.warning(
                        f"Attempt {attempt} for {pdf_file.name} yielded no data."
                    )
                    if attempt < 3:
                        logger.info("Retrying entire file...")
                        time.sleep(5)

            except KeyboardInterrupt:
                logger.warning(
                    "KeyboardInterrupt — saving partial results before exit..."
                )
                _save_partial(
                    output_path,
                    input_path.name,
                    all_components,
                    all_connections,
                    done_files,
                )
                raise
            except Exception as e:
                logger.error(f"Unexpected error processing {pdf_file.name}: {e}")
                if attempt == 3:
                    logger.error(f"Giving up on {pdf_file.name} after 3 attempts")

        if pdfs_since_save >= save_interval:
            _save_partial(
                output_path,
                input_path.name,
                all_components,
                all_connections,
                done_files,
            )
            pdfs_since_save = 0

    saved = _save_partial(
        output_path, input_path.name, all_components, all_connections, done_files
    )
    if saved:
        logger.info(f"Final consolidated results: {saved}")
    else:
        logger.warning(f"No data extracted from any files in {input_folder}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Standalone P&ID Extraction Script")
    parser.add_argument(
        "--input", required=True, help="Path to folder containing P&ID PDFs"
    )
    parser.add_argument(
        "--output",
        help="Path to output folder (defaults to 'output' folder inside input folder)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Max parallel Gemini API calls per PDF (default: 10)",
    )

    args = parser.parse_args()

    if not args.output:
        args.output = os.path.join(args.input, "output")

    start_time = time.time()
    try:
        process_folder(args.input, args.output, max_workers=args.workers)
        logger.info(f"Successfully completed in {time.time() - start_time:.2f} seconds")
    except Exception as e:
        logger.error(f"Fatal Error: {e}")
