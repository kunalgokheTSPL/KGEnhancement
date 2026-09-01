"""Object-store access for the context endpoints (RustFS on-prem, ADLS on azure)."""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from urllib.parse import quote

import pandas as pd

from p0.utils import fs as _fs
from p0.driver import get_object_fs

from ..config import (
    STAGING_DIR,
    rustfs_out_dir,
    rustfs_processed_docs,
    rustfs_processed_pnid,
    rustfs_processed_sap,
    rustfs_processed_sap_mapping,
    rustfs_processed_ts,
    rustfs_staging_pnid,
)
from ..services.docs_taxonomy import _docs_type_label_map, _docs_wanted_dirs

_log = logging.getLogger("cdm.api.context")


def _object_fs():
    """Filesystem for the active object store (RustFS on-prem, ADLS on azure), or None."""
    try:
        return get_object_fs(use_listings_cache=False)
    except Exception as exc:
        _log.warning("[object-store] filesystem unavailable: %s", exc)
        return None


def _read_parquet_from_rustfs(s3_path: str) -> pd.DataFrame:
    """Read a parquet file from the object store; empty DataFrame on any error."""
    try:
        sfs = _object_fs()
        if sfs is None:
            return pd.DataFrame()
        with sfs.open(_fs.strip_scheme(s3_path), "rb") as fh:
            return pd.read_parquet(fh).astype(str).fillna("")
    except Exception as exc:
        _log.warning("[rustfs] parquet read failed for %s: %s", s3_path, exc)
        return pd.DataFrame()


def _list_rustfs_pnid_images(plant_code_id: str | None = None) -> list[dict]:
    """List uploaded P&ID PDFs from RustFS staging path. When plant_code_id is"""
    _PNID_IMAGE_EXTS = {
        ".pdf",
        ".png",
        ".jpg",
        ".jpeg",
        ".tif",
        ".tiff",
        ".svg",
        ".bmp",
        ".webp",
    }
    images = []
    sfs = _object_fs()
    if sfs:
        try:
            base = rustfs_staging_pnid(plant_code_id)
            prefix = _fs.strip_scheme(base)
            for full_path in sfs.ls(prefix, detail=False):
                fname = full_path.split("/")[-1]
                if Path(fname).suffix.lower() in _PNID_IMAGE_EXTS:
                    images.append(
                        {
                            "filename": fname,
                            "stem": Path(fname).stem,
                            "url": f"/api/context/pnid/files/{quote(fname, safe='')}",
                        }
                    )
        except Exception as exc:
            _log.warning("[context/pnid] rustfs staging listing failed: %s", exc)
    if not images:
        local_staging = STAGING_DIR / "pnid"
        if local_staging.exists():
            for f in sorted(local_staging.iterdir()):
                if (
                    f.suffix.lower()
                    in {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff"}
                    and f.is_file()
                ):
                    images.append(
                        {
                            "filename": f.name,
                            "stem": f.stem,
                            "url": f"/api/context/pnid/files/{quote(f.name, safe='')}",
                        }
                    )
    return images


def _delete_rustfs_processed_pnid(plant_code_id: str | None = None) -> None:
    """Delete processed parquet files from RustFS after DB commit."""
    sfs = _object_fs()
    if not sfs:
        return
    base = rustfs_processed_pnid(plant_code_id)
    prefix = _fs.strip_scheme(base)
    try:
        for path in sfs.ls(prefix, detail=False):
            if path.endswith(".parquet"):
                sfs.rm(path)
    except Exception as exc:
        _log.warning("[context] rustfs listing failed: %s", exc)


def _delete_rustfs_processed_docs(plant_code_id: str | None = None) -> None:
    """Delete processed document parquets from RustFS after DB commit."""
    sfs = _object_fs()
    if not sfs:
        return
    base = rustfs_processed_docs(plant_code_id)
    prefix = _fs.strip_scheme(base)
    try:
        for path in sfs.find(prefix):
            if path.endswith(".parquet"):
                sfs.rm(path)
    except Exception as exc:
        _log.warning("[context/docs] rustfs listing failed: %s", exc)


def _list_docs_from_rustfs(
    plant_code_id: str | None = None,
    doc_types: list[str] | None = None,
) -> list[dict]:
    """List per-subtype processed document parquets from RustFS and return combined rows."""
    all_rows: list[dict] = []
    sfs = _object_fs()
    if not sfs:
        return []
    wanted_dirs: set[str] | None = None
    if doc_types:
        wanted_dirs = _docs_wanted_dirs(doc_types)
    try:
        base = rustfs_processed_docs(plant_code_id)
        prefix = _fs.strip_scheme(base)
        paths = sfs.glob(f"{prefix}/*/*.parquet")
        seen_paths: set[str] = set()
        seen_ids: set[str] = set()
        for raw_path in paths:
            if raw_path in seen_paths:
                continue
            seen_paths.add(raw_path)
            if wanted_dirs is not None:
                rel = raw_path[len(prefix) :].lstrip("/")
                subtype_dir = rel.split("/", 1)[0] if "/" in rel else rel
                if subtype_dir not in wanted_dirs:
                    continue
            s3_path = raw_path
            df = _read_parquet_from_rustfs(s3_path)
            if df.empty:
                continue
            for row in df.to_dict(orient="records"):

                rec = row.get("record_id")
                if rec:
                    dedup_key = str(rec)
                else:
                    dedup_key = "|".join(
                        str(row.get(c) or "")
                        for c in (
                            "document_id",
                            "equipment_tag",
                            "title",
                            "extracted_entities",
                            "source_file",
                        )
                    )
                if dedup_key and dedup_key in seen_ids:
                    continue
                if dedup_key:
                    seen_ids.add(dedup_key)
                all_rows.append(row)
    except Exception as exc:
        _log.debug("[context/docs] RustFS listing failed: %s", exc)
    return all_rows


def _write_processed_pnid(
    plant_code_id: str, equipment_pid: list[dict], equipment_connectivity: list[dict]
) -> dict:

    base = rustfs_processed_pnid(plant_code_id)
    ep_path = f"{base}/equipment_pid.parquet"
    ec_path = f"{base}/equipment_connectivity.parquet"
    _fs.write_parquet(pd.DataFrame(equipment_pid or []), ep_path)
    _fs.write_parquet(pd.DataFrame(equipment_connectivity or []), ec_path)
    return {"equipment_pid": ep_path, "equipment_connectivity": ec_path}


def _write_processed_ts(plant_code_id: str, rows: list[dict]) -> dict:

    path = f"{rustfs_processed_ts(plant_code_id)}/ts_timeseries_metadata.parquet"
    _fs.write_parquet(pd.DataFrame(rows or []), path)
    return {"timeseries_metadata": path}


def _write_processed_sap(plant_code_id: str, table: str, rows: list[dict]) -> dict:
    """Persist a SAP review table's edited rows to a per-table parquet under"""

    safe = re.sub(r"[^a-z0-9_-]", "_", str(table).lower().strip()) or "table"
    path = f"{rustfs_processed_sap(plant_code_id)}/{safe}/sap_review.parquet"
    _fs.write_parquet(pd.DataFrame(rows or []), path)
    return {safe: path}


def _write_processed_other_files(
    plant_code_id: str,
    table: str,
    rows: list[dict],
) -> dict:
    """
    Persist reviewed Other Files rows back to the dynamic pipeline entity table.

    Other Files preserve their user-defined schema, so the reviewed table is
    written to the same plant-scoped stage output used by the Other Files
    review/commit endpoints:

        <cdm_out>/<plant>/other_files/entities/<table>.parquet
    """

    safe = re.sub(
        r"[^a-z0-9_-]",
        "_",
        str(table or "").lower().strip(),
    ).strip("_")

    if not safe:
        raise ValueError("Invalid Other Files table name")

    out_path = (
        f"{rustfs_out_dir(plant_code_id)}/"
        f"other_files/entities/{safe}.parquet"
    )

    frame = pd.DataFrame(rows or [])

    # Empty review tables are still valid reviewed state. Writing the empty
    # parquet keeps the table identity and lets the context layer distinguish
    # "reviewed to zero rows" from "pipeline never ran".
    _fs.write_parquet(
        frame,
        out_path,
    )

    return {
        safe: out_path,
    }
def _write_processed_sap_mapping(plant_code_id: str, rows: list[dict]) -> dict:
    """Persist the SAP ↔ P&ID mapping rows to sap_mapping/sap_pid_mapping.parquet."""
    path = f"{rustfs_processed_sap_mapping(plant_code_id)}/sap_pid_mapping.parquet"
    _fs.write_parquet(pd.DataFrame(rows or []), path)
    return {"sap_pid_mapping": path}


def _write_processed_docs(plant_code_id: str, rows: list[dict]) -> dict:
    """Docs land at processed_data/<plant>/documents/<doc_type>/docs_documents.parquet"""

    base = rustfs_processed_docs(plant_code_id)
    _label_to_key = {v: k for k, v in _docs_type_label_map().items()}
    grouped: dict[str, list[dict]] = {}
    for r in rows or []:
        if not str(r.get("record_id") or "").strip():
            seed = "|".join(
                str(r.get(c) or "")
                for c in (
                    "document_id",
                    "document_type",
                    "title",
                    "source_file",
                    "equipment_tag",
                    "extracted_entities",
                )
            )
            h = hashlib.sha1(seed.encode()).hexdigest()[:12]
            r["record_id"] = f"REC-EDIT-{h}".upper()
        label = str(r.get("document_type") or r.get("type") or "").strip()
        dir_key = (
            str(r.get("document_type_key") or "").strip()
            or _label_to_key.get(label, "")
            or label
            or "general"
        )
        grouped.setdefault(dir_key, []).append(r)
    written: dict[str, str] = {}
    for subtype, sub_rows in grouped.items():

        safe = re.sub(r"[^a-z0-9_-]", "_", subtype.lower())
        out_path = f"{base}/{safe}/docs_documents.parquet"
        _fs.write_parquet(pd.DataFrame(sub_rows), out_path)
        written[safe] = out_path
    return written