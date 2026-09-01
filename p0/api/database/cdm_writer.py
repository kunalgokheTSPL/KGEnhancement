"""PostgreSQL writes for the CDM tables the context endpoints commit into."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from psycopg2.extras import execute_values

from ..config import SCHEMA_FROZEN_FILE
from .connection import _get_conn
from ..deps import load_yaml
from ..services.transforms import PG_NULL_TOKENS, _coerce_for_pg, _stamp_commit_timestamps

_log = logging.getLogger("cdm.api.context")


def _ts_metadata_schema() -> tuple[str, dict[str, str]]:
    """Return (primary_key, ordered {column_name: sql_type}) for"""
    cfg = load_yaml(SCHEMA_FROZEN_FILE)
    tables = cfg.get("schema") or cfg.get("tables") or {}
    tdef = tables.get("timeseries_metadata") or {}
    cols = tdef.get("columns") or {}
    pk = tdef.get("primary_key") or "ts_uid"
    return pk, {k: str(v) for k, v in cols.items()}


def _doc_metadata_schema() -> tuple[str, dict[str, str]]:
    """Return (primary_key, ordered {column_name: sql_type}) for"""
    cfg = load_yaml(SCHEMA_FROZEN_FILE)
    tables = cfg.get("schema") or cfg.get("tables") or {}
    tdef = tables.get("document_metadata") or {}
    cols = tdef.get("columns") or {}
    pk = tdef.get("primary_key") or "document_uid"
    return pk, {k: str(v) for k, v in cols.items()}


_WRITE_BATCH_SIZE = 500


def _carry_ontology_uid(rows: list[dict], col_meta: dict, autogen: set) -> list[dict]:
    """Keep the deterministic id the pipeline computed, which a serial column would discard."""
    if "ontology_uid" not in col_meta or not autogen:
        return rows
    carried: list[dict] = []
    for row in rows:
        if str(row.get("ontology_uid") or "").strip():
            carried.append(row)
            continue
        value = ""
        for column in sorted(autogen):
            candidate = str(row.get(column) or "").strip()
            if candidate and not candidate.isdigit():
                value = candidate
                break
        carried.append({**row, "ontology_uid": value} if value else row)
    return carried


def _unique_key_columns(cur, table_name: str) -> list[str]:
    """The columns of the table's narrowest unique index, empty when it has none."""
    cur.execute(
        """
        SELECT i.relname, a.attname
          FROM pg_index x
          JOIN pg_class t ON t.oid = x.indrelid
          JOIN pg_class i ON i.oid = x.indexrelid
          JOIN pg_namespace n ON n.oid = t.relnamespace
          JOIN unnest(x.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
          JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
         WHERE x.indisunique
           AND NOT x.indisprimary
           AND x.indpred IS NULL
           AND n.nspname = current_schema()
           AND t.relname = %s
         ORDER BY i.relname, k.ord
        """,
        (table_name,),
    )
    by_index: dict[str, list[str]] = {}
    for name, column in cur.fetchall():
        by_index.setdefault(name, []).append(column)
    if not by_index:
        return []
    return min(by_index.values(), key=len)


def _merge_rows(earlier: dict, later: dict) -> dict:
    """Two rows sharing a unique key, merged so a later blank cannot erase a value."""
    merged = dict(earlier)
    for column, value in later.items():
        if value is not None and str(value).strip() != "":
            merged[column] = value
    return merged


def _dedupe_rows(rows: list[dict], key_columns: list[str]) -> list[dict]:
    """One row per unique key, since the constraint admits only the first of a clash."""
    if not key_columns:
        return list(rows)
    first_seen: dict[tuple, int] = {}
    out: list[dict] = []
    for row in rows:
        key = tuple(str(row.get(c) or "").strip() for c in key_columns)
        at = first_seen.get(key)
        if at is None:
            first_seen[key] = len(out)
            out.append(dict(row))
            continue
        out[at] = _merge_rows(out[at], row)
    return out


def _insert_page(cur, insert_sql: str, page: list[list]) -> tuple[int, list[str]]:
    """One page in a single round trip, retried row by row only when the page fails."""
    if not page:
        return 0, []
    cur.execute("SAVEPOINT page_sp")
    try:
        execute_values(cur, insert_sql, page)
        cur.execute("RELEASE SAVEPOINT page_sp")
        return len(page), []
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT page_sp")

    written = 0
    rejected: list[str] = []
    for values in page:
        cur.execute("SAVEPOINT row_sp")
        try:
            execute_values(cur, insert_sql, [values])
            cur.execute("RELEASE SAVEPOINT row_sp")
            written += 1
        except Exception as exc:
            cur.execute("ROLLBACK TO SAVEPOINT row_sp")
            rejected.append(str(exc).splitlines()[0][:200])
    return written, rejected


def _write_to_db(
    table_name: str,
    rows: list[dict],
    plant_code_id: str | None = None,
    *,
    conn=None,
    strict: bool = False,
) -> int:
    """Write rows to a provisioned table.

    Behaviour:
    - Uses the current dev batching/deduplication path.
    - When ``conn`` is omitted, this function owns commit/rollback/close.
    - When ``conn`` is supplied, the caller owns the transaction. This allows
      delete + replacement writes to remain atomic.
    - ``strict=False`` keeps best-effort behaviour: a failed batch is retried
      row-by-row and invalid rows are skipped.
    - ``strict=True`` performs batched inserts without savepoint fallback and
      raises immediately on a failed batch so the caller can roll back the
      whole transaction.
    """

    if not rows:
        return 0

    owns_connection = conn is None
    if owns_connection:
        conn = _get_conn(plant_code_id)

    try:
        cur = conn.cursor()

        columns_in_row = list(rows[0].keys())
        for c in columns_in_row:
            cur.execute(
                f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS "{c}" TEXT'
            )

        cur.execute(
            """
            SELECT column_name, data_type, column_default
              FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = %s
            """,
            (table_name,),
        )
        col_meta = {
            r[0]: {"type": r[1], "default": r[2] or ""}
            for r in cur.fetchall()
        }

        autogen = {
            c
            for c, meta in col_meta.items()
            if meta["default"].startswith("nextval(")
        }

        rows = _carry_ontology_uid(rows, col_meta, autogen)
        columns_in_row = list(rows[0].keys())

        write_columns = [
            c
            for c in columns_in_row
            if c in col_meta and c not in autogen
        ]

        if not write_columns:
            if owns_connection:
                conn.commit()
            return 0

        key_columns = [
            c for c in _unique_key_columns(cur, table_name) if c in write_columns
        ]
        deduped = _dedupe_rows(rows, key_columns)
        if len(deduped) != len(rows):
            _log.info(
                "[db/write] %s: %d row(s) collapsed onto %s",
                table_name,
                len(rows) - len(deduped),
                ", ".join(key_columns),
            )

        col_names = ", ".join(f'"{c}"' for c in write_columns)
        insert_sql = f'INSERT INTO "{table_name}" ({col_names}) VALUES %s'  # noqa: S608

        values = [
            [_coerce_for_pg(row.get(c), col_meta[c]["type"]) for c in write_columns]
            for row in deduped
        ]

        inserted = 0
        rejected: list[str] = []

        for start in range(0, len(values), _WRITE_BATCH_SIZE):
            page = values[start : start + _WRITE_BATCH_SIZE]

            if strict:
                # In strict mode any failed batch must escape to the caller.
                # No savepoint fallback is used, so an externally-owned
                # transaction can be rolled back atomically.
                execute_values(cur, insert_sql, page)
                inserted += len(page)
                continue

            written, bad = _insert_page(
                cur,
                insert_sql,
                page,
            )
            inserted += written
            rejected.extend(bad)


        for reason in rejected[:3]:
            _log.warning("[db/write] row rejected in %s: %s", table_name, reason)
        if rejected:
            _log.warning(
                "[db/write] %s: %d row(s) rejected, %d row(s) committed",
                table_name,
                len(rejected),
                inserted,
            )

        if owns_connection:
            conn.commit()

        return inserted

    except Exception:
        if owns_connection:
            conn.rollback()
        raise

    finally:
        if owns_connection:
            conn.close()


def _replace_by_natural_key(
    cur,
    table_name: str,
    rows: list[dict],
    key_col: str,
    plant_code_id: str | None,
) -> int:
    """Generalised re-commit dedup keyed on an arbitrary natural-key column."""
    incoming = {str(r.get(key_col) or "").strip() for r in rows}
    incoming.discard("")
    if not incoming:
        return 0
    cur.execute(
        "SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name = %s)",
        (table_name,),
    )
    if not cur.fetchone()[0]:
        return 0
    cur.execute(
        "SELECT EXISTS(SELECT FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = %s)",
        (table_name, key_col),
    )
    if not cur.fetchone()[0]:
        return 0
    where_plant = "AND plant_code_id = %s" if plant_code_id else ""
    params = [list(incoming), plant_code_id] if plant_code_id else [list(incoming)]
    cur.execute(
        f'DELETE FROM "{table_name}" WHERE "{key_col}" = ANY(%s) {where_plant}',  # noqa: S608
        params,
    )
    return cur.rowcount or 0


def _replace_by_source_file(
    cur,
    table_name: str,
    rows: list[dict],
    plant_code_id: str | None,
    scope_col: str | None = None,
) -> tuple[int, set[str], dict[str, "datetime"]]:
    """Re-commit semantics: replace every row whose ``(plant_code_id, source_file)``"""

    incoming_files = {str(r.get("source_file", "") or "").strip() for r in rows}
    incoming_files.discard("")
    if not incoming_files:
        return 0, set(), {}

    cur.execute(
        "SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name = %s)",
        (table_name,),
    )
    if not cur.fetchone()[0]:
        return 0, set(), {}

    cur.execute(
        "SELECT EXISTS(SELECT FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = 'source_file')",
        (table_name,),
    )
    if not cur.fetchone()[0]:
        return 0, set(), {}

    where_plant = "AND plant_code_id = %s" if plant_code_id else ""
    params_select = (
        [list(incoming_files), plant_code_id] if plant_code_id else [list(incoming_files)]
    )
    has_created_at_cur = cur.execute(
        "SELECT EXISTS(SELECT FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = 'created_at')",
        (table_name,),
    )
    has_created_at = bool(cur.fetchone()[0])

    original_created: dict[str, "datetime"] = {}
    if has_created_at:
        cur.execute(
            f'SELECT source_file, MIN(created_at) FROM "{table_name}" '  # noqa: S608
            f"WHERE source_file = ANY(%s) {where_plant} "
            f"GROUP BY source_file",
            params_select,
        )
        for sf, ts in cur.fetchall():
            if sf and ts is not None:
                original_created[sf] = ts

    use_scope = False
    pairs: list[tuple[str, str]] = []
    if scope_col:
        cur.execute(
            "SELECT EXISTS(SELECT FROM information_schema.columns "
            "WHERE table_name = %s AND column_name = %s)",
            (table_name, scope_col),
        )
        if cur.fetchone()[0]:
            pairs = sorted(
                {
                    (
                        str(r.get("source_file", "") or "").strip(),
                        str(r.get(scope_col, "") or "").strip(),
                    )
                    for r in rows
                    if str(r.get("source_file", "") or "").strip()
                }
            )
            use_scope = bool(pairs) and all(p[1] for p in pairs)

    if use_scope:
        sf_list = [p[0] for p in pairs]
        sc_list = [p[1] for p in pairs]
        cur.execute(
            f'DELETE FROM "{table_name}" '  # noqa: S608
            f'WHERE (source_file, "{scope_col}") IN '
            f"(SELECT * FROM unnest(%s::text[], %s::text[])) {where_plant} "
            f"RETURNING 1",
            ([sf_list, sc_list] + ([plant_code_id] if plant_code_id else [])),
        )
    else:
        cur.execute(
            f'DELETE FROM "{table_name}" WHERE source_file = ANY(%s) {where_plant} '  # noqa: S608
            f"RETURNING 1",
            params_select,
        )
    deleted = cur.rowcount or 0
    return deleted, incoming_files, original_created


def _write_to_doc_metadata_table(rows: list[dict], plant_code_id: str | None = None) -> int:
    """Insert rows into document_metadata using the schema.yaml column list."""
    if not rows:
        return 0
    pk, col_types = _doc_metadata_schema()
    if not col_types:
        raise RuntimeError("document_metadata schema not found in schema.yaml")

    insert_cols = [c for c in col_types if c != pk]

    conn = _get_conn(plant_code_id)
    try:
        cur = conn.cursor()
        first_row_plant = next(
            (str(r.get("plant_code_id") or "") for r in rows if r.get("plant_code_id")), None
        )
        deleted, replaced_files, original_created = _replace_by_source_file(
            cur,
            "document_metadata",
            rows,
            first_row_plant,
            scope_col="document_type",
        )
        _stamp_commit_timestamps(rows, original_created)
        kept = rows
        if replaced_files:
            _log.info(
                "[context/docs/commit] replace-by-source_file: deleted=%d  files=%d  preserved created_at for=%d",
                deleted,
                len(replaced_files),
                len(original_created),
            )

        cur.execute(
            """
            SELECT column_name, data_type
              FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = 'document_metadata'
            """
        )
        col_types_live = {r[0]: r[1] for r in cur.fetchall()}

        if col_types_live:
            behind = [c for c in insert_cols if c not in col_types_live]
            if behind:
                _log.warning(
                    "[db/write] document_metadata is behind schema.yaml — dropping %s from this write; run the plant schema migration to keep them",
                    ", ".join(behind),
                )
                insert_cols = [c for c in insert_cols if c in col_types_live]

        placeholders = ", ".join(["%s"] * len(insert_cols))
        col_names = ", ".join(f'"{c}"' for c in insert_cols)
        insert_sql = (
            f"INSERT INTO document_metadata ({col_names}) "  # noqa: S608
            f"VALUES ({placeholders})"
        )

        inserted = 0
        bad_rows = 0
        for row in kept:
            values = [
                _coerce_for_pg(row.get(c), col_types_live.get(c, "text"))
                for c in insert_cols
            ]
            cur.execute("SAVEPOINT row_sp")
            try:
                cur.execute(insert_sql, values)
                cur.execute("RELEASE SAVEPOINT row_sp")
                inserted += 1
            except Exception as exc:
                cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                bad_rows += 1
                if bad_rows <= 3:
                    _log.warning(
                        "[db/write] document_metadata row rejected: %s",
                        str(exc).splitlines()[0][:200],
                    )

        if bad_rows:
            _log.warning(
                "[db/write] document_metadata: %d row(s) rejected, %d committed",
                bad_rows,
                inserted,
            )
        conn.commit()
        return inserted
    finally:
        conn.close()


_DOC_CONFLICT_COLS = ["plant_code_id", "site", "record_id"]

_TS_CONFLICT_COLS = ["plant_code_id", "site", "tag_name"]

_TS_SITE_DEFAULT = "-"

_TS_ALWAYS_OVERWRITE = {"updated_at", "source_file", "source_system"}

_TS_BATCH_SIZE = 500


def _ts_incoming(column: str) -> str:
    """What an upsert should end up holding for one column."""
    if column in _TS_ALWAYS_OVERWRITE:
        return f'EXCLUDED."{column}"'
    return f'COALESCE(EXCLUDED."{column}", timeseries_metadata."{column}")'


def _ts_update_setter(column: str) -> str:
    """One SET clause, keeping what an earlier source already filled in."""
    return f'"{column}" = {_ts_incoming(column)}'

_DOC_REVIEW_COLS = ["row_version", "edited_by", "edited_at"]

_DOC_STAMP_COLS = ["created_at", "updated_at", "ingested_at"]


def _live_doc_columns(cur) -> dict[str, str]:
    """Columns document_metadata actually has right now, mapped to their type."""
    cur.execute(
        """
        SELECT column_name, data_type
          FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = 'document_metadata'
        """
    )
    return {r[0]: r[1] for r in cur.fetchall()}


def _doc_payload_columns(insert_cols: list[str]) -> list[str]:
    """The columns that carry content, so a re-ingest of the same row is a no-op."""
    skip = set(_DOC_CONFLICT_COLS) | set(_DOC_REVIEW_COLS) | set(_DOC_STAMP_COLS)
    return [c for c in insert_cols if c not in skip]


def _doc_upsert_sql(insert_cols: list[str], editor: str | None) -> str:
    """Upsert one document row on its identity, never reverting a human edit."""
    payload_cols = _doc_payload_columns(insert_cols)
    if not payload_cols:
        raise RuntimeError("document_metadata has no payload column to upsert")

    setters = [f'"{c}" = EXCLUDED."{c}"' for c in payload_cols]
    if "updated_at" in insert_cols:
        setters.append('"updated_at" = EXCLUDED."updated_at"')
    setters.append('"row_version" = document_metadata."row_version" + 1')
    if editor is not None:
        setters.append('"edited_by" = EXCLUDED."edited_by"')
        setters.append('"edited_at" = EXCLUDED."edited_at"')

    predicate = " OR ".join(
        f'document_metadata."{c}" IS DISTINCT FROM EXCLUDED."{c}"' for c in payload_cols
    )
    guard = "" if editor is not None else "document_metadata.edited_by IS NULL AND "

    return (
        f"INSERT INTO document_metadata "  # noqa: S608
        f"({', '.join(chr(34) + c + chr(34) for c in insert_cols)}) "
        f"VALUES ({', '.join(['%s'] * len(insert_cols))}) "
        f"ON CONFLICT ({', '.join(chr(34) + c + chr(34) for c in _DOC_CONFLICT_COLS)}) "
        f"DO UPDATE SET {', '.join(setters)} "
        f"WHERE {guard}({predicate}) "
        f"RETURNING (xmax = 0) AS inserted"
    )


def _upsert_doc_metadata(
    rows: list[dict],
    plant_code_id: str | None = None,
    editor: str | None = None,
) -> tuple[int, int, int]:
    """Upsert document rows on (plant_code_id, site, record_id), returning (inserted, updated, skipped)."""
    if not rows:
        return 0, 0, 0
    pk, col_types = _doc_metadata_schema()
    if not col_types:
        raise RuntimeError("document_metadata schema not found in schema.yaml")

    conn = _get_conn(plant_code_id)
    try:
        cur = conn.cursor()
        live = _live_doc_columns(cur)
        insert_cols = [c for c in col_types if c != pk]
        if live:
            behind = [c for c in insert_cols if c not in live]
            if behind:
                _log.warning(
                    "[db/write] document_metadata is behind schema.yaml — dropping %s from this write; run the plant schema migration to keep them",
                    ", ".join(behind),
                )
                insert_cols = [c for c in insert_cols if c in live]
        if not set(_DOC_CONFLICT_COLS) <= set(insert_cols):
            raise RuntimeError(
                "document_metadata cannot be upserted without "
                f"{', '.join(_DOC_CONFLICT_COLS)}"
            )

        upsert_sql = _doc_upsert_sql(insert_cols, editor)
        stamped = datetime.now(timezone.utc)
        _stamp_commit_timestamps(rows, {})

        inserted = updated = unchanged = rejected = 0
        for row in rows:
            values = []
            for col in insert_cols:
                if col == "row_version":
                    values.append(1)
                elif col == "edited_by":
                    values.append(editor)
                elif col == "edited_at":
                    values.append(stamped if editor is not None else None)
                else:
                    values.append(_coerce_for_pg(row.get(col), live.get(col, "text")))
            cur.execute("SAVEPOINT doc_sp")
            try:
                cur.execute(upsert_sql, values)
                result = cur.fetchone()
                cur.execute("RELEASE SAVEPOINT doc_sp")
            except Exception as exc:
                cur.execute("ROLLBACK TO SAVEPOINT doc_sp")
                rejected += 1
                if rejected <= 3:
                    _log.warning(
                        "[db/write] document_metadata row rejected: %s",
                        str(exc).splitlines()[0][:200],
                    )
                continue
            if result is None:
                unchanged += 1
            elif result[0]:
                inserted += 1
            else:
                updated += 1
        conn.commit()
        _log.info(
            "[context/docs/commit] upsert: inserted=%d updated=%d unchanged=%d editor=%s",
            inserted,
            updated,
            unchanged,
            editor or "pipeline",
        )
        if rejected:
            _log.warning(
                "[db/write] document_metadata: %d row(s) rejected, %d written",
                rejected,
                inserted + updated,
            )
        return inserted, updated, unchanged
    finally:
        conn.close()


def _doc_row_key(row: dict) -> tuple[str, str]:
    """The identity of one document row, as the unique index sees it."""
    return (
        str(row.get("site") or "").strip(),
        str(row.get("record_id") or "").strip(),
    )


def _doc_row_versions(
    rows: list[dict], plant_code_id: str | None = None
) -> dict[tuple[str, str], int]:
    """Stored row_version per identity, so an edit of a moved row can be refused."""
    keys = [_doc_row_key(r) for r in rows]
    record_ids = sorted({rid for _, rid in keys if rid})
    if not record_ids:
        return {}

    conn = _get_conn(plant_code_id)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT "site", "record_id", "row_version"
              FROM document_metadata
             WHERE "plant_code_id" = %s AND "record_id" = ANY(%s)
            """,
            (plant_code_id, record_ids),
        )
        return {
            (str(r[0] or "").strip(), str(r[1] or "").strip()): r[2]
            for r in cur.fetchall()
        }
    finally:
        conn.close()


def _stale_doc_rows(rows: list[dict], plant_code_id: str | None = None) -> list[dict]:
    """Rows whose row_version no longer matches what the table holds."""
    claimed = [r for r in rows if r.get("row_version") not in (None, "")]
    if not claimed:
        return []

    stored = _doc_row_versions(claimed, plant_code_id)
    stale = []
    for row in claimed:
        key = _doc_row_key(row)
        if key not in stored:
            continue
        try:
            expected = int(row["row_version"])
        except (TypeError, ValueError):
            continue
        if expected != stored[key]:
            stale.append(
                {
                    "site": key[0],
                    "record_id": key[1],
                    "expected_row_version": expected,
                    "current_row_version": stored[key],
                }
            )
    return stale


def _delete_doc_rows(rows: list[dict], plant_code_id: str | None = None) -> tuple[int, list[dict]]:
    """Remove the document rows a reviewer struck out, returning (deleted, not_found)."""
    keys = [k for k in (_doc_row_key(r) for r in rows) if k[1]]
    if not keys:
        return 0, []

    conn = _get_conn(plant_code_id)
    try:
        cur = conn.cursor()
        deleted = 0
        missing: list[dict] = []
        for site, record_id in keys:
            cur.execute(
                'DELETE FROM document_metadata WHERE "plant_code_id" = %s '  # noqa: S608
                'AND "site" = %s AND "record_id" = %s',
                (plant_code_id, site, record_id),
            )
            gone = cur.rowcount or 0
            deleted += gone
            if not gone:
                missing.append({"site": site, "record_id": record_id})
        conn.commit()
        _log.info(
            "[context/docs/commit] delete: removed=%d not_found=%d",
            deleted,
            len(missing),
        )
        return deleted, missing
    finally:
        conn.close()


def _ts_site(row: dict) -> str:
    """The site a timeseries row belongs to, defaulted the way the schema does."""
    site = str(row.get("site") or "").strip()
    if site.lower() in PG_NULL_TOKENS:
        return _TS_SITE_DEFAULT
    return site or _TS_SITE_DEFAULT


def _ts_row_values(
    row: dict, insert_cols: list[str], col_types: dict[str, str] | None = None
) -> list:
    """Row values in insert_cols order, site defaulted, blanks nulled, types coerced."""
    out: list = []
    for col in insert_cols:
        val = _ts_site(row) if col == "site" else row.get(col)
        if val == "":
            val = None
        if col_types is not None:
            val = _coerce_for_pg(val, col_types.get(col, "text"))
        out.append(val)
    return out


def _ts_merge_values(earlier: list, later: list, insert_cols: list[str]) -> list:
    """Two rows sharing a conflict key, merged the way sequential upserts would."""
    merged = list(earlier)
    for idx, col in enumerate(insert_cols):
        if col in _TS_ALWAYS_OVERWRITE or later[idx] is not None:
            merged[idx] = later[idx]
    return merged


def _ts_dedupe_values(values: list[list], insert_cols: list[str]) -> list[list]:
    """One row per conflict key, since ON CONFLICT cannot touch a row twice."""
    key_positions = [insert_cols.index(c) for c in _TS_CONFLICT_COLS]
    first_seen: dict[tuple, int] = {}
    out: list[list] = []
    for row in values:
        key = tuple(row[pos] for pos in key_positions)
        at = first_seen.get(key)
        if at is None:
            first_seen[key] = len(out)
            out.append(row)
            continue
        out[at] = _ts_merge_values(out[at], row, insert_cols)
    return out


_UNCOMPARABLE_TYPES = frozenset({"json", "xml", "point", "line", "lseg", "box", "path", "polygon", "circle"})


def _ts_change_test(column: str, live_types: dict[str, str]) -> str:
    """One change test, cast to text for types postgres cannot compare directly."""
    incoming = _ts_incoming(column)
    if str(live_types.get(column, "")).lower() in _UNCOMPARABLE_TYPES:
        return f'timeseries_metadata."{column}"::text IS DISTINCT FROM ({incoming})::text'
    return f'timeseries_metadata."{column}" IS DISTINCT FROM {incoming}'


def _live_columns(cur, table: str) -> dict[str, str]:
    """The columns the table actually has right now, by name."""
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = %s",
        (table,),
    )
    return {r[0]: r[1] for r in cur.fetchall()}


def _reconcile_columns(
    cur, conn, table: str, declared: dict[str, str], live: dict[str, str]
) -> dict[str, str]:
    """Add declared-but-absent columns with their declared type, and report what remains."""
    missing = [c for c in declared if c not in live]
    if not missing:
        return live
    for name in missing:
        sql_type = str(declared.get(name) or "").strip()
        if not sql_type or sql_type.upper().startswith(("BIGSERIAL", "SERIAL")):
            continue
        try:
            cur.execute(
                f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{name}" {sql_type}'
            )
            conn.commit()
            _log.warning(
                "[cdm/%s] column %r was absent and has been added as %s — the startup "
                "schema migration had not reached this database",
                table,
                name,
                sql_type,
            )
        except Exception as exc:
            conn.rollback()
            _log.warning("[cdm/%s] could not add column %r: %s", table, name, exc)
    return _live_columns(cur, table)


def _upsert_ts_metadata(rows: list[dict], plant_code_id: str | None = None) -> tuple[int, int, int]:
    """UPSERT timeseries rows on (plant_code_id, site, tag_name)."""
    if not rows:
        return 0, 0, 0
    pk, col_types = _ts_metadata_schema()
    if not col_types:
        raise RuntimeError("timeseries_metadata schema not found in schema.yaml")

    insert_cols = [c for c in col_types if c != pk]
    rows = _carry_ontology_uid(rows, col_types, {pk})
    conflict_cols = list(_TS_CONFLICT_COLS)
    update_cols = [c for c in insert_cols if c not in conflict_cols]

    conn = _get_conn(plant_code_id)
    try:
        cur = conn.cursor()
        col_types_live = _live_columns(cur, "timeseries_metadata")

        absent_keys = [c for c in conflict_cols if c not in col_types_live]
        if col_types_live and absent_keys:
            raise RuntimeError(
                "timeseries_metadata is missing the upsert key column(s) "
                f"{', '.join(absent_keys)}; run the plant schema migration before "
                "committing, because rows cannot be matched safely without them"
            )

        col_types_live = _reconcile_columns(
            cur, conn, "timeseries_metadata", col_types, col_types_live
        )

        if col_types_live:
            unwritable = [c for c in insert_cols if c not in col_types_live]
            if unwritable:
                _log.error(
                    "[cdm/timeseries_metadata] %d column(s) still absent after "
                    "reconciliation and will not be written: %s",
                    len(unwritable),
                    ", ".join(unwritable),
                )
                insert_cols = [c for c in insert_cols if c in col_types_live]
                update_cols = [c for c in insert_cols if c not in conflict_cols]

        values = _ts_dedupe_values(
            [_ts_row_values(row, insert_cols, col_types_live) for row in rows],
            insert_cols,
        )

        col_names = ", ".join(f'"{c}"' for c in insert_cols)
        conflict_target = ", ".join(f'"{c}"' for c in conflict_cols)
        update_setters = ", ".join(_ts_update_setter(c) for c in update_cols)
        update_predicate = " OR ".join(
            _ts_change_test(c, col_types_live) for c in update_cols
        )
        upsert_sql = (
            f"INSERT INTO timeseries_metadata ({col_names}) "  # noqa: S608
            f"VALUES %s "
            f"ON CONFLICT ({conflict_target}) DO UPDATE "
            f"SET {update_setters} "
            f"WHERE {update_predicate} "
            f"RETURNING (xmax = 0) AS inserted"
        )

        outcomes = execute_values(
            cur, upsert_sql, values, page_size=_TS_BATCH_SIZE, fetch=True
        )
        inserted = sum(1 for outcome in outcomes if outcome[0])
        updated = len(outcomes) - inserted
        conn.commit()
        return inserted, updated, len(rows) - len(outcomes)
    finally:
        conn.close()


def _read_ts_from_db(plant_code_id: str, source_file: str | None = None) -> list[dict]:
    """Read committed timeseries metadata rows from Postgres for review."""
    try:
        conn = _get_conn(plant_code_id)
    except Exception as exc:
        _log.debug("[context/ts] DB read skipped: %s", exc)
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT EXISTS(SELECT FROM information_schema.tables "
            "WHERE table_name = 'timeseries_metadata')"
        )
        if not cur.fetchone()[0]:
            return []
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'timeseries_metadata' ORDER BY ordinal_position"
        )
        cols = [r[0] for r in cur.fetchall()]
        if not cols:
            return []
        has_sf = "source_file" in cols
        col_list = ", ".join(f'"{c}"' for c in cols)
        params: list = [plant_code_id]
        where = 'WHERE "plant_code_id" = %s'
        if source_file and has_sf:
            want = str(source_file).strip().replace("\\", "/").rsplit("/", 1)[-1]
            where += " AND split_part(regexp_replace(\"source_file\", '\\\\', '/', 'g'), '/', -1) = %s"
            params.append(want)
        cur.execute(
            f"SELECT {col_list} FROM timeseries_metadata {where} "  # noqa: S608
            'ORDER BY "tag_name"',
            params,
        )
        fetched = cur.fetchall()
        return [dict(zip(cols, row)) for row in fetched]
    except Exception as exc:
        _log.warning("[context/ts] DB read error: %s", exc)
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass