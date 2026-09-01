import pandas as pd
from p0.utils.fs import (
    exists,
    glob_files,
    mtime,
    read_parquet,
    read_csv,
    read_csv_chunked,
    read_excel,
    CHUNK_SIZE,
)


def _pick_latest(files: list[str]) -> str:
    files_sorted = sorted(files, key=lambda p: mtime(p), reverse=True)
    return files_sorted[0]


def _resolve_table_path(base_path: str, table_name: str) -> tuple[str, str]:
    """Resolve a table to (file_path, format) where format is 'parquet', 'csv', or 'xlsx'."""
    parquet_exact = f"{base_path}/{table_name}.parquet"
    csv_exact = f"{base_path}/{table_name}.csv"
    xlsx_exact = f"{base_path}/{table_name}.xlsx"

    if exists(parquet_exact):
        return parquet_exact, "parquet"
    if exists(csv_exact):
        return csv_exact, "csv"
    if exists(xlsx_exact):
        return xlsx_exact, "xlsx"

    parquet_matches = glob_files(f"{base_path}/{table_name}_*.parquet")
    csv_matches = glob_files(f"{base_path}/{table_name}_*.csv")
    xlsx_matches = glob_files(f"{base_path}/{table_name}_*.xlsx")

    if not (parquet_matches or csv_matches or xlsx_matches):
        parquet_matches = glob_files(f"{base_path}/**/{table_name}*.parquet")
        csv_matches = glob_files(f"{base_path}/**/{table_name}*.csv")
        xlsx_matches = glob_files(f"{base_path}/**/{table_name}*.xlsx")

    if parquet_matches:
        return _pick_latest(parquet_matches), "parquet"
    if csv_matches:
        return _pick_latest(csv_matches), "csv"
    if xlsx_matches:
        return _pick_latest(xlsx_matches), "xlsx"

    raise FileNotFoundError(
        f"Missing {table_name}.[parquet|csv|xlsx] or "
        f"{table_name}_*.[parquet|csv|xlsx] in: {base_path}"
    )


def load_table(base_path: str, table_name: str) -> pd.DataFrame:
    """Loads a table from base_path supporting:"""
    path, fmt = _resolve_table_path(base_path, table_name)
    if fmt == "parquet":
        df = read_parquet(path, engine="pyarrow")
    elif fmt == "xlsx":
        df = read_excel(path)
    else:
        df = read_csv(path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    return df


def load_table_chunked(base_path: str, table_name: str, chunksize: int = CHUNK_SIZE):
    """Yield DataFrames of *chunksize* rows from a table file."""
    path, fmt = _resolve_table_path(base_path, table_name)
    if fmt == "parquet":
        df = read_parquet(path, engine="pyarrow")
        df.columns = [c.strip() for c in df.columns]
        for start in range(0, len(df), chunksize):
            yield df.iloc[start : start + chunksize].copy()
    else:
        header_read = False
        for chunk in read_csv_chunked(path, chunksize=chunksize, low_memory=False):
            chunk.columns = [c.strip() for c in chunk.columns]
            yield chunk
