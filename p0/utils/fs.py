import os
import glob
from contextlib import contextmanager

import pandas as pd

from p0.driver import (
    get_rustfs_settings,
)


def _rustfs_settings():
    """Resolve RustFS endpoint + credentials at call time."""
    try:
        from p0.api.config import (
            RUSTFS_ENDPOINT as _ep,
            RUSTFS_ACCESS_KEY as _key,
            RUSTFS_SECRET_KEY as _secret,
        )

        return _ep, _key, _secret
    except Exception:
        return get_rustfs_settings()


def get_storage_options():
    """Pandas storage_options for the active object store (RustFS on-prem, ADLS on azure)."""
    from p0.driver import storage_backend

    if storage_backend() == "adls":
        opts = {"account_name": os.environ.get("ADLS_ACCOUNT_NAME")}
        account_key = os.environ.get("ADLS_ACCOUNT_KEY")
        if account_key:
            opts["account_key"] = account_key
        return opts
    endpoint, key, secret = _rustfs_settings()
    return {
        "key": key,
        "secret": secret,
        "client_kwargs": {
            "endpoint_url": endpoint,
        },
    }


def get_fs():
    from p0.driver import get_object_fs

    return get_object_fs(use_listings_cache=False)


def scheme_prefix() -> str:
    """URI scheme prefix for the active object store ('s3://' on-prem, 'abfs://' on azure)."""
    from p0.driver import object_store_scheme

    return f"{object_store_scheme()}://"


def ensure_bucket(bucket: str) -> bool:
    """Idempotently create the bucket/container on the active object store."""
    from p0.driver import ensure_object_container

    return ensure_object_container(bucket)


_OBJECT_SCHEMES = ("s3://", "abfs://", "abfss://", "az://")


def is_s3(path: str) -> bool:
    """True when path lives on the object store (s3://, abfs://, abfss:// or az://)."""
    return str(path).startswith(_OBJECT_SCHEMES)


def strip_scheme(path: str) -> str:
    """Remove any object-store scheme prefix; fsspec accepts both forms."""
    s = str(path)
    for sch in _OBJECT_SCHEMES:
        if s.startswith(sch):
            return s[len(sch):]
    return s


def split_s3(path: str):
    if is_s3(path):
        path = "s3://" + strip_scheme(path)
        return path.replace("s3://", "", 1).split("/", 1)[0], path.replace(
            "s3://", "", 1
        ).split("/", 1)[1] if "/" in path.replace("s3://", "", 1) else ""
    return "", ""


def exists(path: str) -> bool:
    if is_s3(path):
        return get_fs().exists(path)
    return os.path.exists(path)


def glob_files(pattern: str) -> list[str]:
    if is_s3(pattern):
        res = get_fs().glob(pattern)
        pref = scheme_prefix()
        return [f"{pref}{r}" for r in res]
    return glob.glob(pattern)


_MTIME_KEYS = ("LastModified", "last_modified", "creation_time", "mtime")


def mtime(path: str) -> float:
    """Modification time, reading whichever key the active object store reports."""
    if is_s3(path):
        info = get_fs().info(path)
        for key in _MTIME_KEYS:
            stamp = info.get(key)
            if stamp is None:
                continue
            if isinstance(stamp, (int, float)):
                return float(stamp)
            try:
                return pd.Timestamp(stamp).timestamp()
            except (TypeError, ValueError):
                continue
        return 0.0
    return os.path.getmtime(path)


def walk(path: str):
    """Returns (dirpath, list of dirnames, list of filenames) similar to os.walk"""
    if is_s3(path):
        pref = scheme_prefix()
        for root, dirs, files in get_fs().walk(path):
            if not root.startswith(pref):
                root = f"{pref}{root}"
            yield root, dirs, files
    else:
        for root, dirs, files in os.walk(path):
            yield root, dirs, files


def read_csv(path: str, **kwargs) -> pd.DataFrame:
    if is_s3(path):
        return pd.read_csv(path, storage_options=get_storage_options(), **kwargs)
    return pd.read_csv(path, **kwargs)


def read_parquet(path: str, **kwargs) -> pd.DataFrame:
    if is_s3(path):
        return pd.read_parquet(path, storage_options=get_storage_options(), **kwargs)
    return pd.read_parquet(path, **kwargs)


def read_excel(path: str, **kwargs) -> pd.DataFrame:
    if is_s3(path):
        with get_fs().open(path, "rb") as f:
            return pd.read_excel(f, **kwargs)
    return pd.read_excel(path, **kwargs)


@contextmanager
def excel_book(path: str):
    """An open pandas ExcelFile for a local or object-store path."""
    if is_s3(path):
        with get_fs().open(path, "rb") as handle:
            with pd.ExcelFile(handle) as book:
                yield book
        return
    with pd.ExcelFile(path) as book:
        yield book


def path_join(*args) -> str:
    path = os.path.join(*args)
    if is_s3(args[0]):
        return path.replace("\\", "/")
    return path


def basename(path: str) -> str:
    if is_s3(path):
        return path.split("/")[-1]
    return os.path.basename(path)


CHUNK_SIZE = 10_000


def write_csv(df: pd.DataFrame, path: str, **kwargs):
    if is_s3(path):
        df.to_csv(path, storage_options=get_storage_options(), **kwargs)
    else:
        df.to_csv(path, **kwargs)


def write_parquet(df: pd.DataFrame, path: str, compression: str = "snappy", **kwargs):
    """Write a DataFrame to Parquet (local or S3)."""
    kwargs.setdefault("index", False)
    kwargs.setdefault("engine", "pyarrow")
    if is_s3(path):
        df.to_parquet(
            path,
            storage_options=get_storage_options(),
            compression=compression,
            **kwargs,
        )
    else:
        df.to_parquet(path, compression=compression, **kwargs)


def read_csv_chunked(path: str, chunksize: int = CHUNK_SIZE, **kwargs):
    """Yield DataFrames of ``chunksize`` rows each."""
    if is_s3(path):
        yield from pd.read_csv(
            path, storage_options=get_storage_options(), chunksize=chunksize, **kwargs
        )
    else:
        yield from pd.read_csv(path, chunksize=chunksize, **kwargs)


def write_csv_chunked(
    df: pd.DataFrame, path: str, chunksize: int = CHUNK_SIZE, **kwargs
):
    """Write a large DataFrame in chunks to reduce peak memory."""
    if is_s3(path) or len(df) <= chunksize:
        write_csv(df, path, **kwargs)
        return
    for i, start in enumerate(range(0, len(df), chunksize)):
        chunk = df.iloc[start : start + chunksize]
        if i == 0:
            chunk.to_csv(path, **kwargs)
        else:
            chunk.to_csv(path, mode="a", header=False, **kwargs)


def write_parquet_chunked(df: pd.DataFrame, path: str, **kwargs):
    """Write a large DataFrame to Parquet. Parquet handles large files well"""
    write_parquet(df, path, **kwargs)


def _append_csv(df: pd.DataFrame, path: str, header: bool = False, **kwargs):
    """Append a DataFrame to an existing CSV (local only)."""
    if is_s3(path):
        df.to_csv(
            path,
            storage_options=get_storage_options(),
            mode="a",
            header=header,
            **kwargs,
        )
    else:
        df.to_csv(path, mode="a", header=header, **kwargs)


def stream_dedup_csvs(
    source_paths: list[str],
    out_path: str,
    dedup_cols: list[str] | None = None,
    chunksize: int = CHUNK_SIZE,
    **read_kwargs,
) -> int:
    """Stream-merge multiple CSV/Parquet source files into one CSV output,"""
    seen_keys: set[tuple] = set()
    total = 0
    first_write = True

    read_kwargs.setdefault("dtype", str)
    read_kwargs.setdefault("keep_default_na", False)

    def _dedup_and_write(chunk: pd.DataFrame) -> None:
        nonlocal total, first_write
        if chunk.empty:
            return
        if dedup_cols:
            valid_keys = [c for c in dedup_cols if c in chunk.columns]
            if valid_keys:
                for c in valid_keys:
                    chunk[c] = chunk[c].astype(str).str.strip()
                key_tuples = chunk[valid_keys].apply(tuple, axis=1)
                new_mask = ~key_tuples.isin(seen_keys)
                seen_keys.update(key_tuples[new_mask])
                chunk = chunk[new_mask]
        if chunk.empty:
            return
        if first_write:
            write_csv(chunk, out_path, index=False)
            first_write = False
        else:
            _append_csv(chunk, out_path, header=False, index=False)
        total += len(chunk)

    for path in source_paths:
        if not path or not exists(path):
            continue
        try:
            if path.endswith(".parquet"):
                chunk = read_parquet(path).astype(str)
                _dedup_and_write(chunk)
            else:
                reader = read_csv_chunked(path, chunksize=chunksize, **read_kwargs)
                for chunk in reader:
                    _dedup_and_write(chunk)
        except Exception:
            continue

    return total


def stream_transform_csv(
    in_path: str,
    out_path: str,
    transform_fn,
    chunksize: int = CHUNK_SIZE,
    **read_kwargs,
) -> int:
    """Read *in_path* in chunks, apply *transform_fn(chunk)* to each chunk, and"""
    read_kwargs.setdefault("dtype", str)
    read_kwargs.setdefault("keep_default_na", False)

    total = 0
    first_write = True

    for chunk in read_csv_chunked(in_path, chunksize=chunksize, **read_kwargs):
        transformed = transform_fn(chunk)
        if transformed is None or transformed.empty:
            continue

        if first_write:
            write_csv(transformed, out_path, index=False)
            first_write = False
        else:
            _append_csv(transformed, out_path, header=False, index=False)
        total += len(transformed)

    return total


def stream_dedup_to_parquet(
    source_paths: list[str],
    out_path: str,
    dedup_cols: list[str] | None = None,
    chunksize: int = CHUNK_SIZE,
    **read_kwargs,
) -> int:
    """Stream-merge multiple CSV/Parquet source files, deduplicate, write Parquet output."""
    seen_keys: set[tuple] = set()
    frames: list[pd.DataFrame] = []
    total = 0

    read_kwargs.setdefault("dtype", str)
    read_kwargs.setdefault("keep_default_na", False)

    def _dedup_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
        if chunk.empty:
            return pd.DataFrame(columns=chunk.columns)
        if dedup_cols:
            valid_keys = [c for c in dedup_cols if c in chunk.columns]
            if valid_keys:
                for c in valid_keys:
                    chunk[c] = chunk[c].astype(str).str.strip()
                key_tuples = chunk[valid_keys].apply(tuple, axis=1)
                new_mask = ~key_tuples.isin(seen_keys)
                seen_keys.update(key_tuples[new_mask])
                chunk = chunk[new_mask]
        return chunk

    for path in source_paths:
        if not path or not exists(path):
            continue
        try:
            if path.endswith(".parquet"):
                chunk = read_parquet(path).astype(str)
                chunk = _dedup_chunk(chunk)
                if not chunk.empty:
                    frames.append(chunk)
                    total += len(chunk)
            else:
                reader = read_csv_chunked(path, chunksize=chunksize, **read_kwargs)
                for chunk in reader:
                    chunk = _dedup_chunk(chunk)
                    if not chunk.empty:
                        frames.append(chunk)
                        total += len(chunk)
        except Exception:
            continue

    if frames:
        merged = pd.concat(frames, ignore_index=True)
        write_parquet(merged, out_path)
    return total


def stream_transform_to_parquet(
    in_path: str,
    out_path: str,
    transform_fn,
    chunksize: int = CHUNK_SIZE,
    **read_kwargs,
) -> int:
    """Read *in_path* (CSV) in chunks, apply *transform_fn(chunk)*, write Parquet output."""
    read_kwargs.setdefault("dtype", str)
    read_kwargs.setdefault("keep_default_na", False)

    frames: list[pd.DataFrame] = []
    total = 0

    for chunk in read_csv_chunked(in_path, chunksize=chunksize, **read_kwargs):
        transformed = transform_fn(chunk)
        if transformed is None or transformed.empty:
            continue
        frames.append(transformed)
        total += len(transformed)

    if frames:
        merged = pd.concat(frames, ignore_index=True)
        write_parquet(merged, out_path)
    return total


def read_entity(path_without_ext: str, **kwargs) -> pd.DataFrame:
    """Read an entity file, trying Parquet first, then CSV fallback."""
    pq = path_without_ext + ".parquet"
    csv = path_without_ext + ".csv"
    if exists(pq):
        return read_parquet(pq, **kwargs)
    if exists(csv):
        return read_csv(csv, **kwargs)
    raise FileNotFoundError(f"No entity file found: {pq} or {csv}")


def entity_exists(path_without_ext: str) -> bool:
    """Check if an entity file exists in either Parquet or CSV format."""
    return exists(path_without_ext + ".parquet") or exists(path_without_ext + ".csv")


def resolve_entity_path(path_without_ext: str) -> str:
    """Return the actual path of an entity file (prefer Parquet over CSV)."""
    pq = path_without_ext + ".parquet"
    if exists(pq):
        return pq
    csv = path_without_ext + ".csv"
    if exists(csv):
        return csv
    return pq
