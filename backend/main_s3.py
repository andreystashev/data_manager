import os
import re
import json
import threading
import time
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

import boto3
from botocore.config import Config

from trim_parquet import trim_bag, reindex_metadata
from download_state import read_state, save_state, mark_complete, is_complete

# --- Configuration ---

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "demo-datasets")
DATASETS_PREFIX = "datasets_"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

PAGE_SIZE = 50

_default_dl = Path.home() / "work_folder"
if not _default_dl.exists():
    _default_dl = Path.home()
DOWNLOAD_BASE = Path(os.getenv("DOWNLOAD_BASE", str(_default_dl)))
DOWNLOAD_DISPLAY = os.getenv(
    "DOWNLOAD_DISPLAY",
    "~/" + ("work_folder" if (Path.home() / "work_folder").exists() else ""),
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Category patterns ---

CATEGORY_PATTERNS = [
    ("lidars",  re.compile(r"_lidars_\d+\.parquet$")),
    ("cameras", re.compile(r"_cameras_\d+\.parquet$")),
    ("radars",  re.compile(r"_radars_\d+\.parquet$")),
    ("other",   re.compile(r"\.parquet$")),
]

def categorize_file(filename: str) -> Optional[str]:
    for cat, pat in CATEGORY_PATTERNS:
        if pat.search(filename):
            return cat
    return None

def _numeric_sort_key(s3_key: str):
    filename = s3_key.split("/")[-1]
    m = re.search(r'_(\d+)\.parquet$', filename)
    return int(m.group(1)) if m else -1

def _build_folder_suffix(params: dict) -> str:
    """Build folder suffix: "" | "_M" | "_LTM" | "_LTM_HHMMSS-HHMMSS" etc."""
    cats = ""
    if params.get("lidars"):  cats += "L"
    if params.get("cameras"): cats += "C"
    if params.get("radars"):  cats += "R"
    if params.get("other"):   cats += "O"
    if params.get("topics") is not False and "topics" in params: cats += "T"
    if params.get("meta") is not False and "meta" in params:     cats += "M"

    has_lcro = bool(set(cats) & set("LCRO"))
    all_selected = set(cats) >= set("LCROTM")
    has_trim = (params.get("trimStart") is not None
                and params.get("trimEnd") is not None
                and params.get("startSec") is not None)

    # All categories + no trim → no suffix at all
    if all_selected and not has_trim:
        return ""

    if not cats:
        return ""

    parts = [cats]

    # Add time range only when LCRO data present AND trim params exist
    if has_lcro and has_trim:
        start_sec = params["startSec"]
        t1 = datetime.fromtimestamp(start_sec + params["trimStart"])
        t2 = datetime.fromtimestamp(start_sec + params["trimEnd"])
        parts.append(f"{t1.strftime('%H%M%S')}-{t2.strftime('%H%M%S')}")

    return "_" + "_".join(parts)

# Reverse parser for folder suffix
RE_FOLDER_SUFFIX = re.compile(r'^(.+?)_([LCROTM]+)(?:_(\d{6})-(\d{6}))?$')
# Pattern to detect dataset names: YYYY-MM-DD-DEVICE_ID-...
RE_BAG_NAME = re.compile(r'^\d{4}-\d{2}-\d{2}-\d+-')

def _parse_folder_suffix(folder_name: str) -> Optional[dict]:
    """Parse folder name → {dataset_name, cats, time_range} or None."""
    m = RE_FOLDER_SUFFIX.match(folder_name)
    if m:
        cat_str = m.group(2)
        return {
            "dataset_name": m.group(1),
            "cats": {
                "lidars": "L" in cat_str, "cameras": "C" in cat_str,
                "radars": "R" in cat_str, "other": "O" in cat_str,
                "topics": "T" in cat_str, "meta": "M" in cat_str,
            },
            "time_range": (m.group(3), m.group(4)) if m.group(3) else None,
        }

    with _lock:
        is_known = folder_name in _datasets
    if is_known or RE_BAG_NAME.match(folder_name):
        return {
            "dataset_name": folder_name,
            "cats": {
                "lidars": True, "cameras": True, "radars": True,
                "other": True, "topics": True, "meta": True,
            },
            "time_range": None,
        }
    return None

# --- S3 ---

def load_aws_credentials():
    global AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        return
    try:
        from configparser import ConfigParser
        creds = Path.home() / ".aws" / "credentials"
        if creds.exists():
            cfg = ConfigParser()
            cfg.read(creds)
            if "default" in cfg:
                AWS_ACCESS_KEY_ID = cfg.get("default", "aws_access_key_id", fallback="")
                AWS_SECRET_ACCESS_KEY = cfg.get("default", "aws_secret_access_key", fallback="")
                ep = cfg.get("default", "endpoint_url", fallback=None)
                if ep:
                    S3_ENDPOINT = ep
                logger.info("[config] Loaded AWS credentials from ~/.aws/credentials")
    except Exception as e:
        logger.debug(f"[config] Could not load AWS credentials: {e}")

s3_client = None

def init_s3_client():
    global s3_client
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name='us-east-1',
        config=Config(
            connect_timeout=30, read_timeout=120,
            retries={'max_attempts': 5, 'mode': 'standard'},
            max_pool_connections=5,
            s3={'addressing_style': 'path'},
        ),
        use_ssl=S3_ENDPOINT.startswith('https://')
    )
    logger.info(f"[s3] S3 client initialized: {S3_ENDPOINT}")

def iter_s3_objects(prefix: str):
    if not s3_client:
        init_s3_client()
    paginator = s3_client.get_paginator('list_objects_v2')
    total = 0
    t0 = time.time()
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                total += 1
                yield {"Key": obj['Key'], "Size": obj['Size']}
    logger.info(f"[s3] Listed {total} objects from '{prefix}' in {time.time() - t0:.0f}s")

def download_s3_file(s3_key: str, local_path: str) -> None:
    if not s3_client:
        init_s3_client()
    s3_client.download_file(S3_BUCKET, s3_key, local_path)

# --- In-memory state ---

_lock = threading.Lock()
_datasets: Dict[str, dict] = {}        # dataset_id → {date, device_id, s3_base_path, dataset_suffix, size_bytes, files, keys}
_queue: List[dict] = []            # [{id, dataset_name, params, status, downloaded, ...}]
_queue_id_counter = 0

# --- Index sync ---

_cache_updating = threading.Lock()

def sync_index_only():
    if not _cache_updating.acquire(blocking=False):
        logger.info("[cache] Update already in progress, skipping")
        return
    try:
        _sync_index_inner()
    finally:
        _cache_updating.release()

def _sync_index_inner():
    logger.info("[cache] Syncing datasets_index.json ...")
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key="datasets_index.json")
        index_data = json.loads(resp["Body"].read().decode("utf-8"))
        logger.info(f"[cache] datasets_index.json: {len(index_data)} entries")
    except Exception as e:
        logger.error(f"[cache] Failed to get datasets_index.json: {e}")
        return

    dataset_data: Dict[str, dict] = {}
    for entry in index_data:
        dataset_name = str(entry.get("filename", "")).strip()
        if not dataset_name:
            continue
        try:
            year = f"{int(entry['year']):04d}"
            month = f"{int(entry['month']):02d}"
            day = f"{int(entry['day']):02d}"
            device_id = str(entry["device_id"]).strip()
        except (KeyError, ValueError, TypeError):
            parts = dataset_name.split("-", 4)
            if len(parts) < 5:
                continue
            year, month, day, device_id = parts[0], parts[1], parts[2], parts[3]

        s3_base_path = f"year={year}/month={month}/day={day}/device_id={device_id}"
        date_prefix = f"{year}-{month}-{day}-{device_id}-"
        dataset_suffix = dataset_name[len(date_prefix):] if dataset_name.startswith(date_prefix) else dataset_name
        topics_key = f"{DATASETS_PREFIX}topics/{s3_base_path}/{dataset_name}_topics.parquet"

        dataset_data[dataset_name] = {
            "date": f"{year}-{month}-{day}",
            "device_id": device_id,
            "s3_base_path": s3_base_path,
            "dataset_suffix": dataset_suffix,
            "size_bytes": 0,
            "files": {"lidars": 0, "cameras": 0, "radars": 0, "other": 0, "topics": 1, "meta": 0},
            "keys": {"topics": [topics_key], "meta": [], "lidars": [], "cameras": [], "radars": [], "other": []},
        }

    if not dataset_data:
        logger.warning("[cache] No bags parsed from datasets_index.json")
        return

    with _lock:
        _datasets.clear()
        _datasets.update(dataset_data)
    logger.info(f"[cache] Loaded {len(dataset_data)} bags into memory")

# --- Per-dataset S3 scan ---

def _live_resolve_bag_keys(dataset_name: str, base_path: str, dataset_suffix: str) -> dict:
    """Narrow S3 scan for a single dataset."""
    keys: dict = {cat: [] for cat in ["topics", "meta", "lidars", "cameras", "radars", "other"]}
    cat_sizes: dict = {cat: 0 for cat in keys}
    total_size = 0

    topics_key = f"{DATASETS_PREFIX}topics/{base_path}/{dataset_name}_topics.parquet"
    try:
        head = s3_client.head_object(Bucket=S3_BUCKET, Key=topics_key)
        keys["topics"].append(topics_key)
        sz = head.get("ContentLength", 0)
        cat_sizes["topics"] += sz
        total_size += sz
    except Exception:
        keys["topics"].append(topics_key)

    for folder in dict.fromkeys([dataset_suffix, dataset_name]):
        meta_key = f"{DATASETS_PREFIX}meta/{base_path}/{folder}/metadata.yaml"
        try:
            head = s3_client.head_object(Bucket=S3_BUCKET, Key=meta_key)
            keys["meta"].append(meta_key)
            sz = head.get("ContentLength", 0)
            cat_sizes["meta"] += sz
            total_size += sz
            break
        except Exception:
            pass

    variants_tried = set()
    for variant in dict.fromkeys([dataset_name, dataset_suffix]):
        if variant in variants_tried:
            continue
        variants_tried.add(variant)
        prefix = f"{DATASETS_PREFIX}messages/{base_path}/{variant}_"
        try:
            for obj in iter_s3_objects(prefix):
                key = obj["Key"]
                filename = key.split("/")[-1]
                cat = categorize_file(filename)
                if cat:
                    keys[cat].append(key)
                    cat_sizes[cat] += obj["Size"]
                    total_size += obj["Size"]
        except Exception as e:
            logger.warning(f"[resolve] Failed to list {prefix}: {e}")

    total_files = sum(len(v) for v in keys.values())
    logger.info(f"[resolve] {dataset_name}: {total_files} files, {total_size / (1024**2):.1f} MB")
    return {"keys": keys, "cat_sizes": cat_sizes, "total_size": total_size}

def _cache_live_resolve(dataset_name: str, resolved: dict):
    with _lock:
        dataset = _datasets.get(dataset_name)
        if not dataset:
            return
        keys = resolved["keys"]
        dataset["keys"] = keys
        dataset["cat_sizes"] = resolved.get("cat_sizes", {})
        for cat in ["lidars", "cameras", "radars", "other"]:
            dataset["files"][cat] = len(keys.get(cat, []))
        dataset["files"]["meta"] = len(keys.get("meta", []))
        dataset["size_bytes"] = resolved.get("total_size", 0)

# --- Metadata ---

_RE_NANOS = re.compile(r"nanoseconds(?:_since_epoch)?:\s*(\d+)")
_RE_FILE_BLOCK = re.compile(
    r"-\s*path:\s*(\S+).*?"
    r"starting_time:\s*\n\s*nanoseconds_since_epoch:\s*(\d+).*?"
    r"duration:\s*\n\s*nanoseconds:\s*(\d+)",
    re.DOTALL,
)

def _fetch_metadata_yaml(dataset_name: str) -> str:
    with _lock:
        dataset = _datasets.get(dataset_name)
    if not dataset:
        raise HTTPException(404, f"Bag not found: {dataset_name}")

    for folder in dict.fromkeys([dataset["dataset_suffix"], dataset_name]):
        meta_key = f"{DATASETS_PREFIX}meta/{dataset['s3_base_path']}/{folder}/metadata.yaml"
        try:
            resp = s3_client.get_object(Bucket=S3_BUCKET, Key=meta_key)
            return resp["Body"].read().decode("utf-8")
        except Exception:
            pass

    raise HTTPException(404, f"metadata.yaml not found for {dataset_name}")

def _parse_metadata_yaml(yaml_text: str) -> dict:
    nanos = _RE_NANOS.findall(yaml_text)
    if len(nanos) < 2:
        raise HTTPException(422, "Could not parse duration/starting_time from metadata.yaml")

    duration_ns = int(nanos[0])
    start_ns = int(nanos[1])
    start_sec = start_ns / 1e9
    duration_sec = duration_ns / 1e9

    files = []
    for m in _RE_FILE_BLOCK.finditer(yaml_text):
        f_start_ns = int(m.group(2))
        f_dur_ns = int(m.group(3))
        f_start_sec = f_start_ns / 1e9
        f_dur_sec = f_dur_ns / 1e9
        files.append({
            "path": m.group(1),
            "start_ns": f_start_ns,
            "duration_ns": f_dur_ns,
            "start_sec": f_start_sec,
            "duration_sec": f_dur_sec,
            "end_sec": f_start_sec + f_dur_sec,
        })

    return {
        "start_ns": start_ns, "duration_ns": duration_ns,
        "start_sec": start_sec, "duration_sec": duration_sec,
        "end_sec": start_sec + duration_sec,
        "files": files,
    }

# --- Download worker ---

_dl_cancel = threading.Event()

def _inject_oneshot_from_zero(dest_dir: Path):
    """Extract one-shot topics from _0.parquet into the first data file, then delete _0.

    Needed when downloading LCR categories without O:
    _0.parquet has all topics (including unwanted ones), but we need the one-shot
    topics (maps, tf_static, route) for autoware to initialize.
    """
    import pyarrow.parquet as pq
    from trim_parquet import ONESHOT_TOPICS, DATA_SCHEMA

    zero_files = [f for f in dest_dir.iterdir() if f.name.endswith("_0.parquet") and "_topics" not in f.name]
    if not zero_files:
        return

    zero_file = zero_files[0]
    topics_file = next((f for f in dest_dir.iterdir() if f.name.endswith("_topics.parquet")), None)
    if not topics_file:
        return

    # Get one-shot topic IDs
    tt = pq.read_table(topics_file)
    oneshot_ids = set()
    for tid, name in zip(tt.column("topic_id").to_pylist(), tt.column("topic_name").to_pylist()):
        if name in ONESHOT_TOPICS:
            oneshot_ids.add(tid)

    if not oneshot_ids:
        zero_file.unlink()
        return

    # Extract one-shot rows from _0
    import pyarrow as pa
    zero_table = pq.read_table(zero_file).cast(DATA_SCHEMA)
    tid_col = zero_table.column("topic_id")
    mask = pa.array([t.as_py() in oneshot_ids for t in tid_col])
    oneshot_rows = zero_table.filter(mask)

    if len(oneshot_rows) == 0:
        zero_file.unlink()
        return

    # Find first non-zero data file to inject into
    import re as _re
    data_files = []
    for f in dest_dir.iterdir():
        m = _re.search(r'_(\d+)\.parquet$', f.name)
        if m and "_topics" not in f.name and f != zero_file:
            data_files.append((int(m.group(1)), f))
    data_files.sort()

    if not data_files:
        zero_file.unlink()
        return

    first_file = data_files[0][1]
    first_table = pq.read_table(first_file).cast(DATA_SCHEMA)

    # Set one-shot timestamps to just before the first real message
    ts_col = first_table.column("timestamp")
    first_ts = ts_col[0].as_py() if len(first_table) > 0 else 0
    n = len(oneshot_rows)
    fake_ts = pa.array([first_ts - n + i for i in range(n)], type=pa.int64())
    oneshot_rows = oneshot_rows.set_column(
        oneshot_rows.schema.get_field_index("timestamp"), "timestamp", fake_ts,
    )

    merged = pa.concat_tables([oneshot_rows, first_table])
    new_ids = pa.array(range(len(merged)), type=pa.uint32())
    merged = merged.set_column(merged.schema.get_field_index("id"), "id", new_ids)
    pq.write_table(merged, first_file, compression="snappy", row_group_size=1000, use_dictionary=False)

    zero_file.unlink()
    logger.info(f"[oneshot] Injected {n} one-shot rows into {first_file.name}, deleted {zero_file.name}")


def _rename_for_ros2(dest_dir: Path, old_prefix: str, new_prefix: str):
    """Rename parquet files so ROS2 parquet plugin can find them by folder name."""
    if old_prefix == new_prefix:
        return
    names = {}
    for f in list(dest_dir.iterdir()):
        if (f.is_file() and f.name.startswith(old_prefix + "_")
                and not f.name.startswith(new_prefix + "_") and f.suffix == ".parquet"):
            new_name = new_prefix + f.name[len(old_prefix):]
            f.rename(dest_dir / new_name)
            names[f.name] = new_name
    meta = dest_dir / "metadata.yaml"
    if meta.exists() and names:
        text = meta.read_text(encoding="utf-8")
        for old_name, new_name in names.items():
            text = text.replace(old_name, new_name)
        meta.write_text(text, encoding="utf-8")
    logger.info(f"[rename] {dest_dir.name}: renamed {len(names)} files")

def _resolve_keys(dataset_name: str, params: dict) -> tuple:
    with _lock:
        dataset = _datasets.get(dataset_name)
    if not dataset:
        raise ValueError(f"Bag not found: {dataset_name}")

    cached_keys = dict(dataset.get("keys", {}))
    has_cache = any(cached_keys.get(cat) for cat in ["lidars", "cameras", "radars", "other"])

    if not has_cache:
        logger.info(f"[resolve] No cached keys for {dataset_name}, doing live S3 lookup")
        resolved = _live_resolve_bag_keys(dataset_name, dataset["s3_base_path"], dataset["dataset_suffix"])
        cached_keys = resolved["keys"]
        _cache_live_resolve(dataset_name, resolved)

    # Time-range filter
    trim_file_set = None
    trim_start = params.get("trimStart")
    trim_end = params.get("trimEnd")
    start_sec = params.get("startSec")
    has_data_cats = any(params.get(c) for c in ["lidars", "cameras", "radars", "other"])

    if trim_start is not None and trim_end is not None and start_sec is not None:
        abs_start = start_sec + trim_start
        abs_end = start_sec + trim_end
        logger.info(f"[resolve] Trim filter: {trim_start}s-{trim_end}s")
        try:
            yaml_text = _fetch_metadata_yaml(dataset_name)
            meta = _parse_metadata_yaml(yaml_text)
            trim_file_set = set()
            for f in meta["files"]:
                if f["start_sec"] <= abs_end and f["end_sec"] >= abs_start:
                    trim_file_set.add(f["path"])

            # Always include _0 parquet (one-shot topics: maps, tf_static, route)
            if has_data_cats:
                for f in meta["files"]:
                    if f["path"].endswith("_0.parquet"):
                        trim_file_set.add(f["path"])

            logger.info(f"[resolve] Trim: {len(trim_file_set)}/{len(meta['files'])} parquets match")
        except Exception as e:
            logger.warning(f"[resolve] Could not apply trim filter: {e}, downloading all")
            trim_file_set = None

    keys_to_dl = []
    if params.get("topics", True):
        keys_to_dl.extend(
            cached_keys.get("topics")
            or [f"{DATASETS_PREFIX}topics/{dataset['s3_base_path']}/{dataset_name}_topics.parquet"]
        )
    if params.get("meta", True):
        keys_to_dl.extend(cached_keys.get("meta", []))

    for cat in ["lidars", "cameras", "radars", "other"]:
        if params.get(cat):
            cat_keys = cached_keys.get(cat, [])
            cat_keys.sort(key=_numeric_sort_key)
            if trim_file_set is not None:
                cat_keys = [k for k in cat_keys if k.split("/")[-1] in trim_file_set]
            keys_to_dl.extend(cat_keys)

    # Always include _0.parquet when LCR selected but not O (one-shot topics live there)
    has_lcr_no_other = (
        any(params.get(c) for c in ["lidars", "cameras", "radars"])
        and not params.get("other")
    )
    if has_lcr_no_other:
        other_keys = cached_keys.get("other", [])
        zero_key = next((k for k in other_keys if k.split("/")[-1].endswith("_0.parquet")), None)
        if zero_key and zero_key not in keys_to_dl:
            keys_to_dl.append(zero_key)

    suffix = _build_folder_suffix(params)
    folder_name = dataset_name + suffix
    dest_dir = DOWNLOAD_BASE / folder_name
    host_dest = f"{DOWNLOAD_DISPLAY}/{folder_name}"
    return keys_to_dl, dest_dir, host_dest

def _update_queue_item(item_id: int, **kwargs):
    with _lock:
        for item in _queue:
            if item["id"] == item_id:
                item.update(kwargs)
                return

def _dl_worker():
    logger.info("[dl-worker] started")
    while True:
        with _lock:
            row = next((item for item in _queue if item["status"] == "pending"), None)

        if not row:
            time.sleep(1)
            continue

        item_id = row["id"]
        dataset_name = row["dataset_name"]
        params = row["params"]
        stored_keys = row.get("keys")
        is_resume = stored_keys is not None
        logger.info(f"[dl-worker] {'resuming' if is_resume else 'starting'} {dataset_name} (id={item_id})")

        try:
            # Resolve timeRangeHint from recovery into real trim params
            if "timeRangeHint" in params and "trimStart" not in params:
                try:
                    yaml_text = _fetch_metadata_yaml(dataset_name)
                    meta = _parse_metadata_yaml(yaml_text)
                    start_sec = meta["start_sec"]
                    t1_str, t2_str = params["timeRangeHint"]
                    today = date.fromtimestamp(start_sec)
                    t1_time = datetime.strptime(t1_str, "%H%M%S").time()
                    t2_time = datetime.strptime(t2_str, "%H%M%S").time()
                    abs_start = datetime.combine(today, t1_time).timestamp()
                    abs_end = datetime.combine(today, t2_time).timestamp()
                    params["trimStart"] = abs_start - start_sec
                    params["trimEnd"] = abs_end - start_sec
                    params["startSec"] = start_sec
                except Exception as e:
                    raise ValueError(f"Could not recover trim window for {dataset_name}: {e}") from e
                params.pop("timeRangeHint", None)

            if stored_keys:
                keys_to_dl = stored_keys
                suffix = _build_folder_suffix(params)
                folder_name = dataset_name + suffix
                dest_dir = DOWNLOAD_BASE / folder_name
                host_dest = f"{DOWNLOAD_DISPLAY}/{folder_name}"
            else:
                keys_to_dl, dest_dir, host_dest = _resolve_keys(dataset_name, params)
                _update_queue_item(item_id, keys=keys_to_dl)
            dest_dir.mkdir(parents=True, exist_ok=True)
            save_state(dest_dir, dataset_name, params, keys_to_dl)
        except Exception as e:
            _update_queue_item(item_id, status="error", error_msg=str(e))
            logger.error(f"[dl-worker] resolve failed for {dataset_name}: {e}")
            continue

        _update_queue_item(
            item_id, status="downloading", total=len(keys_to_dl),
            dest=host_dest, current_file="",
        )
        _dl_cancel.clear()

        downloaded = skipped = error_count = 0
        was_cancelled = False
        network_failed = False

        for s3_key in keys_to_dl:
            if _dl_cancel.is_set():
                was_cancelled = True
                break

            filename = s3_key.split("/")[-1]
            _update_queue_item(item_id, current_file=filename)
            local_path = dest_dir / filename

            if local_path.exists() and local_path.stat().st_size > 0:
                skipped += 1
                _update_queue_item(item_id, downloaded=downloaded, skipped=skipped, errors=error_count)
                continue

            try:
                download_s3_file(s3_key, str(local_path))
                downloaded += 1
            except Exception as e:
                error_count += 1
                logger.warning(f"[dl] fail {filename}: {e}")

                _update_queue_item(item_id, current_file="waiting for network...")
                for _ in range(10):
                    if _dl_cancel.is_set():
                        break
                    time.sleep(1)
                if _dl_cancel.is_set():
                    was_cancelled = True
                    break

                try:
                    download_s3_file(s3_key, str(local_path))
                    downloaded += 1
                except Exception:
                    logger.error(f"[dl] retry failed for {filename}, stopping")
                    network_failed = True
                    break

            _update_queue_item(item_id, downloaded=downloaded, skipped=skipped, errors=error_count)

        if was_cancelled:
            _update_queue_item(item_id, status="stopped", current_file="")
            logger.info(f"[dl-worker] stopped {dataset_name} ({downloaded} dl, {skipped} skip)")
        elif network_failed:
            _update_queue_item(
                item_id, status="stopped", current_file="",
                error_msg=f"Network error after {downloaded} dl, {skipped} skip — resume to continue",
            )
            logger.warning(f"[dl-worker] network fail {dataset_name}: {downloaded} dl, {skipped} skip, {error_count} err")
        elif downloaded == 0 and skipped == 0:
            _update_queue_item(
                item_id, status="error", current_file="",
                error_msg=f"All {error_count} files failed (network?)"
            )
            logger.error(f"[dl-worker] all failed for {dataset_name}: {error_count} errors")
        else:
            suffix = _build_folder_suffix(params)
            has_lcro = any(params.get(c) for c in ["lidars", "cameras", "radars", "other"])
            has_lcr_no_other = (
                any(params.get(c) for c in ["lidars", "cameras", "radars"])
                and not params.get("other")
            )

            try:
                # Errors in renaming/trimming must not produce a completed task.
                if suffix and has_lcro:
                    _rename_for_ros2(dest_dir, dataset_name, dataset_name + suffix)

                trim_start = params.get("trimStart")
                trim_end = params.get("trimEnd")
                start_sec = params.get("startSec")
                if has_lcro and trim_start is not None and trim_end is not None and start_sec is not None:
                    _update_queue_item(item_id, current_file="trimming...")
                    result = trim_bag(str(dest_dir), int((start_sec + trim_start) * 1e9),
                                      int((start_sec + trim_end) * 1e9))
                    if "error" in result:
                        raise ValueError(result["error"])
                    logger.info(f"[dl-worker] trim done for {dataset_name}: {result}")
                elif has_lcr_no_other:
                    _inject_oneshot_from_zero(dest_dir)
                    result = reindex_metadata(dest_dir)
                    if "error" in result:
                        raise ValueError(result["error"])

                mark_complete(dest_dir)
            except Exception as e:
                _update_queue_item(item_id, status="error", current_file="", error_msg=str(e))
                logger.exception(f"[dl-worker] finalization failed for {dataset_name}")
            else:
                _update_queue_item(item_id, status="done", current_file="", error_msg=None)
                logger.info(f"[dl-worker] done {dataset_name} ({downloaded} dl, {skipped} skip, {error_count} err)")
        _dl_cancel.clear()

# --- Recovery ---

def _recover_incomplete_downloads():
    """Scan DOWNLOAD_BASE for dataset folders, add to queue (no S3 calls)."""
    global _queue_id_counter
    if not DOWNLOAD_BASE.exists():
        return 0

    recovered = 0
    for entry in sorted(DOWNLOAD_BASE.iterdir()):
        if not entry.is_dir():
            continue

        state = read_state(entry)
        parsed = _parse_folder_suffix(entry.name)
        if not parsed and not state:
            continue

        dataset_name = state["dataset_name"] if state else parsed["dataset_name"]

        # Skip if already in queue
        with _lock:
            already = any(q["dest"].endswith(entry.name) for q in _queue)
        if already:
            continue

        if state:
            params = state["params"]
        else:
            params = dict(parsed["cats"])
            if parsed.get("time_range"):
                params["timeRangeHint"] = list(parsed["time_range"])

        if is_complete(entry):
            status = "done"
            error_msg = None
        else:
            status = "stopped"
            error_msg = "Recovered from disk"

        with _lock:
            _queue_id_counter += 1
            _queue.append({
                "id": _queue_id_counter, "dataset_name": dataset_name,
                "params": params, "status": status,
                "downloaded": 0, "skipped": 0, "errors": 0, "total": 0,
                "current_file": "",
                "dest": f"{DOWNLOAD_DISPLAY}/{entry.name}",
                "error_msg": error_msg, "keys": state["keys"] if state else None,
            })
        recovered += 1

    if recovered:
        logger.info(f"[recovery] Found {recovered} downloads on disk")
    return recovered

# --- FastAPI app ---

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="DataManager")

@app.on_event("startup")
def startup():
    load_aws_credentials()
    logger.info(f"[startup] S3: {S3_ENDPOINT} / {S3_BUCKET}")
    init_s3_client()

    def bg_init():
        try:
            sync_index_only()
            with _lock:
                count = len(_datasets)
            logger.info(f"[startup] Index ready: {count} bags")
        except Exception as e:
            logger.error(f"[startup] Index sync failed: {e}")
        # State files allow disk recovery even while S3 is unavailable.
        _recover_incomplete_downloads()

    threading.Thread(target=bg_init, daemon=True).start()
    threading.Thread(target=_dl_worker, daemon=True).start()

# -- Health --

_health_s3_client = None

def _get_health_s3_client():
    global _health_s3_client
    if _health_s3_client is None:
        _health_s3_client = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='us-east-1',
            config=Config(
                connect_timeout=3, read_timeout=3,
                retries={'max_attempts': 0},
                max_pool_connections=1,
                s3={'addressing_style': 'path'},
            ),
            use_ssl=S3_ENDPOINT.startswith('https://')
        )
    return _health_s3_client

@app.get("/api/health")
def health_check():
    s3_ok = False
    s3_err = None
    try:
        _get_health_s3_client().head_bucket(Bucket=S3_BUCKET)
        s3_ok = True
    except Exception as e:
        s3_err = str(e)
    with _lock:
        bag_count = len(_datasets)
    body = {"backend": "ok", "s3_endpoint": S3_ENDPOINT, "s3_connected": s3_ok, "datasets_in_cache": bag_count}
    if s3_err:
        body["s3_error"] = s3_err
    return JSONResponse(content=body, status_code=200 if s3_ok else 503)

@app.post("/api/datasets/refresh")
def refresh_datasets_cache():
    with _lock:
        count = len(_datasets)
    threading.Thread(target=sync_index_only, daemon=True).start()
    return {"ok": True, "message": f"Index sync started, currently {count} datasets in cache"}

# -- Datasets --

@app.get("/api/datasets")
def list_datasets(search: str = "", page: int = 1):
    with _lock:
        all_datasets = list(_datasets.items())

    if search:
        s = search.lower()
        all_datasets = [(k, v) for k, v in all_datasets if s in k.lower()]

    all_datasets.sort(key=lambda x: (x[1].get("date") or "", x[0]), reverse=True)

    total = len(all_datasets)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(page, pages))
    offset = (page - 1) * PAGE_SIZE
    page_datasets = all_datasets[offset:offset + PAGE_SIZE]

    datasets = []
    for dataset_id, dataset in page_datasets:
        d = {
            "id": dataset_id,
            "name": dataset_id,
            "date": dataset.get("date"),
            "device_id": dataset.get("device_id"),
            "s3_base_path": dataset.get("s3_base_path"),
            "dataset_suffix": dataset.get("dataset_suffix"),
            "size_bytes": dataset.get("size_bytes", 0),
            "files": {k: v for k, v in dataset.get("files", {}).items()},
        }
        if "cat_sizes" in dataset:
            d["files"]["cat_sizes"] = dataset["cat_sizes"]
        datasets.append(d)
    return {"datasets": datasets, "total": total, "page": page, "pages": pages}

@app.get("/api/datasets/{dataset_name}/metadata")
def get_dataset_metadata(dataset_name: str):
    yaml_text = _fetch_metadata_yaml(dataset_name)
    result = _parse_metadata_yaml(yaml_text)
    result["dataset_name"] = dataset_name
    return result

@app.post("/api/datasets/{dataset_name}/scan")
def scan_single_dataset(dataset_name: str):
    with _lock:
        dataset = _datasets.get(dataset_name)
    if not dataset:
        raise HTTPException(404, f"Dataset not found: {dataset_name}")

    resolved = _live_resolve_bag_keys(dataset_name, dataset["s3_base_path"], dataset["dataset_suffix"])
    _cache_live_resolve(dataset_name, resolved)

    keys = resolved["keys"]
    return {
        "dataset_name": dataset_name,
        "size_bytes": resolved["total_size"],
        "files": {cat: len(keys.get(cat, [])) for cat in ["lidars", "cameras", "radars", "other", "topics", "meta"]},
        "cat_sizes": resolved["cat_sizes"],
    }

# -- Download path --

@app.get("/api/download-path")
def get_download_path():
    return {"path": DOWNLOAD_DISPLAY}

@app.put("/api/download-path")
def set_download_path(req: dict):
    global DOWNLOAD_BASE, DOWNLOAD_DISPLAY
    new_path = req.get("path", "").strip()
    if not new_path:
        raise HTTPException(400, "Path cannot be empty")

    _host_home_mount = Path(os.getenv("HOST_HOME_MOUNT", "/home/appuser/host_home"))

    if new_path.startswith("~/"):
        p = _host_home_mount / new_path[2:]
        display = new_path
    elif new_path.startswith("/"):
        p = Path(new_path)
        display = new_path
    else:
        p = DOWNLOAD_BASE / new_path
        display = f"{DOWNLOAD_DISPLAY}/{new_path}"

    try:
        p.mkdir(parents=True, exist_ok=True)
        test_file = p / ".datamanager_write_test"
        test_file.write_text("ok")
        test_file.unlink()
    except PermissionError:
        raise HTTPException(403, f"No write permission: {display}")
    except Exception as e:
        raise HTTPException(400, f"Cannot create folder {display}: {e}")

    DOWNLOAD_BASE = p
    DOWNLOAD_DISPLAY = display
    logger.info(f"[config] Download path: {p} (display: {display})")
    return {"path": display, "resolved": str(p)}

# -- Queue API --

class QueueAddRequest(BaseModel):
    items: List[Dict]

@app.get("/api/queue")
def get_queue():
    with _lock:
        return [
            {k: v for k, v in item.items() if k != "keys"}
            for item in _queue
        ]

@app.post("/api/queue")
def add_to_queue(req: QueueAddRequest):
    global _queue_id_counter
    added = []
    with _lock:
        for entry in req.items:
            _queue_id_counter += 1
            dataset_name = entry.get("dataset_name", "")
            params = entry.get("params", {})
            suffix = _build_folder_suffix(params)
            folder_name = dataset_name + suffix
            _queue.append({
                "id": _queue_id_counter,
                "dataset_name": dataset_name,
                "params": params,
                "status": "pending",
                "downloaded": 0,
                "skipped": 0,
                "errors": 0,
                "total": 0,
                "current_file": "",
                "dest": f"{DOWNLOAD_DISPLAY}/{folder_name}",
                "error_msg": None,
                "keys": None,
            })
            added.append(_queue_id_counter)
    return {"added": added}

@app.post("/api/queue/{item_id}/stop")
def stop_queue_item(item_id: int):
    with _lock:
        item = next((i for i in _queue if i["id"] == item_id), None)
    if not item:
        raise HTTPException(404)
    if item["status"] == "downloading":
        _dl_cancel.set()
        return {"ok": True, "message": "cancelling"}
    elif item["status"] == "pending":
        _update_queue_item(item_id, status="stopped")
        return {"ok": True}
    raise HTTPException(400, f"Cannot stop item with status: {item['status']}")

@app.post("/api/queue/{item_id}/resume")
def resume_queue_item(item_id: int):
    with _lock:
        item = next((i for i in _queue if i["id"] == item_id), None)
    if not item:
        raise HTTPException(404)
    if item["status"] not in ("stopped", "error"):
        raise HTTPException(400, f"Can only resume stopped/error items, got: {item['status']}")
    _update_queue_item(
        item_id, status="pending", downloaded=0, skipped=0,
        errors=0, total=0, current_file="", error_msg=None,
    )
    return {"ok": True}

@app.delete("/api/queue/done")
def clear_done_items():
    with _lock:
        before = len(_queue)
        _queue[:] = [item for item in _queue if item["status"] not in ("done", "error")]
        removed = before - len(_queue)
    return {"ok": True, "removed": removed}

@app.delete("/api/queue/{item_id}")
def remove_queue_item(item_id: int):
    with _lock:
        item = next((i for i in _queue if i["id"] == item_id), None)
        if not item:
            raise HTTPException(404)
        if item["status"] == "downloading":
            raise HTTPException(400, "Cannot remove while downloading. Stop it first.")
        _queue.remove(item)
    return {"ok": True}

@app.post("/api/queue/rescan")
def rescan_downloads():
    recovered = _recover_incomplete_downloads()
    return {"ok": True, "recovered": recovered}

# -- Static file serving (SPA) --

if STATIC_DIR.exists() and (STATIC_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets")), name="static-assets")

@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    file_path = STATIC_DIR / full_path
    if full_path and file_path.is_file():
        return FileResponse(file_path)
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"message": "DataManager API", "docs": "/docs"}
