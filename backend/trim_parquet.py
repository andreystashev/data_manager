"""
Parquet dataset trimmer + reindexer.

Trims a downloaded ROS2 parquet dataset to a time window and rebuilds metadata.yaml
so the result is playable with `ros2 bag play`. No ROS2 installation required.

Pipeline:
  1. Find which parquet files contain trim_start / trim_end timestamps
  2. Slice the boundary files (keep only rows within the time window)
  3. Inject one-shot topics (maps, tf_static, route, etc.) from _0.parquet into the start file
  4. Leave intermediate files untouched
  5. Remove out-of-range files
  6. Regenerate metadata.yaml from actual parquet contents
"""

import logging
import os
import re
import shutil
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("trim_parquet")

# One-shot / transient_local topics that must be preserved from the first parquet file.
# These are published once at bag start and required for correct playback.
# List derived from actual _0.parquet analysis (topics with 1-2 messages)
# and cross-checked with component_interface_specs (DURABILITY_TRANSIENT_LOCAL).
ONESHOT_TOPICS = frozenset([
    "/map/vector_map",
    "/map/pointcloud_map",
    "/map/pointcloud_map_metadata",
    "/map/vector_map_marker",
    "/map/map_projector_type",
    "/tf_static",
    "/robot_description",
    "/planning/mission_planning/route",
    "/planning/mission_planning/route_state",
    "/planning/mission_planning/normal_route",
    "/planning/mission_planning/mrm_route",
    "/planning/mission_planning/route_marker",
    "/planning/mission_planning/echo_back_goal_pose",
    "/planning/mission_planning/debug/goal_footprint",
    "/planning/scenario_planning/max_velocity_default",
    "/planning/smoother/setpoint_external_acceleration_limit",
    "/planning/smoother/setpoint_external_acceleration_limit_state",
    "/api/routing/route",
    "/api/routing/state",
    "/api/operation_mode/state",
    "/api/localization/initialization_state",
    "/api/fail_safe/mrm_state",
    "/system/operation_mode/state",
    "/localization/initialization_state",
    "/vehicle/wheel_radius_corrector/initial_condition",
    "/vehicle/wheel_radius_corrector/dbg",
    "/vehicle/route_number_report",
    "/bag_additional_reporter/git_information",
    "/planning/scenario_planning/max_velocity_candidates",
    "/planning/mission_planning/device_stops_timetable",
])

# Parquet schema for data files (must match ROS2 parquet storage plugin)
DATA_SCHEMA = pa.schema([
    ("id", pa.uint32()),
    ("topic_id", pa.uint16()),
    ("timestamp", pa.int64()),
    ("data", pa.string()),
])


def _list_data_parquets(dataset_dir: Path) -> list[Path]:
    """List numbered data parquet files sorted by index."""
    pattern = re.compile(r"_(\d+)\.parquet$")
    files = []
    for f in dataset_dir.iterdir():
        m = pattern.search(f.name)
        if m and "_topics" not in f.name:
            files.append((int(m.group(1)), f))
    files.sort(key=lambda x: x[0])
    return [f for _, f in files]


def _find_topics_parquet(dataset_dir: Path) -> Optional[Path]:
    for f in dataset_dir.iterdir():
        if f.name.endswith("_topics.parquet"):
            return f
    return None


def _get_oneshot_topic_ids(topics_path: Path) -> set[int]:
    """Read _topics.parquet and return topic_ids for one-shot/transient_local topics."""
    table = pq.read_table(topics_path)
    ids = set()
    names = table.column("topic_name").to_pylist()
    topic_ids = table.column("topic_id").to_pylist()
    for tid, name in zip(topic_ids, names):
        if name in ONESHOT_TOPICS:
            ids.add(tid)
    return ids


def _read_table(path: Path) -> pa.Table:
    return pq.read_table(path).cast(DATA_SCHEMA)


def _get_ts_range(table: pa.Table) -> tuple[int, int]:
    """Return (min_timestamp, max_timestamp) from a data parquet table."""
    ts = table.column("timestamp")
    return ts[0].as_py(), ts[-1].as_py()


def _filter_by_timestamp(table: pa.Table, ts_start: int, ts_end: int) -> pa.Table:
    """Keep rows where ts_start <= timestamp <= ts_end."""
    ts = table.column("timestamp")
    mask = pa.compute.and_(
        pa.compute.greater_equal(ts, ts_start),
        pa.compute.less_equal(ts, ts_end),
    )
    return table.filter(mask)


def _reindex_ids(table: pa.Table, start_id: int = 0) -> pa.Table:
    """Replace the 'id' column with sequential integers starting from start_id."""
    n = len(table)
    new_ids = pa.array(range(start_id, start_id + n), type=pa.uint32())
    return table.set_column(
        table.schema.get_field_index("id"),
        "id",
        new_ids,
    )


def _write_parquet(table: pa.Table, path: Path):
    """Write a parquet table with snappy compression."""
    pq.write_table(
        table, path,
        compression="snappy",
        row_group_size=1000,
        use_dictionary=False,
    )


def trim_bag(dataset_dir: str, trim_start_ns: int, trim_end_ns: int) -> dict:
    """
    Trim a parquet dataset in-place to the given nanosecond time window.

    Args:
        dataset_dir: Path to the downloaded dataset folder
        trim_start_ns: Start timestamp in nanoseconds since epoch
        trim_end_ns: End timestamp in nanoseconds since epoch

    Returns:
        dict with stats: files_before, files_after, messages_total
    """
    bag_path = Path(dataset_dir)
    data_files = _list_data_parquets(bag_path)

    if not data_files:
        logger.warning(f"[trim] No data parquets found in {dataset_dir}")
        return {"error": "no data parquets"}

    topics_path = _find_topics_parquet(bag_path)
    oneshot_ids = _get_oneshot_topic_ids(topics_path) if topics_path else set()

    logger.info(
        f"[trim] Trimming {dataset_dir}: {len(data_files)} files, "
        f"window {trim_start_ns}-{trim_end_ns}, "
        f"{len(oneshot_ids)} one-shot topic IDs"
    )

    # Classify files: before, start-boundary, middle, end-boundary, after
    files_before = []    # entirely before window — delete
    files_after = []     # entirely after window — delete
    files_middle = []    # entirely within window — keep as-is
    start_file = None    # contains trim_start — slice
    end_file = None      # contains trim_end — slice
    zero_file = data_files[0] if data_files else None

    for f in data_files:
        table = _read_table(f)
        if len(table) == 0:
            files_before.append(f)
            continue
        f_start, f_end = _get_ts_range(table)

        if f_end < trim_start_ns:
            files_before.append(f)
        elif f_start > trim_end_ns:
            files_after.append(f)
        elif f_start < trim_start_ns and f_end >= trim_start_ns:
            start_file = f
        elif f_start <= trim_end_ns and f_end > trim_end_ns:
            end_file = f
        else:
            files_middle.append(f)

    # If start or end timestamp falls within a single file
    if start_file and end_file and start_file == end_file:
        end_file = None  # will handle both cuts in start_file

    # Extract one-shot rows from _0.parquet (before deleting it)
    oneshot_rows = None
    if zero_file and oneshot_ids:
        zero_table = _read_table(zero_file)
        tid_col = zero_table.column("topic_id")
        mask = pa.array([t.as_py() in oneshot_ids for t in tid_col])
        oneshot_rows = zero_table.filter(mask)
        if len(oneshot_rows) > 0:
            # Set timestamps to just before trim_start so they sort first
            n = len(oneshot_rows)
            fake_ts = pa.array(
                [trim_start_ns - n + i for i in range(n)],
                type=pa.int64(),
            )
            oneshot_rows = oneshot_rows.set_column(
                oneshot_rows.schema.get_field_index("timestamp"),
                "timestamp",
                fake_ts,
            )
            logger.info(f"[trim] Extracted {n} one-shot rows from {zero_file.name}")
        else:
            oneshot_rows = None

    # Process start file
    if start_file:
        table = _read_table(start_file)
        if end_file is None and start_file not in files_middle:
            # Both trim_start and trim_end are in this file
            table = _filter_by_timestamp(table, trim_start_ns, trim_end_ns)
        else:
            table = _filter_by_timestamp(table, trim_start_ns, 2**62)

        if oneshot_rows is not None and len(oneshot_rows) > 0:
            table = pa.concat_tables([oneshot_rows, table])
            oneshot_rows = None  # consumed

        table = _reindex_ids(table)
        _write_parquet(table, start_file)
        logger.info(f"[trim] Sliced start file {start_file.name}: {len(table)} rows")

    # If no start_file but we have one-shot rows, inject into first middle file
    if oneshot_rows is not None and len(oneshot_rows) > 0 and files_middle:
        first_middle = files_middle[0]
        table = _read_table(first_middle)
        table = pa.concat_tables([oneshot_rows, table])
        table = _reindex_ids(table)
        _write_parquet(table, first_middle)
        logger.info(f"[trim] Injected one-shot rows into {first_middle.name}")

    # Process end file
    if end_file:
        table = _read_table(end_file)
        table = _filter_by_timestamp(table, 0, trim_end_ns)
        table = _reindex_ids(table)
        _write_parquet(table, end_file)
        logger.info(f"[trim] Sliced end file {end_file.name}: {len(table)} rows")

    # Delete out-of-range files (but keep _0 if it's the start_file or middle)
    keep_set = set()
    if start_file:
        keep_set.add(start_file)
    if end_file:
        keep_set.add(end_file)
    keep_set.update(files_middle)

    deleted = 0
    for f in files_before + files_after:
        if f not in keep_set:
            f.unlink()
            deleted += 1

    logger.info(f"[trim] Deleted {deleted} out-of-range files")

    # Reindex: regenerate metadata.yaml
    result = reindex_metadata(bag_path)
    result["files_before"] = len(data_files)
    result["files_deleted"] = deleted
    return result


def reindex_metadata(dataset_dir: Path) -> dict:
    """
    Regenerate metadata.yaml by reading actual parquet file contents.
    Replaces `ros2 bag reindex -s parquet` — no ROS2 needed.
    """
    data_files = _list_data_parquets(dataset_dir)
    topics_path = _find_topics_parquet(dataset_dir)

    if not data_files:
        logger.warning(f"[reindex] No data parquets in {dataset_dir}")
        return {"error": "no data parquets"}

    # Read topics metadata
    topics_with_count: dict[int, dict] = {}
    if topics_path:
        tt = pq.read_table(topics_path)
        for i in range(len(tt)):
            tid = tt.column("topic_id")[i].as_py()
            topics_with_count[tid] = {
                "name": tt.column("topic_name")[i].as_py(),
                "type": tt.column("message_type")[i].as_py(),
                "serialization_format": tt.column("serialization_format")[i].as_py(),
                "offered_qos_profiles": tt.column("offered_qos_profiles")[i].as_py(),
                "message_count": 0,
            }

    # Scan all data files for per-file stats and topic counts
    global_min_ts = None
    global_max_ts = None
    total_messages = 0
    file_entries = []

    for f in data_files:
        table = pq.read_table(f, columns=["timestamp", "topic_id"])
        n = len(table)
        if n == 0:
            continue

        ts_col = table.column("timestamp")
        f_min = ts_col[0].as_py()
        f_max = ts_col[-1].as_py()

        if global_min_ts is None or f_min < global_min_ts:
            global_min_ts = f_min
        if global_max_ts is None or f_max > global_max_ts:
            global_max_ts = f_max

        total_messages += n

        # Count topics per file
        for tid in table.column("topic_id").to_pylist():
            if tid in topics_with_count:
                topics_with_count[tid]["message_count"] += 1

        file_entries.append({
            "path": f.name,
            "starting_time_ns": f_min,
            "duration_ns": f_max - f_min,
            "message_count": n,
        })

    if global_min_ts is None:
        return {"error": "all files empty"}

    duration_ns = global_max_ts - global_min_ts

    # Build metadata.yaml
    lines = []
    lines.append("rosbag2_bagfile_information:")
    lines.append("  version: 5")
    lines.append("  storage_identifier: parquet")
    lines.append("  duration:")
    lines.append(f"    nanoseconds: {duration_ns}")
    lines.append("  starting_time:")
    lines.append(f"    nanoseconds_since_epoch: {global_min_ts}")
    lines.append(f"  message_count: {total_messages}")

    # Topics with message counts
    lines.append("  topics_with_message_count:")
    for tid in sorted(topics_with_count.keys()):
        t = topics_with_count[tid]
        lines.append("    - topic_metadata:")
        lines.append(f'        name: {t["name"]}')
        lines.append(f'        type: {t["type"]}')
        lines.append(f'        serialization_format: {t["serialization_format"]}')
        qos = t["offered_qos_profiles"] or ""
        # QoS is a YAML string that must stay on one line with literal \n
        qos_escaped = qos.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        lines.append(f'        offered_qos_profiles: "{qos_escaped}"')
        lines.append(f'      message_count: {t["message_count"]}')

    lines.append('  compression_format: ""')
    lines.append('  compression_mode: ""')

    # Relative file paths
    lines.append("  relative_file_paths:")
    for fe in file_entries:
        lines.append(f'    - {fe["path"]}')

    # Per-file info
    lines.append("  files:")
    for fe in file_entries:
        lines.append(f'    - path: {fe["path"]}')
        lines.append("      starting_time:")
        lines.append(f'        nanoseconds_since_epoch: {fe["starting_time_ns"]}')
        lines.append("      duration:")
        lines.append(f'        nanoseconds: {fe["duration_ns"]}')
        lines.append(f'      message_count: {fe["message_count"]}')

    yaml_content = "\n".join(lines) + "\n"

    meta_path = dataset_dir / "metadata.yaml"
    if meta_path.exists():
        backup = dataset_dir / "metadata.yaml.orig"
        if not backup.exists():
            shutil.copy2(meta_path, backup)

    meta_path.write_text(yaml_content, encoding="utf-8")
    logger.info(
        f"[reindex] Wrote metadata.yaml: {total_messages} messages, "
        f"{len(file_entries)} files, {duration_ns / 1e9:.1f}s duration"
    )

    return {
        "files_after": len(file_entries),
        "messages_total": total_messages,
        "duration_sec": round(duration_ns / 1e9, 2),
    }
