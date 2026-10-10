"""
Parquet dataset trimmer + reindexer.

Trims a downloaded ROS2 parquet dataset to a time window and rebuilds metadata.yaml
so the result is playable with `ros2 bag play`. No ROS2 installation required.

Pipeline:
  1. Filter every sensor file to the inclusive time window
  2. Preserve historical one-shot topics from category _0 files
  3. Stage filtered files and regenerated metadata before replacing originals
  4. Remove out-of-range files after successful staging
"""

import logging
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
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
        if f.is_file() and m and "_topics" not in f.name:
            files.append((int(m.group(1)), f))
    files.sort(key=lambda x: (x[0], x[1].name))
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
    return pc.min(ts).as_py(), pc.max(ts).as_py()


def _filter_by_timestamp(table: pa.Table, ts_start: int, ts_end: int) -> pa.Table:
    """Keep rows where ts_start <= timestamp <= ts_end."""
    ts = table.column("timestamp")
    mask = pc.and_(
        pc.greater_equal(ts, ts_start),
        pc.less_equal(ts, ts_end),
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


def _sensor_category(path: Path) -> str:
    match = re.search(r"_(lidars|cameras|radars|other)_\d+\.parquet$", path.name)
    return match.group(1) if match else "other"


def select_sensor_categories(dataset_dir: str, categories: set[str]) -> dict:
    """Filter categories without a time cut, preserving auxiliary one-shot topics."""
    return trim_bag(dataset_dir, -(2**63), 2**63 - 1, selected_categories=categories)


def trim_bag(dataset_dir: str, trim_start_ns: int, trim_end_ns: int,
             *, selected_categories: Optional[set[str]] = None) -> dict:
    """
    Trim a parquet dataset in-place to the given nanosecond time window.

    Args:
        dataset_dir: Path to the downloaded dataset folder
        trim_start_ns: Start timestamp in nanoseconds since epoch
        trim_end_ns: End timestamp in nanoseconds since epoch
        selected_categories: Optional sensor categories to retain; auxiliary _0
            files contribute only one-shot messages, never ordinary sensor rows.

    Returns:
        dict with stats: files_before, files_after, messages_total
    """
    if trim_start_ns > trim_end_ns:
        raise ValueError("Trim start must not be after trim end")
    if selected_categories is not None:
        selected_categories = set(selected_categories)
        if not selected_categories or not selected_categories <= {"lidars", "cameras", "radars", "other"}:
            raise ValueError("Select at least one valid sensor category")
    bag_path = Path(dataset_dir)
    data_files = _list_data_parquets(bag_path)
    if not data_files:
        raise ValueError("No data parquets found")

    topics_path = _find_topics_parquet(bag_path)
    if selected_categories is not None and not topics_path:
        raise ValueError("Topics parquet is required to filter sensor categories")
    oneshot_ids = _get_oneshot_topic_ids(topics_path) if topics_path else set()
    historical = []
    # Each sensor category can have its own _0 file. Preserve only historical
    # context from selected files. Auxiliary files also supply in-window
    # one-shot messages, since their ordinary rows will be excluded.
    for path in data_files:
        if path.name.endswith("_0.parquet") and oneshot_ids:
            table = _read_table(path)
            auxiliary = (selected_categories is not None
                         and _sensor_category(path) not in selected_categories)
            time_mask = (pc.less_equal(table.column("timestamp"), trim_end_ns) if auxiliary
                         else pc.less(table.column("timestamp"), trim_start_ns))
            mask = pc.and_(
                pc.is_in(table.column("topic_id"), value_set=pa.array(
                    sorted(oneshot_ids), type=pa.uint16())), time_mask,
            )
            rows = table.filter(mask)
            if len(rows):
                historical.append(rows)

    # Stage all output before changing originals. Invalid/corrupt input and
    # an empty selection leave the downloaded dataset and metadata untouched.
    with tempfile.TemporaryDirectory(prefix=".trim-", dir=bag_path) as tmp:
        staging = Path(tmp)
        retained = []
        for path in data_files:
            if selected_categories is not None and _sensor_category(path) not in selected_categories:
                continue
            table = _filter_by_timestamp(_read_table(path), trim_start_ns, trim_end_ns)
            if not len(table):
                continue
            table = table.sort_by([("timestamp", "ascending")])
            _write_parquet(_reindex_ids(table), staging / path.name)
            retained.append((table.column("timestamp")[0].as_py(), path.name))
        if not retained:
            raise ValueError("No messages in requested time window")

        if historical:
            _, first_name = min(retained)
            context = pa.concat_tables(historical).sort_by([("timestamp", "ascending")])
            timestamps = context.column("timestamp").to_pylist()
            historical_count = sum(ts < trim_start_ns for ts in timestamps)
            # Keep in-window auxiliary messages at their real timestamps.
            # Only historical context is moved immediately before the window.
            timestamps[:historical_count] = range(trim_start_ns - historical_count, trim_start_ns)
            context = context.set_column(
                context.schema.get_field_index("timestamp"), "timestamp",
                pa.array(timestamps, type=pa.int64()),
            )
            first = _read_table(staging / first_name)
            merged = pa.concat_tables([context, first]).sort_by([("timestamp", "ascending")])
            _write_parquet(_reindex_ids(merged), staging / first_name)

        if topics_path:
            shutil.copy2(topics_path, staging / topics_path.name)
        result = reindex_metadata(staging)
        if "error" in result:
            raise ValueError(result["error"])

        meta_path = bag_path / "metadata.yaml"
        backup = bag_path / "metadata.yaml.orig"
        if meta_path.exists() and not backup.exists():
            shutil.copy2(meta_path, backup)
        kept = {name for _, name in retained}
        for path in data_files:
            if path.name in kept:
                os.replace(staging / path.name, path)
            else:
                path.unlink()
        os.replace(staging / "metadata.yaml", meta_path)

    result["files_before"] = len(data_files)
    result["files_deleted"] = len(data_files) - len(retained)
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
        f_min = pc.min(ts_col).as_py()
        f_max = pc.max(ts_col).as_py()

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
