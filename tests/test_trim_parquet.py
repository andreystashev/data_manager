import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
import trim_parquet as trimmer


class TrimTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name)
        self.topics = self.folder / "bag_topics.parquet"
        pq.write_table(pa.table({
            "topic_id": pa.array([1, 2], type=pa.uint16()),
            "topic_name": ["/sensor", "/tf_static"],
            "message_type": ["sensor_msgs/PointCloud2", "tf2_msgs/TFMessage"],
            "serialization_format": ["cdr", "cdr"],
            "offered_qos_profiles": ["", "- durability: 1\n"],
        }), self.topics)
        (self.folder / "metadata.yaml").write_text("original metadata\n")

    def data(self, name, timestamps, topics=None):
        table = pa.Table.from_pydict({
            "id": list(range(len(timestamps))),
            "topic_id": topics or [1] * len(timestamps),
            "timestamp": timestamps,
            "data": [f"row-{i}" for i in range(len(timestamps))],
        }, schema=trimmer.DATA_SCHEMA)
        pq.write_table(table, self.folder / name)

    def timestamps(self, name):
        return pq.read_table(self.folder / name).column("timestamp").to_pylist()

    def metadata(self):
        return yaml.safe_load((self.folder / "metadata.yaml").read_text())["rosbag2_bagfile_information"]

    def test_overlapping_sensor_files_are_all_trimmed(self):
        self.data("bag_lidars_0.parquet", [10, 20, 30, 40])
        self.data("bag_cameras_0.parquet", [10, 25, 35, 45])
        result = trimmer.trim_bag(str(self.folder), 20, 35)
        self.assertEqual(self.timestamps("bag_lidars_0.parquet"), [20, 30])
        self.assertEqual(self.timestamps("bag_cameras_0.parquet"), [25, 35])
        self.assertEqual(result["messages_total"], 4)
        self.assertEqual(self.metadata()["message_count"], 4)

    def test_unsorted_rows_and_inclusive_boundaries(self):
        self.data("bag_lidars_0.parquet", [50, 20, 10, 30])
        trimmer.trim_bag(str(self.folder), 20, 30)
        self.assertEqual(self.timestamps("bag_lidars_0.parquet"), [20, 30])

    def test_single_timestamp_window(self):
        self.data("bag_lidars_0.parquet", [10, 20, 30])
        trimmer.trim_bag(str(self.folder), 20, 20)
        self.assertEqual(self.timestamps("bag_lidars_0.parquet"), [20])
        self.assertEqual(self.metadata()["duration"]["nanoseconds"], 0)

    def test_oneshot_injected_when_only_end_boundary_file_remains(self):
        self.data("bag_other_0.parquet", [1], [2])
        self.data("bag_lidars_1.parquet", [25, 40])
        trimmer.trim_bag(str(self.folder), 20, 30)
        self.assertEqual(self.timestamps("bag_lidars_1.parquet"), [19, 25])
        self.assertFalse((self.folder / "bag_other_0.parquet").exists())
        meta = self.metadata()
        self.assertEqual(meta["message_count"], 2)
        self.assertEqual(meta["starting_time"]["nanoseconds_since_epoch"], 19)
        self.assertEqual(meta["topics_with_message_count"][1]["message_count"], 1)
        self.assertEqual(meta["topics_with_message_count"][1]["topic_metadata"]["offered_qos_profiles"], "- durability: 1\n")

    def test_oneshot_inside_window_is_not_duplicated(self):
        self.data("bag_other_0.parquet", [20, 25], [2, 1])
        trimmer.trim_bag(str(self.folder), 20, 30)
        self.assertEqual(self.timestamps("bag_other_0.parquet"), [20, 25])

    def test_future_oneshot_is_not_backdated(self):
        self.data("bag_other_0.parquet", [50], [2])
        self.data("bag_lidars_1.parquet", [25])
        trimmer.trim_bag(str(self.folder), 20, 30)
        self.assertEqual(self.timestamps("bag_lidars_1.parquet"), [25])

    def test_empty_selection_does_not_modify_input(self):
        self.data("bag_lidars_0.parquet", [10, 20])
        original = {p.name: p.read_bytes() for p in self.folder.iterdir()}
        with self.assertRaisesRegex(ValueError, "No messages"):
            trimmer.trim_bag(str(self.folder), 100, 200)
        self.assertEqual({p.name: p.read_bytes() for p in self.folder.iterdir()}, original)

    def test_invalid_window_does_not_modify_input(self):
        self.data("bag_lidars_0.parquet", [10, 20])
        original = (self.folder / "bag_lidars_0.parquet").read_bytes()
        with self.assertRaises(ValueError):
            trimmer.trim_bag(str(self.folder), 20, 10)
        self.assertEqual((self.folder / "bag_lidars_0.parquet").read_bytes(), original)

    def test_staging_write_failure_leaves_originals(self):
        self.data("bag_lidars_0.parquet", [10, 20])
        original = {p.name: p.read_bytes() for p in self.folder.iterdir()}
        with patch.object(trimmer, "_write_parquet", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                trimmer.trim_bag(str(self.folder), 10, 20)
        self.assertEqual({p.name: p.read_bytes() for p in self.folder.iterdir()}, original)

    def test_reindex_uses_actual_min_max_and_preserves_backup(self):
        self.data("bag_lidars_0.parquet", [30, 10, 20])
        trimmer.reindex_metadata(self.folder)
        self.assertEqual(self.metadata()["starting_time"]["nanoseconds_since_epoch"], 10)
        self.assertEqual(self.metadata()["duration"]["nanoseconds"], 20)
        trimmer.reindex_metadata(self.folder)
        self.assertEqual((self.folder / "metadata.yaml.orig").read_text(), "original metadata\n")
