import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
import test_trim_parquet as fixtures
from unittest.mock import patch
import main_s3 as server
import download_state as state
import trim_parquet as trimmer


class SensorSelectionTests(unittest.TestCase):
    setUp = fixtures.TrimTests.setUp
    data = fixtures.TrimTests.data
    timestamps = fixtures.TrimTests.timestamps
    metadata = fixtures.TrimTests.metadata

    def process(self, categories, window=None):
        if window:
            return trimmer.trim_bag(str(self.folder), *window, selected_categories=categories)
        return trimmer.select_sensor_categories(str(self.folder), categories)

    def test_camera_zero_is_kept_and_only_auxiliary_zero_removed(self):
        # Creation order deliberately puts cameras_0 before other_0.
        self.data("bag_cameras_0.parquet", [20, 30])
        self.data("bag_other_0.parquet", [1, 2], [2, 1])
        self.process({"cameras"})
        self.assertEqual(self.timestamps("bag_cameras_0.parquet"), [1, 20, 30])
        self.assertFalse((self.folder / "bag_other_0.parquet").exists())
        self.assertEqual(self.metadata()["message_count"], 3)

    def test_selected_zero_is_kept_when_auxiliary_has_no_oneshots(self):
        self.data("bag_lidars_0.parquet", [20, 30])
        self.data("bag_other_0.parquet", [1, 2])
        self.process({"lidars"})
        self.assertEqual(self.timestamps("bag_lidars_0.parquet"), [20, 30])
        self.assertFalse((self.folder / "bag_other_0.parquet").exists())

    def test_trim_excludes_ordinary_other_rows_inside_window(self):
        self.data("bag_lidars_0.parquet", [20, 30, 40])
        self.data("bag_other_0.parquet", [1, 25, 27, 50], [2, 1, 2, 2])
        self.process({"lidars"}, (20, 35))
        self.assertEqual(self.timestamps("bag_lidars_0.parquet"), [19, 20, 27, 30])
        self.assertFalse((self.folder / "bag_other_0.parquet").exists())
        self.assertEqual(self.metadata()["message_count"], 4)

    def test_nonzero_excluded_sensor_files_are_removed(self):
        self.data("bag_lidars_0.parquet", [20])
        self.data("bag_cameras_1.parquet", [25])
        self.process({"lidars"})
        self.assertFalse((self.folder / "bag_cameras_1.parquet").exists())
        self.assertEqual(self.metadata()["relative_file_paths"], ["bag_lidars_0.parquet"])

    def test_all_selected_categories_keep_ordinary_other_data(self):
        self.data("bag_lidars_0.parquet", [20])
        self.data("bag_other_0.parquet", [25], [1])
        self.process({"lidars", "cameras", "radars", "other"}, (20, 30))
        self.assertEqual(self.timestamps("bag_other_0.parquet"), [25])

    def test_repeated_processing_does_not_duplicate_context(self):
        self.data("bag_lidars_0.parquet", [20, 30])
        self.data("bag_other_0.parquet", [1], [2])
        self.process({"lidars"})
        self.process({"lidars"})
        self.assertEqual(self.timestamps("bag_lidars_0.parquet"), [1, 20, 30])
        self.assertEqual(self.metadata()["message_count"], 3)

    def test_empty_selected_files_preserve_auxiliary_and_original_metadata(self):
        self.data("bag_cameras_0.parquet", [])
        self.data("bag_other_0.parquet", [1], [2])
        original = {p.name: p.read_bytes() for p in self.folder.iterdir()}
        with self.assertRaises(ValueError):
            self.process({"cameras"})
        self.assertEqual({p.name: p.read_bytes() for p in self.folder.iterdir()}, original)

    def test_numbered_legacy_auxiliary_file_is_supported(self):
        self.data("bag_radars_0.parquet", [20])
        self.data("bag_0.parquet", [1, 2], [2, 1])
        self.process({"radars"})
        self.assertEqual(self.timestamps("bag_radars_0.parquet"), [1, 20])
        self.assertFalse((self.folder / "bag_0.parquet").exists())

    def test_missing_topics_fails_before_modifying_files(self):
        self.data("bag_cameras_0.parquet", [20])
        self.topics.unlink()
        original = {p.name: p.read_bytes() for p in self.folder.iterdir()}
        with self.assertRaisesRegex(ValueError, "Topics parquet"):
            self.process({"cameras"})
        self.assertEqual({p.name: p.read_bytes() for p in self.folder.iterdir()}, original)

    def test_unknown_or_empty_categories_are_rejected(self):
        self.data("bag_cameras_0.parquet", [20])
        for categories in [set(), {"unknown"}]:
            with self.subTest(categories=categories), self.assertRaises(ValueError):
                self.process(categories)

    def test_worker_camera_only_download_keeps_camera_zero_and_updates_metadata(self):
        self.data("bag_cameras_0.parquet", [20, 30])
        self.data("bag_other_0.parquet", [1, 25], [2, 1])
        params = {"cameras": True, "topics": True, "meta": True}
        keys = ["messages/bag_cameras_0.parquet", "messages/bag_other_0.parquet",
                "topics/bag_topics.parquet"]
        queue = [{"id": 1, "dataset_name": "bag", "params": params,
                  "status": "pending", "keys": None}]
        original_iterdir = Path.iterdir
        def cameras_first(folder):
            # Exercise the unsafe old assumption independently of filesystem order.
            return iter(sorted(original_iterdir(folder),
                               key=lambda path: ("cameras" not in path.name, path.name)))
        with patch.object(Path, "iterdir", cameras_first), \
             patch.object(server, "_queue", queue), \
             patch.object(server, "_resolve_keys", return_value=(keys, self.folder, "~/bag_CTM")), \
             patch.object(server.time, "sleep", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                server._dl_worker()
        self.assertEqual(queue[0]["status"], "done")
        self.assertEqual(self.timestamps("bag_CTM_cameras_0.parquet"), [1, 20, 30])
        self.assertFalse((self.folder / "bag_CTM_other_0.parquet").exists())
        self.assertEqual(self.metadata()["message_count"], 3)
        self.assertTrue(state.is_complete(self.folder))
