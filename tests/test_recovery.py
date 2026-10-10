import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
import main_s3 as server
import download_state as state


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.name = "2026-10-10-0001-demo"
        self.params = {"lidars": True, "topics": True, "meta": True,
                       "trimStart": 1.25, "trimEnd": 6.75, "startSec": 1791662398.5}
        self.folder = self.root / (self.name + server._build_folder_suffix(self.params))
        self.folder.mkdir()
        self.keys = ["messages/" + self.name + "_lidars_0.parquet"]
        (self.folder / (self.folder.name + "_lidars_0.parquet")).write_bytes(b"output-data")
        (self.folder / "metadata.yaml").write_text("metadata")
        for key, value in {"DOWNLOAD_BASE": self.root, "DOWNLOAD_DISPLAY": "~/test",
                           "_queue": [], "_queue_id_counter": 0, "_datasets": {}}.items():
            patcher = patch.object(server, key, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_renamed_file_is_not_proof_of_completion(self):
        server._recover_incomplete_downloads()
        self.assertEqual(server._queue[0]["status"], "stopped")

    def test_completed_download_restores_exact_parameters_and_keys(self):
        state.save_state(self.folder, self.name, self.params, self.keys)
        state.mark_complete(self.folder)
        self.assertEqual(server._recover_incomplete_downloads(), 1)
        item = server._queue[0]
        self.assertEqual(item["status"], "done")
        self.assertEqual(item["params"], self.params)
        self.assertEqual(item["keys"], self.keys)
        self.assertEqual(server._recover_incomplete_downloads(), 0)

    def test_missing_output_invalidates_completion(self):
        state.mark_complete(self.folder)
        next(self.folder.glob("*.parquet")).unlink()
        self.assertFalse(state.is_complete(self.folder))
        server._recover_incomplete_downloads()
        self.assertEqual(server._queue[0]["status"], "stopped")

    def test_truncated_output_invalidates_completion(self):
        state.mark_complete(self.folder)
        next(self.folder.glob("*.parquet")).write_bytes(b"short")
        self.assertFalse(state.is_complete(self.folder))

    def test_restart_of_task_invalidates_old_completion(self):
        state.mark_complete(self.folder)
        state.save_state(self.folder, self.name, self.params, self.keys)
        self.assertFalse(state.is_complete(self.folder))
        self.assertEqual(state.read_state(self.folder)["params"], self.params)

    def test_invalid_json_is_recovered_conservatively(self):
        (self.folder / state.COMPLETE_FILE).write_text("{broken")
        (self.folder / state.STATE_FILE).write_text("[]")
        server._recover_incomplete_downloads()
        self.assertEqual(server._queue[0]["status"], "stopped")

    def test_worker_reports_trim_failure_and_continues_loop(self):
        server._queue.append({"id": 1, "dataset_name": self.name, "params": self.params,
                              "status": "pending", "keys": None})
        def update(item_id, **kwargs):
            server._queue[0].update(kwargs)
        with patch.object(server, "_resolve_keys", return_value=(self.keys, self.folder, "~/test")), \
             patch.object(server, "_update_queue_item", side_effect=update), \
             patch.object(server, "download_s3_file"), \
             patch.object(server, "_rename_for_ros2"), \
             patch.object(server, "trim_bag", side_effect=ValueError("bad parquet")), \
             patch.object(server.time, "sleep", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                server._dl_worker()
        self.assertEqual(server._queue[0]["status"], "error")
        self.assertEqual(server._queue[0]["error_msg"], "bad parquet")
        self.assertFalse(state.is_complete(self.folder))

    def test_completed_unsuffixed_folder_is_recognized(self):
        folder = self.root / self.name
        folder.mkdir()
        (folder / "metadata.yaml").write_text("metadata")
        state.save_state(folder, self.name, {"meta": True}, ["meta/metadata.yaml"])
        state.mark_complete(folder)
        server._recover_incomplete_downloads()
        item = next(item for item in server._queue if item["dest"].endswith("/" + self.name))
        self.assertEqual(item["status"], "done")

    def test_worker_marks_completion_only_after_success(self):
        server._queue.append({"id": 1, "dataset_name": self.name, "params": self.params,
                              "status": "pending", "keys": None})
        def trim(*args):
            self.assertFalse(state.is_complete(self.folder))
            return {"messages_total": 1}
        with patch.object(server, "_resolve_keys", return_value=(self.keys, self.folder, "~/test")), \
             patch.object(server, "download_s3_file"), \
             patch.object(server, "_rename_for_ros2"), \
             patch.object(server, "trim_bag", side_effect=trim), \
             patch.object(server.time, "sleep", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                server._dl_worker()
        self.assertEqual(server._queue[0]["status"], "done")
        self.assertTrue(state.is_complete(self.folder))

    def test_rename_is_idempotent_on_resume(self):
        source = self.folder / (self.name + "_topics.parquet")
        source.write_bytes(b"topics")
        prefix = self.folder.name
        server._rename_for_ros2(self.folder, self.name, prefix)
        names = {path.name for path in self.folder.iterdir()}
        server._rename_for_ros2(self.folder, self.name, prefix)
        self.assertEqual({path.name for path in self.folder.iterdir()}, names)
        self.assertTrue((self.folder / (prefix + "_topics.parquet")).exists())

    def test_state_recovers_arbitrary_dataset_name_without_index(self):
        folder = self.root / "demo-bag"
        folder.mkdir()
        state.save_state(folder, "demo-bag", {"meta": True}, ["meta/metadata.yaml"])
        server._recover_incomplete_downloads()
        self.assertTrue(any(item["dataset_name"] == "demo-bag" for item in server._queue))

    def test_startup_recovers_disk_when_index_sync_fails(self):
        with patch.object(server, "load_aws_credentials"), \
             patch.object(server, "init_s3_client"), \
             patch.object(server, "sync_index_only", side_effect=OSError("offline")), \
             patch.object(server, "_recover_incomplete_downloads") as recover, \
             patch.object(server.threading, "Thread") as thread:
            server.startup()
            background_init = thread.call_args_list[0].kwargs["target"]
            background_init()
        recover.assert_called_once()

    def test_resolve_includes_files_at_exact_window_edges(self):
        server._datasets[self.name] = {"keys": {
            "lidars": ["messages/bag_lidars_1.parquet", "messages/bag_lidars_2.parquet"],
            "other": ["messages/bag_other_0.parquet"],
        }}
        metadata = {"files": [
            {"path": "bag_lidars_1.parquet", "start_sec": 10, "end_sec": 20},
            {"path": "bag_lidars_2.parquet", "start_sec": 30, "end_sec": 40},
            {"path": "bag_other_0.parquet", "start_sec": 0, "end_sec": 1},
        ]}
        params = {"lidars": True, "topics": False, "meta": False,
                  "trimStart": 20, "trimEnd": 30, "startSec": 0}
        with patch.object(server, "_fetch_metadata_yaml", return_value="metadata"), \
             patch.object(server, "_parse_metadata_yaml", return_value=metadata):
            keys, _, _ = server._resolve_keys(self.name, params)
        self.assertIn("messages/bag_lidars_1.parquet", keys)
        self.assertIn("messages/bag_lidars_2.parquet", keys)
