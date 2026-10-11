import copy
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow.parquet as pq
import yaml
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
import init_minio as demo
import trim_parquet as trimmer


class DemoDataTests(unittest.TestCase):
    def setUp(self):
        self.dataset = copy.deepcopy(demo.DATASETS[0])
        self.dataset["files"] = {category: {"count": 2} for category in ("lidars", "cameras", "radars", "other")}

    def test_files_are_readable_and_metadata_matches_actual_tables(self):
        files = demo.build_demo_dataset(self.dataset)
        metadata = yaml.safe_load(files["metadata.yaml"])["rosbag2_bagfile_information"]
        total = 0
        for entry in metadata["files"]:
            table = pq.read_table(io.BytesIO(files[entry["path"]]))
            self.assertEqual(table.schema, trimmer.DATA_SCHEMA)
            timestamps = table.column("timestamp").to_pylist()
            self.assertEqual(entry["message_count"], len(table))
            self.assertEqual(entry["starting_time"]["nanoseconds_since_epoch"], min(timestamps))
            self.assertEqual(entry["duration"]["nanoseconds"], max(timestamps) - min(timestamps))
            total += len(table)
        self.assertEqual(metadata["message_count"], total)
        self.assertEqual(sum(topic["message_count"] for topic in metadata["topics_with_message_count"]), total)
        self.assertEqual(metadata["duration"]["nanoseconds"], self.dataset["duration_sec"] * 10**9)
        self.assertLess(sum(map(len, files.values())), 1024 * 1024)

    def test_demo_generation_is_deterministic(self):
        self.assertEqual(demo.build_demo_dataset(self.dataset), demo.build_demo_dataset(self.dataset))

    def test_generated_lidar_subset_can_be_trimmed_with_oneshot_context(self):
        files = demo.build_demo_dataset(self.dataset)
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            for name, body in files.items():
                (folder / name).write_bytes(body)
            start = self.dataset["start_sec"] * 10**9 + 600 * 10**9
            end = start + 300 * 10**9
            result = trimmer.trim_bag(str(folder), start, end, selected_categories={"lidars"})
            self.assertGreater(result["messages_total"], 3)
            data_paths = trimmer._list_data_parquets(folder)
            self.assertTrue(all("_lidars_" in path.name for path in data_paths))
            topics_seen = set()
            for path in data_paths:
                table = pq.read_table(path)
                for row in table.to_pylist():
                    topics_seen.add(row["topic_id"])
                    if row["topic_id"] == 1:
                        self.assertTrue(start <= row["timestamp"] <= end)
                    else:
                        self.assertIn(row["topic_id"], {5, 6, 7})
                        self.assertLess(row["timestamp"], start)
            self.assertTrue({1, 5, 6, 7} <= topics_seen)

    def test_existing_index_is_not_overwritten(self):
        client = Mock()
        with patch.object(demo, "S3_BUCKET", "demo-datasets"), patch.object(demo.boto3, "client", return_value=client):
            demo.init_minio()
        client.put_object.assert_not_called()
        client.create_bucket.assert_not_called()

    def test_custom_bucket_is_never_seeded(self):
        with patch.object(demo, "S3_BUCKET", "real-datasets"), patch.object(demo.boto3, "client") as client:
            demo.init_minio(refresh=True)
        client.assert_not_called()

    def test_missing_index_is_published_after_readable_objects(self):
        client = Mock()
        client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        with patch.object(demo, "S3_BUCKET", "demo-datasets"), \
             patch.object(demo, "DATASETS", [self.dataset]), \
             patch.object(demo.boto3, "client", return_value=client):
            demo.init_minio()
        calls = client.put_object.call_args_list
        self.assertEqual(calls[-1].kwargs["Key"], "datasets_index.json")
        for call in calls[:-1]:
            if call.kwargs["Key"].endswith(".parquet"):
                self.assertGreater(pq.read_table(io.BytesIO(call.kwargs["Body"])).num_rows, 0)

    def test_upload_failure_does_not_publish_incomplete_index(self):
        client = Mock()
        client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        client.put_object.side_effect = OSError("upload failed")
        with patch.object(demo, "S3_BUCKET", "demo-datasets"), \
             patch.object(demo, "DATASETS", [self.dataset]), \
             patch.object(demo.boto3, "client", return_value=client):
            with self.assertRaises(OSError):
                demo.init_minio()
        self.assertFalse(any(call.kwargs["Key"] == "datasets_index.json" for call in client.put_object.call_args_list))

    def test_explicit_refresh_can_replace_demo_objects(self):
        client = Mock()
        with patch.object(demo, "S3_BUCKET", "demo-datasets"), \
             patch.object(demo, "DATASETS", [self.dataset]), \
             patch.object(demo.boto3, "client", return_value=client):
            demo.init_minio(refresh=True)
        client.head_object.assert_not_called()
        self.assertEqual(client.put_object.call_args_list[-1].kwargs["Key"], "datasets_index.json")
