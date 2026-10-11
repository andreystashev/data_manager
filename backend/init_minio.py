#!/usr/bin/env python3
"""
Initialize MinIO with rich demo datasets for DataManager.
Creates bucket, uploads index, metadata, and mock parquet files
with multiple categories so all UI features work (trim, scan, size, etc.).
"""
import json
import os
import argparse
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from trim_parquet import DATA_SCHEMA, reindex_metadata
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

# Configuration
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "demo-datasets")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Dataset definitions with realistic metadata
DATASETS = [
    {
        "name": "2024-01-15-vehicle001-morning_drive",
        "year": 2024, "month": 1, "day": 15,
        "device_id": "vehicle001",
        "timestamp": "2024-01-15T08:30:00Z",
        "start_sec": 1705314600,
        "duration_sec": 3600,  # 1 hour
        "suffix": "morning_drive",
        # Per-category file counts; each file holds 128 synthetic sensor rows.
        "files": {
            "lidars":  {"count": 12},
            "cameras": {"count": 8},
            "radars":  {"count": 6},
            "other":   {"count": 4},
        },
    },
    {
        "name": "2024-01-16-vehicle002-urban_nav",
        "year": 2024, "month": 1, "day": 16,
        "device_id": "vehicle002",
        "timestamp": "2024-01-16T14:20:00Z",
        "start_sec": 1705414800,
        "duration_sec": 5400,  # 1.5 hours
        "suffix": "urban_nav",
        "files": {
            "lidars":  {"count": 18},
            "cameras": {"count": 12},
            "radars":  {"count": 9},
            "other":   {"count": 6},
        },
    },
    {
        "name": "2024-01-17-vehicle003-highway_test",
        "year": 2024, "month": 1, "day": 17,
        "device_id": "vehicle003",
        "timestamp": "2024-01-17T10:15:00Z",
        "start_sec": 1705486500,
        "duration_sec": 7200,  # 2 hours
        "suffix": "highway_test",
        "files": {
            "lidars":  {"count": 24},
            "cameras": {"count": 16},
            "radars":  {"count": 12},
            "other":   {"count": 8},
        },
    },
    {
        "name": "2024-01-18-vehicle001-evening_run",
        "year": 2024, "month": 1, "day": 18,
        "device_id": "vehicle001",
        "timestamp": "2024-01-18T18:45:00Z",
        "start_sec": 1705596300,
        "duration_sec": 4500,  # 1.25 hours
        "suffix": "evening_run",
        "files": {
            "lidars":  {"count": 15},
            "cameras": {"count": 10},
            "radars":  {"count": 7},
            "other":   {"count": 5},
        },
    },
    {
        "name": "2024-01-19-vehicle002-parking_lot",
        "year": 2024, "month": 1, "day": 19,
        "device_id": "vehicle002",
        "timestamp": "2024-01-19T16:30:00Z",
        "start_sec": 1705681800,
        "duration_sec": 1800,  # 30 min
        "suffix": "parking_lot",
        "files": {
            "lidars":  {"count": 6},
            "cameras": {"count": 4},
            "radars":  {"count": 3},
            "other":   {"count": 2},
        },
    },
]


ROWS_PER_FILE = 128


def make_data_parquet(dataset: dict, category: str, index: int) -> bytes:
    """Generate deterministic, readable Parquet; payloads are demo JSON, not CDR."""
    topic_ids = {"lidars": 1, "cameras": 2, "radars": 3, "other": 8}
    count = dataset["files"][category]["count"]
    if not 0 <= index < count:
        raise ValueError("Invalid file index")
    start_ns = dataset["start_sec"] * 1_000_000_000
    duration_ns = dataset["duration_sec"] * 1_000_000_000
    file_start = start_ns + duration_ns * index // count
    file_end = start_ns + duration_ns * (index + 1) // count
    rows = []
    if category == "other" and index == 0:
        for topic_id in (5, 6, 7):
            rows.append({"id": len(rows), "topic_id": topic_id, "timestamp": start_ns,
                         "data": json.dumps({"synthetic": True, "topic_id": topic_id})})
    for sample in range(ROWS_PER_FILE):
        rows.append({
            "id": len(rows), "topic_id": topic_ids[category],
            "timestamp": file_start + (file_end - file_start) * sample // (ROWS_PER_FILE - 1),
            "data": json.dumps({"synthetic": True, "category": category,
                                "file": index, "sample": sample}, sort_keys=True),
        })
    table = pa.Table.from_pylist(rows, schema=DATA_SCHEMA)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy", use_dictionary=False)
    return buf.getvalue().to_pybytes()


def make_topics_parquet() -> bytes:
    """Create a minimal topics.parquet with realistic topic definitions."""
    topics = [
        (1, "/sensor/lidar/points", "sensor_msgs/msg/PointCloud2", "json", "volatile"),
        (2, "/sensor/camera/image_raw", "sensor_msgs/msg/Image", "json", "volatile"),
        (3, "/sensor/radar/objects", "autoware_perception_msgs/msg/TrackedObjects", "json", "volatile"),
        (4, "/tf", "tf2_msgs/msg/TFMessage", "json", "volatile"),
        (5, "/tf_static", "tf2_msgs/msg/TFMessage", "json", "transient_local"),
        (6, "/map/vector_map", "autoware_map_msgs/msg/VectorMap", "json", "transient_local"),
        (7, "/vehicle/route_number_report", "tier4_vehicle_msgs/msg/RouteNumberReport", "json", "transient_local"),
        (8, "/other/sensor_data", "sensor_msgs/msg/LaserScan", "json", "volatile"),
    ]

    table = pa.table({
        "topic_id": pa.array([t[0] for t in topics], type=pa.uint16()),
        "topic_name": pa.array([t[1] for t in topics], type=pa.string()),
        "message_type": pa.array([t[2] for t in topics], type=pa.string()),
        "serialization_format": pa.array([t[3] for t in topics], type=pa.string()),
        "offered_qos_profiles": pa.array([t[4] for t in topics], type=pa.string()),
    })

    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy", use_dictionary=False)
    return buf.getvalue().to_pybytes()


def build_demo_dataset(dataset: dict) -> dict[str, bytes]:
    """Build message files and derive metadata from their actual contents."""
    name = dataset["name"]
    files = {f"{name}_topics.parquet": make_topics_parquet()}
    for category in ("lidars", "cameras", "radars", "other"):
        for index in range(dataset["files"][category]["count"]):
            files[f"{name}_{category}_{index}.parquet"] = make_data_parquet(dataset, category, index)
    with tempfile.TemporaryDirectory(prefix="datamanager-demo-") as temporary:
        folder = Path(temporary)
        for filename, data in files.items():
            (folder / filename).write_bytes(data)
        result = reindex_metadata(folder)
        if "error" in result:
            raise ValueError(result["error"])
        files["metadata.yaml"] = (folder / "metadata.yaml").read_bytes()
    return files


def init_minio(refresh: bool = False):
    """Seed the designated demo bucket; existing indices are left alone by default."""
    if S3_BUCKET != "demo-datasets":
        print("Demo seeding skipped: S3_BUCKET is not demo-datasets")
        return
    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name='us-east-1',
        config=Config(
            connect_timeout=10, read_timeout=30,
            retries={'max_attempts': 3, 'mode': 'standard'},
            s3={'addressing_style': 'path'},
        ),
        use_ssl=S3_ENDPOINT.startswith('https://')
    )

    # Create bucket
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f"✓ Bucket '{S3_BUCKET}' already exists")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ('404', 'NoSuchBucket'):
            s3.create_bucket(Bucket=S3_BUCKET)
            print(f"✓ Created bucket '{S3_BUCKET}'")
        else:
            raise

    if not refresh:
        try:
            s3.head_object(Bucket=S3_BUCKET, Key="datasets_index.json")
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise
        else:
            print("Existing dataset index found; demo data left unchanged")
            return

    index_data = []
    for ds in DATASETS:
        name = ds["name"]
        base_path = f"year={ds['year']:04d}/month={ds['month']:02d}/day={ds['day']:02d}/device_id={ds['device_id']}"
        files = build_demo_dataset(ds)
        for filename, data in files.items():
            if filename == "metadata.yaml":
                key = f"datasets_meta/{base_path}/{ds['suffix']}/metadata.yaml"
                content_type = "text/yaml"
            elif filename.endswith("_topics.parquet"):
                key = f"datasets_topics/{base_path}/{filename}"
                content_type = "application/octet-stream"
            else:
                key = f"datasets_messages/{base_path}/{filename}"
                content_type = "application/octet-stream"
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=content_type)
        index_data.append({
            "filename": name, "year": ds["year"], "month": ds["month"], "day": ds["day"],
            "device_id": ds["device_id"], "timestamp": ds["timestamp"],
        })
        print(f"✓ {name}: {len(files)} objects, {sum(map(len, files.values())) / 1024:.0f} KiB")

    # Publish the index only after all datasets have been uploaded successfully.
    s3.put_object(Bucket=S3_BUCKET, Key="datasets_index.json",
                  Body=json.dumps(index_data, indent=2).encode("utf-8"), ContentType="application/json")
    print("\n✅ MinIO initialization complete!")
    print(f"   Access MinIO Console: http://localhost:9001")
    print(f"   DataManager will connect to: {S3_ENDPOINT}/{S3_BUCKET}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed the demo-datasets bucket")
    parser.add_argument("--refresh-demo", action="store_true",
                        help="Explicitly replace demo objects and index in demo-datasets")
    args = parser.parse_args()
    try:
        init_minio(refresh=args.refresh_demo)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)