#!/usr/bin/env python3
"""
Initialize MinIO with rich demo datasets for DataManager.
Creates bucket, uploads index, metadata, and mock parquet files
with multiple categories so all UI features work (trim, scan, size, etc.).
"""
import json
import os
import io
import struct
import random
from pathlib import Path
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
        # Per-category file counts and sizes (bytes)
        "files": {
            "lidars":  {"count": 12, "size": 52428800},   # 50 MB each
            "cameras": {"count": 8,  "size": 83886080},   # 80 MB each
            "radars":  {"count": 6,  "size": 10485760},   # 10 MB each
            "other":   {"count": 4,  "size": 5242880},    # 5 MB each
        },
        "topics_size": 65536,
        "meta_size": 2048,
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
            "lidars":  {"count": 18, "size": 52428800},
            "cameras": {"count": 12, "size": 83886080},
            "radars":  {"count": 9,  "size": 10485760},
            "other":   {"count": 6,  "size": 5242880},
        },
        "topics_size": 65536,
        "meta_size": 2048,
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
            "lidars":  {"count": 24, "size": 52428800},
            "cameras": {"count": 16, "size": 83886080},
            "radars":  {"count": 12, "size": 10485760},
            "other":   {"count": 8,  "size": 5242880},
        },
        "topics_size": 65536,
        "meta_size": 2048,
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
            "lidars":  {"count": 15, "size": 52428800},
            "cameras": {"count": 10, "size": 83886080},
            "radars":  {"count": 7,  "size": 10485760},
            "other":   {"count": 5,  "size": 5242880},
        },
        "topics_size": 65536,
        "meta_size": 2048,
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
            "lidars":  {"count": 6,  "size": 52428800},
            "cameras": {"count": 4,  "size": 83886080},
            "radars":  {"count": 3,  "size": 10485760},
            "other":   {"count": 2,  "size": 5242880},
        },
        "topics_size": 65536,
        "meta_size": 2048,
    },
]


def make_fake_parquet(size_bytes: int) -> bytes:
    """Create a minimal valid parquet file of approximately the given size."""
    # Parquet magic bytes + minimal footer
    header = b"PAR1"
    # Pad with zeros to reach desired size
    payload_size = max(0, size_bytes - len(header) - 4)  # 4 bytes for footer magic
    return header + b"\x00" * payload_size + b"PAR1"


def make_topics_parquet() -> bytes:
    """Create a minimal topics.parquet with realistic topic definitions."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    topics = [
        (1, "/sensor/lidar/points", "sensor_msgs/msg/PointCloud2", "cdr", "volatile"),
        (2, "/sensor/camera/image_raw", "sensor_msgs/msg/Image", "cdr", "volatile"),
        (3, "/sensor/radar/objects", "autoware_perception_msgs/msg/TrackedObjects", "cdr", "volatile"),
        (4, "/tf", "tf2_msgs/msg/TFMessage", "cdr", "volatile"),
        (5, "/tf_static", "tf2_msgs/msg/TFMessage", "cdr", "transient_local"),
        (6, "/map/vector_map", "autoware_map_msgs/msg/VectorMap", "cdr", "transient_local"),
        (7, "/vehicle/route_number_report", "tier4_vehicle_msgs/msg/RouteNumberReport", "cdr", "transient_local"),
        (8, "/other/sensor_data", "sensor_msgs/msg/LaserScan", "cdr", "volatile"),
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


def make_metadata_yaml(dataset: dict) -> str:
    """Generate a metadata.yaml with proper file blocks for trim support."""
    start_ns = dataset["start_sec"] * 1_000_000_000
    dur_ns = dataset["duration_sec"] * 1_000_000_000

    lines = []
    lines.append("rosbag2_bagfile_information:")
    lines.append("  version: 5")
    lines.append("  storage_identifier: parquet")
    lines.append("  duration:")
    lines.append(f"    nanoseconds: {dur_ns}")
    lines.append("  starting_time:")
    lines.append(f"    nanoseconds_since_epoch: {start_ns}")
    lines.append(f"  message_count: 8450")
    lines.append("  topics_with_message_count:")
    lines.append("    - topic_metadata:")
    lines.append("        name: /sensor/lidar/points")
    lines.append("        type: sensor_msgs/msg/PointCloud2")
    lines.append("        serialization_format: cdr")
    lines.append('        offered_qos_profiles: "volatile"')
    lines.append("      message_count: 2100")
    lines.append("    - topic_metadata:")
    lines.append("        name: /sensor/camera/image_raw")
    lines.append("        type: sensor_msgs/msg/Image")
    lines.append("        serialization_format: cdr")
    lines.append('        offered_qos_profiles: "volatile"')
    lines.append("      message_count: 3200")
    lines.append("    - topic_metadata:")
    lines.append("        name: /sensor/radar/objects")
    lines.append("        type: autoware_perception_msgs/msg/TrackedObjects")
    lines.append("        serialization_format: cdr")
    lines.append('        offered_qos_profiles: "volatile"')
    lines.append("      message_count: 2150")
    lines.append("    - topic_metadata:")
    lines.append("        name: /tf")
    lines.append("        type: tf2_msgs/msg/TFMessage")
    lines.append("        serialization_format: cdr")
    lines.append('        offered_qos_profiles: "volatile"')
    lines.append("      message_count: 1000")
    lines.append('  compression_format: ""')
    lines.append('  compression_mode: ""')
    lines.append("  relative_file_paths:")

    # Generate file entries with sequential timestamps
    file_index = 0
    file_entries = []
    for cat_key, cat_label in [("lidars", "lidars"), ("cameras", "cameras"),
                                ("radars", "radars"), ("other", "other")]:
        count = dataset["files"][cat_key]["count"]
        for i in range(count):
            file_index += 1
            fname = f"{dataset['name']}_{cat_label}_{i}.parquet"
            lines.append(f"    - {fname}")
            # Each file covers a slice of the total duration
            f_start_ns = start_ns + int(dur_ns * (file_index - 1) / sum(
                dataset["files"][c]["count"] for c in ["lidars", "cameras", "radars", "other"]
            ))
            f_end_ns = start_ns + int(dur_ns * file_index / sum(
                dataset["files"][c]["count"] for c in ["lidars", "cameras", "radars", "other"]
            ))
            f_dur_ns = max(1, f_end_ns - f_start_ns)
            file_entries.append((fname, f_start_ns, f_dur_ns))

    lines.append("  files:")
    for fname, f_start_ns, f_dur_ns in file_entries:
        lines.append(f"    - path: {fname}")
        lines.append("      starting_time:")
        lines.append(f"        nanoseconds_since_epoch: {f_start_ns}")
        lines.append("      duration:")
        lines.append(f"        nanoseconds: {f_dur_ns}")
        lines.append("      message_count: 100")

    return "\n".join(lines) + "\n"


def init_minio():
    """Initialize MinIO with rich demo data."""
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
        if error_code in ('404', 'NoSuchBucket', '400'):
            s3.create_bucket(Bucket=S3_BUCKET)
            print(f"✓ Created bucket '{S3_BUCKET}'")
        else:
            raise

    # Build and upload datasets_index.json
    index_data = []
    for ds in DATASETS:
        index_data.append({
            "filename": ds["name"],
            "year": ds["year"],
            "month": ds["month"],
            "day": ds["day"],
            "device_id": ds["device_id"],
            "timestamp": ds["timestamp"],
        })

    s3.put_object(
        Bucket=S3_BUCKET,
        Key="datasets_index.json",
        Body=json.dumps(index_data, indent=2).encode('utf-8'),
        ContentType='application/json'
    )
    print(f"✓ Uploaded datasets_index.json with {len(index_data)} entries")

    # Pre-generate topics parquet (same for all datasets)
    topics_data = make_topics_parquet()

    # Upload files for each dataset
    for ds in DATASETS:
        name = ds["name"]
        base_path = f"year={ds['year']:04d}/month={ds['month']:02d}/day={ds['day']:02d}/device_id={ds['device_id']}"

        # 1. Upload topics.parquet
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"datasets_topics/{base_path}/{name}_topics.parquet",
            Body=topics_data,
            ContentType='application/octet-stream'
        )

        # 2. Upload metadata.yaml in the suffix folder
        meta_yaml = make_metadata_yaml(ds)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"datasets_meta/{base_path}/{ds['suffix']}/metadata.yaml",
            Body=meta_yaml.encode('utf-8'),
            ContentType='text/yaml'
        )

        # 3. Upload message parquet files for each category
        total_uploaded = 0
        for cat_key in ["lidars", "cameras", "radars", "other"]:
            count = ds["files"][cat_key]["count"]
            size = ds["files"][cat_key]["size"]
            for i in range(count):
                fake_data = make_fake_parquet(size)
                fname = f"{name}_{cat_key}_{i}.parquet"
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=f"datasets_messages/{base_path}/{fname}",
                    Body=fake_data,
                    ContentType='application/octet-stream'
                )
                total_uploaded += 1

        print(f"✓ {name}: {total_uploaded} files, "
              f"{(sum(ds['files'][c]['count'] * ds['files'][c]['size'] for c in ['lidars','cameras','radars','other']) + ds['topics_size'] + ds['meta_size']) / 1e6:.0f} MB")

    print("\n✅ MinIO initialization complete!")
    print(f"   Access MinIO Console: http://localhost:9001 (minioadmin:minioadmin)")
    print(f"   DataManager will connect to: {S3_ENDPOINT}/{S3_BUCKET}")


if __name__ == "__main__":
    try:
        init_minio()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)