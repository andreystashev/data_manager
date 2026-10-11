# Data Manager

[![Backend tests](https://github.com/andreystashev/data_manager/actions/workflows/tests.yml/badge.svg)](https://github.com/andreystashev/data_manager/actions/workflows/tests.yml)

A **FastAPI and React** application for browsing sensor datasets in **MinIO/S3**, downloading selected sensor categories, and trimming Parquet messages to a time window. It rebuilds ROS2-style metadata and preserves recognized one-shot context such as maps, routes, and static transforms.

The repository includes a small synthetic demo so the download and filtering workflow can be explored without a ROS2 installation.

## Preview

<img width="1438" alt="Data Manager dataset browser and download queue" src="https://github.com/user-attachments/assets/cde67ddd-5d4e-4af5-a6b2-6148dbd5edcd" />

## Features

- Browse and search the dataset index, then scan S3 for current file sizes.
- Select lidar, camera, radar, and other message categories.
- Apply an inclusive time window across overlapping sensor files.
- Download through a background queue with progress, stop, and resume.
- Recover task parameters and completion records from local folders.
- Rebuild topic counts, timestamps, and file references after filtering.
- Inspect logs and storage health from the browser interface.

## Quick start: local demo

Requirements: **Docker Engine/Desktop, Compose v2, and Bash**. The default Compose file includes Linux timezone mounts; on macOS, remove the `/etc/localtime` and `/etc/timezone` mounts if Docker cannot mount them.

```bash
git clone https://github.com/andreystashev/data_manager.git
cd data_manager
./start.sh
```

Open **<http://localhost:8000>**. MinIO's console is at <http://localhost:9001> with the demo credentials `minioadmin` / `minioadmin`. No AWS credentials file is required for this setup.

Scan a dataset, select categories and a time range, then add it to the queue. The default download destination is **`~/work_folder` on the host**. Compose mounts your home directory so the application can write downloads there.

The application has no authentication and the demo ports are published on all host interfaces. Use this configuration on a local development machine or a trusted network.

```bash
docker compose logs -f datamanager
./stop.sh
```

Ordinary shutdown preserves MinIO's named volume and local downloads. **`./stop.sh --clean` also removes Compose volumes**, images, and frontend build/dependency folders; it is a reset, not an ordinary stop.

## Demo data and ROS2 playback

Demo initialization creates small, deterministic, valid Parquet tables in the `demo-datasets` bucket. Each sensor file has 128 synthetic rows; all categories span the same timeline. Auxiliary data includes static-transform, map, and route context.

**Demo payloads are JSON examples, not ROS2 CDR messages.** They exercise browsing, downloading, trimming, and metadata generation. They cannot demonstrate ROS2 playback. Playback of real datasets additionally requires compatible payloads, message types, and a Parquet storage plugin.

An existing index is left untouched, and custom buckets are never seeded. To replace old demo placeholder objects deliberately:

```bash
docker compose exec datamanager python init_minio.py --refresh-demo
```

This overwrites the dedicated demo objects and index. It does not update existing local downloads; choose an empty download folder to try the refreshed data.

## Connect your own storage

Set the following in a local `.env` file, excluded from Git:

```dotenv
INIT_DEMO_DATA=0
S3_ENDPOINT=https://your-s3-endpoint
S3_BUCKET=your-dataset-bucket
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
```

Then run `./start.sh`. The default Compose file still starts local MinIO even when an external endpoint is selected; it can be removed from a dedicated external-storage deployment.

The external bucket must follow the project's [dataset layout and Parquet format](docs/data-format.md). This is not a browser for arbitrary S3 files. For a standalone container, demo initialization defaults to disabled.

## Architecture

The app container serves the built React interface and the FastAPI API. Compose adds a separate MinIO container. A background thread processes queued tasks; the frontend polls queue status every two seconds.

| Component | Responsibility |
| --- | --- |
| [backend/main_s3.py](backend/main_s3.py) | API, S3 discovery, queue, and download worker |
| [backend/trim_parquet.py](backend/trim_parquet.py) | Category/time filtering and metadata regeneration |
| [backend/download_state.py](backend/download_state.py) | Persistent task and completion records |
| [backend/init_minio.py](backend/init_minio.py) | Synthetic demo generation and seeding |
| [frontend/src/App.jsx](frontend/src/App.jsx) | Dataset browser and queue interface |
| [Dockerfile](Dockerfile) | Frontend build and Python runtime |
| [docker-compose.yml](docker-compose.yml) | App, MinIO, configuration, and host mounts |

The dataset index and live queue are held in memory. Download state and completion records on disk support recovery after restart. Trimming stages output before replacing files and requires additional disk space; replacement across multiple files is not one transaction.

See [data format, API routes, and recovery guarantees](docs/data-format.md) for exact behavior and limits.

## Development

Use **Python 3.11+** for the backend. From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
mkdir -p downloads
export DOWNLOAD_BASE="$PWD/downloads"
export S3_ENDPOINT=http://localhost:9000
export S3_BUCKET=demo-datasets
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
python -m uvicorn main_s3:app --app-dir backend --host 127.0.0.1 --port 8000
```

A running S3/MinIO instance with a populated index is needed to browse datasets. Stop any app container using port 8000 before starting the development server.

In a second terminal, with Node.js 20:

```bash
cd frontend
npm install
npm run dev
```

Vite is configured for **<http://localhost:3000>** and proxies `/api` to port 8000.

## Tests

```bash
python -m unittest discover -s tests -v
python -m compileall -q backend tests
```

Tests create real temporary Parquet files and mock S3 calls. They require no credentials, MinIO, or ROS2 installation. GitHub Actions runs the backend suite on Python 3.11, 3.12, and 3.13.

Linux user-service options are available through `./start.sh --autostart` and `./start.sh --no-autostart`. Service status and logs are accessible with `systemctl --user status datamanager` and `journalctl --user -u datamanager -f`.
