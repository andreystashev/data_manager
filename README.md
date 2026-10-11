# DataManager

A web-based system for browsing and downloading ROS2 sensor datasets (parquet format) from MinIO/S3 storage. 

This is a demonstration project showcasing a practical architecture for managing multi-source sensor data with time-based filtering and automated metadata handling.

## Quick Start

```bash
./start.sh
```

Opens on http://localhost:8000

## Requirements

- Docker + Docker Compose
- The built-in MinIO demo uses the credentials in Compose; no AWS credentials file is needed.
- For external storage, configure credentials through environment variables or `~/.aws/credentials`:
  ```ini
  [default]
  aws_access_key_id = ...
  aws_secret_access_key = ...
  ```

## Features

- Browse datasets from `datasets_index.json`
- Scan dataset size and file structure
- Download by sensor categories: lidars, cameras, radars, other
- Time-based trimming with automatic metadata re-indexing
- One-shot topics preservation (maps, tf_static, route) 
- Download queue with stop/resume capability
- Automatic file renaming for ROS2 bag play compatibility
- Resume incomplete downloads on restart

## Folder Naming

Downloaded datasets receive a suffix based on selected categories and time range:

| Selection | Trimming | Folder | Actions |
|-----------|----------|--------|----------|
| All | No | `datasetname` | — |
| Meta/Topics only | Any | `datasetname_M` | — |
| Partial (L/C/R + O) | No | `datasetname_LCR` | rename |
| Partial (L/C/R) | No | `datasetname_LCR` | rename + trim + reindex |
| Partial (L/C/R/O) | Yes | `datasetname_LCR_HHMMSS-HHMMSS` | rename + trim + reindex |
| All | Yes | `datasetname_LCROTM_HHMMSS-HHMMSS` | rename + trim + reindex |

Where: L=lidars, C=cameras, R=radars, O=other, T=topics, M=metadata

## Architecture Overview

### System Design

**Single Docker Container** with:
- **Backend**: FastAPI (Python) serving REST API + handling downloads
- **Frontend**: React/Vite SPA for interactive dataset browsing
- **Data Layer**: In-memory state (cleared on restart), local disk for downloads

**Data Flow**:
1. Frontend sends filter/download request (categories, time range, device ID)
2. Backend resolves S3 keys based on filters
3. Downloads files from S3 in background thread
4. Trims parquet files by timestamp if requested
5. Regenerates metadata.yaml for ROS2 compatibility

### Key Components

```
datamanager/
├── backend/
│   ├── main_s3.py           # FastAPI app + S3 integration + download worker
│   ├── trim_parquet.py      # Parquet trimming & metadata reindexing
│   └── requirements.txt      # Python dependencies
├── frontend/
│   ├── src/App.jsx          # React UI with download queue
│   ├── vite.config.js       # Vite bundler config
│   └── package.json
├── Dockerfile               # Multi-stage: node build → python runtime
├── docker-compose.yml       # Service orchestration
├── start.sh / stop.sh       # Lifecycle management
└── .aws/credentials         # AWS/MinIO credentials (mounted)
```

### S3 Storage Structure

```
demo-datasets/
├── datasets_index.json
├── datasets_topics/year=YYYY/month=MM/day=DD/device_id=XXXX/{dataset}_topics.parquet
├── datasets_meta/.../metadata.yaml
└── datasets_messages/.../{dataset}_lidars_N.parquet, _cameras_N.parquet, ...
```

The index contains dataset metadata (filename, device_id, year/month/day) used for quick listing.

### Download Flow

1. **Index Load**: On startup, fetch `datasets_index.json` and cache dataset metadata in memory
2. **Live Resolve**: When user requests a dataset, scan S3 folder to find actual files
3. **Filter Apply**: Select files matching user's category/time filter
4. **Parallel Download**: Download multiple files concurrently from S3
5. **Post-Process**: 
   - If time trim requested → slice parquet files to timestamp range
   - Inject one-shot topics (published once at bag start)
   - Regenerate metadata.yaml with correct statistics
   - Rename files for ROS2 compatibility

### Parquet Trimming

The `trim_parquet.py` module provides ROS2-independent trimming:

- **No ROS2 required**: Pure PyArrow for reading/writing parquets
- **Timestamp filtering**: Inclusive filtering of every selected sensor file, including overlapping time ranges
- **One-shot preservation**: Extracts transient_local topics and prepends them
- **Metadata regeneration**: Scans actual files to rebuild metadata.yaml
- **Result**: Trimmed bag is immediately playable with `ros2 bag play`

### API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/datasets` | GET | List all datasets with metadata |
| `/api/datasets/{name}/scan` | POST | Scan dataset size & files |
| `/api/datasets/{name}/metadata` | GET | Get metadata.yaml (time hints) |
| `/api/queue` | GET | Get download queue status |
| `/api/queue` | POST | Add download task to queue |
| `/api/queue/{id}/stop` | POST | Stop a task |
| `/api/queue/{id}/resume` | POST | Resume a stopped or failed task |
| `/api/queue/{id}` | DELETE | Remove an inactive task |
| `/api/queue/done` | DELETE | Remove completed tasks |
| `/api/queue/rescan` | POST | Recover downloads from local folders |
| `/api/download-path` | GET / PUT | Read or change the local download directory |
| `/api/datasets/refresh` | POST | Refresh the dataset index |
| `/api/health` | GET | Check backend and S3 status |

Downloads are written to the configured local directory; the backend does not expose a dedicated file-download API.

### Frontend UI Features

- **Dataset Browser**: Infinite scroll list with device_id labels
- **Category Filters**: Toggle lidars/cameras/radars/other
- **Time Range Selection**: Pick start/end times from metadata
- **Download Queue**: Track parallel downloads with progress bars
- **Resume Support**: Recover incomplete downloads after refresh
- **Debug Panel**: View logs and system status (collapsible)
- **Dark/Light Themes**: Toggle for comfortable viewing

<img width="1438" height="776" alt="Screenshot 2026-06-25 at 12 28 13 AM" src="https://github.com/user-attachments/assets/cde67ddd-5d4e-4af5-a6b2-6148dbd5edcd" />


## Deployment

```bash
./start.sh              # Build image + run container
./stop.sh               # Stop container
./stop.sh --clean       # Stop + remove image

docker compose logs -f  # View logs
```

### Autostart (systemd user service)

```bash
./start.sh --autostart          # Add to systemd user service
./start.sh --no-autostart       # Remove from systemd

# Manual control:
systemctl --user status datamanager
journalctl --user -u datamanager -f
systemctl --user restart datamanager
```

## Technical Highlights

### Why This Architecture?

1. **Single container simplicity** - Easy to deploy, minimal infrastructure
2. **In-memory caching** - Fast listing with S3 fallback for missing datasets
3. **Background worker** - Non-blocking downloads, graceful stop/resume
4. **PyArrow streaming** - Efficient parquet processing, no temp files for trimming
5. **Metadata regeneration** - Ensures ROS2 compatibility after modifications

### Performance Considerations

- **Index caching**: The dataset index is cached in memory.
- **Live S3 listing**: Dataset scans list files from S3; latency depends on storage and dataset size.
- **Download worker**: A background thread processes the download queue; boto3 handles file transfers.
- **Staged trim**: Parquet trimming processes files sequentially and stages output before replacing source files; allow disk space for the filtered copy
- **UI updates**: The frontend polls queue status every two seconds; downloads run in a background thread.

### Error Handling

- **Resume on error**: Failed downloads marked with error count
- **Recovery**: Stopped or failed tasks can be resumed; local folders are scanned to recover incomplete downloads.
- **Missing file handling**: Gracefully skip corrupted/deleted files
- **Metadata fallback**: If fresh scan fails, use last known file list

## Development

### Backend Development

```bash
cd backend
pip install -r requirements.txt
python3 main_s3.py
```

Environment variables:
- `S3_ENDPOINT`: MinIO/S3 endpoint (default: http://localhost:9000)
- `S3_BUCKET`: S3 bucket name (default: demo-datasets)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`: S3 credentials
- `DOWNLOAD_BASE`: Base path for downloads
- `DOWNLOAD_DISPLAY`: Display path in UI

### Frontend Development

```bash
cd frontend
npm install
npm run dev
```

Vite serves on http://localhost:5173 with hot reload.

## Project Structure Philosophy

This project demonstrates:

1. **Full-stack system design** for real-world data management
2. **Background processing and HTTP polling** for responsive web interfaces
3. **Efficient parquet handling** without heavy dependencies
4. **Container-based deployment** for consistency
5. **Resume/retry patterns** for robust background tasks
6. **UI state management** with React hooks

## Lessons Applied

- Use S3 for scalable data storage, not local filesystems
- Pre-compute indices for faster listing (datasets_index.json)
- Support graceful stop/resume for long-running operations
- Validate metadata after modifications (metadata regeneration)
- Provide debug visibility without exposing sensitive details
- Design for offline-first where possible (in-memory cache)

## License

Demonstration project. Use for learning and interview preparation.

## Backend validation

Use Python 3.11 or newer. Tests create small real Parquet files in temporary
folders and mock S3 calls; no credentials, MinIO, or ROS2 installation is needed.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
```

GitHub Actions runs this suite on Python 3.11, 3.12, and 3.13.

### Trimming and recovery guarantees

- The trim window is inclusive. Every selected data file is filtered, even when
  sensor categories overlap in time or rows arrive out of order.
- Historical one-shot messages from category `_0.parquet` files are preserved
  before the window. Messages already inside the window are not duplicated.
- Filtered files and metadata are staged first. Invalid input, a staging error,
  or a window with no messages leaves the original dataset unchanged.
- Replacing the staged output is not a transaction across all files. A process
  crash during replacement requires resuming the task; extra temporary disk
  space is needed during trimming.
- Task parameters and S3 keys are saved in `.download-state.json`, preserving
  exact timestamps across a restart. `.download-complete.json` is written only
  after successful post-processing and records output names and sizes.
- Recovery validates this completion record. Missing/truncated output and
  legacy folders without a record are shown as stopped, not assumed complete.
  Size validation does not detect same-size content corruption.
- Post-processing errors are reported as errors in the queue and do not mark
  the dataset complete.

### Selecting sensor categories

Category filtering preserves each selected sensor's numbered files, including
`_cameras_0.parquet`, `_lidars_0.parquet`, and `_radars_0.parquet`. Auxiliary
`_other_0.parquet` (or legacy `_0.parquet`) files contribute only recognized
one-shot topics when `other` is disabled; their ordinary sensor rows are not
included, even when they overlap the requested time window.

One-shot messages inside the selected window retain their timestamps. Historical
context is moved just before the window. Partial selections without a time cut
preserve original timestamps and rebuild metadata from the resulting files.
Category filtering requires a topics Parquet file; if it is missing, the task
reports an error instead of deleting files without knowing their topic types.

### Built-in demo data

The default Compose setup seeds the `demo-datasets` MinIO bucket with small,
deterministic Parquet tables. Each sensor file contains 128 synthetic rows;
`other_0` also includes `/tf_static`, map and route context. All sensor categories
cover the same dataset time span, and metadata is derived from actual table rows.
Payloads are JSON examples, **not real ROS2 CDR messages**: the demo exercises
browsing, category selection, downloads, trimming and metadata, not ROS2 playback.

Demo seeding skips an existing dataset index and never seeds a custom bucket.
For your own S3 storage, disable initialization explicitly:

```bash
INIT_DEMO_DATA=0 docker compose up --build -d
```

A standalone container also defaults to disabled initialization. Compose enables
it for the local demo. The dataset index is uploaded after all demo files.
To deliberately replace old placeholder objects in the dedicated demo bucket:

```bash
docker compose exec datamanager python init_minio.py --refresh-demo
```

This refresh overwrites demo objects and the demo index. It is never automatic.
Existing local downloads are not refreshed by that command; use an empty local
download folder to try the new demo data.
