# Dataset format, API, and recovery

See the [README](../README.md) for startup and configuration.

## S3 layout

The bucket contains an index and date/device-partitioned dataset files:

```text
demo-datasets/
├── datasets_index.json
├── datasets_topics/year=YYYY/month=MM/day=DD/device_id=XXXX/{dataset}_topics.parquet
├── datasets_meta/year=YYYY/month=MM/day=DD/device_id=XXXX/{dataset_suffix}/metadata.yaml
└── datasets_messages/year=YYYY/month=MM/day=DD/device_id=XXXX/
    └── {dataset}_lidars_N.parquet, {dataset}_cameras_N.parquet, ...
```

The index supplies dataset identifiers, device IDs, dates, and storage paths. Live scans resolve the current files. See [init_minio.py](../backend/init_minio.py) for an executable example of the complete layout.

Message Parquet tables use this schema:

| Column | Arrow type | Meaning |
| --- | --- | --- |
| `id` | `uint32` | Message identifier |
| `topic_id` | `uint16` | Reference to a topic |
| `timestamp` | `int64` | Nanoseconds since Unix epoch |
| `data` | `string` | Message payload |

Topics files supply topic identifiers, names, types, serialization, and QoS metadata. The built-in demo writes synthetic JSON payloads; real ROS2 playback requires a compatible serialized format and storage plugin.

## Filtering and metadata

The time window includes both boundaries. Every selected file is filtered, including categories with overlapping ranges and rows arriving out of order.

Each selected category retains its numbered files, including `_cameras_0.parquet`, `_lidars_0.parquet`, and `_radars_0.parquet`. When `other` is disabled, auxiliary `_other_0.parquet` or legacy `_0.parquet` files contribute only recognized one-shot topics, not ordinary sensor rows.

Recognition uses the explicit `ONESHOT_TOPICS` list in [trim_parquet.py](../backend/trim_parquet.py). It is not general discovery of every transient-local topic. Historical context is placed just before the selected window; context already inside it retains its timestamps and is not duplicated. Category filtering without a time cut preserves original timestamps.

Filtering by category requires a topics Parquet file. Missing topics produce an error rather than deleting files with unknown topic types. Output metadata is regenerated from actual rows.

Filtered tables and metadata are staged before source replacement. Invalid input, staging failures, and a window without messages leave the original dataset unchanged. Replacement is atomic per file, not across the whole dataset. A crash during replacement can require resuming the task, and temporary output requires extra disk space. Tables are read into memory; processing is not a bounded-memory streaming pipeline.

## Download folders

Category letters are `L` (lidars), `C` (cameras), `R` (radars), `O` (other), `T` (topics), and `M` (metadata). Partial selections append their letters. With all six selected and no trim, the dataset name is unchanged. A message selection with a time cut also appends `HHMMSS-HHMMSS`.

Folders are written to the configured local download directory. The API does not provide a dedicated endpoint to transfer completed files to the browser.

## Persistence and recovery

- `.download-state.json` stores task parameters and S3 keys, including exact trim timestamps.
- `.download-complete.json` is written after successful post-processing and records output names and sizes.
- Recovery validates that completion record. Missing/truncated output and legacy folders without a record are shown as stopped.
- Size checks do not detect corruption that preserves file size.
- Post-processing failures appear as queue errors and do not mark a dataset complete.

The queue itself is in memory; recovery reconstructs tasks from local folders. Recovery can discover folders without fetching the S3 index, while resuming a download still needs access to its source objects.

## HTTP API

Interactive OpenAPI documentation is available at `/docs`.

| Method | Route | Purpose |
| --- | --- | --- |
| GET | `/api/datasets` | Paginated dataset listing and search |
| POST | `/api/datasets/{name}/scan` | Resolve current files and sizes |
| GET | `/api/datasets/{name}/metadata` | Parsed metadata and time hints |
| POST | `/api/datasets/refresh` | Refresh the S3 dataset index |
| GET / POST | `/api/queue` | Read queue / add a task |
| POST | `/api/queue/{id}/stop` | Stop a task |
| POST | `/api/queue/{id}/resume` | Resume a stopped or failed task |
| DELETE | `/api/queue/{id}` | Remove an inactive task |
| DELETE | `/api/queue/done` | Remove completed tasks |
| POST | `/api/queue/rescan` | Recover tasks from local folders |
| GET / PUT | `/api/download-path` | Read / change the download directory |
| GET | `/api/health` | Backend and S3 health |

## Environment variables

| Variable | Purpose |
| --- | --- |
| `S3_ENDPOINT` | Storage endpoint; backend default is `http://localhost:9000` |
| `S3_BUCKET` | Dataset bucket; default is `demo-datasets` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Storage credentials |
| `DOWNLOAD_BASE` | Local directory used by the backend |
| `DOWNLOAD_DISPLAY` | Host path displayed in the interface |
| `HOST_HOME_MOUNT` | Container path corresponding to the mounted host home |
| `INIT_DEMO_DATA` | Startup demo initialization: Compose defaults to `1`, standalone container to `0` |

Compose sets container and display paths separately: `/home/appuser/host_home/work_folder` and `~/work_folder`. The bare backend uses `~/work_folder` when it exists, otherwise the home directory; set `DOWNLOAD_BASE` explicitly during development.
