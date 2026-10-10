"""Durable task parameters and explicit completion records for disk recovery."""
import json
import os
from pathlib import Path

STATE_FILE = ".download-state.json"
COMPLETE_FILE = ".download-complete.json"


def _write_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def read_state(folder: Path):
    try:
        value = json.loads((folder / STATE_FILE).read_text(encoding="utf-8"))
        if (value.get("version") == 1
                and isinstance(value.get("dataset_name"), str)
                and isinstance(value.get("params"), dict)
                and isinstance(value.get("keys"), list)
                and all(isinstance(key, str) for key in value["keys"])):
            return value
    except (OSError, ValueError, AttributeError):
        pass
    return None


def save_state(folder: Path, dataset_name: str, params: dict, keys: list) -> None:
    (folder / COMPLETE_FILE).unlink(missing_ok=True)
    _write_json(folder / STATE_FILE, {
        "version": 1, "dataset_name": dataset_name, "params": params, "keys": keys,
    })


def _inventory(folder: Path) -> dict:
    return {path.name: path.stat().st_size for path in folder.iterdir()
            if path.is_file() and (path.suffix == ".parquet" or path.name == "metadata.yaml")}


def mark_complete(folder: Path) -> None:
    files = _inventory(folder)
    if not files or any(size == 0 for size in files.values()):
        raise ValueError("Cannot complete an empty or zero-length download")
    _write_json(folder / COMPLETE_FILE, {"version": 1, "files": files})


def is_complete(folder: Path) -> bool:
    """Only a successful finalization can mark a folder complete.

    File sizes catch missing/truncated output; this is not a checksum validator.
    Legacy folders without a record are conservatively recovered as stopped.
    """
    try:
        value = json.loads((folder / COMPLETE_FILE).read_text(encoding="utf-8"))
        files = value.get("files")
        return (value.get("version") == 1 and isinstance(files, dict)
                and bool(files) and all(type(size) is int and size > 0 for size in files.values())
                and files == _inventory(folder))
    except (OSError, ValueError, AttributeError):
        return False
