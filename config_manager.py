import json
import os
import tempfile
from threading import RLock
from typing import Dict, Any, List
from config_schema import AppConfig

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.json")
TASKS_FILE = os.path.join(os.path.dirname(__file__), "tasks.json")
_storage_lock = RLock()

DEFAULT_CONFIG = AppConfig().model_dump()

def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        # Never silently replace damaged credentials or task history with defaults.
        raise ValueError(f"Invalid JSON in {path}; restore or repair the file") from exc


def _save_json(path: str, data: Any) -> None:
    # Serialize before touching disk, then replace from the same filesystem.
    content = json.dumps(data, indent=2, ensure_ascii=False)
    directory = os.path.dirname(os.path.abspath(path))
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=directory,
            prefix=f".{os.path.basename(path)}.", suffix=".tmp", delete=False,
        ) as f:
            temporary_path = f.name
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None and os.path.exists(temporary_path):
            os.unlink(temporary_path)


def load_config() -> Dict[str, Any]:
    with _storage_lock:
        data = _load_json(CONFIG_FILE, {})
        if not isinstance(data, dict):
            raise ValueError(f"Expected a JSON object in {CONFIG_FILE}")
        return {**DEFAULT_CONFIG, **data}

def save_config(cfg: Dict[str, Any]):
    with _storage_lock:
        current = load_config()
        current.update(cfg)
        _save_json(CONFIG_FILE, current)

def load_tasks() -> List[Dict[str, Any]]:
    with _storage_lock:
        data = _load_json(TASKS_FILE, [])
        if not isinstance(data, list) or not all(isinstance(task, dict) for task in data):
            raise ValueError(f"Expected a JSON array of task objects in {TASKS_FILE}")
        return data

def save_tasks(tasks: List[Dict[str, Any]]):
    with _storage_lock:
        _save_json(TASKS_FILE, tasks)
