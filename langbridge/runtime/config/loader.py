
from pathlib import Path

import yaml

from .models import LocalRuntimeConfig
from .normalizers import normalize_runtime_config


def load_runtime_config(config_path: str | Path) -> LocalRuntimeConfig:
    path = Path(config_path).resolve()
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    config = LocalRuntimeConfig.model_validate(payload)
    return normalize_runtime_config(config=config, config_path=path)
