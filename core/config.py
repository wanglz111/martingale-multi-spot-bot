import os
from dataclasses import dataclass
from typing import Any, Dict

import yaml


@dataclass
class Config:
    """Container object wrapping nested configuration dictionaries."""

    raw: Dict[str, Any]

    def section(self, name: str, default: Any = None) -> Any:
        return self.raw.get(name, default)

    def __getitem__(self, item: str) -> Any:
        return self.raw[item]


def _expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    if isinstance(value, str):
        return os.path.expandvars(value)
    return value


def load_config(path: str) -> Config:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    expanded = _expand_env(data)
    return Config(expanded)


__all__ = ["Config", "load_config"]
