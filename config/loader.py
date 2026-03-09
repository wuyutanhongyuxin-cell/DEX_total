"""
Configuration loader — reads YAML configs and resolves env vars.
"""

import os
import logging
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent
PROJECT_ROOT = CONFIG_DIR.parent

# Load .env from project root
load_dotenv(PROJECT_ROOT / ".env")


def load_yaml(filename: str) -> dict:
    """Load a YAML config file from the config directory."""
    path = CONFIG_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def get_exchanges_config() -> dict:
    """Load exchanges.yaml and resolve env var references for API keys."""
    config = load_yaml("exchanges.yaml")
    exchanges = config.get("exchanges", {})

    for name, ex_cfg in exchanges.items():
        # Resolve env var references
        for key in list(ex_cfg.keys()):
            if key.endswith("_env"):
                env_name = ex_cfg[key]
                resolved = os.getenv(env_name, "")
                # Store resolved value without _env suffix
                target_key = key[:-4]  # strip "_env"
                ex_cfg[target_key] = resolved
    return exchanges


def get_collection_config() -> dict:
    """Load collection.yaml."""
    return load_yaml("collection.yaml")


def get_strategy_config() -> dict:
    """Load strategy.yaml."""
    return load_yaml("strategy.yaml")


# 已警告过的 symbol mapping 缺失记录，避免每秒刷日志
_symbol_mapping_warned: set[tuple[str, str]] = set()


def get_symbol_for_exchange(exchange_name: str, normalized_symbol: str) -> str:
    """Map a normalized symbol (e.g. BTC-PERP) to exchange-specific format."""
    config = get_collection_config()
    symbol_map = config.get("symbol_map", {})
    exchange_map = symbol_map.get(exchange_name, {})
    mapped = exchange_map.get(normalized_symbol)
    if not mapped:
        key = (exchange_name, normalized_symbol)
        if key not in _symbol_mapping_warned:
            logger.warning(f"No symbol mapping for {exchange_name}:{normalized_symbol}, using raw")
            _symbol_mapping_warned.add(key)
        return normalized_symbol
    return mapped
