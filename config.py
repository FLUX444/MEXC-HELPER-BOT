"""Configuration from YAML config files and optional .env."""
from __future__ import annotations

import os
from pathlib import Path

# Base dir: project root (parent of this file)
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"

# Load .env if present (optional)
_env_path = BASE_DIR / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _load_yaml(name: str) -> dict:
    path = CONFIG_DIR / name
    if not path.exists():
        return {}
    try:
        import yaml
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data or {}
    except Exception:
        return {}


def _get_keys() -> dict:
    return _load_yaml("keys.yml") or _load_yaml("keys.example.yml")


def _get_messages() -> dict:
    return _load_yaml("messages.yml")


def _get_settings() -> dict:
    return _load_yaml("settings.yml")


_keys = _get_keys()
_messages = _get_messages()
_settings = _get_settings()
_scanner = _settings.get("scanner") or {}

# Обязательный Redis (антидубли main + prealert в SETNX как задумано)
_raw_req = _scanner.get("require_redis", True)
if isinstance(_raw_req, str):
    REQUIRE_REDIS = _raw_req.strip().lower() in ("true", "1", "yes")
elif isinstance(_raw_req, bool):
    REQUIRE_REDIS = _raw_req
else:
    REQUIRE_REDIS = True

# Keys (secrets)
_telegram = _keys.get("telegram") or {}
_mexc_keys = _keys.get("mexc") or {}
_db_cfg = _keys.get("db") or {}

TELEGRAM_BOT_TOKEN = (_telegram.get("bot_token") or "").strip() or os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = str(_telegram.get("chat_id") or "").strip() or os.environ.get("TELEGRAM_CHAT_ID", "")
MARKET_MOVERS_CHAT_ID = (
    str(_telegram.get("market_movers_chat_id") or "").strip()
    or os.environ.get("TELEGRAM_MARKET_MOVERS_CHAT_ID", "")
)

# БД: use_redis true → Redis по redis_url, false → локальный SQLite по sqlite_path
_use_redis = _db_cfg.get("use_redis")
if isinstance(_use_redis, str):
    _use_redis = _use_redis.strip().lower() in ("true", "1", "yes")
elif not isinstance(_use_redis, bool):
    _use_redis = False
_redis_url_from_cfg = (_db_cfg.get("redis_url") or "").strip()
_sqlite_path_from_cfg = (_db_cfg.get("sqlite_path") or "").strip()
# env переопределяет: REDIS_URL, SQLITE_PATH
if REQUIRE_REDIS:
    _use_redis = True
    REDIS_URL = os.environ.get("REDIS_URL", "").strip() or _redis_url_from_cfg
    SQLITE_PATH = ""
else:
    REDIS_URL = os.environ.get("REDIS_URL", "").strip() or (_redis_url_from_cfg if _use_redis else "")
    SQLITE_PATH = os.environ.get("SQLITE_PATH", "").strip() or (_sqlite_path_from_cfg if not _use_redis else "")
MEXC_API_KEY = (_mexc_keys.get("api_key") or "").strip() or os.environ.get("MEXC_API_KEY", "")
MEXC_SECRET_KEY = (_mexc_keys.get("secret_key") or "").strip() or os.environ.get("MEXC_SECRET_KEY", "")

# Settings (scanner уже загружен выше)
_mexc_cfg = _settings.get("mexc") or {}

MEXC_REST_BASE = (_mexc_cfg.get("rest_base") or "https://api.mexc.com").strip()
MEXC_WS_URL = (_mexc_cfg.get("ws_url") or "wss://contract.mexc.com/edge").strip()
MEXC_KLINE_INTERVAL = (_mexc_cfg.get("kline_interval") or "Min60").strip()
MEXC_KLINE_BATCH = int(_scanner.get("kline_batch") or os.environ.get("MEXC_KLINE_BATCH", "3"))
MEXC_KLINE_DELAY = float(_scanner.get("kline_delay") or 0.35)
# Всегда RSI3(24) для совпадения с графиком MEXC; иное значение из конфига не используется
RSI_PERIOD = 24
RSI_THRESHOLD_1H = float(_scanner.get("rsi_threshold_1h") or 90.0)
RSI_THRESHOLD_4H = float(_scanner.get("rsi_threshold_4h") or 90.0)
RSI_PREALERT_MIN = float(_scanner.get("rsi_prealert_min") or 85.0)
MIN_ALERT_DELAY_1H_SEC = max(0, int(_scanner.get("min_alert_delay_1h_sec") or 30))
MIN_ALERT_DELAY_4H_SEC = max(0, int(_scanner.get("min_alert_delay_4h_sec") or 60))
KLINE_HISTORY_COUNT = int(_scanner.get("kline_history_count") or 30)
WS_PING_INTERVAL = int(_scanner.get("ws_ping_interval") or 15)
MARKET_MOVERS_INTERVAL_SEC = int(_scanner.get("market_movers_interval_sec") or 120)
MARKET_MOVERS_MIN_RISE_PCT = float(_scanner.get("market_movers_min_rise_pct") or 5.0)
MARKET_MOVERS_TOP_N = int(_scanner.get("market_movers_top_n") or 25)
MARKET_MOVERS_NEW_COOLDOWN_SEC = int(_scanner.get("market_movers_new_cooldown_sec") or 1800)
MARKET_MOVERS_ALERT_DELAY_SEC = float(_scanner.get("market_movers_alert_delay_sec") or 4)
MARKET_MOVERS_MAX_PER_CYCLE = int(_scanner.get("market_movers_max_per_cycle") or 5)

# Продакшн: WS stall, backoff, backup RSI, circuit breaker, heartbeat
def _float_list(key: str, default: list[float]) -> list[float]:
    raw = _scanner.get(key)
    if isinstance(raw, list) and raw:
        try:
            return [float(x) for x in raw]
        except (TypeError, ValueError):
            pass
    return default


WS_STALL_TIMEOUT_SEC = float(_scanner.get("ws_stall_timeout_sec") or 15.0)
WS_RECONNECT_BACKOFF_SEC = _float_list("ws_reconnect_backoff_sec", [5.0, 10.0, 20.0, 60.0])
BACKUP_RSI_INTERVAL_SEC = float(_scanner.get("backup_rsi_interval_sec") or 8.0)
BACKUP_STALE_SYMBOL_SEC = float(_scanner.get("backup_stale_symbol_sec") or 25.0)
BACKUP_REST_CONCURRENCY = int(_scanner.get("backup_rest_concurrency") or 3)
BACKUP_BATCH_SIZE = int(_scanner.get("backup_batch_size") or 80)
WATCHDOG_INTERVAL_SEC = float(_scanner.get("watchdog_interval_sec") or 10.0)
WATCHDOG_WARN_AFTER_SEC = float(_scanner.get("watchdog_warn_after_sec") or 25.0)
HEARTBEAT_INTERVAL_SEC = float(_scanner.get("heartbeat_interval_sec") or 60.0)
HEARTBEAT_FILE = (_scanner.get("heartbeat_file") or "/tmp/mexc_scanner_heartbeat").strip()
TIME_SYNC_INTERVAL_SEC = float(_scanner.get("time_sync_interval_sec") or 300.0)
WS_CIRCUIT_FAIL_THRESHOLD = int(_scanner.get("rest_circuit_fail_threshold") or 8)
WS_CIRCUIT_WINDOW_SEC = float(_scanner.get("rest_circuit_window_sec") or 60.0)
WS_CIRCUIT_OPEN_SEC = float(_scanner.get("rest_circuit_open_sec") or 45.0)
LOG_DIR = (BASE_DIR / (_scanner.get("log_dir") or "logs")).resolve()
LOG_FILE_MAX_MB = int(_scanner.get("log_file_max_mb") or 10)
LOG_FILE_BACKUP_COUNT = int(_scanner.get("log_file_backup_count") or 5)
TELEGRAM_SEND_RETRIES = max(1, int(_scanner.get("telegram_send_retries") or 3))
TELEGRAM_RETRY_BASE_DELAY_SEC = float(_scanner.get("telegram_retry_base_delay_sec") or 1.5)

# Messages (for telegram_notify)
MESSAGES = _messages
