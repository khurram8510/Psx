"""Configuration: YAML file, overridden by environment variables for secrets and deployment knobs."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator

DEFAULT_SESSIONS: dict[str, list[tuple[str, str]]] = {
    "mon": [("09:30", "15:30")],
    "tue": [("09:30", "15:30")],
    "wed": [("09:30", "15:30")],
    "thu": [("09:30", "15:30")],
    "fri": [("09:15", "12:00"), ("14:30", "16:30")],
}


class SourceConfig(BaseModel):
    mode: Literal["live", "simulated"] = "live"
    base_url: str = "https://dps.psx.com.pk"
    symbol: str = "KMI30"
    poll_interval_s: float = Field(10.0, ge=3.0)
    request_timeout_s: float = Field(15.0, gt=0)
    max_backoff_s: float = Field(120.0, gt=0)
    # PSX encodes timestamps as Pakistan wall-clock seconds. "auto" detects this per response.
    timestamp_mode: Literal["auto", "pkt_wallclock", "utc"] = "auto"
    user_agent: str = "Mozilla/5.0 (X11; Linux x86_64) kmi30-agent/0.1"
    # Simulator speed-up factor, only used when mode == "simulated".
    sim_speed: float = Field(30.0, gt=0)


class MarketConfig(BaseModel):
    timezone: str = "Asia/Karachi"
    sessions: dict[str, list[tuple[str, str]]] = Field(default_factory=lambda: dict(DEFAULT_SESSIONS))
    holidays: list[str] = Field(default_factory=list)
    close_grace_min: int = Field(5, ge=0)

    @field_validator("sessions")
    @classmethod
    def _valid_days(cls, v: dict[str, list[tuple[str, str]]]) -> dict[str, list[tuple[str, str]]]:
        allowed = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
        bad = set(v) - allowed
        if bad:
            raise ValueError(f"unknown weekday keys in market.sessions: {sorted(bad)}")
        return v


class LevelConfig(BaseModel):
    enabled: bool = True
    warning_pct: float = 1.0
    critical_pct: float = 2.0
    hysteresis_pct: float = 0.25


class VelocityConfig(BaseModel):
    enabled: bool = True
    window_min: int = Field(5, ge=1)
    warning_z: float = 3.0
    critical_z: float = 4.5
    rearm_z: float = 1.5


class TrendConfig(BaseModel):
    enabled: bool = True
    fast: int = Field(5, ge=2)
    slow: int = Field(20, ge=3)
    confirm_bars: int = Field(2, ge=1)
    slope_window: int = Field(10, ge=3)
    min_separation_pct: float = 0.02
    min_separation_sigma: float = 3.0


class CusumConfig(BaseModel):
    enabled: bool = True
    k: float = 0.5
    h: float = 5.0


class VolRegimeConfig(BaseModel):
    enabled: bool = True
    window_min: int = Field(15, ge=2)
    lookback_days: int = Field(20, ge=1)
    min_history_days: int = Field(5, ge=1)
    warning_ratio: float = 2.0
    critical_ratio: float = 3.0
    rearm_ratio: float = 1.5
    baseline_lambda: float = Field(0.985, gt=0, lt=1)


class FeedConfig(BaseModel):
    enabled: bool = True
    stale_min: float = 3.0
    no_data_after_open_min: float = 30.0
    failure_streak: int = 5


class DetectionConfig(BaseModel):
    warmup_min: int = Field(15, ge=0)
    cooldown_min: int = Field(15, ge=0)
    ewma_lambda: float = Field(0.94, gt=0, lt=1)
    eod_vol_lookback: int = Field(20, ge=5)
    level: LevelConfig = LevelConfig()
    velocity: VelocityConfig = VelocityConfig()
    trend: TrendConfig = TrendConfig()
    cusum: CusumConfig = CusumConfig()
    vol_regime: VolRegimeConfig = VolRegimeConfig()
    feed: FeedConfig = FeedConfig()


class SlackConfig(BaseModel):
    webhook_url: str | None = None
    channel_label: str = "KMI-30 Monitor"
    dashboard_url: str | None = None
    min_severity: Literal["info", "warning", "critical"] = "info"
    timeout_s: float = 10.0
    max_retries: int = 4


class WebConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080


class StorageConfig(BaseModel):
    path: str = "data/kmi30.db"
    retention_days: int = Field(120, ge=7)


class Settings(BaseModel):
    source: SourceConfig = SourceConfig()
    market: MarketConfig = MarketConfig()
    detection: DetectionConfig = DetectionConfig()
    slack: SlackConfig = SlackConfig()
    web: WebConfig = WebConfig()
    storage: StorageConfig = StorageConfig()
    log_level: str = "INFO"


_ENV_MAP: dict[str, tuple[str, ...]] = {
    "SLACK_WEBHOOK_URL": ("slack", "webhook_url"),
    "KMI30_DASHBOARD_URL": ("slack", "dashboard_url"),
    "KMI30_SOURCE_MODE": ("source", "mode"),
    "KMI30_POLL_INTERVAL_S": ("source", "poll_interval_s"),
    "KMI30_DB_PATH": ("storage", "path"),
    "KMI30_WEB_PORT": ("web", "port"),
    "KMI30_LOG_LEVEL": ("log_level",),
}


def load_settings(path: str | os.PathLike | None = None) -> Settings:
    """Load settings from YAML (optional) and apply environment overrides."""
    raw: dict = {}
    cfg_path = path or os.environ.get("KMI30_CONFIG")
    if cfg_path:
        p = Path(cfg_path)
        if not p.is_file():
            raise FileNotFoundError(f"config file not found: {p}")
        raw = yaml.safe_load(p.read_text()) or {}

    for env, keys in _ENV_MAP.items():
        val = os.environ.get(env)
        if val is None or val == "":
            continue
        node = raw
        for k in keys[:-1]:
            node = node.setdefault(k, {})
        node[keys[-1]] = val
    return Settings.model_validate(raw)
