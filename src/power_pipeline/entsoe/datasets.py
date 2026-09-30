"""ENTSO-E request definitions: zones, borders and datasets from config, and request parameters."""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import yaml

# config/ at the repo root. How Databricks jobs find it is decided in Phase 01.
DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"
PERIOD_FORMAT = "%Y%m%d%H%M"


@dataclass(frozen=True)
class Zone:
    code: str
    name: str
    eic: str


@dataclass(frozen=True)
class Border:
    """One direction across a border, for example CH to DE_LU."""

    from_zone: str
    to_zone: str


@dataclass(frozen=True)
class Dataset:
    name: str
    description: str
    params: dict[str, str]
    scope: Literal["zone", "border"] = "zone"


def load_zones(config_dir: Path = DEFAULT_CONFIG_DIR) -> dict[str, Zone]:
    """Zones from zones.yaml, keyed by zone code such as DE_LU."""
    raw = yaml.safe_load((config_dir / "zones.yaml").read_text(encoding="utf-8"))
    return {
        code: Zone(code=code, name=zone["name"], eic=zone["eic"])
        for code, zone in raw["zones"].items()
    }


def load_borders(config_dir: Path = DEFAULT_CONFIG_DIR) -> list[Border]:
    """Border directions from zones.yaml, in file order."""
    raw = yaml.safe_load((config_dir / "zones.yaml").read_text(encoding="utf-8"))
    return [Border(from_zone=b["from"], to_zone=b["to"]) for b in raw["borders"]]


def load_datasets(config_dir: Path = DEFAULT_CONFIG_DIR) -> dict[str, Dataset]:
    """Dataset request definitions from datasets.yaml, keyed by dataset name."""
    raw = yaml.safe_load((config_dir / "datasets.yaml").read_text(encoding="utf-8"))
    datasets = {}
    for name, spec in raw["datasets"].items():
        scope = spec.get("scope", "zone")
        if scope not in ("zone", "border"):
            raise ValueError(f"Dataset {name} has unknown scope {scope!r}.")
        datasets[name] = Dataset(
            name=name,
            description=spec["description"],
            params={key: str(value) for key, value in spec["params"].items()},
            scope=scope,
        )
    return datasets


def format_period(moment: datetime) -> str:
    """Format a timestamp the way ENTSO-E expects periodStart and periodEnd: UTC yyyyMMddHHmm."""
    if moment.tzinfo is None:
        raise ValueError("Period timestamps must be timezone-aware.")
    if moment.second or moment.microsecond:
        raise ValueError(f"Period timestamps must fall on a whole minute, got {moment}.")
    return moment.astimezone(UTC).strftime(PERIOD_FORMAT)


def _render(
    dataset: Dataset,
    placeholders: dict[str, str],
    window_start_utc: datetime,
    window_end_utc: datetime,
) -> dict[str, str]:
    if window_end_utc <= window_start_utc:
        raise ValueError("The window must end after it starts.")
    params = {}
    for key, template in dataset.params.items():
        try:
            params[key] = template.format_map(placeholders)
        except KeyError as exc:
            raise ValueError(f"Unknown placeholder {exc} in {dataset.name} param {key}.") from None
    params["periodStart"] = format_period(window_start_utc)
    params["periodEnd"] = format_period(window_end_utc)
    return params


def build_params(
    dataset: Dataset, zone: Zone, window_start_utc: datetime, window_end_utc: datetime
) -> dict[str, str]:
    """Query parameters for one zone request. The token is never part of them."""
    if dataset.scope != "zone":
        raise ValueError(f"{dataset.name} is requested per border, not per zone.")
    return _render(dataset, {"eic": zone.eic}, window_start_utc, window_end_utc)


def build_border_params(
    dataset: Dataset,
    from_zone: Zone,
    to_zone: Zone,
    window_start_utc: datetime,
    window_end_utc: datetime,
) -> dict[str, str]:
    """Query parameters for one border direction. The token is never part of them."""
    if dataset.scope != "border":
        raise ValueError(f"{dataset.name} is requested per zone, not per border.")
    placeholders = {"from_eic": from_zone.eic, "to_eic": to_zone.eic}
    return _render(dataset, placeholders, window_start_utc, window_end_utc)
