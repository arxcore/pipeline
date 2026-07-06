from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any
import uuid
from config.metadata.load_yaml import AllIndicatorsModel
from core.models.pipeline_schemas import ApiResult, FileResult
import logging

logger = logging.getLogger(__name__)


@dataclass
class PipelineFilter:
    country: str | None = None
    name: str | None = None
    source: list[str] | None = None


async def aplay_filters(
    all_indicator: AllIndicatorsModel,
    filter: PipelineFilter,
) -> AllIndicatorsModel:
    result: dict[str, Any] = {}
    for country, categories in all_indicator.items():
        if filter.country and country.lower() != filter.country:
            continue
        filtered_categori: dict[str, Any] = {}
        for category, indicators in categories.items():
            filtered_indicators: dict[str, Any] = {}
            for indicators_name, meta in indicators.items():
                if filter.name and indicators_name.lower() != filter.name.lower():
                    continue
                if filter.source and meta.source.lower() not in filter.source:
                    continue
                filtered_indicators[indicators_name] = meta

            if filtered_indicators:
                filtered_categori[category] = filtered_indicators

        if filtered_categori:
            result[country] = filtered_categori

    return result


async def export_json(
    data: tuple[list[FileResult] | None, list[ApiResult] | None],
    country: str | None = None,
    name: str | None = "datas",
) -> None:
    """Export data to json file for debugging and testing purpose"""

    if country is None:
        raise ValueError("Country is required for exporting data")
    if name is None:
        raise ValueError("Name is required for exporting data")

    uniq = uuid.uuid4().hex[:10]
    timestamp = datetime.now().strftime("%Y-%m-%d")
    directory = Path("exported_data")
    filename = f"{name}_{uniq}_{timestamp}.json"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / country / filename
    try:
        file_based: list[FileResult] | None
        api_based: list[ApiResult] | None
        file_based, api_based = data

        if file_based:
            logger.info("Exporting file_based data to json")
            for item in file_based:
                serialize = item.model_dump(mode="json")
                with open(path, "w") as f:
                    json.dump(serialize, f, indent=4)

        if api_based:
            logger.info("Exporting api_based data to json")
            for item in api_based:
                serialize = item.model_dump(mode="json")
                with open(path, "w") as f:
                    json.dump(serialize, f, indent=4)

    except Exception as e:
        logger.exception("Failed to export data to JSON: %s", e)
        raise
    logger.info("Data exported to %s", path)
