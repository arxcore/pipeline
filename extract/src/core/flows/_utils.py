from dataclasses import dataclass
from typing import Any
from config.metadata.load_yaml import AllIndicatorsModel


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
