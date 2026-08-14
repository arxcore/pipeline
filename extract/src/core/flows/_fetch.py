from __future__ import annotations
from typing import TYPE_CHECKING
from core.flows._utils import PipelineFilter, aplay_filters, export_json
from core.models.pipeline_schemas import (
    FetchBatchResult,
    FileResult,
    ApiResult,
)
from collections.abc import Coroutine
from typing import Any
import asyncio
import logging


if TYPE_CHECKING:
    from .manager import FlowsManager
logger = logging.getLogger(__name__)


async def run_all(
    manager: FlowsManager,
    country: str | None = None,
    indicator: str | None = None,
    source: list[str] | None = None,
) -> FetchBatchResult | None:
    filter: PipelineFilter = PipelineFilter(country, indicator, source)
    return await manager.fetch_config_indicators(filter)


async def fetch_config_indicators(manager: FlowsManager, filter: PipelineFilter):
    """
    Running ALLConfig Data
    """
    # TODO:
    # DB Traking

    # create task for each indicator and run them concurrently
    tasks: list[Coroutine[Any, Any, ApiResult | FileResult | None]] = []
    tasks_names: list[dict[str, str]] = []
    filters_indicators = await aplay_filters(manager.all_indicators, filter)
    try:
        # Iterate through ALL_INDICATORS and create tasks for each indicator
        for country, categories in filters_indicators.items():
            for category, indicators in categories.items():
                for indicators_name, meta in indicators.items():
                    # indicator: US_NFP, Unemploy
                    # meta: url, id, calc, etc..``
                    tasks.append(
                        manager.fetch_api.process_raw_data(
                            indicators_name, meta, category, country
                        )
                    )
                    tasks_names.append(
                        {
                            "name": indicators_name,
                            "source": meta.source,
                            "country": country,
                            "category": category,
                        }
                    )
        # Run all tasks concurrently and gather results
        results: list[
            ApiResult | FileResult | BaseException | None
        ] = await asyncio.gather(*tasks, return_exceptions=True)

        valid_data: list[ApiResult] = []
        valid_path: list[FileResult] = []
        skipped_count = 0
        error_count = 0
        success_count = 0

        # Process results, handling exceptions and collecting successful results
        for i, result in enumerate(results):
            tasks_info = tasks_names[i]
            if isinstance(result, BaseException):
                logger.exception(
                    "Error task, skiping %s indicator..",
                    tasks_info["name"],
                )
                error_count += 1

                continue
            if isinstance(result, FileResult):
                success_count += 1
                valid_path.append(result)
                continue

            if result is None:
                logger.warning(
                    "No data processed from %s, indicator %s, skipping..",
                    tasks_info["source"],
                    tasks_info["name"],
                )
                skipped_count += 1
                continue
            # result is valid ApiResult
            success_count += 1
            valid_data.append(result)

        if not valid_data and not valid_path:
            logger.warning("No valid data processed, skipping..")
            return None

        logger.info("-" * 50)
        logger.info("Pipeline Summary:")
        logger.info("   >> Total Indicators Processed: %s", len(results))
        logger.info("   >> Successfully Processed: %s Indicators", success_count)
        logger.info("   >> Skipped Failed Indicators: %s", skipped_count)
        logger.info("   >> Failed Indicators: %s", error_count)

        return FetchBatchResult(file=valid_path, apis=valid_data)
    except Exception as e:
        logger.exception("Pipeline process carsh during operation %s", str(e))
        raise


async def run_all_chain(
    manager: FlowsManager,
    source: list[str],
    country: str,
    indicator: str,
):
    """Running all indicator with all chain process, from fetch, loadraw, parse, staging"""
    # raw data from API

    raw = await manager.run_all(country, indicator, source)
    # load raw data
    if raw is None:
        logger.warning("No data to process for all indicators, skipping...")
        return None
    # load raw respons
    await manager.load_raw_result(raw, indicator)

    # parse data from raw data
    await manager.parsing_all_db(source, country, indicator, persist_stg=True)


async def orchest_all_fetch(
    manager: FlowsManager,
    source: list[str],
    persist_raw: bool = False,
    country: str | None = None,
    indicator: str | None = None,
):
    """Running all process of indicators"""

    logger.info(
        "Filter: %s  %s  %s",
        source if source else "",
        country if country else "",
        indicator if indicator else "",
    )
    data: FetchBatchResult | None = await manager.run_all(country, indicator, source)
    try:
        if data is None:
            logger.warning(
                "No data found for Name %s, country %s, source %s",
                indicator,
                country,
                source,
            )
        if persist_raw:
            logger.debug("type data %s", type(data))
            logger.info("persist_raw Enabled")
            return await manager.load_raw_result(data, indicator)

        logger.info("Skipping persist_raw")
        return data

    except Exception as e:
        logger.exception("Unexpected Error %s", e)
        raise


async def replaying_raw_data(
    manager: FlowsManager,
    source: list[str],
    country: str | None = None,
    indicator: str | None = None,
):
    """replaying raw data from database"""
    logger.info("Replaying data from database for all indicators...")
    db_raw = await manager.fetch_db.fetch_from_database(source, country, indicator)

    if db_raw is None:
        logger.warning(
            "no raw data found in database for country %s, indicator %s, source %s",
            country,
            indicator,
            source,
        )
        return None

    logger.info("exporting data to json")
    return await export_json(db_raw, country, indicator)


async def load_raw_result(
    manager: FlowsManager, fetch_batch: FetchBatchResult | None, indicator: str | None
):
    try:
        if fetch_batch is None:
            logger.warning("Skiping load raw no data found %s", indicator)
            return None
        if fetch_batch:
            logger.debug(
                "Type file_path %s, Type apis %s",
                type(fetch_batch.file),
                type(fetch_batch.apis),
            )
            if fetch_batch.apis and fetch_batch.file:
                await manager.load_raw.load_path(fetch_batch.file)
                await manager.load_raw.load_raw_respons(
                    [i.model_dump() for i in fetch_batch.apis]
                )

            elif fetch_batch.file:
                await manager.load_raw.load_path(fetch_batch.file)
            elif fetch_batch.apis:
                await manager.load_raw.load_raw_respons(
                    [i.model_dump() for i in fetch_batch.apis]
                )

    except (TypeError, ValueError) as e:
        logger.error("Unexpected Error  got %s", str(e))
        raise
