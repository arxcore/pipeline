from __future__ import annotations
from typing import TYPE_CHECKING
from core.flows._utils import PipelineFilter, aplay_filters
from core.models.pipeline_schemas import FinalresultFetcher
from collections.abc import Coroutine
from typing import Any
import asyncio
import logging
import monitoring.exc_models as exc

if TYPE_CHECKING:
    from .manager import FlowsManager
logger = logging.getLogger(__name__)


async def run_all(
    manager: FlowsManager,
    source: list[str] | None = None,
) -> list[FinalresultFetcher] | None:
    filter: PipelineFilter = PipelineFilter(source=source)
    return await fetch_config_indicators(manager, filter)


async def fetch_config_indicators(manager: FlowsManager, filter: PipelineFilter):
    """
    Running ALLConfig Data
    """
    # TODO:
    # DB Traking

    # create task for each indicator and run them concurrently
    tasks: list[Coroutine[Any, Any, FinalresultFetcher | None]] = []
    tasks_names: list[dict[str, str]] = []
    indicators = await aplay_filters(manager.all_indicators, filter)
    try:
        # Iterate through ALL_INDICATORS and create tasks for each indicator
        for country, categories in indicators.items():
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
        results: list[FinalresultFetcher | BaseException | None] = await asyncio.gather(
            *tasks, return_exceptions=True
        )

        valid_data: list[FinalresultFetcher] = []
        skipped_count = 0
        error_count = 0
        success_count = 0

        # Process results, handling exceptions and collecting successful results
        for i, result in enumerate(results):
            tasks_info = tasks_names[i]
            if isinstance(result, BaseException):
                logger.error("Error task, skiping indicator %s", result, exc_info=True)
                error_count += 1
                continue
            elif result is None:
                logger.warning(
                    "No data processed from %s, indicator %s, skipping...",
                    tasks_info["source"],
                    tasks_info["name"],
                )
                skipped_count += 1
                continue
            # result is valid FinalresultFetcher
            success_count += 1
            valid_data.append(result)

        if not valid_data:
            logger.warning("No valid data processed, skipping..")
            return

        logger.info("-" * 50)
        logger.info("Pipeline Summary:")
        logger.info("   >> Total Indicators Processed: %s", len(results))
        logger.info("   >> Successfully Processed: %s Indicators", success_count)
        logger.info("   >> Skipped Indicators: %s", skipped_count)
        logger.info("   >> Failed Indicators: %s", error_count)

        return valid_data
    except exc.PipelineCrash:
        logger.exception("Pipeline process carsh during operation")
        raise


async def run_all_chain(manager: FlowsManager, source: list[str], export_json: bool):
    """Running all indicator with all chain process, from fetch, loadraw, parse, staging"""
    # raw data from API
    raw = await manager.run_all()
    # load raw data
    if raw is None:
        logger.warning("No data to process for all indicators, skipping...")
        return None

    await manager.load_raw.create_raw_respons_table()
    await manager.load_raw.load_raw_respons([data.fetch_result for data in raw])

    # parse data from raw data
    await manager.parsing_all_db(export_json, persist_stg=True)


async def single_fetch(
    manager: FlowsManager, name: str, country: str
) -> list[FinalresultFetcher] | None:
    data: list[FinalresultFetcher] = []
    for category, indicators in manager.all_indicators[country].items():
        for indicator_name, meta in indicators.items():
            if indicator_name != name:
                continue
            try:
                records = await manager.fetch_api.process_raw_data(
                    indicator_name, meta, category, country
                )

                if isinstance(records, BaseException) or records is None:
                    logger.warning(
                        "No data processed for Indicator %s, Source %s, Code %s",
                        indicator_name,
                        meta.source,
                        meta.code_name,
                    )
                    raise exc.ProcessingFailed(
                        f"Processing Failed for Indicator {indicator_name}"
                    )

                data.append(records)
            except exc.ProcessingFailed:
                logger.exception("Failed to Procesed Indicators")
                logger.warning("skipping  Indicators: %s", indicator_name)
                continue

    if len(data) == 0:
        logger.warning("No data processed for %s Indicator, skipping data...", name)
        return None

    logger.info("Process Single Indicator Complete.. %s indicator, %s", len(data), name)
    return data


async def orchest_single_fetch(
    manager: FlowsManager,
    country: str,
    name: str,
    persist_raw: bool,
    export_json: bool,
    replay: bool,
) -> list[FinalresultFetcher] | None:
    "Running single process of indicator"
    if replay:
        data_db = await manager.fetch_db.fetch_database(name, country)
        if export_json and data_db is not None:
            await manager.export_json(data_db, name)

    else:
        data = await manager.single_fetch(name, country)
        if data is None:
            logger.warning("No data to export for %s indicator, skipping export", name)
            return None
        if persist_raw:
            # convert data to list[dict[str, Any]]
            datas = [items.fetch_result for items in data]

            await manager.load_raw.create_raw_respons_table()
            await manager.load_raw.load_raw_respons(datas)
        if export_json:
            datas = data[0].fetch_result
            await manager.export_json(datas, name)

        return data


async def orchest_all_fetch(
    manager: FlowsManager,
    persist_raw: bool,
    replay: bool,
    export_json: bool,
    source: list[str],
):
    """Running all process of indicators"""
    if source:
        return await manager.run_all(source)
    if replay:
        logger.info("Replaying data from database for all indicators...")
        data = await manager.fetch_db.fetch_all_database(source)
        if data is not None and export_json:
            for item in data:
                await manager.export_json(item)
    else:
        data = await manager.run_all(source)
        if data is None:
            logger.warning("No data to process for all indicators, skipping...")
            return None
        if persist_raw:
            datas = [items.fetch_result for items in data]
            await manager.load_raw.create_raw_respons_table()
            await manager.load_raw.load_raw_respons(datas)
        if export_json:
            for items in data:
                await manager.export_json(items)
