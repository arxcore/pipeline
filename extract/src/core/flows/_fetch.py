from __future__ import annotations
from typing import TYPE_CHECKING, cast
from core.flows._utils import PipelineFilter, aplay_filters
from core.models.pipeline_schemas import (
    Fetchresult,
    FileResult,
    ApiResult,
    is_file_result,
)
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
    country: str | None = None,
    indicator: str | None = None,
    source: list[str] | None = None,
) -> Fetchresult:
    filter: PipelineFilter = PipelineFilter(country, indicator, source)
    return await fetch_config_indicators(manager, filter)


async def fetch_config_indicators(manager: FlowsManager, filter: PipelineFilter):
    """
    Running ALLConfig Data
    """
    # TODO:
    # DB Traking

    # create task for each indicator and run them concurrently
    tasks: list[Coroutine[Any, Any, ApiResult | FileResult | None]] = []
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
                logger.error(
                    "Error task, skiping %s indicator..",
                    tasks_info["name"],
                    exc_info=True,
                )
                error_count += 1
                continue
            if isinstance(result, FileResult):
                logger.info("Path append")
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
            return

        logger.info("-" * 50)
        logger.info("Pipeline Summary:")
        logger.info("   >> Total Indicators Processed: %s", len(results))
        logger.info("   >> Successfully Processed: %s Indicators", success_count)
        logger.info("   >> Skipped Indicators: %s", skipped_count)
        logger.info("   >> Failed Indicators: %s", error_count)

        if valid_path and valid_data:
            return valid_path, valid_data
        if valid_path:
            return valid_path

        return valid_data
    except exc.PipelineCrash as e:
        logger.exception("Pipeline process carsh during operation %s", e)
        raise


async def run_all_chain(
    manager: FlowsManager,
    source: list[str],
    export_json: bool,
    country: str,
    indicator: str,
):
    """Running all indicator with all chain process, from fetch, loadraw, parse, staging"""
    # raw data from API
    raw = await manager.run_all()
    # load raw data
    if raw is None:
        logger.warning("No data to process for all indicators, skipping...")
        return None
    # load raw respons
    await manager.load_raw_result(raw)

    # parse data from raw data
    await manager.parsing_all_db(
        source, export_json, country, indicator, persist_stg=True
    )


async def orchest_all_fetch(
    manager: FlowsManager,
    source: list[str],
    persist_raw: bool = False,
    replay: bool = False,
    export_json: bool = False,
    country: str | None = None,
    indicator: str | None = None,
):
    """Running all process of indicators"""
    if source or country or indicator and country:
        s = source if source else ""
        c = country if country else ""
        i = indicator if indicator else ""
        logger.info("Filter: %s  %s  %s", s, c, i)
        data = await manager.run_all(country, indicator, source)
        try:
            if persist_raw and data is not None:
                logger.debug("type data %s", type(data))
                await manager.load_raw_result(data)
            if export_json:
                if isinstance(data, tuple):
                    _, api_data = data
                    for items in api_data:
                        await manager.export_json(items)

        except Exception as e:
            logger.error("Unexpected Error %s", e)
            raise
        return data
    if replay:
        logger.info("Replaying data from database for all indicators...")
        data = await manager.fetch_db.fetch_from_database(source, country, indicator)

        if data is None:
            return None

        # file_data: list[Fetchresult]
        api_data: list[ApiResult] | None
        _, api_data = data
        if data and export_json:
            await manager.export_json(api_data)
        else:
            logger.warning("export json not suport into File-based")
            return None

        return data
    else:
        logger.info("running fetching full indicator")
        data = await manager.run_all()
        if data is None:
            logger.warning("No data to process for all indicators, skipping...")
            return None
        if isinstance(data[0], FileResult):
            return data
        if persist_raw:
            logger.debug("type data %s", type(data))
            await manager.load_raw_result(data)
        if export_json:
            if isinstance(data, tuple):
                _, api_data = data
                for items in api_data:
                    await manager.export_json(items)
            else:
                logger.warning("export json not suported")
        return data


async def load_raw_result(manager: FlowsManager, data: Fetchresult):
    if data is None:
        logger.warning("No data to prosess while trying Load Raw")
        return None

    if isinstance(data, tuple):
        file_data, api_data = data
        # file_path
        logger.info("is_file_result %s", type(file_data[0]))
        await manager.load_raw.load_path(file_data)

        # api_data
        logger.info("loading apis data %s", type(api_data[0]))
        await manager.load_raw.load_raw_respons([i.model_dump() for i in api_data])

    elif is_file_result(data):
        logger.info("is_file_result %s", type(data[0]))
        await manager.load_raw.load_path(data)

    else:
        logger.info("loading apis data %s", type(data[0]))
        api_data = cast(list[ApiResult], data)
        await manager.load_raw.load_raw_respons([i.model_dump() for i in api_data])
