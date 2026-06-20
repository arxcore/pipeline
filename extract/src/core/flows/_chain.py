from __future__ import annotations
from typing import TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from .manager import FlowsManager

logger = logging.getLogger(__name__)


async def run_all_chain(
    manager: FlowsManager,
    export_json: bool,
    source: list[str],
    country: str,
    indicator: str,
):
    """Running all indicator with all chain process, from fetch, loadraw, parse, staging"""
    # raw data from API
    raw = await manager.run_all(source)

    # load raw data
    if raw is None:
        logger.warning("No data to process for all indicators, skipping...")
        return None

    await manager.load_raw.create_raw_respons_table()
    await manager.load_raw.load_raw_respons([data.fetch_result for data in raw])

    # parse data from raw data
    await manager.parsing_all_db(
        export_json, source, country, indicator, persist_stg=True
    )


async def run_single_all_chain(
    manager: FlowsManager, country: str, name: str, export_json: bool
) -> None:
    """Running single indicator with all chain process, from fetch, parse, staging"""
    # raw data from API
    raw = await manager.single_fetch(name, country)
    if raw is None:
        logger.warning("No data to process for single indicator, skipping...")
        return None
    # load raw data``
    await manager.load_raw.create_raw_respons_table()
    datas = [items.fetch_result for items in raw]
    await manager.load_raw.load_raw_respons(datas)

    # parse data from raw data
    await manager.parsing_db(country, name, export_json, persist_stg=True)
