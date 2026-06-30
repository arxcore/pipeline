from __future__ import annotations
from typing import TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from .manager import FlowsManager

logger = logging.getLogger(__name__)


async def run_all_chain(
    manager: FlowsManager,
    source: list[str],
    export_json: bool = False,
    country: str | None = None,
    indicator: str | None = None,
):
    """Running all indicator with all chain process, from fetch, loadraw, parse, staging"""
    # raw data from API
    raw = await manager.run_all(source=source)

    # load raw data
    if raw is None:
        logger.warning("No data to process for all indicators, skipping...")
        return None

    await manager.load_raw_result(raw)

    # parse data from raw data
    await manager.parsing_all_db(
        source, export_json, country, indicator, persist_stg=True
    )
