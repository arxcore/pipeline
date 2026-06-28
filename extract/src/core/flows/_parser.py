from __future__ import annotations
from typing import TYPE_CHECKING
from core.process.staging import staging_result
import logging
from core.models.pipeline_schemas import ApiResult, FileResult
from core.parsers.ons.tasks import route_task

if TYPE_CHECKING:
    from .manager import FlowsManager

logger = logging.getLogger(__name__)


async def parsing_all_db(
    manager: FlowsManager,
    export_json: bool,
    sources: list[str],
    country: str,
    indicator: str,
    persist_stg: bool,
):

    data = await manager.fetch_db.fetch_from_database(country, indicator, sources)

    if data is None:
        logger.warning("No data to parse for all indicators, skipping...")
        return None
    # unpact tuple
    file_data: list[FileResult] | None
    api_data: list[ApiResult] | None
    file_data, api_data = data
    if file_data:
        if persist_stg:
            for item in file_data:
                parser = route_task([item])
                stg = staging_result(
                    item.indicator,
                    item.category,
                    item.country,
                    parser,
                    item.source,
                    item.code_name,
                    item.calc,
                    item.sheet_name,
                    item.unit,
                    item.description,
                    item.freq,
                )
                await manager.load_stg.create_stg_table()
                await manager.load_stg.load_stg_indicator(stg)

    if api_data:
        for item in api_data:
            parser = manager.parse.parse_data(item, item.meta.source, item.meta.freq)
            if export_json:
                await manager.export_json(
                    parser.model_dump(mode="json"), item.meta.indicator
                )
            if persist_stg:
                stg = staging_result(
                    item.meta.indicator,
                    item.meta.category,
                    item.meta.country,
                    parser,
                    item.meta.source,
                    item.meta.code_name,
                    item.meta.calc,
                    item.meta.sheet_name,
                    item.meta.unit,
                    item.meta.description,
                    item.meta.freq,
                )

                await manager.load_stg.create_stg_table()
                await manager.load_stg.load_stg_indicator(stg)
