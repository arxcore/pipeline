from __future__ import annotations
from typing import TYPE_CHECKING
from core.process.staging import staging_result
import logging
from core.models.pipeline_schemas import FinalresultFetcher

if TYPE_CHECKING:
    from .manager import FlowsManager

logger = logging.getLogger(__name__)


async def parsing_db(
    manager: FlowsManager, country: str, name: str, export_json: bool, persist_stg: bool
):
    metadb = await manager.utils_field(name, country)
    if metadb is None:
        return None
    raw = metadb["raw"]
    source = metadb["source"]
    freq = metadb["freq"]
    code = metadb["code"]
    calc = metadb["calc"]
    unit = metadb["unit"]
    description = metadb["description"]
    category = metadb["category"]

    parser = manager.parse.parse_data(raw, source, freq)
    if export_json:
        await manager.export_json(parser.parse_result, name)
    if persist_stg:
        stg = staging_result(
            name,
            category,
            country,
            parser,
            source,
            code,
            calc,
            unit,
            description,
            freq,
        )

        await manager.load_stg.create_stg_table()
        await manager.load_stg.load_stg_indicator(stg)


async def parsing_all_db(
    manager: FlowsManager,
    export_json: bool,
    sources: list[str],
    persist_stg: bool,
):
    """Parsing all data from database"""
    # FIXME: DUO handling field
    data = await manager.fetch_db.fetch_all_database(sources)
    if data is None:
        logger.warning("No data to parse for all indicators, skipping...")
        return None
    for item in data:
        raw = FinalresultFetcher(fetch_result=item["source_data"])
        source = item["meta"]["source"]
        freq = item["meta"]["freq"]
        code = item["meta"]["code_name"]
        calc = item["meta"]["calc"]
        unit = item["meta"]["unit"]
        description = item["meta"].get("description", "")
        category = item["meta"]["category"]
        name = item["meta"]["indicator"]

        parser = manager.parse.parse_data(raw, source, freq)
        if export_json:
            await manager.export_json(parser.model_dump(mode="json"), name)
        if persist_stg:
            stg = staging_result(
                name,
                category,
                item["meta"]["country"],
                parser,
                source,
                code,
                calc,
                unit,
                description,
                freq,
            )

            await manager.load_stg.create_stg_table()
            await manager.load_stg.load_stg_indicator(stg)
