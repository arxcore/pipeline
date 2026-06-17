from ._fetch import (
    fetch_config_indicators,
    single_fetch,
    run_all,
    orchest_single_fetch,
    orchest_all_fetch,
)
from ._parser import parsing_db, parsing_all_db
from ._chain import run_all_chain, run_single_all_chain
from ._utils import PipelineFilter
from upload.postgres import LoadRaw, LoadStg, FetchDB
from core.process.parse import ParseProcessors
from core.process.raw import RawProcessors
from config.metadata.load_yaml import load_all_indicator
from typing import Optional, Any
from types import TracebackType
from core.models.pipeline_schemas import FinalresultFetcher, FinalresultParse
from core.models.parsing_schemas import ParsedItems
from datetime import datetime
import logging
import json
from pathlib import Path
import random

logger = logging.getLogger(__name__)


class FlowsManager:
    """Flows runing indicator"""

    def __init__(
        self,
        fetch: RawProcessors,
        load_stg: LoadStg,
        load_raw: LoadRaw,
        parse: ParseProcessors,
        fetch_db: FetchDB,
    ):
        self.all_indicators = load_all_indicator()
        self.load_stg = load_stg
        self.load_raw = load_raw
        self.fetch_api = fetch
        self.fetch_db = fetch_db
        self.parse = parse
        self.filters = PipelineFilter

    async def __aenter__(self):
        await self.fetch_api.__aenter__()
        await self.load_stg.__aenter__()
        await self.load_raw.__aenter__()
        await self.fetch_db.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Optional[TracebackType] | None,
    ):
        await self.fetch_api.__aexit__(exc_type, exc_val, exc_tb)
        await self.load_stg.__aexit__(exc_type, exc_val, exc_tb)
        await self.load_raw.__aexit__(exc_type, exc_val, exc_tb)
        await self.fetch_db.__aexit__(exc_type, exc_val, exc_tb)

    async def export_json(
        self,
        data: list[ParsedItems]
        | dict[str, Any]
        | FinalresultParse
        | FinalresultFetcher,
        name: str = "datas",
    ) -> None:
        """Export data to json file for debugging and testing purpose"""
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M")
        directory = Path("exported_data")
        id = random.randint(1, 100000)
        filename = f"{id}_{name}_{timestamp}.json"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / filename
        try:
            if isinstance(data, FinalresultParse):
                serialize = [item.model_dump(mode="json") for item in data.parse_result]
                with open(path, "w") as f:
                    json.dump(serialize, f, indent=4)

            elif isinstance(data, FinalresultFetcher):
                serialize = [item.model_dump(mode="json") for item in data.fetch_result]
                with open(path, "w") as f:
                    json.dump(serialize, f, indent=4)
            elif isinstance(data, list):
                serialize = [item.model_dump(mode="json") for item in data]
                with open(path, "w") as f:
                    json.dump(serialize, f, indent=4)

            else:
                with open(path, "w") as f:
                    json.dump(data, f, indent=4)
        except Exception as e:
            logger.error("Failed to export data to JSON: %s", e, exc_info=True)
            return
        logger.info("Data exported to %s", path)

    async def utils_field(self, name: str, country: str) -> dict[str, Any] | None:
        # FIXME: DUO handling field
        data = await self.fetch_db.fetch_database(name, country)
        if data is None:
            return None

        try:
            raw_db = data["source_data"]
            raw = FinalresultFetcher(fetch_result=raw_db)
            source = data["meta"]["source"]
            freq = data["meta"]["freq"]
            code = data["meta"]["code_name"]
            calc = data["meta"]["calc"]
            unit = data["meta"]["unit"]
            description = data["meta"].get("description", "")
            category = data["meta"]["category"]
            fields = {
                "raw": raw,
                "source": source,
                "freq": freq,
                "code": code,
                "calc": calc,
                "unit": unit,
                "description": description,
                "category": category,
            }
            return fields
        except KeyError as e:
            logger.error(
                "Missing expected field in database data for %s, country %s: %s",
                name,
                country,
                e,
            )
            return None

    async def fetch_config_indicator(self, filter: PipelineFilter):
        return await fetch_config_indicators(self, filter)

    async def run_all(self, source: list[str]):
        return await run_all(self, source)

    async def single_fetch(self, name: str, country: str):
        return await single_fetch(self, name, country)

    async def parsing_db(
        self, country: str, name: str, export_json: bool, persist_stg: bool
    ):
        return await parsing_db(self, country, name, export_json, persist_stg)

    async def parsing_all_db(
        self, export_json: bool, source: list[str], persist_stg: bool
    ):
        return await parsing_all_db(self, export_json, source, persist_stg)

    async def run_all_chain(self, export_json: bool, source: list[str]):
        return await run_all_chain(self, export_json, source)

    async def run_single_all_chain(self, country: str, name: str, export_json: bool):
        return await run_single_all_chain(self, country, name, export_json)

    async def orchest_single_fetch(
        self,
        country: str,
        name: str,
        persist_raw: bool,
        export_json: bool,
        replay: bool,
    ):
        return await orchest_single_fetch(
            self, country, name, persist_raw, export_json, replay
        )

    async def orchest_all_fetch(
        self, persist_raw: bool, replay: bool, export_json: bool, source: list[str]
    ):
        return await orchest_all_fetch(self, persist_raw, replay, export_json, source)
