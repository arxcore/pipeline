from ._fetch import (
    fetch_config_indicators,
    load_raw_result,
    run_all,
    orchest_all_fetch,
)
from ._parser import parsing_all_db
from ._chain import run_all_chain
from ._utils import PipelineFilter
from upload.postgres import LoadRaw, LoadStg, FetchDB
from core.process.parse import ParseProcessors
from core.process.raw import RawProcessors
from config.metadata.load_yaml import load_all_indicator
from typing import Optional, Any
from types import TracebackType
from core.models.pipeline_schemas import (
    Fetchresult,
    ApiResult,
    ParseResult,
)
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

    async def prepare_scheme_table(self):
        """Prepare scheme table"""
        await self.load_stg.create_stg_table()
        await self.load_raw.create_register_path_table()
        await self.load_raw.create_raw_respons_table()

    async def export_json(
        self,
        data: list[ParsedItems]
        | dict[str, Any]
        | list[dict[str, Any]]
        | ParseResult
        | ApiResult
        | list[ApiResult]
        | None,
        name: str = "datas",
    ) -> None:
        """Export data to json file for debugging and testing purpose"""
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        directory = Path("exported_data")
        id = random.randint(1, 100000)
        filename = f"{id}_{name}_{timestamp}.json"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / filename
        try:
            if data is None:
                return None

            if isinstance(data, ParseResult):
                serialize = [item.model_dump(mode="json") for item in data.parse_result]
                with open(path, "w") as f:
                    json.dump(serialize, f, indent=4)

            elif isinstance(data, ApiResult):
                serialize = [item for item in data.model_dump(mode="json")]
                with open(path, "w") as f:
                    json.dump(serialize, f, indent=4)
            elif isinstance(data, list):
                if isinstance(data, ParsedItems):
                    serialize = [item for item in data.model_dump(mode="json")]
                    with open(path, "w") as f:
                        json.dump(serialize, f, indent=4)
                if isinstance(data, ApiResult):
                    serialize = [item for item in data.model_dump(mode="json")]
                    with open(path, "w") as f:
                        json.dump(serialize, f, indent=4)

                serialize = [item for item in data]
                with open(path, "w") as f:
                    json.dump(serialize, f, indent=4)
            else:
                with open(path, "w") as f:
                    json.dump(data, f, indent=4)
        except Exception as e:
            logger.error("Failed to export data to JSON: %s", e, exc_info=True)
            return
        logger.info("Data exported to %s", path)

    async def fetch_config_indicator(self, filter: PipelineFilter):
        return await fetch_config_indicators(self, filter)

    async def run_all(
        self,
        country: str | None = None,
        indicator: str | None = None,
        source: list[str] | None = None,
    ):
        return await run_all(self, country, indicator, source)

    async def parsing_all_db(
        self,
        source: list[str],
        export_json: bool = False,
        country: str | None = None,
        indicator: str | None = None,
        persist_stg: bool = False,
    ):
        return await parsing_all_db(
            self, source, export_json, country, indicator, persist_stg
        )

    async def run_all_chain(
        self,
        source: list[str],
        export_json: bool = False,
        country: str | None = None,
        indicator: str | None = None,
    ):
        return await run_all_chain(self, source, export_json, country, indicator)

    async def orchest_all_fetch(
        self,
        source: list[str],
        persist_raw: bool = False,
        replay: bool = False,
        export_json: bool = False,
        country: str | None = None,
        indicator: str | None = None,
    ):
        return await orchest_all_fetch(
            self, source, persist_raw, replay, export_json, country, indicator
        )

    async def load_raw_result(self, data: Fetchresult):
        return await load_raw_result(self, data)
