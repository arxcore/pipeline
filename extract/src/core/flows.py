import asyncio
import logging
from pathlib import Path
from types import TracebackType
from config.metadata.load_yaml import load_all_indicator
from collections.abc import Coroutine
from typing import Any, Optional
from core.models.parsing_schemas import ParsedItems
from core.models.pipeline_schemas import FinalresultFetcher, FinalresultParse
from core.process.raw import RawProcessors
import monitoring.exc_models as exc
from upload.postgres import LoadRaw, LoadStg, FetchDB
from core.process.parse import ParseProcessors
from core.process.staging import staging_result
import json
from datetime import datetime

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

    async def parsing_db(
        self, country: str, name: str, export_json: bool, persist_stg: bool
    ):
        metadb = await self.utils_field(name, country)
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

        parser = self.parse.parse_data(raw, source, freq)
        if export_json:
            await self.export_json(parser.parse_result, name)
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

            await self.load_stg.create_stg_table()
            await self.load_stg.load_stg_indicator(stg)

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
        filename = f"{name}_{timestamp}.json"
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

            else:
                with open(path, "w") as f:
                    json.dump(data, f, indent=4)
        except Exception as e:
            logger.error("Failed to export data to JSON: %s", e, exc_info=True)
            return
        logger.info("Data exported to %s", path)

    async def run_all(self) -> list[FinalresultFetcher] | None:
        """
        Running ALLConfig Data
        """
        # TODO:
        # DB Traking

        # create task for each indicator and run them concurrently
        tasks: list[Coroutine[Any, Any, FinalresultFetcher | None]] = []
        tasks_names: list[dict[str, str]] = []
        try:
            # Iterate through ALL_INDICATORS and create tasks for each indicator
            for country, categories in self.all_indicators.items():
                for category, indicators in categories.items():
                    for indicators_name, meta in indicators.items():
                        # indicator: US_NFP, Unemploy
                        # meta: url, id, calc, etc..``
                        tasks.append(
                            self.fetch_api.process_raw_data(
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
                FinalresultFetcher | BaseException | None
            ] = await asyncio.gather(*tasks, return_exceptions=True)

            valid_data: list[FinalresultFetcher] = []
            skipped_count = 0
            error_count = 0
            success_count = 0

            # Process results, handling exceptions and collecting successful results
            for i, result in enumerate(results):
                tasks_info = tasks_names[i]
                if isinstance(result, BaseException):
                    logger.error(
                        "Error task, skiping indicator %s", result, exc_info=True
                    )
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

    async def run_single_all_chain(
        self, country: str, name: str, export_json: bool
    ) -> None:
        """Running single indicator with all chain process, from fetch, parse, staging"""
        # raw data from API
        raw = await self.single_fetch(name, country)
        if raw is None:
            logger.warning("No data to process for single indicator, skipping...")
            return None
        # load raw data``
        await self.load_raw.create_raw_respons_table()
        datas = [items.fetch_result for items in raw]
        await self.load_raw.load_raw_respons(datas)

        # parse data from raw data
        await self.parsing_db(country, name, export_json, persist_stg=True)

    async def single_fetch(
        self, name: str, country: str
    ) -> list[FinalresultFetcher] | None:
        data: list[FinalresultFetcher] = []
        for category, indicators in self.all_indicators[country].items():
            for indicator_name, meta in indicators.items():
                if indicator_name != name:
                    continue
                try:
                    records = await self.fetch_api.process_raw_data(
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

        logger.info(
            "Process Single Indicator Complete.. %s indicator, %s", len(data), name
        )
        return data

    async def orchest_single_fetch(
        self,
        country: str,
        name: str,
        persist_raw: bool,
        export_json: bool,
        replay: bool,
    ) -> list[FinalresultFetcher] | None:
        "Running single process of indicator"
        if replay:
            data_db = await self.fetch_db.fetch_database(name, country)
            if export_json and data_db is not None:
                await self.export_json(data_db, name)

        else:
            data = await self.single_fetch(name, country)
            if data is None:
                logger.warning(
                    "No data to export for %s indicator, skipping export", name
                )
                return None
            if persist_raw:
                # convert data to list[dict[str, Any]]
                datas = [items.fetch_result for items in data]

                await self.load_raw.create_raw_respons_table()
                await self.load_raw.load_raw_respons(datas)
            if export_json:
                datas = data[0].fetch_result
                await self.export_json(datas, name)

            return data

    async def orchest_all_fetch(
        self, persist_raw: bool, replay: bool, export_json: bool
    ):
        """Running all process of indicators"""
        if replay:
            logger.info("Replaying data from database for all indicators...")
            data = await self.fetch_db.fetch_all_database()
            if data is not None and export_json:
                for item in data:
                    await self.export_json(item)
        else:
            data = await self.run_all()
            if data is None:
                logger.warning("No data to process for all indicators, skipping...")
                return None
            if persist_raw:
                datas = [items.fetch_result for items in data]
                await self.load_raw.create_raw_respons_table()
                await self.load_raw.load_raw_respons(datas)
            if export_json:
                for items in data:
                    await self.export_json(items)

    async def parsing_all_db(self, export_json: bool, persist_stg: bool):
        """Parsing all data from database"""
        # FIXME: DUO handling field
        data = await self.fetch_db.fetch_all_database()
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

            parser = self.parse.parse_data(raw, source, freq)
            if export_json:
                await self.export_json(parser.model_dump(mode="json"), name)
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

                await self.load_stg.create_stg_table()
                await self.load_stg.load_stg_indicator(stg)

    async def run_all_chain(self, export_json: bool):
        """Running all indicator with all chain process, from fetch, loadraw, parse, staging"""
        # raw data from API
        raw = await self.run_all()
        # load raw data
        if raw is None:
            logger.warning("No data to process for all indicators, skipping...")
            return None

        await self.load_raw.create_raw_respons_table()
        await self.load_raw.load_raw_respons([data.fetch_result for data in raw])

        # parse data from raw data
        await self.parsing_all_db(export_json, persist_stg=True)
