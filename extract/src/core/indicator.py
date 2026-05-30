import asyncio
import logging
from config.metadata.load_yaml import load_all_indicator
from collections.abc import Coroutine
from typing import Any
import monitoring.exc_models as exc
from core.process import RawProcessors
from upload.postgres.load import LoadDatabase
from core.process.model import FinalresultFetcher

logger = logging.getLogger(__name__)


class Orchest:
    """Orgenaize runing indicaor"""

    def __init__(
        self,
        providers_processors: RawProcessors,
        database: LoadDatabase,
    ):
        self.processors = providers_processors
        self.db = database
        self.all_indicators = load_all_indicator()

    async def __aenter__(self):
        await self.processors.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ):
        await self.processors.__aexit__(exc_type, exc_val, exc_tb)

    async def run_all(self) -> list[dict[str, Any]] | None:
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
                            self.processors.process_raw_data(
                                meta, country, category, indicators_name
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
            # logger.info("   >> Total Rows: %s records", len(valid_data))
            logger.info("   >> Skipped Indicators: %s", skipped_count)
            logger.info("   >> Failed Indicators: %s", error_count)

            # unpact data pydantic
            data: list[dict[str, Any]] = [datas.fetch_result for datas in valid_data]

            return data

        except exc.PipelineCrash:
            logger.exception("Pipeline process carsh during operation")
            raise

    async def run_by_single(
        self, country: str, name: str
    ) -> list[dict[str, Any]] | None:
        "Running single process of indicator"

        data: list[FinalresultFetcher] = []

        for category, indicators in self.all_indicators[country].items():
            for indicator_name, meta in indicators.items():
                if indicator_name != name:
                    continue
                try:
                    records = await self.processors.process_raw_data(
                        meta, country, category, indicator_name
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
            return

        # unpact data from list of FinalresultFetcher to list of dict
        ex_data: list[dict[str, Any]] = [ex_data.fetch_result for ex_data in data]

        logger.info(
            "Process Single Indicator Complete.. %s indicator, %s", len(ex_data), name
        )

        return ex_data
