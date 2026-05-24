import asyncio
from pipeline.processors.indicator import (
    StagingItems,
    StagingData,
    IndicatorsProcessors,
)
import logging
from config.metadata.load_yaml import load_all_indicator
from collections.abc import Coroutine
from typing import Any
import monitoring.exc_models as exc
from upload.postegres.load import LoadDatabase


logger = logging.getLogger(__name__)


class Orchest:
    """Orgenaize runing indicaor"""

    def __init__(
        self,
        indicator_processor: IndicatorsProcessors,
        database: LoadDatabase,
    ):
        self.processors = indicator_processor
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

    async def run_all(self) -> None:
        """
        Running ALLConfig Data
        """
        # TODO:
        # DB Traking

        # create task for each indicator and run them concurrently
        tasks: list[Coroutine[Any, Any, StagingData | None]] = []
        tasks_names: list[dict[str, str]] = []
        try:
            # Iterate through ALL_INDICATORS and create tasks for each indicator
            for country, categories in self.all_indicators.items():
                for category, indicators in categories.items():
                    for indicators_name, meta in indicators.items():
                        # indicator: US_NFP, Unemploy
                        # meta: url, id, calc, etc..``
                        tasks.append(
                            self.processors.process_indicators(
                                indicators_name, meta, category, country
                            )
                        )
                        tasks_names.append(
                            {
                                "name": indicators_name,
                                "source": meta.source,
                            }
                        )
            # Run all tasks concurrently and gather results
            results: list[StagingData | BaseException | None] = await asyncio.gather(
                *tasks, return_exceptions=True
            )

            valid_data: list[StagingItems] = []
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
                # result is valid StagingData
                success_count += 1
                valid_data.extend(result.staging_result)

            logger.info("-" * 50)
            logger.info("Pipeline Summary:")
            logger.info("   >> Total Indicators Processed: %s", len(results))
            logger.info("   >> Successfully Processed: %s Indicators", success_count)
            logger.info("   >> Total Rows: %s records", len(valid_data))
            logger.info("   >> Skipped Indicators: %s", skipped_count)
            logger.info("   >> Failed Indicators: %s", error_count)

            if not valid_data:
                logger.warning("No valid data processed, skipping database loading...")
                return
            # load data
            await self.db.create_table()

            logger.info("Loading %s data rows to database...", len(valid_data))
            await self.db.load_data(StagingData(staging_result=valid_data))

        except exc.PipelineCrash:
            logger.exception("Pipeline process carsh during operation")
            raise

    async def run_by_single(self, country: str, name: str) -> None:
        "Running single process of indicator"

        data: list[StagingItems] = []

        for category, indicators in self.all_indicators[country].items():
            for indicator_name, meta in indicators.items():
                if indicator_name != name:
                    continue
                try:
                    records = await self.processors.process_indicators(
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

                    data.extend(records.staging_result)
                except exc.ProcessingFailed:
                    logger.exception("Failed to Procesed Indicators")
                    logger.warning("skipping  Indicators: %s", indicator_name)
                    continue
        logger.info(
            "Process Single Indicator Complete.. %s Rows Data, %s", len(data), name
        )
        if len(data) == 0:
            logger.warning(
                "No data processed for Indicator %s, skipping database loading...", name
            )
            return
        # create table if not exists
        await self.db.create_table()

        # load data to database
        logger.info("Loading %s Rows Data To database...", len(data))
        await self.db.load_data(StagingData(staging_result=data))
