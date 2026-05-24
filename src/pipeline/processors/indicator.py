from pydantic import BaseModel
from datetime import datetime, timezone, date
import logging
from providers import BaseMetaModel
from pipeline.routing import (
    FinalresultFetcher,
    FinalresultParse,
    RawProcessors,
    ParseProcessors,
)
import monitoring.exc_models as exc


logger = logging.getLogger(__name__)


class StagingItems(BaseModel):
    date: date
    year: int
    source: str
    code: str
    indicator: str
    country: str
    category: str
    value: float
    frequency: str
    method: str
    unit: str
    processed: datetime
    description: str


class StagingData(BaseModel):
    """Staging Data Model for Processed Indicators"""

    staging_result: list[StagingItems]


class IndicatorsProcessors:
    """Handling Process indicators"""

    def __init__(
        self,
        raw_processors: RawProcessors,
        parse_processors: ParseProcessors,
    ):
        self.raw = raw_processors
        self.parse = parse_processors or ParseProcessors()

    async def __aenter__(self):
        await self.raw.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ):
        await self.raw.__aexit__(exc_type, exc_val, exc_tb)

    # ETL Procesed indicators
    async def process_indicators(
        self, name: str, meta: BaseMetaModel, category: str, country: str
    ) -> StagingData | None:
        """Main Process ETL indicatros"""
        try:
            #  Raw Data
            raw_process: FinalresultFetcher | None = await self.raw.process_raw_data(
                meta
            )

            if raw_process is None:
                logger.warning(
                    "No data fetched for Name %s, Source %s, Code %s",
                    name,
                    meta.source,
                    meta.code_name,
                )
                return None
            #  parse data
            parsed_data: FinalresultParse = self.parse(
                raw_process, meta.source, meta.freq
            )

            items: list[StagingItems] = [
                StagingItems(
                    date=date_obj.date(),
                    year=date_obj.year,
                    source=meta.source,
                    code=meta.code_name,
                    indicator=name,
                    value=value,
                    country=country,
                    category=category,
                    frequency=meta.freq,
                    method=meta.calc,
                    unit=meta.unit,
                    description=meta.description,
                    processed=datetime.now(timezone.utc),
                )
                for date_key, value in parsed_data.parse_result.items()
                # inner loop
                # single-element tuple to parse date_key once, reuse for .date() and .year
                for date_obj in (datetime.strptime(date_key, "%Y-%m-%d"),)
            ]
            logger.info("Staging data Done.. %s Data, %s", len(items), name)
            logger.debug("Sample data %s", items[:10])

            return StagingData(staging_result=items)

        except ValueError as e:
            raise exc.FormatError(f"Initialized data Error, Name {name} {e}") from e
        except TypeError as e:
            raise exc.FormatError(
                f"Initialized data Type Error, Name {name} {e}"
            ) from e

        except exc.ProcessingFailed:
            logger.exception("Processing Failed for Name %s", name)
            raise
        except Exception as e:
            logger.exception("Unexpected Error Processing Indicator for Name %s", name)
            raise exc.ProcessingFailed(
                f"Unexpected Error Processing Indicator for Name {name} {e}"
            ) from e
