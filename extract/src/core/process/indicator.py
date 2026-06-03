from datetime import datetime, timezone
import logging
import hashlib
import json
from providers import BaseMetaModel
from core.process import (
    RawProcessors,
    ParseProcessors,
)
import monitoring.exc_models as exc
from upload.postgres import LoadStg, LoadRaw
from core.models import StagingData, StagingItems, FinalresultParse, FinalresultFetcher

logger = logging.getLogger(__name__)


class IndicatorsProcessors:
    """Handling Process indicators"""

    def __init__(
        self,
        raw_processors: RawProcessors,
        parse_processors: ParseProcessors,
        stg_indicator: LoadStg,
        raw_respons_data: LoadRaw,
    ):
        self.raw = raw_processors
        self.parse = parse_processors or ParseProcessors()
        self.stg = stg_indicator
        self.load_raw = raw_respons_data

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

    async def load_raw_respons(
        self,
        raw_respons: FinalresultFetcher,
        country: str,
        category: str,
        name: str,
        meta: BaseMetaModel,
    ):
        # unpact pydantic to json
        payload_respons = {
            "source_data": raw_respons.model_dump(mode="json"),
            "meta": {
                "country": country,
                "category": category,
                "indicator": name,
                **meta.model_dump(mode="json"),
                "load_at": datetime.now(timezone.utc).isoformat(),
                "checksum": hashlib.sha256(
                    json.dumps(
                        raw_respons.model_dump(mode="json"), sort_keys=True
                    ).encode()
                ).hexdigest(),
            },
        }
        # load raw respons into database
        await self.load_raw.load_raw_respons([payload_respons])

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

            # load raw respons into database
            await self.load_raw_respons(raw_process, country, category, name, meta)

            #  parse data
            parsed_data: FinalresultParse = self.parse(
                raw_process, meta.source, meta.freq
            )

            load_at = datetime.now(timezone.utc)

            # FIXME: missing footnotes_note handling
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
                    processed=load_at,
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
