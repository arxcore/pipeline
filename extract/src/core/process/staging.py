from datetime import datetime, timezone
import logging
from core.models.pipeline_schemas import ParseResult
from core.models.stg_schemas import StagingData, StagingItems
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


def staging_result(
    name: str,
    category: str,
    country: str,
    parsed: ParseResult,
    source: str,
    code_name: str | None,
    calc: str,
    sheet_name: str | None,
    unit: str | None,
    description: str,
    freq: str,
) -> StagingData:
    try:
        load_at = datetime.now(timezone.utc)

        items: list[StagingItems] = [
            StagingItems(
                date=date_obj.date(),
                year=date_obj.year,
                source=source,
                code=code_name,
                indicator=name,
                value=items.value,
                country=country,
                category=category,
                frequency=freq,
                method=calc,
                sheet_name=sheet_name,
                unit=unit,
                footnotes_note=items.footnotes,
                description=description,
                processed=load_at,
            )
            for items in parsed.parse_result
            # inner loop
            # single-element tuple to parse date_key once, reuse for .date() and .year
            for date_obj in (datetime.strptime(items.date_key, "%Y-%m-%d"),)
        ]
        logger.info("Staging data Done.. %s Data, %s", len(items), name)
        logger.debug("Sample data %s", items[:10])

        return StagingData(staging_result=items)

    except ValueError as e:
        raise exc.FormatError(f"Initialized data Error, Name {name} {e}") from e
    except TypeError as e:
        raise exc.FormatError(f"Initialized data Type Error, Name {name} {e}") from e

    except exc.ProcessingFailed:
        logger.exception("Processing Failed for Name %s", name)
        raise
    except Exception as e:
        logger.exception("Unexpected Error Processing Indicator for Name %s", name)
        raise exc.ProcessingFailed(
            f"Unexpected Error Processing Indicator for Name {name} {e}"
        ) from e
