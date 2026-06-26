import psycopg
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import TupleRow
from psycopg import AsyncConnection
import psycopg_pool
from core.models import StagingData
import logging
from psycopg.types.json import Json
from types import TracebackType
from typing import Optional

logger = logging.getLogger(__name__)


class LoadStg:
    def __init__(
        self,
        pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    ) -> None:
        self.pool = pool

    async def __aenter__(self):
        await self.pool.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Optional[TracebackType],
    ):

        await self.pool.__aexit__(exc_type, exc_val, exc_tb)

    async def create_stg_table(self):
        """Create table if not exists"""
        async with self.pool.connection() as aconn:
            async with aconn:
                async with aconn.cursor() as cur:
                    await cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS staging_indicators (
                            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            date DATE NOT NULL,
                            year INTEGER,
                            source TEXT NOT NULL,
                            code TEXT NOT NULL,
                            indicator TEXT NOT NULL,
                            value NUMERIC(20, 4),
                            country TEXT NOT NULL,
                            category TEXT NOT NULL,
                            frequency TEXT NOT NULL,
                            method TEXT NOT NULL,
                            sheet_name TEXT,
                            unit TEXT,
                            footnotes_note JSONB,
                            description TEXT NOT NULL,
                            processed TIMESTAMPTZ,
                            UNIQUE (date, source, code, country, frequency)
                        );
                        -- index dasbord
                        CREATE INDEX IF NOT EXISTS idx_stg_lookup
                        ON staging_indicators (code, country, date)
                        """
                    )

    async def load_stg_indicator(self, data: StagingData):
        """Load Data"""
        try:
            async with (
                self.pool.connection() as aconn
            ):  # create AsyncConnection from pool
                async with aconn:  # ensure transaction is properly closed after use
                    async with (
                        aconn.cursor() as acur
                    ):  # create AsyncCursor from connection
                        rows = [
                            (
                                item.date,
                                item.year,
                                item.source,
                                item.code,
                                item.indicator,
                                item.value,
                                item.country,
                                item.category,
                                item.frequency,
                                item.method,
                                item.sheet_name,
                                item.unit,
                                Json(notes)
                                if (
                                    notes := [
                                        {"ref": f.code, "text": f.text}
                                        for f in (item.footnotes_note or [])
                                        if f.code and f.text
                                    ]
                                )
                                else None,
                                item.description,
                                item.processed,
                            )
                            for item in data.staging_result
                        ]
                        logger.info("Loading %s rows to database...", len(rows))
                        await acur.executemany(
                            """
                                    INSERT INTO staging_indicators (
                                        date, year, source, code, indicator, value, country, category, frequency, method, sheet_name, unit, footnotes_note, description, processed
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (date, source, code, country, frequency)
                                    DO UPDATE SET 
                                        value = EXCLUDED.value,
                                        footnotes_note = EXCLUDED.footnotes_note,
                                        processed = EXCLUDED.processed
                                    """,
                            rows,
                        )
                        logger.info("Data %s loaded successfully.", len(rows))

        except psycopg_pool.PoolTimeout as e:
            logger.error("Connection pool timeout while trying to load data.", e)
            raise SystemExit(1)
        except psycopg_pool.PoolClosed as e:
            logger.error("Connection pool is closed while trying to load data: %s", e)
            raise SystemExit(1)
        except psycopg.OperationalError as e:
            logger.error("Operational error while trying to load data: %s", e)
            raise SystemExit(1)
