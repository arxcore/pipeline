import psycopg
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import TupleRow
from psycopg import AsyncConnection
import psycopg_pool
from core.models import StagingData
import logging

logger = logging.getLogger(__name__)


class LoadStg:
    def __init__(
        self,
        pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    ) -> None:
        self.pool = pool

    async def create_stg_table(self):
        """Create table if not exists"""
        async with self.pool.connection() as aconn:
            async with aconn:
                async with aconn.cursor() as cur:
                    await cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS stg_indicators (
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
                            unit TEXT,
                            footnotes_note JSONB,
                            description TEXT,
                            processed TIMESTAMPTZ,
                            UNIQUE (date, source, code, country, frequency)
                        );
                        -- index dasbord
                        CREATE INDEX IF NOT EXISTS idx_stg_lookup
                        ON stg_indicators (code, country, date)
                        """
                    )

    async def load_stg_indicator(self, data: StagingData):
        """Load Data"""
        async with self.pool.connection() as aconn:  # create AsyncConnection from pool
            async with aconn:  # ensure transaction is properly closed after use
                async with aconn.cursor() as acur:  # create AsyncCursor from connection
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
                            item.unit,
                            item.footnotes_note,
                            item.description,
                            item.processed,
                        )
                        for item in data.staging_result
                    ]
                    try:
                        await acur.executemany(
                            """
                                    INSERT INTO stg_indicators (
                                        date, year, source, code, indicator, value, country, category, frequency, method, unit, footnotes_note, description, processed
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (date, source, code, country, frequency)
                                    DO UPDATE SET 
                                        value = EXCLUDE.value,
                                        footnotes_note = EXCLUDE.footnotes_note,
                                        processed = EXCLUDE.processed
                                    """,
                            rows,
                        )
                        logger.info("Data loaded successfully.")

                    except psycopg_pool.PoolTimeout:
                        logger.error(
                            "Connection pool timeout while trying to load data."
                        )
                        raise SystemExit(1)
                    except psycopg_pool.PoolClosed as e:
                        logger.error(
                            "Connection pool is closed while trying to load data: %s", e
                        )
                        raise SystemExit(1)
                    except psycopg.OperationalError as e:
                        logger.error(
                            "Operational error while trying to load data: %s", e
                        )
                        raise SystemExit(1)
