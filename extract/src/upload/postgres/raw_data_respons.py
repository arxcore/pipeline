from typing import Any, Optional
from types import TracebackType
import psycopg
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import TupleRow
from psycopg import AsyncConnection
import psycopg_pool
import logging
from psycopg.types.json import Json

logger = logging.getLogger(__name__)


class LoadRaw:
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

    async def create_raw_respons_table(self):
        """Create table if not exists"""
        async with self.pool.connection() as aconn:
            async with aconn:
                async with aconn.cursor() as cur:
                    await cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS raw_respons_indic (
                        id BIGSERIAL PRIMARY KEY,
                        payload JSONB NOT NULL,
                        load_at TIMESTAMPTZ DEFAULT NOW()
                        );
                        -- uniq index 
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_checksum
                        ON raw_respons_indic ((payload -> 'meta' ->> 'checksum'));

                        -- index filter source 
                        CREATE INDEX IF NOT EXISTS idx_meta_source
                        ON raw_respons_indic ((payload -> 'meta' ->> 'source'));

                        -- index filter country
                        CREATE INDEX IF NOT EXISTS idx_meta_country 
                        ON raw_respons_indic ((payload -> 'meta' ->> 'country'));

                        -- index filter code name indicator
                        CREATE INDEX IF NOT EXISTS idx_meta_codename
                        ON raw_respons_indic ((payload -> 'meta' ->> 'code_name'));
                        """
                    )

    async def load_raw_respons(self, data: list[dict[str, Any]]) -> None:
        """Load Data"""
        async with self.pool.connection() as aconn:  # create AsyncConnection from pool
            async with aconn:  # ensure transaction is properly closed after use
                async with aconn.cursor() as acur:  # create AsyncCursor from connection
                    try:
                        # TODO: use copy bulk for perfomance
                        # FIXME:
                        await acur.executemany(
                            """
                                INSERT INTO raw_respons_indic (payload) VALUES (%s)
                                ON CONFLICT ((payload -> 'meta' ->> 'checksum')) DO NOTHING
                                """,
                            [(Json(payload),) for payload in data],
                        )
                        logger.info("Data loaded successfully %s rows", len(data))

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
