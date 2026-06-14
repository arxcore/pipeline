from typing import Any, Optional
from types import TracebackType
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
import logging

logger = logging.getLogger(__name__)


class FetchDB:
    def __init__(self, pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> None:
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

    async def fetch_database(self, name: str, country: str) -> dict[str, Any] | None:
        query = """
        select payload 
        from raw_respons_indic 
        where payload -> 'meta' ->> 'indicator' = %s and 
        payload -> 'meta' ->> 'country' = %s
        order by load_at desc
        limit 1
        """
        async with self.pool.connection() as acon:
            async with acon:
                async with acon.cursor() as curr:
                    await curr.execute(query, (name, country))
                    record = await curr.fetchone()
                    if record:
                        from rich import print

                        print(record)

                        return record[0]

                    logger.warning(
                        "No raw data found in database for %s, country %s",
                        name,
                        country,
                    )
                    return None

    async def fetch_all_database(self):
        """fetch all data from database, return list of dict"""
        query = """
            SELECT DISTINCT ON (
            payload -> 'meta' ->> 'code', payload -> 'meta' ->> 'source'
            )
            payload, load_at
            FROM raw_respons_indic
            ORDER BY 
            payload -> 'meta' ->> 'code',
            payload -> 'meta' ->> 'source',
            load_at DESC;
            """
        async with self.pool.connection() as acon:
            async with acon:
                async with acon.cursor() as curr:
                    await curr.execute(query)
                    records = await curr.fetchall()
                    if records:
                        return [record[0] for record in records]

                    logger.warning("No raw data found in database")
                    return None
