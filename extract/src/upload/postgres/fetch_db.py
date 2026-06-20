from pathlib import Path
from typing import Any, Optional
from types import TracebackType
from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row
from psycopg_pool import AsyncConnectionPool
import logging
import psycopg_pool
import psycopg

from core.models.pipeline_schemas import FetchMeta, FileResult, ApiResult


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
                        return record[0]

                    logger.warning(
                        "No raw data found in database for %s, country %s",
                        name,
                        country,
                    )
                    return None

    async def db_raw_respons_api(
        self, country: str, indicator: str, sources: list[str]
    ) -> list[ApiResult] | None:
        """fetch raw respons api from database"""
        try:
            conditional: list[Any] = []
            params: list[Any] = []

            if country:
                conditional.append("payload -> 'meta' ->> 'country' = %s")
                params.append(country)

            if indicator:
                conditional.append("payload -> 'meta' ->> 'indicator' = %s")
                params.append(indicator)

            if sources:
                conditional.append("payload -> 'meta' ->> 'source' = ANY(%s)")
                params.append(sources)

            where = f" WHERE {' AND '.join(conditional)}" if conditional else ""

            query = f"""
                SELECT DISTINCT ON (
                payload -> 'meta' ->> 'code_name',
                payload -> 'meta' ->> 'indicator',
                payload -> 'meta' ->> 'country',
                payload -> 'meta' ->> 'source'
                )
                payload, load_at
                FROM raw_respons_api
                {where}
                ORDER BY 
                payload -> 'meta' ->> 'code_name',
                payload -> 'meta' ->> 'indicator',
                payload -> 'meta' ->> 'country',
                payload -> 'meta' ->> 'source',
                load_at DESC;
                """
            async with self.pool.connection() as acon:
                async with acon:
                    async with acon.cursor() as curr:
                        await curr.execute(query, params)
                        records = await curr.fetchall()
                        if records:
                            data = [record[0] for record in records]

                            item: list[ApiResult] = []
                            for x in data:
                                item.append(
                                    ApiResult(
                                        source_data=x["source_data"],
                                        meta=FetchMeta.model_validate(x["meta"]),
                                    )
                                )
                            return item

                        # return None if no records
                        return None
        except psycopg_pool.PoolTimeout:
            logger.error("Connection pool timeout while trying to load data.")
            raise SystemExit(1)
        except psycopg_pool.PoolClosed as e:
            logger.error("Connection pool is closed while trying to load data: %s", e)
            raise SystemExit(1)
        except psycopg.OperationalError as e:
            logger.error("Operational error while trying to load data: %s", e)
            raise SystemExit(1)

    async def db_register_path(
        self, country: str, indicator: str, sources: list[str]
    ) -> list[FileResult] | None:
        """fetch register file_path from database"""
        try:
            conditional: list[Any] = []
            params: list[Any] = []

            if sources:
                conditional.append("source = ANY(%s)")
                params.append(sources)
            if country:
                conditional.append("country = %s")
                params.append(country)
            if indicator:
                conditional.append("indicator = %s")
                params.append(indicator)

            where = f"WHERE {' AND '.join(conditional)}" if conditional else ""

            query = f"""
                SELECT DISTINCT ON (
                file_path, code_name, country, category, source, indicator, file_ext
                )
                file_path, 
                code_name, 
                country, 
                category, 
                source, 
                indicator, 
                file_ext
                from file_registry
                {where}
                ORDER BY
                file_path, 
                code_name, 
                country, 
                category, 
                source, 
                indicator, 
                file_ext, 
                load_at DESC;
                """
            async with self.pool.connection() as acon:
                async with acon:
                    async with acon.cursor(row_factory=dict_row) as acur:
                        await acur.execute(query, params)
                        record = await acur.fetchall()
                        if record:
                            data: list[FileResult] = []
                            for x in record:
                                data.append(
                                    FileResult(
                                        file_path=Path(x["file_path"]),
                                        country=x["country"],
                                        category=x["category"],
                                        indicator=x["indicator"],
                                        source=x["source"],
                                        code_name=x["code_name"],
                                    )
                                )
                            return data

                        # return None if no records
                        return None
        except psycopg_pool.PoolTimeout:
            logger.error("Connection pool timeout while trying to load data.")
            raise SystemExit(1)
        except psycopg_pool.PoolClosed as e:
            logger.error("Connection pool is closed while trying to load data: %s", e)
            raise SystemExit(1)
        except psycopg.OperationalError as e:
            logger.error("Operational error while trying to load data: %s", e)
            raise SystemExit(1)

    async def fetch_from_database(
        self, country: str, indicator: str, sources: list[str]
    ) -> tuple[list[FileResult] | None, list[ApiResult] | None] | None:
        # NOTE:
        # 0 - reg_path
        # 1 - api
        try:
            logger.info(
                "Fetch from database with Filters: %s, %s, %s",
                country,
                indicator,
                sources,
            )
            reg_path = await self.db_register_path(country, indicator, sources)
            api = await self.db_raw_respons_api(country, indicator, sources)

            # count
            reg_count = len(reg_path) if reg_path else 0
            api_count = len(api) if api else 0

            if not api and not reg_path:
                logger.warning(
                    "No data Found in database for %s, %s, %s",
                    country,
                    indicator,
                    sources,
                )
                return None

            logger.info(
                "succesfuly pull %s file-based, %s api record  from database",
                reg_count,
                api_count,
            )
            return reg_path, api

        except Exception as e:
            logger.error("Unexpected Error -_", e)
            raise SystemExit(1)
