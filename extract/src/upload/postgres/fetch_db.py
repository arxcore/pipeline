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
from providers.ons.model import ONSConfigModel


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

    async def db_raw_respons_api(
        self,
        sources: list[str],
        country: str | None = None,
        indicator: str | None = None,
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

            logger.debug("Query Filters: %s", query)
            logger.debug("params %s", params)

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
        self,
        sources: list[str],
        country: str | None = None,
        indicator: str | None = None,
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

            # filter by load_at == CURRENT_DATE
            # conditional.append("load_at::DATE = CURRENT_DATE")

            where = f"WHERE {' AND '.join(conditional)}" if conditional else ""
            logger.debug("Query Filters: %s", where)

            query = f"""
                SELECT DISTINCT ON (
                indicator, code_name
                )
                file_path, 
                code_name, 
                country, 
                category, 
                source, 
                indicator, 
                file_ext,
                frequency,
                calc,
                unit,
                sheet_name,
                description,
                load_at
                from file_registry
                {where}
                ORDER BY
                indicator, code_name,
                load_at DESC;
                """

            logger.debug("Query Filters: %s", query)
            logger.debug("params %s", params)

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
                                        file_ext=x["file_ext"],
                                        country=x["country"],
                                        category=x["category"],
                                        indicator=x["indicator"],
                                        freq=x["frequency"],
                                        source=x["source"],
                                        code_name=x["code_name"],
                                        calc=x["calc"],
                                        unit=x["unit"],
                                        sheet_name=x["sheet_name"],
                                        description=x["description"],
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

    async def load_etag(self, meta: ONSConfigModel, indicator_name: str):
        """Load Entity Tag Headers"""
        source = [meta.source]

        conditional: list[Any] = []
        params: list[Any] = []
        if source:
            conditional.append("source = ANY(%s)")
            params.append([source])
        if meta.code_name:
            conditional.append("code_name = %s")
            params.append(meta.code_name)
        if indicator_name:
            conditional.append("indicator = %s")
            params.append(indicator_name)

        where = f" WHERE {' AND '.join(conditional)}" if conditional else ""
        query = f"""
        SELECT DISTINCT ON (file_path, indicator, code_name, source, etag)
        file_path, indicator, code_name, source, etag, load_at
        FROM file_registry
        {where}
        ORDER BY file_path, indicator, code_name, source, etag, load_at DESC;
        """
        logger.debug("Query Filters: %s", query)
        logger.debug("params %s", params)
        try:
            async with self.pool.connection() as aconn:
                async with aconn:
                    async with aconn.cursor(row_factory=dict_row) as acur:
                        await acur.execute(query, params)
                        record = await acur.fetchall()
                        if record:
                            return record
                        logger.warning(
                            "No resource headers in database found for %s, %s, %s",
                            indicator_name,
                            source,
                            meta.code_name,
                        )
                        return None

        except psycopg_pool.PoolTimeout as e:
            logger.error("connection timeout while trying load data %s", e)
            raise SystemExit(1)
        except psycopg_pool.PoolClosed as e:
            logger.error("Connection pool is closed while trying to load data: %s", e)
            raise SystemExit(1)
        except psycopg.OperationalError as e:
            logger.error("Operational error while trying to load data: %s", e)
            raise SystemExit(1)

    async def delete_path_file_registry(
        self, meta: ONSConfigModel, indicator_name: str
    ):
        source = [meta.source]
        conditional: list[Any] = []
        params: list[Any] = []
        if source:
            conditional.append("source = ANY(%s)")
            params.append(source)
        if meta.code_name:
            conditional.append("code_name = %s")
            params.append(meta.code_name)
        if indicator_name:
            conditional.append("indicator = %s")
            params.append(indicator_name)

        where = f"WHERE {' AND '.join(conditional)}" if conditional else ""

        # FIXME: delete if exists query??
        query = f"""
        DELETE
        FROM 
            file_registry
        {where};
        """
        logger.info("Delete file path registry")
        logger.info("   params: %s", params)
        logger.info("   Query: %s", query)

        try:
            async with self.pool.connection() as aconn:
                async with aconn:
                    async with aconn.cursor() as acur:
                        await acur.execute(query, params)
                        await acur.fetchall()

        except psycopg_pool.PoolTimeout as e:
            logger.error("connection timeout while trying load data %s", e)
            raise SystemExit(1)
        except psycopg_pool.PoolClosed as e:
            logger.error("Connection pool is closed while trying to load data: %s", e)
            raise SystemExit(1)
        except psycopg.OperationalError as e:
            logger.error("Operational error while trying to load data: %s", e)
            raise SystemExit(1)

    async def fetch_from_database(
        self,
        sources: list[str],
        country: str | None = None,
        indicator: str | None = None,
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
            reg_path = await self.db_register_path(sources, country, indicator)
            api = await self.db_raw_respons_api(sources, country, indicator)

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
