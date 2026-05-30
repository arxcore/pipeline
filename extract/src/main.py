import argparse
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
import asyncio
import logging
import sys
from typing import Any, Optional
import traceback
from config.settings import CONN_STR
from config.metadata.load_yaml import load_all_indicator
from core.process.model import FinalresultFetcher
from core.process.raw import RawProcessors
from monitoring.logs.logger import configure_logging
from core.indicator import Orchest
import monitoring.exc_models as exc
from upload.postgres.load import LoadDatabase

logger = logging.getLogger(__name__)

ALL_INDICATORS = load_all_indicator()


def list_of_indicators() -> None:
    """list available indicators"""

    for country, category in ALL_INDICATORS.items():
        print(f"\n-{country}:")

        for categories, indicator in category.items():
            print(f"    -{categories}:")

            for indicators in indicator.keys():
                print(f"         -{indicators}")


def valid_input(
    country: str,
    category: Optional[str] = None,
    indicator_name: Optional[str] = None,
) -> bool:
    """Validation Input Country, Category and Indicators"""
    if country not in ALL_INDICATORS:
        logger.error("country not found in metadata: %s", country)
        return False
    if category:
        if category not in ALL_INDICATORS[country]:
            logger.error("category not found in metadata:  %s", category)
            return False
    if indicator_name:
        found = False
        for _, categories in ALL_INDICATORS[country].items():
            if indicator_name in categories:
                found = True
                break
        if not found:
            logger.warning(
                "indicators not found: indicators %s | country: %s",
                indicator_name,
                country,
            )
            return False

    return True


def resolve_log_level(level_str: str) -> int:
    """Resolve Log Level String to Log Level Int"""
    level_mapping = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    key: str = level_str.lower().strip()
    if key not in level_mapping:
        raise ValueError(f"Unknown log level: {level_str!r}")
    return level_mapping[key]


def level_name(log_level: int) -> str:
    """Get Log Level Name from Log Level Int"""
    level_names = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
    }
    result = level_names.get(log_level)
    if result is None:
        raise ValueError(f"Unknown log level (int): {log_level}")

    return result


def apply_log_level(log_level: int) -> None:
    """Apply Log Level to Logger and Print Log Level Name"""
    log_name: str = level_name(log_level)

    configure_logging(log_level)
    logger.info("Set Logging Level Name %s ", log_name)


def build_injection(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> Orchest:
    """Build Dependency Injection for Pipeline"""
    # Cofigure Connection Pool for Upload Data to Postgres
    database = LoadDatabase(pool)
    procc_raw = RawProcessors()
    return Orchest(procc_raw, database)


async def main() -> FinalresultFetcher | None:
    """Main Execute command line pipeline"""
    parse = argparse.ArgumentParser()
    main_group = parse.add_argument_group("structure data")
    main_group.add_argument("-c", "--country", help="country of indicator")

    main_group.add_argument("-n", "--name", help="name of indicators")

    log_group = parse.add_argument_group("Logging Option")
    log_group.add_argument(
        "-l",
        "--log",
        choices=["debug", "info", "warning", "error", "critical"],
        default="info",
        help="logging level to monitoring",
    )
    run_mode = parse.add_mutually_exclusive_group(required=False)
    run_mode.add_argument(
        "-r",
        "--run",
        choices=["single", "all"],
        default="all",
        help="select running pipeline mode, default=all ",
    )
    run_mode.add_argument(
        "-ls", "--list", action="store_true", help="list of available indicators"
    )
    load = parse.add_argument_group("Load Into Database")
    load.add_argument("--load", action="store_true", help="Load datas into postgres")

    args = parse.parse_args()

    # setup logging
    try:
        if args.log:
            target_level: int = resolve_log_level(args.log)
            apply_log_level(target_level)

    except Exception as e:
        logger.exception("Errors setup logging: %s", e, exc_info=True)
    # List of Indicators Availabel on Config Data
    if args.list:
        print("List Available Indicators:")

        list_of_indicators()
        return
    # Country is required for Single mode indicators and etc.., (exclude run all)
    if not args.country and args.run == "single":
        logger.warning("country is required")
        parse.print_help()
        sys.exit(1)

    # Validate Warning, if run mode not give args Indicator Name
    if args.run == "single" and not args.name:
        logger.warning("name is required")
        parse.print_help()
        sys.exit(1)

    if args.run == "single":
        # Indicators Validation If args in ALL Indicators
        if not valid_input(args.country, indicator_name=args.name):
            parse.print_help()
            sys.exit(1)

    # Execute Pipeline
    data: list[dict[str, Any]] | None = None
    try:
        async with AsyncConnectionPool[AsyncConnection[TupleRow]](
            conninfo=CONN_STR,
            min_size=1,
            max_size=7,
            max_waiting=10,
            timeout=10,
        ) as pool:
            # Execute Pipeline
            orchest: Orchest = build_injection(pool)
            async with orchest as orch:
                # Run Single Indicator
                if args.run == "single":
                    logger.info(
                        "Run Single Indicator country %s, name: %s",
                        args.country,
                        args.name,
                    )
                    data = await orch.run_by_single(args.country, args.name)

                # Default Mode Run Pipeline
                elif args.run == "all":
                    logger.info("Run ALL Config Indicators")
                    data = await orch.run_all()
                if args.load and data is not None:
                    # setup table if not exists
                    await orch.db.create_table()
                    # load data
                    logger.info("Loading %s indicator To database...", len(data))
                    await orch.db.load_data(data)

    except exc.PipelineCrash as e:
        logger.exception("Error during execution pipeline: %s", e)
        print(f"\nFull traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
