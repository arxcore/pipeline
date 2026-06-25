from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
import asyncio
import logging
import sys
import traceback
from config.settings import CONN_STR
from typing import Optional
from core.cli import PipelineCliParser
from config.metadata.load_yaml import load_all_indicator
import argparse
from core.process.parse import ParseProcessors
from core.process.raw import RawProcessors
from core.flows import FlowsManager
import monitoring.exc_models as exc
from monitoring.logs.setup import apply_log_level, resolve_log_level
from upload.postgres import LoadRaw, LoadStg, FetchDB

logger = logging.getLogger(__name__)

ALL_INDICATORS = load_all_indicator()


def build_injection(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
) -> PipelineCliParser:
    """Build Dependency Injection for Pipeline"""
    stg_db = LoadStg(pool)
    raw_db = LoadRaw(pool)
    procc_raw = RawProcessors()
    fetch_db = FetchDB(pool)
    procc_parse = ParseProcessors()
    flows = FlowsManager(procc_raw, stg_db, raw_db, procc_parse, fetch_db)
    return PipelineCliParser(flows)


def list_of_indicators() -> None:
    """List available indicators"""

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
    """
    Validate input parameters.

    Args:
        country (str): The country of the indicator.
        category (Optional[str]): The category of the indicator.
        indicator_name (Optional[str]): The name of the indicator.

    Returns:
        bool: True if inputs are valid, False otherwise.
    """
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


def build_args() -> argparse.ArgumentParser:
    """
    Build and configure the argument parser for the pipeline.

    Returns:
        argparse.ArgumentParser: The configured argument parser.
    """
    parse = argparse.ArgumentParser(
        description="Economic Data Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Logging level
    log_group = parse.add_argument_group("Logging Option")
    log_group.add_argument(
        "-l",
        "--log-level",
        choices=["debug", "info", "warning", "error", "critical"],
        default="info",
        help="set logging level to monitoring (default: INFO)",
    )
    # Pipeline runner mode
    run_mode = parse.add_mutually_exclusive_group(required=False)
    run_mode.add_argument(
        "--list", action="store_true", help="list of available indicators"
    )

    # single run args
    single_group = parse.add_argument_group("single indiator mode (only for -r single")
    single_group.add_argument("-c", "--country", help="country of indicator")
    single_group.add_argument("-n", "--name", help="name of indicators")

    # source control
    source_group = parse.add_argument_group(title="Source Selection")
    source_group.add_argument(
        "--source",
        nargs="+",
        choices=["bls", "bea", "fred", "ons"],
        default=None,
        help="Specific source to fetch (default: all)",
    )

    # stages control
    stage_group = parse.add_argument_group(title="Pipeline Stages")
    stage_group.add_argument(
        "--stage",
        choices=["fetch", "parse", "all"],
        default="fetch",
        help="Execute specific stage. 'all' runs fetch>loadraw>parse>loadstg",
    )

    # utils
    utils_group = parse.add_argument_group("Utilities")
    utils_group.add_argument(
        "--export-json",
        action="store_true",
        help="Export pipeline state/results to json file",
    )
    utils_group.add_argument(
        "--replay",
        action="store_true",
        help="refetch data from database and inspect structure",
    )

    utils_group.add_argument(
        "--persist-raw", action="store_true", help="write raw respons into DB"
    )
    utils_group.add_argument(
        "--persist-stg", action="store_true", help="write staging into DB"
    )
    return parse


def valid_args() -> argparse.Namespace | None:
    """
    Validate and parse command-line arguments.

    Returns:
        argparse.Namespace: The parsed arguments or None if the list of indicators is requested.
    """
    parser = build_args()
    args = parser.parse_args()

    # List of Indicators Availabel on Config Data
    if args.list:
        print("List Available Indicators:")
        list_of_indicators()
        return None

    # single mode Validation
    if args.name:
        if args.source is not None:
            logger.error("single run no options source")
            parser.print_help()
            sys.exit(1)
        if not args.country:
            logger.error("country is required in single run")
            parser.print_help()
            sys.exit(1)

        if not valid_input(args.country, indicator_name=args.name):
            logger.error(f"indicator {args.name}, country {args.country} not found")
            parser.print_help()
            sys.exit(1)

    # replay only for stage fetch
    if args.replay and args.stage != "fetch":
        logger.warning("replay only for fetch stage from db")
        parser.print_help()
        sys.exit(1)
    # If replay mode, persist raw and stg should be false
    if args.replay and (args.persist_raw or args.persist_stg):
        logger.warning("Replay mode cannot be used with persist options")
        parser.print_help()
        sys.exit(1)

    if args.source and (args.country or args.name):
        logger.warning("filter source no options country or indicator -_")
        parser.print_help()
        sys.exit(1)

    # If export json, replay and dry run mode, persist raw and stg should be false
    # because export json is for inspect data structure and pipeline state, not for persist data or replay data
    if args.export_json and (args.persist_raw or args.persist_stg):
        logger.warning("Export JSON cannot be used with  persist options")
        parser.print_help()
        sys.exit(1)

    # If persist raw, persist stg should be false
    if args.persist_raw and args.persist_stg:
        logger.warning("Cannot persist both raw and staging data at the same time")
        parser.print_help()
        sys.exit(1)
    if args.persist_raw and args.stage != "fetch":
        logger.warning("persist_raw invalid stage")
        parser.print_help()
        sys.exit(1)

    if args.persist_stg and args.stage != "parse":
        logger.warning("persist_stg invalid stage")
        parser.print_help()
        sys.exit(1)

    return args


async def main():
    """
    Main entry point for the pipeline execution.

    This function orchestrates the entire pipeline process, including configuration,
    argument parsing, database connection setup, and data processing stages.
    """
    args = valid_args()

    # if list indicator called
    if args is None:
        return None

    # Setup logging
    try:
        if args.log_level:
            target_level: int = resolve_log_level(args.log_level)
            apply_log_level(target_level)

    except Exception as e:
        logger.exception("Errors setup logging: %s", e, exc_info=True)

    try:
        async with AsyncConnectionPool[AsyncConnection[TupleRow]](
            conninfo=CONN_STR,
            min_size=1,
            max_size=7,
            max_waiting=10,
            timeout=10,
        ) as pool:
            # Execute Pipeline
            orchest: PipelineCliParser = build_injection(pool)

            async with orchest as orch:
                # table create
                await orch.flows.load_raw.create_register_path_table()
                await orch.flows.load_raw.create_raw_respons_table()
                await orch.flows.load_stg.create_stg_table()

                # Execution
                await orch.runner(args)

    except exc.PipelineCrash as e:
        logger.exception("Error during execution pipeline: %s", e)
        print(f"\nFull traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
