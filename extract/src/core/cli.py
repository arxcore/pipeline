import argparse
import logging
from core.flows import FlowsManager

logger = logging.getLogger(__name__)


class PipelineCliParser:
    def __init__(
        self,
        flows_manager: FlowsManager,
    ) -> None:
        self.flows = flows_manager

    async def __aenter__(self):
        await self.flows.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tcb: object | None,
    ):
        await self.flows.__aexit__(exc_type, exc_val, exc_tcb)

    async def runner(self, args: argparse.Namespace):
        """Execute cli runner Pipeline
        --source, --stage, export_json, replay, persist_raw, persist_stg
        """
        # data: list[FinalresultFetcher] | None = None

        match args.run:
            case "single":
                match args.stage:
                    case "fetch":
                        await self.flows.orchest_single_fetch(
                            args.country,
                            args.name,
                            persist_raw=args.persist_raw,
                            export_json=args.export_json,
                            replay=args.replay,
                        )
                    case "parse":
                        await self.flows.parsing_db(
                            args.country,
                            args.name,
                            export_json=args.export_json,
                            persist_stg=args.persist_stg,
                        )
                    case "all":
                        await self.flows.run_single_all_chain(
                            args.country, args.name, export_json=args.export_json
                        )
                    case _:
                        assert False, f"unhandel stage {args.stage}"
            case "all":
                match args.source:
                    case "bls":
                        pass
                    case "bea":
                        pass
                    case "fred":
                        pass
                    case "all":
                        print("source all")
                        pass
                    case _:
                        assert False, f"unhandel source {args.source}"
                match args.stage:
                    case "fetch":
                        await self.flows.orchest_all_fetch(
                            persist_raw=args.persist_raw,
                            replay=args.replay,
                            export_json=args.export_json,
                        )
                    case "parse":
                        await self.flows.parsing_all_db(
                            export_json=args.export_json,
                            persist_stg=args.persist_stg,
                        )
                    case "all":
                        await self.flows.run_all_chain(
                            export_json=args.export_json,
                        )
                    case _:
                        assert False, f"unhandel stage {args.stage}"
            case _:
                assert False, f"unhandel run {args.run}"
            # data[0].fetch_result.

        # Default Mode Run Pipeline
        # elif args.run == "all":
        #    logger.info("Run ALL Config Indicators")
        # data = await self.flows.run_all()

        # if args.load and data is not None:
        # setup table if not exists
        # await self.load_stg.create_stg_table()
        # load data
        # logger.info(
        #     "Loading %s indicator To database...", len(data[0].fetch_result)
        # )
        # await self.load_stg.load_stg_indicator(data[0].fetch_result)
