import argparse
import logging
from core.flows import FlowsManager
from types import TracebackType
from typing import Optional

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
        exc_tcb: Optional[TracebackType] | None,
    ):
        await self.flows.__aexit__(exc_type, exc_val, exc_tcb)

    async def runner(self, args: argparse.Namespace):
        """Execute cli runner Pipeline"""
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
                match args.stage:
                    case "fetch":
                        await self.flows.orchest_all_fetch(
                            persist_raw=args.persist_raw,
                            replay=args.replay,
                            export_json=args.export_json,
                            source=args.source,
                        )
                    case "parse":
                        await self.flows.parsing_all_db(
                            export_json=args.export_json,
                            source=args.source,
                            persist_stg=args.persist_stg,
                        )
                    case "all":
                        await self.flows.run_all_chain(
                            export_json=args.export_json, source=args.source
                        )
                    case _:
                        assert False, f"unhandel stage {args.stage}"
            case _:
                assert False, f"unhandel run {args.run}"
