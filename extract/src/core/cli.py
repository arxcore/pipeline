from dataclasses import dataclass
import logging
from core.flows import FlowsManager
from types import TracebackType
from typing import Optional
from enum import Enum

logger = logging.getLogger(__name__)


class Stage(Enum):
    FETCH = "fetch"
    PARSE = "parse"
    ALL = "all"


@dataclass
class PipelineConfig:
    """Pipeline Configuration"""

    stage: Stage
    source: list[str]
    country: str | None = None
    indicator_name: str | None = None
    export_json: bool = False
    replay: bool = False
    persist_raw: bool = False
    persist_stg: bool = False


class PipelineRunner:
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

    async def runner(self, cfg: PipelineConfig):
        """Execute cli runner Pipeline"""
        logger.info("Running Pipeline Stage: %s", cfg.stage)
        match Stage(cfg.stage):
            case Stage.FETCH:
                await self.flows.orchest_all_fetch(
                    source=cfg.source,
                    persist_raw=cfg.persist_raw,
                    replay=cfg.replay,
                    export_json=cfg.export_json,
                    country=cfg.country,
                    indicator=cfg.indicator_name,
                )
            case Stage.PARSE:
                await self.flows.parsing_all_db(
                    source=cfg.source,
                    export_json=cfg.export_json,
                    country=cfg.country,
                    indicator=cfg.indicator_name,
                    persist_stg=cfg.persist_stg,
                )
            case Stage.ALL:
                await self.flows.run_all_chain(
                    source=cfg.source,
                    export_json=cfg.export_json,
                    country=cfg.country,
                    indicator=cfg.indicator_name,
                )
