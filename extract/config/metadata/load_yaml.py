import yaml
from pathlib import Path
from src.providers.ons.model import ONSConfigModel
from src.providers.bls.model import BLSConfigModel
from src.providers.bea.model import BEAConfigModel
from src.providers.fred.model import FREDConfigModel
from src.providers.metamodel import BaseMetaModel
import logging

logger = logging.getLogger(__name__)

MODEL_MAP: dict[str, type[BaseMetaModel]] = {
    "bls": BLSConfigModel,
    "bea": BEAConfigModel,
    "fred": FREDConfigModel,
    "ons": ONSConfigModel,
}

IndicatorModel = BaseMetaModel
CategoryModel = dict[str, IndicatorModel]
CountryModel = dict[str, CategoryModel]
AllIndicatorsModel = dict[str, CountryModel]

DIR_PATH = Path(__file__).resolve().parents[0]
DIR_PATH.mkdir(parents=True, exist_ok=True)


def load_all_indicator() -> AllIndicatorsModel:
    """Loads all indicators from the YAML files in the metadata."""
    # read catalog file
    try:
        catalog = yaml.safe_load((DIR_PATH / "catalog.yaml").read_text())
        all_indicators: AllIndicatorsModel = {}
        for country, categories in catalog.items():
            all_indicators[country] = {}
            for category in categories:
                file = DIR_PATH / country / f"{category}.yaml"
                data = yaml.safe_load(file.read_text())
                all_indicators[country][category] = {
                    indicator_name: MODEL_MAP[fields["source"]](**fields)
                    for indicator_name, fields in data.items()
                }
    except (KeyError, FileNotFoundError, yaml.YAMLError) as e:
        logger.exception("Error loading indicators from YAML files: %s", e)
        raise

    return all_indicators
