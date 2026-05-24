import yaml
from pathlib import Path
from providers.bls.model import BLSConfigModel
from providers.bea.model import BEAConfigModel
from providers.fred.model import FREDConfigModel
from providers.metamodel import BaseMetaModel

MODEL_MAP: dict[str, type[BaseMetaModel]] = {
    "bls": BLSConfigModel,
    "bea": BEAConfigModel,
    "fred": FREDConfigModel,
}

IndicatorModel = BaseMetaModel
CategoryModel = dict[str, IndicatorModel]
CountryModel = dict[str, CategoryModel]
AllIndicatorsModel = dict[str, CountryModel]

DIR_PATH = Path("src/config/metadata")


def load_all_indicator() -> AllIndicatorsModel:
    """Loads all indicators from the YAML files in the metadata."""
    # read catalog file
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

    return all_indicators
