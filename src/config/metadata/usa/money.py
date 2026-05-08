from config.constants import MONTHLY, SOURCE_FRED, UNIT_PERCENT, RAW
from providers.fred import FREDConfigModel

# UNIT_BILLION

US_MONEY: dict[str, FREDConfigModel] = {
    "Fed_Interest_Rate": FREDConfigModel(
        code_name="FEDFUNDS",
        source=SOURCE_FRED,
        calc=RAW,
        freq=MONTHLY,
        start_year=2024,
        start_month=2,
        unit=UNIT_PERCENT,
        description="Federal Funds Effective Rate",
    ),
}


# Acces Latter if Nedded
"""
    "M2_Supply": {
        "id": "M2SL",
        "api": SOURCE_FRED,
        "calc": None,
        "freq": MONTHLY,
        "start_year": 2024,
        "start_month": 3,
        "unit": UNIT_BILLION,
        "description": "M2 Money Stock, Billions of Dollars",
    },
}
"""
