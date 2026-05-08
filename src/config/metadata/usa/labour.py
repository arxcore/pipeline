from config.constants import (
    MOM,
    MONTHLY,
    NET,
    RAW,
    SOURCE_BLS,
    # SOURCE_FRED,
    UNIT_PEOPLE,
    UNIT_PERCENT,
    # WEEKLY,
)
from providers.bls.model import BLSConfigModel

US_LABOUR: dict[str, BLSConfigModel] = {
    "NFP": BLSConfigModel(
        code_name="CES0000000001",
        source=SOURCE_BLS,
        start_year=2024,
        start_month=2,
        calc=NET,
        freq=MONTHLY,
        unit=UNIT_PEOPLE,
        description="Nonfarm Payrolls, Net Change",
    ),
    "Unemployment": BLSConfigModel(
        code_name="LNS14000000",
        source=SOURCE_BLS,
        start_year=2024,
        start_month=3,
        calc=RAW,
        freq=MONTHLY,
        unit=UNIT_PERCENT,
        description="Unemployment Rate",
    ),
    "AverageHourlyEarnings": BLSConfigModel(
        code_name="CES0500000003",
        source=SOURCE_BLS,
        start_year=2024,
        start_month=2,
        calc=MOM,
        freq=MONTHLY,
        unit=UNIT_PERCENT,
        description="Average Hourly Earnings, Month-over-Month Change",
    ),
}
"""
    "InitialJoblessClaim": {
        "id": "ICSA",
        "api": SOURCE_FRED,
        "start_year": 2025,
        "start_month": 1,
        "calc": None,
        "freq": WEEKLY,
        "unit": UNIT_PEOPLE,
        "description": "Initial Jobless Claims, Seasonally Adjusted",
    },
}
"""
