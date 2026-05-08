from config.constants import (
    # MOM,
    MONTHLY,
    # MONTHLY_BEA,
    # SOURCE_BEA,
    SOURCE_BLS,
    UNIT_PERCENT,
    YOY,
)
from providers.bls import BLSConfigModel

US_PRICE: dict[str, BLSConfigModel] = {
    "CPI_YoY": BLSConfigModel(
        code_name="CUUR0000SA0",
        source=SOURCE_BLS,
        freq=MONTHLY,
        start_year=2022,
        start_month=3,
        calc=YOY,
        unit=UNIT_PERCENT,
        description="Consumer Price Index, Year-over-Year Change",
    ),
    "CoreCPI_YoY": BLSConfigModel(
        code_name="CUUR0000SA0L1E",
        source=SOURCE_BLS,
        freq=MONTHLY,
        start_year=2022,
        start_month=3,
        calc=YOY,
        unit=UNIT_PERCENT,
        description="Core Consumer Price Index, Year-over-Year Change",
    ),
}
"""
# Acces Latter if Nedded
    "CPI_MoM": {
        "id": "CUUR0000SA0",
        "api": SOURCE_BLS,
        "freq": MONTHLY,
        "start_year": 2023,
        "start_month": 2,
        "calc": MOM,
        "unit": UNIT_PERCENT,
        "description": "Consumer Price Index, Month-over-Month Change",
    },
    "CoreCPI_MoM": {
        "id": "CUUR0000SA0L1E",
        "api": SOURCE_BLS,
"freq": MONTHLY,
        "start_year": 2023,
        "start_month": 2,
        "calc": MOM,
        "unit": UNIT_PERCENT,
        "description": "Core Consumer Price Index, Month-over-Month Change",
    },
    "PPI_YoY": {
        "id": "WPUFD4",
        "api": SOURCE_BLS,
        "freq": MONTHLY,
        "start_year": 2022,
        "start_month": 3,
        "calc": YOY,
        "unit": UNIT_PERCENT,
        "description": "Producer Price Index, Year-over-Year Change",
    },
    "PPI_MoM": {
        "id": "WPUFD4",
        "api": SOURCE_BLS,
        "freq": MONTHLY,
        "start_year": 2023,
        "start_month": 2,
        "calc": MOM,
        "unit": UNIT_PERCENT,
        "description": "Producer Price Index, Month-over-Month Change",
    },
    "Core_PCE_MoM": {
        "api": SOURCE_BEA,
        "dataset": "NIPA",
        "table": "T20807",
        "frequency": MONTHLY_BEA,
        "start_year": 2023,
        "start_month": 3,
        "calc": None,
        "line_number": "25",
        "unit": UNIT_PERCENT,
        "description": "Core Personal Consumption Expenditures Price Index, Month-over-Month Change",
    },
}
"""
