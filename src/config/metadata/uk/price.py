from config.constants import (
    MONTHLY,
    SOURCE_URL,
    # YOY
    # MOM,
)


UK_PRICE = {
    "CPI_YoY": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflationandpriceindices/timeseries/d7g7/mm23",
        "freq": MONTHLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "CPI ANNUAL RATE 00: ALL ITEMS 2015=100",
    },
    "CoreCPI_YoY": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflationandpriceindices/timeseries/dko9/mm23",
        "freq": MONTHLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "CPI 12mth: Excluding energy & unprocessed food (SP) 2015=100",
    },
}
# Acces Latter if Nedded
"""
    },
    "CPI_MoM": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflationandpriceindices/timeseries/d7oe/mm23",
        "freq": MONTHLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "CPI MONTHLY RATE 00: ALL ITEMS 2015=100",
    },
    "CoreCPI_MoM": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflationandpriceindices/timeseries/dkc7/mm23",
        "freq": MONTHLY,
        "calc": MOM,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "CPI INDEX: Excluding energy & unprocessed food (SP) 2015=100",
    },
    "PPI_YoY": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflationandpriceindices/timeseries/ghik/ppi",
        "freq": MONTHLY,
        "calc": YOY,
        "api": SOURCE_URL,
        "start_year": 2020,
        "start_month": 3,
        "description": "PPI INDEX INPUT - C_MAT Inputs into production of Materials for all manufacturing, excluding Climate Change Levy 2015=100",
    },
    "PPI_MoM": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflationandpriceindices/timeseries/gb7s/mm22",
        "freq": MONTHLY,
        "calc": MOM,
        "api": SOURCE_URL,
        "start_year": 2023,
        "start_month": 3,
        "description": "PPI INDEX OUTPUT DOMESTIC - C Manufactured products, excluding Duty 2015=100",
    },
}
"""
