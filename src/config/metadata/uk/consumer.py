from config.constants import (
    MONTHLY,
    SOURCE_URL,
    # QUARTERLY,
)

UK_CONSUMER = {
    "Retail_SalesValue_MoM": {
        "url": "https://www.ons.gov.uk/file?uri=/businessindustryandtrade/retailindustry/datasets/retailsalesindexreferencetables/current/mainreferencetables.xlsx",
        "cdid": "J5BT",
        "freq": MONTHLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "Retail sales (Value, MoM, incl. full)",
    },
    "Retail_SalesVolume_MoM": {
        "url": "https://www.ons.gov.uk/file?uri=/businessindustryandtrade/retailindustry/datasets/retailsalesindexreferencetables/current/mainreferencetables.xlsx",
        "cdid": "J5EC",  # T.E HeadLine Retail Sales MoM
        "freq": MONTHLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "Retail sales (Value, MoM, incl. full)",
    },
}

# Acces Latter if Nedded
"""
"Consumer_Spending": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/economy/nationalaccounts/satelliteaccounts/timeseries/abjr/ukea",
        "freq": QUARTERLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2020,
        "start_month": 1,
        "description": "Household final consumption expenditure :National concept CVM SA - £m",
    },
"""

