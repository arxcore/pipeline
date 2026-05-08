from config.constants import MOM, MONTHLY, SOURCE_FRED, UNIT_PERCENT
from providers.fred.model import FREDConfigModel

# UNIT_INDEX

US_CONSUMER: dict[str, FREDConfigModel] = {
    "Retail_Sales_MoM": FREDConfigModel(
        code_name="RSXFS",
        source=SOURCE_FRED,
        calc=MOM,
        freq=MONTHLY,
        start_year=2024,
        start_month=2,
        unit=UNIT_PERCENT,
        description="Retail Sales, Month-over-Month Change",
    ),
}

# Acces Latter if Nedded
"""
    "Michigan_Consumer_Sentiment": {
        "id": "UMCSENT",
        "api": SOURCE_FRED,
        "calc": None,
        "freq": MONTHLY,
        "start_year": 2024,
        "start_month": 3,
        "unit": UNIT_INDEX,
        "description": "University of Michigan Consumer Sentiment Index",
    },
}
"""
