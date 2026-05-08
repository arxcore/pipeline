from config.constants import MOM, MONTHLY, SOURCE_FRED, UNIT_PERCENT

US_BUSINESS = {
    "Industrial_production_MoM": {
        "id": "INDPRO",
        "api": SOURCE_FRED,
        "calc": MOM,
        "freq": MONTHLY,
        "start_year": 2024,
        "start_month": 2,
        "unit": UNIT_PERCENT,
        "description": "Industrial Production Index, Month-over-Month Change",
    },
}
