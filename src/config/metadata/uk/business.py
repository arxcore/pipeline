from config.constants import MONTHLY, SOURCE_URL, YOY

UK_BUSINESS = {
    "IndustrialProduction_YoY": {
        "url": "https://www.ons.gov.uk/file?uri=/economy/economicoutputandproductivity/output/datasets/indexofproduction/current/diop.xlsx",
        "cdid": "K27Q",
        "freq": MONTHLY,
        "api": SOURCE_URL,
        "calc": YOY,
        "start_year": 2023,
        "start_month": 3,
        "description": "INDUSTRUAL PRODUCT PRODUCTION YOY Change DATASET = DIOP",
    },
    "IndustrialProduction_MoM": {
        "url": "https://www.ons.gov.uk/file?uri=/economy/economicoutputandproductivity/output/datasets/indexofproduction/current/diop.xlsx",
        "cdid": "K27Q",
        "freq": MONTHLY,
        "api": SOURCE_URL,
        "calc": None,
        "start_year": 2024,
        "start_month": 3,
        "description": "INDUSTRUAL PRODUCT PRODUCTION MoM Change DATASET = DIOP",
    },
}
