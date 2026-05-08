from config.constants import (
    MONTHLY,
    PERCENT,
    SOURCE_URL,
    YOY,
    # QUARTERLY,
)

UK_LABOUR = {
    "Unemployment": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/employmentandlabourmarket/peoplenotinwork/unemployment/timeseries/mgsx/lms",
        "freq": MONTHLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "Unemployment rate (aged 16 and over, seasonally adjusted): %",
    },
    "AvgEarnExcldBonus_YoY": {
        "url": "https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/averageweeklyearningsearn01/current/earn01jun2025.xls",
        "cdid": "KAC4",
        "freq": MONTHLY,
        "calc": YOY,
        "api": SOURCE_URL,
        "start_year": 2023,
        "start_month": 3,
        "description": "AVERAGE EARNINGS EXCLUDING BONUS Dengan DataSet (EARN01)",
    },
    "Wage_Growth_YoY": {
        "url": "https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/averageweeklyearningsearn01/current/earn01jun2025.xls",
        "cdid": "KAC6",
        "freq": MONTHLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "WageGrowth Dengan DataSet (EARN01)",
    },
    "Claim_Count_Change": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/employmentandlabourmarket/peoplenotinwork/outofworkbenefits/timeseries/bcjd/unem",
        "freq": MONTHLY,
        "calc": PERCENT,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "Claimant Count : K02000001 UK : People : SA : Thousands",
    },
}


# Acces Latter if Nedded
"""
    "Job_Vacancies": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/timeseries/jp9z/lms",
        "freq": MONTHLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "UK Job Vacancies (thousands) - Total Services",
    },
    "Labour_Productivity_QoQ": {
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/employmentandlabourmarket/peopleinwork/labourproductivity/timeseries/txbb/prdy",
        "freq": QUARTERLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2020,
        "start_month": 1,
        "description": "UK Whole Economy: Output per hour worked % change quarter on previous quarter SA",
    },
    "RealEarnExcldBonus_YoY": {
        "url": "https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/averageweeklyearningsearn01/current/earn01jun2025.xls",
        "cdid": "A2F9",
        "freq": MONTHLY,
        "calc": None,
        "api": SOURCE_URL,
        "start_year": 2024,
        "start_month": 3,
        "description": "Real Earnings Excluding Bonus Dengan DataSet (EARN01)",
    },
}
# Labour productivity index seriID (longterm - analysis)
#    "Labour_Productivity_IDX": {
#        "url": "https://www.ons.gov.uk/generator?format=csv&uri=/employmentandlabourmarket/peopleinwork/labourproductivity/timeseries/lzvb/prdy",
#        "freq": QUARTERLY,
#        "calc": None,
#        "api": SOURCE_CSV,
#        "start_year": 2020,
#        "start_month": 1,
#        "description": "UK Whole Economy: Output per hour worked SA: Index 2022 = 100",
#    },
"""

