from config.constants import QSA, SOURCE_BEA, UNIT_BILLION, RAW
from providers.bea.model import BEAConfigModel


US_TRADE: dict[str, BEAConfigModel] = {
    "CurrentAccount": BEAConfigModel(
        code_name="BalCurrAcct",
        source=SOURCE_BEA,
        start_year=2020,
        start_month=1,
        calc=RAW,
        freq=QSA,
        unit=UNIT_BILLION,
        description="Balance on Current Account, Billions of Dollars",
    ),
}

# FRED
# TRADE: dict[str, BEAConfigModel] = {
#     "CurrentAccount": BEAConfigModel(
#         id="IEABC",
#         api=SOURCE_FRED,
#         start_year=2020,
#         start_month=1,
#         calc=RAW,
#         freq=QUARTERLY,
#         unit=UNIT_BILLION,
#         description="Balance on Current Account, Billions of Dollars",
#     )
# }
