from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Resources(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parents[2] / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    # local
    bls_api_key: str | None = None
    fred_api_key: str | None = None
    bea_api_key: str | None = None
    postgres_dsn: str | None = None
    db_user: str | None = None
    db_password: str | None = None
    db_host: str | None = None
    db_port: int | None = None
    db_name: str | None = None

    # supabase
    sb_password: str
    sb_host: str
    sb_port: int
    sb_database: str
    sb_user: str


# connection local postgres
# r = Resources()
# if r.postgres_dsn:
# CONN_STR = r.postgres_dsn
# elif r.db_password:
# CONN_STR = (
# f"postgresql://{r.db_user}:{r.db_password}@{r.db_host}:{r.db_port}/{r.db_name}"
# )
# else:
# CONN_STR = f"postgresql://{r.db_user}@{r.db_host}:{r.db_port}/{r.db_name}"

# supa
source = Resources()
CONN_STR = f"postgresql://{source.sb_user}:{source.sb_password}@{source.sb_host}:{source.sb_port}/{source.sb_database}"
