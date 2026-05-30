from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Resources(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parents[2] / ".env",
        env_file_encoding="utf-8",
    )

    bls_api_key: str | None = None
    fred_api_key: str | None = None
    bea_api_key: str | None = None
    postgres_dsn: str | None = None
    db_user: str | None = None
    db_password: str | None = None
    db_host: str | None = None
    db_port: int | None = None
    db_name: str | None = None


# connetion postgres
r = Resources()
CONN_STR = f"postgresql://{r.db_user}@{r.db_host}:{r.db_port}/{r.db_name}"
