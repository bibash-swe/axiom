"""Application settings, read from the environment."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed access to the environment configuration."""

    database_url: str = "postgresql://axiom:axiom_dev@localhost:5432/axiom"

    db_pool_min_size: int = 2
    db_pool_max_size: int = 10

    log_level: str = "INFO"

    env: str = "development"

    model_config = SettingsConfigDict(
        env_prefix="AXIOM_",
        env_file=".env",
        extra="ignore"
    )

settings = Settings()