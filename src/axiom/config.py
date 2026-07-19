"""Application settings, read from the environment."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed, validated access to the environment configuration currently consumed by real code."""

    model_config = SettingsConfigDict(env_prefix="AXIOM_", env_file=".env", extra="ignore")

    database_url: str = "postgresql://axiom:axiom_dev@localhost:5432/axiom"
    redis_url: str = "redis://localhost:6379/0"

    db_pool_min_size: int = 2
    db_pool_max_size: int = 10

    relay_batch_size: int = 100
    relay_claim_lease_seconds: int = 30
    relay_redis_socket_timeout_seconds: int = 1
    relay_max_retries: int = 5
    relay_poll_interval_seconds: float = 0.1

    worker_lease_seconds: int = 30
    worker_heartbeat_interval_seconds: int = 10
    worker_xautoclaim_min_idle_seconds: int = 35
    worker_max_retries: int = 5
    worker_batch_size: int = 10


    log_level: str = "INFO"
    env: str = "development"


settings = Settings()