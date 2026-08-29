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

    # Full-jitter exponential backoff between retries of a failed handler.
    # Jitter is not decoration: without it, N workflows failing on the same
    # provider outage retry in lockstep and re-create the spike that caused
    # the outage.
    worker_retry_base_seconds: float = 1.0
    worker_retry_cap_seconds: float = 60.0

    # The highest chain_depth a workflow may reach; the root of a chain is 0,
    # so a chain may hold max_chain_depth + 1 workflows. A handler bug that
    # always chains would otherwise loop forever — and unlike a retry loop, a
    # chain loop is not bounded by max_retries, because every link is a fresh
    # workflow with a fresh budget.
    worker_max_chain_depth: int = 50


    log_level: str = "INFO"
    env: str = "development"


settings = Settings()