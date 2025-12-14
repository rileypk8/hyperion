"""Configuration for Kafka producers."""

from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache
from pathlib import Path


class ProducerSettings(BaseSettings):
    """Producer settings loaded from environment variables."""

    # Kafka settings
    kafka_bootstrap_servers: str = Field(default="localhost:9092")

    # Data paths
    data_dir: str = Field(default=str(Path(__file__).parent.parent / "data"))

    # Producer settings
    batch_size: int = Field(default=100)
    linger_ms: int = Field(default=10)

    @property
    def kafka_config(self) -> dict:
        """Build Kafka producer config."""
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "linger.ms": self.linger_ms,
            "batch.size": 16384,
            "acks": "all",
        }

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> ProducerSettings:
    """Get cached settings instance."""
    return ProducerSettings()


settings = get_settings()
