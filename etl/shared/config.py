"""Configuration settings for Hyperion ETL pipeline."""

from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Kafka settings
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    schema_registry_url: str = Field(default="http://localhost:8081")

    # PostgreSQL settings
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_user: str = Field(default="hyperion")
    postgres_password: str = Field(default="hyperion_dev")
    postgres_db: str = Field(default="hyperion")

    # Data paths
    data_dir: str = Field(default="../data")

    # ETL settings
    batch_size: int = Field(default=100)
    consumer_group_prefix: str = Field(default="hyperion-etl")

    @property
    def postgres_url(self) -> str:
        """Build PostgreSQL connection URL."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def kafka_config(self) -> dict:
        """Build Kafka producer/consumer config."""
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
        }

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


settings = get_settings()
