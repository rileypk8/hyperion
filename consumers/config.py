"""Configuration for Kafka consumers that write to HDFS."""

from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class ConsumerSettings(BaseSettings):
    """Consumer settings loaded from environment variables."""

    # Kafka settings
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    consumer_group_id: str = Field(default="hyperion-hdfs-consumer")
    auto_offset_reset: str = Field(default="earliest")

    # HDFS settings
    hdfs_host: str = Field(default="localhost")
    hdfs_port: int = Field(default=9870)  # WebHDFS port
    hdfs_user: str = Field(default="root")

    # Consumer behavior
    batch_size: int = Field(default=100)
    batch_timeout_ms: int = Field(default=5000)
    poll_timeout_seconds: float = Field(default=1.0)

    @property
    def kafka_config(self) -> dict:
        """Build Kafka consumer config."""
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.consumer_group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,  # Manual commit after HDFS write
        }

    @property
    def webhdfs_base_url(self) -> str:
        """Build WebHDFS base URL."""
        return f"http://{self.hdfs_host}:{self.hdfs_port}/webhdfs/v1"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> ConsumerSettings:
    """Get cached settings instance."""
    return ConsumerSettings()


settings = get_settings()
