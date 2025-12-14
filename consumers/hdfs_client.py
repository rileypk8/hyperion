"""WebHDFS client for writing data to HDFS."""

import json
import logging
from datetime import datetime
from typing import Any
import requests

from config import settings

logger = logging.getLogger(__name__)


class HDFSClient:
    """
    Client for interacting with HDFS via WebHDFS REST API.

    Provides methods for creating directories and writing files
    to HDFS without requiring native Hadoop libraries.
    """

    def __init__(
        self,
        base_url: str = None,
        user: str = None,
    ):
        self.base_url = base_url or settings.webhdfs_base_url
        self.user = user or settings.hdfs_user
        self.session = requests.Session()

    def _url(self, path: str, op: str, **params) -> str:
        """Build WebHDFS URL with operation and parameters."""
        path = path.lstrip("/")
        url = f"{self.base_url}/{path}?op={op}&user.name={self.user}"
        for key, value in params.items():
            if value is not None:
                url += f"&{key}={value}"
        return url

    def mkdir(self, path: str, permission: str = "755") -> bool:
        """
        Create a directory in HDFS.

        Args:
            path: HDFS path to create
            permission: Unix-style permission string

        Returns:
            True if successful
        """
        url = self._url(path, "MKDIRS", permission=permission)
        try:
            response = self.session.put(url, allow_redirects=True)
            response.raise_for_status()
            result = response.json()
            return result.get("boolean", False)
        except requests.RequestException as e:
            logger.error(f"Failed to create directory {path}: {e}")
            return False

    def exists(self, path: str) -> bool:
        """Check if a path exists in HDFS."""
        url = self._url(path, "GETFILESTATUS")
        try:
            response = self.session.get(url)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def write_file(
        self,
        path: str,
        data: bytes | str,
        overwrite: bool = True,
    ) -> bool:
        """
        Write data to a file in HDFS.

        Uses WebHDFS two-step process:
        1. PUT to get redirect location
        2. PUT data to DataNode

        Args:
            path: HDFS path for the file
            data: Content to write (bytes or str)
            overwrite: Whether to overwrite existing file

        Returns:
            True if successful
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        url = self._url(path, "CREATE", overwrite=str(overwrite).lower())

        try:
            # Step 1: Get redirect to DataNode
            response = self.session.put(
                url,
                allow_redirects=False,
                headers={"Content-Type": "application/octet-stream"},
            )

            if response.status_code not in (307, 201):
                logger.error(f"Unexpected status {response.status_code} for CREATE: {response.text}")
                return False

            # Step 2: Write to DataNode
            if response.status_code == 307:
                datanode_url = response.headers.get("Location")
                if not datanode_url:
                    logger.error("No Location header in redirect")
                    return False

                response = self.session.put(
                    datanode_url,
                    data=data,
                    headers={"Content-Type": "application/octet-stream"},
                )
                response.raise_for_status()

            return True

        except requests.RequestException as e:
            logger.error(f"Failed to write file {path}: {e}")
            return False

    def append_file(self, path: str, data: bytes | str) -> bool:
        """
        Append data to an existing file in HDFS.

        Args:
            path: HDFS path for the file
            data: Content to append

        Returns:
            True if successful
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        url = self._url(path, "APPEND")

        try:
            # Step 1: Get redirect to DataNode
            response = self.session.post(url, allow_redirects=False)

            if response.status_code != 307:
                logger.error(f"Unexpected status {response.status_code} for APPEND")
                return False

            # Step 2: Append to DataNode
            datanode_url = response.headers.get("Location")
            if not datanode_url:
                logger.error("No Location header in redirect")
                return False

            response = self.session.post(
                datanode_url,
                data=data,
                headers={"Content-Type": "application/octet-stream"},
            )
            response.raise_for_status()
            return True

        except requests.RequestException as e:
            logger.error(f"Failed to append to file {path}: {e}")
            return False

    def write_json_batch(
        self,
        base_path: str,
        records: list[dict],
        partition_key: str = None,
    ) -> str | None:
        """
        Write a batch of JSON records to HDFS.

        Creates a timestamped file with newline-delimited JSON.

        Args:
            base_path: Base HDFS directory path
            records: List of records to write
            partition_key: Optional partition key for path (e.g., date)

        Returns:
            Path of created file, or None if failed
        """
        if not records:
            return None

        # Build path with optional partition
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        if partition_key:
            path = f"{base_path}/{partition_key}/batch_{timestamp}.json"
        else:
            path = f"{base_path}/batch_{timestamp}.json"

        # Ensure directory exists
        dir_path = "/".join(path.split("/")[:-1])
        self.mkdir(dir_path)

        # Convert records to newline-delimited JSON
        content = "\n".join(json.dumps(record) for record in records)

        if self.write_file(path, content):
            logger.info(f"Wrote {len(records)} records to {path}")
            return path
        return None


def get_hdfs_client() -> HDFSClient:
    """Get HDFS client instance."""
    return HDFSClient()
