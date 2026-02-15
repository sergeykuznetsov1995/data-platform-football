"""
HDFS Client (WebHDFS)
=====================

Lightweight HDFS client using WebHDFS REST API.
No JVM dependencies required.
"""

import logging
import os
from typing import List, Optional
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)


class HDFSClient:
    """
    WebHDFS client for HDFS operations without JVM.

    Usage:
        client = HDFSClient()

        # Create directory
        client.mkdir('/data/bronze/fbref')

        # Upload file
        client.upload_file('/tmp/data.parquet', '/data/bronze/fbref/data.parquet')

        # Check existence
        if client.exists('/data/bronze/fbref'):
            ...

        # List directory
        files = client.list_dir('/data/bronze/fbref')

        # Delete
        client.delete('/data/bronze/fbref/old.parquet')
    """

    def __init__(
        self,
        namenode_host: str = 'namenode',
        port: int = 9870,
        user: str = 'root',
        timeout: int = 30,
    ):
        """
        Initialize WebHDFS client.

        Args:
            namenode_host: HDFS NameNode hostname
            port: WebHDFS port (default 9870)
            user: HDFS user for operations
            timeout: Request timeout in seconds
        """
        self.namenode_host = namenode_host
        self.port = port
        self.user = user
        self.timeout = timeout
        self.base_url = f"http://{namenode_host}:{port}/webhdfs/v1"
        self._session = None

    @property
    def session(self) -> requests.Session:
        """Get or create requests session."""
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def _url(self, path: str, op: str, **params) -> str:
        """Build WebHDFS URL."""
        # Ensure path starts with /
        if not path.startswith('/'):
            path = '/' + path

        # URL encode the path (but not the slashes)
        encoded_path = '/'.join(quote(segment, safe='') for segment in path.split('/'))

        url = f"{self.base_url}{encoded_path}?op={op}&user.name={self.user}"

        for key, value in params.items():
            if value is not None:
                url += f"&{key}={value}"

        return url

    def mkdir(self, path: str, permission: str = '755') -> bool:
        """
        Create directory in HDFS.

        Args:
            path: HDFS directory path
            permission: Unix permission (default 755)

        Returns:
            True if created successfully
        """
        url = self._url(path, 'MKDIRS', permission=permission)

        try:
            response = self.session.put(url, timeout=self.timeout)
            response.raise_for_status()

            result = response.json()
            success = result.get('boolean', False)

            if success:
                logger.debug(f"Created directory: {path}")
            else:
                logger.warning(f"Failed to create directory: {path}")

            return success

        except requests.RequestException as e:
            logger.error(f"Error creating directory {path}: {e}")
            raise HDFSError(f"Failed to create directory {path}: {e}") from e

    def upload_file(self, local_path: str, hdfs_path: str, overwrite: bool = True) -> bool:
        """
        Upload local file to HDFS.

        Args:
            local_path: Path to local file
            hdfs_path: Destination path in HDFS
            overwrite: Overwrite if exists

        Returns:
            True if uploaded successfully
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")

        # Ensure parent directory exists
        parent_dir = os.path.dirname(hdfs_path)
        if parent_dir and parent_dir != '/':
            self.mkdir(parent_dir)

        # Step 1: Get redirect URL
        url = self._url(hdfs_path, 'CREATE', overwrite=str(overwrite).lower())

        try:
            # WebHDFS returns redirect to DataNode
            response = self.session.put(
                url,
                allow_redirects=False,
                timeout=self.timeout
            )

            if response.status_code not in (307, 201):
                response.raise_for_status()

            # Step 2: Upload to DataNode (if redirected)
            if response.status_code == 307:
                datanode_url = response.headers['Location']

                with open(local_path, 'rb') as f:
                    upload_response = self.session.put(
                        datanode_url,
                        data=f,
                        headers={'Content-Type': 'application/octet-stream'},
                        timeout=self.timeout * 2  # Longer timeout for upload
                    )
                    upload_response.raise_for_status()

            logger.info(f"Uploaded {local_path} to {hdfs_path}")
            return True

        except requests.RequestException as e:
            logger.error(f"Error uploading {local_path} to {hdfs_path}: {e}")
            raise HDFSError(f"Failed to upload file: {e}") from e

    def upload_bytes(self, data: bytes, hdfs_path: str, overwrite: bool = True) -> bool:
        """
        Upload bytes directly to HDFS.

        Args:
            data: Bytes to upload
            hdfs_path: Destination path in HDFS
            overwrite: Overwrite if exists

        Returns:
            True if uploaded successfully
        """
        # Ensure parent directory exists
        parent_dir = os.path.dirname(hdfs_path)
        if parent_dir and parent_dir != '/':
            self.mkdir(parent_dir)

        url = self._url(hdfs_path, 'CREATE', overwrite=str(overwrite).lower())

        try:
            # Get redirect URL
            response = self.session.put(
                url,
                allow_redirects=False,
                timeout=self.timeout
            )

            if response.status_code not in (307, 201):
                response.raise_for_status()

            # Upload to DataNode
            if response.status_code == 307:
                datanode_url = response.headers['Location']

                upload_response = self.session.put(
                    datanode_url,
                    data=data,
                    headers={'Content-Type': 'application/octet-stream'},
                    timeout=self.timeout * 2
                )
                upload_response.raise_for_status()

            logger.info(f"Uploaded {len(data)} bytes to {hdfs_path}")
            return True

        except requests.RequestException as e:
            logger.error(f"Error uploading bytes to {hdfs_path}: {e}")
            raise HDFSError(f"Failed to upload bytes: {e}") from e

    def exists(self, path: str) -> bool:
        """
        Check if path exists in HDFS.

        Args:
            path: HDFS path to check

        Returns:
            True if path exists
        """
        url = self._url(path, 'GETFILESTATUS')

        try:
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code == 404:
                return False

            response.raise_for_status()
            return True

        except requests.RequestException:
            return False

    def list_dir(self, path: str) -> List[dict]:
        """
        List directory contents.

        Args:
            path: HDFS directory path

        Returns:
            List of file/directory info dicts with keys:
            - name: File/directory name
            - type: 'FILE' or 'DIRECTORY'
            - size: Size in bytes (files only)
            - modificationTime: Modification timestamp (ms)
        """
        url = self._url(path, 'LISTSTATUS')

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            statuses = data.get('FileStatuses', {}).get('FileStatus', [])

            result = []
            for status in statuses:
                result.append({
                    'name': status.get('pathSuffix'),
                    'type': status.get('type'),
                    'size': status.get('length', 0),
                    'modificationTime': status.get('modificationTime'),
                    'permission': status.get('permission'),
                    'owner': status.get('owner'),
                })

            return result

        except requests.RequestException as e:
            logger.error(f"Error listing directory {path}: {e}")
            raise HDFSError(f"Failed to list directory: {e}") from e

    def delete(self, path: str, recursive: bool = False) -> bool:
        """
        Delete file or directory from HDFS.

        Args:
            path: HDFS path to delete
            recursive: Delete recursively (required for non-empty directories)

        Returns:
            True if deleted successfully
        """
        url = self._url(path, 'DELETE', recursive=str(recursive).lower())

        try:
            response = self.session.delete(url, timeout=self.timeout)
            response.raise_for_status()

            result = response.json()
            success = result.get('boolean', False)

            if success:
                logger.debug(f"Deleted: {path}")
            else:
                logger.warning(f"Failed to delete: {path}")

            return success

        except requests.RequestException as e:
            logger.error(f"Error deleting {path}: {e}")
            raise HDFSError(f"Failed to delete: {e}") from e

    def get_file_status(self, path: str) -> Optional[dict]:
        """
        Get file/directory status.

        Args:
            path: HDFS path

        Returns:
            Status dict or None if not found
        """
        url = self._url(path, 'GETFILESTATUS')

        try:
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code == 404:
                return None

            response.raise_for_status()

            data = response.json()
            status = data.get('FileStatus', {})

            return {
                'path': path,
                'type': status.get('type'),
                'size': status.get('length', 0),
                'modificationTime': status.get('modificationTime'),
                'permission': status.get('permission'),
                'owner': status.get('owner'),
                'group': status.get('group'),
                'replication': status.get('replication'),
                'blockSize': status.get('blockSize'),
            }

        except requests.RequestException as e:
            logger.error(f"Error getting status for {path}: {e}")
            return None

    def read_file(self, path: str) -> bytes:
        """
        Read file contents from HDFS.

        Args:
            path: HDFS file path

        Returns:
            File contents as bytes
        """
        url = self._url(path, 'OPEN')

        try:
            response = self.session.get(
                url,
                allow_redirects=True,
                timeout=self.timeout * 2
            )
            response.raise_for_status()

            return response.content

        except requests.RequestException as e:
            logger.error(f"Error reading file {path}: {e}")
            raise HDFSError(f"Failed to read file: {e}") from e

    def close(self):
        """Close the session."""
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class HDFSError(Exception):
    """HDFS operation error."""
    pass
