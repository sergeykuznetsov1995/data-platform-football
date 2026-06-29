"""
Unit tests for HDFSClient (WebHDFS).
"""

import pytest
from unittest.mock import MagicMock, patch, mock_open
import json

from scrapers.base.hdfs_client import HDFSClient, HDFSError


@pytest.fixture
def hdfs_client():
    """Create HDFSClient instance for testing."""
    return HDFSClient(
        namenode_host='test-namenode',
        port=9870,
        user='test-user',
        timeout=10,
    )


@pytest.fixture
def mock_session():
    """Mock requests session."""
    with patch('scrapers.base.hdfs_client.requests.Session') as mock:
        session = MagicMock()
        mock.return_value = session
        yield session


class TestHDFSClientInit:
    """Tests for HDFSClient initialization."""

    def test_init_default_values(self):
        """Test default initialization values."""
        client = HDFSClient()
        assert client.namenode_host == 'namenode'
        assert client.port == 9870
        assert client.user == 'root'
        assert client.timeout == 30
        assert client.base_url == 'http://namenode:9870/webhdfs/v1'

    def test_init_custom_values(self, hdfs_client):
        """Test custom initialization values."""
        assert hdfs_client.namenode_host == 'test-namenode'
        assert hdfs_client.port == 9870
        assert hdfs_client.user == 'test-user'
        assert hdfs_client.base_url == 'http://test-namenode:9870/webhdfs/v1'


class TestHDFSClientUrlBuilder:
    """Tests for URL building."""

    def test_url_with_leading_slash(self, hdfs_client):
        """Test URL building with leading slash."""
        url = hdfs_client._url('/data/bronze', 'MKDIRS')
        assert 'op=MKDIRS' in url
        assert 'user.name=test-user' in url
        assert '/data/bronze' in url

    def test_url_without_leading_slash(self, hdfs_client):
        """Test URL building without leading slash."""
        url = hdfs_client._url('data/bronze', 'MKDIRS')
        assert '/data/bronze' in url

    def test_url_with_params(self, hdfs_client):
        """Test URL building with additional parameters."""
        url = hdfs_client._url('/test', 'CREATE', overwrite='true', permission='755')
        assert 'overwrite=true' in url
        assert 'permission=755' in url


class TestHDFSClientMkdir:
    """Tests for mkdir operation."""

    def test_mkdir_success(self, hdfs_client, mock_session):
        """Test successful directory creation."""
        mock_session.put.return_value.json.return_value = {'boolean': True}
        mock_session.put.return_value.raise_for_status = MagicMock()

        result = hdfs_client.mkdir('/data/bronze/test')

        assert result is True
        mock_session.put.assert_called_once()

    def test_mkdir_failure(self, hdfs_client, mock_session):
        """Test failed directory creation."""
        mock_session.put.return_value.json.return_value = {'boolean': False}
        mock_session.put.return_value.raise_for_status = MagicMock()

        result = hdfs_client.mkdir('/data/bronze/test')

        assert result is False

    def test_mkdir_request_error(self, hdfs_client, mock_session):
        """Test mkdir with request error."""
        import requests
        mock_session.put.side_effect = requests.RequestException("Connection failed")

        with pytest.raises(HDFSError) as exc_info:
            hdfs_client.mkdir('/data/bronze/test')

        assert "Failed to create directory" in str(exc_info.value)


class TestHDFSClientUpload:
    """Tests for file upload operations."""

    def test_upload_file_success(self, hdfs_client, mock_session, tmp_path):
        """Test successful file upload."""
        # Create a test file
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test content")

        # Mock redirect response
        redirect_response = MagicMock()
        redirect_response.status_code = 307
        redirect_response.headers = {'Location': 'http://datanode:9870/upload'}

        upload_response = MagicMock()
        upload_response.raise_for_status = MagicMock()

        # Mock mkdir for parent directory
        mkdir_response = MagicMock()
        mkdir_response.json.return_value = {'boolean': True}
        mkdir_response.raise_for_status = MagicMock()

        mock_session.put.side_effect = [mkdir_response, redirect_response, upload_response]

        result = hdfs_client.upload_file(str(test_file), '/data/test.parquet')

        assert result is True

    def test_upload_file_not_found(self, hdfs_client):
        """Test upload with non-existent file."""
        with pytest.raises(FileNotFoundError):
            hdfs_client.upload_file('/nonexistent/file.parquet', '/data/test.parquet')

    def test_upload_bytes_success(self, hdfs_client, mock_session):
        """Test successful bytes upload."""
        # Mock mkdir response
        mkdir_response = MagicMock()
        mkdir_response.json.return_value = {'boolean': True}
        mkdir_response.raise_for_status = MagicMock()

        # Mock redirect response
        redirect_response = MagicMock()
        redirect_response.status_code = 307
        redirect_response.headers = {'Location': 'http://datanode:9870/upload'}

        upload_response = MagicMock()
        upload_response.raise_for_status = MagicMock()

        mock_session.put.side_effect = [mkdir_response, redirect_response, upload_response]

        result = hdfs_client.upload_bytes(b"test data", '/data/test.txt')

        assert result is True


class TestHDFSClientExists:
    """Tests for exists operation."""

    def test_exists_true(self, hdfs_client, mock_session):
        """Test exists returns True for existing path."""
        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.raise_for_status = MagicMock()

        result = hdfs_client.exists('/data/bronze')

        assert result is True

    def test_exists_false_404(self, hdfs_client, mock_session):
        """Test exists returns False for 404."""
        mock_session.get.return_value.status_code = 404

        result = hdfs_client.exists('/data/nonexistent')

        assert result is False

    def test_exists_false_on_error(self, hdfs_client, mock_session):
        """Test exists returns False on error."""
        import requests
        mock_session.get.side_effect = requests.RequestException("Error")

        result = hdfs_client.exists('/data/test')

        assert result is False


class TestHDFSClientListDir:
    """Tests for list_dir operation."""

    def test_list_dir_success(self, hdfs_client, mock_session):
        """Test successful directory listing."""
        mock_session.get.return_value.json.return_value = {
            'FileStatuses': {
                'FileStatus': [
                    {
                        'pathSuffix': 'file1.parquet',
                        'type': 'FILE',
                        'length': 1024,
                        'modificationTime': 1704067200000,
                        'permission': '644',
                        'owner': 'root',
                    },
                    {
                        'pathSuffix': 'subdir',
                        'type': 'DIRECTORY',
                        'length': 0,
                        'modificationTime': 1704067200000,
                        'permission': '755',
                        'owner': 'root',
                    },
                ]
            }
        }
        mock_session.get.return_value.raise_for_status = MagicMock()

        result = hdfs_client.list_dir('/data/bronze')

        assert len(result) == 2
        assert result[0]['name'] == 'file1.parquet'
        assert result[0]['type'] == 'FILE'
        assert result[1]['name'] == 'subdir'
        assert result[1]['type'] == 'DIRECTORY'

    def test_list_dir_empty(self, hdfs_client, mock_session):
        """Test listing empty directory."""
        mock_session.get.return_value.json.return_value = {
            'FileStatuses': {'FileStatus': []}
        }
        mock_session.get.return_value.raise_for_status = MagicMock()

        result = hdfs_client.list_dir('/data/empty')

        assert result == []


class TestHDFSClientDelete:
    """Tests for delete operation."""

    def test_delete_success(self, hdfs_client, mock_session):
        """Test successful delete."""
        mock_session.delete.return_value.json.return_value = {'boolean': True}
        mock_session.delete.return_value.raise_for_status = MagicMock()

        result = hdfs_client.delete('/data/test.parquet')

        assert result is True

    def test_delete_recursive(self, hdfs_client, mock_session):
        """Test recursive delete."""
        mock_session.delete.return_value.json.return_value = {'boolean': True}
        mock_session.delete.return_value.raise_for_status = MagicMock()

        result = hdfs_client.delete('/data/subdir', recursive=True)

        assert result is True
        call_args = mock_session.delete.call_args
        assert 'recursive=true' in call_args[0][0]


class TestHDFSClientGetFileStatus:
    """Tests for get_file_status operation."""

    def test_get_file_status_success(self, hdfs_client, mock_session):
        """Test successful file status retrieval."""
        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = {
            'FileStatus': {
                'type': 'FILE',
                'length': 2048,
                'modificationTime': 1704067200000,
                'permission': '644',
                'owner': 'root',
                'group': 'supergroup',
                'replication': 3,
                'blockSize': 134217728,
            }
        }
        mock_session.get.return_value.raise_for_status = MagicMock()

        result = hdfs_client.get_file_status('/data/test.parquet')

        assert result is not None
        assert result['type'] == 'FILE'
        assert result['size'] == 2048
        assert result['owner'] == 'root'

    def test_get_file_status_not_found(self, hdfs_client, mock_session):
        """Test file status for non-existent file."""
        mock_session.get.return_value.status_code = 404

        result = hdfs_client.get_file_status('/data/nonexistent')

        assert result is None


class TestHDFSClientReadFile:
    """Tests for read_file operation."""

    def test_read_file_success(self, hdfs_client, mock_session):
        """Test successful file read."""
        mock_session.get.return_value.content = b"file content"
        mock_session.get.return_value.raise_for_status = MagicMock()

        result = hdfs_client.read_file('/data/test.txt')

        assert result == b"file content"

    def test_read_file_error(self, hdfs_client, mock_session):
        """Test file read error."""
        import requests
        mock_session.get.side_effect = requests.RequestException("Read failed")

        with pytest.raises(HDFSError):
            hdfs_client.read_file('/data/test.txt')


class TestHDFSClientContextManager:
    """Tests for context manager support."""

    def test_context_manager(self, mock_session):
        """Test context manager usage."""
        with HDFSClient() as client:
            assert client is not None

    def test_close(self, hdfs_client, mock_session):
        """Test close method."""
        # Access session to create it
        _ = hdfs_client.session
        hdfs_client.close()
        # Session should be None after close
        assert hdfs_client._session is None
