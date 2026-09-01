"""
Tests for P&ID connectors — Local + cloud storage (S3, ADLS, GCS, OneDrive).
"""

from unittest.mock import MagicMock, patch

import pytest
import yaml
from connectors.pnid.connector import LocalPnidConnector
from connectors.pnid.connector import S3PnidConnector
from connectors.pnid.connector import ADLSPnidConnector
from connectors.pnid.connector import GCSPnidConnector
from connectors.pnid.connector import OneDrivePnidConnector




def _make_pnid_config(tmp_path, source_type, source_cfg):
    """Build a consolidated P&ID config.yaml and return its path."""
    cfg = {
        "common": {
            "extensions": [".pdf", ".tiff", ".tif", ".png"],
            "filters": {
                "max_file_size_mb": 100,
            },
        },
        "sources": {source_type: source_cfg},
    }
    p = tmp_path / "pnid_config.yaml"
    p.write_text(yaml.dump(cfg))
    return str(p)


def _create_pnid_files(base_dir, files):
    """Write dummy P&ID files into base_dir."""
    base_dir.mkdir(parents=True, exist_ok=True)
    for fname, content in files.items():
        (base_dir / fname).write_bytes(content)




class TestLocalPnidConnector:
    def test_extract_pdf_files(self, tmp_path, dest_config_path, tmp_staging):

        pnid_dir = tmp_path / "pnid_drawings"
        _create_pnid_files(
            pnid_dir,
            {
                "drawing_001.pdf": b"%PDF-1.4 fake pdf content",
                "drawing_002.pdf": b"%PDF-1.4 another drawing",
                "drawing_003.tiff": b"TIFF-fake-content",
            },
        )

        config_path = _make_pnid_config(
            tmp_path,
            "local",
            {
                "base_path": str(pnid_dir),
            },
        )

        conn = LocalPnidConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        conn.connect()
        results = conn.extract()

        assert len(results) == 1
        df = results[0]["df"]
        assert len(df) == 3
        assert set(df["file_extension"]) == {".pdf", ".tiff"}
        assert results[0]["filename"] == "pnid_manifest.parquet"

        pnid_staging = tmp_staging / "pnid"
        assert pnid_staging.exists()
        assert len(list(pnid_staging.iterdir())) == 3

    def test_extension_filter(self, tmp_path, dest_config_path):

        pnid_dir = tmp_path / "pnid_drawings"
        _create_pnid_files(
            pnid_dir,
            {
                "drawing.pdf": b"pdf",
                "readme.txt": b"not a drawing",
                "notes.docx": b"not a drawing",
            },
        )

        config_path = _make_pnid_config(
            tmp_path,
            "local",
            {
                "base_path": str(pnid_dir),
                "extensions": [".pdf"],
            },
        )

        conn = LocalPnidConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        conn.connect()
        results = conn.extract()

        assert len(results) == 1
        df = results[0]["df"]
        assert len(df) == 1
        assert df.iloc[0]["filename"] == "drawing.pdf"

    def test_empty_directory(self, tmp_path, dest_config_path):

        pnid_dir = tmp_path / "empty_pnid"
        pnid_dir.mkdir()

        config_path = _make_pnid_config(
            tmp_path,
            "local",
            {
                "base_path": str(pnid_dir),
            },
        )

        conn = LocalPnidConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        conn.connect()
        results = conn.extract()

        assert results == []

    def test_missing_base_path_raises(self, tmp_path, dest_config_path):

        config_path = _make_pnid_config(tmp_path, "local", {})

        conn = LocalPnidConnector(config_path, dest_config_path, "local", "local")
        with pytest.raises(ValueError, match="base_path"):
            conn.validate_config()

    def test_nonexistent_path_raises(self, tmp_path, dest_config_path):

        config_path = _make_pnid_config(
            tmp_path,
            "local",
            {
                "base_path": "/nonexistent/path/xyz",
            },
        )

        conn = LocalPnidConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        with pytest.raises(FileNotFoundError):
            conn.connect()

    def test_max_file_size_filter(self, tmp_path, dest_config_path):

        pnid_dir = tmp_path / "pnid_big"
        pnid_dir.mkdir()
        (pnid_dir / "small.pdf").write_bytes(b"x" * 100)
        (pnid_dir / "big.pdf").write_bytes(b"x" * (2 * 1024 * 1024))

        config_path = _make_pnid_config(
            tmp_path,
            "local",
            {
                "base_path": str(pnid_dir),
                "filters": {"max_file_size_mb": 1},
            },
        )

        conn = LocalPnidConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        conn.connect()
        results = conn.extract()

        assert len(results) == 1
        assert len(results[0]["df"]) == 1
        assert results[0]["df"].iloc[0]["filename"] == "small.pdf"

    def test_filename_pattern_filter(self, tmp_path, dest_config_path):

        pnid_dir = tmp_path / "pnid_filter"
        _create_pnid_files(
            pnid_dir,
            {
                "PID-001.pdf": b"pid1",
                "PID-002.pdf": b"pid2",
                "OTHER-001.pdf": b"other",
            },
        )

        config_path = _make_pnid_config(
            tmp_path,
            "local",
            {
                "base_path": str(pnid_dir),
                "filters": {"filename_pattern": "^PID-"},
            },
        )

        conn = LocalPnidConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        conn.connect()
        results = conn.extract()

        assert len(results[0]["df"]) == 2

    def test_run_full_lifecycle(self, tmp_path, dest_config_path, tmp_staging):
        """Test the complete run() lifecycle method."""

        pnid_dir = tmp_path / "pnid_run"
        _create_pnid_files(
            pnid_dir,
            {
                "test.pdf": b"%PDF-test",
            },
        )

        config_path = _make_pnid_config(
            tmp_path,
            "local",
            {
                "base_path": str(pnid_dir),
            },
        )

        conn = LocalPnidConnector(config_path, dest_config_path, "local", "local")
        conn.run()

        pnid_staging = tmp_staging / "pnid"
        assert pnid_staging.exists()
        parquets = list(pnid_staging.glob("*.parquet"))
        assert len(parquets) >= 1




class TestS3PnidConnector:
    def _mock_s3_setup(self, tmp_path, dest_config_path):
        config_path = _make_pnid_config(
            tmp_path,
            "s3",
            {
                "bucket": "test-bucket",
                "prefix": "pnid-files",
                "connection": {
                    "access_key": "AKTEST",
                    "secret_key": "secret123",
                    "region": "us-east-1",
                },
            },
        )
        return config_path

    @patch("connectors.pnid.connector.BaseConnector.__init__")
    def test_validate_config_ok(self, mock_init, tmp_path):

        conn = S3PnidConnector.__new__(S3PnidConnector)
        conn.source_cfg = {
            "bucket": "test-bucket",
            "connection": {"access_key": "x", "secret_key": "y"},
        }
        conn.validate_config()

    @patch("connectors.pnid.connector.BaseConnector.__init__")
    def test_validate_missing_bucket(self, mock_init):

        conn = S3PnidConnector.__new__(S3PnidConnector)
        conn.source_cfg = {}
        with pytest.raises(ValueError, match="bucket"):
            conn.validate_config()

    @patch("boto3.client")
    def test_extract_s3_files(
        self, mock_boto_client, tmp_path, dest_config_path, tmp_staging
    ):

        config_path = self._mock_s3_setup(tmp_path, dest_config_path)

        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "pnid-files/drawing_A.pdf", "Size": 1024},
                    {"Key": "pnid-files/drawing_B.tiff", "Size": 2048},
                    {"Key": "pnid-files/readme.txt", "Size": 100},
                ]
            }
        ]

        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=b"fake-content"))
        }

        conn = S3PnidConnector(config_path, dest_config_path, "s3", "local")
        conn._s3 = mock_s3
        results = conn.extract()

        assert len(results) == 1
        df = results[0]["df"]
        assert len(df) == 2
        fnames = set(df["filename"])
        assert "drawing_A.pdf" in fnames
        assert "drawing_B.tiff" in fnames
        assert "readme.txt" not in fnames




class TestADLSPnidConnector:
    @patch("connectors.pnid.connector.BaseConnector.__init__")
    def test_validate_config_ok(self, mock_init):

        conn = ADLSPnidConnector.__new__(ADLSPnidConnector)
        conn.source_cfg = {
            "container": "test-container",
            "connection": {"account_name": "myaccount"},
        }
        conn.validate_config()

    @patch("connectors.pnid.connector.BaseConnector.__init__")
    def test_validate_missing_container(self, mock_init):

        conn = ADLSPnidConnector.__new__(ADLSPnidConnector)
        conn.source_cfg = {}
        with pytest.raises(ValueError, match="container"):
            conn.validate_config()

    def test_extract_adls_files(self, tmp_path, dest_config_path, tmp_staging):

        config_path = _make_pnid_config(
            tmp_path,
            "adls",
            {
                "container": "pnid-container",
                "prefix": "drawings",
                "connection": {
                    "account_name": "testaccount",
                    "account_key": "fakekey123",
                },
            },
        )

        conn = ADLSPnidConnector(config_path, dest_config_path, "adls", "local")

        mock_fs = MagicMock()
        conn._fs = mock_fs

        mock_path1 = MagicMock()
        mock_path1.is_directory = False
        mock_path1.name = "drawings/site_A/PID-001.pdf"
        mock_path1.content_length = 5000

        mock_path2 = MagicMock()
        mock_path2.is_directory = False
        mock_path2.name = "drawings/site_A/PID-002.png"
        mock_path2.content_length = 8000

        mock_dir = MagicMock()
        mock_dir.is_directory = True
        mock_dir.name = "drawings/site_A"

        mock_fs.get_paths.return_value = [mock_dir, mock_path1, mock_path2]

        mock_file_client = MagicMock()
        mock_file_client.download_file.return_value = MagicMock(
            readall=MagicMock(return_value=b"fake-data")
        )
        mock_fs.get_file_client.return_value = mock_file_client

        results = conn.extract()

        assert len(results) == 1
        df = results[0]["df"]
        assert len(df) == 2
        assert set(df["filename"]) == {"PID-001.pdf", "PID-002.png"}




class TestGCSPnidConnector:
    @patch("connectors.pnid.connector.BaseConnector.__init__")
    def test_validate_config_ok(self, mock_init):

        conn = GCSPnidConnector.__new__(GCSPnidConnector)
        conn.source_cfg = {"bucket": "test-gcs-bucket"}
        conn.validate_config()

    @patch("connectors.pnid.connector.BaseConnector.__init__")
    def test_validate_missing_bucket(self, mock_init):

        conn = GCSPnidConnector.__new__(GCSPnidConnector)
        conn.source_cfg = {}
        with pytest.raises(ValueError, match="bucket"):
            conn.validate_config()

    def test_extract_gcs_files(self, tmp_path, dest_config_path, tmp_staging):

        config_path = _make_pnid_config(
            tmp_path,
            "gcs",
            {
                "bucket": "gcs-pnid-bucket",
                "prefix": "pnid",
                "connection": {
                    "project_id": "test-project",
                },
            },
        )

        conn = GCSPnidConnector(config_path, dest_config_path, "gcs", "local")

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        conn._client = mock_client
        conn._bucket = mock_bucket

        blob1 = MagicMock()
        blob1.name = "pnid/diagram_01.pdf"
        blob1.size = 4096
        blob1.download_as_bytes.return_value = b"pdf-data-1"

        blob2 = MagicMock()
        blob2.name = "pnid/diagram_02.tiff"
        blob2.size = 8192
        blob2.download_as_bytes.return_value = b"tiff-data-2"

        mock_client.list_blobs.return_value = [blob1, blob2]

        results = conn.extract()

        assert len(results) == 1
        df = results[0]["df"]
        assert len(df) == 2
        assert set(df["filename"]) == {"diagram_01.pdf", "diagram_02.tiff"}




class TestOneDrivePnidConnector:
    @patch("connectors.pnid.connector.BaseConnector.__init__")
    def test_validate_config_ok(self, mock_init):

        conn = OneDrivePnidConnector.__new__(OneDrivePnidConnector)
        conn.source_cfg = {
            "connection": {
                "tenant_id": "tid",
                "client_id": "cid",
                "client_secret": "csecret",
            }
        }
        conn.validate_config()

    @patch("connectors.pnid.connector.BaseConnector.__init__")
    def test_validate_missing_creds(self, mock_init):

        conn = OneDrivePnidConnector.__new__(OneDrivePnidConnector)
        conn.source_cfg = {"connection": {}}
        with pytest.raises(ValueError, match="tenant_id"):
            conn.validate_config()

    def test_extract_onedrive_files(self, tmp_path, dest_config_path, tmp_staging):

        config_path = _make_pnid_config(
            tmp_path,
            "onedrive",
            {
                "folder_path": "Engineering/PnID",
                "drive_id": "test-drive-id",
                "connection": {
                    "tenant_id": "test-tenant",
                    "client_id": "test-client",
                    "client_secret": "test-secret",
                },
            },
        )

        conn = OneDrivePnidConnector(config_path, dest_config_path, "onedrive", "local")

        mock_session = MagicMock()
        conn._session = mock_session
        conn._drive_url = "https://graph.microsoft.com/v1.0/drives/test-drive-id"
        conn._timeout = 300

        list_resp = MagicMock()
        list_resp.json.return_value = {
            "value": [
                {
                    "name": "PID-100.pdf",
                    "size": 5000,
                    "file": {},
                    "@microsoft.graph.downloadUrl": "https://dl/1",
                },
                {
                    "name": "PID-200.tiff",
                    "size": 7000,
                    "file": {},
                    "@microsoft.graph.downloadUrl": "https://dl/2",
                },
                {
                    "name": "notes.docx",
                    "size": 200,
                    "file": {},
                    "@microsoft.graph.downloadUrl": "https://dl/3",
                },
            ],
        }
        list_resp.raise_for_status = MagicMock()

        dl_resp = MagicMock()
        dl_resp.content = b"downloaded-content"
        dl_resp.raise_for_status = MagicMock()

        mock_session.get.side_effect = [list_resp, dl_resp, dl_resp]

        results = conn.extract()

        assert len(results) == 1
        df = results[0]["df"]
        assert len(df) == 2
        assert "PID-100.pdf" in df["filename"].values
        assert "notes.docx" not in df["filename"].values
