"""
Tests for Document connectors — Local + cloud storage (S3, ADLS, GCS, OneDrive).
"""

from unittest.mock import MagicMock, patch

import pytest
import yaml
from connectors.documents.connector import LocalDocumentConnector
from connectors.documents.connector import S3DocumentConnector
from connectors.documents.connector import ADLSDocumentConnector
from connectors.documents.connector import GCSDocumentConnector
from connectors.documents.connector import OneDriveDocumentConnector




def _make_doc_config(tmp_path, source_type, source_cfg):
    """Build a consolidated document config.yaml and return its path."""
    cfg = {
        "common": {
            "document_types": {
                "maintenance": {
                    "path": "maintenance",
                    "extensions": [".pdf", ".docx"],
                },
                "sop": {
                    "path": "sop",
                    "extensions": [".pdf"],
                },
            },
            "filters": {
                "max_file_size_mb": 100,
            },
        },
        "sources": {source_type: source_cfg},
    }
    p = tmp_path / "doc_config.yaml"
    p.write_text(yaml.dump(cfg))
    return str(p)


def _create_doc_tree(base_dir, tree):
    """Create a document folder tree. tree = {subdir: {filename: content}}"""
    for subdir, files in tree.items():
        d = base_dir / subdir
        d.mkdir(parents=True, exist_ok=True)
        for fname, content in files.items():
            (d / fname).write_bytes(content)




class TestLocalDocumentConnector:
    def test_extract_document_types(self, tmp_path, dest_config_path, tmp_staging):
        base = tmp_path / "docs"
        _create_doc_tree(
            base,
            {
                "maintenance": {
                    "report_001.pdf": b"%PDF-maintenance-report",
                    "checklist.docx": b"DOCX-checklist",
                },
                "sop": {
                    "sop_valve.pdf": b"%PDF-sop-valve",
                },
            },
        )

        config_path = _make_doc_config(
            tmp_path,
            "local",
            {
                "base_path": str(base),
            },
        )

        conn = LocalDocumentConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        conn.connect()
        results = conn.extract()

        assert len(results) == 2
        doc_types = {r["document_type"] for r in results}
        assert doc_types == {"maintenance", "sop"}

        maint = [r for r in results if r["document_type"] == "maintenance"][0]
        assert len(maint["df"]) == 2

        sop = [r for r in results if r["document_type"] == "sop"][0]
        assert len(sop["df"]) == 1

        assert (tmp_staging / "documents" / "maintenance").exists()
        assert (tmp_staging / "documents" / "sop").exists()

    def test_extension_filtering(self, tmp_path, dest_config_path):

        base = tmp_path / "docs_ext"
        _create_doc_tree(
            base,
            {
                "maintenance": {
                    "report.pdf": b"pdf-file",
                    "image.jpg": b"jpg-file",
                    "notes.txt": b"txt-file",
                },
            },
        )

        config_path = _make_doc_config(
            tmp_path,
            "local",
            {
                "base_path": str(base),
            },
        )

        conn = LocalDocumentConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        conn.connect()
        results = conn.extract()

        maint = [r for r in results if r["document_type"] == "maintenance"][0]
        assert len(maint["df"]) == 1
        assert maint["df"].iloc[0]["filename"] == "report.pdf"

    def test_empty_directory(self, tmp_path, dest_config_path):

        base = tmp_path / "docs_empty"
        (base / "maintenance").mkdir(parents=True)
        (base / "sop").mkdir(parents=True)

        config_path = _make_doc_config(
            tmp_path,
            "local",
            {
                "base_path": str(base),
            },
        )

        conn = LocalDocumentConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        conn.connect()
        results = conn.extract()

        assert results == []

    def test_missing_base_path_raises(self, tmp_path, dest_config_path):

        config_path = _make_doc_config(tmp_path, "local", {})
        conn = LocalDocumentConnector(config_path, dest_config_path, "local", "local")
        with pytest.raises(ValueError, match="base_path"):
            conn.validate_config()

    def test_missing_document_types_raises(self, tmp_path, dest_config_path):

        cfg = {
            "common": {},
            "sources": {"local": {"base_path": str(tmp_path)}},
        }
        p = tmp_path / "doc_no_types.yaml"
        p.write_text(yaml.dump(cfg))

        conn = LocalDocumentConnector(str(p), dest_config_path, "local", "local")
        with pytest.raises(ValueError, match="document_types"):
            conn.validate_config()

    def test_max_file_size_filter(self, tmp_path, dest_config_path):

        base = tmp_path / "docs_size"
        _create_doc_tree(
            base,
            {
                "maintenance": {
                    "small.pdf": b"x" * 100,
                    "huge.pdf": b"x" * (2 * 1024 * 1024),
                },
            },
        )

        config_path = _make_doc_config(
            tmp_path,
            "local",
            {
                "base_path": str(base),
                "filters": {"max_file_size_mb": 1},
            },
        )

        conn = LocalDocumentConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        conn.connect()
        results = conn.extract()

        maint = [r for r in results if r["document_type"] == "maintenance"][0]
        assert len(maint["df"]) == 1
        assert maint["df"].iloc[0]["filename"] == "small.pdf"

    def test_metadata_columns(self, tmp_path, dest_config_path):

        base = tmp_path / "docs_meta"
        _create_doc_tree(
            base,
            {
                "maintenance": {"test.pdf": b"content123"},
            },
        )

        config_path = _make_doc_config(
            tmp_path,
            "local",
            {
                "base_path": str(base),
            },
        )

        conn = LocalDocumentConnector(config_path, dest_config_path, "local", "local")
        conn.validate_config()
        conn.connect()
        results = conn.extract()

        df = results[0]["df"]
        assert "filename" in df.columns
        assert "document_type" in df.columns
        assert "file_extension" in df.columns
        assert "size_bytes" in df.columns
        assert df.iloc[0]["file_extension"] == ".pdf"
        assert df.iloc[0]["size_bytes"] == len(b"content123")

    def test_run_full_lifecycle(self, tmp_path, dest_config_path, tmp_staging):

        base = tmp_path / "docs_run"
        _create_doc_tree(
            base,
            {
                "maintenance": {"lifecycle.pdf": b"%PDF-lifecycle"},
            },
        )

        config_path = _make_doc_config(
            tmp_path,
            "local",
            {
                "base_path": str(base),
            },
        )

        conn = LocalDocumentConnector(config_path, dest_config_path, "local", "local")
        conn.run()

        maint_dir = tmp_staging / "documents" / "maintenance"
        assert maint_dir.exists()
        files = list(maint_dir.iterdir())
        assert len(files) >= 1




class TestS3DocumentConnector:
    @patch("connectors.documents.connector.BaseConnector.__init__")
    def test_validate_config_ok(self, mock_init):

        conn = S3DocumentConnector.__new__(S3DocumentConnector)
        conn.source_cfg = {
            "bucket": "test-bucket",
            "connection": {"access_key": "AK", "secret_key": "SK"},
        }
        conn.validate_config()

    @patch("connectors.documents.connector.BaseConnector.__init__")
    def test_validate_missing_bucket(self, mock_init):
        conn = S3DocumentConnector.__new__(S3DocumentConnector)
        conn.source_cfg = {"connection": {"access_key": "a", "secret_key": "b"}}
        with pytest.raises(ValueError, match="bucket"):
            conn.validate_config()

    @patch("boto3.client")
    def test_extract_s3_documents(
        self, mock_boto_client, tmp_path, dest_config_path, tmp_staging
    ):

        config_path = _make_doc_config(
            tmp_path,
            "s3",
            {
                "bucket": "doc-bucket",
                "prefix": "company-docs",
                "connection": {
                    "access_key": "AKTEST",
                    "secret_key": "secret123",
                    "region": "us-east-1",
                },
            },
        )

        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        mock_paginator.paginate.side_effect = [
            [
                {
                    "Contents": [
                        {"Key": "company-docs/maintenance/report.pdf", "Size": 1024},
                        {
                            "Key": "company-docs/maintenance/checklist.docx",
                            "Size": 2048,
                        },
                    ]
                }
            ],
            [
                {
                    "Contents": [
                        {"Key": "company-docs/sop/sop_001.pdf", "Size": 512},
                    ]
                }
            ],
        ]

        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=b"fake-content"))
        }

        conn = S3DocumentConnector(config_path, dest_config_path, "s3", "local")
        conn._s3 = mock_s3
        results = conn.extract()

        assert len(results) == 2
        types = {r["document_type"] for r in results}
        assert types == {"maintenance", "sop"}




class TestADLSDocumentConnector:
    @patch("connectors.documents.connector.BaseConnector.__init__")
    def test_validate_config_ok(self, mock_init):
        conn = ADLSDocumentConnector.__new__(ADLSDocumentConnector)
        conn.source_cfg = {
            "container": "docs-container",
            "connection": {"account_name": "myaccount"},
        }
        conn.validate_config()

    @patch("connectors.documents.connector.BaseConnector.__init__")
    def test_validate_missing_container(self, mock_init):
        conn = ADLSDocumentConnector.__new__(ADLSDocumentConnector)
        conn.source_cfg = {"connection": {"account_name": "x"}}
        with pytest.raises(ValueError, match="container"):
            conn.validate_config()

    def test_extract_adls_documents(self, tmp_path, dest_config_path, tmp_staging):

        config_path = _make_doc_config(
            tmp_path,
            "adls",
            {
                "container": "docs-container",
                "prefix": "org-docs",
                "connection": {
                    "account_name": "testaccount",
                    "account_key": "fakekey",
                },
            },
        )

        conn = ADLSDocumentConnector(config_path, dest_config_path, "adls", "local")

        mock_fs = MagicMock()
        conn._fs = mock_fs

        p1 = MagicMock(is_directory=False, content_length=3000)
        p1.name = "org-docs/maintenance/report.pdf"
        p2 = MagicMock(is_directory=False, content_length=4000)
        p2.name = "org-docs/maintenance/notes.docx"

        p3 = MagicMock(is_directory=False, content_length=2000)
        p3.name = "org-docs/sop/valve_sop.pdf"

        mock_fs.get_paths.side_effect = [[p1, p2], [p3]]

        mock_fc = MagicMock()
        mock_fc.download_file.return_value = MagicMock(
            readall=MagicMock(return_value=b"data")
        )
        mock_fs.get_file_client.return_value = mock_fc

        results = conn.extract()

        assert len(results) == 2
        maint = [r for r in results if r["document_type"] == "maintenance"][0]
        assert len(maint["df"]) == 2




class TestGCSDocumentConnector:
    @patch("connectors.documents.connector.BaseConnector.__init__")
    def test_validate_config_ok(self, mock_init):

        conn = GCSDocumentConnector.__new__(GCSDocumentConnector)
        conn.source_cfg = {"bucket": "gcs-docs"}
        conn.validate_config()

    @patch("connectors.documents.connector.BaseConnector.__init__")
    def test_validate_missing_bucket(self, mock_init):
        conn = GCSDocumentConnector.__new__(GCSDocumentConnector)
        conn.source_cfg = {}
        with pytest.raises(ValueError, match="bucket"):
            conn.validate_config()

    def test_extract_gcs_documents(self, tmp_path, dest_config_path, tmp_staging):

        config_path = _make_doc_config(
            tmp_path,
            "gcs",
            {
                "bucket": "gcs-docs-bucket",
                "prefix": "docs",
                "connection": {"project_id": "test-project"},
            },
        )

        conn = GCSDocumentConnector(config_path, dest_config_path, "gcs", "local")

        mock_client = MagicMock()
        mock_bucket = MagicMock()
        conn._client = mock_client
        conn._bucket = mock_bucket

        blob1 = MagicMock(size=4096)
        blob1.name = "docs/maintenance/report.pdf"
        blob1.download_as_bytes.return_value = b"pdf-data"

        blob2 = MagicMock(size=2048)
        blob2.name = "docs/sop/valve.pdf"
        blob2.download_as_bytes.return_value = b"pdf-data"

        mock_client.list_blobs.side_effect = [[blob1], [blob2]]

        results = conn.extract()

        assert len(results) == 2
        types = {r["document_type"] for r in results}
        assert types == {"maintenance", "sop"}




class TestOneDriveDocumentConnector:
    @patch("connectors.documents.connector.BaseConnector.__init__")
    def test_validate_config_ok(self, mock_init):

        conn = OneDriveDocumentConnector.__new__(OneDriveDocumentConnector)
        conn.source_cfg = {
            "connection": {
                "tenant_id": "tid",
                "client_id": "cid",
                "client_secret": "csecret",
            }
        }
        conn.validate_config()

    @patch("connectors.documents.connector.BaseConnector.__init__")
    def test_validate_missing_client_id(self, mock_init):

        conn = OneDriveDocumentConnector.__new__(OneDriveDocumentConnector)
        conn.source_cfg = {"connection": {"tenant_id": "t"}}
        with pytest.raises(ValueError, match="client_id"):
            conn.validate_config()

    def test_extract_onedrive_documents(self, tmp_path, dest_config_path, tmp_staging):

        config_path = _make_doc_config(
            tmp_path,
            "onedrive",
            {
                "folder_path": "Shared Documents",
                "site_id": "test-site-id",
                "connection": {
                    "tenant_id": "test-tenant",
                    "client_id": "test-client",
                    "client_secret": "test-secret",
                },
            },
        )

        conn = OneDriveDocumentConnector(
            config_path, dest_config_path, "onedrive", "local"
        )

        mock_session = MagicMock()
        conn._session = mock_session
        conn._drive_url = "https://graph.microsoft.com/v1.0/sites/test-site-id/drive"
        conn._timeout = 300

        maint_resp = MagicMock()
        maint_resp.json.return_value = {
            "value": [
                {
                    "name": "report.pdf",
                    "size": 1024,
                    "file": {},
                    "@microsoft.graph.downloadUrl": "https://dl/1",
                },
                {
                    "name": "image.jpg",
                    "size": 512,
                    "file": {},
                    "@microsoft.graph.downloadUrl": "https://dl/2",
                },
            ],
        }
        maint_resp.raise_for_status = MagicMock()

        sop_resp = MagicMock()
        sop_resp.json.return_value = {
            "value": [
                {
                    "name": "sop_001.pdf",
                    "size": 2000,
                    "file": {},
                    "@microsoft.graph.downloadUrl": "https://dl/3",
                },
            ],
        }
        sop_resp.raise_for_status = MagicMock()

        dl_resp = MagicMock()
        dl_resp.content = b"downloaded-doc"
        dl_resp.raise_for_status = MagicMock()

        mock_session.get.side_effect = [maint_resp, dl_resp, sop_resp, dl_resp]

        results = conn.extract()

        assert len(results) == 2
        maint = [r for r in results if r["document_type"] == "maintenance"][0]
        assert len(maint["df"]) == 1
        assert maint["df"].iloc[0]["filename"] == "report.pdf"

        sop = [r for r in results if r["document_type"] == "sop"][0]
        assert len(sop["df"]) == 1




class TestDestinationBackwardCompat:
    def test_old_format_still_works(
        self, tmp_path, dest_config_path_old_format, tmp_staging
    ):
        """Old single-destination config format should still work."""
        base = tmp_path / "docs_compat"
        _create_doc_tree(
            base,
            {
                "maintenance": {"old.pdf": b"%PDF-old"},
            },
        )

        config_path = _make_doc_config(
            tmp_path,
            "local",
            {
                "base_path": str(base),
            },
        )

        conn = LocalDocumentConnector(config_path, dest_config_path_old_format, "local")
        conn.run()

        maint_dir = tmp_staging / "documents" / "maintenance"
        assert maint_dir.exists()
