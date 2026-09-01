"""
Tests for the DestinationWriter — consolidated config + backward compat.
"""

import pandas as pd
import pytest
import yaml
from connectors.destination import DestinationWriter




class TestDestinationWriterConsolidated:
    def _make_dest_config(self, tmp_path, staging_path):
        cfg = {
            "common": {
                "layout": {
                    "documents": "documents/{document_type}",
                    "pnid": "pnid",
                    "sap": "sap/{table_name}",
                    "timeseries": "timeseries",
                },
                "format": {
                    "documents": "parquet",
                    "pnid": "parquet",
                    "sap": "parquet",
                    "timeseries": "parquet",
                },
                "options": {"overwrite": True, "compression": "snappy"},
            },
            "destinations": {
                "local": {"type": "local", "base_path": str(staging_path)},
                "other_local": {
                    "type": "local",
                    "base_path": str(staging_path / "other"),
                },
            },
        }
        p = tmp_path / "dest.yaml"
        p.write_text(yaml.dump(cfg))
        return str(p)

    def test_select_destination_type(self, tmp_path):
        staging = tmp_path / "staging"
        staging.mkdir()
        cfg_path = self._make_dest_config(tmp_path, staging)

        dw = DestinationWriter(cfg_path, destination_type="local")
        assert dw.dest_type == "local"
        assert dw.cfg["base_path"] == str(staging)

    def test_default_destination(self, tmp_path):

        staging = tmp_path / "staging"
        staging.mkdir()
        cfg_path = self._make_dest_config(tmp_path, staging)

        dw = DestinationWriter(cfg_path)
        assert dw.dest_type == "local"

    def test_unknown_destination_raises(self, tmp_path):
        staging = tmp_path / "staging"
        staging.mkdir()
        cfg_path = self._make_dest_config(tmp_path, staging)

        with pytest.raises(ValueError, match="Unknown destination"):
            DestinationWriter(cfg_path, destination_type="nonexistent")

    def test_write_dataframe(self, tmp_path):

        staging = tmp_path / "staging"
        staging.mkdir()
        cfg_path = self._make_dest_config(tmp_path, staging)

        dw = DestinationWriter(cfg_path, destination_type="local")

        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        dw.write_dataframe(df, "pnid", "manifest.parquet")

        written = staging / "pnid" / "manifest.parquet"
        assert written.exists()
        result = pd.read_parquet(written)
        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2"]

    def test_write_file_raw(self, tmp_path):

        staging = tmp_path / "staging"
        staging.mkdir()
        cfg_path = self._make_dest_config(tmp_path, staging)

        dw = DestinationWriter(cfg_path, destination_type="local")
        dw.write_file("pnid", "drawing.pdf", b"%PDF-content")

        written = staging / "pnid" / "drawing.pdf"
        assert written.exists()
        assert written.read_bytes() == b"%PDF-content"

    def test_layout_substitution(self, tmp_path):

        staging = tmp_path / "staging"
        staging.mkdir()
        cfg_path = self._make_dest_config(tmp_path, staging)

        dw = DestinationWriter(cfg_path, destination_type="local")
        dw.write_file("documents", "report.pdf", b"data", document_type="maintenance")

        written = staging / "documents" / "maintenance" / "report.pdf"
        assert written.exists()

    def test_common_options_merged(self, tmp_path):

        staging = tmp_path / "staging"
        staging.mkdir()
        cfg_path = self._make_dest_config(tmp_path, staging)

        dw = DestinationWriter(cfg_path, destination_type="local")
        assert dw.options["compression"] == "snappy"
        assert dw.layout["pnid"] == "pnid"




class TestDestinationWriterOldFormat:
    def test_old_format_loads(self, tmp_path):

        staging = tmp_path / "staging"
        staging.mkdir()
        cfg = {
            "destination": {
                "type": "local",
                "base_path": str(staging),
                "layout": {"pnid": "pnid"},
                "format": {"pnid": "parquet"},
                "options": {"overwrite": True, "compression": "snappy"},
            }
        }
        p = tmp_path / "old_dest.yaml"
        p.write_text(yaml.dump(cfg))

        dw = DestinationWriter(str(p))
        assert dw.dest_type == "local"

        df = pd.DataFrame({"x": [1]})
        dw.write_dataframe(df, "pnid", "test.parquet")
        assert (staging / "pnid" / "test.parquet").exists()

    def test_invalid_config_raises(self, tmp_path):

        cfg = {"something_else": {"type": "local"}}
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.dump(cfg))

        with pytest.raises(ValueError, match="destinations.*destination"):
            DestinationWriter(str(p))




class TestEnvResolution:
    def test_env_var_resolved(self, tmp_path, monkeypatch):

        staging = tmp_path / "env_staging"
        staging.mkdir()
        monkeypatch.setenv("TEST_DEST_PATH", str(staging))

        cfg = {
            "common": {
                "layout": {"pnid": "pnid"},
                "format": {"pnid": "parquet"},
                "options": {"overwrite": True},
            },
            "destinations": {
                "local": {
                    "type": "local",
                    "base_path": "${TEST_DEST_PATH}",
                },
            },
        }
        p = tmp_path / "env_dest.yaml"
        p.write_text(yaml.dump(cfg))

        dw = DestinationWriter(str(p), destination_type="local")
        assert dw.cfg["base_path"] == str(staging)

    def test_env_var_default(self, tmp_path):

        staging = tmp_path / "default_staging"
        staging.mkdir()

        cfg = {
            "common": {
                "layout": {"pnid": "pnid"},
                "format": {"pnid": "parquet"},
                "options": {"overwrite": True},
            },
            "destinations": {
                "local": {
                    "type": "local",
                    "base_path": "${NONEXISTENT_VAR:-" + str(staging) + "}",
                },
            },
        }
        p = tmp_path / "env_default.yaml"
        p.write_text(yaml.dump(cfg))

        dw = DestinationWriter(str(p), destination_type="local")
        assert dw.cfg["base_path"] == str(staging)
