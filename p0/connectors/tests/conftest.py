"""
Shared fixtures for connector tests.
"""

import pytest
import yaml


@pytest.fixture
def tmp_staging(tmp_path):
    """Provide a temporary staging directory for destination output."""
    staging = tmp_path / "staging"
    staging.mkdir()
    return staging


@pytest.fixture
def dest_config_path(tmp_path, tmp_staging):
    """Create a local destination config.yaml and return its path."""
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
            "options": {
                "overwrite": True,
                "compression": "snappy",
                "max_file_size_mb": 256,
            },
        },
        "destinations": {
            "local": {
                "type": "local",
                "base_path": str(tmp_staging),
            },
        },
    }
    p = tmp_path / "dest_config.yaml"
    p.write_text(yaml.dump(cfg))
    return str(p)


@pytest.fixture
def dest_config_path_old_format(tmp_path, tmp_staging):
    """Old single-destination format for backward compat tests."""
    cfg = {
        "destination": {
            "type": "local",
            "base_path": str(tmp_staging),
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
        }
    }
    p = tmp_path / "dest_old.yaml"
    p.write_text(yaml.dump(cfg))
    return str(p)
