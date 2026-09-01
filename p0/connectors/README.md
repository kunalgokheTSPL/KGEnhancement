# CDM Data Source Connectors

Config-driven connectors that pull data from various sources and stage it into a unified folder structure.

## Architecture

```
connectors/
├── run_connector.py              # CLI entry point
├── requirements.txt
├── base/
│   ├── __init__.py               # BaseConnector class + config loader
│   └── registry.py               # Connector class registry
├── destination/
│   ├── __init__.py               # DestinationWriter (decoupled from connectors)
│   └── config.yaml              # All destination backend configs
├── documents/                    # Document connectors
│   ├── connector.py              # Local, S3, ADLS, GCS, OneDrive
│   └── config.yaml              # All document source configs
├── pnid/                         # P&ID diagram connectors
│   ├── connector.py              # Local, S3, ADLS, GCS, OneDrive
│   └── config.yaml              # All P&ID source configs
├── sap/                          # SAP / EAM connectors
│   ├── connector.py              # Local, S3, ADLS, GCS, OneDrive,
│   │                             # SAP ERP6, S/4HANA, BW/4HANA,
│   │                             # HANA, S/4HANA Cloud, IBM Maximo
│   └── config.yaml              # All SAP/EAM source configs
└── timeseries/                   # Timeseries connectors
    ├── connector.py              # Local, S3, ADLS, GCS, OneDrive,
    │                             # OSIsoft PI, Wonderware, PHD,
    │                             # Exaquantum, DeltaV, AVEVA
    └── config.yaml              # All timeseries source configs
```

## Config Structure

Each data type has a **single `config.yaml`** with a `common` section (shared settings) and a `sources` section (per-source connection details):

```yaml
# Example: sap/config.yaml
common:
  file_format: auto
  encoding: utf-8
  filters:
    max_file_size_mb: 500

sources:
  local:
    base_path: "./data/staging/sap"
    tables: [EQUI, AUFK, ...]

  s3:
    bucket: "${S3_SAP_BUCKET}"
    connection:
      access_key: "${AWS_ACCESS_KEY_ID}"
      secret_key: "${AWS_SECRET_ACCESS_KEY}"

  maximo:
    connection:
      base_url: "${MAXIMO_BASE_URL}"
      auth_type: apikey
      api_key: "${MAXIMO_API_KEY}"
    object_structures:
      - name: MXASSET
      - name: MXWO
```

The `common` settings are deep-merged with the chosen source's settings at runtime.

## Supported Sources

| Data Type   | Sources                                                                              |
|-------------|--------------------------------------------------------------------------------------|
| documents   | local, s3, adls, gcs, **onedrive**                                                   |
| pnid        | local, s3, adls, gcs, **onedrive**                                                   |
| sap         | local, s3, adls, gcs, **onedrive**, sap_erp6, sap_s4hana_onprem, sap_bw4hana, sap_hana, sap_s4hana_cloud, **maximo** |
| timeseries  | local, s3, adls, gcs, **onedrive**, osisoft_pi, wonderware, honeywell_phd, yokogawa_exaquantum, emerson_deltav, aveva_historian |

## Usage

```bash
# Local SAP export → RustFS staging
python -m connectors.run_connector \
  --data-type sap \
  --source-type local \
  --config connectors/sap/config.yaml \
  --destination connectors/destination/config.yaml \
  --destination-type rustfs

# PI Historian → RustFS staging
python -m connectors.run_connector \
  --data-type timeseries \
  --source-type osisoft_pi \
  --config connectors/timeseries/config.yaml \
  --destination connectors/destination/config.yaml \
  --destination-type rustfs

# OneDrive documents → local staging
python -m connectors.run_connector \
  --data-type documents \
  --source-type onedrive \
  --config connectors/documents/config.yaml \
  --destination connectors/destination/config.yaml \
  --destination-type local

# IBM Maximo → RustFS staging
python -m connectors.run_connector \
  --data-type sap \
  --source-type maximo \
  --config connectors/sap/config.yaml \
  --destination connectors/destination/config.yaml \
  --destination-type rustfs
```

## Staging Output Structure

All connectors write to the same destination layout (configured in `destination/config.yaml`):

```
staging/
├── documents/
│   ├── maintenance/          # PDFs + manifest.parquet
│   ├── sop/
│   ├── oem/
│   ├── rca/
│   └── inspection/
├── pnid/                     # P&ID PDFs + manifest.parquet
├── sap/
│   ├── EQUI/                 # EQUI.parquet
│   ├── AUFK/                 # AUFK.parquet
│   ├── IFLOT/
│   └── ...
└── timeseries/               # timeseries_data.parquet + metadata.parquet
```

## Design Principles

1. **Config-driven**: All source-specific connection fields are in YAML — no hardcoded credentials or paths
2. **Single config per data type**: One YAML file with `common` + `sources` sections, merged at runtime
3. **Destination decoupled**: Connectors don't know where data lands. Swap destination configs to redirect output
4. **Env-var substitution**: Use `${VAR:-default}` in YAML for secrets management
5. **Uniform lifecycle**: Every connector follows `validate → connect → extract → write`
6. **Registry pattern**: `(data_type, source_type)` tuple maps to a connector class

## Adding a New Connector

1. Add source-specific settings under `sources.<source_type>` in `<data_type>/config.yaml`
2. Add a connector class in `<data_type>/connector.py` extending `BaseConnector`
3. Register it in `base/registry.py`
