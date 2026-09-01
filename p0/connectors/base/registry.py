"""Connector registry — discovers and runs connectors by data-type + source-type."""

from connectors.documents.connector import (
    LocalDocumentConnector,
    S3DocumentConnector,
    ADLSDocumentConnector,
    GCSDocumentConnector,
    OneDriveDocumentConnector,
)
from connectors.pnid.connector import (
    LocalPnidConnector,
    S3PnidConnector,
    ADLSPnidConnector,
    GCSPnidConnector,
    OneDrivePnidConnector,
)
from connectors.sap.connector import (
    LocalSapConnector,
    S3SapConnector,
    ADLSSapConnector,
    GCSSapConnector,
    OneDriveSapConnector,
    SapBW4HANAConnector,
    SapERP6Connector,
    SapHANAConnector,
    SapS4HANACloudConnector,
    SapS4HANAOnPremConnector,
    MaximoConnector,
)
from connectors.timeseries.connector import (
    LocalTimeseriesConnector,
    S3TimeseriesConnector,
    ADLSTimeseriesConnector,
    GCSTimeseriesConnector,
    OneDriveTimeseriesConnector,
    OsisoftPIConnector,
    WonderwareConnector,
    HoneywellPHDConnector,
    YokogawaExaquantumConnector,
    EmersonDeltaVConnector,
    AVEVAHistorianConnector,
)

REGISTRY = {
    ("documents", "local"): LocalDocumentConnector,
    ("documents", "s3"): S3DocumentConnector,
    ("documents", "adls"): ADLSDocumentConnector,
    ("documents", "gcs"): GCSDocumentConnector,
    ("documents", "onedrive"): OneDriveDocumentConnector,
    ("pnid", "local"): LocalPnidConnector,
    ("pnid", "s3"): S3PnidConnector,
    ("pnid", "adls"): ADLSPnidConnector,
    ("pnid", "gcs"): GCSPnidConnector,
    ("pnid", "onedrive"): OneDrivePnidConnector,
    ("sap", "local"): LocalSapConnector,
    ("sap", "s3"): S3SapConnector,
    ("sap", "adls"): ADLSSapConnector,
    ("sap", "gcs"): GCSSapConnector,
    ("sap", "onedrive"): OneDriveSapConnector,
    ("sap", "sap_bw4hana"): SapBW4HANAConnector,
    ("sap", "sap_erp6"): SapERP6Connector,
    ("sap", "sap_hana"): SapHANAConnector,
    ("sap", "sap_s4hana_cloud"): SapS4HANACloudConnector,
    ("sap", "sap_s4hana_onprem"): SapS4HANAOnPremConnector,
    ("sap", "maximo"): MaximoConnector,
    ("timeseries", "local"): LocalTimeseriesConnector,
    ("timeseries", "s3"): S3TimeseriesConnector,
    ("timeseries", "adls"): ADLSTimeseriesConnector,
    ("timeseries", "gcs"): GCSTimeseriesConnector,
    ("timeseries", "onedrive"): OneDriveTimeseriesConnector,
    ("timeseries", "osisoft_pi"): OsisoftPIConnector,
    ("timeseries", "wonderware"): WonderwareConnector,
    ("timeseries", "honeywell_phd"): HoneywellPHDConnector,
    ("timeseries", "yokogawa_exaquantum"): YokogawaExaquantumConnector,
    ("timeseries", "emerson_deltav"): EmersonDeltaVConnector,
    ("timeseries", "aveva_historian"): AVEVAHistorianConnector,
}


def get_connector_class(data_type: str, source_type: str):
    key = (data_type, source_type)
    if key not in REGISTRY:
        raise ValueError(
            f"No connector registered for ({data_type}, {source_type}). "
            f"Available: {sorted(REGISTRY.keys())}"
        )
    return REGISTRY[key]
