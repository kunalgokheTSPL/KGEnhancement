#!/usr/bin/env python3
"""
run_connector.py — Main entry point for running data source connectors.

Usage:
    python run_connector.py --data-type sap --source-type local \
        --config connectors/sap/config.yaml \
        --destination connectors/destination/config.yaml \
        --destination-type rustfs

    python run_connector.py --data-type timeseries --source-type osisoft_pi \
        --config connectors/timeseries/config.yaml \
        --destination connectors/destination/config.yaml \
        --destination-type local
"""

import argparse
import logging

from connectors.base.registry import get_connector_class


def main():
    parser = argparse.ArgumentParser(
        description="Run a CDM data source connector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--data-type",
        required=True,
        choices=["documents", "pnid", "sap", "timeseries"],
        help="Type of data to ingest",
    )
    parser.add_argument(
        "--source-type",
        required=True,
        help="Source backend (e.g. local, s3, adls, gcs, osisoft_pi, sap_hana, ...)",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the source connector YAML config",
    )
    parser.add_argument(
        "--destination",
        required=True,
        help="Path to the destination YAML config",
    )
    parser.add_argument(
        "--destination-type",
        default=None,
        help="Destination backend to use from consolidated config (e.g. local, rustfs, s3, adls, gcs)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s \u2014 %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    connector_cls = get_connector_class(args.data_type, args.source_type)
    connector = connector_cls(
        args.config,
        args.destination,
        args.source_type,
        args.destination_type,
    )
    connector.run()


if __name__ == "__main__":
    main()
