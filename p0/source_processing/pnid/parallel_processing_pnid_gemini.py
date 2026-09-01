"""
Entry-point wrapper for the parallel P&ID extraction pipeline.

Usage:
    python parallel_processing_pnid_gemini.py --input /path/to/pnid_folder --workers 10
    python parallel_processing_pnid_gemini.py --input /path/to/pnid_folder --output /path/to/out --workers 5
"""

import argparse
import os
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from complete_pnid_extraction_without_ui import process_folder, logger


def main():
    parser = argparse.ArgumentParser(
        description="Parallel P&ID Extraction — processes all PDFs in a folder using Gemini Vision"
    )
    parser.add_argument(
        "--input", required=True, help="Folder containing P&ID PDF files"
    )
    parser.add_argument(
        "--output", help="Output folder for results (default: <input>/output)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Max parallel Gemini API calls per PDF page (default: 10)",
    )
    args = parser.parse_args()

    if not args.output:
        args.output = os.path.join(args.input, "output")

    Path(args.output).mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Parallel P&ID Extraction")
    logger.info(f"  Input   : {args.input}")
    logger.info(f"  Output  : {args.output}")
    logger.info(f"  Workers : {args.workers}")
    logger.info("=" * 60)

    start = time.time()
    try:
        process_folder(args.input, args.output, max_workers=args.workers)
        logger.info(f"Completed in {time.time() - start:.1f}s")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
