# ─────────────────────────────────────────────────────────────────────────────
# p0 CDM Pipeline — Production Container Image
#
# Runs as UID 1000 (non-root), matching the platform security baseline.
# Supports two modes:
#   Worker mode (default): Redis Stream consumer → reads jobs → runs CDM pipeline
#   CLI mode:              direct invocation of any pipeline script
#
# Build (from p0/ directory):
#   podman build -t localhost:5000/decisionops/cdm-worker:v0.1.0 .
#
# Push to cluster registry:
#   podman push localhost:5000/decisionops/cdm-worker:v0.1.0
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS base

# System dependencies for PyMuPDF (PDF page rendering for P&ID extraction)
# libmupdf-dev: MuPDF library used by pymupdf (fitz)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libmupdf-dev \
        mupdf-tools \
        poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Python dependencies ───────────────────────────────────────────────────────
# Install pipeline deps first (better layer caching — change less often)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install worker-mode deps (Redis consumer + S3 client for RustFS)
COPY requirements-worker.txt ./
RUN pip install --no-cache-dir -r requirements-worker.txt

# ── Application source ────────────────────────────────────────────────────────
COPY config/   ./config/
COPY pipelines/          ./pipelines/
COPY source_processing/  ./source_processing/
COPY utils/              ./utils/
COPY worker_entrypoint.py ./

# ── Runtime user setup ────────────────────────────────────────────────────────
# UID 1000 matches the platform-wide security baseline (runAsUser: 1000).
# /data is the PVC mount point for staging input and CDM output.
# /tmp is emptyDir in K8s.
RUN useradd -r -u 1000 -g root -s /bin/false p0user \
    && mkdir -p /data/staging /data/work /data/out /tmp \
    && chown -R 1000:root /app /data

USER 1000

# ── Default: Redis Stream worker mode ────────────────────────────────────────
# To run a single pipeline stage directly:
#   docker run ... python -m pipelines.run_sap_end_to_end --help
CMD ["python", "-u", "worker_entrypoint.py"]
