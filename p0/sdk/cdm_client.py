"""
DecisionOps CDM — Python client SDK.

A thin, dependency-light wrapper over the CDM Admin REST API (p0) so the platform
can be driven programmatically, not just through the UI. Wraps the EXISTING
endpoints — it adds no server behaviour.

    pip install requests   # the only dependency

    from cdm_client import CdmClient
    cdm = CdmClient("http://localhost:4400/p0_cdm", token="<jwt>")

    cdm.health()
    cdm.plants()                                   # list plants
    cdm.upload_timeseries("HYDRO", ["tags.csv"])   # upload metadata
    job = cdm.run_pipeline("ts", "HYDRO")          # process
    cdm.wait_for_job(job["pipeline_job_id"])                # poll to completion
    rows = cdm.get_ts_context("HYDRO")["rows"]     # review
    cdm.commit_ts("HYDRO", rows)                   # commit
    cdm.lineage("HYDRO", flow="ts")                # run-event lineage
    cdm.activity("HYDRO")                          # who-did-what feed

Auth: pass a Keycloak bearer ``token`` (or set ``CDM_TOKEN`` env). Every call
except health requires it. All data calls are scoped by ``plant_code_id``.
"""

from __future__ import annotations

import os
import time
from typing import Any

import requests


class CdmError(RuntimeError):
    """Raised on a non-2xx response. Carries status + body for debugging."""

    def __init__(self, status: int, body: Any, url: str):
        self.status, self.body, self.url = status, body, url
        super().__init__(f"CDM {status} on {url}: {str(body)[:300]}")


class CdmClient:
    """Client for the DecisionOps CDM Admin API."""

    def __init__(self, base_url: str, token: str | None = None, *, timeout: int = 60):
        self.base = base_url.rstrip("/")
        self.token = token or os.environ.get("CDM_TOKEN")
        self.timeout = timeout
        self._s = requests.Session()

    def _headers(self) -> dict:
        h = {"Accept": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def _req(self, method: str, path: str, **kw) -> Any:
        url = f"{self.base}/api{path}"
        r = self._s.request(
            method, url, headers=self._headers(), timeout=self.timeout, **kw
        )
        if not r.ok:
            try:
                body = r.json()
            except Exception:
                body = r.text
            raise CdmError(r.status_code, body, url)
        if r.headers.get("content-type", "").startswith("application/json"):
            return r.json()
        return r.content

    def health(self) -> Any:
        return self._req("GET", "/health")

    def plants(self) -> Any:
        return self._req("GET", "/config/plants")

    def create_plant(
        self, plant_code_id: str, label: str | None = None, industry: str | None = None
    ) -> Any:
        return self._req(
            "POST",
            "/config/plants",
            json={"plant_code_id": plant_code_id, "label": label, "industry": industry},
        )

    def delete_plant(self, plant_code_id: str, *, cascade: bool = False) -> Any:
        params = {"cascade": "true", "confirm": plant_code_id} if cascade else {}
        return self._req("DELETE", f"/config/plants/{plant_code_id}", params=params)

    def upload_timeseries(
        self, plant_code_id: str, files: list[str], file_type: str = "metadata"
    ) -> Any:
        """Upload TS metadata or values files. file_type: 'metadata' | 'values'."""
        return self._upload(
            "/connectors/timeseries/upload",
            plant_code_id,
            files,
            extra={"file_type": file_type},
        )

    def upload_documents(
        self, plant_code_id: str, files: list[str], doc_type: str = "sop"
    ) -> Any:
        return self._upload(
            "/connectors/documents/upload",
            plant_code_id,
            files,
            extra={"document_type": doc_type},
        )

    def _upload(self, path: str, plant_code_id: str, files: list[str], extra: dict) -> Any:
        opened = [("files", (os.path.basename(f), open(f, "rb"))) for f in files]
        try:
            data = {"plant_code_id": plant_code_id, **extra}
            url = f"{self.base}/api{path}"
            r = self._s.post(
                url,
                headers={k: v for k, v in self._headers().items() if k != "Accept"},
                data=data,
                files=opened,
                timeout=self.timeout,
            )
            if not r.ok:
                try:
                    body = r.json()
                except Exception:
                    body = r.text
                raise CdmError(r.status_code, body, url)
            return r.json()
        finally:
            for _, (_, fh) in opened:
                try:
                    fh.close()
                except Exception:
                    pass

    def run_pipeline(
        self,
        stage: str,
        plant_code_id: str,
        *,
        file_names: list[str] | None = None,
        industry: str | None = None,
    ) -> Any:
        """Trigger a pipeline run. stage: ts|docs|pnid|sap|full. Returns {pipeline_job_id,…}."""
        body: dict = {"stage": stage, "plant_code_id": plant_code_id}
        if file_names:
            body["file_names"] = file_names
        if industry:
            body["industry"] = industry
        return self._req("POST", "/pipeline/run", json=body)

    def get_job(self, pipeline_job_id: str) -> Any:
        return self._req("GET", f"/pipeline/jobs/{pipeline_job_id}")

    def list_jobs(self, plant_code_id: str | None = None, stage: str | None = None) -> Any:
        return self._req(
            "GET", "/pipeline/jobs", params={"plant_code_id": plant_code_id, "stage": stage}
        )

    def wait_for_job(
        self, pipeline_job_id: str, *, poll: float = 3.0, max_wait: float = 1800
    ) -> Any:
        """Block until a job reaches completed/failed/cancelled (or timeout)."""
        deadline = time.time() + max_wait
        while time.time() < deadline:
            j = self.get_job(pipeline_job_id)
            if j.get("status") in ("completed", "failed", "cancelled"):
                return j
            time.sleep(poll)
        raise TimeoutError(f"job {pipeline_job_id} did not finish within {max_wait}s")

    def get_ts_context(
        self,
        plant_code_id: str,
        *,
        source_file: str | None = None,
        upload_batch_id: str | None = None,
    ) -> Any:
        return self._req(
            "GET",
            "/context/ts",
            params={
                "plant_code_id": plant_code_id,
                "source_file": source_file,
                "upload_batch_id": upload_batch_id,
            },
        )

    def commit_ts(
        self, plant_code_id: str, rows: list[dict], *, upload_batch_id: str | None = None
    ) -> Any:
        return self._req(
            "POST",
            "/context/ts/commit",
            json={"plant_code_id": plant_code_id, "rows": rows, "upload_batch_id": upload_batch_id},
        )

    def get_docs_context(self, plant_code_id: str, **params) -> Any:
        return self._req(
            "GET", "/context/docs", params={"plant_code_id": plant_code_id, **params}
        )

    def commit_docs(self, plant_code_id: str, rows: list[dict], **body) -> Any:
        return self._req(
            "POST",
            "/context/docs/commit",
            json={"plant_code_id": plant_code_id, "rows": rows, **body},
        )

    def flow_state(self, plant_code_id: str, flow: str | None = None) -> Any:
        return self._req(
            "GET", "/flow/state", params={"plant_code_id": plant_code_id, "flow": flow}
        )

    def activity(self, plant_code_id: str | None = None, limit: int = 100) -> Any:
        return self._req(
            "GET", "/logs/activity", params={"plant_code_id": plant_code_id, "limit": limit}
        )

    def log_events(
        self,
        *,
        level: str | None = None,
        source: str | None = None,
        q: str | None = None,
        limit: int = 500,
    ) -> Any:
        return self._req(
            "GET",
            "/logs/events",
            params={"level": level, "source": source, "q": q, "limit": limit},
        )

    def summary(self, plant_code_id: str | None = None) -> Any:
        return self._req("GET", "/logs/summary", params={"plant_code_id": plant_code_id})

    def tables(self) -> Any:
        return self._req("GET", "/data/tables")

    def equipment(self, plant_code_id: str, **params) -> Any:
        return self._req(
            "GET", "/data/equipment", params={"plant_code_id": plant_code_id, **params}
        )

    def get_equipment(self, equipment_uid: str) -> Any:
        return self._req("GET", f"/data/equipment/{equipment_uid}")

    def relationships(self, plant_code_id: str, **params) -> Any:
        return self._req(
            "GET", "/data/relationships", params={"plant_code_id": plant_code_id, **params}
        )

    def query_table(
        self, table: str, plant_code_id: str, *, limit: int = 100, offset: int = 0
    ) -> Any:
        return self._req(
            "GET",
            f"/data/query/{table}",
            params={"plant_code_id": plant_code_id, "limit": limit, "offset": offset},
        )
