# Attic

Dead code, parked here rather than deleted.

Nothing in this folder is imported, executed, or referenced by the running p0
service (gateway mounts only `Implementation/api/main.py`, `Login/main.py` and
`dataquality/app.py`). These files were the original p0 prototype scripts and a
standalone catalog/agent experiment; they import each other and nothing else
imports them.

Verified dead by: no `p0.<module>` import anywhere in the repo, no reference from
any Dockerfile / k8s manifest / shell script, and no subprocess invocation by
path (unlike `p0/connectors/` and `p0/Implementation/src/`, which ARE launched by
path and are very much alive).

Delete once the original author confirms.

## Dockerfile + requirements-worker.txt

Parked 2026-07-21. This was p0's only Dockerfile and its `CMD` ran
`worker_entrypoint.py` — a Redis Stream consumer for `p0:jobs`. Removed because
nothing produced to that stream (the only `xadd` calls were its own DLQ and
retry), no manifest or script ever built the image, and no module imported it.
The p0 API is unaffected: it ships in `deployment/application` and runs pipeline
stages itself via `subprocess.Popen`.

`worker_entrypoint.py` itself was deleted rather than parked — recover it from
git history at `c1b4293` if the async-job architecture is ever revived. Until
then the Dockerfile here does not build: its `COPY worker_entrypoint.py` refers
to a file no longer in the tree.
