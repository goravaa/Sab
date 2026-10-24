# Dataset Engine API

The HTTP layer lives in `api/`; `main/` stays the framework independent engine
library. The API is split into `routes/`, `services/`, and `schemas.py`.
Install its dependencies and launch it from `dataset-engine/`:

```sh
python -m pip install -r api/requirements.txt
uvicorn api.app:app --host 0.0.0.0 --port 8000
```

The service exposes `GET /health`, `POST /v1/sources/check`,
`POST /v1/datasets/reconcile`, `POST /v1/jobs`,
`POST /v1/jobs/stream`, and `POST /v1/jobs/{job_id}/status`. Reconcile and job
requests include the dataset's source definitions and BYOB S3 target. Job
status is written to that target alongside the manifest and batch receipts.
Credentials are accepted in request bodies and are never included in job
responses or URLs.

The streaming endpoint responds with `text/event-stream`. It sends a `job`
event when the job is accepted, a `progress` event after each batch receipt is
written, then a `completed` or `failed` event. Since job submission includes
credentials in the POST body, browser clients should read the response stream
with `fetch()` rather than native `EventSource` (which only supports GET).
Each progress event contains `progress.processed_items`,
`progress.total_items`, `progress.completed_batches`, and `progress.ratio`.
