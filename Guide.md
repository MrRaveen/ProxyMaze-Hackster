# ProxyMaze — Guide

Comprehensive guide for the ProxyMaze project (ProxyMaze-Hacksters).

This document describes project architecture, models, API endpoints, background monitoring, webhook integrations, deployment options (including PythonAnywhere), testing, troubleshooting, and development notes.

---

## Table of Contents

- Project overview
- Architecture & components
- Requirements
- File layout
- Database schema (models)
- API reference (endpoints, request/response examples)
- Background monitor (behaviour & implementation)
- Alerts & webhook delivery
- Webhook integrations (Slack / Discord / Generic)
- Metrics and observability
- Running locally (development) — step-by-step
- Deploying to PythonAnywhere (detailed)
- Trigger endpoint + cron scheduling (free hosting approach)
- Troubleshooting
- Tests & verification
- Contributing
- License

---

## Project overview

ProxyMaze is a lightweight service that monitors a pool of HTTP proxy endpoints and alerts via webhooks when the pool failure rate exceeds a threshold. It was implemented as a Flask application with SQLAlchemy models saved to a local SQLite file and a background monitoring service which probes proxies using `httpx` asynchronously.

Key capabilities:

- Maintain a pool of proxies with CRUD operations
- Background health checks for each proxy (async `httpx` probes)
- Persist check results and proxy metadata to SQLite
- Fire and resolve alerts when failure rate crosses a threshold (0.20)
- Deliver alert notifications to registered webhooks (generic/Slack/Discord) with retry logic
- Track webhook delivery attempts for observability
- Provide metrics and per-proxy history endpoints

This guide documents how everything works and how to run and deploy the system.

---

## Architecture & components

- Flask API: exposes endpoints for configuration, proxies, webhooks, integrations, alerts, metrics.
- SQLAlchemy models: declarative models stored in `proxymaze.db`.
- Background monitor: a Python thread (daemon) that runs monitoring rounds on a configurable interval. Each round:
  - Loads config and proxies
  - Probes proxies concurrently using `httpx.AsyncClient` and `asyncio.gather`
  - Persists `CheckResult` rows and updates `Proxy` status
  - Computes failure rate and fires/resolves alerts
  - Notifies webhooks (filtered by event subscription)
- Webhook delivery: asynchronous POST requests with 3 retries on server errors; every attempt is recorded in `WebhookDelivery` table.

---

## Requirements

- Python 3.11+ (project uses 3.13 in dev, but 3.11+ is safe for deployment platforms)
- Flask >= 3.0.0
- SQLAlchemy >= 2.0.0
- httpx >= 0.27.0
- Optional: virtualenv or pyenv for an isolated environment

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate   # or .venv\\Scripts\\activate on Windows
pip install -r requirements.txt
```

---

## File layout

Top-level project tree (important files):

- `run.py` — application entrypoint used in local runs and for WSGI servers
- `requirements.txt` — Python dependencies
- `proxymaze.db` — SQLite database file (created on first run)
- `app/__init__.py` — creates and registers Flask app
- `app/api/routes.py` — Flask blueprint with all REST endpoints
- `app/models/schemas.py` — SQLAlchemy models and helper mixins
- `app/services/monitor.py` — background monitor, probing, alerts, webhooks
- `tests/` — (if present) test harness
- `Guide.md` — (this document)

Refer to these files when following examples in the rest of the document.

---

## Database schema (models)

All models are defined in `app/models/schemas.py`. Summarized fields:

- `Config` (table `config`)
  - `id` (int, PK)
  - `interval` (int) — seconds between monitoring rounds (default 60)
  - `timeout` (int) — request timeout for probes in seconds (default 5)

- `Proxy` (table `proxy`)
  - `id` (str, PK) — proxy identifier
  - `url` (str) — full proxy URL used by httpx (e.g. `http://user:pass@host:port`)
  - `added_at` (datetime, timezone-aware)
  - `status` (str) — `pending` | `up` | `down`
  - `last_checked_at` (datetime)
  - `consecutive_failures` (int)
  - relationship: `check_results` -> `CheckResult`

- `CheckResult` (table `check_result`)
  - `id` (int, PK)
  - `proxy_id` (str, FK -> proxy.id)
  - `status` (str) — `up` or `down`
  - `latency` (float) — measured in seconds
  - `timestamp` (datetime)

- `Alert` (table `alert`)
  - `alert_id` (str, PK) — timestamp-based unique ID
  - `status` (str) — `active` or `resolved`
  - `failure_rate` (float)
  - `fired_at` (datetime)
  - `resolved_at` (datetime | null)
  - `total_proxies` (int)
  - `failed_proxies` (int)
  - `failed_proxy_ids` (JSON list[str])
  - `threshold` (float) — default 0.2
  - `message` (str)

- `Webhook` (table `webhook`)
  - `id` (int, PK)
  - `url` (str)
  - `integration_type` (str) — `generic` | `slack` | `discord`
  - `username` (str | null)
  - `events` (JSON list[str]) — list of events to receive, e.g. `['alert.fired', 'alert.resolved']`

- `WebhookDelivery` (table `webhook_delivery`)
  - `id` (int, PK)
  - `webhook_id` (int, FK -> webhook.id)
  - `success` (bool)
  - `timestamp` (datetime)

All timestamps use `datetime.now(timezone.utc)` and are serialized as ISO 8601 strings in API responses.

---

## API reference

All endpoints are implemented in the `api_v1` blueprint in `app/api/routes.py`. Summary with examples follows.

Base host: `http://<host>` (e.g., `http://127.0.0.1:5000` or your production domain)

### GET /health

Description: quick service health check.

Response (200):

```json
{ "status": "active", "timestamp": "2026-05-01T12:00:00+00:00" }
```

### POST /config

Description: update interval and timeout.

Request JSON (required fields):

```json
{ "interval": 60, "timeout": 5 }
```

Response (200): returns saved config.

### GET /config

Returns current config or defaults if none set.

### POST /proxies

Description: add proxies to the pool. Payload expects `proxies` as a list of objects (each must include `id` and `url`). Optional `replace` flag clears existing proxies.

Request example:

```json
{
  "proxies": [
    { "id": "px-001", "url": "http://user:pass@1.2.3.4:8080" },
    { "id": "px-002", "url": "http://1.2.3.5:8080" }
  ],
  "replace": true
}
```

Response (201): `{ "created": N, "proxies": [ ... serialized proxies ... ] }`

### GET /proxies

Returns pool summary:

```json
{
  "total": 2,
  "up": 1,
  "down": 1,
  "failure_rate": 0.5,
  "proxies": [ { ... } ]
}
```

### DELETE /proxies

Clears the proxy pool. Response: HTTP 204 No Content.

### GET /proxies/{id}

Returns proxy details and recent history (last 10 checks). Example fields: `total_checks`, `uptime_percentage`, `history`.

### GET /proxies/{id}/history

Returns ordered list of checks for the proxy.

### POST /webhooks

Register a webhook. Accepts `url` (or `webhook_url`), optional `integration_type`, `username`, `events`.

Request example:

```json
{
  "url": "https://hooks.example.com/abc",
  "integration_type": "slack",
  "username": "ProxyBot",
  "events": ["alert.fired"]
}
```

Response (201): returns webhook record.

### POST /integrations

Helper endpoint to create Slack/Discord integrations with validation.

### GET /alerts

List of all alerts (active + resolved) sorted by `fired_at` desc. Each alert includes `failed_proxy_ids` array.

### GET /metrics

Returns `total_checks`, `current_pool_size`, `active_alerts`, `total_alerts`, `webhook_deliveries`.

### Optional: POST /trigger-monitor (recommended for free hosting)

This endpoint can be added to trigger a single monitoring round synchronously. It calls `run_monitoring_round()` and returns the results. Use an external ping/cron service to POST this endpoint at your desired cadence (e.g., every 60 seconds).

Implementation example (add to `app/api/routes.py`):

```python
@api_bp.route('/trigger-monitor', methods=['POST'])
def trigger_monitor():
    from app.services.monitor import run_monitoring_round
    try:
        result = run_monitoring_round()
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

```

---

## Background monitor — behaviour & implementation

Location: `app/services/monitor.py`

Key behaviours:

- The monitor runs rounds at the configured interval (`Config.interval`). Each round:
  - Loads current `Config` and `Proxy` list into memory (snapshots of `id` and `url` to avoid using detached ORM objects in async tasks).
  - Asynchronously probes proxies using `httpx.AsyncClient(proxy=proxy_url, timeout=timeout)` hitting a fixed `TARGET_URL` (currently `http://httpbin.org/get`).
  - If a probe returns an HTTP 2xx status, the proxy is `up`; otherwise `down`. Exceptions/timeouts count as `down`.
  - Persists a `CheckResult` row for each probe with `status`, `latency`, and `timestamp`.
  - Updates `Proxy` row: `status`, `last_checked_at`, and increments or resets `consecutive_failures`.
  - Computes failure rate `failed_proxies / total_proxies`.
  - Alert logic:
    - If `failure_rate >= threshold` (0.20) and there is no active alert: create a new `Alert` with `status='active'` and include failed proxy IDs.
    - If `failure_rate < threshold` and there is an active alert: set `status='resolved'` and record `resolved_at`.
  - Normalizes duplicate active alerts: any extra active alerts are marked resolved to guarantee at most one active alert.
  - Creates notification payload(s) (event `alert.fired` or `alert.resolved`) and calls `notify_webhooks`.

`notify_webhooks(alert_data)` behaviour:

- Accepts an `alert_data` dict containing `event` (e.g. `alert.fired`) and fields describing the alert.
- Queries `Webhook` records and selects `{id, url, integration_type, username, events}`.
- Filters webhooks in Python to those where `event` is included in their `events` list.
- Formats payload per `integration_type` via `_format_integration_payload()` (see details below).
- Posts payloads with `httpx.AsyncClient.post(json=formatted_payload)`.
- Retries up to 3 attempts on server-side errors (5xx) with 5-second delays; records each attempt to `WebhookDelivery` (success flag and timestamp).

Important implementation notes:

- ORM objects are only used within DB sessions; async probe tasks receive plain dict snapshots to avoid DetachedInstanceError.
- All DB writes (CheckResult, Alert, WebhookDelivery) occur within sessions created by `get_session()` context manager.

---

## Alerts & webhook delivery details

Alert format examples created by monitor (internal payloads passed to `notify_webhooks`):

- `alert.fired` example:

```json
{
  "event": "alert.fired",
  "alert_id": "20260509120000000000",
  "fired_at": "2026-05-09T12:00:00+00:00",
  "failure_rate": 0.25,
  "total_proxies": 10,
  "failed_proxies": 3,
  "failed_proxy_ids": ["px-3", "px-7", "px-9"],
  "threshold": 0.2,
  "message": "Proxy pool failure rate exceeded threshold"
}
```

- `alert.resolved` example:

```json
{
  "event": "alert.resolved",
  "alert_id": "20260509120000000000",
  "resolved_at": "2026-05-09T12:05:00+00:00"
}
```

Webhook delivery specifics:

- Webhooks register which events they want (JSON `events` list). Only matching events receive notifications.
- Payload formatting per integration type is done via `_format_integration_payload(integration_type, username, alert_data)`:
  - Slack: `{username, text, attachments}` with color `#FF0000` (fired) / `#00FF00` (resolved), fields with `Alert ID`, `Failure Rate`, `Failed Proxies`, `Threshold`, `Failed IDs`, `Fired At`, `footer`, `ts`.
  - Discord: `{username, embeds}` with `title`, `description`, `color` (integers), `fields`, `footer`.
  - Generic: raw `alert_data` JSON is posted as-is.
- Retries: 3 attempts, 5s delay between attempts on failures; success is considered when HTTP status code < 500.
- Each attempt (success or failure) results in an inserted `WebhookDelivery` record for traceability.

---

## Webhook integrations (Slack / Discord / Generic)

Integration types supported:

- `slack`: send Slack-style payloads; good for incoming webhook endpoints expecting attachments.
- `discord`: send Discord embed payloads.
- `generic`: send raw JSON; good for custom receivers.

When registering a webhook (POST `/webhooks`), you can supply `integration_type` plus `events` (e.g. `['alert.fired']`) to only receive a subset of events.

Example: register Slack webhook (curl):

```bash
curl -X POST https://<host>/webhooks \
  -H "Content-Type: application/json" \
  -d '{"url":"https://hooks.example.com/abc","integration_type":"slack","username":"ProxyBot","events":["alert.fired","alert.resolved"]}'
```

---

## Metrics and observability

GET `/metrics` returns:

- `total_checks`: total `CheckResult` rows
- `current_pool_size`: number of `Proxy` rows
- `active_alerts`: count of alerts where `status == 'active'`
- `total_alerts`: total number of alerts ever created
- `webhook_deliveries`: total webhook delivery attempts recorded

Use these numbers to monitor the system health and webhook success rate.

---

## Running locally (development)

1. Create and activate virtualenv:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

2. Initialize DB and run app (development):

```bash
python run.py
```

`run.py` prints the `url_map` and starts Flask (debug mode when `FLASK_ENV=development`). For local testing you can also call `run_monitoring_round()` manually in the Python REPL or via the `trigger-monitor` endpoint described earlier.

3. Using the background monitor locally:

The project uses a dedicated `start_monitor()` call that starts a daemon thread. When running via `python run.py` the monitor thread is started in non-debug mode to avoid running twice under the reloader.

If you run locally and want to keep monitoring in the foreground for debugging, call `run_monitoring_round()` directly.

---

## Deploying to PythonAnywhere (detailed)

PythonAnywhere is a recommended platform for simple deployments that need a long-running process and file-based storage.

High-level steps (see earlier instructions in the UI):

1. Push your project to a Git repository (GitHub, GitLab, etc.).
2. On PythonAnywhere, open a Bash console and `git clone` the repository into `/home/yourusername/ProxyMaze-Hacksters`.
3. Create a virtualenv and install dependencies:

```bash
mkvirtualenv --python=/usr/bin/python3.11 proxymaze
pip install -r requirements.txt
```

4. Create `wsgi.py` pointing to `run.app` and configure the Web app in the PythonAnywhere Web tab. Set the `virtualenv` path.

5. For monitoring, use one of these approaches:

- **Always-on console (paid accounts)**: run `python monitor_daemon.py` in a console that stays open
- **Scheduled task (paid)**: add an always-on or scheduled task to run `monitor_daemon.py`
- **Free account (recommended workaround)**: add a `trigger-monitor` endpoint and schedule an external pinger (UptimeRobot, EasyCron) to POST the endpoint every minute. This keeps monitoring rounds running without an always-on console.

6. Reload the web app and verify endpoints.

Notes about PythonAnywhere and SQLite:

- SQLite file must be located inside your home directory (cloned repo path is fine).
- Ensure the web app and any console run under the same user so they can access the same DB file.

---

## Trigger endpoint + external cron scheduling (free hosting approach)

For hosting options that don't support always-on processes (Vercel or free PythonAnywhere), use the `trigger-monitor` endpoint and schedule an external cron-like service to call it every 60 seconds.

Example using `curl` for a cron job:

```bash
curl -X POST https://<your-host>/trigger-monitor
```

Recommended free pingers:

- https://cron-job.org/
- https://uptimerobot.com/ (1-minute checks on paid tiers, longer on free)
- https://www.easycron.com/

Important: `trigger-monitor` runs synchronously, so total execution time must fit the platform's request timeout budget. The monitor performs parallel probes and may block while awaiting responses (default timeout is 5s per proxy). If your platform enforces a 10s request timeout, adjust `Config.timeout` and the pool size accordingly.

---

## Troubleshooting

Common errors and fixes:

- `ModuleNotFoundError: No module named 'app.services.monitor'`:
  - Ensure the project root is in `sys.path` and the working directory is set to the project folder before import. If running from PythonAnywhere bash, add:

    ```python
    import sys, os
    project_home = os.path.expanduser('~/ProxyMaze-Hacksters')
    if project_home not in sys.path:
        sys.path.insert(0, project_home)
    os.chdir(project_home)
    ```

- DetachedInstanceError when using ORM objects in async tasks:
  - Snapshot only primitive fields (`id`, `url`, etc.) from ORM objects before releasing the session and use those snapshots in async tasks.

- Webhook not receiving events:
  - Verify the webhook `events` list contains the event type (e.g., `alert.fired`).
  - Check `WebhookDelivery` records to see attempt timestamps and `success` values.

- 502 on PythonAnywhere after deployment:
  - Look at the error log in the Web tab for detailed tracebacks. Ensure the WSGI file and virtualenv are correctly configured.

- SQLite database permission or missing table issues:
  - Ensure the user that runs the web app and the user running the monitor have access to the same `proxymaze.db` file.

---

## Tests & verification

Manually test endpoints using `curl` or Postman. Example sequence:

1. Health check:

```bash
curl http://127.0.0.1:5000/health
```

2. Create proxies:

```bash
curl -X POST http://127.0.0.1:5000/proxies -H "Content-Type: application/json" -d '{"proxies":[{"id":"px-1","url":"http://1.2.3.4:3128"}], "replace": true}'
```

3. Trigger monitoring (locally or via `/trigger-monitor`):

```bash
curl -X POST http://127.0.0.1:5000/trigger-monitor
```

4. Check `/metrics`, `/proxies`, `/alerts` for expected state changes.

For unit tests, add test modules to `tests/` using `pytest` and mock `httpx.AsyncClient` to simulate up/down responses. Tests should assert that:

- `CheckResult` rows are created
- `Proxy` statuses are updated correctly
- Alerts are created and resolved as expected
- Webhook deliveries are recorded with expected success/failure counts

---

## Contributing

If you want to extend this project, recommended next steps:

- Add configuration to change `TARGET_URL` (current probe target is `http://httpbin.org/get`).
- Add authentication / API keys for webhook endpoints and admin actions.
- Replace SQLite with a managed DB (Postgres) for production reliability.
- Add structured logging and metrics exporters (Prometheus) for better observability.

When contributing:

- Fork the repo and create a branch for changes.
- Add tests under `tests/` and run locally using `pytest`.
- Create PRs against `main` and include a summary of changes.

---

## License

Project license: add your chosen license file (e.g., `LICENSE`) to the repo. If none present, assume private.

---

If you'd like, I can:

- Add the `trigger-monitor` endpoint to `app/api/routes.py` and open a PR.
- Add a small `README` with quick start commands.
- Create a `docker-compose.yml` for easier local testing.

The `Guide.md` file was created in the repository root.
