from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.schemas import Alert, Base, CheckResult, Config, Proxy, Webhook, WebhookDelivery
from config.database import engine, get_session

# Initialize tables (ensure schemas are ready for background service)
Base.metadata.create_all(bind=engine)

TARGET_URL = 'http://httpbin.org/get'


async def probe_proxy(proxy_url: str, timeout: int | float) -> tuple[str, float]:
    started_at = time.perf_counter()
    status = 'down'

    try:
        async with httpx.AsyncClient(proxy=proxy_url, timeout=timeout, follow_redirects=True) as client:
            response = await client.get(TARGET_URL)
            if 200 <= response.status_code < 300:
                status = 'up'
    except httpx.HTTPError:
        status = 'down'
    finally:
        latency = time.perf_counter() - started_at

    return status, latency


def _get_current_config(session: Session) -> Config:
    statement = select(Config).order_by(Config.id.asc())
    config = session.execute(statement).scalars().first()
    if config is None:
        config = Config(id=1, interval=60, timeout=5)
    return config


def _get_all_proxies(session: Session) -> list[Proxy]:
    statement = select(Proxy).order_by(Proxy.id.asc())
    return list(session.execute(statement).scalars().all())


def _get_active_alerts(session: Session) -> list[Alert]:
    statement = select(Alert).where(
        Alert.status == 'active').order_by(Alert.fired_at.desc())
    return list(session.execute(statement).scalars().all())


def _load_monitoring_state(session: Session) -> tuple[int, list[dict[str, str]]]:
    config = _get_current_config(session)
    proxies = _get_all_proxies(session)

    proxy_snapshots = [
        {
            'id': proxy.id,
            'url': proxy.url,
        }
        for proxy in proxies
    ]

    return config.timeout, proxy_snapshots


def _format_integration_payload(integration_type: str, username: str, alert_data: dict) -> dict:
    event = alert_data.get('event')
    is_fired = event == 'alert.fired'

    if integration_type == 'slack':
        text = f"Alert {alert_data.get('alert_id')}: {'FIRED' if is_fired else 'RESOLVED'}"

        fields = [
            {'title': 'Alert ID', 'value': alert_data.get('alert_id', 'N/A')},
            {'title': 'Failure Rate', 'value': str(
                alert_data.get('failure_rate', 'N/A'))},
            {'title': 'Failed Proxies', 'value': str(
                alert_data.get('failed_proxies', 'N/A'))},
            {'title': 'Threshold', 'value': str(
                alert_data.get('threshold', 'N/A'))},
            {'title': 'Failed IDs', 'value': ', '.join(
                alert_data.get('failed_proxy_ids', []))},
            {'title': 'Fired At', 'value': alert_data.get('fired_at', 'N/A')},
        ]

        return {
            'username': username,
            'text': text,
            'attachments': [
                {
                    'color': '#FF0000' if is_fired else '#00FF00',
                    'fields': fields,
                    'footer': 'ProxyMaze Alert',
                    'ts': int(time.time()),
                }
            ],
        }

    elif integration_type == 'discord':
        title = f"Alert {'FIRED' if is_fired else 'RESOLVED'}"
        description = f"Alert {alert_data.get('alert_id')}"

        fields = [
            {'name': 'Alert ID', 'value': alert_data.get('alert_id', 'N/A')},
            {'name': 'Failure Rate', 'value': str(
                alert_data.get('failure_rate', 'N/A'))},
            {'name': 'Failed Proxies', 'value': str(
                alert_data.get('failed_proxies', 'N/A'))},
            {'name': 'Threshold', 'value': str(
                alert_data.get('threshold', 'N/A'))},
            {'name': 'Failed IDs', 'value': ', '.join(
                alert_data.get('failed_proxy_ids', []))},
        ]

        return {
            'username': username,
            'embeds': [
                {
                    'title': title,
                    'description': description,
                    'color': 16711680 if is_fired else 65280,
                    'fields': fields,
                    'footer': {'text': 'ProxyMaze Alert'},
                }
            ],
        }

    else:  # generic
        return alert_data


async def notify_webhooks(alert_data: dict[str, Any]) -> dict[str, int]:
    event_type = alert_data.get('event')

    with get_session() as session:
        webhooks = list(
            session.execute(select(Webhook.id, Webhook.url, Webhook.integration_type, Webhook.username, Webhook.events).order_by(
                Webhook.id.asc())).all()
        )

    # Filter webhooks where event_type is in the events list
    webhooks = [w for w in webhooks if event_type in w.events]

    if not webhooks:
        return {'total': 0, 'sent': 0, 'failed': 0}

    async def post_webhook(webhook_id: int, url: str, integration_type: str, username: str) -> bool:
        success = False
        # Format the payload based on integration type
        formatted_payload = _format_integration_payload(
            integration_type, username or '', alert_data)

        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                    response = await client.post(url, json=formatted_payload)
                if response.status_code < 500:
                    success = True
            except httpx.HTTPError:
                success = False

            # Log this delivery attempt
            with get_session() as session:
                delivery = WebhookDelivery(
                    webhook_id=webhook_id,
                    success=success,
                    timestamp=datetime.now(timezone.utc),
                )
                session.add(delivery)

            if success:
                break

            if attempt < 2:
                await asyncio.sleep(5)

        return success

    results = await asyncio.gather(
        *(post_webhook(webhook_id, url, integration_type, username) for webhook_id, url, integration_type, username, events in webhooks)
    )
    sent = sum(1 for result in results if result)
    failed = len(webhooks) - sent

    return {'total': len(webhooks), 'sent': sent, 'failed': failed}


async def _run_monitoring_round_async() -> dict[str, Any]:
    now = datetime.now(timezone.utc)

    with get_session() as session:
        timeout, proxies = _load_monitoring_state(session)

    failed_proxies = 0
    results: list[dict[str, Any]] = []
    probes: list[tuple[str, float]] = []

    failed_ids: list[str] = []

    if proxies:
        probes = await asyncio.gather(
            *(probe_proxy(proxy['url'], timeout) for proxy in proxies)
        )

        with get_session() as session:
            for proxy, (status, latency) in zip(proxies, probes):
                if status == 'down':
                    failed_proxies += 1
                    failed_ids.append(proxy['id'])

                check_result = CheckResult(
                    proxy_id=proxy['id'],
                    status=status,
                    latency=latency,
                    timestamp=now,
                )
                session.add(check_result)
                results.append({
                    'proxy_id': proxy['id'],
                    'status': status,
                    'latency': latency,
                    'timestamp': check_result.timestamp.isoformat(),
                })

                # Update Proxy record with status and last_checked_at
                proxy_obj = session.get(Proxy, proxy['id'])
                if proxy_obj is not None:
                    proxy_obj.status = status
                    proxy_obj.last_checked_at = now
                    if status == 'down':
                        proxy_obj.consecutive_failures += 1
                    else:
                        proxy_obj.consecutive_failures = 0

    if proxies:
        failure_rate = failed_proxies / len(proxies)
    else:
        failure_rate = 0.0
    pending_alert_notifications: list[dict[str, Any]] = []

    with get_session() as session:
        active_alerts = _get_active_alerts(session)
        active_alert = active_alerts[0] if active_alerts else None

        for stale_alert in active_alerts[1:]:
            stale_alert.status = 'resolved'
            stale_alert.resolved_at = now

        if failure_rate >= 0.20:
            if active_alert is None:
                alert = Alert(
                    alert_id=now.strftime('%Y%m%d%H%M%S%f'),
                    status='active',
                    failure_rate=failure_rate,
                    fired_at=now,
                    resolved_at=None,
                    total_proxies=len(proxies),
                    failed_proxies=failed_proxies,
                    failed_proxy_ids=failed_ids,
                    threshold=0.2,
                    message='Proxy pool failure rate exceeded threshold',
                )
                session.add(alert)
                pending_alert_notifications.append({
                    'event': 'alert.fired',
                    'alert_id': alert.alert_id,
                    'fired_at': alert.fired_at.isoformat(),
                    'failure_rate': failure_rate,
                    'total_proxies': len(proxies),
                    'failed_proxies': failed_proxies,
                    'failed_proxy_ids': failed_ids,
                    'threshold': 0.2,
                    'message': alert.message,
                })
        elif active_alert is not None:
            active_alert.status = 'resolved'
            active_alert.resolved_at = now
            pending_alert_notifications.append({
                'event': 'alert.resolved',
                'alert_id': active_alert.alert_id,
                'resolved_at': now.isoformat(),
            })

    if pending_alert_notifications:
        await asyncio.gather(
            *(notify_webhooks(alert_data) for alert_data in pending_alert_notifications)
        )

    return {
        'total_proxies': len(proxies),
        'failed_proxies': failed_proxies,
        'failure_rate': failure_rate,
        'results': results,
    }


def run_monitoring_round() -> dict[str, Any]:
    return asyncio.run(_run_monitoring_round_async())


def _monitor_loop(stop_event: threading.Event | None = None) -> None:
    while stop_event is None or not stop_event.is_set():
        with get_session() as session:
            interval, _ = _load_monitoring_state(session)

        run_monitoring_round()

        if stop_event is not None and stop_event.wait(interval):
            break
        if stop_event is None:
            time.sleep(interval)


def start_monitor(stop_event: threading.Event | None = None) -> threading.Thread:
    thread = threading.Thread(
        target=_monitor_loop,
        args=(stop_event,),
        daemon=True,
        name='proxy-maze-monitor',
    )
    thread.start()
    return thread
