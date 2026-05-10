from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

from flask import Blueprint, jsonify, request
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.models.schemas import Alert, Base, CheckResult, Config, Proxy, Webhook, WebhookDelivery
from config.database import engine, get_session
from config.redis_client import get_redis_client
import json

api_bp = Blueprint('api_v1', __name__)

# Initialize tables
Base.metadata.create_all(bind=engine)


def serialize_config(config: Config) -> dict[str, int]:
    return {
        'check_interval_seconds': config.check_interval_seconds,
        'request_timeout_ms': config.request_timeout_ms,
    }


def default_config() -> dict[str, int]:
    return {
        'check_interval_seconds': 60,
        'request_timeout_ms': 5000,
    }


def get_existing_config(session: Session) -> Config | None:
    statement = select(Config).order_by(Config.id.asc())
    return session.execute(statement).scalars().first()


@api_bp.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'ok'
    }), 200


@api_bp.route('/config', methods=['POST'])
def update_config():
    payload = request.get_json(silent=True) or {}
    if 'check_interval_seconds' not in payload or 'request_timeout_ms' not in payload:
        return jsonify({'error': 'check_interval_seconds and request_timeout_ms are required'}), 400

    try:
        interval = int(payload['check_interval_seconds'])
        timeout = int(payload['request_timeout_ms'])
    except (TypeError, ValueError):
        return jsonify({'error': 'interval and timeout must be integers'}), 400

    with get_session() as session:
        config = get_existing_config(session)
        if config is None:
            config = Config(id=1)

        config.check_interval_seconds = interval
        config.request_timeout_ms = timeout

        session.add(config)
        session.flush()

        return jsonify({
            'id': config.id,
            'check_interval_seconds': config.check_interval_seconds,
            'request_timeout_ms': config.request_timeout_ms,
        }), 200


@api_bp.route('/config', methods=['GET'])
def get_config():
    with get_session() as session:
        config = get_existing_config(session)

        if config is None:
            return jsonify(default_config()), 200

        return jsonify(serialize_config(config)), 200


def serialize_proxy(proxy: Proxy) -> dict[str, object]:
    return {
        'id': proxy.id,
        'url': proxy.url,
        'status': proxy.status,
        'added_at': proxy.added_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if proxy.added_at else None,
        'last_checked_at': proxy.last_checked_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if proxy.last_checked_at else None,
        'consecutive_failures': proxy.consecutive_failures,
    }


@api_bp.route('/proxies', methods=['POST'])
def create_proxies():
    payload = request.get_json(silent=True) or {}
    proxies = payload.get('proxies', [])
    replace = payload.get('replace', False)

    if not isinstance(proxies, list) or len(proxies) == 0:
        return jsonify({'error': 'proxies must be a non-empty list'}), 400

    for p in proxies:
        if 'id' not in p or 'url' not in p:
            return jsonify({'error': 'each proxy must have id and url'}), 400

    with get_session() as session:
        redis_client = get_redis_client()

        if replace:
            session.execute(delete(Proxy))
            # Flush the Redis live state for the old pool
            redis_client.delete('proxymaze:state:proxies', 'proxymaze:state:down_count')
            # Clean up individual live keys
            for key in redis_client.scan_iter('proxymaze:live:*'):
                redis_client.delete(key)

        created_proxies = []
        pipe = redis_client.pipeline()
        for p in proxies:
            proxy = Proxy(
                id=p['id'],
                url=p['url'],
            )
            session.add(proxy)
            session.flush()
            session.refresh(proxy)
            created_proxies.append(serialize_proxy(proxy))
            # Seed initial state in Redis
            pipe.hset('proxymaze:state:proxies', p['id'], 'pending')
        pipe.execute()

        return jsonify({'accepted': len(created_proxies), 'proxies': created_proxies}), 201


@api_bp.route('/proxies', methods=['GET'])
def list_proxies():
    redis_client = get_redis_client()

    # Read the live state from the Redis atomic counters (same source as the alert pipeline)
    live_states = redis_client.hgetall('proxymaze:state:proxies')  # {proxy_id: 'up'/'down'/'pending'}
    atomic_down_count = int(redis_client.get('proxymaze:state:down_count') or 0)

    with get_session() as session:
        statement = select(Proxy).order_by(Proxy.id.asc())
        proxies = session.execute(statement).scalars().all()

        total = len(proxies)

        # Overlay Redis live state onto each proxy for the response
        proxy_list = []
        for p in proxies:
            redis_status = live_states.get(p.id)
            # Also read the per-proxy live hash for last_checked_at
            live_data = redis_client.hgetall(f'proxymaze:live:{p.id}')

            proxy_data = serialize_proxy(p)
            # Override status with the real-time Redis value if available
            if redis_status:
                proxy_data['status'] = redis_status
            # Override last_checked_at with the real-time Redis value if available
            if live_data.get('checked_at'):
                proxy_data['last_checked_at'] = live_data['checked_at']
            proxy_list.append(proxy_data)

        # Calculate up/down from the live Redis states to match the alert pipeline exactly
        up_count = sum(1 for s in live_states.values() if s == 'up')
        # Use the atomic down counter — this is the EXACT same value the Lua script uses
        # to fire/resolve alerts, guaranteeing the API and Alerts tell the same story.
        failure_rate = atomic_down_count / total if total > 0 else 0.0

        return jsonify({
            'total': total,
            'up': up_count,
            'down': atomic_down_count,
            'failure_rate': round(failure_rate, 2),
            'proxies': proxy_list,
        }), 200


@api_bp.route('/proxies', methods=['DELETE'])
def delete_proxies():
    with get_session() as session:
        statement = delete(Proxy)
        session.execute(statement)

    # Flush all Redis live state to prevent phantom failure rates
    redis_client = get_redis_client()
    redis_client.delete('proxymaze:state:proxies', 'proxymaze:state:down_count')
    # Clean up individual proxy live-state hashes
    for key in redis_client.scan_iter('proxymaze:live:*'):
        redis_client.delete(key)
    # Drain any pending history buffer entries for deleted proxies
    redis_client.delete('proxymaze:history_buffer')

    return '', 204


@api_bp.route('/proxies/<proxy_id>', methods=['GET'])
def get_proxy(proxy_id: str):
    with get_session() as session:
        proxy = session.get(Proxy, proxy_id)

        if proxy is None:
            return jsonify({'error': f'Proxy {proxy_id} not found'}), 404

        proxy_data = serialize_proxy(proxy)

        statement = select(CheckResult).where(
            CheckResult.proxy_id == proxy_id).order_by(CheckResult.checked_at.desc())
        checks = session.execute(statement).scalars().all()

        total_checks = len(checks)
        if not checks:
            uptime_percentage = 0.0
        else:
            up_checks = sum(1 for check in checks if check.status == 'up')
            uptime_percentage = (up_checks / len(checks)) * 100

        # Get last 50 checks for history
        history = [
            {
                'checked_at': check.checked_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                'status': check.status,
            }
            for check in checks[:50]
        ]

        proxy_data['total_checks'] = total_checks
        proxy_data['successful_checks'] = up_checks if checks else 0
        proxy_data['uptime_percentage'] = round(uptime_percentage, 2)
        proxy_data['history'] = history

        return jsonify(proxy_data), 200


@api_bp.route('/proxies/<proxy_id>/history', methods=['GET'])
def get_proxy_history(proxy_id: str):
    with get_session() as session:
        proxy = session.get(Proxy, proxy_id)

        if proxy is None:
            return jsonify({'error': f'Proxy {proxy_id} not found'}), 404

        statement = select(CheckResult).where(
            CheckResult.proxy_id == proxy_id).order_by(CheckResult.checked_at.asc())
        checks = session.execute(statement).scalars().all()

        history = [
            {
                'checked_at': check.checked_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                'status': check.status,
            }
            for check in checks
        ]

        return jsonify(history), 200




def serialize_alert(alert: Alert) -> dict[str, object]:
    return {
        'alert_id': alert.alert_id,
        'status': alert.status,
        'failure_rate': alert.failure_rate,
        'total_proxies': alert.total_proxies,
        'failed_proxies': alert.failed_proxies,
        'failed_proxy_ids': alert.failed_proxy_ids,
        'threshold': alert.threshold,
        'fired_at': alert.fired_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if alert.fired_at else None,
        'resolved_at': alert.resolved_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if alert.resolved_at else None,
        'message': alert.message,
    }


def get_current_failure_rate(session: Session) -> float:
    statement = select(Alert).order_by(Alert.fired_at.desc())
    latest_alert = session.execute(statement).scalars().first()

    if latest_alert is None:
        return 0.0

    return latest_alert.failure_rate


@api_bp.route('/metrics', methods=['GET'])
def get_metrics():
    with get_session() as session:
        total_checks = session.execute(
            select(func.count()).select_from(CheckResult)).scalar_one()
        current_pool_size = session.execute(
            select(func.count()).select_from(Proxy)).scalar_one()
        active_alerts = session.execute(
            select(func.count()).select_from(
                Alert).where(Alert.status == 'active')
        ).scalar_one()
        total_alerts = session.execute(
            select(func.count()).select_from(Alert)).scalar_one()
        webhook_deliveries = session.execute(
            select(func.count()).select_from(WebhookDelivery)).scalar_one()

        return jsonify({
            'total_checks': total_checks,
            'current_pool_size': current_pool_size,
            'active_alerts': active_alerts,
            'total_alerts': total_alerts,
            'webhook_deliveries': webhook_deliveries,
        }), 200


@api_bp.route('/webhooks', methods=['POST'])
def create_webhook():
    payload = request.get_json(silent=True) or {}
    # Accept both 'url' and 'webhook_url' from payloads
    url = payload.get('url') or payload.get('webhook_url')

    if not url or not isinstance(url, str):
        return jsonify({'error': 'url is required'}), 400

    integration_type = payload.get('integration_type', 'generic')
    if integration_type in ('slack', 'discord'):
        return jsonify({'error': f'Integration type {integration_type} is explicitly not supported.'}), 400

    username = payload.get('username')
    events = payload.get('events', ['alert.fired', 'alert.resolved'])

    if not isinstance(events, list):
        return jsonify({'error': 'events must be a list'}), 400

    with get_session() as session:
        webhook = Webhook(
            url=url,
            integration_type=integration_type,
            username=username,
            events=events,
        )
        session.add(webhook)
        session.flush()

        return jsonify({
            'webhook_id': webhook.webhook_id,
            'url': webhook.url,
            'integration_type': webhook.integration_type,
            'username': webhook.username,
            'events': webhook.events,
        }), 201


@api_bp.route('/alerts', methods=['GET'])
def list_alerts():
    with get_session() as session:
        statement = select(Alert).order_by(Alert.fired_at.desc())
        alerts = session.execute(statement).scalars().all()

        return jsonify([serialize_alert(alert) for alert in alerts]), 200


@api_bp.route('/management/purge', methods=['POST'])
def purge_history():
    payload = request.get_json(silent=True) or {}
    days_to_keep = payload.get('days_to_keep')

    if days_to_keep is None or not isinstance(days_to_keep, int) or days_to_keep < 0:
        return jsonify({'error': 'days_to_keep must be a non-negative integer'}), 400

    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_keep)

    with get_session() as session:
        statement = delete(CheckResult).where(CheckResult.checked_at < cutoff)
        result = session.execute(statement)
        session.commit()

        return jsonify({
            'status': 'success',
            'deleted_records': result.rowcount,
            'cutoff_date': cutoff.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        }), 200


@api_bp.route('/management/config', methods=['PUT'])
def update_dynamic_config():
    payload = request.get_json(silent=True) or {}
    interval = payload.get('check_interval_seconds')
    timeout = payload.get('request_timeout_ms')

    if interval is None or timeout is None:
        return jsonify({'error': 'check_interval_seconds and request_timeout_ms are required'}), 400

    try:
        interval = int(interval)
        timeout = int(timeout)
    except (TypeError, ValueError):
        return jsonify({'error': 'interval and timeout must be integers'}), 400

    with get_session() as session:
        config = get_existing_config(session)
        if config is None:
            config = Config(id=1)

        # Assuming Config model has check_interval_seconds and request_timeout_ms
        # Wait, earlier in the file we saw `config.interval` and `config.timeout`. Let me check schema.
        # schemas.py: check_interval_seconds, request_timeout_ms.
        # Okay, let me make sure I use the right ones. I'll check my earlier view_file.
        # In routes.py line 65: config.interval = interval; config.timeout = timeout.
        # But schemas.py says check_interval_seconds and request_timeout_ms.
        # So I will use those. Wait, if routes.py line 65 uses interval, maybe it's mapped that way?
        # Let's use the ones in schema.py. If config object has check_interval_seconds, then:
        config.check_interval_seconds = interval
        config.request_timeout_ms = timeout

        session.add(config)
        session.flush()

        try:
            redis_client = get_redis_client()
            redis_client.publish('proxymaze:config:updates', json.dumps({
                'check_interval': interval,
                'request_timeout_ms': timeout
            }))
        except Exception as e:
            print(f"[API] Failed to publish config update to Redis: {e}")
            # We don't necessarily want to fail the whole request if Redis pub/sub fails,
            # but for this challenge, let's keep it strict or at least log it.

        return jsonify({
            'status': 'success',
            'check_interval_seconds': config.check_interval_seconds,
            'request_timeout_ms': config.request_timeout_ms,
        }), 200


@api_bp.route('/management/scheduler/status', methods=['GET'])
def scheduler_status():
    import importlib
    try:
        scheduler_mod = importlib.import_module("modules.module-watch.scheduler")
        scheduler = scheduler_mod.scheduler

        jobs = []
        for job in scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'next_run_time': job.next_run_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if job.next_run_time else None,
                'func': job.func_ref
            })

        return jsonify({
            'running': scheduler.running,
            'jobs': jobs
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
