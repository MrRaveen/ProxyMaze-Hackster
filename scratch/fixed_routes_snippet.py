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
