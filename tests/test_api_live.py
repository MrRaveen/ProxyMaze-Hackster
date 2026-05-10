import pytest
from datetime import datetime, timezone
from app import create_app
from config.database import engine, get_session
from app.models.schemas import Base, Proxy, CheckResult, Alert

@pytest.fixture(scope="module", autouse=True)
def setup_db():
    """Wipe and recreate the database before running the suite."""
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

@pytest.fixture(scope="module")
def app():
    app = create_app()
    app.config.update({"TESTING": True})
    return app

@pytest.fixture(scope="module")
def client(app):
    return app.test_client()

# ==========================================
# 1. Core Health & Config (Chapters 01 - 03)
# ==========================================

def test_01_health(client):
    """GET /health - Chapter 01"""
    res = client.get('/health')
    assert res.status_code == 200
    assert res.get_json() == {"status": "ok"}

def test_02_post_config(client):
    """POST /config - Chapter 02"""
    payload = {"check_interval_seconds": 45, "request_timeout_ms": 2500}
    res = client.post('/config', json=payload)
    assert res.status_code == 200
    data = res.get_json()
    assert data['check_interval_seconds'] == 45

def test_03_get_config(client):
    """GET /config - Chapter 03"""
    res = client.get('/config')
    assert res.status_code == 200
    data = res.get_json()
    assert data['check_interval_seconds'] == 45
    assert data['request_timeout_ms'] == 2500

# ==========================================
# 2. Pool Operations (Chapters 04 - 08)
# ==========================================

def test_04_post_proxies(client):
    """POST /proxies - Chapter 04"""
    proxies = [
        {"id": "px-101", "url": "http://127.0.0.1:9001/status"},
        {"id": "px-102", "url": "http://127.0.0.1:9002/status"}
    ]
    res = client.post('/proxies', json={"proxies": proxies, "replace": True})
    assert res.status_code == 201
    assert res.get_json()['created'] == 2

def test_05_get_proxies(client):
    """GET /proxies - Chapter 05"""
    res = client.get('/proxies')
    assert res.status_code == 200
    data = res.get_json()
    assert data['total'] == 2
    assert 'failure_rate' in data

def test_06_get_proxy_dossier(client):
    """GET /proxies/{id} - Chapter 06 (Dossier)"""
    res = client.get('/proxies/px-101')
    assert res.status_code == 200
    data = res.get_json()
    assert data['id'] == "px-101"
    assert 'uptime_percentage' in data
    assert 'history' in data
    assert 'total_checks' in data

def test_07_get_proxy_history(client):
    """GET /proxies/{id}/history - Chapter 07"""
    res = client.get('/proxies/px-101/history')
    assert res.status_code == 200
    assert isinstance(res.get_json(), list)

# ==========================================
# 3. Observability & Webhooks (Chapters 09 - 12)
# ==========================================

def test_08_get_metrics(client):
    """GET /metrics - Chapter 12"""
    res = client.get('/metrics')
    assert res.status_code == 200
    data = res.get_json()
    assert 'current_pool_size' in data
    assert data['current_pool_size'] == 2

def test_09_get_alerts(client):
    """GET /alerts - Chapter 09"""
    res = client.get('/alerts')
    assert res.status_code == 200
    assert isinstance(res.get_json(), list)

def test_10_post_webhooks(client):
    """POST /webhooks - Chapter 10"""
    payload = {"url": "http://example.com/webhook", "events": ["alert.fired"]}
    res = client.post('/webhooks', json=payload)
    assert res.status_code == 201
    assert 'id' in res.get_json()

def test_11_post_integrations(client):
    """POST /integrations - Chapter 11 (Bonus)"""
    payload = {
        "type": "slack",
        "webhook_url": "https://hooks.slack.com/services/XXX",
        "events": ["alert.fired"]
    }
    res = client.post('/integrations', json=payload)
    assert res.status_code == 201
    assert res.get_json()['type'] == 'slack'

# ==========================================
# 4. Cleanup & Deletion Routes
# ==========================================

def test_12_post_management_purge(client):
    """POST /management/purge - DB Hygiene"""
    payload = {"days_to_keep": 0}
    res = client.post('/management/purge', json=payload)
    assert res.status_code == 200
    assert res.get_json()['status'] == 'success'

def test_13_delete_proxies(client):
    """DELETE /proxies - Chapter 08"""
    # Execute delete
    res = client.delete('/proxies')
    assert res.status_code == 204
    
    # Verify the pool is actually empty
    verify_res = client.get('/proxies')
    assert verify_res.get_json()['total'] == 0

# import pytest
# import json
# from datetime import datetime, timezone
# from app import create_app
# from config.database import engine, get_session
# from app.models.schemas import Base, Proxy, Alert, CheckResult, Config, Webhook, WebhookDelivery

# @pytest.fixture(scope="session", autouse=True)
# def setup_db():
#     Base.metadata.drop_all(bind=engine)
#     Base.metadata.create_all(bind=engine)

# @pytest.fixture
# def app():
#     app = create_app()
#     app.config.update({
#         "TESTING": True,
#     })
#     return app

# @pytest.fixture
# def client(app):
#     return app.test_client()

# def test_health(client):
#     """Verify GET /health returns exactly {"status": "ok"}"""
#     response = client.get('/health')
#     assert response.status_code == 200
#     data = response.get_json()
#     assert data == {"status": "ok"}

# def test_config_workflow(client):
#     """Verify POST /config and GET /config with correct field names"""
#     # 1. Update config
#     payload = {
#         "check_interval_seconds": 45,
#         "request_timeout_ms": 2500
#     }
#     response = client.post('/config', json=payload)
#     assert response.status_code == 200
#     data = response.get_json()
#     assert data['check_interval_seconds'] == 45
#     assert data['request_timeout_ms'] == 2500

#     # 2. Get config
#     response = client.get('/config')
#     assert response.status_code == 200
#     data = response.get_json()
#     assert data['check_interval_seconds'] == 45
#     assert data['request_timeout_ms'] == 2500

#     # 3. Management Dynamic Config (PUT)
#     payload_mgt = {
#         "check_interval_seconds": 15,
#         "request_timeout_ms": 3000
#     }
#     response = client.put('/management/config', json=payload_mgt)
#     assert response.status_code == 200
    
#     # 4. Final verification
#     response = client.get('/config')
#     data = response.get_json()
#     assert data['check_interval_seconds'] == 15
#     assert data['request_timeout_ms'] == 3000

# def test_proxy_operations(client):
#     """Verify POST, GET, DELETE /proxies and Dossier output"""
#     # 1. Clean pool
#     res_del = client.delete('/proxies')
#     assert res_del.status_code == 204

#     # 2. Add proxies
#     proxies = [
#         {"id": "px-live-1", "url": "http://127.0.0.1:9001/status"},
#         {"id": "px-live-2", "url": "http://127.0.0.1:9002/status"}
#     ]
#     res_post = client.post('/proxies', json={"proxies": proxies, "replace": True})
#     assert res_post.status_code == 201
    
#     # 3. List and check counts
#     res_list = client.get('/proxies')
#     assert res_list.status_code == 200
#     data = res_list.get_json()
#     assert data['total'] == 2
#     # Verify timestamp format (Z suffix)
#     for p in data['proxies']:
#         if p['added_at']:
#             assert p['added_at'].endswith('Z')
#             # Verify ISO format length (approx)
#             assert len(p['added_at']) >= 20

#     # 4. Get Single Proxy (Dossier - fulfilling Chapter 06)
#     res_one = client.get('/proxies/px-live-1')
#     assert res_one.status_code == 200
#     data = res_one.get_json()
#     assert data['id'] == "px-live-1"
#     assert 'total_checks' in data
#     assert 'uptime_percentage' in data
#     assert 'history' in data
#     assert 'successful_checks' in data

#     # 5. History Endpoint
#     res_hist = client.get('/proxies/px-live-1/history')
#     assert res_hist.status_code == 200
#     assert isinstance(res_hist.get_json(), list)

# def test_alerts_metrics_and_webhooks(client):
#     """Verify remaining read and registration routes"""
#     # 1. Metrics
#     res_met = client.get('/metrics')
#     assert res_met.status_code == 200
#     data = res_met.get_json()
#     assert 'total_checks' in data
#     assert 'current_pool_size' in data
#     assert 'active_alerts' in data

#     # 2. Alerts List
#     res_al = client.get('/alerts')
#     assert res_al.status_code == 200
#     assert isinstance(res_al.get_json(), list)

#     # 3. Webhook Registration
#     wh_payload = {"url": "http://example.com/webhook", "events": ["alert.fired"]}
#     res_wh = client.post('/webhooks', json=wh_payload)
#     assert res_wh.status_code == 201
#     assert 'id' in res_wh.get_json()

#     # 4. Integration Registration
#     int_payload = {
#         "type": "slack",
#         "webhook_url": "https://hooks.slack.com/services/XXX",
#         "events": ["alert.fired"]
#     }
#     res_int = client.post('/integrations', json=int_payload)
#     assert res_int.status_code == 201

# def test_management_purge(client):
#     """Verify the purge route handles parameters correctly"""
#     payload = {"days_to_keep": 0} # Purge everything older than now
#     res = client.post('/management/purge', json=payload)
#     assert res.status_code == 200
#     data = res.get_json()
#     assert data['status'] == 'success'
#     assert 'deleted_records' in data
#     assert data['cutoff_date'].endswith('Z')
