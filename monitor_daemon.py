#!/usr/bin/env python3
from app.services.monitor import run_monitoring_round
import os
import sys
import time
from datetime import datetime

# Ensure project is on path and set cwd
project_home = os.path.abspath(os.path.dirname(__file__))
if project_home not in sys.path:
    sys.path.insert(0, project_home)
os.chdir(project_home)

os.environ.setdefault('FLASK_ENV', 'production')


print(f"[{datetime.now()}] Monitor daemon started")

while True:
    try:
        print(f"[{datetime.now()}] Running monitoring round...")
        result = run_monitoring_round()
        print(
            f"[{datetime.now()}] Success: failure_rate={result.get('failure_rate', 'N/A')}")
    except Exception as e:
        print(f"[{datetime.now()}] ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Wait 60 seconds before next round
    time.sleep(60)
