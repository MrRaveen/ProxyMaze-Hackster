import urllib.request
import json
import time
import os
import sys

BASE_URL = "http://127.0.0.1:5000"

def get_data(path):
    try:
        with urllib.request.urlopen(f"{BASE_URL}{path}", timeout=2) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception:
        return None

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def render():
    while True:
        clear_screen()
        print("="*80)
        print("🛰️  PROXY MAZE - LIVE ACTIVITY MONITOR 🛰️")
        print("="*80)
        
        # 1. Metrics
        metrics = get_data('/metrics')
        if metrics:
            print(f"TOTAL CHECKS: {metrics.get('total_checks', 0)} | POOL SIZE: {metrics.get('current_pool_size', 0)}")
            print(f"ACTIVE ALERTS: {metrics.get('active_alerts', 0)} | TOTAL ALERTS: {metrics.get('total_alerts', 0)}")
            print(f"WEBHOOK DELIVERIES: {metrics.get('webhook_deliveries', 0)}")
        else:
            print("⚠️  Unable to fetch metrics. Is Flask running?")
        
        print("-"*80)
        
        # 2. Scheduler
        sched = get_data('/management/scheduler/status')
        if sched:
            print(f"SCHEDULER: {'🟢 RUNNING' if sched.get('running') else '🔴 STOPPED'}")
            for job in sched.get('jobs', []):
                print(f"  - [{job['id']}] Next: {job['next_run_time']} ({job['func']})")
        else:
            print("SCHEDULER: ❓ UNKNOWN")

        print("-"*80)
        
        # 3. Proxies
        proxies_data = get_data('/proxies')
        if proxies_data:
            print(f"POOL HEALTH: UP: {proxies_data.get('up')} | DOWN: {proxies_data.get('down')} | FAILURE RATE: {proxies_data.get('failure_rate')*100:.1f}%")
            print("\nID\t\tSTATUS\t\tLAST CHECKED\t\tURL")
            for p in proxies_data.get('proxies', [])[:10]: # Show top 10
                last = p.get('last_checked_at', 'NEVER')
                print(f"{p['id']}\t{p['status'].upper()}\t\t{last}\t{p['url'][:30]}...")
            if len(proxies_data.get('proxies', [])) > 10:
                print(f"... and {len(proxies_data['proxies']) - 10} more.")
        
        print("\n" + "="*80)
        print("Press Ctrl+C to exit. Refreshing every 2 seconds...")
        
        time.sleep(2)

if __name__ == "__main__":
    try:
        render()
    except KeyboardInterrupt:
        print("\nExiting monitor...")
        sys.exit(0)
