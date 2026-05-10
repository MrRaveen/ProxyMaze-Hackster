from app import create_app
from app.services.monitor import start_monitor
import threading
import os

app = create_app()


if __name__ == '__main__':
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not app.debug:
        threading.Thread(target=start_monitor, daemon=True).start()
    print(app.url_map)
    app.run(host='127.0.0.1', port=5000, debug=True)
