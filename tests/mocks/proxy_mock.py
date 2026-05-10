import time
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/proxy/<proxy_id>', methods=['GET'])
def get_proxy(proxy_id):
    state = request.args.get('state', 'healthy').lower()
    
    if state == 'timeout':
        # Intentionally exceed standard 3-second timeout
        time.sleep(5)
        return jsonify({
            "status": "success",
            "proxy_id": proxy_id,
            "message": "Delayed response simulated"
        }), 200
    
    elif state == 'error':
        return jsonify({
            "status": "error",
            "proxy_id": proxy_id,
            "message": "Simulated upstream failure"
        }), 503
    
    # Default 'healthy' state
    return jsonify({
        "status": "success",
        "proxy_id": proxy_id,
        "message": "Healthy proxy response"
    }), 200

if __name__ == '__main__':
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    app.run(port=port, host='0.0.0.0')
