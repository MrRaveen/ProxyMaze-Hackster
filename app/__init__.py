from flask import Flask

def create_app():
    app = Flask(__name__)

    # Import the blueprint from your routes file
    # Ensure the path matches your actual folder structure
    from app.api.routes import api_bp 

    # Register the blueprint
    # If you register it without a url_prefix, the route is just /health
    app.register_blueprint(api_bp)

    # OR, if you want versioned URLs like /api/v1/health:
    # app.register_blueprint(api_bp, url_prefix='/api/v1')

    # Initialize the Watch subsystem (APScheduler, PubSub, Kafka Delivery)
    try:
        import importlib
        module_watch = importlib.import_module("modules.module-watch")
        module_watch.init_app(app)
    except Exception as e:
        print(f"⚠️ Failed to initialize module-watch subsystem: {e}")

    return app