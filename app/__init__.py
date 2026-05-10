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

    return app