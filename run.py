from app import create_app
import os

app = create_app()

if __name__ == '__main__':
    print(app.url_map)
    app.run(host='127.0.0.1', port=5000, debug=True)
