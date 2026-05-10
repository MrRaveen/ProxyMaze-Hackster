## setup
- Activate virtual environment (Linux/macOS)

```source venv/bin/activate```

- Activate it (Windows)

```venv\Scripts\activate```

- Install packages

```pip install -r requirements.txt```

- Run the backend

```flask run```

- test endpoint

```curl -X GET http://127.0.0.1:5000/api/v1/health```

## Folder structure
```
proxymaze-flask/
├── app/
│   ├── __init__.py             # Application factory (initializes extensions)
│   ├── api/                    # API Blueprints
│   │   ├── __init__.py
│   │   └── v1_routes.py        # Core endpoints
│   ├── models/                 # Database schemas (e.g., SQLAlchemy)
│   │   └── schemas.py
│   ├── services/               # Business logic & integrations
│   │   ├── torch_service.py    # ISOLATED: Your Torch Proxies integration logic
│   │   └── cache_service.py    # Redis caching layer for efficiency scoring
│   └── utils/                  # Helper functions (logging, formatting)
├── tests/                      
│   ├── conftest.py             # Pytest fixtures
│   └── test_critical.py        # Only test the main Torch Proxies integration path
├── docker-compose.yml          # ONLY for spinning up backing services (DB/Redis) fast
├── requirements.txt            
├── .env                        
├── .gitignore
└── run.py                      # Simple entry point: app = create_app()
```
