# Light ETL Windows Container POC

A proof of concept for accessing Windows shared drives (Z:\) from Linux containers using a Windows file server container as an intermediary.

## Architecture

- **Windows Container**: File server that accesses Z:\ drive and exposes REST API
- **Linux Container**: ETL worker running Python/Celery that consumes the file server API
- **Communication**: REST API over Docker network

## Project Structure

<details>
<summary>Click to expand project structure</summary>
```bash
light-etl-windows-container-poc/
│
├── README.md
├── .gitignore
├── .env.example
├── docker-compose.yml
│
├── fileserver/                    # Windows container service
│   ├── Dockerfile
│   ├── fileserver.py
│   └── requirements.txt
│
├── etl-worker/                    # Linux container service
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── celery_config.py
│   ├── etl_processor/
│   │   ├── __init__.py
│   │   ├── celery_app.py
│   │   ├── tasks.py
│   │   ├── file_access.py
│   │   └── utils.py
│   └── tests/
│       ├── __init__.py
│       └── test_file_access.py
│
├── shared_data/                   # Local test data (not for production)
│   └── sample.xlsx
│
├── scripts/                       # Utility scripts
│   ├── start-windows.ps1
│   ├── start-linux.sh
│   └── test-connection.py
│
└── docs/
    ├── architecture.md
    └── setup-guide.md
```
</details>

## Quick Start

1. Clone the repository
2. Copy `.env.example` to `.env` and configure
3. Ensure Z:\ drive is mounted on Windows host
4. Run: `docker-compose up --build`

## Services

- `fileserver`: Windows container (port 5000) - File access API
- `etl-worker`: Linux container - Celery worker for ETL processing
- `redis`: Redis for Celery broker

See [docs/setup-guide.md](docs/setup-guide.md) for detailed instructions.