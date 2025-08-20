# Architecture Overview

## System Design

┌─────────────────┐         ┌──────────────────┐
│   Windows Host  │         │  Linux Container │
│                 │         │   (ETL Worker)   │
│   Z:\ Drive     │         │                  │
│       ↓         │         │   - Celery       │
│ ┌─────────────┐ │  REST   │   - Python ETL   │
│ │  Windows    │◄├─────────┤   - Pandas       │
│ │  Container  │ │  API    │   - SQLAlchemy   │
│ │(File Server)│ │         │                  │
│ └─────────────┘ │         └──────────────────┘
└─────────────────┘                ↑
│
┌────┴────┐
│  Redis  │
└─────────┘

## Components

### Windows File Server
- **Purpose**: Bridge between Windows file system and Linux containers
- **Technology**: Python Flask API running on Windows Server Core
- **Endpoints**:
  - `/health` - Health check
  - `/list` - List available files
  - `/read/<filename>` - Read file as JSON
  - `/download/<filename>` - Download raw file
  - `/sheets/<filename>` - Get Excel sheet names

### ETL Worker
- **Purpose**: Process Excel files and perform ETL operations
- **Technology**: Python with Celery for task queue
- **Key Libraries**: pandas, requests, pyodbc, SQLAlchemy

### Redis
- **Purpose**: Message broker for Celery
- **Persistence**: Optional volume for data persistence

## Data Flow

1. ETL Worker receives task (via Celery/Redis)
2. Worker requests file from File Server via REST API
3. File Server reads file from Z:\ drive
4. File Server returns data to Worker
5. Worker processes data and writes to database

## Security Considerations

- File Server only exposes specific endpoints
- No direct file system access from Linux containers
- Network isolation via Docker networks
- Consider adding authentication for production