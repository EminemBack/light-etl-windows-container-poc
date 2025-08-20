# PowerShell script to start the services
Write-Host "Starting Light ETL POC..." -ForegroundColor Green

# Check if Z: drive is accessible
if (Test-Path "Z:\") {
    Write-Host "✓ Z: drive is accessible" -ForegroundColor Green
} else {
    Write-Host "✗ Z: drive is not accessible" -ForegroundColor Red
    Write-Host "Please ensure Z: drive is mounted before continuing"
    exit 1
}

# Build and start containers
Write-Host "Building containers..." -ForegroundColor Yellow
docker-compose build

Write-Host "Starting services..." -ForegroundColor Yellow
docker-compose up -d

# Wait for services to be ready
Write-Host "Waiting for services to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Test connection
Write-Host "Testing connection..." -ForegroundColor Yellow
python scripts/test-connection.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ All services are running!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Services available at:" -ForegroundColor Cyan
    Write-Host "  - File Server: http://localhost:5000"
    Write-Host "  - Flower (Celery monitoring): http://localhost:5555"
    Write-Host "  - Redis: localhost:6379"
} else {
    Write-Host "✗ Service startup failed" -ForegroundColor Red
    Write-Host "Check logs with: docker-compose logs"
}