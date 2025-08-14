#!/bin/bash

# Light ETL POC - Linux Startup Script
# This script starts the services on Linux/Mac systems

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Light ETL POC...${NC}"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

if ! command_exists docker; then
    echo -e "${RED}✗ Docker is not installed${NC}"
    echo "Please install Docker from https://docs.docker.com/get-docker/"
    exit 1
fi

if ! command_exists docker-compose; then
    echo -e "${RED}✗ Docker Compose is not installed${NC}"
    echo "Please install Docker Compose from https://docs.docker.com/compose/install/"
    exit 1
fi

echo -e "${GREEN}✓ Docker is installed${NC}"
echo -e "${GREEN}✓ Docker Compose is installed${NC}"

# Check if .env file exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env file from template...${NC}"
    cp .env.example .env
    echo -e "${YELLOW}Please edit .env file with your configuration${NC}"
    read -p "Press enter to continue after editing .env file..."
fi

# Create necessary directories
echo -e "${YELLOW}Creating necessary directories...${NC}"
mkdir -p shared_data logs etl-worker/logs

# Stop any existing containers
echo -e "${YELLOW}Stopping any existing containers...${NC}"
docker-compose down 2>/dev/null || true

# Build containers
echo -e "${YELLOW}Building containers...${NC}"
docker-compose build

# Start services
echo -e "${YELLOW}Starting services...${NC}"
docker-compose up -d

# Wait for services to be ready
echo -e "${YELLOW}Waiting for services to be ready...${NC}"
sleep 10

# Function to check service health
check_service() {
    local service=$1
    local url=$2
    local max_attempts=30
    local attempt=1
    
    echo -n "Checking $service..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "$url" > /dev/null 2>&1; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo -e " ${RED}✗${NC}"
    return 1
}

# Check services
echo -e "${YELLOW}Checking service health...${NC}"

# Check Redis
if docker-compose exec -T redis redis-cli ping > /dev/null 2>&1; then
    echo -e "Redis... ${GREEN}✓${NC}"
else
    echo -e "Redis... ${RED}✗${NC}"
fi

# Check File Server (Windows container might not be available on Linux)
if [[ "$OSTYPE" == "linux-gnu"* ]] || [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "${YELLOW}Note: Windows file server container requires Windows host${NC}"
    echo -e "${YELLOW}You may need to run a mock file server or use an alternative approach${NC}"
else
    check_service "File Server" "http://localhost:5000/health"
fi

# Check Flower
check_service "Flower (Celery Monitor)" "http://localhost:5555"

# Test connection
echo -e "${YELLOW}Testing connection...${NC}"
if command_exists python3; then
    python3 scripts/test-connection.py || true
elif command_exists python; then
    python scripts/test-connection.py || true
else
    echo -e "${YELLOW}Python not installed locally, skipping connection test${NC}"
fi

# Show logs command
echo ""
echo -e "${GREEN}All services started!${NC}"
echo ""
echo -e "${GREEN}Services available at:${NC}"
echo "  - File Server: http://localhost:5000 (Windows host only)"
echo "  - Flower (Celery monitoring): http://localhost:5555"
echo "  - Redis: localhost:6379"
echo ""
echo -e "${YELLOW}Useful commands:${NC}"
echo "  View logs:           docker-compose logs -f"
echo "  View specific logs:  docker-compose logs -f etl-worker"
echo "  Stop services:       docker-compose down"
echo "  Restart services:    docker-compose restart"
echo "  Access worker shell: docker-compose exec etl-worker bash"
echo "  Run tests:          docker-compose exec etl-worker pytest tests/"
echo ""
echo -e "${GREEN}ETL Worker is ready to process files!${NC}"

# Option to tail logs
read -p "Would you like to view the logs now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker-compose logs -f
fi