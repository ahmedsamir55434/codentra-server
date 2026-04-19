#!/bin/bash

# Codentra Application Setup Script
# Run this after deploy.sh to initialize the application

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

APP_DIR="/opt/codentra"

echo -e "${GREEN}=== Codentra Application Setup ===${NC}"

# Check if we're in the right directory
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}Error: docker-compose.yml not found${NC}"
    echo "Please run this script from the codentra directory"
    exit 1
fi

# Copy files to app directory
echo -e "${YELLOW}Copying application files...${NC}"
sudo mkdir -p $APP_DIR
sudo cp -r . $APP_DIR/
sudo chown -R $USER:$USER $APP_DIR

# Create necessary directories
mkdir -p $APP_DIR/uploads
mkdir -p $APP_DIR/ssl
mkdir -p $APP_DIR/data

cd $APP_DIR

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}Creating .env file...${NC}"
    cat > .env << 'EOF'
# Database Configuration
DB_PASSWORD=change_this_secure_password

# Application Secrets (CHANGE THESE!)
SESSION_SECRET=change_this_session_secret_32_characters
JWT_SECRET=change_this_jwt_secret_32_characters

# Environment
NODE_ENV=production
EOF
    echo -e "${RED}WARNING: Please edit .env and change the default passwords!${NC}"
fi

# Pull latest images
echo -e "${YELLOW}Pulling Docker images...${NC}"
docker-compose pull

# Build application
echo -e "${YELLOW}Building application...${NC}"
docker-compose build --no-cache

# Initialize database
echo -e "${YELLOW}Starting database...${NC}"
docker-compose up -d db

# Wait for database to be ready
echo -e "${YELLOW}Waiting for database...${NC}"
sleep 5
until docker-compose exec -T db pg_isready -U codentra -d codentra; do
    echo -e "${YELLOW}Database not ready yet, waiting...${NC}"
    sleep 2
done

# Initialize schema
echo -e "${YELLOW}Initializing database schema...${NC}"
docker-compose exec -T db psql -U codentra -d codentra < db/schema.sql

# Start all services
echo -e "${YELLOW}Starting all services...${NC}"
docker-compose up -d

# Check status
echo -e "${YELLOW}Checking service status...${NC}"
docker-compose ps

echo ""
echo -e "${GREEN}=== Setup Complete! ===${NC}"
echo ""
echo "Application is running at:"
echo "  - HTTP:  http://$(curl -s ifconfig.me 2>/dev/null || echo 'your-server-ip')"
echo "  - HTTPS: https://$(curl -s ifconfig.me 2>/dev/null || echo 'your-server-ip') (if SSL configured)"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f"
echo ""
echo "To stop:"
echo "  docker-compose down"
echo ""
echo "Next steps:"
echo "1. Edit $APP_DIR/.env and change default passwords"
echo "2. Configure SSL certificates (see README-VPS.md)"
echo "3. Set up domain name and DNS"
