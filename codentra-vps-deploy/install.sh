#!/bin/bash
# VPS Installation Script - Run this on your VPS

set -e

echo "╔════════════════════════════════════════════════════════════╗"
echo "║          Codentra VPS Installation                         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check if root
if [ "$EUID" -ne 0 ]; then 
    echo "❌ Please run as root: sudo ./install.sh"
    exit 1
fi

# Update system
echo "📦 Updating system..."
apt-get update && apt-get upgrade -y

# Install Docker
echo "🐳 Installing Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
fi

# Install Docker Compose
echo "🐳 Installing Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
        -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
fi

# Move to /opt
APP_DIR="/opt/codentra"
echo "📁 Setting up application directory..."
mkdir -p "$APP_DIR"

# Copy files
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cp -r "$SCRIPT_DIR/"* "$APP_DIR/"

# Set permissions
chown -R root:root "$APP_DIR"
chmod +x "$APP_DIR/deploy.sh"
chmod +x "$APP_DIR/setup.sh"

cd "$APP_DIR"

# Run deploy script
./deploy.sh

# Run setup
./setup.sh

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║          Installation Complete!                            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "🌐 Your application should be running at:"
echo "   http://$(curl -s ifconfig.me 2>/dev/null || echo 'YOUR_SERVER_IP')"
echo ""
echo "⚙️  Next steps:"
echo "   1. Edit $APP_DIR/.env and change default passwords"
echo "   2. Configure SSL: certbot --nginx -d yourdomain.com"
echo "   3. Check logs: docker-compose logs -f"
echo ""
