#!/bin/bash
set -e
cd "$(dirname "$0")"
PROJECT_DIR="$(pwd)"

[ -f .env ] && export $(grep -v '^#' .env | xargs)
export DOCKER_UID="$(id -u)"
export DOCKER_GID="$(id -g)"

if [ "$1" = "--autostart" ]; then
    SERVICE_NAME="datamanager"
    SERVICE_FILE="$HOME/.config/systemd/user/${SERVICE_NAME}.service"
    mkdir -p "$HOME/.config/systemd/user"
    cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=DataManager
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=${PROJECT_DIR}
ExecStart=/usr/bin/docker compose up --build -d
ExecStop=/usr/bin/docker compose down
Environment=DOCKER_UID=${DOCKER_UID}
Environment=DOCKER_GID=${DOCKER_GID}

[Install]
WantedBy=default.target
EOF
    systemctl --user daemon-reload
    systemctl --user enable "$SERVICE_NAME"
    systemctl --user start "$SERVICE_NAME"
    loginctl enable-linger "$(whoami)"
    echo "DataManager added to autostart (systemd user service)"
    echo "  Status:  systemctl --user status $SERVICE_NAME"
    echo "  Logs:    journalctl --user -u $SERVICE_NAME -f"
    echo "  Remove:  ./start.sh --no-autostart"
    exit 0
fi

if [ "$1" = "--no-autostart" ]; then
    SERVICE_NAME="datamanager"
    systemctl --user stop "$SERVICE_NAME" 2>/dev/null || true
    systemctl --user disable "$SERVICE_NAME" 2>/dev/null || true
    rm -f "$HOME/.config/systemd/user/${SERVICE_NAME}.service"
    systemctl --user daemon-reload
    echo "DataManager removed from autostart"
    exit 0
fi

echo "Starting DataManager (UID=$DOCKER_UID)..."
docker compose down 2>/dev/null || true
docker compose up --build -d

echo ""
echo "DataManager: http://localhost:8000"
echo "Logs:      docker compose logs -f"
