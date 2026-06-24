#!/bin/bash
cd "$(dirname "$0")"

if [ "$1" = "--clean" ]; then
    docker compose down --rmi all --volumes 2>/dev/null || true
    rm -rf backend/__pycache__ frontend/node_modules frontend/dist
    echo "DataManager stopped, images removed"
else
    docker compose down
    echo "DataManager stopped"
fi
