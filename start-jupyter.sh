#!/bin/bash

echo "Starting Jupyter Notebook with Docker..."
echo ""
echo "Once started, open your browser and go to:"
echo "  http://localhost:8888"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

cd "$(dirname "$0")"
docker-compose up jupyter

