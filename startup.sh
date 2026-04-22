#!/bin/bash
set -e

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install --no-cache-dir -r requirements.txt

# Start FastAPI using Gunicorn + Uvicorn worker
gunicorn app.main:app -w 1 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:$PORT