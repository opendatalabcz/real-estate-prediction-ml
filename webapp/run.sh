#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
kill $(lsof -ti :8080) 2>/dev/null || true
sleep 1
python3 -m pip install -q -r requirements.txt 2>/dev/null || true
exec python3 -c "from app import app; app.run(host='0.0.0.0', port=8080, debug=False)"
