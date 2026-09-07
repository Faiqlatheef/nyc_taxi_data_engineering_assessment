#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
docker compose up -d postgres
mkdir -p data/raw data/rejected logs
echo "Ready. Run: python src/generate_sample_data.py --rows 50000"
echo "Then: python src/flow.py"
