#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Running benchmark..."
python3 src/benchmark.py \
  --max-ticks 56 \
  --map-mode varied \
  --write-replay-trace \
  --runs 100 \
  --seed 3999 \
  --scenario mixed

echo
echo "Running replay audit..."
python3 src/replay_trace_audit.py \
  --dir data/grenade_missionflow_replays_v3 \
  --print \
  --limit 100

echo
echo "Launching replay viewer..."
python3 src/replay_viewer.py \
  --dir data/grenade_missionflow_replays_v3
