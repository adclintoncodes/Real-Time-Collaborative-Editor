#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for Kafka and Cassandra health checks..."
for i in $(seq 1 60); do
  kafka=$(docker inspect --format='{{.State.Health.Status}}' collab-kafka 2>/dev/null || echo starting)
  cass=$(docker inspect --format='{{.State.Health.Status}}' collab-cassandra 2>/dev/null || echo starting)
  echo "[$i] kafka=$kafka cassandra=$cass"
  if [[ "$kafka" == "healthy" && "$cass" == "healthy" ]]; then
    echo "Infrastructure healthy."
    exit 0
  fi
  sleep 5
done

echo "Timed out waiting for infrastructure."
exit 1
