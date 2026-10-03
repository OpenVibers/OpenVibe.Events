#!/usr/bin/env bash
# A NATS Core broker in a container for test/fabric-nats.test.js (the optional nats-v1 TOPIC carrier). Not production:
# no auth, loopback only, no JetStream.
#
#   scripts/test-nats.sh up      start (or reuse) and print the env to export (OV_TEST_NATS_URL)
#   scripts/test-nats.sh down    remove the container
#
# Without OV_TEST_NATS_URL the broker-backed cases print "fabric nats: skipped (…)" and the rest still runs.
set -euo pipefail
NAME=events-test-nats; PORT=${OV_TEST_NATS_PORT:-54222}; IMAGE=nats:2.11-alpine
case "${1:-up}" in
up)
  if ! docker ps --format '{{.Names}}' | grep -qx $NAME; then
    docker rm -f $NAME >/dev/null 2>&1 || true
    docker run -d --name $NAME -p 127.0.0.1:$PORT:4222 $IMAGE >/dev/null
  fi
  for i in $(seq 1 40); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && break; sleep 0.25; done
  echo "export OV_TEST_NATS_URL=nats://127.0.0.1:$PORT"
  ;;
down) docker rm -f $NAME >/dev/null 2>&1 || true; echo removed ;;
*) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
