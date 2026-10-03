#!/usr/bin/env bash
# A NATS Core broker in a container for test/fabric-nats.test.js (the optional nats-v1 TOPIC carrier). Not production:
# loopback only, no JetStream; scripts/nats-test.conf adds two users without publish or subscribe permission.
#
#   scripts/test-nats.sh up      start (or reuse) and print the env to export (OV_TEST_NATS_URL)
#   scripts/test-nats.sh down    remove the container
#
# Without OV_TEST_NATS_URL the broker-backed cases print "fabric nats: skipped (…)" and the rest still runs.
set -euo pipefail
NAME=events-test-nats; PORT=${OV_TEST_NATS_PORT:-54222}; IMAGE=nats:2.11-alpine
CONF="$(cd "$(dirname "$0")" && pwd)/nats-test.conf"; SUM=$(sha256sum "$CONF" | cut -c1-12)
case "${1:-up}" in
up)
  # Reused only while it runs this config on this port (the labels carry the checksum and the port).
  if [ "$(docker inspect -f '{{.State.Running}} {{index .Config.Labels "ov.conf"}} {{index .Config.Labels "ov.port"}}' $NAME 2>/dev/null)" != "true $SUM $PORT" ]; then
    docker rm -f $NAME >/dev/null 2>&1 || true
    docker run -d --name $NAME --label ov.conf=$SUM --label ov.port=$PORT -p 127.0.0.1:$PORT:4222 -v "$CONF:/etc/nats/test.conf:ro" $IMAGE -c /etc/nats/test.conf >/dev/null
  fi
  ready=""
  for i in $(seq 1 40); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && { ready=1; break; }; sleep 0.25; done
  if [ -z "$ready" ]; then echo "$NAME not listening on 127.0.0.1:$PORT after 10s" >&2; exit 1; fi
  echo "export OV_TEST_NATS_URL=nats://127.0.0.1:$PORT"
  ;;
down) docker rm -f $NAME >/dev/null 2>&1 || true; echo removed ;;
*) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
