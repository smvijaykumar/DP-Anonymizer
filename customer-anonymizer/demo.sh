#!/usr/bin/env bash
# One-click demo of the customer anonymizer.
#
#   ./demo.sh          build and start everything, then open http://localhost:8080
#   ./demo.sh logs     follow the pipeline logs
#   ./demo.sh rerun-profile   run the daily profile job again (publishes a new version)
#   ./demo.sh stop     stop the containers, keep the data
#   ./demo.sh clean    stop and delete all demo data (Kafka topics, snapshot, profiles, output)
#
# Needs Docker with Compose v2 (Docker Desktop, OrbStack, Colima...) or Podman with a compose provider.
# The first run downloads ~2 GB of images and Maven dependencies and takes a few minutes.
set -euo pipefail
cd "$(dirname "$0")"

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
  COMPOSE=(podman compose)
else
  echo "Docker Compose v2 (or Podman with a compose provider) is required." >&2
  exit 1
fi

PORT="${DEMO_PORT:-8080}"
URL="http://localhost:${PORT}"

wait_for_viewer() {
  printf 'Waiting for the pipeline'
  for _ in $(seq 1 180); do
    if curl -fsS "$URL/health" >/dev/null 2>&1; then
      printf '\n'
      return 0
    fi
    printf '.'
    sleep 2
  done
  printf '\nThe viewer did not come up; check: %s logs\n' "$0" >&2
  return 1
}

open_browser() {
  [[ -n "${NO_BROWSER:-}" ]] && return 0
  if command -v open >/dev/null 2>&1; then open "$URL"
  elif command -v xdg-open >/dev/null 2>&1; then xdg-open "$URL" >/dev/null 2>&1 || true
  fi
}

case "${1:-up}" in
  up)
    "${COMPOSE[@]}" up -d --build
    wait_for_viewer
    cat <<EOF

  Customer Anonymizer demo is running.

    Viewer           $URL
    Stream metrics   http://localhost:9095/metrics
    Kafka (host)     localhost:9092   topics: customer-events, customer-events-dlq

  The stream job picks up new events every ${DEMO_TRIGGER:-15 seconds}; the first batch appears
  about 30 seconds after Spark starts.

    $0 logs            follow pipeline logs
    $0 rerun-profile   publish a new profile version
    $0 stop | clean    stop (keep data) | stop and delete all data
EOF
    open_browser
    ;;
  logs)
    "${COMPOSE[@]}" logs -f snapshot profile-job stream-job viewer
    ;;
  rerun-profile)
    "${COMPOSE[@]}" run --rm profile-job
    ;;
  stop)
    "${COMPOSE[@]}" stop
    ;;
  clean)
    "${COMPOSE[@]}" down -v --remove-orphans
    ;;
  *)
    sed -n '2,12p' "$0"
    exit 1
    ;;
esac
