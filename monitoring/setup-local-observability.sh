#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)
OBS_DIR="${SCRIPT_DIR}/local-observability"
SOURCE_DASHBOARD="${ROOT_DIR}/monitoring/grafana-distributed-dashboard.json"
TARGET_DASHBOARD="${OBS_DIR}/grafana/dashboards/spice-distributed.json"
METRICS_TARGET="${SPICE_METRICS_TARGET:-host.docker.internal:9090}"
ACTION="${1:-}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but not installed." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose plugin is required but not available." >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to prepare the Grafana dashboard." >&2
  exit 1
fi

if [[ "${ACTION}" == "clean" ]]; then
  if [[ -f "${OBS_DIR}/docker-compose.yml" ]]; then
    docker compose -f "${OBS_DIR}/docker-compose.yml" down --volumes --remove-orphans
  fi
  rm -rf "${OBS_DIR}"
  echo "Local observability stack removed."
  exit 0
fi

if [[ -n "${ACTION}" ]]; then
  echo "Unknown action: ${ACTION}" >&2
  echo "Usage: $(basename "$0") [clean]" >&2
  exit 1
fi

mkdir -p \
  "${OBS_DIR}/grafana/dashboards" \
  "${OBS_DIR}/grafana/provisioning/dashboards" \
  "${OBS_DIR}/grafana/provisioning/datasources"

cat <<'EOF' > "${OBS_DIR}/docker-compose.yml"
services:
  prometheus:
    image: prom/prometheus:v2.54.1
    restart: unless-stopped
    ports:
      - "9091:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
      - --storage.tsdb.retention.time=24h
    extra_hosts:
      - "host.docker.internal:host-gateway"

  grafana:
    image: grafana/grafana:11.3.1
    restart: unless-stopped
    ports:
      - "3000:3000"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
      - grafana-data:/var/lib/grafana
    depends_on:
      - prometheus

volumes:
  prometheus-data: {}
  grafana-data: {}
EOF

cat <<EOF > "${OBS_DIR}/prometheus.yml"
global:
  scrape_interval: 10s
  scrape_timeout: 5s
  evaluation_interval: 10s

scrape_configs:
  - job_name: spice-cluster
    metrics_path: /
    params:
      scope:
        - cluster
    static_configs:
      - targets:
          - ${METRICS_TARGET}
EOF

cat <<'EOF' > "${OBS_DIR}/grafana/provisioning/datasources/prometheus.yml"
apiVersion: 1

datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
EOF

cat <<'EOF' > "${OBS_DIR}/grafana/provisioning/dashboards/dashboards.yml"
apiVersion: 1

providers:
  - name: spice-distributed
    orgId: 1
    folder: Spice
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /var/lib/grafana/dashboards
      foldersFromFilesStructure: false
EOF

python3 - <<'PY' "${SOURCE_DASHBOARD}" "${TARGET_DASHBOARD}"
import json
import sys

source_path, target_path = sys.argv[1], sys.argv[2]
with open(source_path, "r", encoding="utf-8") as handle:
    dashboard = json.load(handle)


def patch_datasources(value):
    if isinstance(value, dict):
        if value.get("datasource") and isinstance(value["datasource"], dict):
            if value["datasource"].get("uid") == "${DS_PROMETHEUS}":
                value["datasource"]["uid"] = "prometheus"
        for child in value.values():
            patch_datasources(child)
    elif isinstance(value, list):
        for child in value:
            patch_datasources(child)


patch_datasources(dashboard)
with open(target_path, "w", encoding="utf-8") as handle:
    json.dump(dashboard, handle, indent=2)
    handle.write("\n")
PY

docker compose -f "${OBS_DIR}/docker-compose.yml" up -d

echo "Grafana: http://localhost:3000 (admin/admin by default)"
echo "Prometheus: http://localhost:9091"
