#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-ares-system}"

# You set these per cluster:
CLUSTER_ID="${CLUSTER_ID:-$(kubectl config current-context)}"
CLOUD="${CLOUD:-unknown}"
REGION="${REGION:-unknown}"
ZONE="${ZONE:-unknown}"

kubectl -n "$NAMESPACE" create configmap cluster-config \
  --from-literal=cluster-id="$CLUSTER_ID" \
  --from-literal=cloud="$CLOUD" \
  --from-literal=region="$REGION" \
  --from-literal=zone="$ZONE" \
  --dry-run=client -o yaml | kubectl apply -f -