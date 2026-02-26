#!/usr/bin/env bash
#set -euo pipefail
#
#NAMESPACE="${NAMESPACE:-ares-system}"
#
## You set these per cluster:
#CLUSTER_ID="${CLUSTER_ID:-$(kubectl config current-context)}"
#CLOUD="${CLOUD:-unknown}"
#REGION="${REGION:-unknown}"
#ZONE="${ZONE:-unknown}"
#
#kubectl -n "$NAMESPACE" create configmap cluster-config \
#  --from-literal=cluster-id="$CLUSTER_ID" \
#  --from-literal=cloud="$CLOUD" \
#  --from-literal=region="$REGION" \
#  --from-literal=zone="$ZONE" \
#  --dry-run=client -o yaml | kubectl apply -f -

#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-ares-system}"

# Fetch current cluster context
CLUSTER_ID="${CLUSTER_ID:-$(kubectl config current-context)}"

# Cloud = AWS
CLOUD="aws"

# Region (fixed for your setup)
REGION="us-east-1"

# Auto-detect zone from first node (reliable way)
ZONE="$(kubectl get nodes -o jsonpath='{.items[0].metadata.labels.topology\.kubernetes\.io/zone}')"
ZONE="${ZONE:-unknown}"

echo "Cluster: $CLUSTER_ID"
echo "Cloud: $CLOUD"
echo "Region: $REGION"
echo "Zone: $ZONE"
echo "Namespace: $NAMESPACE"

kubectl -n "$NAMESPACE" create configmap cluster-config \
  --from-literal=cluster-id="$CLUSTER_ID" \
  --from-literal=cloud="$CLOUD" \
  --from-literal=region="$REGION" \
  --from-literal=zone="$ZONE" \
  --dry-run=client -o yaml | kubectl apply -f -