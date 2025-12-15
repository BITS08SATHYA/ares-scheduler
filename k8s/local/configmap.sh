#!/usr/bin/zsh

# Get full name from kubectl config
FULL_CLUSTER_NAME=$(kubectl config view --minify -o jsonpath='{.clusters[0].name}')

# Parse components
IFS='_' read -ra PARTS <<< "$FULL_CLUSTER_NAME"
GCP_PROJECT="${PARTS[1]}"
REGION="${PARTS[2]}"
CLUSTER_SHORT_NAME="${PARTS[3]}"

# Create ConfigMap with BOTH full and short names
kubectl create configmap cluster-config \
  --from-literal=cluster-name-full="$FULL_CLUSTER_NAME" \
  --from-literal=cluster-name-short="$CLUSTER_SHORT_NAME" \
  --from-literal=gcp-project="$GCP_PROJECT" \
  --from-literal=region="$REGION" \
  --namespace ares-system \
  --dry-run=client \
  -o yaml | kubectl apply -f -