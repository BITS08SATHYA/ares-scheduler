#!/usr/bin/zsh

set -e

echo "Building Docker Images of global Scheduler"
# Building docker images for global and local schedulers
docker build -f Dockerfile.global -t us-east4-docker.pkg.dev/ares-gpu-test/ares-scheduler/ares-scheduler-global:latest .
docker push us-east4-docker.pkg.dev/ares-gpu-test/ares-scheduler/ares-scheduler-global:latest

echo "Finished building and pushing image to global scheduler repo!"

echo "Building Docker Images of local Scheduler"
# Building docker images for global and local schedulers
docker build -f Dockerfile.local -t us-east4-docker.pkg.dev/ares-gpu-test/ares-scheduler/ares-scheduler-local:latest .
docker push us-east4-docker.pkg.dev/ares-gpu-test/ares-scheduler/ares-scheduler-local:latest

echo "Finished building and pushing image to local scheduler repo!"

echo "Deleting global Scheduler pod"
k-global
k apply -f $PWD/k8s/global/global-scheduler.yaml

sleep 10

echo "Deleted Global Scheduler Pod Successfully!"

echo "Deleting Local Scheduler pod"
k-worker
k apply -f $PWD/k8s/local/local-scheduler.yaml

sleep 10

echo "Deleted Local Scheduler Pod Successfully!"

