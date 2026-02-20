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

echo "Switching to k-global k8s context"
kubectl config use-context gke_ares-gpu-test_us-east4-a_ares-global
#echo "Deleting global Scheduler pod"
#kubectl delete -f $PWD/k8s/global/global-scheduler.yaml
#sleep 10
#echo "Deleted Global Scheduler Pod Successfully!"
echo "Creating Global Scheduler Pod!"
kubectl apply -f $PWD/k8s/global/global-scheduler.yaml
sleep 5
echo "Created Global Scheduler Pod Successfully!"

echo "Switching k-worker k8s context"
kubectl config use-context gke_ares-gpu-test_us-east4-c_ares-gpu-worker
#echo "Deleting Local Scheduler pod"
#kubectl delete -f $PWD/k8s/local/local-scheduler.yaml
#sleep 10
#echo "Deleted Local Scheduler Pod Successfully!"

echo "Creating Local Scheduler Pod!"
kubectl apply -f $PWD/k8s/local/local-scheduler.yaml
sleep 10
echo "Created Local Scheduler Pod!"

echo "Pods Deployed Successfully!"

echo "Switching k-worker k8s context in AWS"
kubectl config use-context arn:aws:eks:us-east-1:971422715802:cluster/ares-aws-gpu
kubectl label node ip-192-168-3-131.ec2.internal ares.gpu/type=A10G
kubectl label node ip-192-168-3-131.ec2.internal ares.gpu=true

echo "Creating Local Scheduler Pod in AWS"
kubectl apply -f $PWD/k8s/local/local-scheduler-aws.yaml
sleep 5
echo "Created Local Scheduler Pod!"