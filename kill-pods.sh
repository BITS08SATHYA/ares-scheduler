#!/usr/bin/zsh

echo "Switching to k-global k8s context"
kubectl config use-context gke_ares-gpu-test_us-east4-a_ares-global
echo "Deleting global Scheduler pod"
kubectl delete -f $PWD/k8s/global/global-scheduler.yaml
sleep 3
echo "Deleted Global Scheduler Pod Successfully!"

echo "Switching k-worker k8s context"
kubectl config use-context gke_ares-gpu-test_us-east4-c_ares-gpu-worker
echo "Deleting Local Scheduler pod"
kubectl delete -f $PWD/k8s/local/local-scheduler.yaml
sleep 3
echo "Deleted Local Scheduler Pod Successfully!"

echo "Pods Deleted Successfully!"