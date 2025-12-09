package kubernetes

import (
	"context"
	"fmt"
	"io"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// K8sClientImpl: Real Kubernetes client using client-go
type K8sClientImpl struct {
	clientset kubernetes.Interface
	namespace string
}

// NewK8sClient: Create real K8s client (in-cluster or kubeconfig)
func NewK8sClient(namespace string) (*K8sClientImpl, error) {
	var config *rest.Config
	var err error

	// Try in-cluster config first (for pods running in K8s)
	config, err = rest.InClusterConfig()
	if err != nil {
		// Fallback to kubeconfig (for local development)
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			home := os.Getenv("HOME")
			kubeconfig = fmt.Sprintf("%s/.kube/config", home)
		}

		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create K8s config: %w", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create K8s clientset: %w", err)
	}

	if namespace == "" {
		namespace = "default"
	}

	return &K8sClientImpl{
		clientset: clientset,
		namespace: namespace,
	}, nil
}

// CreatePod: Actually create a Kubernetes Pod
func (kc *K8sClientImpl) CreatePod(ctx context.Context, podSpec *PodSpec) (string, error) {
	if podSpec == nil || podSpec.PodName == "" {
		return "", fmt.Errorf("invalid pod spec")
	}

	// Build GPU requests
	requests := corev1.ResourceList{
		"memory": resource.MustParse(fmt.Sprintf("%dMi", podSpec.MemoryMB)),
		"cpu":    resource.MustParse(fmt.Sprintf("%dm", podSpec.CPUMillis)),
	}

	// Add GPU requests if specified
	if podSpec.GPUCount > 0 {
		requests["nvidia.com/gpu"] = resource.MustParse(fmt.Sprintf("%d", podSpec.GPUCount))
	}

	// Build Pod object
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podSpec.PodName,
			Namespace: podSpec.Namespace,
			Labels:    podSpec.Labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:            podSpec.PodName,
					Image:           podSpec.Image,
					ImagePullPolicy: corev1.PullPolicy(podSpec.ImagePullPolicy),
					Env:             envMapToEnvVars(podSpec.EnvVars),
					Resources: corev1.ResourceRequirements{
						Requests: requests,
						Limits:   requests,
					},
				},
			},
			RestartPolicy: corev1.RestartPolicy(podSpec.RestartPolicy),
			NodeSelector: map[string]string{
				"kubernetes.io/hostname": podSpec.NodeID,
			},
		},
	}

	// Create Pod in Kubernetes
	created, err := kc.clientset.CoreV1().Pods(kc.namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to create pod: %w", err)
	}

	return created.Name, nil
}

// GetPod: Get Pod information
func (kc *K8sClientImpl) GetPod(ctx context.Context, podName string) (*PodInfo, error) {
	pod, err := kc.clientset.CoreV1().Pods(kc.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get pod: %w", err)
	}

	return &PodInfo{
		PodName:   pod.Name,
		Namespace: pod.Namespace,
		Phase:     PodPhase(pod.Status.Phase),
		CreatedAt: pod.CreationTimestamp.Time,
		Ready:     len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].Ready,
	}, nil
}

// DeletePod: Delete a Pod
func (kc *K8sClientImpl) DeletePod(ctx context.Context, podName string) error {
	return kc.clientset.CoreV1().Pods(kc.namespace).Delete(ctx, podName, metav1.DeleteOptions{})
}

// ListPods: List all Pods in namespace
func (kc *K8sClientImpl) ListPods(ctx context.Context) ([]*PodInfo, error) {
	pods, err := kc.clientset.CoreV1().Pods(kc.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	var podInfos []*PodInfo
	for _, pod := range pods.Items {
		podInfos = append(podInfos, &PodInfo{
			PodName:   pod.Name,
			Namespace: pod.Namespace,
			Phase:     PodPhase(pod.Status.Phase),
		})
	}

	return podInfos, nil
}

// GetPodLogs: Get logs from Pod
func (kc *K8sClientImpl) GetPodLogs(ctx context.Context, podName string) (string, error) {
	req := kc.clientset.CoreV1().Pods(kc.namespace).GetLogs(podName, &corev1.PodLogOptions{})
	logs, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get logs: %w", err)
	}
	defer logs.Close()

	data, err := io.ReadAll(logs)
	return string(data), err
}

// WatchPod: Watch Pod for status changes
func (kc *K8sClientImpl) WatchPod(ctx context.Context, podName string, callback func(*PodInfo)) error {
	watcher, err := kc.clientset.CoreV1().Pods(kc.namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", podName),
	})
	if err != nil {
		return fmt.Errorf("failed to watch pod: %w", err)
	}
	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		pod := event.Object.(*corev1.Pod)
		callback(&PodInfo{
			PodName:   pod.Name,
			Namespace: pod.Namespace,
			Phase:     PodPhase(pod.Status.Phase),
		})
	}

	return nil
}

// Helper to convert env map to K8s EnvVar array
func envMapToEnvVars(envMap map[string]string) []corev1.EnvVar {
	var envVars []corev1.EnvVar
	for key, value := range envMap {
		envVars = append(envVars, corev1.EnvVar{
			Name:  key,
			Value: value,
		})
	}
	return envVars
}
