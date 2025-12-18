package kubernetes

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	_ "go.etcd.io/etcd/client/v3/kubernetes"
	"io"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sClient "k8s.io/client-go/kubernetes"
	k8sCoreClient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ============================================================================
// K8sClientImpl: Real Kubernetes client using client-go
// ============================================================================
// ENHANCEMENTS:
// 1. ✅ Wraps both custom interface AND standard kubernetes.Interface
// 2. ✅ Exposes real k8s.io client via GetKubernetesInterface()
// 3. ✅ Single source of truth for all K8s operations
// 4. ✅ GPU discovery and main.go use this wrapper
// ============================================================================

type K8sClientImpl struct {
	clientset k8sCoreClient.Interface // ✅ Real k8s.io client
	namespace string
	//log       *logger.Logger
}

// NewK8sClient: Create real K8s client (in-cluster or kubeconfig)
// ✅ ENHANCED: Now exposes both custom interface and real k8s.io client
func NewK8sClient(namespace string) (*K8sClientImpl, error) {

	//log := logger.Get()

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

	// ✅ NEW: Create standard kubernetes.Interface here
	clientset, err := k8sClient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create K8s clientset: %w", err)
	}

	if namespace == "" {
		namespace = "default"
	}

	return &K8sClientImpl{
		clientset: clientset,
		namespace: namespace,
		//log:       logger.Get(),
	}, nil
}

// ✅ NEW: GetKubernetesInterface() - Expose real k8s.io client
// This allows gpu_discovery and other components to use the standard client
//
// Usage in gpu_discovery:
//
//	k8sClientInterface := executor.GetKubernetesInterface()
//	gpuDiscovery := gpu.NewGPUDiscoveryWithK8s(redisClient, k8sClientInterface, nodeName)
//
// Usage in main.go:
//
//	k8sClientInterface := executor.GetKubernetesInterface()
//	gpuDiscovery := gpu.NewGPUDiscoveryWithK8s(redisClient, k8sClientInterface, nodeName)
func (kc *K8sClientImpl) GetKubernetesInterface() k8sClient.Interface {
	return kc.clientset
}

// ============================================================================
// POD OPERATIONS
// ============================================================================

// CreatePod: Actually create a Kubernetes Pod
func (kc *K8sClientImpl) CreatePod(ctx context.Context, podSpec *executor.PodSpec) (string, error) {

	//log.Info("Create Pod Entered: ")

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
func (kc *K8sClientImpl) GetPod(ctx context.Context, podName string) (*executor.PodInfo, error) {
	pod, err := kc.clientset.CoreV1().Pods(kc.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get pod: %w", err)
	}

	return &executor.PodInfo{
		PodName:   pod.Name,
		Namespace: pod.Namespace,
		Phase:     convertK8sPodPhase(pod.Status.Phase),
		CreatedAt: pod.CreationTimestamp.Time,
		Ready:     len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].Ready,
	}, nil
}

// DeletePod: Delete a Pod
func (kc *K8sClientImpl) DeletePod(ctx context.Context, podName string) error {
	return kc.clientset.CoreV1().Pods(kc.namespace).Delete(ctx, podName, metav1.DeleteOptions{})
}

// ListPods: List all Pods in namespace
func (kc *K8sClientImpl) ListPods(ctx context.Context) ([]*executor.PodInfo, error) {
	pods, err := kc.clientset.CoreV1().Pods(kc.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	var podInfos []*executor.PodInfo
	for _, pod := range pods.Items {
		podInfos = append(podInfos, &executor.PodInfo{
			PodName:   pod.Name,
			Namespace: pod.Namespace,
			Phase:     convertK8sPodPhase(pod.Status.Phase),
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

// GetPodMetrics: Get Pod metrics (placeholder for metrics-server integration)
func (kc *K8sClientImpl) GetPodMetrics(ctx context.Context, podName string) (map[string]interface{}, error) {
	// TODO: Integrate with metrics-server
	// For now, return empty metrics
	return map[string]interface{}{}, nil
}

// WatchPod: Watch Pod for status changes
func (kc *K8sClientImpl) WatchPod(ctx context.Context, podName string, callback func(*executor.PodInfo)) error {
	watcher, err := kc.clientset.CoreV1().Pods(kc.namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", podName),
	})
	if err != nil {
		return fmt.Errorf("failed to watch pod: %w", err)
	}
	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		pod := event.Object.(*corev1.Pod)
		callback(&executor.PodInfo{
			PodName:   pod.Name,
			Namespace: pod.Namespace,
			Phase:     convertK8sPodPhase(pod.Status.Phase),
		})
	}

	return nil
}

// ============================================================================
// TYPE CONVERSION HELPERS
// ============================================================================

// convertK8sPodPhase: Convert Kubernetes Pod phase to common.PodPhase
//
// Explanation:
// - corev1.PodPhase is from k8s.io/api/core/v1 (Kubernetes Pod phases)
// - common.PodPhase is from pkg/executor/common (our app's phases)
// - Both are string types, but have different constant values
// - This function maps between them safely
//
// Mapping:
//
//	corev1.PodPending   -> common.PhasePending
//	corev1.PodRunning   -> common.PhaseRunning
//	corev1.PodSucceeded -> common.PhaseSucceeded
//	corev1.PodFailed    -> common.PhaseFailed
//	(others)            -> common.PhaseUnknown
func convertK8sPodPhase(k8sPhase corev1.PodPhase) executor.PodPhase {
	switch k8sPhase {
	case corev1.PodPending:
		return executor.PhasePending
	case corev1.PodRunning:
		return executor.PhaseRunning
	case corev1.PodSucceeded:
		return executor.PhaseSucceeded
	case corev1.PodFailed:
		return executor.PhaseFailed
	default:
		return executor.PhaseUnknown
	}
}

// envMapToEnvVars: Convert map[string]string to []corev1.EnvVar
// Used when building Pod environment variables
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
