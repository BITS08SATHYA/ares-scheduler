package kubernetes

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	_ "go.etcd.io/etcd/client/v3/kubernetes"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sClient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned"
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
	clientset     k8sClient.Interface
	namespace     string
	metricsClient metricsv1beta1.Interface
}

// NewK8sClient: Create real K8s client (in-cluster or kubeconfig)
// ✅ ENHANCED: Now exposes both custom interface and real k8s.io client
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

	// ✅ NEW: Create standard kubernetes.Interface here
	clientset, err := k8sClient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create K8s clientset: %w", err)
	}

	if namespace == "" {
		namespace = "default"
	}

	// Initialize metrics-server client (best-effort — not all clusters have it)
	mc, mcErr := metricsv1beta1.NewForConfig(config)
	if mcErr != nil {
		logger.Get().Warn("Metrics-server client init failed (pod metrics unavailable): %v", mcErr)
	}

	return &K8sClientImpl{
		clientset:     clientset,
		namespace:     namespace,
		metricsClient: mc,
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

// CreatePod: Actually create a Kubernetes Pod
func (kc *K8sClientImpl) CreatePod(ctx context.Context, podSpec *executor.PodSpec) (string, error) {
	logger.Get().Info("Implementation --> Create Pod Entered")

	// Validate input
	if podSpec == nil || podSpec.PodName == "" {
		return "", fmt.Errorf("invalid pod spec: podName is required")
	}
	if podSpec.Image == "" {
		return "", fmt.Errorf("invalid pod spec: image is required")
	}

	// Build resource requirements (requests = limits for Guaranteed QoS)
	requests := corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dMi", podSpec.MemoryMB)),
		corev1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%dm", podSpec.CPUMillis)),
	}

	// Add GPU requests if specified
	if podSpec.GPUCount > 0 {
		requests[corev1.ResourceName("nvidia.com/gpu")] = resource.MustParse(fmt.Sprintf("%d", podSpec.GPUCount))
	}

	// Build env vars; inject GPU driver-lib hints when the pod requests a GPU
	envVars := podSpec.EnvVars
	if podSpec.GPUCount > 0 {
		envVars = mergeGPUDriverEnv(envVars)
	}

	// Build container spec
	container := corev1.Container{
		Name:            podSpec.PodName,
		Image:           podSpec.Image,
		ImagePullPolicy: corev1.PullPolicy(podSpec.ImagePullPolicy),
		Env:             envMapToEnvVars(envVars),
		Resources: corev1.ResourceRequirements{
			Requests: requests,
			Limits:   requests,
		},
	}

	// Add Command and Args if provided
	if len(podSpec.Command) > 0 {
		container.Command = podSpec.Command
	}
	if len(podSpec.Args) > 0 {
		container.Args = podSpec.Args
	}

	// Build tolerations - ALIGNED WITH DAEMONSET (OPTION 1: CATCH-ALL)
	tolerations := buildPodTolerations()

	// Attach GPU driver-lib hostPath mount when the pod requests a GPU.
	// The device-plugin provides /dev/nvidia* char devices via the GPU request,
	// but CUDA user-space libs (libcuda, libnvidia-ml) and nvidia-smi must be
	// bind-mounted from the host — nvidia-container-runtime/CDI injection is
	// not working on the target containerd on local microk8s.
	var podVolumes []corev1.Volume
	if podSpec.GPUCount > 0 {
		vol, mount := buildGPUDriverVolume()
		podVolumes = append(podVolumes, vol)
		container.VolumeMounts = append(container.VolumeMounts, mount)
	}

	// Build Pod object
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podSpec.PodName,
			Namespace: podSpec.Namespace,
			Labels:    podSpec.Labels,
		},
		Spec: corev1.PodSpec{
			Containers:    []corev1.Container{container},
			RestartPolicy: corev1.RestartPolicy(podSpec.RestartPolicy),
			NodeSelector: map[string]string{
				"kubernetes.io/hostname": podSpec.NodeID,
			},
			Tolerations: tolerations, // ✅ CRITICAL: Aligned with DaemonSet
			Volumes:     podVolumes,
		},
	}

	// Create Pod in Kubernetes
	created, err := kc.clientset.CoreV1().Pods(kc.namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		logger.Get().Error("Failed to create pod %s: %v", podSpec.PodName, err)
		return "", fmt.Errorf("failed to create pod %s: %w", podSpec.PodName, err)
	}

	logger.Get().Info("Pod created successfully: %s (namespace=%s, node=%s, gpuCount=%d)",
		created.Name, created.Namespace, podSpec.NodeID, podSpec.GPUCount,
	)

	return created.Name, nil
}

// buildPodTolerations: Build tolerations matching ares-local DaemonSet behavior
// This ensures job pods can schedule on ANY node where ares-local runs
func buildPodTolerations() []corev1.Toleration {
	tolerationSeconds := int64(300)

	return []corev1.Toleration{
		// ============================================
		// STANDARD KUBERNETES NODE CONDITIONS
		// ============================================
		// Tolerate temporarily unreachable nodes
		{
			Key:               "node.kubernetes.io/not-ready",
			Operator:          corev1.TolerationOpExists,
			Effect:            corev1.TaintEffectNoExecute,
			TolerationSeconds: &tolerationSeconds,
		},
		{
			Key:               "node.kubernetes.io/unreachable",
			Operator:          corev1.TolerationOpExists,
			Effect:            corev1.TaintEffectNoExecute,
			TolerationSeconds: &tolerationSeconds,
		},

		// ============================================
		// GPU-SPECIFIC TAINTS
		// ============================================
		// NVIDIA GPU taints (common in GPU clusters)
		{
			Key:      "nvidia.com/gpu",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
		{
			Key:      "nvidia.com/gpu",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},

		// ============================================
		// CLOUD PROVIDER GPU TAINTS
		// ============================================
		// GKE (Google Kubernetes Engine) GPU taints
		{
			Key:      "cloud.google.com/gke-accelerator",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
		{
			Key:      "cloud.google.com/gke-accelerator",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},

		// Generic accelerator taints (some clusters use this)
		{
			Key:      "accelerator",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
		{
			Key:      "accelerator",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},

		// ============================================
		// CATCH-ALL TOLERATIONS (OPTION 1)
		// ============================================
		// Tolerate ANY taint with NoSchedule effect
		// This matches ares-local DaemonSet behavior
		{
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
		// Tolerate ANY taint with NoExecute effect
		{
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoExecute,
		},
	}
}

// gpuDriverHostPath is the directory on each GPU node that aggregates
// NVIDIA driver libraries and nvidia-smi for containers to consume.
// Matches the ares-local DaemonSet's gpu.hostPath value — keep them in sync.
const gpuDriverHostPath = "/opt/nvidia"

// buildGPUDriverVolume returns the hostPath volume and container mount that
// expose the node's NVIDIA driver libs + nvidia-smi to a GPU pod. The char
// devices under /dev/nvidia* come from the device-plugin via the GPU resource
// request; this mount only covers user-space libs that nvidia-container-runtime
// would normally inject.
func buildGPUDriverVolume() (corev1.Volume, corev1.VolumeMount) {
	hostPathType := corev1.HostPathDirectory
	return corev1.Volume{
			Name: "ares-gpu-driver",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: gpuDriverHostPath,
					Type: &hostPathType,
				},
			},
		}, corev1.VolumeMount{
			Name:      "ares-gpu-driver",
			MountPath: gpuDriverHostPath,
			ReadOnly:  true,
		}
}

// mergeGPUDriverEnv returns a copy of envMap with LD_LIBRARY_PATH and PATH
// pre-set to point at the gpu driver mount. Caller-supplied values always win.
func mergeGPUDriverEnv(envMap map[string]string) map[string]string {
	out := make(map[string]string, len(envMap)+2)
	out["LD_LIBRARY_PATH"] = gpuDriverHostPath + "/lib64:" + gpuDriverHostPath + "/lib"
	out["PATH"] = gpuDriverHostPath + "/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	for k, v := range envMap {
		out[k] = v
	}
	return out
}

// envMapToEnvVars: Convert map to Kubernetes EnvVar slice
func envMapToEnvVars(envMap map[string]string) []corev1.EnvVar {
	if len(envMap) == 0 {
		return []corev1.EnvVar{}
	}

	envVars := make([]corev1.EnvVar, 0, len(envMap))
	for key, value := range envMap {
		envVars = append(envVars, corev1.EnvVar{
			Name:  key,
			Value: value,
		})
	}

	// Sort for deterministic output (helpful for testing/debugging)
	sort.Slice(envVars, func(i, j int) bool {
		return envVars[i].Name < envVars[j].Name
	})

	return envVars
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
	defer func() { _ = logs.Close() }()

	data, err := io.ReadAll(logs)
	return string(data), err
}

// GetPodMetrics queries the metrics-server for pod resource usage (CPU, memory).
// Returns empty map if metrics-server is unavailable (graceful degradation).
func (kc *K8sClientImpl) GetPodMetrics(ctx context.Context, podName string) (map[string]interface{}, error) {
	if kc.metricsClient == nil {
		return map[string]interface{}{}, nil
	}

	podMetrics, err := kc.metricsClient.MetricsV1beta1().PodMetricses(kc.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return map[string]interface{}{}, fmt.Errorf("metrics-server query failed: %w", err)
	}

	result := map[string]interface{}{}
	for _, container := range podMetrics.Containers {
		cpuUsage := container.Usage.Cpu()
		memUsage := container.Usage.Memory()
		result[container.Name+"_cpu_millicores"] = cpuUsage.MilliValue()
		result[container.Name+"_memory_bytes"] = memUsage.Value()
	}

	return result, nil
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
		pod, ok := event.Object.(*corev1.Pod)
		if !ok {
			continue // Skip non-Pod events (e.g., Status objects on error)
		}
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
