# Ares Scheduler — Build, Test, Docker & Helm Makefile
# Load .env if present
-include .env

# ─── Variables ────────────────────────────────────────────────────────────────
REGISTRY       ?=
TAG            ?= latest
CLUSTER_ID     ?=
CONTROL_PLANE_ADDR ?=
REDIS_ADDR     ?=
ETCD_ADDR      ?=
LOCAL_EXTERNAL_ADDR ?=
REGION         ?=
ZONE           ?=
GLOBAL_K8S_CONTEXT ?=
LOCAL_K8S_CONTEXT  ?=
GRAFANA_ADMIN_PASSWORD ?=

GLOBAL_IMAGE   = $(REGISTRY)/ares-scheduler-global:$(TAG)
LOCAL_IMAGE    = $(REGISTRY)/ares-scheduler-local:$(TAG)
GOFLAGS        = CGO_ENABLED=0 GOOS=linux

.DEFAULT_GOAL := help

# ─── Help ─────────────────────────────────────────────────────────────────────
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: info
info: ## Print current config values
	@echo "REGISTRY            = $(REGISTRY)"
	@echo "TAG                 = $(TAG)"
	@echo "CLUSTER_ID          = $(CLUSTER_ID)"
	@echo "CONTROL_PLANE_ADDR  = $(CONTROL_PLANE_ADDR)"
	@echo "REDIS_ADDR          = $(REDIS_ADDR)"
	@echo "ETCD_ADDR           = $(ETCD_ADDR)"
	@echo "LOCAL_EXTERNAL_ADDR = $(LOCAL_EXTERNAL_ADDR)"
	@echo "REGION              = $(REGION)"
	@echo "ZONE                = $(ZONE)"

# ─── Development ──────────────────────────────────────────────────────────────
.PHONY: build build-global build-local build-benchmark
build: build-global build-local build-benchmark ## Build all binaries

build-global: ## Build global scheduler
	$(GOFLAGS) go build -o bin/ares-global ./cmd/global

build-local: ## Build local scheduler
	$(GOFLAGS) go build -o bin/ares-local ./cmd/local

build-benchmark: ## Build benchmark tool
	$(GOFLAGS) go build -o bin/ares-benchmark ./cmd/benchmark

.PHONY: test
test: ## Run unit tests
	go test ./tests/unit/...

.PHONY: fmt
fmt: ## Format Go source files
	gofmt -w .

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin/
	rm -f ares-global ares-local ares-benchmark benchmark_results.json

.PHONY: dev
dev: ## Start local etcd + redis (docker-compose)
	docker-compose up -d

.PHONY: dev-down
dev-down: ## Stop local etcd + redis
	docker-compose down -v

.PHONY: run-global
run-global: ## Run global scheduler locally
	go run cmd/global/main.go \
		-gateway.port 8080 \
		-etcd.endpoint $(ETCD_ADDR) \
		-redis.addr $(REDIS_ADDR) \
		-enable-coordinator true \
		-enable-metrics true

.PHONY: run-local
run-local: ## Run local scheduler locally (requires CLUSTER_ID)
	@test -n "$(CLUSTER_ID)" || (echo "ERROR: CLUSTER_ID is required"; exit 1)
	ARES_CLUSTER_ID=$(CLUSTER_ID) go run cmd/local/main.go \
		-cluster-id $(CLUSTER_ID) \
		-port 9090 \
		-redis $(REDIS_ADDR) \
		-control-plane $(CONTROL_PLANE_ADDR)

.PHONY: benchmark
benchmark: ## Run all benchmark suites
	go run cmd/benchmark/main.go -control-plane $(CONTROL_PLANE_ADDR) -suite all

.PHONY: benchmark-suite
SUITE ?= stress
benchmark-suite: ## Run a single benchmark suite (SUITE=stress|exactlyonce|failure|gang|drf|priority|multicluster|chaos)
	go run cmd/benchmark/main.go -control-plane $(CONTROL_PLANE_ADDR) -suite $(SUITE)

# ─── Docker ───────────────────────────────────────────────────────────────────
.PHONY: docker-build docker-build-global docker-build-local
docker-build: docker-build-global docker-build-local ## Build both Docker images

docker-build-global: ## Build global scheduler Docker image
	@test -n "$(REGISTRY)" || (echo "ERROR: REGISTRY is required"; exit 1)
	docker build -f Dockerfile.global -t $(GLOBAL_IMAGE) .

docker-build-local: ## Build local scheduler Docker image
	@test -n "$(REGISTRY)" || (echo "ERROR: REGISTRY is required"; exit 1)
	docker build -f Dockerfile.local -t $(LOCAL_IMAGE) .

.PHONY: docker-push docker-push-global docker-push-local
docker-push: docker-push-global docker-push-local ## Push both Docker images

docker-push-global: ## Push global scheduler Docker image
	@test -n "$(REGISTRY)" || (echo "ERROR: REGISTRY is required"; exit 1)
	docker push $(GLOBAL_IMAGE)

docker-push-local: ## Push local scheduler Docker image
	@test -n "$(REGISTRY)" || (echo "ERROR: REGISTRY is required"; exit 1)
	docker push $(LOCAL_IMAGE)

# ─── Helm Deployment ─────────────────────────────────────────────────────────
HELM_NS = ares-system
CLUSTER ?=

define helm_ctx
$(if $(1),--kube-context $(1),)
endef

define kubectl_ctx
$(if $(1),--context $(1),)
endef

# Extract kubeContext from a cluster values file
define cluster_ctx_from_file
$(shell grep '^kubeContext:' $(1) 2>/dev/null | awk '{print $$2}' | tr -d "'\"")
endef

# ── Global scheduler ────────────────────────────────────────────────────────

.PHONY: deploy-global
deploy-global: ## Deploy global scheduler (CLUSTER=global or .env vars)
	@test -n "$(REGISTRY)" || (echo "ERROR: REGISTRY is required"; exit 1)
ifdef CLUSTER
	@test -f clusters/$(CLUSTER).yaml || (echo "ERROR: clusters/$(CLUSTER).yaml not found"; exit 1)
	$(eval _CTX := $(call cluster_ctx_from_file,clusters/$(CLUSTER).yaml))
	helm install ares-global ./charts/ares-global \
		$(if $(_CTX),--kube-context $(_CTX),) \
		--namespace $(HELM_NS) --create-namespace \
		--values clusters/$(CLUSTER).yaml \
		--set image.repository=$(REGISTRY)/ares-scheduler-global \
		--set image.tag=$(TAG)
else
	@test -n "$(GRAFANA_ADMIN_PASSWORD)" || (echo "ERROR: GRAFANA_ADMIN_PASSWORD is required"; exit 1)
	helm install ares-global ./charts/ares-global \
		$(call helm_ctx,$(GLOBAL_K8S_CONTEXT)) \
		--namespace $(HELM_NS) --create-namespace \
		--set image.repository=$(REGISTRY)/ares-scheduler-global \
		--set image.tag=$(TAG) \
		--set grafana.adminPassword=$(GRAFANA_ADMIN_PASSWORD)
endif

.PHONY: upgrade-global
upgrade-global: ## Upgrade global scheduler (CLUSTER=global or .env vars)
	@test -n "$(REGISTRY)" || (echo "ERROR: REGISTRY is required"; exit 1)
ifdef CLUSTER
	@test -f clusters/$(CLUSTER).yaml || (echo "ERROR: clusters/$(CLUSTER).yaml not found"; exit 1)
	$(eval _CTX := $(call cluster_ctx_from_file,clusters/$(CLUSTER).yaml))
	helm upgrade ares-global ./charts/ares-global \
		$(if $(_CTX),--kube-context $(_CTX),) \
		--namespace $(HELM_NS) \
		--values clusters/$(CLUSTER).yaml \
		--set image.repository=$(REGISTRY)/ares-scheduler-global \
		--set image.tag=$(TAG)
else
	@test -n "$(GRAFANA_ADMIN_PASSWORD)" || (echo "ERROR: GRAFANA_ADMIN_PASSWORD is required"; exit 1)
	helm upgrade ares-global ./charts/ares-global \
		$(call helm_ctx,$(GLOBAL_K8S_CONTEXT)) \
		--namespace $(HELM_NS) \
		--set image.repository=$(REGISTRY)/ares-scheduler-global \
		--set image.tag=$(TAG) \
		--set grafana.adminPassword=$(GRAFANA_ADMIN_PASSWORD)
endif

.PHONY: undeploy-global
undeploy-global: ## Uninstall global scheduler (CLUSTER=global or .env vars)
ifdef CLUSTER
	@test -f clusters/$(CLUSTER).yaml || (echo "ERROR: clusters/$(CLUSTER).yaml not found"; exit 1)
	$(eval _CTX := $(call cluster_ctx_from_file,clusters/$(CLUSTER).yaml))
	helm uninstall ares-global \
		$(if $(_CTX),--kube-context $(_CTX),) \
		--namespace $(HELM_NS)
else
	helm uninstall ares-global \
		$(call helm_ctx,$(GLOBAL_K8S_CONTEXT)) \
		--namespace $(HELM_NS)
endif

# ── Local scheduler (per worker cluster) ────────────────────────────────────

.PHONY: deploy-local
deploy-local: ## Deploy local scheduler (CLUSTER=<name> or .env vars)
	@test -n "$(REGISTRY)" || (echo "ERROR: REGISTRY is required"; exit 1)
ifdef CLUSTER
	@test -f clusters/$(CLUSTER).yaml || (echo "ERROR: clusters/$(CLUSTER).yaml not found"; exit 1)
	$(eval _CTX := $(call cluster_ctx_from_file,clusters/$(CLUSTER).yaml))
	helm install ares-local ./charts/ares-local \
		$(if $(_CTX),--kube-context $(_CTX),) \
		--namespace $(HELM_NS) --create-namespace \
		--values clusters/$(CLUSTER).yaml \
		--set image.repository=$(REGISTRY)/ares-scheduler-local \
		--set image.tag=$(TAG)
else
	@test -n "$(CLUSTER_ID)" || (echo "ERROR: CLUSTER_ID is required"; exit 1)
	@test -n "$(LOCAL_EXTERNAL_ADDR)" || (echo "ERROR: LOCAL_EXTERNAL_ADDR is required"; exit 1)
	@test -n "$(CONTROL_PLANE_ADDR)" || (echo "ERROR: CONTROL_PLANE_ADDR is required"; exit 1)
	@test -n "$(REDIS_ADDR)" || (echo "ERROR: REDIS_ADDR is required"; exit 1)
	@test -n "$(ETCD_ADDR)" || (echo "ERROR: ETCD_ADDR is required"; exit 1)
	helm install ares-local ./charts/ares-local \
		$(call helm_ctx,$(LOCAL_K8S_CONTEXT)) \
		--namespace $(HELM_NS) --create-namespace \
		--set image.repository=$(REGISTRY)/ares-scheduler-local \
		--set image.tag=$(TAG) \
		--set global.controlPlane=$(CONTROL_PLANE_ADDR) \
		--set global.redis=$(REDIS_ADDR) \
		--set global.etcd=$(ETCD_ADDR) \
		--set cluster.id=$(CLUSTER_ID) \
		--set cluster.externalAddr=$(LOCAL_EXTERNAL_ADDR) \
		--set cluster.region=$(REGION) \
		--set cluster.zone=$(ZONE)
endif

.PHONY: upgrade-local
upgrade-local: ## Upgrade local scheduler (CLUSTER=<name> or .env vars)
	@test -n "$(REGISTRY)" || (echo "ERROR: REGISTRY is required"; exit 1)
ifdef CLUSTER
	@test -f clusters/$(CLUSTER).yaml || (echo "ERROR: clusters/$(CLUSTER).yaml not found"; exit 1)
	$(eval _CTX := $(call cluster_ctx_from_file,clusters/$(CLUSTER).yaml))
	helm upgrade ares-local ./charts/ares-local \
		$(if $(_CTX),--kube-context $(_CTX),) \
		--namespace $(HELM_NS) \
		--values clusters/$(CLUSTER).yaml \
		--set image.repository=$(REGISTRY)/ares-scheduler-local \
		--set image.tag=$(TAG)
else
	helm upgrade ares-local ./charts/ares-local \
		$(call helm_ctx,$(LOCAL_K8S_CONTEXT)) \
		--namespace $(HELM_NS) \
		--set image.repository=$(REGISTRY)/ares-scheduler-local \
		--set image.tag=$(TAG) \
		--set global.controlPlane=$(CONTROL_PLANE_ADDR) \
		--set global.redis=$(REDIS_ADDR) \
		--set global.etcd=$(ETCD_ADDR) \
		--set cluster.id=$(CLUSTER_ID) \
		--set cluster.externalAddr=$(LOCAL_EXTERNAL_ADDR) \
		--set cluster.region=$(REGION) \
		--set cluster.zone=$(ZONE)
endif

.PHONY: undeploy-local
undeploy-local: ## Uninstall local scheduler (CLUSTER=<name> or .env vars)
ifdef CLUSTER
	@test -f clusters/$(CLUSTER).yaml || (echo "ERROR: clusters/$(CLUSTER).yaml not found"; exit 1)
	$(eval _CTX := $(call cluster_ctx_from_file,clusters/$(CLUSTER).yaml))
	helm uninstall ares-local \
		$(if $(_CTX),--kube-context $(_CTX),) \
		--namespace $(HELM_NS)
else
	helm uninstall ares-local \
		$(call helm_ctx,$(LOCAL_K8S_CONTEXT)) \
		--namespace $(HELM_NS)
endif

# ── Teardown ────────────────────────────────────────────────────────────────

.PHONY: teardown-local
teardown-local: ## Remove local scheduler and cleanup namespace (CLUSTER=<name>)
ifdef CLUSTER
	@test -f clusters/$(CLUSTER).yaml || (echo "ERROR: clusters/$(CLUSTER).yaml not found"; exit 1)
	$(eval _CTX := $(call cluster_ctx_from_file,clusters/$(CLUSTER).yaml))
	-helm uninstall ares-local \
		$(if $(_CTX),--kube-context $(_CTX),) \
		--namespace $(HELM_NS)
	kubectl delete namespace $(HELM_NS) $(if $(_CTX),--context $(_CTX),) --ignore-not-found
else
	-helm uninstall ares-local \
		$(call helm_ctx,$(LOCAL_K8S_CONTEXT)) \
		--namespace $(HELM_NS)
	kubectl delete namespace $(HELM_NS) $(call kubectl_ctx,$(LOCAL_K8S_CONTEXT)) --ignore-not-found
endif

.PHONY: teardown-global
teardown-global: ## Remove global scheduler and cleanup namespace
	-helm uninstall ares-global \
		$(call helm_ctx,$(GLOBAL_K8S_CONTEXT)) \
		--namespace $(HELM_NS)
	kubectl delete namespace $(HELM_NS) $(call kubectl_ctx,$(GLOBAL_K8S_CONTEXT)) --ignore-not-found

.PHONY: teardown-all
teardown-all: ## Remove everything — all worker clusters then global
	@echo "Tearing down all worker clusters..."
	@ls clusters/*.yaml 2>/dev/null | while read f; do \
		name=$$(basename "$$f" .yaml); \
		case "$$name" in *-example*|*global*) continue;; esac; \
		ctx=$$(grep '^kubeContext:' "$$f" 2>/dev/null | awk '{print $$2}' | tr -d "'\""); \
		echo "  Removing ares-local from $$name..."; \
		helm uninstall ares-local $${ctx:+--kube-context $$ctx} --namespace $(HELM_NS) 2>/dev/null || true; \
		kubectl delete namespace $(HELM_NS) $${ctx:+--context $$ctx} --ignore-not-found 2>/dev/null || true; \
	done
	@echo "Tearing down global scheduler..."
	@-helm uninstall ares-global \
		$(call helm_ctx,$(GLOBAL_K8S_CONTEXT)) \
		--namespace $(HELM_NS) 2>/dev/null || true
	@kubectl delete namespace $(HELM_NS) $(call kubectl_ctx,$(GLOBAL_K8S_CONTEXT)) --ignore-not-found

# ── Cluster management ──────────────────────────────────────────────────────

.PHONY: get-global-ips
get-global-ips: ## Print global scheduler external IPs (uses GLOBAL_K8S_CONTEXT)
	@echo "Fetching external IPs from ares-system services..."
	@CP=$$(kubectl get svc ares-global -n $(HELM_NS) $(call kubectl_ctx,$(GLOBAL_K8S_CONTEXT)) -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null); \
	REDIS=$$(kubectl get svc redis-external -n $(HELM_NS) $(call kubectl_ctx,$(GLOBAL_K8S_CONTEXT)) -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null); \
	ETCD=$$(kubectl get svc etcd-external -n $(HELM_NS) $(call kubectl_ctx,$(GLOBAL_K8S_CONTEXT)) -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null); \
	echo ""; \
	echo "CONTROL_PLANE_ADDR=http://$${CP}:8080"; \
	echo "REDIS_ADDR=$${REDIS}:6379"; \
	echo "ETCD_ADDR=$${ETCD}:2379"; \
	if [ -z "$$CP" ] || [ -z "$$REDIS" ] || [ -z "$$ETCD" ]; then \
		echo ""; \
		echo "WARNING: Some IPs are still pending. Wait and retry."; \
	fi

.PHONY: init-cluster
init-cluster: ## Generate a worker cluster config (CLUSTER=<name>)
	@test -n "$(CLUSTER)" || (echo "ERROR: CLUSTER is required — e.g. make init-cluster CLUSTER=gke-gpu-worker"; exit 1)
	@test ! -f clusters/$(CLUSTER).yaml || (echo "ERROR: clusters/$(CLUSTER).yaml already exists"; exit 1)
	@mkdir -p clusters
	@CP=$$(kubectl get svc ares-global -n $(HELM_NS) $(call kubectl_ctx,$(GLOBAL_K8S_CONTEXT)) -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null); \
	REDIS=$$(kubectl get svc redis-external -n $(HELM_NS) $(call kubectl_ctx,$(GLOBAL_K8S_CONTEXT)) -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null); \
	ETCD=$$(kubectl get svc etcd-external -n $(HELM_NS) $(call kubectl_ctx,$(GLOBAL_K8S_CONTEXT)) -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null); \
	if [ -z "$$CP" ] || [ -z "$$REDIS" ] || [ -z "$$ETCD" ]; then \
		echo "ERROR: Global IPs not ready yet. Run 'make get-global-ips' to check."; exit 1; \
	fi; \
	CTX=$$(kubectl config current-context 2>/dev/null); \
	REGION=$$(kubectl get nodes -o jsonpath='{.items[0].metadata.labels.topology\.kubernetes\.io/region}' 2>/dev/null); \
	ZONE=$$(kubectl get nodes -o jsonpath='{.items[0].metadata.labels.topology\.kubernetes\.io/zone}' 2>/dev/null); \
	PROVIDER="unknown"; \
	NODE_SELECTOR="ares.ai/gpu: \"true\""; \
	GPU_HOST_PATH="/opt/nvidia"; \
	if echo "$$CTX" | grep -q "^gke_"; then \
		PROVIDER="gke"; \
	elif echo "$$CTX" | grep -q "eks" || echo "$$CTX" | grep -q "\.eksctl\.io"; then \
		PROVIDER="eks"; \
		NODE_SELECTOR="nvidia.com/gpu.present: \"true\""; \
		GPU_HOST_PATH="/usr/local/nvidia"; \
	fi; \
	printf '%s\n' \
		"# Worker cluster: $(CLUSTER) ($$PROVIDER)" \
		"# Generated from live cluster info" \
		"" \
		"kubeContext: $$CTX" \
		"" \
		"global:" \
		"  controlPlane: http://$$CP:8080" \
		"  redis: $$REDIS:6379" \
		"  etcd: $$ETCD:2379" \
		"" \
		"cluster:" \
		"  id: $(CLUSTER)" \
		'  externalAddr: ""   # set after deploy — make get-local-ip CLUSTER=$(CLUSTER)' \
		"  region: $$REGION" \
		"  zone: $$ZONE" \
		"" \
		"nodeSelector:" \
		"  $$NODE_SELECTOR" \
		"" \
		"gpu:" \
		"  hostPath: $$GPU_HOST_PATH" \
		> clusters/$(CLUSTER).yaml; \
	echo "Created clusters/$(CLUSTER).yaml ($$PROVIDER)"; \
	echo "  kubeContext:    $$CTX"; \
	echo "  region:         $${REGION:-(not detected)}"; \
	echo "  zone:           $${ZONE:-(not detected)}"; \
	echo "  nodeSelector:   $$NODE_SELECTOR"; \
	echo "  gpu.hostPath:   $$GPU_HOST_PATH"; \
	echo "  global IPs:     pre-filled"; \
	echo ""; \
	echo "Only externalAddr remains — run after deploy:"; \
	echo "  make get-local-ip CLUSTER=$(CLUSTER)"

.PHONY: get-local-ip
get-local-ip: ## Get local scheduler external IP and update cluster file (CLUSTER=<name>)
ifdef CLUSTER
	@test -f clusters/$(CLUSTER).yaml || (echo "ERROR: clusters/$(CLUSTER).yaml not found"; exit 1)
	$(eval _CTX := $(call cluster_ctx_from_file,clusters/$(CLUSTER).yaml))
	@IP=$$(kubectl get svc ares-local-external -n $(HELM_NS) $(if $(_CTX),--context $(_CTX),) -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null); \
	if [ -z "$$IP" ]; then \
		echo "WARNING: ares-local external IP not ready yet. Wait and retry."; \
	else \
		sed -i 's|  externalAddr:.*|  externalAddr: http://'"$$IP"':9090|' clusters/$(CLUSTER).yaml; \
		echo "Updated clusters/$(CLUSTER).yaml:"; \
		echo "  externalAddr: http://$$IP:9090"; \
		echo ""; \
		echo "Run 'make upgrade-local CLUSTER=$(CLUSTER)' to apply."; \
	fi
else
	@IP=$$(kubectl get svc ares-local-external -n $(HELM_NS) -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null); \
	if [ -z "$$IP" ]; then \
		echo "WARNING: ares-local external IP not ready yet. Wait and retry."; \
	else \
		echo "LOCAL_EXTERNAL_ADDR=http://$$IP:9090"; \
	fi
endif

.PHONY: list-clusters
list-clusters: ## List available cluster configs in clusters/
	@echo "Available cluster configs:"
	@ls clusters/*.yaml 2>/dev/null | while read f; do \
		name=$$(basename "$$f" .yaml); \
		ctx=$$(grep '^kubeContext:' "$$f" 2>/dev/null | awk '{print $$2}' | tr -d "'\""); \
		id=$$(grep '  id:' "$$f" 2>/dev/null | head -1 | awk '{print $$2}' | tr -d "'\""); \
		printf "  \033[36m%-20s\033[0m context=%-40s cluster-id=%s\n" "$$name" "$${ctx:-(default)}" "$${id:--}"; \
	done || echo "  (none — copy clusters/worker-example.yaml to get started)"

.PHONY: status
status: ## Show ares-system pods
	kubectl get pods -n $(HELM_NS) -o wide

.PHONY: logs-global
logs-global: ## Tail global scheduler logs
	kubectl logs -n $(HELM_NS) -l app=ares-global -f --tail=100

.PHONY: logs-local
logs-local: ## Tail local scheduler logs
	kubectl logs -n $(HELM_NS) -l app=ares-local -f --tail=100

.PHONY: logs-prometheus
logs-prometheus: ## Tail Prometheus logs
	kubectl logs -n $(HELM_NS) -l app=prometheus -f --tail=100

.PHONY: logs-grafana
logs-grafana: ## Tail Grafana logs
	kubectl logs -n $(HELM_NS) -l app=grafana -f --tail=100
