package dashboard

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/credentials"
	"dolphindb.io/operator/internal/ddbclient"
	"dolphindb.io/operator/internal/exposure"
	"dolphindb.io/operator/internal/migration"
)

//go:embed ui/*
var uiFS embed.FS

// Server provides the HTTP dashboard and REST API
type Server struct {
	client           client.Client
	kubeClient       kubernetes.Interface
	listen           string
	namespace        string
	fingerprintCache map[string]string
	fpMu             sync.RWMutex
}

func NewServer(cli client.Client, kubeClient kubernetes.Interface, listen string, defaultNamespace string) *Server {
	return &Server{
		client:           cli,
		kubeClient:       kubeClient,
		listen:           listen,
		namespace:        defaultNamespace,
		fingerprintCache: make(map[string]string),
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// API Routes
	mux.HandleFunc("/api/healthz", s.handleHealthz)
	mux.HandleFunc("/api/namespaces", s.handleNamespaces)
	mux.HandleFunc("/api/storageclasses", s.handleStorageClasses)

	mux.HandleFunc("/api/clusters", s.handleClusters)
	mux.HandleFunc("/api/clusters/", s.handleClusterSubRoutes)

	mux.HandleFunc("/api/nodes/", s.handleNodeSubRoutes)

	mux.HandleFunc("/api/operations", s.handleOperations)
	mux.HandleFunc("/api/operations/", s.handleOperationSubRoutes)

	mux.HandleFunc("/api/backups", s.handleBackups)
	mux.HandleFunc("/api/backups/", s.handleBackupSubRoutes)

	mux.HandleFunc("/api/restores", s.handleRestores)
	mux.HandleFunc("/api/restores/", s.handleRestoreSubRoutes)

	mux.HandleFunc("/api/licenses", s.handleAllLicenses)
	mux.HandleFunc("/api/k8s/nodes-fingerprints", s.handleNodeFingerprints)
	mux.HandleFunc("/api/k8s/nodes-fingerprints/probe", s.handleProbeFingerprints)

	mux.HandleFunc("/api/migration/legacy-clusters", s.handleLegacyClusters)
	mux.HandleFunc("/api/migration/legacy-clusters/", s.handleLegacyClusterSubRoutes)

	// Embedded UI Static Assets
	subUI, err := fs.Sub(uiFS, "ui")
	if err == nil {
		fileServer := http.FileServer(http.FS(subUI))
		mux.Handle("/", fileServer)
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte("DolphinDB Operator Dashboard UI unavailable"))
		})
	}

	server := &http.Server{
		Addr:         s.listen,
		Handler:      corsMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-serverErr:
		return err
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func jsonResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func errorResponse(w http.ResponseWriter, statusCode int, message string) {
	jsonResponse(w, statusCode, map[string]string{"error": message})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

func (s *Server) handleNamespaces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	ctx := r.Context()
	nsList := &corev1.NamespaceList{}
	if err := s.client.List(ctx, nsList); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list namespaces: %v", err))
		return
	}
	names := make([]string, 0, len(nsList.Items))
	for _, ns := range nsList.Items {
		names = append(names, ns.Name)
	}
	sort.Strings(names)
	jsonResponse(w, http.StatusOK, names)
}

type StorageClassSummary struct {
	Name        string `json:"name"`
	Provisioner string `json:"provisioner"`
	IsDefault   bool   `json:"isDefault"`
}

func (s *Server) handleStorageClasses(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	ctx := r.Context()
	scList := &storagev1.StorageClassList{}
	if err := s.client.List(ctx, scList); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list storage classes: %v", err))
		return
	}
	summaries := make([]StorageClassSummary, 0, len(scList.Items))
	for _, sc := range scList.Items {
		isDefault := false
		if sc.Annotations != nil {
			if sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" ||
				sc.Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
				isDefault = true
			}
		}
		summaries = append(summaries, StorageClassSummary{
			Name:        sc.Name,
			Provisioner: sc.Provisioner,
			IsDefault:   isDefault,
		})
	}
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].IsDefault != summaries[j].IsDefault {
			return summaries[i].IsDefault
		}
		return summaries[i].Name < summaries[j].Name
	})
	jsonResponse(w, http.StatusOK, summaries)
}

// ClusterSummary represents high-level metrics for dashboard cards
type ClusterSummary struct {
	Name                 string               `json:"name"`
	Namespace            string               `json:"namespace"`
	Mode                 v1alpha1.ClusterMode `json:"mode"`
	Version              string               `json:"version"`
	Health               string               `json:"health"`
	DesiredNodes         int32                `json:"desiredNodes"`
	ReadyNodes           int32                `json:"readyNodes"`
	DataNodes            int32                `json:"dataNodes"`
	ComputeNodes         int32                `json:"computeNodes"`
	DfsReplicationFactor int32                `json:"dfsReplicationFactor"`
	CreationTimestamp    metav1.Time          `json:"creationTimestamp"`
	ControllerPort       int32                `json:"controllerPort"`
	NodePort             int32                `json:"nodePort,omitempty"`
	AccessURL            string               `json:"accessURL,omitempty"`
}

func getDfsReplicationFactor(c *v1alpha1.DolphinDBCluster) int32 {
	if c.Spec.Mode == v1alpha1.ModeStandalone {
		return 1
	}
	repFactor := int32(2)
	if c.Spec.Controller != nil {
		if rfStr, ok := c.Spec.Controller.Config["dfsReplicationFactor"]; ok {
			if rf, err := strconv.Atoi(rfStr); err == nil && rf > 0 {
				return int32(rf)
			}
		}
	}
	return repFactor
}

type NodeGroupConfig struct {
	Replicas     int32  `json:"replicas"`
	CPURequest   string `json:"cpuRequest,omitempty"`
	CPULimit     string `json:"cpuLimit,omitempty"`
	MemRequest   string `json:"memRequest,omitempty"`
	MemLimit     string `json:"memLimit,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
	StorageSize  string `json:"storageSize,omitempty"`
}

type CreateClusterRequest struct {
	YAML         string                `json:"yaml,omitempty"`
	Name         string                `json:"name"`
	Namespace    string                `json:"namespace"`
	Mode         string                `json:"mode"`
	Version      string                `json:"version"`
	Image        string                `json:"image"`
	StorageClass string                `json:"storageClass"`
	Controller   *NodeGroupConfig      `json:"controller,omitempty"`
	DataNode     *NodeGroupConfig      `json:"dataNode,omitempty"`
	ComputeNode  *NodeGroupConfig      `json:"computeNode,omitempty"`
	Standalone   *NodeGroupConfig      `json:"standalone,omitempty"`
	Logging      *v1alpha1.LoggingSpec `json:"logging,omitempty"`
	Config       map[string]string     `json:"config,omitempty"`
}

func parseResources(cpuReq, cpuLim, memReq, memLim string) corev1.ResourceRequirements {
	res := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}
	if cpuReq != "" {
		if q, err := resource.ParseQuantity(cpuReq); err == nil {
			res.Requests[corev1.ResourceCPU] = q
		}
	}
	if cpuLim != "" {
		if q, err := resource.ParseQuantity(cpuLim); err == nil {
			res.Limits[corev1.ResourceCPU] = q
		}
	}
	if memReq != "" {
		if q, err := resource.ParseQuantity(memReq); err == nil {
			res.Requests[corev1.ResourceMemory] = q
		}
	}
	if memLim != "" {
		if q, err := resource.ParseQuantity(memLim); err == nil {
			res.Limits[corev1.ResourceMemory] = q
		}
	}
	return res
}

func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.Method == http.MethodGet {
		clusterList := &v1alpha1.DolphinDBClusterList{}
		listOpts := []client.ListOption{}
		ns := r.URL.Query().Get("namespace")
		if ns == "" && s.namespace != "" {
			ns = s.namespace
		}
		if ns != "" {
			listOpts = append(listOpts, client.InNamespace(ns))
		}

		if err := s.client.List(ctx, clusterList, listOpts...); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list clusters: %v", err))
			return
		}

		svcList := &corev1.ServiceList{}
		_ = s.client.List(ctx, svcList, listOpts...)
		svcMap := make(map[string]*corev1.Service)
		for i := range svcList.Items {
			svcMap[fmt.Sprintf("%s/%s", svcList.Items[i].Namespace, svcList.Items[i].Name)] = &svcList.Items[i]
		}

		nodeList := &v1alpha1.DDBNodeList{}
		_ = s.client.List(ctx, nodeList, listOpts...)
		nodeMap := make(map[string]*v1alpha1.DDBNode)
		for i := range nodeList.Items {
			nodeMap[fmt.Sprintf("%s/%s", nodeList.Items[i].Namespace, nodeList.Items[i].Name)] = &nodeList.Items[i]
		}

		summaries := make([]ClusterSummary, 0, len(clusterList.Items))
		for _, c := range clusterList.Items {
			ctlPort := int32(8900)
			if c.Spec.Mode == v1alpha1.ModeStandalone {
				ctlPort = 8848
				if c.Spec.Standalone != nil && c.Spec.Standalone.Port > 0 {
					ctlPort = c.Spec.Standalone.Port
				}
			} else if c.Spec.Controller != nil && c.Spec.Controller.Port > 0 {
				ctlPort = c.Spec.Controller.Port
			}

			repl := getDfsReplicationFactor(&c)

			dataNodes := int32(0)
			if c.Spec.DataNode != nil {
				dataNodes = c.Spec.DataNode.Replicas
			}

			computeNodes := int32(0)
			if c.Spec.ComputeNode != nil {
				computeNodes = c.Spec.ComputeNode.Replicas
			}

			desired := c.Status.Controller.Desired + c.Status.DataNode.Desired + c.Status.ComputeNode.Desired
			ready := c.Status.Controller.Ready + c.Status.DataNode.Ready + c.Status.ComputeNode.Ready
			if c.Spec.Mode == v1alpha1.ModeStandalone {
				desired = 1
				if c.Status.Phase == v1alpha1.PhaseReady {
					ready = 1
				}
			}

			health := string(c.Status.Phase)
			if health == "" {
				health = "Creating"
			}

			var nodePort int32
			var accessURL string
			if svc, exists := svcMap[fmt.Sprintf("%s/%s", c.Namespace, c.Name)]; exists && svc.Spec.Type == corev1.ServiceTypeNodePort {
				for _, p := range svc.Spec.Ports {
					if p.NodePort > 0 {
						nodePort = p.NodePort
						break
					}
				}
				if nodePort > 0 {
					hIP := "192.168.100.44"
					if c.Spec.Networking.PublicIP != "" {
						hIP = c.Spec.Networking.PublicIP
					} else if c.Status.MasterController != "" {
						if leaderNode, exists := nodeMap[fmt.Sprintf("%s/%s", c.Namespace, c.Status.MasterController)]; exists && leaderNode.Status.HostIP != "" {
							hIP = leaderNode.Status.HostIP
						}
					}
					if hIP == "192.168.100.44" && r != nil && r.Host != "" {
						h, _, err := net.SplitHostPort(r.Host)
						if err == nil && h != "" && h != "localhost" && h != "127.0.0.1" {
							hIP = h
						}
					}
					accessURL = fmt.Sprintf("http://%s:%d", hIP, nodePort)
				}
			}

			summaries = append(summaries, ClusterSummary{
				Name:                 c.Name,
				Namespace:            c.Namespace,
				Mode:                 c.Spec.Mode,
				Version:              c.Spec.Version,
				Health:               health,
				DesiredNodes:         desired,
				ReadyNodes:           ready,
				DataNodes:            dataNodes,
				ComputeNodes:         computeNodes,
				DfsReplicationFactor: repl,
				CreationTimestamp:    c.CreationTimestamp,
				ControllerPort:       ctlPort,
				NodePort:             nodePort,
				AccessURL:            accessURL,
			})
		}

		jsonResponse(w, http.StatusOK, summaries)
		return
	}

	if r.Method == http.MethodPost {
		var req CreateClusterRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			errorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON request: %v", err))
			return
		}

		cluster := &v1alpha1.DolphinDBCluster{}

		if req.YAML != "" {
			if err := yaml.Unmarshal([]byte(req.YAML), cluster); err != nil {
				errorResponse(w, http.StatusBadRequest, fmt.Sprintf("Failed to parse YAML: %v", err))
				return
			}
		} else {
			if req.Name == "" {
				errorResponse(w, http.StatusBadRequest, "Cluster name is required")
				return
			}
			ns := req.Namespace
			if ns == "" {
				ns = s.namespace
				if ns == "" {
					ns = "default"
				}
			}

			mode := v1alpha1.ClusterMode(req.Mode)
			if mode == "" {
				mode = v1alpha1.ModeCluster
			}

			version := req.Version
			if version == "" {
				version = "3.00.6"
			}

			image := req.Image
			if image == "" {
				image = fmt.Sprintf("192.168.100.250/dolphindb/dolphindb:v%s", version)
			}

			cluster.ObjectMeta = metav1.ObjectMeta{
				Name:      req.Name,
				Namespace: ns,
			}
			cluster.Spec = v1alpha1.DolphinDBClusterSpec{
				Mode:    mode,
				Version: version,
				Image:   image,
				License: v1alpha1.LicenseSpec{
					Mode:       v1alpha1.LicenseModeMachineFingerprint,
					SecretName: "dolphindb-license",
					AutoReload: true,
					MachineFingerprint: &v1alpha1.MachineFingerprintSpec{
						AutoPassthrough: true,
						MountMachineId:  true,
						MountHostEtc:    true,
						UseHostName:     false,
					},
				},
			}

			defaultStdout := int32(2)
			defaultMaxLogSize := int32(200)
			defaultLogLevel := "INFO"
			logging := &v1alpha1.LoggingSpec{
				StdoutLog:  &defaultStdout,
				MaxLogSize: defaultMaxLogSize,
				LogLevel:   defaultLogLevel,
			}
			if req.Logging != nil {
				if req.Logging.StdoutLog != nil {
					logging.StdoutLog = req.Logging.StdoutLog
				}
				if req.Logging.LogFile != "" {
					logging.LogFile = req.Logging.LogFile
				}
				if req.Logging.MaxLogSize > 0 {
					logging.MaxLogSize = req.Logging.MaxLogSize
				}
				if req.Logging.LogLevel != "" {
					logging.LogLevel = req.Logging.LogLevel
				}
			}
			cluster.Spec.Logging = logging

			scGlobal := req.StorageClass

			if mode == v1alpha1.ModeStandalone {
				sc := scGlobal
				size := "20Gi"
				cpuReq, cpuLim, memReq, memLim := "1000m", "2000m", "2Gi", "4Gi"
				if req.Standalone != nil {
					if req.Standalone.StorageClass != "" {
						sc = req.Standalone.StorageClass
					}
					if req.Standalone.StorageSize != "" {
						size = req.Standalone.StorageSize
					}
					if req.Standalone.CPURequest != "" {
						cpuReq = req.Standalone.CPURequest
					}
					if req.Standalone.CPULimit != "" {
						cpuLim = req.Standalone.CPULimit
					}
					if req.Standalone.MemRequest != "" {
						memReq = req.Standalone.MemRequest
					}
					if req.Standalone.MemLimit != "" {
						memLim = req.Standalone.MemLimit
					}
				}
				var scPtr *string
				if sc != "" {
					scPtr = &sc
				}
				cluster.Spec.Standalone = &v1alpha1.StandaloneSpec{
					Port:      8848,
					Resources: parseResources(cpuReq, cpuLim, memReq, memLim),
					Storage: v1alpha1.StorageSpec{
						StorageClassName: scPtr,
						DataVolumeSize:   size,
						MetaVolumeSize:   "10Gi",
						DataVolumeCount:  1,
					},
				}
			} else {
				// Controller
				ctlReplicas := int32(1)
				ctlSC := scGlobal
				ctlSize := "10Gi"
				ctlCpuReq, ctlCpuLim, ctlMemReq, ctlMemLim := "1000m", "2000m", "2Gi", "4Gi"
				if req.Controller != nil {
					if req.Controller.Replicas > 0 {
						ctlReplicas = req.Controller.Replicas
					}
					if req.Controller.StorageClass != "" {
						ctlSC = req.Controller.StorageClass
					}
					if req.Controller.StorageSize != "" {
						ctlSize = req.Controller.StorageSize
					}
					if req.Controller.CPURequest != "" {
						ctlCpuReq = req.Controller.CPURequest
					}
					if req.Controller.CPULimit != "" {
						ctlCpuLim = req.Controller.CPULimit
					}
					if req.Controller.MemRequest != "" {
						ctlMemReq = req.Controller.MemRequest
					}
					if req.Controller.MemLimit != "" {
						ctlMemLim = req.Controller.MemLimit
					}
				}
				var ctlScPtr *string
				if ctlSC != "" {
					ctlScPtr = &ctlSC
				}
				cluster.Spec.Controller = &v1alpha1.ControllerGroupSpec{
					Replicas:  ctlReplicas,
					Port:      8900,
					Resources: parseResources(ctlCpuReq, ctlCpuLim, ctlMemReq, ctlMemLim),
					Storage: v1alpha1.StorageSpec{
						StorageClassName: ctlScPtr,
						MetaVolumeSize:   ctlSize,
						DataVolumeSize:   ctlSize,
						DataVolumeCount:  1,
					},
					Config: req.Config,
				}

				// DataNode
				dnReplicas := int32(2)
				dnSC := scGlobal
				dnSize := "50Gi"
				dnCpuReq, dnCpuLim, dnMemReq, dnMemLim := "2000m", "4000m", "4Gi", "8Gi"
				if req.DataNode != nil {
					if req.DataNode.Replicas > 0 {
						dnReplicas = req.DataNode.Replicas
					}
					if req.DataNode.StorageClass != "" {
						dnSC = req.DataNode.StorageClass
					}
					if req.DataNode.StorageSize != "" {
						dnSize = req.DataNode.StorageSize
					}
					if req.DataNode.CPURequest != "" {
						dnCpuReq = req.DataNode.CPURequest
					}
					if req.DataNode.CPULimit != "" {
						dnCpuLim = req.DataNode.CPULimit
					}
					if req.DataNode.MemRequest != "" {
						dnMemReq = req.DataNode.MemRequest
					}
					if req.DataNode.MemLimit != "" {
						dnMemLim = req.DataNode.MemLimit
					}
				}
				var dnScPtr *string
				if dnSC != "" {
					dnScPtr = &dnSC
				}
				cluster.Spec.DataNode = &v1alpha1.DataNodeGroupSpec{
					Replicas:  dnReplicas,
					Port:      8901,
					Resources: parseResources(dnCpuReq, dnCpuLim, dnMemReq, dnMemLim),
					Storage: v1alpha1.StorageSpec{
						StorageClassName: dnScPtr,
						DataVolumeSize:   dnSize,
						MetaVolumeSize:   "20Gi",
						DataVolumeCount:  1,
					},
					Config: req.Config,
				}

				// ComputeNode (optional)
				if req.ComputeNode != nil && req.ComputeNode.Replicas > 0 {
					cnCpuReq, cnCpuLim, cnMemReq, cnMemLim := "1000m", "2000m", "2Gi", "4Gi"
					if req.ComputeNode.CPURequest != "" {
						cnCpuReq = req.ComputeNode.CPURequest
					}
					if req.ComputeNode.CPULimit != "" {
						cnCpuLim = req.ComputeNode.CPULimit
					}
					if req.ComputeNode.MemRequest != "" {
						cnMemReq = req.ComputeNode.MemRequest
					}
					if req.ComputeNode.MemLimit != "" {
						cnMemLim = req.ComputeNode.MemLimit
					}
					cluster.Spec.ComputeNode = &v1alpha1.ComputeNodeGroupSpec{
						Replicas:  req.ComputeNode.Replicas,
						Port:      8902,
						Resources: parseResources(cnCpuReq, cnCpuLim, cnMemReq, cnMemLim),
						Config:    req.Config,
					}
				}
			}
		}

		if cluster.Spec.Networking.ServiceType == "" {
			cluster.Spec.Networking = v1alpha1.NetworkingSpec{
				ExposureMode: v1alpha1.ExposureNodePortDirect,
				ServiceType:  corev1.ServiceTypeNodePort,
			}
		}

		if err := s.client.Create(ctx, cluster); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create cluster: %v", err))
			return
		}

		jsonResponse(w, http.StatusCreated, cluster)
		return
	}

	errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
}

func (s *Server) handleClusterSubRoutes(w http.ResponseWriter, r *http.Request) {
	// Pattern: /api/clusters/{namespace}/{name}[/{action}]
	path := strings.TrimPrefix(r.URL.Path, "/api/clusters/")
	parts := strings.Split(strings.Trim(path, "/"), "/")

	if len(parts) < 2 {
		errorResponse(w, http.StatusBadRequest, "Invalid path: expected /api/clusters/{namespace}/{name}")
		return
	}

	namespace := parts[0]
	name := parts[1]

	if len(parts) == 2 {
		if r.Method == http.MethodGet {
			s.getClusterDetail(w, r, namespace, name)
			return
		}
		if r.Method == http.MethodDelete {
			s.deleteCluster(w, r, namespace, name)
			return
		}
		if r.Method == http.MethodPut {
			s.updateCluster(w, r, namespace, name)
			return
		}
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	action := parts[2]
	switch action {
	case "restart":
		if r.Method == http.MethodPost {
			s.triggerRollingRestart(w, r, namespace, name)
			return
		}
	case "scale":
		if r.Method == http.MethodPost {
			s.scaleCluster(w, r, namespace, name)
			return
		}
	case "rebalance":
		if r.Method == http.MethodPost {
			s.triggerRebalance(w, r, namespace, name)
			return
		}
	case "reload-license":
		if r.Method == http.MethodPost {
			s.triggerReloadLicense(w, r, namespace, name)
			return
		}
	case "license":
		if r.Method == http.MethodGet {
			s.handleClusterLicense(w, r, namespace, name)
			return
		} else if r.Method == http.MethodPost {
			s.handleLicenseUpload(w, r, namespace, name)
			return
		}
	case "license-upload":
		if r.Method == http.MethodPost {
			s.handleLicenseUpload(w, r, namespace, name)
			return
		}
	case "config":
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			s.updateClusterConfig(w, r, namespace, name)
			return
		}
	case "expose":
		if r.Method == http.MethodPost {
			s.toggleClusterExpose(w, r, namespace, name)
			return
		}
	case "metrics":
		if r.Method == http.MethodGet {
			s.getClusterMetrics(w, r, namespace, name)
			return
		}
	default:
		errorResponse(w, http.StatusNotFound, "Action not found")
		return
	}

	errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
}

func (s *Server) deleteCluster(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	cluster := &v1alpha1.DolphinDBCluster{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	if err := s.client.Delete(ctx, cluster); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to delete cluster: %v", err))
		return
	}
	jsonResponse(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("Cluster %s/%s deletion requested", namespace, name),
	})
}

func (s *Server) updateCluster(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	var updated v1alpha1.DolphinDBCluster
	if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
		errorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON body: %v", err))
		return
	}

	cluster := &v1alpha1.DolphinDBCluster{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Cluster %s/%s not found", namespace, name))
		return
	}

	cluster.Spec = updated.Spec
	if err := s.client.Update(ctx, cluster); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to update cluster: %v", err))
		return
	}
	jsonResponse(w, http.StatusOK, cluster)
}

type UpdateConfigRequest struct {
	Config map[string]string `json:"config"`
}

func (s *Server) updateClusterConfig(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	var req UpdateConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	cluster := &v1alpha1.DolphinDBCluster{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Cluster %s/%s not found", namespace, name))
		return
	}

	if cluster.Spec.DataNode != nil {
		if cluster.Spec.DataNode.Config == nil {
			cluster.Spec.DataNode.Config = make(map[string]string)
		}
		for k, v := range req.Config {
			cluster.Spec.DataNode.Config[k] = v
		}
	}
	if cluster.Spec.Controller != nil {
		if cluster.Spec.Controller.Config == nil {
			cluster.Spec.Controller.Config = make(map[string]string)
		}
		for k, v := range req.Config {
			cluster.Spec.Controller.Config[k] = v
		}
	}

	if err := s.client.Update(ctx, cluster); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to update cluster configuration: %v", err))
		return
	}
	jsonResponse(w, http.StatusOK, cluster)
}

// ClusterExposeRequest is the body of POST /api/clusters/{ns}/{name}/expose.
type ClusterExposeRequest struct {
	// Enabled requests an explicit desired state. When it is omitted the endpoint
	// toggles, which is kept only for backwards compatibility.
	//
	// A blind toggle is dangerous here: a caller that believes the cluster is not
	// exposed - for example because a stale response reported an empty accessURL -
	// would switch an already-exposed cluster *off*, deleting the cloud load
	// balancer and producing a brand new DNS name on the next click.
	Enabled *bool `json:"enabled,omitempty"`
	// Scheme is "internet-facing" (default) or "internal".
	Scheme string `json:"scheme,omitempty"`
	// Component selects which component to act on: "controller", "datanode",
	// "computenode", or "all".
	//
	// A cluster exposes them independently because they are different pod sets on
	// different ports, and each exposed one becomes its own cloud load balancer:
	// the Web console lives on the controller, while DFS reads and writes only run
	// on data or compute nodes. Exposing "all" on a cloud therefore costs three load
	// balancers.
	Component string `json:"component,omitempty"`
}

func (s *Server) toggleClusterExpose(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	var req ClusterExposeRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	cluster := &v1alpha1.DolphinDBCluster{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Cluster %s/%s not found: %v", namespace, name, err))
		return
	}

	// On a managed cloud prefer a load balancer: the control plane provisions it
	// and wires the security groups, so exposing produces a reachable address with
	// no manual security group work and no IRSA. Bare metal keeps the NodePort
	// behaviour because nothing would fulfil a LoadBalancer there.
	provider := s.detectProvider(ctx)
	exposedType := corev1.ServiceTypeNodePort
	if provider.SupportsLoadBalancer() {
		exposedType = corev1.ServiceTypeLoadBalancer
	}

	// Current per-component selection. A cluster created before per-component
	// exposure existed has no `expose` block; fall back to its cluster-wide
	// ServiceType so a toggle does not silently drop an existing exposure.
	state := map[string]bool{"controller": false, "datanode": false, "computenode": false}
	if e := cluster.Spec.Networking.Expose; e != nil {
		state["controller"], state["datanode"], state["computenode"] = e.Controller, e.DataNode, e.ComputeNode
	} else if t := cluster.Spec.Networking.ServiceType; t != "" && t != corev1.ServiceTypeClusterIP {
		state["controller"], state["datanode"], state["computenode"] = true, true, true
	}

	targets := []string{"controller", "datanode", "computenode"}
	if req.Component != "" && req.Component != "all" {
		if _, ok := state[req.Component]; !ok {
			errorResponse(w, http.StatusBadRequest, fmt.Sprintf("unknown component %q: expected controller, datanode, computenode or all", req.Component))
			return
		}
		targets = []string{req.Component}
	}

	// Default to toggling only when the caller did not state its intent.
	wantExposed := !state[targets[0]]
	if req.Enabled != nil {
		wantExposed = *req.Enabled
	}
	for _, t := range targets {
		state[t] = wantExposed
	}

	cluster.Spec.Networking.Expose = &v1alpha1.ExposeSpec{
		Controller:  state["controller"],
		DataNode:    state["datanode"],
		ComputeNode: state["computenode"],
	}

	if wantExposed {
		// State the type explicitly. Without this a cluster still carrying the old
		// NodePort default would keep publishing unreachable NodePort URLs.
		cluster.Spec.Networking.ServiceType = exposedType
		if exposedType == corev1.ServiceTypeLoadBalancer {
			cluster.Spec.Networking.ExposureMode = v1alpha1.ExposureGateway
			if cluster.Spec.Networking.Annotations == nil {
				cluster.Spec.Networking.Annotations = map[string]string{}
			}
			scheme := exposure.ParseScheme(req.Scheme, exposure.SchemeInternetFacing)
			for k, v := range exposure.DefaultAnnotations(provider, scheme) {
				cluster.Spec.Networking.Annotations[k] = v
			}
		} else {
			cluster.Spec.Networking.ExposureMode = v1alpha1.ExposureNodePortDirect
		}
	} else if !cluster.Spec.Networking.Expose.Any() {
		cluster.Spec.Networking.ServiceType = corev1.ServiceTypeClusterIP
		cluster.Spec.Networking.ExposureMode = v1alpha1.ExposureClusterIP
	}

	if err := s.client.Update(ctx, cluster); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to update cluster networking: %v", err))
		return
	}

	// The cluster-level Access Services are owned and reconciled by the
	// DolphinDBCluster controller. Mutating them from here would create two writers
	// for the same object and race the controller's cached read: the controller can
	// briefly observe the old type and flip the Service back, which for a
	// LoadBalancer means the cloud provider deletes the load balancer it just
	// created ("SyncLoadBalancerFailed ... may only be used when spec.type is
	// 'LoadBalancer'"). Updating the CR and letting the single owner act is both
	// correct and sufficient - the reconcile is triggered by this write.
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"cluster":      cluster,
		"provider":     provider,
		"providerName": provider.DisplayName(),
		"serviceType":  cluster.Spec.Networking.ServiceType,
		"enabled":      wantExposed,
		"component":    req.Component,
		"exposed":      cluster.Spec.Networking.Expose.Names(),
		"message":      "networking updated; the controller will reconcile the access services",
	})
}

// ClusterDetail represents comprehensive topology view for a single cluster
type ClusterDetail struct {
	Cluster     v1alpha1.DolphinDBCluster `json:"cluster"`
	Nodes       []NodeInfo                `json:"nodes"`
	Operations  []v1alpha1.DDBOperation   `json:"operations"`
	Backups     []v1alpha1.DDBBackup      `json:"backups"`
	ServiceType string                    `json:"serviceType,omitempty"`
	AccessURL   string                    `json:"accessURL,omitempty"`

	// Exposure fields describe the cluster-level entry point (as opposed to the
	// per-node ones in NodeInfo) so the UI can render the same states for both:
	// a real address, a pending cloud load balancer, or an unreachable NodePort.
	ExposureMode    exposure.Mode   `json:"exposureMode,omitempty"`
	ExposureScheme  exposure.Scheme `json:"exposureScheme,omitempty"`
	ExposurePending bool            `json:"exposurePending,omitempty"`
	ExposureMessage string          `json:"exposureMessage,omitempty"`
	ExposureTLS     bool            `json:"exposureTLS,omitempty"`

	// TLSEnabled reports whether native HTTPS is on, TLSManaged whether the
	// certificate is operator-issued (and therefore untrusted by browsers).
	TLSEnabled bool `json:"tlsEnabled,omitempty"`
	TLSManaged bool `json:"tlsManaged,omitempty"`

	// Components describes every Access Service individually. A cluster has up to
	// three (controller, data nodes, compute nodes) on different ports and different
	// pod sets, and each exposed one is a separate cloud load balancer, so the UI
	// needs per-component state rather than a single cluster-wide URL.
	Components []ComponentExposure `json:"components,omitempty"`
}

// ComponentExposure is the external reachability of one cluster component.
type ComponentExposure struct {
	// Component is controller, datanode or computenode.
	Component string `json:"component"`
	// ServiceName is the Kubernetes Service backing this component.
	ServiceName string `json:"serviceName"`
	// ServiceType is ClusterIP, NodePort or LoadBalancer.
	ServiceType string `json:"serviceType"`
	// Exposed is true when the component has a non-ClusterIP entry point.
	Exposed bool `json:"exposed"`
	// URL is the external address, when one is known.
	URL string `json:"url,omitempty"`
	// Port is the exposed port (the NodePort for NodePort exposure).
	Port int32 `json:"port,omitempty"`
	// Pending is true while the cloud provisions the load balancer.
	Pending bool `json:"pending,omitempty"`
	// Reachable is false when the address is known not to work, for example a
	// NodePort on a cloud whose security group has not been opened. Always
	// serialised so the UI can tell "false" from "not reported".
	Reachable bool `json:"reachable"`
	// Message explains a pending or unreachable endpoint.
	Message string `json:"message,omitempty"`
	// TLS is true when the endpoint speaks HTTPS.
	TLS bool `json:"tls,omitempty"`
}

// anyNode picks a deterministic node from the map. NodePort is reachable through
// any node because kube-proxy forwards it, so the choice only matters for
// reproducibility.
func anyNode(m map[string]*corev1.Node) *corev1.Node {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil
	}
	return m[names[0]]
}

// NodeInfo describes one DolphinDB node as shown in the topology view.
type NodeInfo struct {
	Name        string            `json:"name"`
	Role        v1alpha1.NodeRole `json:"role"`
	Phase       string            `json:"phase"`
	PodIP       string            `json:"podIP"`
	HostIP      string            `json:"hostIP"`
	Port        int32             `json:"port"`
	Ready       bool              `json:"ready"`
	ChunksCount int32             `json:"chunksCount"`
	DDBState    int32             `json:"ddbState"`
	NodePort    int32             `json:"nodePort,omitempty"`
	ExternalURL string            `json:"externalURL,omitempty"`
	IsLeader    bool              `json:"isLeader,omitempty"`

	// ExposureMode is the effective Service type used to reach this node from
	// outside the cluster: ClusterIP, NodePort or LoadBalancer.
	ExposureMode exposure.Mode `json:"exposureMode,omitempty"`
	// ExposureScheme distinguishes an internet-facing from an internal load balancer.
	ExposureScheme exposure.Scheme `json:"exposureScheme,omitempty"`
	// ExposurePending is true while the cloud provider is still provisioning the
	// load balancer, letting the UI show progress instead of a dead link.
	ExposurePending bool `json:"exposurePending,omitempty"`
	// ExposureMessage explains a pending or potentially unreachable endpoint.
	ExposureMessage string `json:"exposureMessage,omitempty"`

	// ExposureTLS is true when the endpoint speaks HTTPS.
	ExposureTLS bool `json:"exposureTLS,omitempty"`
}

func (s *Server) getClusterDetail(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()

	cluster := &v1alpha1.DolphinDBCluster{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Cluster %s/%s not found: %v", namespace, name, err))
		return
	}

	// Fetch Services in namespace to detect NodePort mappings
	svcList := &corev1.ServiceList{}
	_ = s.client.List(ctx, svcList, client.InNamespace(namespace))
	svcMap := make(map[string]*corev1.Service)
	for i := range svcList.Items {
		svcMap[svcList.Items[i].Name] = &svcList.Items[i]
	}

	// The Kubernetes nodes are required to resolve a *reachable* address for
	// NodePort exposure. On every cloud DDBNode.Status.HostIP is a private pod
	// subnet address, which is why the previous implementation handed out URLs
	// that could never be opened from outside the cluster.
	k8sNodeMap := map[string]*corev1.Node{}
	nodeList := &corev1.NodeList{}
	if err := s.client.List(ctx, nodeList); err == nil {
		for i := range nodeList.Items {
			k8sNodeMap[nodeList.Items[i].Name] = &nodeList.Items[i]
		}
	}

	clusterSvcType := "ClusterIP"
	var clusterAccessURL string
	var clusterEndpoint exposure.Endpoint
	if clusterSvc, exists := svcMap[name]; exists {
		clusterSvcType = string(clusterSvc.Spec.Type)
		// Resolve the cluster entry point the same cloud-aware way as a per-node
		// exposure. Previously this only ran for NodePort and derived the address
		// from the request Host or a hardcoded IP, so a LoadBalancer produced an
		// empty accessURL - which made the UI report "ClusterIP, not exposed" for a
		// cluster that was in fact exposed, and offer a toggle that then turned the
		// exposure *off*.
		clusterEndpoint = exposure.ResolveEndpoint(nil, anyNode(k8sNodeMap), clusterSvc)
		clusterAccessURL = clusterEndpoint.URL
	}

	// Per-component view. Each component is a different pod set on a different port,
	// so each exposed one is its own Service and its own cloud load balancer.
	components := make([]ComponentExposure, 0, 3)
	for _, comp := range []struct {
		name    string
		svcName string
	}{
		{"controller", name},
		{"datanode", name + "-dn"},
		{"computenode", name + "-cn"},
	} {
		svc, ok := svcMap[comp.svcName]
		if !ok {
			continue
		}
		entry := ComponentExposure{
			Component:   comp.name,
			ServiceName: comp.svcName,
			ServiceType: string(svc.Spec.Type),
			Exposed:     svc.Spec.Type != corev1.ServiceTypeClusterIP,
		}
		if entry.Exposed {
			ep := exposure.ResolveEndpoint(nil, anyNode(k8sNodeMap), svc)
			entry.URL = ep.URL
			entry.Port = ep.Port
			entry.Pending = ep.Pending
			entry.Reachable = ep.Reachable
			entry.Message = ep.Message
			entry.TLS = ep.TLS
		}
		components = append(components, entry)
	}

	// Fetch DDBNodes
	ddbNodeList := &v1alpha1.DDBNodeList{}
	_ = s.client.List(ctx, ddbNodeList, client.InNamespace(namespace))

	// Find the leader controller for HA clusters
	var leaderNode *v1alpha1.DDBNode
	var leaderAlias string
	isHACluster := cluster.Spec.Mode == v1alpha1.ModeCluster && cluster.Spec.Controller != nil && cluster.Spec.Controller.Replicas > 1

	if isHACluster {
		if cluster.Status.MasterController != "" {
			for idx := range ddbNodeList.Items {
				if ddbNodeList.Items[idx].Name == cluster.Status.MasterController {
					leaderNode = &ddbNodeList.Items[idx]
					leaderAlias = leaderNode.Spec.Alias
					break
				}
			}
		}
		// If not found yet via status, query live controller directly
		if leaderNode == nil {
			for idx := range ddbNodeList.Items {
				item := &ddbNodeList.Items[idx]
				if item.Spec.ClusterName == name && item.Spec.Role == v1alpha1.NodeRoleController && item.Status.PodIP != "" && item.Status.Phase == v1alpha1.NodePhaseRunning {
					adminUser, adminPass := credentials.Resolve(ctx, s.client, namespace, cluster.Spec.Security.AdminSecretName)
					c := ddbclient.NewClient(fmt.Sprintf("http://%s:%d", item.Status.PodIP, item.Spec.Port), adminUser, adminPass)
					if alias, err := c.GetLeaderControllerAlias(ctx); err == nil && alias != "" {
						leaderAlias = alias
						for j := range ddbNodeList.Items {
							if ddbNodeList.Items[j].Spec.Alias == alias {
								leaderNode = &ddbNodeList.Items[j]
								break
							}
						}
						break
					}
				}
			}
		}
	}

	var leaderAccessURL string
	if leaderNode != nil {
		hIP := leaderNode.Status.HostIP
		if hIP == "" && cluster.Spec.Networking.PublicIP != "" {
			hIP = cluster.Spec.Networking.PublicIP
		}
		if hIP == "" && r != nil && r.Host != "" {
			h, _, err := net.SplitHostPort(r.Host)
			if err == nil && h != "" && h != "localhost" && h != "127.0.0.1" {
				hIP = h
			}
		}
		if hIP == "" {
			hIP = "192.168.100.44"
		}
		var leaderPort int32
		if clusterSvc, exists := svcMap[cluster.Name]; exists && clusterSvc.Spec.Type == corev1.ServiceTypeNodePort {
			for _, p := range clusterSvc.Spec.Ports {
				if p.NodePort > 0 {
					leaderPort = p.NodePort
					break
				}
			}
		}
		if leaderPort > 0 {
			leaderAccessURL = fmt.Sprintf("http://%s:%d", hIP, leaderPort)
			clusterAccessURL = leaderAccessURL
		}
	}

	nodes := make([]NodeInfo, 0, len(ddbNodeList.Items))
	for _, n := range ddbNodeList.Items {
		if n.Spec.ClusterName != name {
			continue
		}
		podIP := n.Status.PodIP
		hostIP := n.Status.HostIP
		ready := false
		if n.Status.Phase == v1alpha1.NodePhaseRunning && n.Status.DDBState == 1 {
			ready = true
		}

		// Choose the Service that exposes this node: the dedicated per-node
		// exposure first, then the component level Service, then the cluster Service.
		var expSvc *corev1.Service
		if s, ok := svcMap[n.Name+"-ext"]; ok {
			expSvc = s
		} else {
			compSvcName := n.Spec.ClusterName
			switch n.Spec.Role {
			case v1alpha1.NodeRoleDataNode:
				compSvcName = n.Spec.ClusterName + "-dn"
			case v1alpha1.NodeRoleComputeNode:
				compSvcName = n.Spec.ClusterName + "-cn"
			}
			expSvc = svcMap[compSvcName]
			if expSvc == nil {
				expSvc = svcMap[n.Spec.ClusterName]
			}
		}

		var nodePort int32
		var extURL string
		var endpoint exposure.Endpoint
		if expSvc != nil && expSvc.Spec.Type != corev1.ServiceTypeClusterIP {
			endpoint = exposure.ResolveEndpoint(&n, k8sNodeMap[n.Status.NodeName], expSvc)
			extURL = endpoint.URL
			if endpoint.Mode == exposure.ModeNodePort {
				nodePort = endpoint.Port
			}
		}
		// Legacy fallback for platforms where the node object is not readable:
		// synthesise a URL from cluster networking, but never invent a hardcoded IP.
		if extURL == "" && nodePort > 0 {
			hIP := n.Status.HostIP
			if hIP == "" && cluster.Spec.Networking.PublicIP != "" {
				hIP = cluster.Spec.Networking.PublicIP
			}
			if hIP == "" && r != nil && r.Host != "" {
				h, _, err := net.SplitHostPort(r.Host)
				if err == nil && h != "" && h != "localhost" && h != "127.0.0.1" {
					hIP = h
				}
			}
			if hIP != "" {
				extURL = fmt.Sprintf("http://%s:%d", hIP, nodePort)
			}
		}

		isLeader := false
		if isHACluster {
			if (leaderNode != nil && n.Name == leaderNode.Name) || (leaderAlias != "" && n.Spec.Alias == leaderAlias) {
				isLeader = true
			}
			// As user requested: "高可用集群的控制节点的web，都修改为leader而不是本机的吧"
			// For all controller nodes in HA cluster, point their web externalURL to the Leader!
			if n.Spec.Role == v1alpha1.NodeRoleController && leaderAccessURL != "" {
				extURL = leaderAccessURL
			}
		}

		nodes = append(nodes, NodeInfo{
			Name:            n.Name,
			Role:            n.Spec.Role,
			Phase:           string(n.Status.Phase),
			PodIP:           podIP,
			HostIP:          hostIP,
			Port:            n.Spec.Port,
			Ready:           ready,
			ChunksCount:     n.Status.ChunksCount,
			DDBState:        n.Status.DDBState,
			NodePort:        nodePort,
			ExternalURL:     extURL,
			IsLeader:        isLeader,
			ExposureMode:    endpoint.Mode,
			ExposureScheme:  endpoint.Scheme,
			ExposurePending: endpoint.Pending,
			ExposureMessage: endpoint.Message,
			ExposureTLS:     endpoint.TLS,
		})
	}

	// Sort nodes: Controller first, then DataNode, then ComputeNode, then Single
	roleRank := func(r v1alpha1.NodeRole) int {
		switch r {
		case v1alpha1.NodeRoleController:
			return 1
		case v1alpha1.NodeRoleDataNode:
			return 2
		case v1alpha1.NodeRoleComputeNode:
			return 3
		case v1alpha1.NodeRoleSingle:
			return 4
		default:
			return 5
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		rI, rJ := roleRank(nodes[i].Role), roleRank(nodes[j].Role)
		if rI != rJ {
			return rI < rJ
		}
		return nodes[i].Name < nodes[j].Name
	})

	// Fetch Operations for this cluster
	opList := &v1alpha1.DDBOperationList{}
	_ = s.client.List(ctx, opList, client.InNamespace(namespace))
	clusterOps := make([]v1alpha1.DDBOperation, 0)
	for _, op := range opList.Items {
		if op.Spec.Cluster == name {
			clusterOps = append(clusterOps, op)
		}
	}
	sort.Slice(clusterOps, func(i, j int) bool {
		return clusterOps[i].CreationTimestamp.After(clusterOps[j].CreationTimestamp.Time)
	})

	// Fetch Backups for this cluster
	backupList := &v1alpha1.DDBBackupList{}
	_ = s.client.List(ctx, backupList, client.InNamespace(namespace))
	clusterBackups := make([]v1alpha1.DDBBackup, 0)
	for _, b := range backupList.Items {
		if b.Spec.Cluster == name {
			clusterBackups = append(clusterBackups, b)
		}
	}
	sort.Slice(clusterBackups, func(i, j int) bool {
		return clusterBackups[i].CreationTimestamp.After(clusterBackups[j].CreationTimestamp.Time)
	})

	jsonResponse(w, http.StatusOK, ClusterDetail{
		Cluster:         *cluster,
		Nodes:           nodes,
		Operations:      clusterOps,
		Backups:         clusterBackups,
		ServiceType:     clusterSvcType,
		AccessURL:       clusterAccessURL,
		ExposureMode:    clusterEndpoint.Mode,
		ExposureScheme:  clusterEndpoint.Scheme,
		ExposurePending: clusterEndpoint.Pending,
		ExposureMessage: clusterEndpoint.Message,
		ExposureTLS:     clusterEndpoint.TLS,
		TLSEnabled:      cluster.Spec.Security.TLS != nil && cluster.Spec.Security.TLS.Enabled,
		TLSManaged:      cluster.Spec.Security.TLS != nil && cluster.Spec.Security.TLS.Enabled && cluster.Spec.Security.TLS.SecretName == "",
		Components:      components,
	})
}

// Node actions & logs
func (s *Server) handleNodeSubRoutes(w http.ResponseWriter, r *http.Request) {
	// Pattern: /api/nodes/{namespace}/{name}/{action}
	path := strings.TrimPrefix(r.URL.Path, "/api/nodes/")
	parts := strings.Split(strings.Trim(path, "/"), "/")

	if len(parts) < 3 {
		errorResponse(w, http.StatusBadRequest, "Invalid path: expected /api/nodes/{namespace}/{name}/{logs|action}")
		return
	}

	namespace := parts[0]
	name := parts[1]
	action := parts[2]

	switch action {
	case "logs":
		if r.Method == http.MethodGet {
			s.getNodeLogs(w, r, namespace, name)
			return
		}
	case "action":
		if r.Method == http.MethodPost {
			s.executeNodeAction(w, r, namespace, name)
			return
		}
	case "expose":
		switch r.Method {
		case http.MethodGet:
			s.getNodeExposure(w, r, namespace, name)
			return
		case http.MethodPost:
			s.exposeNodeService(w, r, namespace, name)
			return
		case http.MethodDelete:
			s.unexposeNodeService(w, r, namespace, name)
			return
		}
	case "metrics":
		if r.Method == http.MethodGet {
			s.getNodeMetrics(w, r, namespace, name)
			return
		}
	}

	errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
}

func (s *Server) getNodeMetrics(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	node := &v1alpha1.DDBNode{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, node); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("DDBNode %s/%s not found", namespace, name))
		return
	}

	pod := &corev1.Pod{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pod); err != nil || pod.Status.PodIP == "" {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Pod %s/%s not ready or has no IP", namespace, name))
		return
	}

	targetURL := fmt.Sprintf("http://%s:%d/metrics", pod.Status.PodIP, node.Spec.Port)
	httpClient := &http.Client{Timeout: 3 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		errorResponse(w, http.StatusBadGateway, fmt.Sprintf("Failed to fetch metrics from %s: %v", targetURL, err))
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to read metrics response: %v", err))
		return
	}

	rawText := string(bodyBytes)
	if r.URL.Query().Get("format") == "json" {
		parsed := parsePrometheusText(rawText)
		jsonResponse(w, http.StatusOK, map[string]interface{}{
			"node":      name,
			"alias":     node.Spec.Alias,
			"role":      node.Spec.Role,
			"cluster":   node.Spec.ClusterName,
			"targetUrl": targetURL,
			"metrics":   parsed,
			"raw":       rawText,
		})
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bodyBytes)
}

func (s *Server) getClusterMetrics(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	ddbNodeList := &v1alpha1.DDBNodeList{}
	if err := s.client.List(ctx, ddbNodeList, client.InNamespace(namespace)); err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	var targetNodes []v1alpha1.DDBNode
	for _, n := range ddbNodeList.Items {
		if n.Spec.ClusterName == name {
			targetNodes = append(targetNodes, n)
		}
	}

	type NodeMetricItem struct {
		Node     string             `json:"node"`
		Alias    string             `json:"alias"`
		Role     string             `json:"role"`
		Status   string             `json:"status"`
		IsLeader bool               `json:"isLeader,omitempty"`
		Metrics  map[string]float64 `json:"metrics,omitempty"`
		Error    string             `json:"error,omitempty"`
	}

	results := make([]NodeMetricItem, len(targetNodes))
	var wg sync.WaitGroup
	httpClient := &http.Client{Timeout: 3 * time.Second}

	for i := range targetNodes {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n := targetNodes[idx]
			item := NodeMetricItem{
				Node:   n.Name,
				Alias:  n.Spec.Alias,
				Role:   string(n.Spec.Role),
				Status: string(n.Status.Phase),
			}

			pod := &corev1.Pod{}
			if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: n.Name}, pod); err != nil || pod.Status.PodIP == "" {
				item.Error = "Pod not ready or no IP"
				results[idx] = item
				return
			}

			if pod.Labels != nil && pod.Labels["dolphindb.io/leader"] == "true" {
				item.IsLeader = true
			}

			targetURL := fmt.Sprintf("http://%s:%d/metrics", pod.Status.PodIP, n.Spec.Port)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
			if err != nil {
				item.Error = err.Error()
				results[idx] = item
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				item.Error = err.Error()
				results[idx] = item
				return
			}
			defer resp.Body.Close()

			b, err := io.ReadAll(resp.Body)
			if err != nil {
				item.Error = err.Error()
				results[idx] = item
				return
			}

			item.Metrics = parsePrometheusText(string(b))
			results[idx] = item
		}(i)
	}

	wg.Wait()

	var rawProm strings.Builder
	for _, item := range results {
		if item.Metrics != nil {
			rawProm.WriteString(fmt.Sprintf("# Metrics for node %s (%s, role=%s)\n", item.Node, item.Alias, item.Role))
			for k, v := range item.Metrics {
				rawProm.WriteString(fmt.Sprintf("%s{cluster=\"%s\",node=\"%s\",alias=\"%s\",role=\"%s\"} %f\n", k, name, item.Node, item.Alias, item.Role, v))
				snake := camelToSnake(k)
				rawProm.WriteString(fmt.Sprintf("dolphindb_metric_%s{dolphindb_io_dolphindb_name=\"%s\",pod=\"%s\",dolphindb_io_component_type=\"%s\"} %f\n", snake, name, item.Node, getLegacyComponentType(v1alpha1.NodeRole(item.Role)), v))
			}
		}
	}

	if r.URL.Query().Get("format") == "prometheus" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(rawProm.String()))
		return
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"cluster":       name,
		"namespace":     namespace,
		"nodes":         results,
		"rawPrometheus": rawProm.String(),
	})
}

func parsePrometheusText(text string) map[string]float64 {
	result := make(map[string]float64)
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			key := parts[0]
			if idx := strings.Index(key, "{"); idx > 0 {
				key = key[:idx]
			}
			val, err := strconv.ParseFloat(parts[1], 64)
			if err == nil {
				result[key] = val
			}
		}
	}
	return result
}

func camelToSnake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r + ('a' - 'A'))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func getLegacyComponentType(role v1alpha1.NodeRole) string {
	switch role {
	case v1alpha1.NodeRoleController:
		return "Controllernode"
	case v1alpha1.NodeRoleComputeNode:
		return "Computenode"
	case v1alpha1.NodeRoleSingle:
		return "Standalone"
	default:
		return "Datanode"
	}
}

func (s *Server) getNodeLogs(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	tailLines := int64(100)
	if tStr := r.URL.Query().Get("tailLines"); tStr != "" {
		if t, err := strconv.ParseInt(tStr, 10, 64); err == nil && t > 0 {
			tailLines = t
		}
	}

	if s.kubeClient == nil {
		errorResponse(w, http.StatusServiceUnavailable, "Kubernetes clientset not available for streaming logs")
		return
	}

	req := s.kubeClient.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{
		Container: "dolphindb",
		TailLines: &tailLines,
	})

	stream, err := req.Stream(ctx)
	if err != nil {
		// Try without container name in case it's named differently
		reqFallback := s.kubeClient.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{
			TailLines: &tailLines,
		})
		streamFallback, errFallback := reqFallback.Stream(ctx)
		if errFallback != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch pod logs: %v", err))
			return
		}
		stream = streamFallback
	}
	defer stream.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, stream)
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to read log stream: %v", err))
		return
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"pod":       name,
		"namespace": namespace,
		"tailLines": tailLines,
		"logs":      buf.String(),
	})
}

type NodeActionRequest struct {
	Action string `json:"action"` // "stop", "start", "restart"
}

func (s *Server) executeNodeAction(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	var req NodeActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	node := &v1alpha1.DDBNode{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, node); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("DDBNode %s/%s not found", namespace, name))
		return
	}

	clusterName := node.Spec.ClusterName
	switch req.Action {
	case "stop":
		op := &v1alpha1.DDBOperation{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("stop-%s-%d", name, time.Now().Unix()),
				Namespace: namespace,
			},
			Spec: v1alpha1.DDBOperationSpec{
				Cluster:     clusterName,
				Type:        v1alpha1.OpStopNode,
				TargetNodes: []string{name},
			},
		}
		if err := s.client.Create(ctx, op); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create StopNode operation: %v", err))
			return
		}
		jsonResponse(w, http.StatusCreated, op)

	case "start":
		op := &v1alpha1.DDBOperation{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("start-%s-%d", name, time.Now().Unix()),
				Namespace: namespace,
			},
			Spec: v1alpha1.DDBOperationSpec{
				Cluster:     clusterName,
				Type:        v1alpha1.OpStartNode,
				TargetNodes: []string{name},
			},
		}
		if err := s.client.Create(ctx, op); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create StartNode operation: %v", err))
			return
		}
		jsonResponse(w, http.StatusCreated, op)

	case "restart":
		op := &v1alpha1.DDBOperation{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("restart-%s-%d", name, time.Now().Unix()),
				Namespace: namespace,
			},
			Spec: v1alpha1.DDBOperationSpec{
				Cluster:     clusterName,
				Type:        v1alpha1.OpRollingRestart,
				TargetNodes: []string{name},
			},
		}
		if err := s.client.Create(ctx, op); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create Restart operation: %v", err))
			return
		}
		jsonResponse(w, http.StatusCreated, op)

	default:
		errorResponse(w, http.StatusBadRequest, "Invalid action. Supported: stop, start, restart")
	}
}

type ExposeNodeRequest struct {
	NodePort int32 `json:"nodePort,omitempty"`
	// Mode selects the exposure mechanism: "loadBalancer" (the default on a
	// managed cloud), "nodePort", or "clusterIP" to tear the exposure down.
	Mode string `json:"mode,omitempty"`
	// Scheme is "internet-facing" (default) or "internal".
	Scheme string `json:"scheme,omitempty"`
	// Annotations are merged on top of the provider defaults, letting an operator
	// that does run the cloud load balancer controller opt into the modern
	// annotation flavour without the suite having to guess.
	Annotations map[string]string `json:"annotations,omitempty"`
}

// detectProvider works out which cloud the cluster runs on by inspecting the
// nodes. It degrades to bare metal (i.e. NodePort) when nodes cannot be listed,
// for example when RBAC does not grant `nodes` access.
func (s *Server) detectProvider(ctx context.Context) exposure.Provider {
	nodeList := &corev1.NodeList{}
	if err := s.client.List(ctx, nodeList); err != nil {
		return exposure.ProviderBare
	}
	return exposure.DetectProvider(nodeList.Items)
}

// exposureOptions builds the effective exposure options for a node, folding in
// cluster level annotations as defaults.
func (s *Server) exposureOptions(ctx context.Context, node *v1alpha1.DDBNode, req ExposeNodeRequest) exposure.Options {
	provider := s.detectProvider(ctx)
	opts := exposure.Options{
		Mode:        exposure.ParseMode(req.Mode, provider.DefaultMode()),
		Scheme:      exposure.ParseScheme(req.Scheme, exposure.SchemeInternetFacing),
		Provider:    provider,
		NodePort:    req.NodePort,
		Annotations: req.Annotations,
	}.Normalize()

	cluster := &v1alpha1.DolphinDBCluster{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: node.Spec.ClusterName}, cluster); err == nil {
		merged := map[string]string{}
		for k, v := range cluster.Spec.Networking.Annotations {
			merged[k] = v
		}
		for k, v := range req.Annotations {
			merged[k] = v
		}
		if len(merged) > 0 {
			opts.Annotations = merged
		}
		// Native HTTPS is terminated by DolphinDB on the same port, so the exposed
		// URL has to be advertised as https:// rather than http://.
		opts.TLS = cluster.Spec.Security.TLS != nil && cluster.Spec.Security.TLS.Enabled
	}
	return opts
}

// resolveNodeEndpoint reads the live objects back and turns them into a usable
// external address.
func (s *Server) resolveNodeEndpoint(ctx context.Context, node *v1alpha1.DDBNode) exposure.Endpoint {
	svc := &corev1.Service{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: exposure.ServiceName(node.Name)}, svc); err != nil {
		return exposure.Endpoint{Mode: exposure.ModeClusterIP, Message: "node is not exposed outside the cluster"}
	}
	k8sNode := &corev1.Node{}
	_ = s.client.Get(ctx, client.ObjectKey{Name: node.Status.NodeName}, k8sNode)
	return exposure.ResolveEndpoint(node, k8sNode, svc)
}

func exposeNodeResponse(ep exposure.Endpoint, provider exposure.Provider, svcName string) map[string]interface{} {
	return map[string]interface{}{
		"success":      true,
		"serviceName":  svcName,
		"provider":     provider,
		"providerName": provider.DisplayName(),
		"mode":         ep.Mode,
		"scheme":       ep.Scheme,
		"nodePort":     ep.Port,
		"externalURL":  ep.URL,
		"pending":      ep.Pending,
		"message":      ep.Message,
		"tls":          ep.TLS,
	}
}

// exposeNodeService publishes a single DolphinDB node outside the cluster.
//
// On a managed cloud the default is a Service of type LoadBalancer: the cloud
// control plane provisions the load balancer *and* wires the security groups,
// so clicking "expose" yields a working endpoint with no manual steps and no
// IRSA - which matters on projects whose SCP denies OIDC provider management.
// On bare metal it falls back to NodePort.
func (s *Server) exposeNodeService(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	var req ExposeNodeRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	node := &v1alpha1.DDBNode{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, node); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("DDBNode %s/%s not found: %v", namespace, name, err))
		return
	}

	opts := s.exposureOptions(ctx, node, req)
	if opts.Mode == exposure.ModeClusterIP {
		s.unexposeNodeService(w, r, namespace, name)
		return
	}
	desired := exposure.BuildService(node, opts)

	existing := &corev1.Service{}
	err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: desired.Name}, existing)
	created := false
	switch {
	case errors.IsNotFound(err):
		if err := s.client.Create(ctx, desired); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create exposure service: %v", err))
			return
		}
		created = true
	case err != nil:
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to query exposure service: %v", err))
		return
	case !exposure.Matches(existing, desired):
		// Changing the type or annotations must happen in place: recreating the
		// Service would churn addresses and leak the previous load balancer.
		existing.Spec.Type = desired.Spec.Type
		if existing.Annotations == nil {
			existing.Annotations = map[string]string{}
		}
		for k, v := range desired.Annotations {
			existing.Annotations[k] = v
		}
		if existing.Labels == nil {
			existing.Labels = map[string]string{}
		}
		for k, v := range desired.Labels {
			existing.Labels[k] = v
		}
		if len(existing.Spec.Ports) == len(desired.Spec.Ports) {
			for i := range desired.Spec.Ports {
				if desired.Spec.Ports[i].NodePort > 0 {
					existing.Spec.Ports[i].NodePort = desired.Spec.Ports[i].NodePort
				}
			}
		}
		if err := s.client.Update(ctx, existing); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to update exposure service: %v", err))
			return
		}
	}

	ep := s.resolveNodeEndpoint(ctx, node)
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	jsonResponse(w, status, exposeNodeResponse(ep, opts.Provider, desired.Name))
}

// getNodeExposure reports the current exposure of a node without changing it, so
// the UI can poll while the cloud finishes provisioning a load balancer.
func (s *Server) getNodeExposure(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	node := &v1alpha1.DDBNode{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, node); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("DDBNode %s/%s not found: %v", namespace, name, err))
		return
	}
	provider := s.detectProvider(ctx)
	ep := s.resolveNodeEndpoint(ctx, node)
	jsonResponse(w, http.StatusOK, exposeNodeResponse(ep, provider, exposure.ServiceName(node.Name)))
}

func (s *Server) unexposeNodeService(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	svcName := exposure.ServiceName(name)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: namespace,
		},
	}
	if err := s.client.Delete(ctx, svc); err != nil && !errors.IsNotFound(err) {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to delete service %s: %v", svcName, err))
		return
	}
	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Service %s deleted; any cloud load balancer behind it is released automatically", svcName),
	})
}

type RollingRestartRequest struct {
	NodeType string `json:"nodeType,omitempty"`
}

func (s *Server) triggerRollingRestart(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	var req RollingRestartRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	opName := fmt.Sprintf("restart-%s-%d", name, time.Now().Unix())
	params := map[string]string{}
	if req.NodeType != "" {
		params["nodeType"] = req.NodeType
	}

	op := &v1alpha1.DDBOperation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opName,
			Namespace: namespace,
			Labels: map[string]string{
				"dolphindb.io/cluster-name": name,
			},
		},
		Spec: v1alpha1.DDBOperationSpec{
			Cluster:    name,
			Type:       v1alpha1.OpRollingRestart,
			Parameters: params,
		},
	}

	if err := s.client.Create(ctx, op); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create DDBOperation: %v", err))
		return
	}

	jsonResponse(w, http.StatusCreated, op)
}

type ScaleRequest struct {
	DataNodes    *int32 `json:"dataNodes,omitempty"`
	ComputeNodes *int32 `json:"computeNodes,omitempty"`
}

func (s *Server) scaleCluster(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	var req ScaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	cluster := &v1alpha1.DolphinDBCluster{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Cluster %s/%s not found", namespace, name))
		return
	}

	repl := getDfsReplicationFactor(cluster)

	if req.DataNodes != nil {
		if *req.DataNodes < repl {
			errorResponse(w, http.StatusBadRequest, fmt.Sprintf(
				"Cannot scale DataNodes to %d: cluster dfsReplicationFactor is %d. DataNodes must be >= %d to prevent chunk replica insufficiency.",
				*req.DataNodes, repl, repl))
			return
		}
		if cluster.Spec.DataNode == nil {
			cluster.Spec.DataNode = &v1alpha1.DataNodeGroupSpec{Replicas: *req.DataNodes}
		} else {
			cluster.Spec.DataNode.Replicas = *req.DataNodes
		}
	}

	if req.ComputeNodes != nil {
		if *req.ComputeNodes < 0 {
			errorResponse(w, http.StatusBadRequest, "computeNodes cannot be negative")
			return
		}
		if cluster.Spec.ComputeNode == nil {
			cluster.Spec.ComputeNode = &v1alpha1.ComputeNodeGroupSpec{Replicas: *req.ComputeNodes}
		} else {
			cluster.Spec.ComputeNode.Replicas = *req.ComputeNodes
		}
	}

	if err := s.client.Update(ctx, cluster); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to scale cluster: %v", err))
		return
	}

	jsonResponse(w, http.StatusOK, cluster)
}

func (s *Server) triggerRebalance(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	opName := fmt.Sprintf("rebalance-%s-%d", name, time.Now().Unix())
	op := &v1alpha1.DDBOperation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opName,
			Namespace: namespace,
			Labels: map[string]string{
				"dolphindb.io/cluster-name": name,
			},
		},
		Spec: v1alpha1.DDBOperationSpec{
			Cluster: name,
			Type:    v1alpha1.OpRebalanceChunks,
		},
	}

	if err := s.client.Create(ctx, op); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create DDBOperation: %v", err))
		return
	}

	jsonResponse(w, http.StatusCreated, op)
}

func (s *Server) triggerReloadLicense(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	opName := fmt.Sprintf("reload-license-%s-%d", name, time.Now().Unix())
	op := &v1alpha1.DDBOperation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opName,
			Namespace: namespace,
			Labels: map[string]string{
				"dolphindb.io/cluster-name": name,
			},
		},
		Spec: v1alpha1.DDBOperationSpec{
			Cluster: name,
			Type:    v1alpha1.OpReloadLicense,
		},
	}

	if err := s.client.Create(ctx, op); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create ReloadLicense operation: %v", err))
		return
	}

	jsonResponse(w, http.StatusCreated, op)
}

func (s *Server) handleOperations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method == http.MethodGet {
		opList := &v1alpha1.DDBOperationList{}
		listOpts := []client.ListOption{}
		ns := r.URL.Query().Get("namespace")
		if ns == "" && s.namespace != "" {
			ns = s.namespace
		}
		if ns != "" {
			listOpts = append(listOpts, client.InNamespace(ns))
		}

		if err := s.client.List(ctx, opList, listOpts...); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list operations: %v", err))
			return
		}

		clusterFilter := r.URL.Query().Get("cluster")
		filtered := make([]v1alpha1.DDBOperation, 0, len(opList.Items))
		for _, op := range opList.Items {
			if clusterFilter == "" || op.Spec.Cluster == clusterFilter {
				filtered = append(filtered, op)
			}
		}

		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].CreationTimestamp.After(filtered[j].CreationTimestamp.Time)
		})

		jsonResponse(w, http.StatusOK, filtered)
		return
	}

	if r.Method == http.MethodPost {
		var op v1alpha1.DDBOperation
		if err := json.NewDecoder(r.Body).Decode(&op); err != nil {
			errorResponse(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		if op.Name == "" {
			op.Name = fmt.Sprintf("op-%s-%d", op.Spec.Cluster, time.Now().Unix())
		}
		if op.Namespace == "" {
			op.Namespace = s.namespace
			if op.Namespace == "" {
				op.Namespace = "default"
			}
		}
		if err := s.client.Create(ctx, &op); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create operation: %v", err))
			return
		}
		jsonResponse(w, http.StatusCreated, op)
		return
	}

	errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
}

func (s *Server) handleOperationSubRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/operations/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 2 && r.Method == http.MethodDelete {
		ctx := r.Context()
		namespace, name := parts[0], parts[1]
		op := &v1alpha1.DDBOperation{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		}
		if err := s.client.Delete(ctx, op); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to delete operation: %v", err))
			return
		}
		jsonResponse(w, http.StatusOK, map[string]string{"message": "Operation deleted"})
		return
	}
	errorResponse(w, http.StatusNotFound, "Route not found")
}

type CreateBackupRequest struct {
	Cluster    string   `json:"cluster"`
	Namespace  string   `json:"namespace"`
	Name       string   `json:"name,omitempty"`
	Databases  []string `json:"databases"`
	Tables     []string `json:"tables,omitempty"`
	BackupPath string   `json:"backupPath,omitempty"`
	Schedule   string   `json:"schedule,omitempty"`
	Parallel   bool     `json:"parallel,omitempty"`
	Snapshot   bool     `json:"snapshot,omitempty"`
	Force      bool     `json:"force,omitempty"`
}

func (s *Server) handleBackups(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method == http.MethodGet {
		bList := &v1alpha1.DDBBackupList{}
		listOpts := []client.ListOption{}
		ns := r.URL.Query().Get("namespace")
		if ns == "" && s.namespace != "" {
			ns = s.namespace
		}
		if ns != "" {
			listOpts = append(listOpts, client.InNamespace(ns))
		}

		if err := s.client.List(ctx, bList, listOpts...); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list backups: %v", err))
			return
		}
		sort.Slice(bList.Items, func(i, j int) bool {
			return bList.Items[i].CreationTimestamp.After(bList.Items[j].CreationTimestamp.Time)
		})
		jsonResponse(w, http.StatusOK, bList.Items)
		return
	}

	if r.Method == http.MethodPost {
		var req CreateBackupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			errorResponse(w, http.StatusBadRequest, "Invalid request body")
			return
		}

		if req.Cluster == "" || len(req.Databases) == 0 {
			errorResponse(w, http.StatusBadRequest, "spec.cluster and spec.databases are required")
			return
		}

		ns := req.Namespace
		if ns == "" {
			ns = s.namespace
			if ns == "" {
				ns = "default"
			}
		}

		bName := req.Name
		if bName == "" {
			bName = fmt.Sprintf("backup-%s-%d", req.Cluster, time.Now().Unix())
		}

		snapshotVal := req.Snapshot
		backup := &v1alpha1.DDBBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bName,
				Namespace: ns,
				Labels: map[string]string{
					"dolphindb.io/cluster-name": req.Cluster,
				},
			},
			Spec: v1alpha1.DDBBackupSpec{
				Cluster:    req.Cluster,
				Databases:  req.Databases,
				Tables:     req.Tables,
				BackupPath: req.BackupPath,
				Schedule:   req.Schedule,
				Parallel:   req.Parallel,
				Snapshot:   &snapshotVal,
				Force:      req.Force,
			},
		}

		if err := s.client.Create(ctx, backup); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create backup: %v", err))
			return
		}

		jsonResponse(w, http.StatusCreated, backup)
		return
	}

	errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
}

func (s *Server) handleBackupSubRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/backups/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 2 && r.Method == http.MethodDelete {
		ctx := r.Context()
		namespace, name := parts[0], parts[1]
		b := &v1alpha1.DDBBackup{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		}
		if err := s.client.Delete(ctx, b); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to delete backup: %v", err))
			return
		}
		jsonResponse(w, http.StatusOK, map[string]string{"message": "Backup deleted"})
		return
	}
	errorResponse(w, http.StatusNotFound, "Route not found")
}

type CreateRestoreRequest struct {
	Cluster        string `json:"cluster"`
	Namespace      string `json:"namespace"`
	Name           string `json:"name,omitempty"`
	BackupName     string `json:"backupName,omitempty"`
	BackupPath     string `json:"backupPath,omitempty"`
	Database       string `json:"database"`
	Table          string `json:"table,omitempty"`
	TargetDatabase string `json:"targetDatabase,omitempty"`
	TargetTable    string `json:"targetTable,omitempty"`
}

func (s *Server) handleRestores(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method == http.MethodGet {
		rList := &v1alpha1.DDBRestoreList{}
		listOpts := []client.ListOption{}
		ns := r.URL.Query().Get("namespace")
		if ns == "" && s.namespace != "" {
			ns = s.namespace
		}
		if ns != "" {
			listOpts = append(listOpts, client.InNamespace(ns))
		}

		if err := s.client.List(ctx, rList, listOpts...); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list restores: %v", err))
			return
		}
		sort.Slice(rList.Items, func(i, j int) bool {
			return rList.Items[i].CreationTimestamp.After(rList.Items[j].CreationTimestamp.Time)
		})
		jsonResponse(w, http.StatusOK, rList.Items)
		return
	}

	if r.Method == http.MethodPost {
		var req CreateRestoreRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			errorResponse(w, http.StatusBadRequest, "Invalid request body")
			return
		}

		if req.Cluster == "" || req.Database == "" {
			errorResponse(w, http.StatusBadRequest, "spec.cluster and spec.database are required")
			return
		}

		ns := req.Namespace
		if ns == "" {
			ns = s.namespace
			if ns == "" {
				ns = "default"
			}
		}

		rName := req.Name
		if rName == "" {
			rName = fmt.Sprintf("restore-%s-%d", req.Cluster, time.Now().Unix())
		}

		targetDB := req.TargetDatabase
		if targetDB == "" {
			targetDB = req.Database
		}

		restore := &v1alpha1.DDBRestore{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rName,
				Namespace: ns,
				Labels: map[string]string{
					"dolphindb.io/cluster-name": req.Cluster,
				},
			},
			Spec: v1alpha1.DDBRestoreSpec{
				Cluster:        req.Cluster,
				BackupName:     req.BackupName,
				BackupPath:     req.BackupPath,
				Database:       req.Database,
				Table:          req.Table,
				TargetDatabase: targetDB,
				TargetTable:    req.TargetTable,
			},
		}

		if err := s.client.Create(ctx, restore); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create restore: %v", err))
			return
		}

		jsonResponse(w, http.StatusCreated, restore)
		return
	}

	errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
}

func (s *Server) handleRestoreSubRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/restores/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 2 && r.Method == http.MethodDelete {
		ctx := r.Context()
		namespace, name := parts[0], parts[1]
		rest := &v1alpha1.DDBRestore{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		}
		if err := s.client.Delete(ctx, rest); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to delete restore: %v", err))
			return
		}
		jsonResponse(w, http.StatusOK, map[string]string{"message": "Restore deleted"})
		return
	}
	errorResponse(w, http.StatusNotFound, "Route not found")
}

// LicenseInfo represents detailed license status for a cluster
type LicenseInfo struct {
	ClusterName      string   `json:"clusterName"`
	Namespace        string   `json:"namespace"`
	Mode             string   `json:"mode"`
	Version          string   `json:"version"`
	LicenseMode      string   `json:"licenseMode"`
	SecretName       string   `json:"secretName"`
	Expiration       string   `json:"expiration"`
	DaysRemaining    int      `json:"daysRemaining"`
	Status           string   `json:"status"` // "active", "expiring_soon", "expired", "unknown"
	ClientName       string   `json:"clientName"`
	Authorization    string   `json:"authorization"`
	MaxNodes         string   `json:"maxNodes"`
	MaxCoresPerNode  string   `json:"maxCoresPerNode"`
	MaxMemoryPerNode string   `json:"maxMemoryPerNode"`
	Modules          string   `json:"modules"`
	ModuleList       []string `json:"moduleList"`
	ProductKey       string   `json:"productKey"`
	Available        bool     `json:"available"`
	Message          string   `json:"message,omitempty"`
}

type NodeFingerprint struct {
	Name           string `json:"name"`
	Hostname       string `json:"hostname"`
	IP             string `json:"ip"`
	DDBFingerprint string `json:"ddbFingerprint"`
	MachineID      string `json:"machineId"`
	SystemUUID     string `json:"systemUuid"`
	OS             string `json:"os"`
	Kernel         string `json:"kernel"`
	Arch           string `json:"arch"`
	Status         string `json:"status"`
	Source         string `json:"source"`
}

func (s *Server) getClusterLicenseDetails(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) LicenseInfo {
	info := LicenseInfo{
		ClusterName:   cluster.Name,
		Namespace:     cluster.Namespace,
		Mode:          string(cluster.Spec.Mode),
		Version:       cluster.Spec.Version,
		LicenseMode:   string(cluster.Spec.License.Mode),
		SecretName:    cluster.Spec.License.SecretName,
		Status:        "unknown",
		Authorization: "commercial",
	}
	if info.LicenseMode == "" {
		info.LicenseMode = "machineFingerprint"
	}
	if info.SecretName == "" {
		info.SecretName = "dolphindb-license"
	}

	// 1. Try querying live DolphinDB instance
	targetPodName := fmt.Sprintf("%s-ctl-0", cluster.Name)
	port := int32(8900)
	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		targetPodName = fmt.Sprintf("%s-single-0", cluster.Name)
		port = 8848
		if cluster.Spec.Standalone != nil && cluster.Spec.Standalone.Port > 0 {
			port = cluster.Spec.Standalone.Port
		}
	} else if cluster.Spec.Controller != nil && cluster.Spec.Controller.Port > 0 {
		port = cluster.Spec.Controller.Port
	}

	pod := &corev1.Pod{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: targetPodName}, pod); err == nil &&
		pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" {
		adminUser, adminPass := credentials.Resolve(ctx, s.client, cluster.Namespace, cluster.Spec.Security.AdminSecretName)
		cli := ddbclient.NewClient(fmt.Sprintf("http://%s:%d", pod.Status.PodIP, port), adminUser, adminPass)
		if details, err := cli.GetLicenseDetails(ctx); err == nil && len(details) > 0 {
			info.Available = true
			info.Expiration = details["expiration"]
			info.ClientName = details["clientName"]
			if auth, ok := details["authorization"]; ok && auth != "" {
				info.Authorization = auth
			}
			info.MaxNodes = details["maxNodes"]
			info.MaxCoresPerNode = details["maxCoresPerNode"]
			info.MaxMemoryPerNode = details["maxMemoryPerNode"]
			info.Modules = details["modules"]
			if modNames, ok := details["moduleNames"]; ok {
				info.ModuleList = strings.Fields(modNames)
			}
			info.ProductKey = details["productKey"]

			if info.Expiration != "" {
				if expDate, err := time.Parse("2006.01.02", info.Expiration); err == nil {
					diff := time.Until(expDate)
					info.DaysRemaining = int(diff.Hours() / 24)
					if info.DaysRemaining < 0 {
						info.Status = "expired"
					} else if info.DaysRemaining <= 30 {
						info.Status = "expiring_soon"
					} else {
						info.Status = "active"
					}
				}
			}
			return info
		}
	}

	// 2. Fallback to inspecting Kubernetes Secret
	sec := &corev1.Secret{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: info.SecretName}, sec); err == nil && sec.Data != nil {
		var licBytes []byte
		if b, ok := sec.Data["dolphindb.lic"]; ok && len(b) > 0 {
			licBytes = b
		} else {
			for _, b := range sec.Data {
				if len(b) > 0 {
					licBytes = b
					break
				}
			}
		}
		if len(licBytes) > 0 {
			lines := strings.Split(string(licBytes), "\n")
			if len(lines) > 0 {
				parts := strings.Split(strings.TrimSpace(lines[0]), ",")
				if len(parts) >= 10 {
					expStr := ""
					if len(parts) >= 13 && parts[12] != "" {
						expStr = parts[12]
					} else {
						expStr = parts[0]
					}
					info.Available = true
					info.Expiration = expStr
					info.ClientName = parts[4]
					info.MaxNodes = parts[6]
					info.MaxCoresPerNode = parts[2]
					info.MaxMemoryPerNode = parts[1]
					if len(parts) > 9 && parts[9] != "" {
						info.Authorization = parts[9]
					}
					if expDate, err := time.Parse("2006.01.02", expStr); err == nil {
						diff := time.Until(expDate)
						info.DaysRemaining = int(diff.Hours() / 24)
						if info.DaysRemaining < 0 {
							info.Status = "expired"
						} else if info.DaysRemaining <= 30 {
							info.Status = "expiring_soon"
						} else {
							info.Status = "active"
						}
					}
					return info
				}
			}
		}
	}

	info.Message = "Cluster pods offline or license details pending startup"
	return info
}

func (s *Server) handleAllLicenses(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusterList := &v1alpha1.DolphinDBClusterList{}
	listOpts := []client.ListOption{}
	ns := r.URL.Query().Get("namespace")
	if ns == "" && s.namespace != "" {
		ns = s.namespace
	}
	if ns != "" {
		listOpts = append(listOpts, client.InNamespace(ns))
	}
	if err := s.client.List(ctx, clusterList, listOpts...); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list clusters: %v", err))
		return
	}

	var results []LicenseInfo
	for _, c := range clusterList.Items {
		results = append(results, s.getClusterLicenseDetails(ctx, &c))
	}
	jsonResponse(w, http.StatusOK, results)
}

func (s *Server) handleClusterLicense(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	cluster := &v1alpha1.DolphinDBCluster{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Cluster %s/%s not found", namespace, name))
		return
	}
	info := s.getClusterLicenseDetails(ctx, cluster)
	jsonResponse(w, http.StatusOK, info)
}

func queryDDBFingerprint(ctx context.Context, podIP string, port int) (string, error) {
	if podIP == "" || port <= 0 {
		return "", fmt.Errorf("invalid podIP or port")
	}
	url := fmt.Sprintf("http://%s:%d/", podIP, port)
	// This runs against a throwaway probe Pod built from the vanilla DolphinDB
	// image with no configuration, so it always uses the factory credentials
	// rather than the target cluster's Secret.
	script := credentials.LoginScript(credentials.AdminUser, credentials.DefaultAdminPassword) + ` generateMachineFingerprint("/tmp"); concat(file("/tmp/MachineFingerprint").readLines(), "\n")`

	payload := map[string]interface{}{
		"functionName": "executeCode",
		"params": []map[string]string{
			{
				"name":  "script",
				"form":  "scalar",
				"type":  "string",
				"value": script,
			},
		},
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Close = true

	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy:             nil, // Bypass any proxy
			DisableKeepAlives: true,
		},
		Timeout: 4 * time.Second,
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		ResultCode string `json:"resultCode"`
		Msg        string `json:"msg"`
		Object     []struct {
			Value string `json:"value"`
		} `json:"object"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if result.ResultCode != "0" {
		return "", fmt.Errorf("ddb error: %s", result.Msg)
	}
	if len(result.Object) > 0 && result.Object[0].Value != "" {
		return strings.TrimSpace(result.Object[0].Value), nil
	}
	return "", fmt.Errorf("empty fingerprint returned")
}

func (s *Server) runProbePodForNode(ctx context.Context, nodeName, targetNs string) (string, error) {
	if targetNs == "" {
		targetNs = "default"
	}
	cleanNode := strings.ReplaceAll(strings.ToLower(nodeName), ".", "-")
	podName := fmt.Sprintf("ddb-probe-%s-%d", cleanNode, time.Now().Unix()%100000)

	probePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: targetNs,
			Labels: map[string]string{
				"app.kubernetes.io/name": "ddb-fingerprint-probe",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:      nodeName,
			RestartPolicy: corev1.RestartPolicyNever,
			Volumes: []corev1.Volume{
				{
					Name: "host-machine-id",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/etc/machine-id",
						},
					},
				},
				{
					Name: "host-etc",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/etc",
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "probe",
					Image: "192.168.100.250/dolphindb/dolphindb:v3.00.6",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "host-machine-id",
							MountPath: "/etc/machine-id",
							ReadOnly:  true,
						},
						{
							Name:      "host-etc",
							MountPath: "/dolphindb/etc",
							ReadOnly:  true,
						},
					},
					Command: []string{"/bin/sh", "-c"},
					Args: []string{
						// Throwaway probe Pod from the vanilla image: factory credentials apply.
						fmt.Sprintf(`echo '%s generateMachineFingerprint("/tmp");' > /tmp/gen.dos
/data/ddb/server/dolphindb -console 0 -mode single -localSite 127.0.0.1:9999:tmpnode -startup /tmp/gen.dos &
for i in $(seq 1 40); do
  if [ -f /tmp/MachineFingerprint ]; then
    echo "===FINGERPRINT_BEGIN==="
    cat /tmp/MachineFingerprint
    echo "===FINGERPRINT_END==="
    exit 0
  fi
  sleep 0.2
done
exit 1`, credentials.LoginScript(credentials.AdminUser, credentials.DefaultAdminPassword)),
					},
				},
			},
		},
	}

	if err := s.client.Create(ctx, probePod); err != nil {
		return "", fmt.Errorf("failed to create probe pod: %w", err)
	}
	defer func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.client.Delete(delCtx, probePod)
	}()

	// Wait up to 10s for pod completion
	for i := 0; i < 25; i++ {
		time.Sleep(400 * time.Millisecond)
		freshPod := &corev1.Pod{}
		if err := s.client.Get(ctx, client.ObjectKey{Namespace: targetNs, Name: podName}, freshPod); err != nil {
			continue
		}
		if freshPod.Status.Phase == corev1.PodSucceeded || freshPod.Status.Phase == corev1.PodFailed {
			break
		}
	}

	if s.kubeClient != nil {
		req := s.kubeClient.CoreV1().Pods(targetNs).GetLogs(podName, &corev1.PodLogOptions{})
		stream, err := req.Stream(ctx)
		if err == nil {
			defer stream.Close()
			buf := new(bytes.Buffer)
			_, _ = io.Copy(buf, stream)
			out := buf.String()
			start := strings.Index(out, "===FINGERPRINT_BEGIN===")
			end := strings.Index(out, "===FINGERPRINT_END===")
			if start != -1 && end != -1 && end > start {
				fp := strings.TrimSpace(out[start+len("===FINGERPRINT_BEGIN===") : end])
				if fp != "" {
					s.fpMu.Lock()
					s.fingerprintCache[nodeName] = fp
					s.fpMu.Unlock()
					return fp, nil
				}
			}
		}
	}

	return "", fmt.Errorf("probe pod timed out or failed to output fingerprint")
}

func (s *Server) handleNodeFingerprints(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	nodeList := &corev1.NodeList{}
	if err := s.client.List(ctx, nodeList); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list nodes: %v", err))
		return
	}

	// List DolphinDB pods across cluster to find live pods per physical node
	podList := &corev1.PodList{}
	_ = s.client.List(ctx, podList, client.MatchingLabels{"app.kubernetes.io/name": "dolphindb"})

	// Map nodeName -> live pod
	nodePodMap := make(map[string]*corev1.Pod)
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Status.Phase == corev1.PodRunning && p.Status.PodIP != "" && p.Spec.NodeName != "" {
			curr := nodePodMap[p.Spec.NodeName]
			if curr == nil {
				nodePodMap[p.Spec.NodeName] = p
			} else if curr.Labels["app.kubernetes.io/component"] == "controller" && p.Labels["app.kubernetes.io/component"] != "controller" {
				nodePodMap[p.Spec.NodeName] = p
			}
		}
	}

	var items []NodeFingerprint
	for _, n := range nodeList.Items {
		hostname := n.Name
		ip := ""
		for _, addr := range n.Status.Addresses {
			if addr.Type == corev1.NodeHostName && addr.Address != "" {
				hostname = addr.Address
			}
			if addr.Type == corev1.NodeInternalIP && ip == "" {
				ip = addr.Address
			}
		}
		status := "Ready"
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady {
				if c.Status != corev1.ConditionTrue {
					status = "NotReady"
				}
				break
			}
		}

		s.fpMu.RLock()
		ddbFP := s.fingerprintCache[n.Name]
		s.fpMu.RUnlock()

		source := "cached"
		if ddbFP == "" {
			if p, ok := nodePodMap[n.Name]; ok {
				port := 8900
				switch p.Labels["app.kubernetes.io/component"] {
				case "datanode":
					port = 8901
				case "computenode":
					port = 8902
				case "single":
					port = 8848
				}
				for _, c := range p.Spec.Containers {
					for _, cp := range c.Ports {
						if cp.ContainerPort > 0 {
							port = int(cp.ContainerPort)
							break
						}
					}
				}

				fp, err := queryDDBFingerprint(ctx, p.Status.PodIP, port)
				if err == nil && fp != "" {
					ddbFP = fp
					source = "live-pod"
					s.fpMu.Lock()
					s.fingerprintCache[n.Name] = fp
					s.fpMu.Unlock()
				}
			}
		}

		if ddbFP == "" {
			source = "pending"
		}

		item := NodeFingerprint{
			Name:           n.Name,
			Hostname:       hostname,
			IP:             ip,
			DDBFingerprint: ddbFP,
			MachineID:      n.Status.NodeInfo.MachineID,
			SystemUUID:     n.Status.NodeInfo.SystemUUID,
			OS:             n.Status.NodeInfo.OSImage,
			Kernel:         n.Status.NodeInfo.KernelVersion,
			Arch:           n.Status.NodeInfo.Architecture,
			Status:         status,
			Source:         source,
		}
		items = append(items, item)
	}

	var reportLines []string
	reportLines = append(reportLines, "==================================================")
	reportLines = append(reportLines, "DolphinDB 商业版许可证申请表 (官方 generateMachineFingerprint)")
	reportLines = append(reportLines, fmt.Sprintf("生成时间: %s", time.Now().Format("2006-01-02 15:04:05")))
	reportLines = append(reportLines, "==================================================")

	for i, item := range items {
		reportLines = append(reportLines, "")
		reportLines = append(reportLines, fmt.Sprintf("【物理节点 %d】: %s", i+1, item.Name))
		reportLines = append(reportLines, fmt.Sprintf("- 物理主机名: %s", item.Hostname))
		reportLines = append(reportLines, fmt.Sprintf("- 物理内网 IP: %s", item.IP))
		reportLines = append(reportLines, fmt.Sprintf("- 操作系统: %s (%s)", item.OS, item.Arch))
		if item.DDBFingerprint != "" {
			reportLines = append(reportLines, "- DolphinDB 机器指纹 (用于商务签发 License):")
			reportLines = append(reportLines, item.DDBFingerprint)
		} else {
			reportLines = append(reportLines, "- DolphinDB 机器指纹: [未采集，请点击一键探针采集]")
		}
	}
	reportLines = append(reportLines, "")
	reportLines = append(reportLines, "==================================================")
	reportLines = append(reportLines, "提示: 请直接复制上述清单发送给 DolphinDB 商务团队，商务将根据各节点的 MachineFingerprint 签发绑机商业许可证。")

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"nodes":          items,
		"copyableReport": strings.Join(reportLines, "\n"),
	})
}

func (s *Server) handleProbeFingerprints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	ctx := r.Context()

	var req struct {
		NodeName string `json:"nodeName"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	nodeList := &corev1.NodeList{}
	if err := s.client.List(ctx, nodeList); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list nodes: %v", err))
		return
	}

	targetNs := "default"
	if s.namespace != "" {
		targetNs = s.namespace
	}

	probed := make(map[string]string)
	var errs []string

	for _, n := range nodeList.Items {
		if req.NodeName != "" && n.Name != req.NodeName {
			continue
		}

		s.fpMu.RLock()
		cached := s.fingerprintCache[n.Name]
		s.fpMu.RUnlock()
		if cached != "" && req.NodeName == "" {
			probed[n.Name] = cached
			continue
		}

		fp, err := s.runProbePodForNode(ctx, n.Name, targetNs)
		if err != nil {
			errs = append(errs, fmt.Sprintf("Node %s: %v", n.Name, err))
		} else {
			probed[n.Name] = fp
		}
	}

	if len(errs) > 0 && len(probed) == 0 {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("探针采集失败: %s", strings.Join(errs, "; ")))
		return
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"probed":  probed,
		"errors":  errs,
		"message": fmt.Sprintf("成功为 %d 个物理节点采集 DolphinDB 官方机器指纹", len(probed)),
	})
}

func (s *Server) handleLicenseUpload(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	var req struct {
		SecretName string `json:"secretName"`
		TargetNode string `json:"targetNode"`
		Content    string `json:"content"`
		Files      []struct {
			Filename string `json:"filename"`
			Content  string `json:"content"`
		} `json:"files"`
		AutoReload bool `json:"autoReload"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON body: %v", err))
		return
	}

	if req.SecretName == "" {
		req.SecretName = "dolphindb-license"
	}

	secretData := make(map[string][]byte)

	for _, f := range req.Files {
		cleanName := strings.TrimSpace(f.Filename)
		if cleanName != "" && f.Content != "" {
			secretData[cleanName] = []byte(f.Content)
		}
	}

	if req.Content != "" {
		if req.TargetNode != "" {
			key := fmt.Sprintf("%s.lic", req.TargetNode)
			secretData[key] = []byte(req.Content)
		} else if len(secretData) == 0 {
			secretData["dolphindb.lic"] = []byte(req.Content)
		}
	}

	if len(secretData) == 0 {
		errorResponse(w, http.StatusBadRequest, "No license file content provided")
		return
	}

	sec := &corev1.Secret{}
	err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: req.SecretName}, sec)
	if errors.IsNotFound(err) {
		sec = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      req.SecretName,
				Namespace: namespace,
				Labels: map[string]string{
					"app.kubernetes.io/name": "dolphindb-license",
				},
			},
			Data: secretData,
		}
		if err := s.client.Create(ctx, sec); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create license Secret: %v", err))
			return
		}
	} else if err == nil {
		if sec.Data == nil {
			sec.Data = make(map[string][]byte)
		}
		for k, v := range secretData {
			sec.Data[k] = v
		}
		if err := s.client.Update(ctx, sec); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to update license Secret: %v", err))
			return
		}
	} else {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch Secret: %v", err))
		return
	}

	var op *v1alpha1.DDBOperation
	if req.AutoReload {
		opName := fmt.Sprintf("reload-license-%s-%d", name, time.Now().Unix())
		op = &v1alpha1.DDBOperation{
			ObjectMeta: metav1.ObjectMeta{
				Name:      opName,
				Namespace: namespace,
				Labels: map[string]string{
					"dolphindb.io/cluster-name": name,
				},
			},
			Spec: v1alpha1.DDBOperationSpec{
				Cluster: name,
				Type:    v1alpha1.OpReloadLicense,
			},
		}
		_ = s.client.Create(ctx, op)
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"message":   fmt.Sprintf("许可证已成功保存至 Secret %s (共 %d 个证书文件)", req.SecretName, len(secretData)),
		"operation": op,
	})
}

// handleLegacyClusters lists discovered legacy clusters
func (s *Server) handleLegacyClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = s.namespace
	}

	clusters, err := migration.DiscoverLegacyClusters(r.Context(), s.client, ns)
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to discover legacy clusters: %v", err))
		return
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"clusters": clusters,
	})
}

// handleLegacyClusterSubRoutes handles /api/migration/legacy-clusters/{ns}/{name}/{action}
func (s *Server) handleLegacyClusterSubRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/migration/legacy-clusters/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 3 {
		errorResponse(w, http.StatusBadRequest, "Invalid URL format, expected /api/migration/legacy-clusters/{namespace}/{name}/{action}")
		return
	}

	namespace := parts[0]
	name := parts[1]
	action := parts[2]

	switch action {
	case "assess":
		if r.Method != http.MethodGet {
			errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		detail, report, err := migration.AnalyzeLegacyCluster(r.Context(), s.client, s.kubeClient, namespace, name)
		if err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to assess legacy cluster: %v", err))
			return
		}
		jsonResponse(w, http.StatusOK, map[string]interface{}{
			"detail": detail,
			"report": report,
		})

	case "convert":
		if r.Method != http.MethodPost {
			errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		var opts migration.ConversionOptions
		if err := json.NewDecoder(r.Body).Decode(&opts); err != nil && err != io.EOF {
			errorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid conversion options: %v", err))
			return
		}
		if opts.TargetNamespace == "" {
			opts.TargetNamespace = namespace
		}

		detail, _, err := migration.AnalyzeLegacyCluster(r.Context(), s.client, s.kubeClient, namespace, name)
		if err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to analyze legacy cluster: %v", err))
			return
		}

		res, err := migration.ConvertLegacyCluster(detail, opts)
		if err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to convert legacy cluster: %v", err))
			return
		}
		jsonResponse(w, http.StatusOK, res)

	case "adopt-step":
		if r.Method != http.MethodPost {
			errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		var req struct {
			StepID        string                     `json:"stepId"`
			TargetCluster *v1alpha1.DolphinDBCluster `json:"targetCluster"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			errorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
			return
		}

		stepRes, err := migration.ExecuteAdoptionStep(r.Context(), s.client, s.kubeClient, namespace, name, req.StepID, req.TargetCluster)
		if err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Step execution failed: %v", err))
			return
		}
		jsonResponse(w, http.StatusOK, stepRes)

	default:
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Unknown migration action: %s", action))
	}
}
