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
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"dolphindb.io/operator/api/v1alpha1"
)

//go:embed ui/*
var uiFS embed.FS

// Server provides the HTTP dashboard and REST API
type Server struct {
	client     client.Client
	kubeClient kubernetes.Interface
	listen     string
	namespace  string
}

func NewServer(cli client.Client, kubeClient kubernetes.Interface, listen string, defaultNamespace string) *Server {
	return &Server{
		client:     cli,
		kubeClient: kubeClient,
		listen:     listen,
		namespace:  defaultNamespace,
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
	YAML         string            `json:"yaml,omitempty"`
	Name         string            `json:"name"`
	Namespace    string            `json:"namespace"`
	Mode         string            `json:"mode"`
	Version      string            `json:"version"`
	Image        string            `json:"image"`
	StorageClass string            `json:"storageClass"`
	Controller   *NodeGroupConfig  `json:"controller,omitempty"`
	DataNode     *NodeGroupConfig  `json:"dataNode,omitempty"`
	ComputeNode  *NodeGroupConfig  `json:"computeNode,omitempty"`
	Standalone   *NodeGroupConfig  `json:"standalone,omitempty"`
	Config       map[string]string `json:"config,omitempty"`
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
					} else if r != nil && r.Host != "" {
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
				version = "3.00.5"
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
						UseHostName:     true,
						NodeName:        "aftersale2",
						Hostname:        "aftersale2",
					},
				},
			}

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

func (s *Server) toggleClusterExpose(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	cluster := &v1alpha1.DolphinDBCluster{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Cluster %s/%s not found: %v", namespace, name, err))
		return
	}

	if cluster.Spec.Networking.ServiceType == corev1.ServiceTypeNodePort {
		cluster.Spec.Networking.ServiceType = corev1.ServiceTypeClusterIP
		cluster.Spec.Networking.ExposureMode = v1alpha1.ExposureClusterIP
	} else {
		cluster.Spec.Networking.ServiceType = corev1.ServiceTypeNodePort
		cluster.Spec.Networking.ExposureMode = v1alpha1.ExposureNodePortDirect
	}

	if err := s.client.Update(ctx, cluster); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to update cluster networking: %v", err))
		return
	}

	// Also directly update K8s service if exists
	svc := &corev1.Service{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, svc); err == nil {
		svc.Spec.Type = cluster.Spec.Networking.ServiceType
		_ = s.client.Update(ctx, svc)
	}

	jsonResponse(w, http.StatusOK, cluster)
}

// ClusterDetail represents comprehensive topology view for a single cluster
type ClusterDetail struct {
	Cluster     v1alpha1.DolphinDBCluster `json:"cluster"`
	Nodes       []NodeInfo                `json:"nodes"`
	Operations  []v1alpha1.DDBOperation   `json:"operations"`
	Backups     []v1alpha1.DDBBackup      `json:"backups"`
	ServiceType string                    `json:"serviceType,omitempty"`
	AccessURL   string                    `json:"accessURL,omitempty"`
}

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

	clusterSvcType := "ClusterIP"
	var clusterAccessURL string
	if clusterSvc, exists := svcMap[name]; exists {
		clusterSvcType = string(clusterSvc.Spec.Type)
		if clusterSvc.Spec.Type == corev1.ServiceTypeNodePort {
			var ctlNodePort int32
			for _, p := range clusterSvc.Spec.Ports {
				if p.NodePort > 0 {
					ctlNodePort = p.NodePort
					break
				}
			}
			if ctlNodePort > 0 {
				hostIP := "192.168.100.44"
				if cluster.Spec.Networking.PublicIP != "" {
					hostIP = cluster.Spec.Networking.PublicIP
				} else if r != nil && r.Host != "" {
					h, _, err := net.SplitHostPort(r.Host)
					if err == nil && h != "" && h != "localhost" && h != "127.0.0.1" {
						hostIP = h
					}
				}
				clusterAccessURL = fmt.Sprintf("http://%s:%d", hostIP, ctlNodePort)
			}
		}
	}

	// Fetch DDBNodes
	ddbNodeList := &v1alpha1.DDBNodeList{}
	_ = s.client.List(ctx, ddbNodeList, client.InNamespace(namespace))

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

		var nodePort int32
		// 1. Check dedicated <node.Name>-ext
		if extSvc, exists := svcMap[n.Name+"-ext"]; exists && extSvc.Spec.Type == corev1.ServiceTypeNodePort {
			for _, p := range extSvc.Spec.Ports {
				if p.NodePort > 0 {
					nodePort = p.NodePort
					break
				}
			}
		}
		// 2. Check cluster-level service <cluster.Name>
		if nodePort == 0 {
			if clusterSvc, exists := svcMap[n.Spec.ClusterName]; exists && clusterSvc.Spec.Type == corev1.ServiceTypeNodePort {
				if n.Spec.Role == v1alpha1.NodeRoleSingle {
					for _, p := range clusterSvc.Spec.Ports {
						if p.Port == n.Spec.Port && p.NodePort > 0 {
							nodePort = p.NodePort
							break
						}
					}
				} else if n.Spec.Role == v1alpha1.NodeRoleController {
					for _, p := range clusterSvc.Spec.Ports {
						if (p.Name == "ctl-port" || p.Port == 8900) && p.NodePort > 0 {
							nodePort = p.NodePort
							break
						}
					}
				}
			}
		}

		var extURL string
		if nodePort > 0 {
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
			if hIP == "" {
				hIP = "192.168.100.44"
			}
			extURL = fmt.Sprintf("http://%s:%d", hIP, nodePort)
		}

		nodes = append(nodes, NodeInfo{
			Name:        n.Name,
			Role:        n.Spec.Role,
			Phase:       string(n.Status.Phase),
			PodIP:       podIP,
			HostIP:      hostIP,
			Port:        n.Spec.Port,
			Ready:       ready,
			ChunksCount: n.Status.ChunksCount,
			DDBState:    n.Status.DDBState,
			NodePort:    nodePort,
			ExternalURL: extURL,
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
		Cluster:     *cluster,
		Nodes:       nodes,
		Operations:  clusterOps,
		Backups:     clusterBackups,
		ServiceType: clusterSvcType,
		AccessURL:   clusterAccessURL,
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
		if r.Method == http.MethodPost {
			s.exposeNodeService(w, r, namespace, name)
			return
		}
		if r.Method == http.MethodDelete {
			s.unexposeNodeService(w, r, namespace, name)
			return
		}
	}

	errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
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
}

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

	svcName := fmt.Sprintf("%s-ext", name)
	svc := &corev1.Service{}
	err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: svcName}, svc)
	if err == nil {
		var allocatedNodePort int32
		for _, p := range svc.Spec.Ports {
			if p.NodePort > 0 {
				allocatedNodePort = p.NodePort
				break
			}
		}
		hostIP := getEffectiveHostIP(node, r)
		jsonResponse(w, http.StatusOK, map[string]interface{}{
			"success":     true,
			"serviceName": svcName,
			"nodePort":    allocatedNodePort,
			"externalURL": fmt.Sprintf("http://%s:%d", hostIP, allocatedNodePort),
		})
		return
	}

	if !errors.IsNotFound(err) {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to query service: %v", err))
		return
	}

	port := node.Spec.Port
	if port == 0 {
		port = 8848
	}

	svcPort := corev1.ServicePort{
		Name:       "web-port",
		Port:       port,
		TargetPort: intstr.FromInt(int(port)),
	}
	if req.NodePort > 0 {
		svcPort.NodePort = req.NodePort
	}

	trueVal := true
	ownerRef := metav1.OwnerReference{
		APIVersion:         "dolphindb.io/v1alpha1",
		Kind:               "DDBNode",
		Name:               node.Name,
		UID:                node.UID,
		Controller:         &trueVal,
		BlockOwnerDeletion: &trueVal,
	}

	newSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "dolphindb",
				"app.kubernetes.io/instance":  node.Spec.ClusterName,
				"app.kubernetes.io/component": string(node.Spec.Role),
				"dolphindb.io/node-alias":     node.Spec.Alias,
				"dolphindb.io/exposed":        "true",
			},
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeNodePort,
			Selector: map[string]string{
				"dolphindb.io/node-alias": node.Spec.Alias,
			},
			Ports: []corev1.ServicePort{svcPort},
		},
	}

	if err := s.client.Create(ctx, newSvc); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create NodePort service: %v", err))
		return
	}

	// Fetch back to inspect assigned NodePort
	createdSvc := &corev1.Service{}
	_ = s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: svcName}, createdSvc)
	var allocatedNodePort int32
	for _, p := range createdSvc.Spec.Ports {
		if p.NodePort > 0 {
			allocatedNodePort = p.NodePort
			break
		}
	}

	hostIP := getEffectiveHostIP(node, r)
	jsonResponse(w, http.StatusCreated, map[string]interface{}{
		"success":     true,
		"serviceName": svcName,
		"nodePort":    allocatedNodePort,
		"externalURL": fmt.Sprintf("http://%s:%d", hostIP, allocatedNodePort),
	})
}

func (s *Server) unexposeNodeService(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	svcName := fmt.Sprintf("%s-ext", name)
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
		"message": fmt.Sprintf("Service %s deleted", svcName),
	})
}

func getEffectiveHostIP(node *v1alpha1.DDBNode, r *http.Request) string {
	if node != nil && node.Status.HostIP != "" {
		return node.Status.HostIP
	}
	if r != nil && r.Host != "" {
		host, _, err := net.SplitHostPort(r.Host)
		if err == nil && host != "" && host != "localhost" && host != "127.0.0.1" {
			return host
		}
	}
	return "192.168.100.44"
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
