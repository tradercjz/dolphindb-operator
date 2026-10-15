package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	authzv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"

	"dolphindb.io/operator/internal/exposure"
)

// Defaults for the container image offered by the create wizard.
//
// These used to be hardcoded in the browser, pointing at an internal registry
// (192.168.100.250) that does not exist on a cloud. The value is now an
// installation setting so that an air-gapped or on-premise deployment can point
// at its own mirror without editing the dashboard.
const (
	defaultImageRegistry   = "dolphindb"
	defaultImageRepository = "dolphindb"
	defaultImageVersion    = "3.00.6"
)

// NamespaceOption is a namespace the wizard may offer, with whether the operator
// can actually create a cluster there.
type NamespaceOption struct {
	Name string `json:"name"`
	// Writable is false when a SelfSubjectAccessReview says the operator may not
	// create DolphinDBCluster objects in this namespace. Offering such a namespace
	// would only produce a confusing failure on submit.
	Writable bool `json:"writable"`
}

// ExposeDefaults is the per-component exposure the wizard should preselect.
type ExposeDefaults struct {
	Controller  bool `json:"controller"`
	DataNode    bool `json:"datanode"`
	ComputeNode bool `json:"computenode"`
}

// Any reports whether any component would be published.
func (e ExposeDefaults) Any() bool { return e.Controller || e.DataNode || e.ComputeNode }

// PlatformInfo describes what this installation and this cluster can do, so the
// create wizard can default sensibly instead of guessing. Everything here was
// previously either missing (empty dropdowns) or wrong (an internal registry and
// a NodePort exposure default that is unreachable on a cloud).
type PlatformInfo struct {
	Provider             exposure.Provider `json:"provider"`
	ProviderName         string            `json:"providerName"`
	SupportsLoadBalancer bool              `json:"supportsLoadBalancer"`

	// Namespace is where the operator runs, and the natural default for a new
	// cluster: it is guaranteed to be writable.
	Namespace  string            `json:"namespace"`
	Namespaces []NamespaceOption `json:"namespaces"`

	StorageClasses      []StorageClassSummary `json:"storageClasses"`
	DefaultStorageClass string                `json:"defaultStorageClass,omitempty"`

	// ImageRegistry / ImageRepository / DefaultVersion build the image reference
	// shown in the wizard.
	ImageRegistry   string `json:"imageRegistry"`
	ImageRepository string `json:"imageRepository"`
	DefaultVersion  string `json:"defaultVersion"`

	// DefaultExpose is the exposure preselected for a new cluster: nothing on a
	// managed cloud, all components on bare metal.
	DefaultExpose ExposeDefaults `json:"defaultExpose"`
	// DefaultServiceType is what an exposed component becomes by default.
	DefaultServiceType string `json:"defaultServiceType"`
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (s *Server) handlePlatform(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	ctx := r.Context()

	provider := s.detectProvider(ctx)

	info := PlatformInfo{
		Provider:             provider,
		ProviderName:         provider.DisplayName(),
		SupportsLoadBalancer: provider.SupportsLoadBalancer(),
		Namespace:            s.defaultNamespace(),
		ImageRegistry:        envOr("DDB_IMAGE_REGISTRY", defaultImageRegistry),
		ImageRepository:      envOr("DDB_IMAGE_REPOSITORY", defaultImageRepository),
		DefaultVersion:       envOr("DDB_DEFAULT_VERSION", defaultImageVersion),
	}

	if provider.SupportsLoadBalancer() {
		// Nothing preselected: every exposed component costs a load balancer, and a
		// NodePort is firewalled off by the node security group.
		info.DefaultServiceType = string(corev1.ServiceTypeLoadBalancer)
	} else {
		info.DefaultExpose = ExposeDefaults{Controller: true, DataNode: true, ComputeNode: true}
		info.DefaultServiceType = string(corev1.ServiceTypeNodePort)
	}

	info.Namespaces = s.listNamespaceOptions(ctx)
	info.StorageClasses, info.DefaultStorageClass = s.listStorageClasses(ctx)

	jsonResponse(w, http.StatusOK, info)
}

// defaultNamespace is the namespace a new cluster should be created in.
//
// The dashboard is usually started with an empty --namespace (meaning "watch every
// namespace"), so without this the wizard preselected whatever sorted first -
// usually `default`. The pod's own namespace is both guaranteed writable and the
// least surprising choice.
func (s *Server) defaultNamespace() string {
	if s.namespace != "" {
		return s.namespace
	}
	if v := os.Getenv("POD_NAMESPACE"); v != "" {
		return v
	}
	return "default"
}

// listNamespaceOptions lists namespaces and marks the ones the operator may
// create DolphinDBCluster objects in.
func (s *Server) listNamespaceOptions(ctx context.Context) []NamespaceOption {
	nsList := &corev1.NamespaceList{}
	if err := s.client.List(ctx, nsList); err != nil {
		return nil
	}
	out := make([]NamespaceOption, 0, len(nsList.Items))
	for i := range nsList.Items {
		name := nsList.Items[i].Name
		out = append(out, NamespaceOption{Name: name, Writable: s.canCreateClusterIn(ctx, name)})
	}
	sort.Slice(out, func(i, j int) bool {
		// The operator's own namespace first: it is always writable and is the
		// least surprising default.
		if (out[i].Name == s.defaultNamespace()) != (out[j].Name == s.defaultNamespace()) {
			return out[i].Name == s.defaultNamespace()
		}
		if out[i].Writable != out[j].Writable {
			return out[i].Writable
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// canCreateClusterIn asks the API server whether this identity may create a
// DolphinDBCluster in a namespace.
func (s *Server) canCreateClusterIn(ctx context.Context, namespace string) bool {
	review := &authzv1.SelfSubjectAccessReview{
		Spec: authzv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authzv1.ResourceAttributes{
				Namespace: namespace,
				Verb:      "create",
				Group:     "dolphindb.io",
				Resource:  "dolphindbclusters",
			},
		},
	}
	if err := s.client.Create(ctx, review); err != nil {
		// Without an answer, assume writable rather than hiding a usable namespace.
		return true
	}
	return review.Status.Allowed
}

// listStorageClasses returns the available classes plus the name of the default.
func (s *Server) listStorageClasses(ctx context.Context) ([]StorageClassSummary, string) {
	scList := &storagev1.StorageClassList{}
	if err := s.client.List(ctx, scList); err != nil {
		return nil, ""
	}
	drivers := s.installedCSIDrivers(ctx)

	summaries := make([]StorageClassSummary, 0, len(scList.Items))
	for i := range scList.Items {
		sc := scList.Items[i]
		isDefault := sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" ||
			sc.Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true"
		usable, hint := classifyStorageClass(sc.Provisioner, drivers)
		if hint == "" {
			hint = storageClassHint(sc.Provisioner)
		}
		summaries = append(summaries, StorageClassSummary{
			Name:        sc.Name,
			Provisioner: sc.Provisioner,
			IsDefault:   isDefault,
			Usable:      usable,
			Hint:        hint,
		})
	}

	// Prefer: the default class, then anything known to work, then alphabetical.
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].IsDefault != summaries[j].IsDefault {
			return summaries[i].IsDefault
		}
		if summaries[i].Usable != summaries[j].Usable {
			return summaries[i].Usable
		}
		return summaries[i].Name < summaries[j].Name
	})

	// Preselect a class that will actually bind. Falling back to "first in the list"
	// preselected a CSI class whose driver was not installed, so every PVC stayed
	// Pending with no explanation.
	def := ""
	for _, sum := range summaries {
		if sum.Usable {
			def = sum.Name
			break
		}
	}
	return summaries, def
}

// installedCSIDrivers returns the CSI drivers present in the cluster. A driver
// creates its CSIDriver object when it is installed, so its absence is a reliable
// signal that a StorageClass backed by it cannot provision anything.
func (s *Server) installedCSIDrivers(ctx context.Context) map[string]bool {
	out := map[string]bool{}
	list := &storagev1.CSIDriverList{}
	if err := s.client.List(ctx, list); err != nil {
		return out
	}
	for i := range list.Items {
		out[list.Items[i].Name] = true
	}
	return out
}

// classifyStorageClass reports whether a StorageClass is expected to work, and why
// not when it is not.
func classifyStorageClass(provisioner string, drivers map[string]bool) (bool, string) {
	// CSI-backed class: the driver has to be installed.
	if drivers[provisioner] {
		return true, ""
	}
	if strings.Contains(provisioner, ".csi.") || strings.HasSuffix(provisioner, ".csi") {
		return false, fmt.Sprintf("the CSI driver %q is not installed in this cluster, so PVCs would stay Pending", provisioner)
	}
	// The legacy in-tree AWS provisioner was removed from upstream Kubernetes and is
	// not served by modern EKS clusters.
	if provisioner == "kubernetes.io/aws-ebs" || provisioner == "kubernetes.io/aws-efs" {
		return false, "the legacy in-tree AWS provisioner is no longer served by this cluster"
	}
	// Anything else (local-path, NFS, a cloud block provisioner, ...) is assumed to
	// work: the operator has no cheap way to prove otherwise, and hiding a working
	// class would be worse than offering it.
	return true, ""
}

// storageClassHint warns about provisioners that need extra cluster components,
// because choosing one without them leaves PVCs Pending forever with no clue why.
// That is a real trap on a locked-down AWS project, where the EBS CSI driver
// cannot be installed the documented way (it needs IRSA).
func storageClassHint(provisioner string) string {
	switch provisioner {
	case "rancher.io/local-path":
		return "node-local storage: data is tied to one node and is lost if that node is replaced"
	}
	return ""
}
