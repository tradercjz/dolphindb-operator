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
	StoragePresets      []StoragePreset       `json:"storagePresets,omitempty"`

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

// StoragePreset is an opinionated, cloud-aware storage profile offered in the create wizard.
type StoragePreset struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Category     string   `json:"category"`
	Badge        string   `json:"badge,omitempty"`
	Icon         string   `json:"icon,omitempty"`
	Description  string   `json:"description"`
	StorageClass string   `json:"storageClass"`
	Available    bool     `json:"available"`
	Selected     bool     `json:"selected"`
	Tip          string   `json:"tip,omitempty"`
	Features     []string `json:"features,omitempty"`
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
	info.StoragePresets = buildStoragePresets(provider, info.StorageClasses, info.DefaultStorageClass)

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
	drivers := s.csiDriverState(ctx)

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

// csiDriverState is what the cluster knows about CSI drivers, from two sources
// that mean different things.
type csiDriverState struct {
	// registered is what kubelet actually registered, read from CSINode. This is
	// the strong signal: it only lists a driver whose node plugin is running.
	registered map[string]bool
	// declared is what CSIDriver objects exist, read from CSIDriver. This is the
	// weak signal: the object survives the driver, as observed on a real cluster
	// where `efs.csi.aws.com` had a CSIDriver object applied by hand but no plugin
	// running anywhere.
	declared map[string]bool
}

func (s *Server) csiDriverState(ctx context.Context) csiDriverState {
	state := csiDriverState{registered: map[string]bool{}, declared: map[string]bool{}}

	nodes := &storagev1.CSINodeList{}
	if err := s.client.List(ctx, nodes); err == nil {
		for i := range nodes.Items {
			for _, d := range nodes.Items[i].Spec.Drivers {
				state.registered[d.Name] = true
			}
		}
	}

	declared := &storagev1.CSIDriverList{}
	if err := s.client.List(ctx, declared); err == nil {
		for i := range declared.Items {
			state.declared[declared.Items[i].Name] = true
		}
	}
	return state
}

// classifyStorageClass reports whether a StorageClass is expected to work, and why
// not when it is not.
//
// Verified against a real EKS cluster: `ebs-gp3-sc` (ebs.csi.aws.com) and `gp2`
// (kubernetes.io/aws-ebs) both stay Pending forever, because neither the EBS CSI
// driver is installed nor - thanks to CSI migration - is the in-tree provisioner
// still served. Both PVCs reported:
//
//	ExternalProvisioning: Waiting for a volume to be created either by the external
//	provisioner 'ebs.csi.aws.com' or manually by the system administrator.
func classifyStorageClass(provisioner string, drivers csiDriverState) (bool, string) {
	if drivers.registered[provisioner] {
		return true, ""
	}
	// A CSIDriver object without a registered node plugin means the driver was
	// applied but is not running: it cannot provision anything.
	if drivers.declared[provisioner] {
		return false, fmt.Sprintf("the CSI driver %q is declared but no node plugin is running, so PVCs would stay Pending", provisioner)
	}
	if strings.Contains(provisioner, ".csi.") {
		return false, fmt.Sprintf("the CSI driver %q is not installed in this cluster, so PVCs would stay Pending", provisioner)
	}
	// The legacy in-tree AWS provisioner is migrated to the EBS CSI driver on modern
	// clusters, so it fails for the same reason.
	if provisioner == "kubernetes.io/aws-ebs" || provisioner == "kubernetes.io/aws-efs" {
		return false, "the legacy in-tree AWS provisioner is migrated to the EBS/EFS CSI driver on modern clusters; without that driver PVCs stay Pending"
	}
	// Anything else (local-path, NFS, an in-cluster provisioner) is assumed to work:
	// there is no cheap way to prove otherwise, and hiding a working class would be
	// worse than offering it.
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

// buildStoragePresets creates tailored, opinionated storage profiles based on the detected cloud.
func buildStoragePresets(p exposure.Provider, classes []StorageClassSummary, defaultSC string) []StoragePreset {
	findClass := func(nameSub, provSub string) *StorageClassSummary {
		for i := range classes {
			c := &classes[i]
			if nameSub != "" && strings.Contains(strings.ToLower(c.Name), strings.ToLower(nameSub)) {
				return c
			}
			if provSub != "" && strings.Contains(strings.ToLower(c.Provisioner), strings.ToLower(provSub)) {
				return c
			}
		}
		return nil
	}

	hasSC := func(name string) *StorageClassSummary {
		for i := range classes {
			if classes[i].Name == name {
				return &classes[i]
			}
		}
		return nil
	}

	var presets []StoragePreset

	switch p {
	case exposure.ProviderAWS:
		// 1. EBS gp3 - Balanced & Cost-effective (Recommended)
		gp3Class := findClass("gp3", "")
		if gp3Class == nil {
			gp3Class = hasSC("ebs-gp3-sc")
		}
		gp3Name := "ebs-gp3-sc"
		gp3Avail := false
		var gp3Tip string
		if gp3Class != nil {
			gp3Name = gp3Class.Name
			gp3Avail = gp3Class.Usable
			gp3Tip = gp3Class.Hint
		} else {
			gp3Tip = "未创建 ebs-gp3-sc 存储类 (可应用 config/samples/aws_eks_storageclass.yaml)"
		}

		// 2. EBS io2 Block Express - Financial Core / Low Latency
		io2Class := findClass("io2", "")
		if io2Class == nil {
			io2Class = hasSC("ebs-io2-sc")
		}
		io2Name := "ebs-io2-sc"
		io2Avail := false
		var io2Tip string
		if io2Class != nil {
			io2Name = io2Class.Name
			io2Avail = io2Class.Usable
			io2Tip = io2Class.Hint
		} else {
			io2Tip = "未创建 ebs-io2-sc 存储类 (可应用 config/samples/aws_eks_storageclass.yaml)"
		}

		// 3. Local NVMe Instance Store - Extreme Throughput
		localClass := findClass("local", "local-path")
		if localClass == nil {
			localClass = hasSC("local-path")
		}
		localName := "local-path"
		localAvail := false
		var localTip string
		if localClass != nil {
			localName = localClass.Name
			localAvail = localClass.Usable
			localTip = "本地物理盘随实例销毁而重置，建议数据节点副本数配置 dfsReplicationFactor >= 2"
		} else {
			localTip = "未检测到 local-path 存储类"
		}

		presets = []StoragePreset{
			{
				ID:           "gp3",
				Name:         "EBS gp3 弹性云盘",
				Category:     "高性价比通用型 (推荐)",
				Badge:        "首选推荐",
				Icon:         "⚖️",
				Description:  "低成本云块存储，支持在线不停机扩容，IOPS (最高16,000) 与吞吐 (最高1,000MB/s) 独立按需调优",
				StorageClass: gp3Name,
				Available:    gp3Avail,
				Tip:          gp3Tip,
				Features:     []string{"在线热扩容", "吞吐独立调优", "成本仅为 io2 的 20%"},
			},
			{
				ID:           "io2",
				Name:         "EBS io2 Block Express",
				Category:     "金融核心极速型",
				Badge:        "亚毫秒延迟",
				Icon:         "💎",
				Description:  "最高 256,000 IOPS 及 4,000MB/s 吞吐，亚毫秒级稳定时延，99.999% 企业级持久性，支持节点故障跨机热漂移",
				StorageClass: io2Name,
				Available:    io2Avail,
				Tip:          io2Tip,
				Features:     []string{"256K IOPS", "亚毫秒时延", "99.999% 持久性"},
			},
			{
				ID:           "local-nvme",
				Name:         "EC2 本地 NVMe 实例存储",
				Category:     "本地直通极致性能 (i4i/i3en)",
				Badge:        "百万 IOPS",
				Icon:         "🚀",
				Description:  "直通 EC2 物理 PCIe 4.0 NVMe SSD，微秒级极低延迟与数 GB/s 吞吐，突破云盘网络上限；需配合 DFS 多副本容灾",
				StorageClass: localName,
				Available:    localAvail,
				Tip:          localTip,
				Features:     []string{"1,000,000+ IOPS", "微秒级超低延迟", "零网络传输开销"},
			},
			{
				ID:           "custom",
				Name:         "自定义存储类",
				Category:     "高级手动指定",
				Icon:         "⚙️",
				Description:  "自由选择当前 Kubernetes 集群中已存在的任意 StorageClass",
				StorageClass: defaultSC,
				Available:    true,
				Features:     []string{"自由指定", "支持任意 CSI 驱动"},
			},
		}

	case exposure.ProviderAlibaba:
		essdClass := findClass("alicloud-disk", "")
		essdName := "alicloud-disk-topology"
		essdAvail := false
		if essdClass != nil {
			essdName = essdClass.Name
			essdAvail = essdClass.Usable
		}
		localClass := findClass("local", "")
		localName := "local-path"
		localAvail := false
		if localClass != nil {
			localName = localClass.Name
			localAvail = localClass.Usable
		}
		presets = []StoragePreset{
			{
				ID:           "essd",
				Name:         "阿里云 ESSD 云盘",
				Category:     "高性能弹性云块存储",
				Badge:        "推荐",
				Icon:         "⚖️",
				Description:  "极速 ESSD PL1/PL2 云盘，支持在线不停机扩容，高可靠性保障",
				StorageClass: essdName,
				Available:    essdAvail,
				Features:     []string{"在线热扩容", "多副本可靠性"},
			},
			{
				ID:           "local-disk",
				Name:         "阿里云本地 NVMe 盘 (i3)",
				Category:     "本地直通极致性能",
				Badge:        "极致性能",
				Icon:         "🚀",
				Description:  "直通物理本地 NVMe 盘，提供百万级 IOPS 与超低延迟，需配合 DFS 副本容灾",
				StorageClass: localName,
				Available:    localAvail,
				Features:     []string{"百万 IOPS", "零网络损耗"},
			},
			{
				ID:           "custom",
				Name:         "自定义存储类",
				Category:     "高级手动指定",
				Icon:         "⚙️",
				Description:  "自由选择当前 Kubernetes 集群中已存在的任意 StorageClass",
				StorageClass: defaultSC,
				Available:    true,
			},
		}

	default:
		// Bare metal or other providers
		presets = []StoragePreset{
			{
				ID:           "local",
				Name:         "本地路径 / 本地盘 (Local Storage)",
				Category:     "高性能本地存储 (推荐)",
				Badge:        "推荐",
				Icon:         "🚀",
				Description:  "使用宿主机物理磁盘或本地目录，IOPS 最高、延迟最低，适合自建集群生产环境",
				StorageClass: defaultSC,
				Available:    defaultSC != "",
				Features:     []string{"物理极限性能", "零网络损耗"},
			},
			{
				ID:           "custom",
				Name:         "自定义存储类",
				Category:     "高级手动指定",
				Icon:         "⚙️",
				Description:  "自由选择当前 Kubernetes 集群中已存在的任意 StorageClass",
				StorageClass: defaultSC,
				Available:    true,
			},
		}
	}

	// Determine selected preset
	selectedIdx := -1
	for i, p := range presets {
		if p.ID == "custom" {
			continue
		}
		if p.StorageClass != "" && p.StorageClass == defaultSC && p.Available {
			selectedIdx = i
			break
		}
	}
	if selectedIdx == -1 {
		// Pick first available non-custom
		for i, p := range presets {
			if p.ID != "custom" && p.Available {
				selectedIdx = i
				break
			}
		}
	}
	if selectedIdx == -1 {
		selectedIdx = 0
	}
	presets[selectedIdx].Selected = true

	return presets
}
