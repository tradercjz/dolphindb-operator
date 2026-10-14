package migration

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	LegacyGVK = schema.GroupVersionKind{
		Group:   "dolphindb.dolphindb.io",
		Version: "v1",
		Kind:    "DolphinDB",
	}

	LegacyListGVK = schema.GroupVersionKind{
		Group:   "dolphindb.dolphindb.io",
		Version: "v1",
		Kind:    "DolphinDBList",
	}
)

// DiscoverLegacyClusters scans the Kubernetes cluster for legacy DolphinDB CRs
func DiscoverLegacyClusters(ctx context.Context, cli client.Client, ns string) ([]LegacyClusterInfo, error) {
	uList := &unstructured.UnstructuredList{}
	uList.SetGroupVersionKind(LegacyListGVK)

	var listOpts []client.ListOption
	if ns != "" {
		listOpts = append(listOpts, client.InNamespace(ns))
	}

	if err := cli.List(ctx, uList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list legacy DolphinDB CRs: %w", err)
	}

	var results []LegacyClusterInfo
	for _, item := range uList.Items {
		spec, _, _ := unstructured.NestedMap(item.Object, "spec")
		mode, _, _ := unstructured.NestedString(spec, "mode")
		if mode == "" {
			mode = "standalone"
		}
		version, _, _ := unstructured.NestedString(spec, "version")
		storageClassName, _, _ := unstructured.NestedString(spec, "storageClassName")

		statusPhase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		if statusPhase == "" {
			statusPhase = "Unknown"
		}

		dnReplicas, _, _ := unstructured.NestedInt64(spec, "datanode", "replicas")
		ctlReplicas, _, _ := unstructured.NestedInt64(spec, "controller", "replicas")
		cnReplicas, _, _ := unstructured.NestedInt64(spec, "computenode", "replicas")
		totalNodes := int(dnReplicas + ctlReplicas + cnReplicas)
		if totalNodes == 0 {
			totalNodes = 1
		}

		dnDataSize, _, _ := unstructured.NestedString(spec, "datanode", "dataSize")

		results = append(results, LegacyClusterInfo{
			Name:             item.GetName(),
			Namespace:        item.GetNamespace(),
			Mode:             mode,
			Version:          version,
			Status:           statusPhase,
			CreatedAt:        item.GetCreationTimestamp().Time,
			NodeCount:        totalNodes,
			DataSize:         dnDataSize,
			StorageClassName: storageClassName,
		})
	}

	return results, nil
}
