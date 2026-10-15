package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/exposure"
)

// componentExposure is the Service type for each cluster Access Service.
//
// Controllers, data nodes and compute nodes are reached on different ports and
// are different pod sets, and a Kubernetes Service can only target one pod set.
// There is therefore no way to publish all three through a single Service, and a
// single cloud load balancer cannot serve them either - each exposed component
// needs its own load balancer.
type componentExposure struct {
	Controller  corev1.ServiceType
	DataNode    corev1.ServiceType
	ComputeNode corev1.ServiceType
}

// ExposedComponents lists the components whose Services are not ClusterIP.
func (c componentExposure) ExposedComponents() []string {
	var out []string
	if c.Controller != corev1.ServiceTypeClusterIP {
		out = append(out, "controller")
	}
	if c.DataNode != corev1.ServiceTypeClusterIP {
		out = append(out, "datanode")
	}
	if c.ComputeNode != corev1.ServiceTypeClusterIP {
		out = append(out, "computenode")
	}
	return out
}

// exposedTypeFor picks the Service type for a component the user asked to expose.
func exposedTypeFor(provider exposure.Provider, requested corev1.ServiceType) corev1.ServiceType {
	if requested != "" {
		return requested
	}
	if provider.SupportsLoadBalancer() {
		// The cloud control plane provisions the load balancer and wires the
		// security groups, so this works with no manual AWS work and no IRSA.
		return corev1.ServiceTypeLoadBalancer
	}
	return corev1.ServiceTypeNodePort
}

// resolveExposure decides the Service type for every Access Service.
//
// Precedence:
//
//  1. `networking.expose` - explicit per-component selection.
//  2. `networking.serviceType` - legacy whole-cluster override.
//  3. Platform default: nothing on a managed cloud, everything on bare metal.
//
// The cloud default matters. The previous default was NodePort for every cluster,
// which on EKS produced a link such as `http://16.176.229.160:32074`: the address
// is public, but the node security group does not allow the port and the operator
// never opens it, so the link was dead while looking perfectly healthy.
func resolveExposure(cluster *v1alpha1.DolphinDBCluster, provider exposure.Provider) componentExposure {
	primary := corev1.ServiceTypeClusterIP

	if net := cluster.Spec.Networking; net.Expose != nil {
		t := exposedTypeFor(provider, net.ServiceType)
		out := componentExposure{Controller: primary, DataNode: primary, ComputeNode: primary}
		if net.Expose.Controller {
			out.Controller = t
		}
		if net.Expose.DataNode {
			out.DataNode = t
		}
		if net.Expose.ComputeNode {
			out.ComputeNode = t
		}
		return out
	}

	if t := cluster.Spec.Networking.ServiceType; t != "" {
		return componentExposure{Controller: t, DataNode: t, ComputeNode: t}
	}

	if provider.SupportsLoadBalancer() {
		// Every exposed component costs a load balancer, so default to exposing
		// nothing rather than to a broken URL.
		return componentExposure{Controller: primary, DataNode: primary, ComputeNode: primary}
	}
	return componentExposure{
		Controller:  corev1.ServiceTypeNodePort,
		DataNode:    corev1.ServiceTypeNodePort,
		ComputeNode: corev1.ServiceTypeNodePort,
	}
}

// detectProvider inspects the Kubernetes nodes to work out which cloud the
// cluster runs on. It degrades to bare metal, i.e. NodePort, when nodes cannot
// be listed.
func (r *DolphinDBClusterReconciler) provider(ctx context.Context) exposure.Provider {
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return exposure.ProviderBare
	}
	return exposure.DetectProvider(nodeList.Items)
}
