package flowengine

import (
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/ddbclient"
)

// BuildRollingRestartFlow constructs a DAG flow to restart cluster nodes sequentially with health gating
func BuildRollingRestartFlow(
	k8sClient client.Client,
	getDDBClient func(cluster *v1alpha1.DolphinDBCluster) *ddbclient.Client,
	targetNodes []string,
	targetAliases []string,
) *TaskFlow {
	steps := []Step{
		&CheckClusterHealthyStep{GetDDBClient: getDDBClient},
	}

	for i, nodeName := range targetNodes {
		alias := targetAliases[i]
		steps = append(steps, &RollingRestartNodeStep{
			K8sClient:    k8sClient,
			GetDDBClient: getDDBClient,
			NodeName:     nodeName,
			NodeAlias:    alias,
		})
	}

	return &TaskFlow{
		Name:  "RollingRestart",
		Steps: steps,
	}
}

// BuildScaleDownFlow constructs a DAG flow to safely drain data chunks before deleting a node
func BuildScaleDownFlow(
	k8sClient client.Client,
	getDDBClient func(cluster *v1alpha1.DolphinDBCluster) *ddbclient.Client,
	candidateName string,
	candidateAlias string,
	survivorAlias string,
) *TaskFlow {
	steps := []Step{
		&CheckClusterHealthyStep{GetDDBClient: getDDBClient},
		&DrainDataNodeChunksStep{
			GetDDBClient:   getDDBClient,
			CandidateAlias: candidateAlias,
			SurvivorAlias:  survivorAlias,
		},
		&CleanNodeReplicaMetadataStep{
			GetDDBClient:   getDDBClient,
			CandidateAlias: candidateAlias,
		},
		&DeleteDDBNodeCRStep{
			K8sClient: k8sClient,
			NodeName:  candidateName,
		},
	}

	return &TaskFlow{
		Name:  fmt.Sprintf("ScaleDown-%s", candidateAlias),
		Steps: steps,
	}
}
