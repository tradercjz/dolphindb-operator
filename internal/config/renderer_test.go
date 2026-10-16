package config

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"dolphindb.io/operator/api/v1alpha1"
)

func clusterWithDatanode(cfg map[string]string) *v1alpha1.DolphinDBCluster {
	return &v1alpha1.DolphinDBCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ddb-cluster", Namespace: "ddb"},
		Spec: v1alpha1.DolphinDBClusterSpec{
			Mode:    v1alpha1.ModeCluster,
			Version: "3.00.5",
			Controller: &v1alpha1.ControllerGroupSpec{
				Replicas: 1,
				Port:     8900,
			},
			DataNode: &v1alpha1.DataNodeGroupSpec{
				Replicas: 2,
				Port:     8901,
				Config:   cfg,
			},
		},
	}
}

func configValue(t *testing.T, rendered, key string) string {
	t.Helper()
	for _, line := range strings.Split(rendered, "\n") {
		if strings.HasPrefix(line, key+"=") {
			return strings.TrimPrefix(line, key+"=")
		}
	}
	return ""
}

// Regression test: the renderer set dataSync=1 but never enabled the cache
// engine, so a cluster created from a minimal CR could not start its DataNodes:
//
//	The ChunkNode failed to initialize with exception
//	[When the feature of dataSync and cache engine must be enable at same time.]
func TestDataSyncEnablesCacheEngineByDefault(t *testing.T) {
	r := NewConfigRenderer(clusterWithDatanode(nil))
	cfgs, err := r.RenderAll()
	if err != nil {
		t.Fatal(err)
	}

	for name, rendered := range map[string]string{
		"datanode.cfg": cfgs.DataNodeCfg,
		"cluster.cfg":  cfgs.ClusterCfg,
		"computenode":  cfgs.ComputeNodeCfg,
	} {
		if got := configValue(t, rendered, "dataSync"); got != "1" {
			t.Fatalf("%s: dataSync = %q, want 1", name, got)
		}
		if got := configValue(t, rendered, "chunkCacheEngineMemSize"); got == "" {
			t.Errorf("%s: chunkCacheEngineMemSize must be set when dataSync=1", name)
		}
		tsdb := configValue(t, rendered, "TSDBCacheEngineSize")
		if tsdb == "" {
			t.Errorf("%s: TSDBCacheEngineSize must be set when dataSync=1", name)
			continue
		}
		// DolphinDB parses this as whole gigabytes; a fractional value becomes 0
		// and is rejected at startup.
		if strings.Contains(tsdb, ".") {
			t.Errorf("%s: TSDBCacheEngineSize must be a whole number of GB, got %q", name, tsdb)
		}
	}
}

func TestExplicitCacheConfigWins(t *testing.T) {
	r := NewConfigRenderer(clusterWithDatanode(map[string]string{
		"chunkCacheEngineMemSize": "8",
		"TSDBCacheEngineSize":     "4",
	}))
	cfgs, err := r.RenderAll()
	if err != nil {
		t.Fatal(err)
	}
	if got := configValue(t, cfgs.DataNodeCfg, "chunkCacheEngineMemSize"); got != "8" {
		t.Errorf("chunkCacheEngineMemSize = %q, want the user value 8", got)
	}
	if got := configValue(t, cfgs.DataNodeCfg, "TSDBCacheEngineSize"); got != "4" {
		t.Errorf("TSDBCacheEngineSize = %q, want the user value 4", got)
	}
}

func TestDataSyncDisabledSkipsCacheDefaults(t *testing.T) {
	r := NewConfigRenderer(clusterWithDatanode(map[string]string{"dataSync": "0"}))
	cfgs, err := r.RenderAll()
	if err != nil {
		t.Fatal(err)
	}
	if got := configValue(t, cfgs.DataNodeCfg, "dataSync"); got != "0" {
		t.Fatalf("dataSync = %q, want 0", got)
	}
	if got := configValue(t, cfgs.DataNodeCfg, "chunkCacheEngineMemSize"); got != "" {
		t.Errorf("cache engine must not be forced when dataSync=0, got %q", got)
	}
}

// Standalone does not use dataSync, so it must not gain cache engine settings.
func TestStandaloneHasNoCacheDefaults(t *testing.T) {
	cluster := &v1alpha1.DolphinDBCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ddb-standalone", Namespace: "ddb"},
		Spec: v1alpha1.DolphinDBClusterSpec{
			Mode:       v1alpha1.ModeStandalone,
			Version:    "3.00.5",
			Standalone: &v1alpha1.StandaloneSpec{Port: 8848},
		},
	}
	cfgs, err := NewConfigRenderer(cluster).RenderAll()
	if err != nil {
		t.Fatal(err)
	}
	if got := configValue(t, cfgs.StandaloneCfg, "dataSync"); got != "0" {
		t.Fatalf("standalone dataSync = %q, want 0", got)
	}
	if strings.Contains(cfgs.StandaloneCfg, "chunkCacheEngineMemSize") {
		t.Error("standalone should not carry data node cache settings")
	}
}

func TestParseMemoryString(t *testing.T) {
	cases := map[string]int64{"8Gi": 8, "16GI": 16, "4G": 4, "2": 2}
	for in, want := range cases {
		got, err := ParseMemoryString(in)
		if err != nil {
			t.Fatalf("ParseMemoryString(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("ParseMemoryString(%q) = %d, want %d", in, got, want)
		}
	}
}
