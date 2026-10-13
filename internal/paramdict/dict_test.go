package paramdict

import (
	"testing"
)

func TestParamDict(t *testing.T) {
	if !IsDynamic("maxConnections") {
		t.Fatalf("expected maxConnections to be dynamic")
	}
	if !IsDynamic("TSDBCacheEngineSize") {
		t.Fatalf("expected TSDBCacheEngineSize to be dynamic")
	}
	if IsDynamic("volumes") {
		t.Fatalf("expected volumes to be static")
	}
	if IsDynamic("workerNum") {
		t.Fatalf("expected workerNum to be static")
	}

	oldCfg := map[string]string{
		"maxConnections": "512",
		"workerNum":      "4",
	}

	// 1. Only dynamic changes
	newCfgDynamic := map[string]string{
		"maxConnections": "1024",
		"workerNum":      "4",
	}
	diff1 := ClassifyDiff(oldCfg, newCfgDynamic)
	if !diff1.IsOnlyDynamic() {
		t.Fatalf("expected diff1 to be only dynamic")
	}
	if diff1.RequiresRestart() {
		t.Fatalf("expected diff1 not to require restart")
	}

	// 2. Static changes present
	newCfgStatic := map[string]string{
		"maxConnections": "1024",
		"workerNum":      "8",
	}
	diff2 := ClassifyDiff(oldCfg, newCfgStatic)
	if diff2.IsOnlyDynamic() {
		t.Fatalf("expected diff2 not to be only dynamic")
	}
	if !diff2.RequiresRestart() {
		t.Fatalf("expected diff2 to require restart")
	}
}
