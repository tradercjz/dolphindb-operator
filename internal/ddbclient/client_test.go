package ddbclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetClusterPerf(t *testing.T) {
	mockResponse := `[{"name":"orle","form":"table","size":"3","value":[{"name":"name","form":"vector","type":"string","size":"3","value":["ddb-cluster-dn-2","ddb-cluster-dn-1","ddb-cluster-dn-0"]},{"name":"host","form":"vector","type":"string","size":"3","value":["h2","h1","h0"]},{"name":"port","form":"vector","type":"int","size":"3","value":[8901,8901,8901]},{"name":"mode","form":"vector","type":"int","size":"3","value":[0,0,0]},{"name":"state","form":"vector","type":"int","size":"3","value":[1,1,1]}]}]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := `{"sessionID":"123","resultCode":"0","msg":"","object":` + mockResponse + `}`
		w.Write([]byte(resp))
	}))
	defer ts.Close()

	cli := NewClient(ts.URL, "admin", "123456")
	nodes, err := cli.GetClusterPerf(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	if nodes[0].Name != "ddb-cluster-dn-2" || nodes[0].State != 1 {
		t.Errorf("unexpected node 0: %+v", nodes[0])
	}
	if nodes[1].Name != "ddb-cluster-dn-1" || nodes[1].State != 1 {
		t.Errorf("unexpected node 1: %+v", nodes[1])
	}
	if nodes[2].Name != "ddb-cluster-dn-0" || nodes[2].State != 1 {
		t.Errorf("unexpected node 2: %+v", nodes[2])
	}
}

func TestHasActiveRecoveryTasks(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := `{"sessionID":"123","resultCode":"0","msg":"","object":[{"name":"","form":"scalar","type":"int","value":"0"}]}`
		w.Write([]byte(resp))
	}))
	defer ts.Close()

	cli := NewClient(ts.URL, "admin", "123456")
	active, err := cli.HasActiveRecoveryTasks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if active {
		t.Errorf("expected active=false, got true")
	}
}

func TestGetClusterNodesConfigs(t *testing.T) {
	mockResponse := `[{"name":"","form":"vector","type":"string","size":"4","value":["maxConnections=4096","workerNum=8","dataSync=1","newValuePartitionPolicy=add"]}]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := `{"sessionID":"123","resultCode":"0","msg":"","object":` + mockResponse + `}`
		w.Write([]byte(resp))
	}))
	defer ts.Close()

	cli := NewClient(ts.URL, "admin", "123456")
	cfgs, err := cli.GetClusterNodesConfigs(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfgs["maxConnections"] != "4096" || cfgs["workerNum"] != "8" || cfgs["dataSync"] != "1" {
		t.Errorf("unexpected parsed configs: %+v", cfgs)
	}
}

