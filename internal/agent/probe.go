package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Server encapsulates the agent daemon running alongside DolphinDB
type Server struct {
	Port     int
	DDBPort  int
	Role     string
	isMaster bool
	mu       sync.RWMutex
	ready    bool
	stopping bool
}

// NewServer creates a new agent probe server
func NewServer(port int, ddbPort int, role string) *Server {
	return &Server{
		Port:    port,
		DDBPort: ddbPort,
		Role:    role,
	}
}

// Start launches the probe HTTP listener
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz/startup", s.handleStartup)
	mux.HandleFunc("/healthz/ready", s.handleReady)
	mux.HandleFunc("/healthz/master", s.handleMaster)
	mux.HandleFunc("/state/master", s.handleSetMaster)
	mux.HandleFunc("/metrics", s.handleMetrics)

	addr := fmt.Sprintf("0.0.0.0:%d", s.Port)
	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	return server.ListenAndServe()
}

// handleStartup returns 200 as soon as the dolphindb port is open or process exists
func (s *Server) handleStartup(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	stopping := s.stopping
	s.mu.RUnlock()

	if stopping {
		http.Error(w, "Node is shutting down", http.StatusServiceUnavailable)
		return
	}

	// Probe local DDB port
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", s.DDBPort), 500*time.Millisecond)
	if err != nil {
		http.Error(w, fmt.Sprintf("DDB port %d not accepting connections yet: %v", s.DDBPort, err), http.StatusServiceUnavailable)
		return
	}
	_ = conn.Close()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// handleReady returns 200 if the node is alive and ready for client traffic.
// For DataNode, it verifies isDataNodeInitialized() == true.
// For Controller/Single/ComputeNode, it verifies isNodeInitialized() == true.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	stopping := s.stopping
	s.mu.RUnlock()

	if stopping {
		http.Error(w, "Node is terminating", http.StatusServiceUnavailable)
		return
	}

	initialized, err := s.checkInitialization()
	if err != nil {
		http.Error(w, fmt.Sprintf("Health probe check failed: %v", err), http.StatusServiceUnavailable)
		return
	}

	if !initialized {
		http.Error(w, "Node is still initializing (not ready)", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("READY"))
}

// checkInitialization queries local DolphinDB server for initialization status
func (s *Server) checkInitialization() (bool, error) {
	queryFunc := "isDataNodeInitialized()"
	if s.Role != "datanode" {
		queryFunc = "isNodeInitialized()"
	}

	payload := map[string]interface{}{
		"functionName": "executeCode",
		"params": []map[string]string{
			{
				"name":  "script",
				"form":  "scalar",
				"type":  "string",
				"value": queryFunc,
			},
		},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: nil,
		},
		Timeout: 3 * time.Second,
	}

	url := fmt.Sprintf("http://127.0.0.1:%d", s.DDBPort)
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("unexpected HTTP status code: %d", resp.StatusCode)
	}

	var ddbResp struct {
		ResultCode string `json:"resultCode"`
		Msg        string `json:"msg"`
		Object     []struct {
			Value string `json:"value"`
		} `json:"object"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&ddbResp); err != nil {
		return false, err
	}

	if ddbResp.ResultCode != "0" {
		return false, fmt.Errorf("ddb error: %s", ddbResp.Msg)
	}

	if len(ddbResp.Object) > 0 && (ddbResp.Object[0].Value == "1" || ddbResp.Object[0].Value == "true") {
		return true, nil
	}

	return false, nil
}

// handleMaster returns 200 ONLY if this controller is the active Raft Master
func (s *Server) handleMaster(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.Role != "controller" {
		http.Error(w, "Only controller has master status", http.StatusBadRequest)
		return
	}

	if !s.isMaster {
		http.Error(w, "Node is not active Raft Master", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("MASTER"))
}

// handleSetMaster allows internal controller to update master leadership status
func (s *Server) handleSetMaster(w http.ResponseWriter, r *http.Request) {
	master := r.URL.Query().Get("master") == "true"
	s.mu.Lock()
	s.isMaster = master
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "master set to %v", master)
}

// handleMetrics proxies Prometheus metrics from local DolphinDB process
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", s.DDBPort)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to query local DDB metrics: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// MarkTerminating signals prestop shutdown
func (s *Server) MarkTerminating() {
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
}

// PreStopShutdown issues SIGTERM (signal 15) to the DolphinDB server process.
// DolphinDB server intercepts signal 15, cleanly flushes TSDB/OLAP caches to disk, and terminates gracefully.
func PreStopShutdown(ddbPort int) error {
	fmt.Fprintf(os.Stderr, "[ddb-agent] Executing graceful shutdown (SIGTERM / signal 15) for DolphinDB...\n")

	// 1. Find the dolphindb process PID
	pid := findDolphinDBPID()

	fmt.Fprintf(os.Stderr, "[ddb-agent] Sending SIGTERM (kill -15) to dolphindb (PID %d)...\n", pid)
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		fmt.Fprintf(os.Stderr, "[ddb-agent] kill -15 returned: %v (process may have already terminated)\n", err)
		return nil
	}

	// 2. Poll until the process has exited or timeout
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		// Check if process still exists
		if err := syscall.Kill(pid, 0); err != nil {
			// Process no longer exists (ESRCH)
			fmt.Fprintf(os.Stderr, "[ddb-agent] DolphinDB (PID %d) caught signal 15 and terminated gracefully.\n", pid)
			return nil
		}
	}

	fmt.Fprintf(os.Stderr, "[ddb-agent] Warning: DolphinDB process did not exit within timeout, continuing...\n")
	return nil
}

// PreStopFlush alias for PreStopShutdown
func PreStopFlush(ddbPort int) error {
	return PreStopShutdown(ddbPort)
}

func findDolphinDBPID() int {
	// Check if PID 1 is dolphindb
	data, err := os.ReadFile("/proc/1/comm")
	if err == nil && strings.Contains(strings.TrimSpace(string(data)), "dolphindb") {
		return 1
	}

	// Read /proc to scan for comm == "dolphindb"
	entries, err := os.ReadDir("/proc")
	if err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			pid, err := strconv.Atoi(entry.Name())
			if err != nil {
				continue
			}
			commData, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
			if err == nil && strings.Contains(strings.TrimSpace(string(commData)), "dolphindb") {
				return pid
			}
		}
	}

	return 1
}
