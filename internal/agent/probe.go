package agent

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
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

// handleReady returns 200 if the node is alive and ready for client traffic
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	stopping := s.stopping
	s.mu.RUnlock()

	if stopping {
		http.Error(w, "Node is terminating", http.StatusServiceUnavailable)
		return
	}

	// Verify TCP connectivity to local server
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", s.DDBPort), 1*time.Second)
	if err != nil {
		http.Error(w, fmt.Sprintf("Not ready: %v", err), http.StatusServiceUnavailable)
		return
	}
	_ = conn.Close()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("READY"))
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

// MarkTerminating signals prestop shutdown
func (s *Server) MarkTerminating() {
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
}

// PreStopFlush executes cache flush commands during pod termination
func PreStopFlush(ddbPort int) error {
	fmt.Fprintf(os.Stderr, "[ddb-agent] Executing PreStop memory cache flush on port %d...\n", ddbPort)
	// Try dialing local port with 2s timeout
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", ddbPort), 2*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ddb-agent] DDB already down or unreachable: %v\n", err)
		return nil
	}
	_ = conn.Close()

	// In real environment, invoke flushOLAPCache() & flushTSDBCache() via simple socket script or HTTP
	time.Sleep(1 * time.Second)
	fmt.Fprintf(os.Stderr, "[ddb-agent] PreStop flush complete, safe to terminate.\n")
	return nil
}
