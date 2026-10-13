package main

import (
	"flag"
	"fmt"
	"os"

	"dolphindb.io/operator/internal/agent"
)

func main() {
	mode := flag.String("mode", "daemon", "Running mode: daemon, prestop, or init")
	probePort := flag.Int("probePort", 9090, "HTTP probe listening port")
	ddbPort := flag.Int("ddbPort", 8848, "DolphinDB listening port")
	role := flag.String("role", "datanode", "Node role: controller, datanode, computenode, single")
	flag.Parse()

	switch *mode {
	case "daemon":
		fmt.Printf("[ddb-agent] Starting companion daemon on :%d (targeting DDB on :%d, role=%s)...\n", *probePort, *ddbPort, *role)
		srv := agent.NewServer(*probePort, *ddbPort, *role)
		if err := srv.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "[ddb-agent] Daemon error: %v\n", err)
			os.Exit(1)
		}
	case "prestop":
		fmt.Printf("[ddb-agent] Running prestop flush on port %d...\n", *ddbPort)
		if err := agent.PreStopFlush(*ddbPort); err != nil {
			fmt.Fprintf(os.Stderr, "[ddb-agent] Prestop flush error: %v\n", err)
			os.Exit(1)
		}
	case "init":
		fmt.Printf("[ddb-agent] Running init configuration setup for role=%s...\n", *role)
		// Config sync logic can be executed here
		fmt.Printf("[ddb-agent] Init setup complete.\n")
	default:
		fmt.Fprintf(os.Stderr, "Unknown mode: %s\n", *mode)
		os.Exit(1)
	}
}
