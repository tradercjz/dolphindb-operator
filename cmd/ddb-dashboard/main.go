package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/dashboard"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(storagev1.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

func main() {
	var listenAddr string
	var namespace string

	flag.StringVar(&listenAddr, "listen", ":9090", "HTTP server address to bind to (e.g., :9090 or 0.0.0.0:9090)")
	flag.StringVar(&namespace, "namespace", "", "Default Kubernetes namespace to monitor (empty means all namespaces)")
	flag.Parse()

	cfg, err := ctrl.GetConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error obtaining Kubernetes client configuration: %v\n", err)
		os.Exit(1)
	}

	cli, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating Kubernetes client: %v\n", err)
		os.Exit(1)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: unable to create kubernetes clientset for pod logs: %v\n", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	srv := dashboard.NewServer(cli, kubeClient, listenAddr, namespace)

	fmt.Printf("====================================================\n")
	fmt.Printf(" DolphinDB Operator Web Console & REST API\n")
	fmt.Printf(" Listening on: %s\n", listenAddr)
	if namespace != "" {
		fmt.Printf(" Default Namespace: %s\n", namespace)
	} else {
		fmt.Printf(" Default Namespace: all\n")
	}
	fmt.Printf("====================================================\n")

	if err := srv.Start(ctx); err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "Dashboard server exited with error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Dashboard server shut down cleanly.")
}
