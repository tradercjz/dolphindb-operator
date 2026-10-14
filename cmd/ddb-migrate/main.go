package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/migration"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(storagev1.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

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
		fmt.Fprintf(os.Stderr, "Warning: unable to create kubernetes clientset: %v\n", err)
	}

	ctx := context.Background()

	switch command {
	case "scan":
		runScan(ctx, cli, os.Args[2:])
	case "inspect":
		runInspect(ctx, cli, kubeClient, os.Args[2:])
	case "convert":
		runConvert(ctx, cli, kubeClient, os.Args[2:])
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command %q\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("================================================================================")
	fmt.Println(" DolphinDB Legacy K8s Suite Migration Tool (ddb-migrate)")
	fmt.Println("================================================================================")
	fmt.Println("Usage: ddb-migrate <command> [arguments]")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println("  scan     [-n namespace]                 扫描集群中所有旧版 DolphinDB CR 实例")
	fmt.Println("  inspect  -n <ns> <cluster-name>         深度体检旧集群：生成兼容性与风险评估报告")
	fmt.Println("  convert  -n <ns> <cluster-name> [flags] 转换为新版 DolphinDBCluster YAML 并导出脚本")
	fmt.Println("")
	fmt.Println("Convert Flags:")
	fmt.Println("  --mode      blue-green | in-place (默认: blue-green)")
	fmt.Println("  --version   目标版本 (默认: 3.00.6)")
	fmt.Println("  --name      新集群名称 (默认: <old-name>-v2 或 <old-name>)")
	fmt.Println("  -o, --out   输出 YAML 文件路径 (默认: stdout)")
	fmt.Println("================================================================================")
}

func runScan(ctx context.Context, cli client.Client, args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	ns := fs.String("n", "", "Namespace to scan (empty for all namespaces)")
	fs.Parse(args)

	fmt.Printf("🔍 正在扫描 Kubernetes 集群中的旧版 DolphinDB (dolphindb.dolphindb.io/v1) 资产...\n")
	clusters, err := migration.DiscoverLegacyClusters(ctx, cli, *ns)
	if err != nil {
		fmt.Fprintf(os.Stderr, "扫描失败: %v\n", err)
		os.Exit(1)
	}

	if len(clusters) == 0 {
		fmt.Println("未发现任何旧版 DolphinDB 集群。")
		return
	}

	fmt.Printf("\n✅ 发现 %d 个旧版 DolphinDB 集群实例:\n", len(clusters))
	fmt.Printf("%-18s %-16s %-12s %-10s %-12s %-8s %s\n",
		"NAMESPACE", "NAME", "MODE", "VERSION", "STATUS", "NODES", "STORAGE_CLASS")
	fmt.Println(strings.Repeat("-", 88))
	for _, c := range clusters {
		fmt.Printf("%-18s %-16s %-12s %-10s %-12s %-8d %s\n",
			c.Namespace, c.Name, c.Mode, c.Version, c.Status, c.NodeCount, c.StorageClassName)
	}
	fmt.Println("")
	fmt.Println("💡 提示：使用 `ddb-migrate inspect -n <namespace> <name>` 执行深度体检与兼容性评估。")
}

func runInspect(ctx context.Context, cli client.Client, kubeClient kubernetes.Interface, args []string) {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	ns := fs.String("n", "", "Namespace of legacy cluster")
	fs.Parse(args)

	if *ns == "" || fs.NArg() < 1 {
		fmt.Println("错误: 缺少必要参数。用法: ddb-migrate inspect -n <namespace> <cluster-name>")
		os.Exit(1)
	}
	name := fs.Arg(0)

	fmt.Printf("🔬 正在深度体检旧集群: %s/%s ...\n", *ns, name)
	detail, report, err := migration.AnalyzeLegacyCluster(ctx, cli, kubeClient, *ns, name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "体检失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf(" 📋 DolphinDB 旧集群升级兼容性体检评估报告: %s\n", detail.Info.Name)
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("• 命名空间:     %s\n", detail.Info.Namespace)
	fmt.Printf("• 架构模式:     %s (版本: %s)\n", detail.Info.Mode, detail.Info.Version)
	fmt.Printf("• 健康评分:     %d / 100  (综合风险级别: %s)\n", report.HealthScore, report.RiskLevel)
	fmt.Printf("• 推荐迁移策略: %s\n", report.RecommendedStrategy)
	fmt.Printf("• 推荐理由:     %s\n", report.RecommendationReason)

	fmt.Println("\n【存储与数据卷资产】")
	fmt.Printf("• 持久化数据卷: %d 个 (总容量: %s, 存储类: %s)\n",
		report.StorageSummary.TotalVolumes, report.StorageSummary.TotalCapacity, report.StorageSummary.StorageClass)
	for _, pvc := range detail.PVCs {
		fmt.Printf("  - PVC: %-32s 容量: %-8s 挂载点: %s\n", pvc.Name, pvc.Capacity, pvc.MountPath)
	}

	if len(report.CustomConfigs) > 0 {
		fmt.Println("\n【提取到的自定义参数 (来自 dolphindb.cnf)】")
		for k, v := range report.CustomConfigs {
			fmt.Printf("  • %-28s = %s\n", k, v)
		}
	}

	fmt.Println("\n【容器与架构优化收益】")
	fmt.Printf("• 旧版架构:     %s\n", report.ResourceComparison.LegacyArchitecture)
	fmt.Printf("• 新版架构:     %s\n", report.ResourceComparison.NewArchitecture)
	fmt.Printf("• 内存节约预估: %s\n", report.ResourceComparison.MemorySavedPerPod)

	fmt.Println("\n【重点诊断发现与风险提示】")
	for i, f := range report.Findings {
		fmt.Printf(" [%d] [%s] %s\n", i+1, f.Severity, f.Title)
		fmt.Printf("     详情: %s\n", f.Detail)
		fmt.Printf("     建议: %s\n\n", f.Recommendation)
	}

	fmt.Println("💡 提示：使用 `ddb-migrate convert -n " + *ns + " " + name + " --mode " + strings.ToLower(report.RecommendedStrategy) + "` 生成新版 YAML。")
}

func runConvert(ctx context.Context, cli client.Client, kubeClient kubernetes.Interface, args []string) {
	fs := flag.NewFlagSet("convert", flag.ExitOnError)
	ns := fs.String("n", "", "Namespace of legacy cluster")
	mode := fs.String("mode", "blue-green", "Migration mode (blue-green or in-place)")
	ver := fs.String("version", "3.00.6", "Target DolphinDB version")
	newName := fs.String("name", "", "New cluster name")
	outPath := fs.String("o", "", "Output path for generated YAML file")
	fs.Parse(args)

	if *ns == "" || fs.NArg() < 1 {
		fmt.Println("错误: 缺少必要参数。用法: ddb-migrate convert -n <namespace> <cluster-name> [flags]")
		os.Exit(1)
	}
	name := fs.Arg(0)

	detail, _, err := migration.AnalyzeLegacyCluster(ctx, cli, kubeClient, *ns, name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "分析旧集群失败: %v\n", err)
		os.Exit(1)
	}

	opts := migration.ConversionOptions{
		TargetMode:      *mode,
		TargetVersion:   *ver,
		NewClusterName:  *newName,
		TargetNamespace: *ns,
	}

	res, err := migration.ConvertLegacyCluster(detail, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "转换失败: %v\n", err)
		os.Exit(1)
	}

	if *outPath != "" {
		if err := os.WriteFile(*outPath, []byte(res.TargetYAML), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "保存 YAML 失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("✅ 已成功导出新版 DolphinDBCluster YAML 至: %s\n", *outPath)
	} else {
		fmt.Println("---")
		fmt.Print(res.TargetYAML)
		fmt.Println("---")
	}

	if *mode == "in-place" {
		fmt.Println("\n【存量 PVC 原地接管分步执行指南】")
		for _, step := range res.AdoptionSteps {
			fmt.Printf("• %s\n  命令: %s\n", step.Title, step.Command)
		}
	} else {
		fmt.Println("\n【双轨数据迁移脚本预览】")
		fmt.Println(res.SyncScript)
	}
}
