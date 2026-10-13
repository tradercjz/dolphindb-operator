package flowengine

import (
	"context"
	"testing"
	"time"

	"dolphindb.io/operator/api/v1alpha1"
)

type mockStep struct {
	name   string
	result StepResult
	calls  int
}

func (m *mockStep) Name() string { return m.name }
func (m *mockStep) Execute(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) StepResult {
	m.calls++
	return m.result
}

func TestFlowEngineExecution(t *testing.T) {
	engine := NewFlowEngine()

	step1 := &mockStep{name: "step1", result: DoneResult("ok")}
	step2 := &mockStep{name: "step2", result: WaitResult(2*time.Second, "waiting")}
	step3 := &mockStep{name: "step3", result: DoneResult("done")}

	flow := &TaskFlow{
		Name:  "TestFlow",
		Steps: []Step{step1, step2, step3},
	}
	engine.RegisterFlow(flow)

	cluster := &v1alpha1.DolphinDBCluster{}

	// Run 1: step1 finishes, step2 waits
	res, done := engine.Run(context.Background(), cluster, "TestFlow")
	if done {
		t.Fatalf("expected flow not done")
	}
	if res.Status != StepStatusWait {
		t.Fatalf("expected step2 to return Wait, got %s", res.Status)
	}
	if cluster.Status.ActiveFlow != "TestFlow:step2" {
		t.Fatalf("expected checkpoint TestFlow:step2, got %s", cluster.Status.ActiveFlow)
	}

	// Change step2 to Done and resume
	step2.result = DoneResult("ok now")
	res, done = engine.Run(context.Background(), cluster, "TestFlow")
	if !done {
		t.Fatalf("expected flow to finish")
	}
	if cluster.Status.ActiveFlow != "" {
		t.Fatalf("expected ActiveFlow cleared, got %s", cluster.Status.ActiveFlow)
	}
	if cluster.Status.FlowProgress != "100%" {
		t.Fatalf("expected FlowProgress 100%%, got %s", cluster.Status.FlowProgress)
	}
}
