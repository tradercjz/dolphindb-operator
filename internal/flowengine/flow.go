package flowengine

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"dolphindb.io/operator/api/v1alpha1"
)

// StepStatus defines the outcome of a step execution
type StepStatus string

const (
	StepStatusDone  StepStatus = "Done"
	StepStatusWait  StepStatus = "Wait"
	StepStatusError StepStatus = "Error"
)

// StepResult represents the result of running a single step
type StepResult struct {
	Status       StepStatus
	RequeueAfter time.Duration
	Message      string
	Error        error
}

// DoneResult returns a successful step completion
func DoneResult(msg string) StepResult {
	return StepResult{Status: StepStatusDone, Message: msg}
}

// WaitResult returns a waiting step completion with a retry duration
func WaitResult(after time.Duration, msg string) StepResult {
	return StepResult{Status: StepStatusWait, RequeueAfter: after, Message: msg}
}

// ErrorResult returns a failed step result
func ErrorResult(err error) StepResult {
	return StepResult{Status: StepStatusError, Error: err, Message: err.Error()}
}

// Step defines a single checkpointed action in a DAG task flow
type Step interface {
	Name() string
	Execute(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) StepResult
}

// TaskFlow defines a sequence of steps to transition a cluster to desired state
type TaskFlow struct {
	Name  string
	Steps []Step
}

// FlowEngine executes task flows step-by-step with checkpointing
type FlowEngine struct {
	Flows map[string]*TaskFlow
}

// NewFlowEngine creates an empty flow engine
func NewFlowEngine() *FlowEngine {
	return &FlowEngine{
		Flows: make(map[string]*TaskFlow),
	}
}

// RegisterFlow registers a named task flow
func (e *FlowEngine) RegisterFlow(flow *TaskFlow) {
	e.Flows[flow.Name] = flow
}

// Run executes the active flow or starts from step 0
func (e *FlowEngine) Run(ctx context.Context, cluster *v1alpha1.DolphinDBCluster, flowName string) (StepResult, bool) {
	flow, exists := e.Flows[flowName]
	if !exists {
		return ErrorResult(fmt.Errorf("flow %s not found", flowName)), false
	}

	// Determine starting step index from ActiveFlow status
	startIndex := 0
	for i, step := range flow.Steps {
		if cluster.Status.ActiveFlow == fmt.Sprintf("%s:%s", flow.Name, step.Name()) {
			startIndex = i
			break
		}
	}

	// Execute steps sequentially
	for i := startIndex; i < len(flow.Steps); i++ {
		step := flow.Steps[i]
		cluster.Status.ActiveFlow = fmt.Sprintf("%s:%s", flow.Name, step.Name())
		cluster.Status.FlowProgress = fmt.Sprintf("%d/%d (%s)", i+1, len(flow.Steps), step.Name())

		res := step.Execute(ctx, cluster)
		log.FromContext(ctx).Info("Flow step execution", "flow", flow.Name, "step", step.Name(), "status", res.Status, "msg", res.Message)
		if res.Status != StepStatusDone {
			return res, false
		}
	}

	// All steps completed!
	cluster.Status.ActiveFlow = ""
	cluster.Status.FlowProgress = "100%"
	return DoneResult("Flow completed successfully"), true
}
