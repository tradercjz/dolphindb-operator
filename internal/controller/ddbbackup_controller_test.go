package controller

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
)

func TestCronScheduleParsing(t *testing.T) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	// Valid 5-field expression
	sched, err := parser.Parse("*/5 * * * *")
	if err != nil {
		t.Fatalf("expected valid schedule, got error: %v", err)
	}

	baseTime := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	nextTime := sched.Next(baseTime)
	expectedTime := time.Date(2026, 10, 14, 12, 5, 0, 0, time.UTC)

	if !nextTime.Equal(expectedTime) {
		t.Errorf("expected next time %v, got %v", expectedTime, nextTime)
	}

	// Valid descriptor
	schedDaily, err := parser.Parse("@daily")
	if err != nil {
		t.Fatalf("expected valid descriptor @daily, got error: %v", err)
	}
	nextDaily := schedDaily.Next(baseTime)
	expectedDaily := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	if !nextDaily.Equal(expectedDaily) {
		t.Errorf("expected next daily %v, got %v", expectedDaily, nextDaily)
	}

	// Invalid cron expression
	_, err = parser.Parse("invalid expression")
	if err == nil {
		t.Fatalf("expected error for invalid expression, got nil")
	}
}
