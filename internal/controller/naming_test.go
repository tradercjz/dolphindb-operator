package controller

import "testing"

// Regression test for the EKS standalone failure:
//
//	Pod is invalid: spec.hostname: Invalid value:
//	"ip-172-31-2-254.ap-southeast-2.compute.internal": must not contain dots
func TestSanitizeHostname(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"eks node", "ip-172-31-2-254.ap-southeast-2.compute.internal", "ip-172-31-2-254"},
		{"gke node", "gke-prod-default-pool-abc123-x4y5.c.project.internal", "gke-prod-default-pool-abc123-x4y5"},
		{"bare metal short name", "aftersale2", "aftersale2"},
		{"uppercase is lowercased", "Node-A", "node-a"},
		{"underscores become separators", "node_a", "node-a"},
		{"dots only", "...", "dolphindb"},
		{"empty", "", "dolphindb"},
		{"leading and trailing separators trimmed", "-node-", "node"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := SanitizeHostname(tc.in); got != tc.want {
				t.Fatalf("SanitizeHostname(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSanitizeHostnameTruncatesToDNSLabelLimit(t *testing.T) {
	long := ""
	for i := 0; i < 100; i++ {
		long += "a"
	}
	got := SanitizeHostname(long)
	if len(got) > 63 {
		t.Fatalf("hostname label must be at most 63 characters, got %d", len(got))
	}
	if got == "" {
		t.Fatal("truncation must not produce an empty hostname")
	}
}
