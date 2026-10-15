package controller

import (
	"fmt"
	"strings"

	"dolphindb.io/operator/api/v1alpha1"
)

// SanitizeHostname converts a Kubernetes node name into a value that is legal
// for a Pod's `spec.hostname`.
//
// Kubernetes rejects hostnames containing dots, but managed node names *are*
// private DNS names on every cloud — for example
// `ip-172-31-2-254.ap-southeast-2.compute.internal` on EKS. Passing a node name
// straight through therefore made every standalone Pod fail to create with:
//
//	spec.hostname: Invalid value: "ip-172-31-2-254...": must not contain dots
//
// Self-managed clusters hid the problem because their node names are short and
// dot-free (`aftersale2`), so it only ever surfaced on a real cloud.
func SanitizeHostname(name string) string {
	// Keep the leading label: the part before the first dot is the short node
	// name on clouds ("ip-172-31-2-254").
	if i := strings.IndexByte(name, '.'); i >= 0 {
		name = name[:i]
	}

	var b strings.Builder
	b.Grow(len(name))
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			// Kubernetes DNS labels allow letters, digits and '-'; anything else
			// becomes a separator rather than being dropped, so distinct node
			// names cannot silently collapse into the same hostname.
			b.WriteByte('-')
		}
	}

	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "dolphindb"
	}
	// A DNS label is limited to 63 characters.
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-")
	}
	return out
}

// nodeHomeDir is the DolphinDB `-home` directory for a node. It is shared by the
// container args builder and by the TLS material installer so the two cannot
// drift apart.
func nodeHomeDir(node *v1alpha1.DDBNode) string {
	if node.Spec.Role == v1alpha1.NodeRoleDataNode || node.Spec.Role == v1alpha1.NodeRoleComputeNode {
		return fmt.Sprintf("/data/ddb/server/clusterDemo/data/%s", node.Spec.Alias)
	}
	return "/data/ddb/server/data"
}

// shellQuoteAll renders paths as single-quoted shell words.
func shellQuoteAll(vals []string) string {
	quoted := make([]string, 0, len(vals))
	for _, v := range vals {
		quoted = append(quoted, "'"+strings.ReplaceAll(v, "'", `'\''`)+"'")
	}
	return strings.Join(quoted, " ")
}
