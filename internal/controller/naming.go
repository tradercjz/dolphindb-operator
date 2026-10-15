package controller

import "strings"

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
