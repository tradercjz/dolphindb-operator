// Package credentials resolves the DolphinDB administrator credentials that the
// operator and dashboard use to talk to a cluster.
//
// The operator previously hardcoded `admin` / `123456` at every call site, and
// `DolphinDBCluster.spec.security.adminSecretName` (whose CRD default is
// `dolphindb-admin`) was never read. That is fine for a throwaway local cluster
// and unacceptable everywhere else: the same credentials are used by the backup,
// restore, rolling-restart and ACL-sync paths, and on a cloud deployment they are
// reachable from the network.
//
// The resolution order is:
//
//  1. the password stored under the `password` key of the Secret named by
//     `spec.security.adminSecretName`;
//  2. the DolphinDB factory default, for backwards compatibility with existing
//     clusters that never created such a Secret.
package credentials

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// AdminUser is DolphinDB's built-in administrator account.
	AdminUser = "admin"

	// AdminPasswordKey is the Secret key holding the administrator password.
	AdminPasswordKey = "password"

	// DefaultAdminSecretName matches the CRD default for
	// DolphinDBCluster.spec.security.adminSecretName. It is used by code paths that
	// act on legacy clusters, where no CR is available to read the field from.
	DefaultAdminSecretName = "dolphindb-admin"

	// DefaultAdminPassword is the password the DolphinDB image ships with. It is
	// only ever used when no Secret is configured, so that existing installations
	// keep working.
	DefaultAdminPassword = "123456"
)

// Resolve returns the administrator username and password for a cluster.
//
// It never fails: a missing or malformed Secret falls back to the factory
// default so that a misconfigured cluster is still operable and can be fixed.
func Resolve(ctx context.Context, c client.Client, namespace, secretName string) (string, string) {
	if c == nil || secretName == "" {
		return AdminUser, DefaultAdminPassword
	}
	sec := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName}, sec); err != nil {
		return AdminUser, DefaultAdminPassword
	}
	if pw := strings.TrimSpace(string(sec.Data[AdminPasswordKey])); pw != "" {
		return AdminUser, pw
	}
	return AdminUser, DefaultAdminPassword
}

// LoginScript renders a DolphinDB `login(...)` prefix for the given credentials,
// escaping the password so a password containing quotes cannot break out of the
// generated script.
func LoginScript(user, password string) string {
	return "login(" + quote(user) + ", " + quote(password) + ");"
}

func quote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// ChangePwdScript rotates the current session's password.
//
// DolphinDB keeps the admin password in the node's ACL metadata, which lives in its
// home directory. While that directory was ephemeral, every pod restart silently
// reset the account to the factory default, and every operator call that needs
// authentication started failing with
// `login("admin", "…") => The user name or password is incorrect`.
func ChangePwdScript(oldPwd, newPwd string) string {
	return "changePwd(" + quote(oldPwd) + ", " + quote(newPwd) + ");"
}
