package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/credentials"
)

func logPasswordRotated(cluster string) {
	log.Log.WithName("admin-password").Info("Rotated the DolphinDB admin password to the value declared in the Secret", "cluster", cluster)
}

func logPasswordRotateFailure(cluster string, err error) {
	log.Log.WithName("admin-password").Error(err, "Failed to rotate the DolphinDB admin password; the operator will not be able to authenticate", "cluster", cluster)
}

// postDDBScript runs an executeCode script and returns the first value, or an error.
func postDDBScript(ctx context.Context, baseURL, script string) (string, error) {
	payload := map[string]interface{}{
		"functionName": "executeCode",
		"params": []map[string]string{
			{"name": "script", "form": "scalar", "type": "string", "value": script},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	// Keep-alive is disabled on purpose: DolphinDB has a concurrent connection
	// ceiling and idle sockets accumulate against it.
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out struct {
		ResultCode string `json:"resultCode"`
		Msg        string `json:"msg"`
		Object     []struct {
			Value string `json:"value"`
		} `json:"object"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.ResultCode != "0" {
		return "", fmt.Errorf("%s", out.Msg)
	}
	if len(out.Object) == 0 {
		return "", nil
	}
	return out.Object[0].Value, nil
}

// ensureAdminPassword aligns DolphinDB's admin password with the declared Secret.
//
// The account is the cluster's only authentication path for the operator itself:
// the ACL key sync, backups, restores and rolling restarts all log in with it. It is
// stored inside the node's home directory, so a cluster whose home was recreated
// silently reverts to the factory default and every one of those calls starts
// failing.
//
// This reconciles it instead of hoping: if the declared password does not work but
// the factory default does, rotate the account to the declared value. It is
// idempotent and a no-op once they match.
func (r *DolphinDBClusterReconciler) ensureAdminPassword(ctx context.Context, cluster *v1alpha1.DolphinDBCluster, baseURL string) {
	_, pass := credentials.Resolve(ctx, r.Client, cluster.Namespace, cluster.Spec.Security.AdminSecretName)
	if pass == credentials.DefaultAdminPassword {
		return
	}

	// Already correct?
	if _, err := postDDBScript(ctx, baseURL, credentials.LoginScript(credentials.AdminUser, pass)+` 1`); err == nil {
		return
	}
	// Can we still get in with the factory default?
	if _, err := postDDBScript(ctx, baseURL, credentials.LoginScript(credentials.AdminUser, credentials.DefaultAdminPassword)+` 1`); err != nil {
		// Neither works; leave the cluster alone rather than guessing.
		return
	}

	script := credentials.LoginScript(credentials.AdminUser, credentials.DefaultAdminPassword) + " " +
		credentials.ChangePwdScript(credentials.DefaultAdminPassword, pass) + ` 1`
	if _, err := postDDBScript(ctx, baseURL, script); err != nil {
		logPasswordRotateFailure(cluster.Name, err)
		return
	}
	logPasswordRotated(cluster.Name)
}
