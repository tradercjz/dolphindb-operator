package ddbclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client interacts with a DolphinDB node via HTTP JSON API
type Client struct {
	BaseURL  string
	Username string
	Password string
	HTTP     *http.Client
}

// NewClient creates a DolphinDB HTTP client
func NewClient(baseURL, username, password string) *Client {
	return &Client{
		BaseURL:  baseURL,
		Username: username,
		Password: password,
		HTTP: &http.Client{
			Transport: &http.Transport{
				Proxy:             nil,
				DisableKeepAlives: true,
			},
			Timeout: 10 * time.Second,
		},
	}
}

type executePayload struct {
	FunctionName string       `json:"functionName"`
	Params       []paramEntry `json:"params"`
}

type paramEntry struct {
	Name  string `json:"name"`
	Form  string `json:"form"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

type ddbResponse struct {
	SessionID  string          `json:"sessionID"`
	ResultCode string          `json:"resultCode"`
	Msg        string          `json:"msg"`
	Object     json.RawMessage `json:"object"`
}

// ExecuteScript runs a script and returns the raw object JSON or error
func (c *Client) ExecuteScript(ctx context.Context, script string) (string, error) {
	loginPrefix := ""
	if c.Username != "" {
		loginPrefix = fmt.Sprintf("login(%q, %q); ", c.Username, c.Password)
	}
	fullScript := loginPrefix + script

	payload := executePayload{
		FunctionName: "executeCode",
		Params: []paramEntry{
			{
				Name:  "script",
				Form:  "scalar",
				Type:  "string",
				Value: fullScript,
			},
		},
	}

	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(payload); err != nil {
		return "", err
	}
	bodyBytes := buf.Bytes()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Close = true

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var ddbResp ddbResponse
	if err := json.Unmarshal(respBody, &ddbResp); err != nil {
		// Fallback for DolphinDB server error strings containing unescaped quotes in msg
		bodyStr := string(respBody)
		if strings.Contains(bodyStr, `"resultCode":"1"`) || strings.Contains(bodyStr, `"resultCode": "1"`) {
			msg := bodyStr
			if idx := strings.Index(bodyStr, `"msg"`); idx != -1 {
				sub := bodyStr[idx:]
				if colon := strings.Index(sub, ":"); colon != -1 {
					sub = sub[colon+1:]
					if objIdx := strings.LastIndex(sub, `,"object"`); objIdx != -1 {
						msg = strings.Trim(strings.TrimSpace(sub[:objIdx]), `"`)
					}
				}
			}
			return "", fmt.Errorf("dolphindb script error: %s", msg)
		}
		return "", fmt.Errorf("failed to decode response %q: %w", bodyStr, err)
	}

	if ddbResp.ResultCode != "0" {
		return "", fmt.Errorf("dolphindb script error: %s", ddbResp.Msg)
	}

	return string(ddbResp.Object), nil
}

// SetDynamicConfig applies a dynamic parameter to the local node
func (c *Client) SetDynamicConfig(ctx context.Context, key, val string) error {
	script := fmt.Sprintf("setDynamicConfig(%q, %q);", key, val)
	_, err := c.ExecuteScript(ctx, script)
	return err
}

// NodePerf summarizes a node's performance from getClusterPerf
type NodePerf struct {
	Name  string
	Host  string
	Port  int
	Mode  int
	State int // 1 = survive, 0 = unsurvive
}

// GetClusterPerf queries cluster nodes and their live states
func (c *Client) GetClusterPerf(ctx context.Context) ([]NodePerf, error) {
	script := "exec name, host, port, mode, state from getClusterPerf();"
	raw, err := c.ExecuteScript(ctx, script)
	if err != nil {
		return nil, err
	}

	// DolphinDB tables in JSON return array containing table objects whose value is the column array
	var tables []struct {
		Name  string `json:"name"`
		Form  string `json:"form"`
		Value []struct {
			Name  string          `json:"name"`
			Value json.RawMessage `json:"value"`
		} `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &tables); err != nil || len(tables) == 0 {
		return nil, fmt.Errorf("unexpected table format: %s", raw)
	}
	cols := tables[0].Value

	// Map column values
	var names []string
	var hosts []string
	var ports []int
	var modes []int
	var states []int

	for _, col := range cols {
		switch col.Name {
		case "name":
			_ = json.Unmarshal(col.Value, &names)
		case "host":
			_ = json.Unmarshal(col.Value, &hosts)
		case "port":
			_ = json.Unmarshal(col.Value, &ports)
		case "mode":
			_ = json.Unmarshal(col.Value, &modes)
		case "state":
			_ = json.Unmarshal(col.Value, &states)
		}
	}

	n := len(names)
	res := make([]NodePerf, n)
	for i := 0; i < n; i++ {
		res[i] = NodePerf{
			Name:  names[i],
			Host:  hosts[i],
			Port:  ports[i],
			Mode:  modes[i],
			State: states[i],
		}
	}

	return res, nil
}

// ChunkReplicaInfo represents chunk placement on nodes
type ChunkReplicaInfo struct {
	ChunkID  string
	Replicas string
}

// GetChunksOnNode finds all chunks allocated to a given node alias
func (c *Client) GetChunksOnNode(ctx context.Context, nodeAlias string) ([]string, error) {
	script := fmt.Sprintf("exec chunkId from getClusterChunksStatus() where regexFind(replicas, %q) >= 0;", nodeAlias)
	raw, err := c.ExecuteScript(ctx, script)
	if err != nil {
		return nil, err
	}

	var res []struct {
		Value []string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &res); err != nil || len(res) == 0 {
		return nil, nil
	}
	return res[0].Value, nil
}

// CopyReplicas transfers a chunk replica from srcNode to destNode
func (c *Client) CopyReplicas(ctx context.Context, srcNode, destNode, chunkID string) error {
	script := fmt.Sprintf("copyReplicas(%q, %q, %q);", srcNode, destNode, chunkID)
	_, err := c.ExecuteScript(ctx, script)
	return err
}

// DeleteReplicas removes a chunk replica from a decommissioned node
func (c *Client) DeleteReplicas(ctx context.Context, chunkID, nodeAlias string) error {
	script := fmt.Sprintf("deleteReplicas(%q, %q);", chunkID, nodeAlias)
	_, err := c.ExecuteScript(ctx, script)
	return err
}

// HasActiveRecoveryTasks checks if any data chunk replication or recovery is currently in progress
func (c *Client) HasActiveRecoveryTasks(ctx context.Context) (bool, error) {
	script := "exec count(*) from getRecoveryTaskStatus() where Status not in [\"Finished\", \"Failed\", \"Aborted\"];"
	raw, err := c.ExecuteScript(ctx, script)
	if err != nil {
		return false, err
	}

	var res []struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &res); err != nil || len(res) == 0 {
		return false, nil
	}
	return res[0].Value != "0", nil
}

// IsDataNodeInitialized checks whether the target DataNode has completed chunk and redo log initialization
func (c *Client) IsDataNodeInitialized(ctx context.Context) (bool, error) {
	script := "isDataNodeInitialized();"
	raw, err := c.ExecuteScript(ctx, script)
	if err != nil {
		return false, err
	}
	var res []struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &res); err != nil || len(res) == 0 {
		return false, nil
	}
	return res[0].Value == "1" || res[0].Value == "true", nil
}

// IsNodeInitialized checks whether the node has completed initialization
func (c *Client) IsNodeInitialized(ctx context.Context) (bool, error) {
	script := "isNodeInitialized();"
	raw, err := c.ExecuteScript(ctx, script)
	if err != nil {
		return false, err
	}
	var res []struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &res); err != nil || len(res) == 0 {
		return false, nil
	}
	return res[0].Value == "1" || res[0].Value == "true", nil
}

// GetLicenseDetails retrieves the license attributes as a key-value map
func (c *Client) GetLicenseDetails(ctx context.Context) (map[string]string, error) {
	raw, err := c.ExecuteScript(ctx, "license();")
	if err != nil {
		return nil, err
	}
	return parseDDBDict(raw)
}

// ReloadLicense broadcasts updateLicense across cluster nodes and returns updated license info
func (c *Client) ReloadLicense(ctx context.Context) (map[string]string, error) {
	script := "try { pnodeRun(updateLicense); } catch(ex) {}; try { updateLicense(); } catch(ex) {}; license();"
	raw, err := c.ExecuteScript(ctx, script)
	if err != nil {
		return nil, err
	}
	return parseDDBDict(raw)
}

func parseDDBDict(raw string) (map[string]string, error) {
	var dicts []struct {
		Value []struct {
			Form  string          `json:"form"`
			Value json.RawMessage `json:"value"`
		} `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &dicts); err != nil || len(dicts) == 0 || len(dicts[0].Value) < 2 {
		return nil, fmt.Errorf("unexpected dict format: %s", raw)
	}

	var keys []string
	if err := json.Unmarshal(dicts[0].Value[0].Value, &keys); err != nil {
		return nil, err
	}

	var vals []struct {
		Value interface{} `json:"value"`
	}
	if err := json.Unmarshal(dicts[0].Value[1].Value, &vals); err != nil {
		return nil, err
	}

	res := make(map[string]string)
	for i, k := range keys {
		if i < len(vals) {
			res[k] = fmt.Sprintf("%v", vals[i].Value)
		}
	}
	return res, nil
}

// Backup triggers DFS database/table backup executed on the specified DataNode
func (c *Client) Backup(ctx context.Context, dataNodeAlias, backupDir, dbPath, tableName string, force, parallel, snapshot bool) (int, error) {
	var script string
	if tableName != "" {
		script = fmt.Sprintf("rpc(%q, def(bDir, dbP, f, p, s, tN){ return backup(bDir, dbP, f, p, s, tN); }, %q, %q, %t, %t, %t, %q);",
			dataNodeAlias, backupDir, dbPath, force, parallel, snapshot, tableName)
	} else {
		script = fmt.Sprintf("rpc(%q, def(bDir, dbP, f, p, s){ return backup(bDir, dbP, f, p, s); }, %q, %q, %t, %t, %t);",
			dataNodeAlias, backupDir, dbPath, force, parallel, snapshot)
	}

	raw, err := c.ExecuteScript(ctx, script)
	if err != nil {
		return 0, err
	}

	var res []struct {
		Value interface{} `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &res); err != nil || len(res) == 0 {
		return 0, nil
	}

	switch v := res[0].Value.(type) {
	case float64:
		return int(v), nil
	case string:
		var n int
		_, _ = fmt.Sscanf(v, "%d", &n)
		return n, nil
	default:
		return 0, nil
	}
}

// Restore triggers DFS database/table restore executed on the specified DataNode
func (c *Client) Restore(ctx context.Context, dataNodeAlias, backupDir, dbPath, tableName, targetDBPath, targetTableName string) error {
	var script string
	if tableName != "" {
		if targetDBPath != "" && targetTableName != "" {
			script = fmt.Sprintf("rpc(%q, def(bDir, dbP, tN, newDBP, newTN){ return restoreTable(bDir, dbP, tN, newDBP, newTN); }, %q, %q, %q, %q, %q);",
				dataNodeAlias, backupDir, dbPath, tableName, targetDBPath, targetTableName)
		} else {
			script = fmt.Sprintf("rpc(%q, def(bDir, dbP, tN){ return restoreTable(bDir, dbP, tN); }, %q, %q, %q);",
				dataNodeAlias, backupDir, dbPath, tableName)
		}
	} else {
		if targetDBPath != "" {
			script = fmt.Sprintf("rpc(%q, def(bDir, dbP, newDBP){ return restoreDB(bDir, dbP, newDBP); }, %q, %q, %q);",
				dataNodeAlias, backupDir, dbPath, targetDBPath)
		} else {
			script = fmt.Sprintf("rpc(%q, def(bDir, dbP){ return restoreDB(bDir, dbP); }, %q, %q);",
				dataNodeAlias, backupDir, dbPath)
		}
	}

	_, err := c.ExecuteScript(ctx, script)
	return err
}

// CheckBackup checks the integrity of backed up database/tables
func (c *Client) CheckBackup(ctx context.Context, dataNodeAlias, backupDir, dbPath, tableName string) (int, error) {
	var script string
	if tableName != "" {
		script = fmt.Sprintf("rpc(%q, def(bDir, dbP, tN){ return exec count(*) from checkBackup(bDir, dbP, tN); }, %q, %q, %q);",
			dataNodeAlias, backupDir, dbPath, tableName)
	} else {
		script = fmt.Sprintf("rpc(%q, def(bDir, dbP){ return exec count(*) from checkBackup(bDir, dbP); }, %q, %q);",
			dataNodeAlias, backupDir, dbPath)
	}

	raw, err := c.ExecuteScript(ctx, script)
	if err != nil {
		return 0, err
	}

	var res []struct {
		Value interface{} `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &res); err != nil || len(res) == 0 {
		return 0, nil
	}

	switch v := res[0].Value.(type) {
	case float64:
		return int(v), nil
	case string:
		var n int
		_, _ = fmt.Sscanf(v, "%d", &n)
		return n, nil
	default:
		return 0, nil
	}
}

// RebalanceClusterChunks dynamically rebalances chunk replicas across all online DataNodes
func (c *Client) RebalanceClusterChunks(ctx context.Context) (int, string, error) {
	nodes, err := c.GetClusterPerf(ctx)
	if err != nil {
		return 0, "", fmt.Errorf("failed to get cluster perf: %w", err)
	}

	var onlineNodes []string
	for _, n := range nodes {
		if n.State == 1 {
			onlineNodes = append(onlineNodes, n.Name)
		}
	}

	if len(onlineNodes) < 2 {
		return 0, "At least 2 online DataNodes are required for rebalancing", nil
	}

	nodeChunks := make(map[string][]string)
	for _, alias := range onlineNodes {
		chunks, err := c.GetChunksOnNode(ctx, alias)
		if err != nil {
			return 0, "", fmt.Errorf("failed to get chunks on %s: %w", alias, err)
		}
		nodeChunks[alias] = chunks
	}

	// Find max and min loaded nodes
	var maxNode, minNode string
	maxCount := -1
	minCount := 10000000

	for _, alias := range onlineNodes {
		cnt := len(nodeChunks[alias])
		if cnt > maxCount {
			maxCount = cnt
			maxNode = alias
		}
		if cnt < minCount {
			minCount = cnt
			minNode = alias
		}
	}

	skew := maxCount - minCount
	if skew <= 1 {
		return 0, fmt.Sprintf("Cluster chunks are well-balanced (skew=%d, max=%d on %s, min=%d on %s)",
			skew, maxCount, maxNode, minCount, minNode), nil
	}

	// Move up to skew/2 chunks from maxNode to minNode
	minSet := make(map[string]bool)
	for _, ch := range nodeChunks[minNode] {
		minSet[ch] = true
	}

	var moved int
	targetMove := skew / 2
	for _, ch := range nodeChunks[maxNode] {
		if moved >= targetMove {
			break
		}
		if !minSet[ch] {
			if err := c.CopyReplicas(ctx, maxNode, minNode, ch); err != nil {
				return moved, "", fmt.Errorf("failed to copy chunk %s to %s: %w", ch, minNode, err)
			}
			moved++
		}
	}

	// Wait up to 10 seconds for replication tasks to finish
	for attempt := 0; attempt < 10; attempt++ {
		recovering, err := c.HasActiveRecoveryTasks(ctx)
		if err == nil && !recovering {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// Now check which chunks successfully arrived on minNode and clean from maxNode safely
	newMinChunks, err := c.GetChunksOnNode(ctx, minNode)
	if err == nil {
		newMinSet := make(map[string]bool)
		for _, ch := range newMinChunks {
			newMinSet[ch] = true
		}
		for _, ch := range nodeChunks[maxNode] {
			if newMinSet[ch] && !minSet[ch] {
				_ = c.DeleteReplicas(ctx, ch, maxNode)
			}
		}
	}

	return moved, fmt.Sprintf("Transferred %d chunk(s) from %s (%d) to %s (%d)", moved, maxNode, maxCount, minNode, minCount), nil
}
