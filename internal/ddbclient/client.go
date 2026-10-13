package ddbclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
				Proxy: nil,
			},
			Timeout: 10 * time.Second,
		},
	}
}

type executePayload struct {
	FunctionName string            `json:"functionName"`
	Params       []paramEntry      `json:"params"`
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
		return "", fmt.Errorf("failed to decode response %q: %w", string(respBody), err)
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
	script := "exec count(*) from getRecoveryTaskStatus() where Status not in [\"Finished\", \"Failed\"];"
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
