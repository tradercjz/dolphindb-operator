package paramdict

import "strings"

// dynamicParams contains parameters that can be updated live via setDynamicConfig without restarting processes.
var dynamicParams = map[string]bool{
	"maxqueryexecutiontime":         true,
	"enablenullsafejoin":            true,
	"tsdbblockcachesize":            true,
	"reservedmemsize":               true,
	"olapcacheenginesize":           true,
	"memlimitofalltempresults":      true,
	"memlimitoftempresult":          true,
	"recoveryworkers":               true,
	"maxpartitionnumperquery":       true,
	"maxmemsize":                    true,
	"memlimitoftaskgroupresult":     true,
	"tsdbvectorindexcachesize":      true,
	"memlimitofqueryresult":         true,
	"maxconnections":                true,
	"maxblocksizeforreservedmemory": true,
	"tsdbcacheenginesize":           true,
	"dfschunknodeheartbeattimeout":  true,
	"loglevel":                      true,
	"enablemultithreadmerge":        true,
	"warningmemsize":                true,
}

// IsDynamic checks if a DolphinDB configuration parameter can be reloaded dynamically.
func IsDynamic(param string) bool {
	return dynamicParams[strings.ToLower(param)]
}

// DiffResult holds the classification of configuration changes.
type DiffResult struct {
	DynamicDiff map[string]string
	StaticDiff  map[string]string
	HasChanges  bool
}

// ClassifyDiff compares old and new configs and segregates dynamic vs static changes.
func ClassifyDiff(oldConfig, newConfig map[string]string) DiffResult {
	res := DiffResult{
		DynamicDiff: make(map[string]string),
		StaticDiff:  make(map[string]string),
	}

	// Check additions and modifications
	for k, newV := range newConfig {
		oldV, exists := oldConfig[k]
		if !exists || oldV != newV {
			res.HasChanges = true
			if IsDynamic(k) {
				res.DynamicDiff[k] = newV
			} else {
				res.StaticDiff[k] = newV
			}
		}
	}

	// Check deletions
	for k := range oldConfig {
		if _, exists := newConfig[k]; !exists {
			res.HasChanges = true
			if IsDynamic(k) {
				res.DynamicDiff[k] = "" // cleared / default
			} else {
				res.StaticDiff[k] = ""
			}
		}
	}

	return res
}

// IsOnlyDynamic returns true if there are changes and ALL of them are dynamic.
func (d DiffResult) IsOnlyDynamic() bool {
	return d.HasChanges && len(d.StaticDiff) == 0 && len(d.DynamicDiff) > 0
}

// RequiresRestart returns true if any static parameter has changed.
func (d DiffResult) RequiresRestart() bool {
	return len(d.StaticDiff) > 0
}
