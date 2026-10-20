package executor

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestTopLevelCacheTTLFollowsDowngrades(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		run  func([]byte) []byte
	}{
		{
			name: "probe strips extended TTL everywhere",
			body: `{"cache_control":{"type":"ephemeral","ttl":"1h","scope":"global"},"messages":[{"role":"user","content":[{"type":"text","text":"q","cache_control":{"type":"ephemeral","ttl":"1h"}}]}]}`,
			run:  stripClaudeCacheControlTTL,
		},
		{
			name: "explicit 5m before upgraded markers",
			body: `{"cache_control":{"type":"ephemeral","scope":"global"},"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":[{"type":"text","text":"q","cache_control":{"type":"ephemeral"}}]}]}`,
			run: func(body []byte) []byte {
				return normalizeCacheControlTTL(upgradeClaudeCacheControlTTL(body, "1h"))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := tc.run([]byte(tc.body))
			for _, path := range []string{"cache_control", "messages.0.content.0.cache_control"} {
				if cc := gjson.GetBytes(out, path); cc.Get("ttl").Exists() || cc.Get("type").String() != "ephemeral" {
					t.Errorf("downgraded %s = %s, want ephemeral without extended TTL", path, cc.Raw)
				}
			}
			if got := gjson.GetBytes(out, "cache_control.scope").String(); got != "global" {
				t.Errorf("downgraded cache scope = %q, want global", got)
			}
		})
	}
}
