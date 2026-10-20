//go:build hosttest && linux && cgo

package main

import (
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/pluginhost"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"github.com/tidwall/gjson"
	"gopkg.in/yaml.v3"
)

func TestReloadKeepsScrubbedRequestsRestorable(t *testing.T) {
	dir := t.TempDir()
	archDir := filepath.Join(dir, runtime.GOOS, runtime.GOARCH)
	if err := os.MkdirAll(archDir, 0o700); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(archDir, "secret-scrub-v0.1.0.so")
	build := exec.Command("go", "build", "-buildmode=c-shared", "-o", first, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build test plugin: %v\n%s", err, out)
	}
	cfg := &config.Config{Plugins: config.PluginsConfig{
		Enabled: true, Dir: dir,
		Configs: make(map[string]config.PluginInstanceConfig),
	}}
	selectVersion := func(version string) {
		var instance config.PluginInstanceConfig
		if err := yaml.Unmarshal([]byte("enabled: true\nstore:\n  version: "+version+"\n"), &instance); err != nil {
			t.Fatal(err)
		}
		cfg.Plugins.Configs["secret-scrub"] = instance
	}
	selectVersion("0.1.0")
	host := pluginhost.New()
	t.Cleanup(host.ShutdownAll)
	host.ApplyConfig(t.Context(), cfg)
	if !host.PluginRegistered("secret-scrub") {
		t.Fatal("test plugin did not register")
	}
	placeholders := make(map[string]string)
	for _, id := range []string{"whole", "stream"} {
		resp := host.InterceptRequestBeforeAuth(t.Context(), pluginapi.RequestInterceptRequest{
			RequestID: id, SourceFormat: formatChat, Headers: http.Header{"X-Js-Scrub": {"on"}}, Body: []byte(userBody(anthropicKey)),
		})
		placeholders[id] = placeholderPattern.FindString(decodedContent(t, string(resp.Body)))
		if placeholders[id] == "" {
			t.Fatalf("request %s was not scrubbed", id)
		}
	}
	firstChunk := host.InterceptStreamChunk(t.Context(), pluginapi.StreamChunkInterceptRequest{
		RequestID: "stream", Body: []byte(toolChunk(0, `{"key":"`+placeholders["stream"][:6])),
	})
	if !firstChunk.DropChunk {
		t.Fatal("first placeholder fragment was not held")
	}
	// Distinct builds matter: the host deliberately reuses the same loaded
	// image when two versioned files have identical bytes.
	second := exec.Command("go", "build", "-buildmode=c-shared", "-ldflags=-buildid=secret-scrub-reload-test", "-o", filepath.Join(archDir, "secret-scrub-v0.1.1.so"), ".")
	if out, err := second.CombinedOutput(); err != nil {
		t.Fatalf("build replacement test plugin: %v\n%s", err, out)
	}
	selectVersion("0.1.1")
	host.ApplyConfig(t.Context(), cfg)
	fresh := host.InterceptRequestBeforeAuth(t.Context(), pluginapi.RequestInterceptRequest{
		RequestID: "fresh", SourceFormat: formatChat, Headers: http.Header{"X-Js-Scrub": {"on"}}, Body: []byte(userBody(anthropicKey)),
	})
	if placeholder := placeholderPattern.FindString(decodedContent(t, string(fresh.Body))); placeholder == "" || placeholder == placeholders["whole"] {
		t.Fatalf("fresh request placeholder = %q, want a new plugin instance's placeholder", placeholder)
	}
	whole := host.InterceptResponse(t.Context(), pluginapi.ResponseInterceptRequest{
		RequestID: "whole", Body: []byte(`{"choices":[{"message":{"content":"` + placeholders["whole"] + `"}}]}`),
	})
	if got := gjson.GetBytes(whole.Body, "choices.0.message.content").String(); got != anthropicKey {
		t.Errorf("reply after reload = %q, want original secret", got)
	}
	var arguments strings.Builder
	for i, chunk := range []string{toolChunk(0, placeholders["stream"][6:]+`"}`), `{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`} {
		out := host.InterceptStreamChunk(t.Context(), pluginapi.StreamChunkInterceptRequest{RequestID: "stream", ChunkIndex: i + 1, Body: []byte(chunk)})
		if !out.DropChunk {
			arguments.WriteString(gjson.GetBytes(out.Body, "choices.0.delta.tool_calls.0.function.arguments").String())
		}
	}
	if got := gjson.Get(arguments.String(), "key").String(); got != anthropicKey {
		t.Errorf("streamed arguments after reload = %q, want original secret", arguments.String())
	}
}
