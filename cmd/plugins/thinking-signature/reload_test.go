//go:build sotest

package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

// payload calls method on p and returns the envelope's Body, or nil.
func payload(t *testing.T, p *loadedPlugin, method string, req any) []byte {
	t.Helper()
	raw, _ := json.Marshal(req)
	out, errCall := p.call(method, raw)
	if errCall != nil {
		t.Fatal(errCall)
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(out, &env); errUnmarshal != nil || !env.OK {
		t.Fatalf("envelope %s: %v", out, errUnmarshal)
	}
	var result struct{ Body []byte }
	if errUnmarshal := json.Unmarshal(env.Result, &result); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	return result.Body
}

// Review 4: two copies of the built library, loaded into one process as a
// reload leaves them, share the carrier: a carrier made by one copy is
// read by the other, in both directions. Run with -tags sotest.
func TestReloadedCopiesShareTheCarrier(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.so")
	build := exec.Command("go", "build", "-buildmode=c-shared", "-o", first, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=1", "HOME="+realHome)
	if out, errBuild := build.CombinedOutput(); errBuild != nil {
		t.Fatalf("build: %v\n%s", errBuild, out)
	}
	raw, errRead := os.ReadFile(first)
	if errRead != nil {
		t.Fatal(errRead)
	}
	// The copies find the nonce file under HOME.
	t.Setenv("HOME", t.TempDir())
	second := filepath.Join(dir, "second.so")
	if errWrite := os.WriteFile(second, raw, 0o755); errWrite != nil {
		t.Fatal(errWrite)
	}
	a, errA := loadPlugin(first)
	if errA != nil {
		t.Fatal(errA)
	}
	b, errB := loadPlugin(second)
	if errB != nil {
		t.Fatal(errB)
	}

	// Response: the first copy marks, the native translator copies, the
	// second copy lifts.
	line := []byte(`data: {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"` + signature + `"}}`)
	marked := payload(t, a, pluginabi.MethodResponseNormalizeBefore, pluginapi.ResponseTransformRequest{FromFormat: formatClaude, ToFormat: formatOpenAI, Stream: true, Body: line})
	var param any
	start := []byte(`data: {"type":"message_start","message":{"id":"m","model":"claude-opus-4-5","usage":{"input_tokens":1}}}`)
	sdktranslator.TranslateStream(t.Context(), formatClaude, formatOpenAI, "claude-opus-4-5", nil, nil, start, &param)
	chunks := sdktranslator.TranslateStream(t.Context(), formatClaude, formatOpenAI, "claude-opus-4-5", nil, nil, marked, &param)
	if len(chunks) != 1 {
		t.Fatalf("translated %d chunks", len(chunks))
	}
	lifted := payload(t, b, pluginabi.MethodResponseNormalizeAfter, pluginapi.ResponseTransformRequest{FromFormat: formatClaude, ToFormat: formatOpenAI, Stream: true, Body: chunks[0]})
	if strings.Contains(string(lifted), "cpa-reasoning-block") || gjson.GetBytes(lifted, "choices.0.delta.reasoning_blocks.0.signature").String() != signature {
		t.Fatalf("second copy did not lift the first copy's carrier: %s", lifted)
	}

	// Request: the first copy carries, the native translator copies, the
	// second copy rebuilds.
	body := `{"model":"claude-opus-4-5","messages":[{"role":"user","content":"a"},{"role":"assistant","content":"b","reasoning_blocks":[{"type":"thinking","thinking":"t","signature":"` + signature + `"}]},{"role":"user","content":"c"}]}`
	carried := payload(t, a, pluginabi.MethodRequestInterceptAfter, pluginapi.RequestInterceptRequest{SourceFormat: formatOpenAI, ToFormat: formatClaude, Body: []byte(body)})
	translated := sdktranslator.TranslateRequest(formatOpenAI, formatClaude, "claude-opus-4-5", carried, false)
	rebuilt := payload(t, b, pluginabi.MethodRequestNormalize, pluginapi.RequestTransformRequest{FromFormat: formatOpenAI, ToFormat: formatClaude, Body: translated})
	if strings.Contains(string(rebuilt), "cpa-reasoning-block") || gjson.GetBytes(rebuilt, "messages.1.content.0.signature").String() != signature {
		t.Fatalf("second copy did not rebuild the first copy's carrier: %s", rebuilt)
	}
}
