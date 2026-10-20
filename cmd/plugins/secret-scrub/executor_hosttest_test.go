//go:build hosttest

package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	_ "github.com/router-for-me/CLIProxyAPI/v8/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

// upstreamSSE answers every request with one SSE body.
type upstreamSSE struct{ body string }

func (u upstreamSSE) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(u.body)),
		Request:    req,
	}, nil
}

// codexExecutorChunks runs the native Codex executor's HTTP stream over an
// upstream that sends events as SSE frames (data: first when dataFirst),
// and returns the chunks the
// executor hands the hooks.
func codexExecutorChunks(t *testing.T, dataFirst bool) func(events []string) []string {
	return func(events []string) []string {
		var upstream strings.Builder
		for _, event := range events {
			name := "event: " + gjson.Get(event, "type").String()
			if dataFirst {
				upstream.WriteString("data: " + event + "\n" + name + "\n\n")
			} else {
				upstream.WriteString(name + "\ndata: " + event + "\n\n")
			}
		}
		ctx := t.Context()
		ctx = contextWithRoundTripper(ctx, upstreamSSE{body: upstream.String()})
		codex := executor.NewCodexExecutor(&config.Config{})
		auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "test"}}
		result, errStream := codex.ExecuteStream(ctx, auth,
			cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"hi"}`)},
			cliproxyexecutor.Options{Stream: true, SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse})
		if errStream != nil {
			t.Fatal(errStream)
		}
		var chunks []string
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatal(chunk.Err)
			}
			if len(chunk.Payload) > 0 {
				chunks = append(chunks, string(chunk.Payload))
			}
		}
		return chunks
	}
}

// Review 7, against the real executor: the chunks the native Codex HTTP
// executor emits reach the client whole. Run with -tags hosttest.
func TestRealCodexExecutorStreamIsDelivered(t *testing.T) {
	checkArgumentStream(t, "r7-host", codexExecutorChunks(t, false))
}

// Review 9, against the real executor: an upstream that writes data:
// before event: in each frame reaches the client with every event whole.
func TestRealCodexExecutorDataFirstStreamIsDelivered(t *testing.T) {
	checkArgumentStream(t, "r9-host", codexExecutorChunks(t, true))
}

// contextWithRoundTripper hands the executor its HTTP transport the way the
// executor's own tests do.
func contextWithRoundTripper(ctx context.Context, rt http.RoundTripper) context.Context {
	return context.WithValue(ctx, "cliproxy.roundtripper", rt) //nolint:staticcheck // the key the executor reads
}

// Review 10, against the real executor: an item id written with a JSON
// escape in one event is the same call as in the others.
func TestRealCodexExecutorEscapedIDIsOneCall(t *testing.T) {
	checkArgumentStream(t, "r10-host", escapedItemID(codexExecutorChunks(t, false)))
}
