//go:build hosttest

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers/openai"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// pluginHost runs this plugin in process behind the host's interceptor
// interface, through the same JSON method calls the C ABI carries.
type pluginHost struct{}

func viaPlugin[T any](method string, req any) T {
	var result T
	raw, _ := json.Marshal(req)
	out, errHandle := handleMethod(method, raw)
	if errHandle != nil {
		panic(errHandle)
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(out, &env); errUnmarshal != nil || !env.OK {
		panic(fmt.Sprintf("envelope %s", out))
	}
	if errUnmarshal := json.Unmarshal(env.Result, &result); errUnmarshal != nil {
		panic(errUnmarshal)
	}
	return result
}

func (pluginHost) InterceptRequestBeforeAuth(_ context.Context, req pluginapi.RequestInterceptRequest) pluginapi.RequestInterceptResponse {
	return viaPlugin[pluginapi.RequestInterceptResponse](pluginabi.MethodRequestInterceptBefore, req)
}

func (pluginHost) InterceptRequestAfterAuth(context.Context, pluginapi.RequestInterceptRequest) pluginapi.RequestInterceptResponse {
	return pluginapi.RequestInterceptResponse{}
}

func (pluginHost) InterceptResponse(_ context.Context, req pluginapi.ResponseInterceptRequest) pluginapi.ResponseInterceptResponse {
	return viaPlugin[pluginapi.ResponseInterceptResponse](pluginabi.MethodResponseInterceptAfter, req)
}

func (pluginHost) InterceptStreamChunk(_ context.Context, req pluginapi.StreamChunkInterceptRequest) pluginapi.StreamChunkInterceptResponse {
	return viaPlugin[pluginapi.StreamChunkInterceptResponse](pluginabi.MethodResponseInterceptStreamChunk, req)
}

func (pluginHost) HasStreamInterceptors() bool { return true }

// corpusExecutor streams the chunks the current case names for its model.
type corpusExecutor struct {
	mu      sync.Mutex
	chunks  map[string][]string
	attempt map[string]int
}

// Markers in a case's chunks: attemptFails ends an attempt with an upstream
// error the host may retry; nextAttempt starts the chunks of the next one.
const (
	attemptFails = "\x00attempt fails"
	nextAttempt  = "\x00next attempt"
)

func (*corpusExecutor) Identifier() string { return "scrub-differential" }

func (*corpusExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *corpusExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	var attempts [][]string
	current := []string{}
	for _, chunk := range e.chunks[req.Model] {
		if chunk == nextAttempt {
			attempts = append(attempts, current)
			current = []string{}
			continue
		}
		current = append(current, chunk)
	}
	attempts = append(attempts, current)
	chunks := attempts[min(e.attempt[req.Model], len(attempts)-1)]
	e.attempt[req.Model]++
	e.mu.Unlock()
	out := make(chan coreexecutor.StreamChunk, len(chunks))
	for _, chunk := range chunks {
		if chunk == attemptFails {
			out <- coreexecutor.StreamChunk{Err: &coreauth.Error{HTTPStatus: http.StatusInternalServerError, Message: "attempt failed"}}
			continue
		}
		out <- coreexecutor.StreamChunk{Payload: []byte(chunk)}
	}
	close(out)
	return &coreexecutor.StreamResult{Chunks: out}, nil
}

func (*corpusExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (*corpusExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*corpusExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

// differentialHost is the real host: Responses and chat handlers over an
// auth manager with the corpus executor and the real Codex and xAI HTTP
// executors, the latter reading an upstream the case sets. One handler set
// runs without the plugin, one with it.
type differentialHost struct {
	corpus   *corpusExecutor
	upstream struct {
		sync.Mutex
		body     string
		received []string
	}
	plain, scrubbed *gin.Engine
	headers         map[string]string
}

// hostSerial keeps the auth IDs of each host's managers apart.
var hostSerial int

const (
	fakeModel  = "scrub-differential-fake"
	chatModel  = "scrub-differential-chat"
	codexModel = "scrub-differential-codex"
	xaiModel   = "scrub-differential-xai"
)

func newDifferentialHost(t *testing.T) *differentialHost {
	return newDifferentialHostWith(t, sdkconfig.SDKConfig{})
}

// newDifferentialHostWith builds the host with config (for one, bootstrap
// retries).
func newDifferentialHostWith(t *testing.T, cfg sdkconfig.SDKConfig) *differentialHost {
	t.Helper()
	gin.SetMode(gin.TestMode)
	host := &differentialHost{corpus: &corpusExecutor{chunks: make(map[string][]string), attempt: make(map[string]int)}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, _ := io.ReadAll(r.Body)
		host.upstream.Lock()
		body := host.upstream.body
		host.upstream.received = append(host.upstream.received, string(request))
		host.upstream.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	// Each run has its own auth manager: an upstream error cools an auth
	// down, which must not leak from one run into the other.
	router := func(withPlugin bool) *gin.Engine {
		manager := coreauth.NewManager(nil, nil, nil)
		manager.RegisterExecutor(host.corpus)
		manager.RegisterExecutor(executor.NewCodexExecutor(&config.Config{}))
		manager.RegisterExecutor(executor.NewXAIExecutor(&config.Config{}))
		hostSerial++
		suffix := "-" + strconv.Itoa(hostSerial)
		auths := []struct {
			id, provider string
			models       []string
			attributes   map[string]string
		}{
			{"scrub-differential-fake" + suffix, host.corpus.Identifier(), []string{fakeModel, chatModel}, nil},
			// A second credential, so a bootstrap retry can pick one after
			// the first is cooled down by its failure.
			{"scrub-differential-fake-b" + suffix, host.corpus.Identifier(), []string{fakeModel, chatModel}, nil},
			{"scrub-differential-codex" + suffix, "codex", []string{codexModel}, map[string]string{"api_key": "test", "base_url": server.URL}},
			{"scrub-differential-xai" + suffix, "xai", []string{xaiModel}, map[string]string{"api_key": "test", "base_url": server.URL}},
		}
		for _, a := range auths {
			auth := &coreauth.Auth{ID: a.id, Provider: a.provider, Status: coreauth.StatusActive, Attributes: a.attributes}
			if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
				t.Fatalf("register %s: %v", a.id, errRegister)
			}
			var infos []*registry.ModelInfo
			for _, model := range a.models {
				infos = append(infos, &registry.ModelInfo{ID: model})
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, infos)
			id := auth.ID
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
		}
		runConfig := cfg
		base := handlers.NewBaseAPIHandlers(&runConfig, manager)
		if withPlugin {
			base.SetPluginHost(pluginHost{})
		}
		engine := gin.New()
		engine.POST("/v1/responses", openai.NewOpenAIResponsesAPIHandler(base).Responses)
		engine.POST("/v1/chat/completions", openai.NewOpenAIAPIHandler(base).ChatCompletions)
		return engine
	}
	host.plain, host.scrubbed = router(false), router(true)
	return host
}

// send posts body to path and returns the status and what the client
// receives.
func (h *differentialHost) send(withPlugin bool, path, body string) (int, string) {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-JS-Scrub", "on")
	recorder := httptest.NewRecorder()
	if withPlugin {
		h.scrubbed.ServeHTTP(recorder, request)
	} else {
		h.plain.ServeHTTP(recorder, request)
	}
	return recorder.Code, recorder.Body.String()
}

// run sends one request and returns what the client receives.
func (h *differentialHost) run(t *testing.T, withPlugin bool, model string) string {
	t.Helper()
	path := "/v1/responses"
	body := `{"model":"` + model + `","input":"my key is ` + anthropicKey + `","stream":true}`
	if model == chatModel {
		path = "/v1/chat/completions"
		body = `{"model":"` + model + `","messages":[{"role":"user","content":"my key is ` + anthropicKey + `"}],"stream":true}`
	}
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-JS-Scrub", "on")
	for name, value := range h.headers {
		request.Header.Set(name, value)
	}
	h.corpus.mu.Lock()
	h.corpus.attempt = make(map[string]int)
	h.corpus.mu.Unlock()
	recorder := httptest.NewRecorder()
	if withPlugin {
		h.scrubbed.ServeHTTP(recorder, request)
	} else {
		h.plain.ServeHTTP(recorder, request)
	}
	return recorder.Body.String()
}

// differentialCase is one stream: chunks for the corpus executor, or the
// upstream body for a real executor.
type differentialCase struct {
	name     string
	model    string
	chunks   []string
	upstream string
	headers  map[string]string
}

func (h *differentialHost) set(c differentialCase) {
	h.corpus.mu.Lock()
	h.corpus.chunks[c.model] = c.chunks
	h.corpus.mu.Unlock()
	h.headers = c.headers
	h.upstream.Lock()
	h.upstream.body = c.upstream
	h.upstream.Unlock()
}

// compare runs a case without and with the plugin and checks the client
// dispatches the same events.
func (h *differentialHost) compare(t *testing.T, c differentialCase) {
	t.Helper()
	h.set(c)
	plain := h.run(t, false, c.model)
	scrubbed := h.run(t, true, c.model)
	want, got := dispatch(plain), dispatch(scrubbed)
	if len(want) == 0 {
		// The host answered with an error, not a stream: the same error.
		if plain == "" || scrubbed != plain {
			t.Fatalf("%s: without plugin %q, with plugin %q", c.name, plain, scrubbed)
		}
		return
	}
	if !reflect.DeepEqual(want, got) {
		at := 0
		for at < len(want) && at < len(got) && want[at] == got[at] {
			at++
		}
		var w, g dispatchedEvent
		if at < len(want) {
			w = want[at]
		}
		if at < len(got) {
			g = got[at]
		}
		t.Fatalf("%s: event %d of %d/%d differs\nwithout plugin: %+v\nwith plugin:    %+v\nraw without: %q\nraw with:    %q", c.name, at, len(want), len(got), w, g, plain, scrubbed)
	}
}

// corpusEvents is a Responses reply with text, a tool call and terminal.
func corpusEvents(terminal string) []string {
	ids := `"item_id":"fc_1","output_index":1`
	text := `"item_id":"m1","output_index":0,"content_index":0`
	events := []string{
		`{"type":"response.created","sequence_number":0,"response":{"id":"resp_1","status":"in_progress"}}`,
		`{"type":"response.output_text.delta","sequence_number":1,` + text + `,"delta":"hello"}`,
		`{"type":"response.output_text.delta","sequence_number":2,` + text + `,"delta":" world"}`,
		`{"type":"response.output_text.done","sequence_number":3,` + text + `,"text":"hello world"}`,
		`{"type":"response.function_call_arguments.delta","sequence_number":4,` + ids + `,"delta":"{\"a\":"}`,
		`{"type":"response.function_call_arguments.delta","sequence_number":5,` + ids + `,"delta":"1}"}`,
		`{"type":"response.function_call_arguments.done","sequence_number":6,` + ids + `,"arguments":"{\"a\":1}"}`,
	}
	events = append(events, terminalEvent(terminal))
	return events
}

// terminalEvent is the reply's last event in each form the host takes as
// the end: completed, error and failed by type; a typeless payload named
// response.completed or error by its event: line; an error envelope.
func terminalEvent(terminal string) string {
	switch terminal {
	case "typeless-completed":
		return `{"__event":"response.completed","sequence_number":7,"response":{"id":"resp_1","status":"completed","output":[]}}`
	case "typeless-error":
		return `{"__event":"error","sequence_number":7,"code":"server_error","message":"boom"}`
	case "envelope":
		return `{"__event":"response.output_text.delta","sequence_number":7,"error":{"code":"server_error","message":"boom"}}`
	case "codex-metadata-envelope":
		return `{"__event":"codex.response.metadata","sequence_number":7,"code":"server_error","message":"boom"}`
	case "envelope-unnamed":
		return `{"sequence_number":7,"code":"server_error","message":"boom"}`
	}
	var events []string
	switch terminal {
	case "error":
		events = append(events, `{"type":"error","sequence_number":7,"code":"server_error","message":"boom"}`)
	case "failed":
		events = append(events, `{"type":"response.failed","sequence_number":7,"response":{"id":"resp_1","status":"failed","error":{"code":"server_error","message":"boom"}}}`)
	default:
		events = append(events, `{"type":"response.completed","sequence_number":7,"response":{"id":"resp_1","status":"completed","output":[]}}`)
	}
	return events[0]
}

var terminals = []string{"completed", "error", "failed", "typeless-completed", "typeless-error", "envelope", "envelope-unnamed", "codex-metadata-envelope"}

// codexOriginator makes the client an official Codex client, for which the
// host keeps most codex.* events.
var codexOriginator = map[string]string{"Originator": "codex_cli_rs"}

// frameLines writes one event's fields in order: event, data, id, retry.
// The event: name is the payload type, or a "__event" field taken out of
// the payload (for events whose payload has no type); with neither, the
// frame has no event: line.
func frameLines(event string, n int, order []string) []string {
	name := gjson.Get(event, "type").String()
	if override := gjson.Get(event, "__event"); override.Exists() {
		name = override.String()
		event, _ = sjson.Delete(event, "__event")
	}
	var lines []string
	for _, field := range order {
		switch field {
		case "event":
			if name != "" {
				lines = append(lines, "event: "+name)
			}
		case "data":
			lines = append(lines, "data: "+event)
		case "id":
			lines = append(lines, "id: "+strconv.Itoa(100+n))
		case "retry":
			lines = append(lines, "retry: 1500")
		case "comment":
			lines = append(lines, ": keep-alive")
		}
	}
	return lines
}

var fieldOrders = [][]string{
	{"event", "data"},
	{"data", "event"},
	{"event", "id", "data"},
	{"event", "data", "id"},
	{"id", "event", "data"},
	{"retry", "event", "data"},
	{"event", "data", "retry"},
	{"data", "id", "event"},
	{"event", "comment", "data"},
	{"comment", "event", "data"},
	{"event", "data", "comment"},
}

// delimited is one frame per chunk, each ended by a blank line.
func delimited(events []string, order []string) []string {
	var chunks []string
	for n, event := range events {
		chunks = append(chunks, strings.Join(frameLines(event, n, order), "\n")+"\n\n")
	}
	return chunks
}

// linePerChunk is the native executors' shape: each line its own chunk, no
// newlines, no blank lines.
func linePerChunk(events []string, order []string) []string {
	var chunks []string
	for n, event := range events {
		chunks = append(chunks, frameLines(event, n, order)...)
	}
	return chunks
}

// Review 11: the host's own behaviour is the oracle. Every stream in the
// corpus (both chunk shapes, every field order, comments, each terminal
// event, the corpus executor and the real Codex and xAI executors, chat
// completions) reaches the client with the same dispatched events, each
// with its event name, data, id and retry, with the plugin switched on as
// without it. The request carries a secret, so the plugin keeps state and
// frames the stream; the stream carries none.
func TestDifferentialAgainstTheHost(t *testing.T) {
	h := newDifferentialHost(t)
	for _, terminal := range terminals {
		events := corpusEvents(terminal)
		for i, order := range fieldOrders {
			label := terminal + "/" + strings.Join(order, ",")
			h.compare(t, differentialCase{name: "fake delimited " + label, model: fakeModel, chunks: delimited(events, order)})
			h.compare(t, differentialCase{name: "fake one chunk " + label, model: fakeModel, chunks: []string{strings.Join(delimited(events, order), "")}})
			h.compare(t, differentialCase{name: "fake line per chunk " + label, model: fakeModel, chunks: linePerChunk(events, order)})
			// A real executor's upstream error cools its auth down: a fresh
			// host for each such case.
			upstream := strings.Join(delimited(events, order), "")
			newDifferentialHost(t).compare(t, differentialCase{name: "codex " + label, model: codexModel, upstream: upstream})
			if i < 4 {
				newDifferentialHost(t).compare(t, differentialCase{name: "xai " + label, model: xaiModel, upstream: upstream})
			}
		}
	}
	chat := []string{
		`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"},"finish_reason":null}]}`,
		`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"f","arguments":"{\"a\":"}}]},"finish_reason":null}]}`,
		`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":null}]}`,
		`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
	}
	h.compare(t, differentialCase{name: "chat", model: chatModel, chunks: chat})

	// Frames whose data is not valid JSON: the host's validator fails the
	// stream there, after the valid frames before it.
	good := delimited(corpusEvents("completed")[:3], []string{"event", "data"})
	bad := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\n\n"
	for name, chunks := range map[string][]string{
		"malformed alone":            {bad},
		"malformed after frames":     append(append([]string(nil), good...), bad),
		"malformed in a frame chunk": {good[0], good[1] + bad + good[2]},
		"malformed after a fragment": {good[0], good[1][:20], good[1][20:] + bad},
	} {
		h.compare(t, differentialCase{name: name, model: fakeModel, chunks: chunks})
	}

	// Review 13, finding 1: comments in fragments; a comment line ends only
	// at its newline.
	created := `{"type":"response.created","sequence_number":0,"response":{"id":"resp_1"}}`
	for name, chunks := range map[string][]string{
		"comment fragment then data": {": keepalive ", "da", "ta: " + created + "\n\n"},
		"comment inside a frame":     {"event: response.created", ": keep-alive", "data: " + created},
		"comment frames":             {": a\n\n", ": b", "\n\nevent: response.created\ndata: " + created + "\n\n"},
	} {
		h.compare(t, differentialCase{name: name, model: fakeModel, chunks: chunks})
	}
	commented := ": keep-alive\nevent: response.created\n: inside\ndata: " + created + "\n\n: after\n\n" +
		strings.Join(delimited(corpusEvents("completed")[1:], []string{"event", "data"}), "")
	for k := 1; k < len(commented); k++ {
		h.compare(t, differentialCase{name: fmt.Sprintf("comment split at %d", k), model: fakeModel, chunks: []string{commented[:k], commented[k:]}})
	}

	// Review 16, finding 2: data that starts with blank data lines or
	// spaces, which the host's JSON check accepts.
	for _, form := range []func(string) string{
		func(event string) string { return "data:\ndata: " + event },
		func(event string) string { return "data:    " + event },
		func(event string) string { return "data: \ndata:\n data: " + event },
	} {
		var chunks []string
		for _, event := range corpusEvents("completed") {
			chunks = append(chunks, "event: "+gjson.Get(event, "type").String()+"\n"+form(event)+"\n\n")
		}
		h.compare(t, differentialCase{name: "spaced data " + form("x"), model: fakeModel, chunks: chunks})
	}

	// Review 14, finding 5: the same corpus from an official Codex client.
	for _, terminal := range terminals {
		events := corpusEvents(terminal)
		for _, order := range fieldOrders[:4] {
			label := "codex client " + terminal + "/" + strings.Join(order, ",")
			h.compare(t, differentialCase{name: label + " delimited", model: fakeModel, chunks: delimited(events, order), headers: codexOriginator})
			h.compare(t, differentialCase{name: label + " line per chunk", model: fakeModel, chunks: linePerChunk(events, order), headers: codexOriginator})
		}
	}

	// Review 14, finding 2: fragments the host's validator passes on before
	// a line is whole, and many fragmentations of a full stream.
	h.compare(t, differentialCase{name: "three fragments", model: fakeModel, chunks: []string{
		"event: response.output_text.delta\nda",
		`ta: {"type":"response.output_text.delta","content_index"`,
		":0,\"delta\":\"hello\"}\n\n",
		"event: response.completed\ndata: " + terminalEvent("completed") + "\n\n",
	}})
	full := strings.Join(delimited(corpusEvents("completed"), []string{"comment", "event", "data", "id", "retry"}), "")
	var each []string
	for i := range len(full) {
		each = append(each, full[i:i+1])
	}
	h.compare(t, differentialCase{name: "each byte", model: fakeModel, chunks: each})
	for seed := 1; seed < 100; seed++ {
		var chunks []string
		for offset := 0; offset < len(full); {
			n := min(1+((seed*37+offset*19)%63), len(full)-offset)
			chunks = append(chunks, full[offset:offset+n])
			offset += n
		}
		h.compare(t, differentialCase{name: fmt.Sprintf("fragmented %d", seed), model: fakeModel, chunks: chunks})
	}

	// Two chunks, split at every byte boundary.
	for _, c := range []struct {
		terminal string
		order    []string
	}{{"completed", []string{"event", "data"}}, {"error", []string{"data", "id", "event"}}} {
		text := strings.Join(delimited(corpusEvents(c.terminal), c.order), "")
		for k := 1; k < len(text); k++ {
			h.compare(t, differentialCase{name: fmt.Sprintf("split %s at %d", c.terminal, k), model: fakeModel, chunks: []string{text[:k], text[k:]}})
		}
	}
}

// normalized is a client's events with the delta events of one part joined
// and sequence numbers left out, and with replace applied to every decoded
// string, so a stream with secrets restored can be compared with the same
// stream as the host sends it without the plugin.
func normalized(events []dispatchedEvent, replace func(string) string) []string {
	var out []string
	var deltas []string
	lastKey := ""
	flush := func() {
		if lastKey != "" {
			out = append(out, lastKey+" "+replace(strings.Join(deltas, "")))
		}
		lastKey, deltas = "", nil
	}
	var rewrite func(v any) any
	rewrite = func(v any) any {
		switch value := v.(type) {
		case string:
			return replace(value)
		case map[string]any:
			for k, child := range value {
				value[k] = rewrite(child)
			}
		case []any:
			for i, child := range value {
				value[i] = rewrite(child)
			}
		}
		return v
	}
	for _, e := range events {
		data := gjson.Parse(e.Data)
		eventType := data.Get("type").String()
		if strings.HasSuffix(eventType, ".delta") {
			key := e.Event + " " + eventType + "/" + data.Get("item_id").String() + "/" + data.Get("output_index").Raw + "/" + data.Get("content_index").Raw
			if key != lastKey {
				flush()
				lastKey = key
			}
			deltas = append(deltas, data.Get("delta").String())
			continue
		}
		flush()
		var fields map[string]any
		_ = json.Unmarshal([]byte(e.Data), &fields)
		delete(fields, "sequence_number")
		raw, _ := json.Marshal(rewrite(fields))
		out = append(out, e.Event+" "+string(raw))
	}
	flush()
	return out
}

// Review 11, with secrets: the stream carries the request's placeholder in
// text and in tool-call arguments, split across chunks. With the plugin the
// client gets the same events as without it except that the secret is
// restored (deltas of a part joined, sequence numbers aside).
func TestDifferentialWithSecrets(t *testing.T) {
	h := newDifferentialHost(t)
	placeholder := placeholderOf(t, "differential-placeholder", anthropicKey)
	text := `"item_id":"m1","output_index":0,"content_index":0`
	ids := `"item_id":"fc_1","output_index":1`
	args, _ := json.Marshal(`{"k":"` + placeholder + `"}`)
	events := []string{
		`{"type":"response.created","sequence_number":0,"response":{"id":"resp_1"}}`,
		`{"type":"response.output_text.delta","sequence_number":1,` + text + `,"delta":"key ` + placeholder[:6] + `"}`,
		`{"type":"response.output_text.delta","sequence_number":2,` + text + `,"delta":"` + placeholder[6:] + ` ok"}`,
		`{"type":"response.output_text.done","sequence_number":3,` + text + `,"text":"key ` + placeholder + ` ok"}`,
		`{"type":"response.function_call_arguments.delta","sequence_number":4,` + ids + `,"delta":` + string(args) + `}`,
		`{"type":"response.function_call_arguments.done","sequence_number":5,` + ids + `,"arguments":` + string(args) + `}`,
		`{"type":"response.completed","sequence_number":6,"response":{"id":"resp_1","status":"completed","output":[]}}`,
	}
	// Review 11, finding 1: text that ends in the start of a placeholder,
	// then the reply ends with an error: the text arrives whole.
	streams := [][]string{events}
	// Review 16, finding 2: the terminal frame's data starts with an empty
	// data line.
	spacedTerminal := "event: response.completed\ndata:\ndata: " + terminalEvent("completed") + "\n\n"
	compareRestored(t, h, placeholder, []string{
		delimited(events[:1], []string{"event", "data"})[0],
		delimited([]string{`{"type":"response.output_text.delta","sequence_number":1,` + text + `,"delta":"before ` + placeholder[:6] + `"}`}, []string{"event", "data"})[0],
		spacedTerminal,
	}, "spaced terminal")
	for _, terminal := range terminals {
		if terminal == "codex-metadata-envelope" {
			// For a client that is not Codex the host drops this event, so
			// the stream ends with no terminal event: held text is then lost
			// (no end-of-stream hook). Run below for a Codex client only.
			continue
		}
		streams = append(streams, []string{
			events[0],
			`{"type":"response.output_text.delta","sequence_number":1,` + text + `,"delta":"before ` + placeholder[:6] + `"}`,
			terminalEvent(terminal),
		})
	}
	// Review 13, finding 2: arguments holding a JSON document written with
	// \u escapes, split just before the placeholder.
	nested, cut := nestedArguments(placeholder)
	first, _ := json.Marshal(nested[:cut])
	second, _ := json.Marshal(nested[cut:])
	whole, _ := json.Marshal(nested)
	streams = append(streams, []string{
		events[0],
		`{"type":"response.function_call_arguments.delta","sequence_number":1,` + ids + `,"delta":` + string(first) + `}`,
		`{"type":"response.function_call_arguments.delta","sequence_number":2,` + ids + `,"delta":` + string(second) + `}`,
		`{"type":"response.function_call_arguments.done","sequence_number":3,` + ids + `,"arguments":` + string(whole) + `}`,
		events[len(events)-1],
	})
	for _, stream := range streams {
		for _, order := range fieldOrders[:4] {
			for _, shape := range []func([]string, []string) []string{delimited, linePerChunk} {
				label := fmt.Sprint(order, " ", gjson.Get(stream[len(stream)-1], "__event").String())
				h.headers = codexOriginator
				codexStream := []string{
					events[0],
					`{"type":"response.output_text.delta","sequence_number":1,` + text + `,"delta":"before ` + placeholder[:6] + `"}`,
					terminalEvent("codex-metadata-envelope"),
				}
				for _, order := range fieldOrders[:4] {
					for _, shape := range []func([]string, []string) []string{delimited, linePerChunk} {
						compareRestored(t, h, placeholder, shape(codexStream, order), fmt.Sprint(order, " codex metadata envelope, codex client"))
					}
				}
				h.headers = nil
				compareRestored(t, h, placeholder, shape(stream, order), label)
				h.headers = codexOriginator
				compareRestored(t, h, placeholder, shape(stream, order), label+" codex client")
			}
		}
	}
	h.headers = nil
}

// compareRestored runs a stream with the request's placeholder without and
// with the plugin and checks the events differ only by the restored secret.
func compareRestored(t *testing.T, h *differentialHost, placeholder string, chunks []string, label string) {
	t.Helper()
	h.corpus.mu.Lock()
	h.corpus.chunks[fakeModel] = chunks
	h.corpus.mu.Unlock()
	restore := func(text string) string { return strings.ReplaceAll(text, placeholder, anthropicKey) }
	want := normalized(dispatch(h.run(t, false, fakeModel)), restore)
	got := normalized(dispatch(h.run(t, true, fakeModel)), func(text string) string { return text })
	if len(want) == 0 || !reflect.DeepEqual(want, got) {
		t.Fatalf("%s: events differ beyond the restored secret\nwant %q\ngot  %q", label, want, got)
	}
}

// Review 12, finding 1, through the real Codex executor: a switched request
// whose body is not JSON (a trailing comma, which the translator still
// reads) is rejected with the plugin and never reaches upstream; without
// the plugin the secret would.
func TestUnscrubbableRequestNeverReachesUpstream(t *testing.T) {
	h := newDifferentialHost(t)
	h.set(differentialCase{model: codexModel, upstream: strings.Join(delimited(corpusEvents("completed"), []string{"event", "data"}), "")})
	body := `{"model":"` + codexModel + `","messages":[{"role":"user","content":"my key is ` + anthropicKey + `"},],"stream":true}`
	reached := func() bool {
		h.upstream.Lock()
		defer h.upstream.Unlock()
		for _, request := range h.upstream.received {
			if strings.Contains(request, anthropicKey) {
				return true
			}
		}
		return false
	}
	h.send(false, "/v1/chat/completions", body)
	if !reached() {
		t.Fatal("control: without the plugin the malformed request did not reach upstream with its secret")
	}
	h.upstream.Lock()
	h.upstream.received = nil
	h.upstream.Unlock()
	status, response := h.send(true, "/v1/chat/completions", body)
	if status != http.StatusBadRequest || !strings.Contains(response, "secret_scrub_unparsable") {
		t.Fatalf("status %d: %s", status, response)
	}
	if reached() {
		t.Fatal("the secret reached upstream")
	}
}

// Review 14, finding 3: with bootstrap retries, an attempt that fails
// before its first delivered chunk leaves nothing behind: the retry's
// arguments arrive alone and restored, and a stream without secrets
// reaches the client as without the plugin.
func TestBootstrapRetryStartsAfresh(t *testing.T) {
	h := newDifferentialHostWith(t, sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{BootstrapRetries: 1}})
	placeholder := placeholderOf(t, "r14-3-seed", anthropicKey)
	finish := `{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`
	h.set(differentialCase{model: chatModel, chunks: []string{
		toolChunk(0, `{"failed":"`+placeholder[:6]), attemptFails, nextAttempt,
		toolChunk(0, `{"k":"`+placeholder+`"}`), finish,
	}})
	body := h.run(t, true, chatModel)
	var args strings.Builder
	for _, e := range dispatch(body) {
		args.WriteString(gjson.Get(e.Data, "choices.0.delta.tool_calls.0.function.arguments").String())
	}
	if want := `{"k":"` + anthropicKey + `"}`; args.String() != want {
		t.Fatalf("arguments %q, want %q; body %s", args.String(), want, body)
	}
	// Without secrets and with nothing held, the host sees what it sees
	// without the plugin, retry or not. (A chunk the plugin holds, one that
	// ends in "<" say, is not yet delivered when its attempt fails, so the
	// host may retry where without the plugin it could not.)
	h.compare(t, differentialCase{name: "retry without secrets", model: chatModel, chunks: []string{
		toolChunk(0, `{"a":"x`), attemptFails, nextAttempt, toolChunk(0, `{"a":1}`), finish,
	}})
	events := corpusEvents("completed")
	retried := append(append([]string{}, delimited(events[:2], []string{"event", "data"})...), attemptFails, nextAttempt)
	retried = append(retried, delimited(events, []string{"event", "data"})...)
	h.compare(t, differentialCase{name: "responses retry", model: fakeModel, chunks: retried})
}

// Review 16, finding 1, through the real chat handler: held tool-call
// arguments and the finishing chunk that releases them go out as one chat
// chunk that carries its finish_reason, so the stream ends as it should,
// with [DONE].
func TestHeldChatChunksKeepTheirFinish(t *testing.T) {
	h := newDifferentialHost(t)
	placeholder := placeholderOf(t, "r16-1-seed", anthropicKey)
	h.set(differentialCase{model: chatModel, chunks: []string{
		toolChunk(0, `{"k":"`+placeholder[:6]),
		toolChunk(0, placeholder[6:]+`"}`),
		`{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
	}})
	body := h.run(t, true, chatModel)
	var args strings.Builder
	finished := false
	for _, e := range dispatch(body) {
		args.WriteString(gjson.Get(e.Data, "choices.0.delta.tool_calls.0.function.arguments").String())
		finished = finished || gjson.Get(e.Data, "choices.0.finish_reason").String() == "tool_calls"
	}
	if !strings.Contains(body, "data: [DONE]") || !finished || args.String() != `{"k":"`+anthropicKey+`"}` {
		t.Fatalf("body %s", body)
	}
}
