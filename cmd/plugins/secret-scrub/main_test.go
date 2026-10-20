package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"github.com/tidwall/gjson"
)

const anthropicKey = "sk-ant-api03-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"

const pemKey = "-----BEGIN PRIVATE KEY-----\nMIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEA\nq7BFUpkGp3+XQ/wl==\n-----END PRIVATE KEY-----"

func call[T any](t *testing.T, method string, req any) T {
	t.Helper()
	raw, errMarshal := json.Marshal(req)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	out, errHandle := handleMethod(method, raw)
	if errHandle != nil {
		t.Fatal(errHandle)
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(out, &env); errUnmarshal != nil || !env.OK {
		t.Fatalf("envelope %s: %v", out, errUnmarshal)
	}
	var result T
	if errUnmarshal := json.Unmarshal(env.Result, &result); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	return result
}

func reconfigure(t *testing.T, yaml string) {
	t.Helper()
	call[registration](t, pluginabi.MethodPluginReconfigure, lifecycleRequest{ConfigYAML: []byte(yaml)})
}

func scrubbedRequest(t *testing.T, requestID, body string, on bool) pluginapi.RequestInterceptResponse {
	t.Helper()
	headers := http.Header{"Authorization": {"Bearer client"}}
	if on {
		headers.Set("X-JS-Scrub", "on")
	}
	return call[pluginapi.RequestInterceptResponse](t, pluginabi.MethodRequestInterceptBefore,
		pluginapi.RequestInterceptRequest{RequestID: requestID, SourceFormat: formatChat, Headers: headers, Body: []byte(body)})
}

// upstream is the body that goes upstream for a switched request.
func upstream(t *testing.T, requestID, body string) string {
	t.Helper()
	resp := scrubbedRequest(t, requestID, body, true)
	if len(resp.Body) == 0 {
		return body
	}
	if !json.Valid(resp.Body) {
		t.Fatalf("body is no longer JSON: %s", resp.Body)
	}
	return string(resp.Body)
}

func userBody(content string) string {
	raw, _ := json.Marshal(map[string]any{"messages": []any{map[string]any{"role": "user", "content": content}}})
	return string(raw)
}

// placeholderOf is the placeholder a request for value gets.
func placeholderOf(t *testing.T, requestID, value string) string {
	t.Helper()
	placeholder := placeholderPattern.FindString(decodedContent(t, upstream(t, requestID, userBody(value))))
	if placeholder == "" {
		t.Fatalf("no placeholder for %q", value)
	}
	return placeholder
}

func decodedContent(t *testing.T, body string) string {
	t.Helper()
	var req struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	if errUnmarshal := json.Unmarshal([]byte(body), &req); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	var parts []string
	for _, m := range req.Messages {
		parts = append(parts, m.Content)
	}
	return strings.Join(parts, "\n")
}

func reply(t *testing.T, requestID, body string) string {
	t.Helper()
	out := call[pluginapi.ResponseInterceptResponse](t, pluginabi.MethodResponseInterceptAfter,
		pluginapi.ResponseInterceptRequest{RequestID: requestID, Body: []byte(body)})
	if len(out.Body) == 0 {
		return body
	}
	return string(out.Body)
}

func deliver(t *testing.T, requestID string, index int, chunk string) string {
	t.Helper()
	out := call[pluginapi.StreamChunkInterceptResponse](t, pluginabi.MethodResponseInterceptStreamChunk,
		pluginapi.StreamChunkInterceptRequest{RequestID: requestID, ChunkIndex: index, Body: []byte(chunk)})
	if out.DropChunk {
		return ""
	}
	if len(out.Body) == 0 {
		return chunk
	}
	return string(out.Body)
}

type chatChunk struct {
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				Index    int `json:"index"`
				Function struct {
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
	} `json:"choices"`
}

// chatStream delivers bare chat-completion chunks, as the OpenAI handler
// passes them, and returns the content text and each tool call's arguments.
func chatStream(t *testing.T, requestID string, chunks []string) (string, map[int]string) {
	t.Helper()
	var text strings.Builder
	args := make(map[int]string)
	// The stream ends as a chat completion stream does, with a finishing
	// chunk; held tool-call arguments arrive there.
	chunks = append(chunks, `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
	for i, chunk := range chunks {
		delivered := deliver(t, requestID, i, chunk)
		if delivered == "" {
			continue // held, to go on with a later chunk
		}
		// Chunks sent together are joined as the chat handler writes one.
		for _, part := range strings.Split(delivered, "\n\ndata: ") {
			var event chatChunk
			if errUnmarshal := json.Unmarshal([]byte(part), &event); errUnmarshal != nil {
				t.Fatalf("chunk %d not JSON: %q", i, delivered)
			}
			for _, choice := range event.Choices {
				text.WriteString(choice.Delta.Content)
				for _, tool := range choice.Delta.ToolCalls {
					args[tool.Index] += tool.Function.Arguments
				}
			}
		}
	}
	return text.String(), args
}

func contentChunk(content string, finish bool) string {
	reason := "null"
	if finish {
		reason = `"stop"`
	}
	raw, _ := json.Marshal(content)
	return `{"choices":[{"index":0,"delta":{"content":` + string(raw) + `},"finish_reason":` + reason + `}]}`
}

func toolChunk(tool int, args string) string {
	raw, _ := json.Marshal(args)
	return `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":` + string(rune('0'+tool)) + `,"function":{"arguments":` + string(raw) + `}}]},"finish_reason":null}]}`
}

func TestSwitchedRequestGoesUpstreamWithPlaceholdersAndNoSwitchHeader(t *testing.T) {
	body := userBody("key is " + anthropicKey + " and\nOPENAI_API_KEY=abcd1234efgh5678 ok")
	resp := scrubbedRequest(t, "r1", body, true)

	if strings.Contains(string(resp.Body), anthropicKey) || strings.Contains(string(resp.Body), "abcd1234efgh5678") {
		t.Fatalf("secret reached upstream: %s", resp.Body)
	}
	if !json.Valid(resp.Body) {
		t.Fatalf("body is no longer JSON: %s", resp.Body)
	}
	if len(resp.ClearHeaders) != 1 || !strings.EqualFold(resp.ClearHeaders[0], "X-JS-Scrub") {
		t.Fatalf("switch header not cleared: %v", resp.ClearHeaders)
	}
	again := scrubbedRequest(t, "r2", body, true)
	if string(again.Body) != string(resp.Body) {
		t.Fatalf("one value got two placeholders:\n%s\n%s", resp.Body, again.Body)
	}
}

func TestRequestWithoutSwitchPassesUnchanged(t *testing.T) {
	resp := scrubbedRequest(t, "plain", userBody(anthropicKey), false)
	if len(resp.Body) != 0 || len(resp.ClearHeaders) != 0 {
		t.Fatalf("unswitched request changed: %+v", resp)
	}
}

func TestReplyGetsTheRealValueBack(t *testing.T) {
	placeholder := placeholderOf(t, "r3", anthropicKey)
	body := `{"choices":[{"message":{"content":"use ` + placeholder + `","tool_calls":[{"function":{"arguments":"{\"k\":\"` + placeholder + `\"}"}}]}}]}`
	out := reply(t, "r3", body)
	if strings.Count(out, anthropicKey) != 2 || strings.Contains(out, "SECRET_") {
		t.Fatalf("reply not restored: %s", out)
	}
}

func TestPlaceholderSplitAcrossStreamChunksIsRestored(t *testing.T) {
	placeholder := placeholderOf(t, "r4", anthropicKey)
	text, _ := chatStream(t, "r4", []string{contentChunk("key: "+placeholder[:6], false), contentChunk(placeholder[6:]+" done", false)})
	if text != "key: "+anthropicKey+" done" {
		t.Fatalf("stream text = %q", text)
	}
}

func TestImageDataIsNotScrubbed(t *testing.T) {
	image := strings.Repeat("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==", 20)
	body := `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,` + image + `"}}]}]}`
	if out := upstream(t, "r5", body); out != body {
		t.Fatalf("image data changed: %s", out)
	}
}

// Finding 8: a small PNG whose base64 has no + or / is still image data.
func TestSmallImageWithoutPlusOrSlashIsNotScrubbed(t *testing.T) {
	png := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII"
	if strings.ContainsAny(png, "+/") {
		t.Fatal("fixture has + or /")
	}
	for _, body := range []string{
		`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,` + png + `"}}]}]}`,
		`{"input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,` + png + `"}]}]}`,
	} {
		if out := upstream(t, "r8", body); out != body {
			t.Fatalf("image data changed: %s", out)
		}
	}
}

func TestRememberedValuesExpireAfterTheTTL(t *testing.T) {
	s := newScrubber()
	clock := time.Unix(0, 0)
	s.now = func() time.Time { return clock }
	s.ttl = time.Hour
	s.Scrub("first", []byte(userBody(anthropicKey)))
	clock = clock.Add(2 * time.Hour)
	s.Sweep()
	if len(s.values) != 0 || len(s.byValue) != 0 {
		t.Fatalf("expired value still remembered: %d", len(s.values))
	}
}

// Finding 1: a reply restores only the secrets of its own request.
func TestReplyRestoresOnlyTheRequestsOwnSecrets(t *testing.T) {
	placeholder := placeholderOf(t, "owner", anthropicKey)
	upstream(t, "other", userBody("my own key is ghp_0123456789abcdefghijABCDEFGHIJ0123456789"))
	out := reply(t, "other", `{"choices":[{"message":{"content":"`+placeholder+`"}}]}`)
	if strings.Contains(out, anthropicKey) {
		t.Fatalf("another request's secret was disclosed: %s", out)
	}
}

// Finding 2: ordinary .env assignments are scrubbed.
func TestEnvAssignmentsAreScrubbed(t *testing.T) {
	values := []string{"abcdefghijklmno", "pqrstuvwxyzabcd", "short", "quoted value"}
	body := userBody("TOKEN=" + values[0] + "\nPASSWORD=" + values[1] + "\nDB_PASSWORD=" + values[2] + "\nexport API_KEY=\"" + values[3] + "\"\nHOME=/home/me")
	out := decodedContent(t, upstream(t, "env", body))
	for _, value := range values {
		if strings.Contains(out, value) {
			t.Fatalf("%q reached upstream: %s", value, out)
		}
	}
	if !strings.Contains(out, "HOME=/home/me") {
		t.Fatalf("non-secret assignment changed: %s", out)
	}
}

// Finding 3: secrets are found in decoded strings, so JSON escaping neither
// hides a secret nor gets cut, and a match never spans two strings.
func TestSecretsAreFoundInDecodedStrings(t *testing.T) {
	escaped := strings.Replace(anthropicKey, "-", `\u002d`, 1)
	body := `{"messages":[{"role":"user","content":"` + escaped + `"}]}`
	if out := decodedContent(t, upstream(t, "dec1", body)); strings.Contains(out, anthropicKey) {
		t.Fatalf("escaped key reached upstream: %s", out)
	}

	token := "Zx9Qw3Er7Ty1Ui5Op2As8Df4Gh6"
	body = `{"messages":[{"role":"user","content":"caf\u00e9 ` + token + `"}]}`
	out := decodedContent(t, upstream(t, "dec2", body))
	if strings.Contains(out, token) || !strings.HasPrefix(out, "café <SECRET_") {
		t.Fatalf("unicode escape mangled: %q", out)
	}

	body = `{"messages":[{"role":"user","content":"-----BEGIN PRIVATE KEY----- is a marker"},{"role":"assistant","content":"middle"},{"role":"user","content":"and -----END PRIVATE KEY----- too"}]}`
	var req struct{ Messages []json.RawMessage }
	if errUnmarshal := json.Unmarshal([]byte(upstream(t, "dec3", body)), &req); errUnmarshal != nil || len(req.Messages) != 3 {
		t.Fatalf("messages collapsed: %d %v", len(req.Messages), errUnmarshal)
	}
}

// Finding 4: high-entropy tokens need not mix cases, may hold + or /, and
// may be long.
func TestEntropyTokensOfAnyAlphabetAreScrubbed(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	long := make([]byte, 300)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	for i := range long {
		long[i] = alphabet[random.Intn(len(alphabet))]
	}
	tokens := []string{
		"k3j9x2m7q1w8e4r6t0y5u2i8o3p7a1s6",           // lower case and digits
		"wJalrXUtnFEMI/K7MDENG/bPxRfiCY1EXAMPLEKEY+", // AWS secret shape
		string(long),
	}
	out := decodedContent(t, upstream(t, "ent", userBody(strings.Join(tokens, " and "))))
	for _, token := range tokens {
		if strings.Contains(out, token) {
			t.Fatalf("%q reached upstream: %s", token, out)
		}
	}
	path := "see /home/ronald_rump/CLIProxyAPI-scrub/cmd/plugins/secret-scrub2 and github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	if out := decodedContent(t, upstream(t, "ent2", userBody(path))); out != path {
		t.Fatalf("paths scrubbed: %s", out)
	}
}

// Finding 5: a PEM key restored into tool-call arguments keeps them JSON.
func TestPEMRestoredIntoToolArgumentsKeepsThemJSON(t *testing.T) {
	placeholder := placeholderOf(t, "pem", pemKey)
	args := `{"path":"k.pem","content":"` + placeholder + `"}`
	argsJSON, _ := json.Marshal(args)
	out := reply(t, "pem", `{"choices":[{"message":{"tool_calls":[{"function":{"arguments":`+string(argsJSON)+`}}]}}]}`)
	var body struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					Function struct{ Arguments string } `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if errUnmarshal := json.Unmarshal([]byte(out), &body); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	checkArgs := func(raw string) {
		t.Helper()
		var parsed struct{ Content string }
		if errUnmarshal := json.Unmarshal([]byte(raw), &parsed); errUnmarshal != nil || parsed.Content != pemKey {
			t.Fatalf("arguments %q: %v", raw, errUnmarshal)
		}
	}
	checkArgs(body.Choices[0].Message.ToolCalls[0].Function.Arguments)

	placeholderOf(t, "pem-stream", pemKey)
	_, streamed := chatStream(t, "pem-stream", []string{
		toolChunk(0, `{"path":"k.pem","content":"`+placeholder[:4]),
		toolChunk(0, placeholder[4:]+`"}`),
	})
	checkArgs(streamed[0])
}

// Finding 6: parallel tool calls hold their placeholder tails apart.
func TestParallelToolCallsHoldTailsApart(t *testing.T) {
	placeholder := placeholderOf(t, "par", anthropicKey)
	_, args := chatStream(t, "par", []string{
		toolChunk(0, `{"a":"`+placeholder[:6]),
		toolChunk(1, `{"b":"x`),
		toolChunk(0, placeholder[6:]+`"}`),
		toolChunk(1, `"}`),
	})
	if args[0] != `{"a":"`+anthropicKey+`"}` || args[1] != `{"b":"x"}` {
		t.Fatalf("tool arguments = %v", args)
	}
}

// Finding 7: text held back at the end of a stream is delivered when the
// choice finishes.
func TestHeldTextIsDeliveredWhenTheChoiceFinishes(t *testing.T) {
	placeholderOf(t, "flush", anthropicKey)
	text, _ := chatStream(t, "flush", []string{contentChunk("a <", false), `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`})
	if text != "a <" {
		t.Fatalf("stream text = %q", text)
	}
}

// Finding 7, Responses: a held tail is sent as its own delta event before
// the event that ends its content part.
func TestHeldTextIsDeliveredBeforeThePartEnds(t *testing.T) {
	placeholderOf(t, "flush-typed", anthropicKey)
	chunks := []string{
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"a <SEC\"}\n\n",
		"event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"text\":\"a <SEC\"}\n\n",
	}
	var text strings.Builder
	for i, chunk := range chunks {
		for _, line := range strings.Split(deliver(t, "flush-typed", i, chunk), "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			text.WriteString(gjson.Get(strings.TrimPrefix(line, "data: "), "delta").String())
		}
	}
	if text.String() != "a <SEC" {
		t.Fatalf("stream text = %q", text.String())
	}
}

// Finding 9: reloading the configuration while requests are scrubbed is
// race free (run with -race).
func TestReloadDuringScrubbingIsRaceFree(t *testing.T) {
	defer reconfigure(t, "")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			raw, _ := json.Marshal(lifecycleRequest{ConfigYAML: []byte("entropy: " + map[bool]string{true: "true", false: "false"}[i%2 == 0])})
			if _, errHandle := handleMethod(pluginabi.MethodPluginReconfigure, raw); errHandle != nil {
				t.Error(errHandle)
			}
		}
	}()
	for i := 0; i < 50; i++ {
		upstream(t, "race", userBody(anthropicKey))
	}
	wg.Wait()
}

// Finding 10: the TTL never takes the values of a request still in flight.
func TestTTLKeepsValuesOfARequestInFlight(t *testing.T) {
	s := newScrubber()
	clock := time.Unix(0, 0)
	s.now = func() time.Time { return clock }
	s.ttl = time.Hour
	scrubbed := s.Scrub("slow", []byte(userBody(anthropicKey)))
	placeholder := placeholderPattern.FindString(decodedContent(t, string(scrubbed)))
	clock = clock.Add(50 * time.Minute)
	s.RestoreChunk("slow", []byte(contentChunk("still going", false)))
	clock = clock.Add(40 * time.Minute)
	s.Sweep()
	if out := string(s.Restore("slow", []byte(`{"content":"`+placeholder+`"}`))); !strings.Contains(out, anthropicKey) {
		t.Fatalf("in-flight request lost its values: %s", out)
	}
}

// Finding 11: the placeholder is the HMAC of the decoded value, whatever
// the JSON escaping.
func TestOneValueOneIDWhateverTheEscaping(t *testing.T) {
	a := upstream(t, "esc1", `{"messages":[{"role":"user","content":"`+strings.ReplaceAll(pemKey, "\n", `\n`)+`"}]}`)
	b := upstream(t, "esc2", `{"messages":[{"role":"user","content":"`+strings.ReplaceAll(pemKey, "\n", `\u000a`)+`"}]}`)
	if placeholderPattern.FindString(decodedContent(t, a)) != placeholderPattern.FindString(decodedContent(t, b)) {
		t.Fatalf("two ids for one value:\n%s\n%s", a, b)
	}
}

// Finding 12: the header that is cleared is the header that switched
// scrubbing on, while the header name is reloaded.
func TestClearedHeaderIsTheSwitchThatMatched(t *testing.T) {
	defer reconfigure(t, "")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			raw, _ := json.Marshal(lifecycleRequest{ConfigYAML: []byte("header: X-" + map[bool]string{true: "A", false: "B"}[i%2 == 0])})
			if _, errHandle := handleMethod(pluginabi.MethodPluginReconfigure, raw); errHandle != nil {
				t.Error(errHandle)
			}
		}
	}()
	for running := true; running; {
		select {
		case <-done:
			running = false
		default:
		}
		resp := call[pluginapi.RequestInterceptResponse](t, pluginabi.MethodRequestInterceptBefore,
			pluginapi.RequestInterceptRequest{RequestID: "hdr", SourceFormat: formatChat, Headers: http.Header{"X-A": {"on"}}, Body: []byte(userBody(anthropicKey))})
		if len(resp.ClearHeaders) > 0 && resp.ClearHeaders[0] != "X-A" {
			t.Fatalf("cleared %v, switched by X-A", resp.ClearHeaders)
		}
	}
}

// Finding 13: two values whose HMACs collide get two placeholders and each
// restores as itself.
func TestCollidingIDsRestoreTheirOwnValues(t *testing.T) {
	s := newScrubber()
	s.id = func(key []byte, value string, attempt int) string {
		if attempt == 0 {
			return "deadbeef"
		}
		return hmacID(key, value, attempt)
	}
	other := "sk-proj-ZyXwVuTsRqPoNmLkJiHgFeDcBa9876543210"
	scrubbed := decodedContent(t, string(s.Scrub("col", []byte(userBody(anthropicKey+" "+other)))))
	placeholders := placeholderPattern.FindAllString(scrubbed, -1)
	if len(placeholders) != 2 || placeholders[0] == placeholders[1] {
		t.Fatalf("placeholders = %v", placeholders)
	}
	out := string(s.Restore("col", []byte(`{"content":"`+placeholders[1]+`"}`)))
	if !strings.Contains(out, other) {
		t.Fatalf("collision restored the wrong value: %s", out)
	}
}

// Signed reasoning goes upstream and back byte for byte: chat
// reasoning_blocks and Responses encrypted_content are neither scrubbed nor
// restored.
func TestSignedReasoningIsLeftAlone(t *testing.T) {
	sig := "EqQBCgIYAhIM1gbcDa9GJwZA2b3hGgxKIZsKgP1ZbCXh0NAiMHt8abc1xyz9"
	body := `{"messages":[{"role":"user","content":"` + anthropicKey + `"},{"role":"assistant","content":"ok","reasoning_blocks":[{"type":"thinking","thinking":"use Zx9Qw3Er7Ty1Ui5Op2As8Df4Gh6","signature":"` + sig + `"},{"type":"redacted_thinking","data":"` + sig + `"}]}]}`
	out := upstream(t, "signed", body)
	if strings.Count(out, sig) != 2 || !strings.Contains(out, "Zx9Qw3Er7Ty1Ui5Op2As8Df4Gh6") {
		t.Fatalf("signed reasoning changed: %s", out)
	}
	placeholder := placeholderPattern.FindString(decodedContent(t, `{"messages":[`+gjson.Get(out, "messages.0").Raw+`]}`))
	got := reply(t, "signed", `{"choices":[{"message":{"content":"`+placeholder+`","reasoning_blocks":[{"type":"thinking","thinking":"`+placeholder+`","signature":"s"}]}}]}`)
	if gjson.Get(got, "choices.0.message.reasoning_blocks.0.thinking").String() != placeholder ||
		gjson.Get(got, "choices.0.message.content").String() != anthropicKey {
		t.Fatalf("reply = %s", got)
	}
}

// Review 2, finding 1: field names exempt only protocol reasoning, not JSON
// a user wrote.
func TestReasoningFieldNamesInUserTextAreScrubbed(t *testing.T) {
	for _, content := range []string{
		`{"signature":"` + anthropicKey + `"}`,
		`{"type":"thinking","thinking":"` + anthropicKey + `"}`,
		`{"reasoning_content":"` + anthropicKey + `"}`,
	} {
		if out := upstream(t, "r2-1", userBody(content)); strings.Contains(out, anthropicKey) {
			t.Fatalf("secret in %s reached upstream: %s", content, out)
		}
	}
}

// Review 2, finding 2: file data is still searched for key shapes and .env
// assignments.
func TestFileDataIsStillSearchedForKnownSecrets(t *testing.T) {
	ghToken := "ghp_0123456789abcdefghijABCDEFGHIJ0123456789"
	long := strings.Repeat("Qm9i", 130) + ghToken
	dataURI := "data:text/plain;base64,QUJD\nAPI_KEY=abcdef123456"
	raw, _ := json.Marshal(map[string]any{"messages": []any{
		map[string]any{"role": "user", "content": long},
		map[string]any{"role": "user", "content": dataURI},
	}})
	out := decodedContent(t, upstream(t, "r2-2", string(raw)))
	if strings.Contains(out, ghToken) || strings.Contains(out, "abcdef123456") {
		t.Fatalf("secret in file data reached upstream: %s", out)
	}
}

// Review 2, finding 3: object keys are scrubbed too.
func TestSecretsInObjectKeysAreScrubbed(t *testing.T) {
	if out := upstream(t, "r2-3", userBody(`{"`+anthropicKey+`":"active"}`)); strings.Contains(out, anthropicKey) {
		t.Fatalf("secret key reached upstream: %s", out)
	}
	raw := `{"messages":[{"role":"user","content":"x","` + anthropicKey + `":1}]}`
	if out := upstream(t, "r2-3b", raw); strings.Contains(out, anthropicKey) {
		t.Fatalf("secret key reached upstream: %s", out)
	}
}

// Review 2, finding 4: only a bare variable reference or a placeholder this
// plugin issued is left as is.
func TestEnvValuesAreExemptOnlyAsReferencesOrPlaceholders(t *testing.T) {
	body := userBody("PASSWORD='$literal_password'\nTOKEN=SECRET_realcredential\nAPI_KEY==startsWithEquals\nOTHER_TOKEN=$OTHER\nNEXT_TOKEN=${NEXT}")
	out := decodedContent(t, upstream(t, "r2-4", body))
	for _, leaked := range []string{"literal_password", "SECRET_realcredential", "=startsWithEquals"} {
		if strings.Contains(out, leaked) {
			t.Fatalf("%q reached upstream: %s", leaked, out)
		}
	}
	for _, kept := range []string{"OTHER_TOKEN=$OTHER", "NEXT_TOKEN=${NEXT}"} {
		if !strings.Contains(out, kept) {
			t.Fatalf("variable reference %q scrubbed: %s", kept, out)
		}
	}
	placeholder := placeholderOf(t, "r2-4b", anthropicKey)
	if out := decodedContent(t, upstream(t, "r2-4c", userBody("TOKEN="+placeholder))); out != "TOKEN="+placeholder {
		t.Fatalf("placeholder scrubbed again: %s", out)
	}
}

// Review 2, finding 5: a quoted value runs past backslash-escaped quotes.
func TestQuotedEnvValueWithEscapedQuoteIsScrubbedWhole(t *testing.T) {
	out := decodedContent(t, upstream(t, "r2-5", userBody(`PASSWORD="hello\"world" and SECRET='it\'s'`)))
	if strings.Contains(out, "hello") || strings.Contains(out, "world") || strings.Contains(out, "it\\'s") {
		t.Fatalf("part of a quoted value reached upstream: %s", out)
	}
}

// Review 2, finding 6: a high-entropy token needs no digit.
func TestHighEntropyTokenWithoutDigitsIsScrubbed(t *testing.T) {
	token := "QwErTyUiOpAsDfGhJkLzXcVbNm"
	if out := decodedContent(t, upstream(t, "r2-6", userBody("key "+token))); strings.Contains(out, token) {
		t.Fatalf("token reached upstream: %s", out)
	}
}

// Review 2, finding 7: snapshot events carry the whole text and are
// restored whole, never held back.
func TestSnapshotEventsAreRestoredWhole(t *testing.T) {
	for i, event := range []string{
		`{"type":"response.output_text.done","item_id":"m1","output_index":0,"content_index":0,"text":"a <"}`,
		`{"type":"response.completed","response":{"output":[{"content":[{"type":"output_text","text":"a <"}]}]}}`,
		`{"type":"response.completed","response":{"output":[{"content":[{"type":"output_text","text":"k PLACEHOLDER"}]}]}}`,
	} {
		// One request per event: a terminal event ends its stream.
		requestID := "r2-7-" + strconv.Itoa(i)
		placeholder := placeholderOf(t, requestID, anthropicKey)
		event = strings.ReplaceAll(event, "PLACEHOLDER", placeholder)
		chunk := "event: x\ndata: " + event + "\n\n"
		got := deliver(t, requestID, 0, chunk)
		if strings.Count(got, "data: ") != 1 {
			t.Fatalf("one event became several: %q", got)
		}
		want := strings.ReplaceAll(event, placeholder, anthropicKey)
		if line := strings.TrimPrefix(strings.Split(got, "\n")[1], "data: "); line != want {
			t.Fatalf("event = %s, want %s", line, want)
		}
	}
}

// Review 2, finding 8: a placeholder written with < and > inside
// tool-call arguments is restored.
func TestEscapedPlaceholderInArgumentsIsRestored(t *testing.T) {
	placeholder := placeholderOf(t, "r2-8", anthropicKey)
	escapedForm := "\\u003c" + strings.TrimSuffix(strings.TrimPrefix(placeholder, "<"), ">") + "\\u003e"
	args, _ := json.Marshal(`{"k":"` + escapedForm + `"}`)
	out := reply(t, "r2-8", `{"choices":[{"message":{"tool_calls":[{"function":{"arguments":`+string(args)+`}}]}}]}`)
	var parsed struct{ K string }
	if errUnmarshal := json.Unmarshal([]byte(gjson.Get(out, "choices.0.message.tool_calls.0.function.arguments").String()), &parsed); errUnmarshal != nil || parsed.K != anthropicKey {
		t.Fatalf("arguments not restored: %s (%v)", out, errUnmarshal)
	}
}

// Review 2, finding 9: base64 padding is part of the token.
func TestPaddingIsPartOfTheToken(t *testing.T) {
	token := "Zx9Qw3Er7Ty1Ui5Op2As8Df4Gh6=="
	prose := placeholderPattern.FindString(decodedContent(t, upstream(t, "r2-9", userBody("use "+token+" now"))))
	env := placeholderPattern.FindString(decodedContent(t, upstream(t, "r2-9", userBody("API_KEY="+token))))
	if prose == "" || prose != env {
		t.Fatalf("two placeholders for one token: %q %q", prose, env)
	}
	if out := reply(t, "r2-9", `{"choices":[{"message":{"content":"`+prose+`"}}]}`); gjson.Get(out, "choices.0.message.content").String() != token {
		t.Fatalf("restored %s", out)
	}
}

// Scope: a switched request in a format other than OpenAI chat completions
// or Responses passes through untouched, and its reply is not restored.
func TestOtherFormatsPassThroughUntouched(t *testing.T) {
	for _, format := range []string{"claude", "gemini", "openai-image"} {
		resp := call[pluginapi.RequestInterceptResponse](t, pluginabi.MethodRequestInterceptBefore,
			pluginapi.RequestInterceptRequest{RequestID: "fmt-" + format, SourceFormat: format,
				Headers: http.Header{"X-Js-Scrub": {"on"}}, Body: []byte(userBody(anthropicKey))})
		if len(resp.Body) != 0 || len(resp.ClearHeaders) != 0 {
			t.Fatalf("%s request changed: %+v", format, resp)
		}
		if scrub.Active("fmt-" + format) {
			t.Fatalf("%s request is being restored", format)
		}
	}
}

// Responses requests are scrubbed: input items, instructions, function
// call arguments and outputs.
func TestResponsesRequestIsScrubbed(t *testing.T) {
	args, _ := json.Marshal(`{"key":"` + anthropicKey + `"}`)
	body := `{"instructions":"` + anthropicKey + `","input":[{"role":"user","content":[{"type":"input_text","text":"` + anthropicKey + `"}]},` +
		`{"type":"function_call","call_id":"c1","name":"f","arguments":` + string(args) + `},` +
		`{"type":"function_call_output","call_id":"c1","output":"` + anthropicKey + `"}]}`
	resp := call[pluginapi.RequestInterceptResponse](t, pluginabi.MethodRequestInterceptBefore,
		pluginapi.RequestInterceptRequest{RequestID: "resp-req", SourceFormat: formatResponses,
			Headers: http.Header{"X-Js-Scrub": {"on"}}, Body: []byte(body)})
	if len(resp.Body) == 0 || strings.Contains(string(resp.Body), anthropicKey) || !json.Valid(resp.Body) {
		t.Fatalf("responses request = %s", resp.Body)
	}
}

// Review 3, finding 1: only protocol reasoning is exempt. Reasoning-shaped
// data elsewhere is scrubbed, and a reply's reasoning_content (display
// text, not signed) is restored.
func TestOnlyProtocolReasoningIsExempt(t *testing.T) {
	body := `{"messages":[{"role":"user","content":"x","reasoning_blocks":[{"type":"thinking","thinking":"` + anthropicKey + `"}]},` +
		`{"role":"tool","tool_call_id":"t","content":[{"type":"text","text":"{\"type\":\"thinking\",\"thinking\":\"` + anthropicKey + `\"}"}]}],` +
		`"metadata":{"type":"reasoning","encrypted_content":"` + anthropicKey + `"}}`
	out := upstream(t, "r3-1", body)
	if strings.Contains(out, anthropicKey) {
		t.Fatalf("secret reached upstream: %s", out)
	}
	placeholder := placeholderOf(t, "r3-1b", anthropicKey)
	args, _ := json.Marshal(`{"reasoning_content":"` + placeholder + `"}`)
	got := reply(t, "r3-1b", `{"choices":[{"message":{"reasoning_content":"`+placeholder+`","tool_calls":[{"function":{"arguments":`+string(args)+`}}]}}]}`)
	if strings.Contains(got, "SECRET_") {
		t.Fatalf("reply not restored: %s", got)
	}
}

// Review 3, finding 2: .env names in any case around the marker word, and
// quoted values over several lines.
func TestEnvNamesAnyCaseAndMultilineValues(t *testing.T) {
	body := userBody("service_TOKEN=shortCredential1\ndb_PASSWORD=shortCredential2\nPASSWORD=\"line one\nline two\"\nAPP_KEY='a\nb'")
	out := decodedContent(t, upstream(t, "r3-2", body))
	for _, leaked := range []string{"shortCredential1", "shortCredential2", "line one", "line two", "a\nb"} {
		if strings.Contains(out, leaked) {
			t.Fatalf("%q reached upstream: %s", leaked, out)
		}
	}
}

// Review 3, finding 3: Responses reasoning encrypted_content is neither
// scrubbed nor restored.
func TestResponsesEncryptedReasoningIsLeftAlone(t *testing.T) {
	encrypted := "gAAAAABoQwErTyUiOpAsDfGhJkLzXcVbNm1234567890abcdefgh=="
	body := `{"input":[{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"` + encrypted + `"},{"role":"user","content":"` + anthropicKey + `"}]}`
	resp := call[pluginapi.RequestInterceptResponse](t, pluginabi.MethodRequestInterceptBefore,
		pluginapi.RequestInterceptRequest{RequestID: "r3-3", SourceFormat: formatResponses,
			Headers: http.Header{"X-Js-Scrub": {"on"}}, Body: []byte(body)})
	if !strings.Contains(string(resp.Body), encrypted) || strings.Contains(string(resp.Body), anthropicKey) {
		t.Fatalf("request = %s", resp.Body)
	}
	placeholder := placeholderPattern.FindString(string(resp.Body))
	item := `{"type":"reasoning","id":"rs_1","encrypted_content":"x` + placeholder + `"}`
	got := reply(t, "r3-3", `{"output":[`+item+`]}`)
	if gjson.Get(got, "output.0.encrypted_content").String() != "x"+placeholder {
		t.Fatalf("encrypted content restored: %s", got)
	}
	chunk := "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":" + item + "}\n\n"
	if got := deliver(t, "r3-3", 0, chunk); !strings.Contains(got, "x"+placeholder) {
		t.Fatalf("streamed encrypted content restored: %s", got)
	}
}

// argumentsAt parses tool-call arguments and returns the JSON document held
// in their content field.
func passwordInJSONFile(t *testing.T, arguments string) string {
	t.Helper()
	var outer struct{ Content string }
	if errUnmarshal := json.Unmarshal([]byte(arguments), &outer); errUnmarshal != nil {
		t.Fatalf("arguments %q: %v", arguments, errUnmarshal)
	}
	var inner struct{ Password string }
	if errUnmarshal := json.Unmarshal([]byte(outer.Content), &inner); errUnmarshal != nil {
		t.Fatalf("file %q: %v", outer.Content, errUnmarshal)
	}
	return inner.Password
}

// Review 3, finding 4: a value restored into a JSON file held in tool-call
// arguments is escaped for every level it sits in.
func TestValueInJSONFileInArgumentsStaysValid(t *testing.T) {
	password := `hello"world\n`
	placeholder := placeholderOf(t, "r3-4", "PASSWORD='"+password+"'")
	if placeholder == "" {
		t.Fatal("no placeholder")
	}
	file, _ := json.Marshal(map[string]string{"password": placeholder})
	args, _ := json.Marshal(map[string]string{"path": "x.json", "content": string(file)})
	argsString, _ := json.Marshal(string(args))
	out := reply(t, "r3-4", `{"choices":[{"message":{"tool_calls":[{"function":{"arguments":`+string(argsString)+`}}]}}]}`)
	if got := passwordInJSONFile(t, gjson.Get(out, "choices.0.message.tool_calls.0.function.arguments").String()); got != password {
		t.Fatalf("password = %q", got)
	}
	cut := strings.Index(string(args), "SECRET_")
	_, streamed := chatStream(t, "r3-4", []string{toolChunk(0, string(args)[:cut]), toolChunk(0, string(args)[cut:])})
	if got := passwordInJSONFile(t, streamed[0]); got != password {
		t.Fatalf("streamed password = %q", got)
	}
}

// responsesEvents delivers SSE chunks through the host's framing and
// returns the data of each event the client reads, in order.
func responsesEvents(t *testing.T, requestID string, chunks []string) []gjson.Result {
	t.Helper()
	var client hostClient
	for i, chunk := range chunks {
		client.write(deliver(t, requestID, i, chunk))
	}
	return sseFrames(t, client.end())
}

// Review 3, finding 9: an inserted tail event takes the next sequence
// Review 3, findings 9 and 10: a delta held because it ended in what may
// be the start of a placeholder goes on unchanged when none formed: same
// frame, same fields, same sequence number.
func TestHeldDeltaWithoutPlaceholderGoesOnUnchanged(t *testing.T) {
	placeholderOf(t, "r3-9", anthropicKey)
	delta := "event: response.output_text.delta\nid: 1\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"a <\"}\n\n"
	done := "event: response.output_text.done\nid: 2\ndata: {\"type\":\"response.output_text.done\",\"sequence_number\":5,\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"text\":\"a <\"}\n\n"
	if got := deliver(t, "r3-9", 0, delta); got != "" {
		t.Fatalf("held delta sent early: %q", got)
	}
	if got := deliver(t, "r3-9", 1, done); got != delta+done {
		t.Fatalf("frames = %q", got)
	}
}

// Review 3, finding 11: a placeholder in arguments written with one bracket
// literal and one escaped, split inside the escape, is restored.
func TestMixedPlaceholderSplitAcrossArgumentChunksIsRestored(t *testing.T) {
	placeholder := placeholderOf(t, "r3-11", anthropicKey)
	mixed := strings.TrimSuffix(placeholder, ">") + "\\u003e"
	cut := len(mixed) - 3
	_, args := chatStream(t, "r3-11", []string{toolChunk(0, `{"k":"`+mixed[:cut]), toolChunk(0, mixed[cut:]+`"}`)})
	if args[0] != `{"k":"`+anthropicKey+`"}` {
		t.Fatalf("arguments = %q", args[0])
	}
}

// Review 4, finding 1: an unquoted .env value runs to the end of the line;
// # starts a comment only after whitespace.
func TestBareEnvValueFollowsDotenv(t *testing.T) {
	out := decodedContent(t, upstream(t, "r4-1", userBody("PASSWORD=#s3cretAAA\nDB_PASSWORD=abc#s3cretBBB\nAPI_TOKEN=valCCC # a comment")))
	for _, leaked := range []string{"s3cretAAA", "s3cretBBB", "valCCC"} {
		if strings.Contains(out, leaked) {
			t.Fatalf("%q reached upstream: %s", leaked, out)
		}
	}
	if !strings.Contains(out, "# a comment") {
		t.Fatalf("comment scrubbed: %s", out)
	}
}

// Review 4, finding 2: a run counts padding toward its length and may hold
// punctuation.
func TestEntropyRunsCountPaddingAndPunctuation(t *testing.T) {
	for _, token := range []string{"ABEiM0RVZneImaq7zN3u/w==", "aB3!cD4@eF5#gH6$iJ7%kL8^mN9&oP0*"} {
		if out := decodedContent(t, upstream(t, "r4-2", userBody("token "+token+" here"))); strings.Contains(out, token) {
			t.Fatalf("%q reached upstream: %s", token, out)
		}
	}
}

// Review 4, finding 3: JSON documents held in strings are opened at any
// depth, and every level stays valid.
func TestNestedJSONAtAnyDepthIsScrubbed(t *testing.T) {
	doc3, _ := json.Marshal(map[string]string{"note": `TOKEN="hello world"`})
	doc2, _ := json.Marshal(map[string]string{"file": string(doc3)})
	doc1, _ := json.Marshal(map[string]string{"args": string(doc2)})
	out := decodedContent(t, upstream(t, "r4-3", userBody(string(doc1))))
	var level1 struct{ Args string }
	var level2 struct{ File string }
	var level3 struct{ Note string }
	if json.Unmarshal([]byte(out), &level1) != nil || json.Unmarshal([]byte(level1.Args), &level2) != nil ||
		json.Unmarshal([]byte(level2.File), &level3) != nil {
		t.Fatalf("nested document broken: %s", out)
	}
	if strings.Contains(level3.Note, "hello world") || !strings.HasPrefix(level3.Note, `TOKEN="<SECRET_`) {
		t.Fatalf("innermost note = %q", level3.Note)
	}
}

// Review 4, finding 4: overlapping detections are scrubbed as their union.
func TestOverlappingDetectionsAreScrubbedWhole(t *testing.T) {
	content := "github_pat_" + strings.Repeat("a", 20) + "sk-" + strings.Repeat("b", 30)
	if out := decodedContent(t, upstream(t, "r4-4", userBody(content))); strings.Contains(out, "bbbbb") || strings.Contains(out, "aaaaa") {
		t.Fatalf("part of a key reached upstream: %s", out)
	}
}

// Review 4, finding 5: a run that is a path is left whole, high-entropy
// parts included.
func TestPathRunIsLeftWhole(t *testing.T) {
	path := "open /tmp/cache.QwErTyUiOpAsDfGhJkLzXcVbNm.json now"
	if out := decodedContent(t, upstream(t, "r4-5", userBody(path))); out != path {
		t.Fatalf("path scrubbed: %s", out)
	}
}

// Review 4, finding 6: legacy function_call arguments are restored as JSON,
// whole and streamed.
func TestLegacyFunctionCallArgumentsStayJSON(t *testing.T) {
	secret := `a"b`
	placeholder := placeholderOf(t, "r4-6", "PASSWORD='"+secret+"'")
	args, _ := json.Marshal(map[string]string{"password": placeholder})
	argsString, _ := json.Marshal(string(args))
	out := reply(t, "r4-6", `{"choices":[{"message":{"function_call":{"name":"f","arguments":`+string(argsString)+`}}}]}`)
	var parsed struct{ Password string }
	if errUnmarshal := json.Unmarshal([]byte(gjson.Get(out, "choices.0.message.function_call.arguments").String()), &parsed); errUnmarshal != nil || parsed.Password != secret {
		t.Fatalf("arguments = %s (%v)", out, errUnmarshal)
	}
	half := len(args) / 2
	var streamed strings.Builder
	for i, piece := range []string{string(args)[:half], string(args)[half:]} {
		raw, _ := json.Marshal(piece)
		got := deliver(t, "r4-6", i, `{"choices":[{"index":0,"delta":{"function_call":{"arguments":`+string(raw)+`}},"finish_reason":null}]}`)
		streamed.WriteString(gjson.Get(got, "choices.0.delta.function_call.arguments").String())
	}
	got := deliver(t, "r4-6", 2, `{"choices":[{"index":0,"delta":{},"finish_reason":"function_call"}]}`)
	streamed.WriteString(gjson.Get(got, "choices.0.delta.function_call.arguments").String())
	parsed.Password = ""
	if errUnmarshal := json.Unmarshal([]byte(streamed.String()), &parsed); errUnmarshal != nil || parsed.Password != secret {
		t.Fatalf("streamed arguments = %q (%v)", streamed.String(), errUnmarshal)
	}
}

// Review 4, finding 7: arguments past the buffer bound go to the client as
// they came, placeholders left, never half restored.
func TestOversizedArgumentsFailClosed(t *testing.T) {
	defer func(old int) { maxArgumentsBytes = old }(maxArgumentsBytes)
	maxArgumentsBytes = 64
	placeholder := placeholderOf(t, "r4-7", "PASSWORD='q\"uote'")
	args := `{"content":"` + strings.Repeat("x", 80) + `","password":"` + placeholder + `"}`
	_, streamed := chatStream(t, "r4-7", []string{toolChunk(0, args[:40]), toolChunk(0, args[40:])})
	if streamed[0] != args {
		t.Fatalf("arguments = %q", streamed[0])
	}
}

// Review 4, finding 8: a file whose JSON root is a string is restored as a
// valid JSON string.
func TestJSONStringDocumentIsRestoredValid(t *testing.T) {
	secret := "line \"one\"\nline two"
	placeholder := placeholderOf(t, "r4-8", "PASSWORD='"+strings.ReplaceAll(secret, "\n", "\\n")+"'")
	stored := strings.ReplaceAll(secret, "\n", "\\n")
	file, _ := json.Marshal(placeholder)
	args, _ := json.Marshal(map[string]string{"path": "x.json", "content": string(file)})
	argsString, _ := json.Marshal(string(args))
	out := reply(t, "r4-8", `{"choices":[{"message":{"tool_calls":[{"function":{"arguments":`+string(argsString)+`}}]}}]}`)
	var outer struct{ Content string }
	var inner string
	if json.Unmarshal([]byte(gjson.Get(out, "choices.0.message.tool_calls.0.function.arguments").String()), &outer) != nil ||
		json.Unmarshal([]byte(outer.Content), &inner) != nil || inner != stored {
		t.Fatalf("file = %q, want %q", outer.Content, stored)
	}
}

// Review 4, finding 9: shutdown stops the sweeper and forgets every secret.
func TestShutdownForgetsEverything(t *testing.T) {
	reconfigure(t, "")
	upstream(t, "r4-9", userBody(anthropicKey))
	scrub.mu.Lock()
	running := scrub.stop != nil
	scrub.mu.Unlock()
	if !running {
		t.Fatal("no sweeper after register")
	}
	shutdownPlugin()
	scrub.mu.Lock()
	defer scrub.mu.Unlock()
	if scrub.stop != nil || len(scrub.values) != 0 || len(scrub.requests) != 0 {
		t.Fatalf("after shutdown: sweeper %v, %d values, %d requests", scrub.stop != nil, len(scrub.values), len(scrub.requests))
	}
}

// Review 4, finding 10: idle eviction uses the configured TTL, counted
// from the request's last use.
func TestIdleEvictionUsesTheConfiguredTTL(t *testing.T) {
	s := newScrubber()
	clock := time.Unix(0, 0)
	s.now = func() time.Time { return clock }
	s.ttl = 72 * time.Hour
	placeholder := placeholderPattern.FindString(decodedContent(t, string(s.Scrub("long", []byte(userBody(anthropicKey))))))
	clock = clock.Add(25 * time.Hour)
	s.Sweep()
	if out := string(s.Restore("long", []byte(`{"content":"`+placeholder+`"}`))); !strings.Contains(out, anthropicKey) {
		t.Fatalf("request evicted inside the TTL: %s", out)
	}
	clock = clock.Add(73 * time.Hour)
	s.Sweep()
	if s.Active("long") {
		t.Fatal("idle request kept past the TTL")
	}
}

// Review 5, finding 1: only a bare reference is exempt; a reference with a
// literal fallback is scrubbed.
func TestEnvReferenceWithFallbackIsScrubbed(t *testing.T) {
	out := decodedContent(t, upstream(t, "r5-1", userBody("PASSWORD=${MISSING:-hunter2xyz}\nTOKEN=${PLAIN}")))
	if strings.Contains(out, "hunter2xyz") || !strings.Contains(out, "TOKEN=${PLAIN}") {
		t.Fatalf("upstream = %s", out)
	}
}

// Review 5, finding 2: a scrubbed request over the Responses WebSocket is
// rejected, never sent where later turns bypass the scrubber.
func TestScrubbedWebSocketRequestIsRejected(t *testing.T) {
	headers := http.Header{"X-Js-Scrub": {"on"}, "Connection": {"Upgrade"}, "Upgrade": {"websocket"}}
	resp := call[pluginapi.RequestInterceptResponse](t, pluginabi.MethodRequestInterceptBefore,
		pluginapi.RequestInterceptRequest{RequestID: "r5-2", SourceFormat: formatResponses, Headers: headers,
			Body: []byte(`{"input":[{"role":"user","content":"` + anthropicKey + `"}]}`)})
	if !resp.Terminate || resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(resp.ResponseBody), "secret_scrub_websocket") {
		t.Fatalf("websocket request not rejected: %+v", resp)
	}
	headers.Del("X-Js-Scrub")
	plain := call[pluginapi.RequestInterceptResponse](t, pluginabi.MethodRequestInterceptBefore,
		pluginapi.RequestInterceptRequest{RequestID: "r5-2b", SourceFormat: formatResponses, Headers: headers, Body: []byte(`{}`)})
	if plain.Terminate {
		t.Fatal("unswitched websocket request rejected")
	}
}

// Review 5, finding 3: entropy and length are counted in runes.
func TestEntropyCountsRunes(t *testing.T) {
	token := "αβγδεζηθικλμνξοπρστυφχψω"
	if out := decodedContent(t, upstream(t, "r5-3", userBody("key "+token))); strings.Contains(out, token) {
		t.Fatalf("token reached upstream: %s", out)
	}
}

// Review 5, finding 4: a placeholder written with JSON escapes is found
// after decoding, in reply text, in arguments and in documents inside them.
func TestEscapedPlaceholderIsRestoredEverywhere(t *testing.T) {
	placeholder := placeholderOf(t, "r5-4", anthropicKey)
	escaped := `<\u0053ECRET_` + placeholder[len("<SECRET_"):]
	out := reply(t, "r5-4", `{"choices":[{"message":{"content":"`+escaped+`"}}]}`)
	if gjson.Get(out, "choices.0.message.content").String() != anthropicKey {
		t.Fatalf("content = %s", out)
	}
	args := `{\"k\":\"` + strings.ReplaceAll(escaped, `\`, `\\`) + `\"}`
	out = reply(t, "r5-4", `{"choices":[{"message":{"tool_calls":[{"function":{"arguments":"`+args+`"}}]}}]}`)
	if got := gjson.Get(gjson.Get(out, "choices.0.message.tool_calls.0.function.arguments").String(), "k").String(); got != anthropicKey {
		t.Fatalf("arguments = %s", out)
	}
	doc, _ := json.Marshal(map[string]string{"content": `{"k":"` + escaped + `"}`})
	docString, _ := json.Marshal(string(doc))
	out = reply(t, "r5-4", `{"choices":[{"message":{"tool_calls":[{"function":{"arguments":`+string(docString)+`}}]}}]}`)
	held := gjson.Get(gjson.Get(out, "choices.0.message.tool_calls.0.function.arguments").String(), "content").String()
	if gjson.Get(held, "k").String() != anthropicKey {
		t.Fatalf("nested document = %s", held)
	}
}

// Review 5, finding 5: a Scrub still running when the request's
// completion event comes keeps no state for it.
func TestScrubAfterEndKeepsNoState(t *testing.T) {
	s := newScrubber()
	s.afterScan = func(requestID string) { s.End(requestID) }
	if out := string(s.Scrub("late", []byte(userBody(anthropicKey)))); strings.Contains(out, anthropicKey) {
		t.Fatalf("late request not scrubbed: %s", out)
	}
	if s.Active("late") || len(s.requests) != 0 || len(s.scrubbing) != 0 {
		t.Fatal("ended request got state again")
	}
}

// Review 5, finding 7: arguments over the bound are left whole, and a
// Responses call whose stream was sent unrestored keeps its placeholders in
// every snapshot.
func TestArgumentsBoundIsOnePolicyPerCall(t *testing.T) {
	defer func(old int) { maxArgumentsBytes = old }(maxArgumentsBytes)
	maxArgumentsBytes = 64
	placeholder := placeholderOf(t, "r5-7", anthropicKey)
	args := `{"pad":"` + strings.Repeat("x", 80) + `","k":"` + placeholder + `"}`
	argsString, _ := json.Marshal(args)
	out := reply(t, "r5-7", `{"choices":[{"message":{"tool_calls":[{"function":{"arguments":`+string(argsString)+`}}]}}]}`)
	if gjson.Get(out, "choices.0.message.tool_calls.0.function.arguments").String() != args {
		t.Fatalf("oversized arguments restored: %s", out)
	}
	frame := func(event string) string {
		return "event: " + gjson.Get(event, "type").String() + "\ndata: " + event + "\n\n"
	}
	deltaText, _ := json.Marshal(args)
	item := `{"type":"function_call","id":"fc_1","call_id":"c1","name":"f","arguments":` + string(argsString) + `}`
	events := responsesEvents(t, "r5-7", []string{
		frame(`{"type":"response.function_call_arguments.delta","sequence_number":1,"item_id":"fc_1","output_index":0,"delta":` + string(deltaText) + `}`),
		frame(`{"type":"response.function_call_arguments.done","sequence_number":2,"item_id":"fc_1","output_index":0,"arguments":` + string(argsString) + `}`),
		frame(`{"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":` + item + `}`),
		frame(`{"type":"response.completed","sequence_number":4,"response":{"output":[` + item + `]}}`),
	})
	for _, e := range events {
		if strings.Contains(e.Raw, anthropicKey) {
			t.Fatalf("a snapshot of an unrestored call was restored: %s", e.Raw)
		}
	}
}

// sseFrames splits delivered SSE text into frames and checks that each
// frame with an event: line is named after the type of its data.
func sseFrames(t *testing.T, text string) []gjson.Result {
	t.Helper()
	var events []gjson.Result
	for _, frame := range strings.Split(strings.TrimSpace(text), "\n\n") {
		var name string
		var data gjson.Result
		fields := 0
		for _, line := range strings.Split(frame, "\n") {
			if !strings.HasPrefix(line, ":") && strings.TrimSpace(line) != "" {
				fields++
			}
			switch {
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				data = gjson.Parse(strings.TrimPrefix(line, "data: "))
			}
		}
		if fields == 0 {
			continue // a comment frame
		}
		if name != "" && name != data.Get("type").String() {
			t.Fatalf("frame %q is named %q but carries %q", frame, name, data.Get("type").String())
		}
		events = append(events, data)
	}
	return events
}

// Review 6, finding 1: SSE lines that arrive in separate chunks are
// regrouped into frames, so a flushed arguments delta is a whole frame of
// its own, before the frame that ends the call.
func TestFlushedDeltaKeepsFramesAcrossChunks(t *testing.T) {
	stream := func(requestID string) string {
		secret := `pa"ss`
		placeholder := placeholderOf(t, requestID, "PASSWORD='"+secret+"'")
		args, _ := json.Marshal(map[string]string{"password": placeholder})
		half := len(args) / 2
		delta := func(n int, piece string) string {
			raw, _ := json.Marshal(piece)
			return "event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"sequence_number\":" +
				string(rune('0'+n)) + ",\"item_id\":\"fc_1\",\"output_index\":0,\"delta\":" + string(raw) + "}\n\n"
		}
		argsString, _ := json.Marshal(string(args))
		return delta(1, string(args)[:half]) + delta(2, string(args)[half:]) +
			"event: response.function_call_arguments.done\ndata: {\"type\":\"response.function_call_arguments.done\",\"sequence_number\":3,\"item_id\":\"fc_1\",\"output_index\":0,\"arguments\":" + string(argsString) + "}\n\n"
	}
	check := func(delivered string) {
		t.Helper()
		var args strings.Builder
		for _, e := range sseFrames(t, delivered) {
			if e.Get("type").String() == "response.function_call_arguments.delta" {
				args.WriteString(e.Get("delta").String())
			}
		}
		if got := gjson.Get(args.String(), "password").String(); got != `pa"ss` || !json.Valid([]byte(args.String())) {
			t.Fatalf("arguments %q", args.String())
		}
	}

	// event: and data: lines in separate chunks, as native streams send them.
	text := stream("r6-1a")
	var pieces []string
	for _, frame := range strings.SplitAfter(text, "\n\n") {
		if frame == "" {
			continue
		}
		cut := strings.Index(frame, "\n") + 1
		pieces = append(pieces, frame[:cut], frame[cut:])
	}
	var client hostClient
	for i, piece := range pieces {
		client.write(deliver(t, "r6-1a", i, piece))
	}
	check(client.end())

	// Split in two at every byte boundary the host itself carries intact
	// (its framing of the two chunks, without the plugin, is that of the
	// whole text); at the others the host changes the stream with or
	// without the plugin.
	text = stream("r6-1b")
	whole := hostFrames(text)
	for k := 1; k < len(text); k++ {
		if hostFrames(text[:k], text[k:]) != whole {
			continue
		}
		requestID := "r6-1b-" + strconv.Itoa(k)
		split := strings.Replace(stream(requestID), "", "", 0)
		client = hostClient{}
		client.write(deliver(t, requestID, 0, split[:k]))
		client.write(deliver(t, requestID, 1, split[k:]))
		check(client.end())
	}
}

// Review 6, finding 2: when the request asks for JSON output, the message
// text is restored as JSON, whole and streamed, in chat and Responses; in
// plain text a placeholder written with escapes is restored JSON escaped.
func TestStructuredOutputIsRestoredAsJSON(t *testing.T) {
	secret := `a"b`
	request := func(requestID, format string) string {
		body := `{"messages":[{"role":"user","content":"PASSWORD='a\"b'"}],` + format + `}`
		return placeholderPattern.FindString(upstream(t, requestID, body))
	}
	password := func(text string) string {
		t.Helper()
		if !json.Valid([]byte(text)) {
			t.Fatalf("not JSON: %q", text)
		}
		return gjson.Get(text, "password").String()
	}
	doc := func(placeholder string) string {
		raw, _ := json.Marshal(map[string]string{"password": placeholder})
		return string(raw)
	}

	placeholder := request("r6-2a", `"response_format":{"type":"json_object"}`)
	content, _ := json.Marshal(doc(placeholder))
	out := reply(t, "r6-2a", `{"choices":[{"message":{"content":`+string(content)+`}}]}`)
	if got := password(gjson.Get(out, "choices.0.message.content").String()); got != secret {
		t.Fatalf("chat content password %q", got)
	}

	placeholder = request("r6-2b", `"response_format":{"type":"json_schema","json_schema":{"name":"x","schema":{}}}`)
	text := doc(placeholder)
	streamed, _ := chatStream(t, "r6-2b", []string{contentChunk(text[:12], false), contentChunk(text[12:], false)})
	if got := password(streamed); got != secret {
		t.Fatalf("streamed chat content password %q", got)
	}

	placeholder = request("r6-2c", `"text":{"format":{"type":"json_schema","name":"x","schema":{}}}`)
	text = doc(placeholder)
	half, _ := json.Marshal(text[:12])
	rest, _ := json.Marshal(text[12:])
	whole, _ := json.Marshal(text)
	frame := func(event string) string {
		return "event: " + gjson.Get(event, "type").String() + "\ndata: " + event + "\n\n"
	}
	ids := `"item_id":"m1","output_index":0,"content_index":0`
	events := responsesEvents(t, "r6-2c", []string{
		frame(`{"type":"response.output_text.delta",` + ids + `,"delta":` + string(half) + `}`),
		frame(`{"type":"response.output_text.delta",` + ids + `,"delta":` + string(rest) + `}`),
		frame(`{"type":"response.output_text.done",` + ids + `,"text":` + string(whole) + `}`),
		frame(`{"type":"response.completed","response":{"output":[{"type":"message","content":[{"type":"output_text","text":` + string(whole) + `}]}]}}`),
	})
	var deltas strings.Builder
	for _, e := range events {
		switch e.Get("type").String() {
		case "response.output_text.delta":
			deltas.WriteString(e.Get("delta").String())
		case "response.output_text.done":
			if got := password(e.Get("text").String()); got != secret {
				t.Fatalf("done text password %q", got)
			}
		case "response.completed":
			if got := password(e.Get("response.output.0.content.0.text").String()); got != secret {
				t.Fatalf("completed text password %q", got)
			}
		}
	}
	if got := password(deltas.String()); got != secret {
		t.Fatalf("streamed text password %q", got)
	}

	placeholder = request("r6-2d", `"stream":false`)
	escaped := `{"password":"\u003c` + strings.TrimSuffix(strings.TrimPrefix(placeholder, "<"), ">") + `\u003e"}`
	content, _ = json.Marshal(escaped)
	out = reply(t, "r6-2d", `{"choices":[{"message":{"content":`+string(content)+`}}]}`)
	if got := password(gjson.Get(out, "choices.0.message.content").String()); got != secret {
		t.Fatalf("plain escaped placeholder password %q", got)
	}
}

// hostClient is what a client receives from delivered chunks: the host's
// Responses framer (as hostFramer ports it) and writer, and at the end of
// the stream the framer's Flush. The tests behind -tags hosttest run the
// real host instead.
type hostClient struct {
	validator hostValidator
	framer    hostFramer
	out       strings.Builder
}

func (c *hostClient) write(chunk string) {
	validated, errValidate := c.validator.add(chunk)
	if errValidate != nil {
		c.out.WriteString("event: error\ndata: {\"type\":\"error\"}\n\n")
		return
	}
	for _, frame := range c.framer.write(validated) {
		c.out.WriteString(frame + sseFrameSuffix(frame))
	}
}

// hostFrames is what the host sends for chunks, with no plugin.
func hostFrames(chunks ...string) string {
	var client hostClient
	for _, chunk := range chunks {
		client.write(chunk)
	}
	return client.end()
}

// end is the stream once the host has flushed what it still holds.
func (c *hostClient) end() string {
	pending := c.framer.pending
	if strings.TrimSpace(pending) != "" && sseHasField(pending, "data:") && sseDataLinesValid(pending) {
		c.out.WriteString(pending + sseFrameSuffix(pending))
	}
	c.framer.pending = ""
	return c.out.String()
}

// codexLineChunks is a Responses stream in the shape the native Codex HTTP
// executor hands the hooks (codex_executor_stream.go): each SSE line its
// own chunk, without its newline ("event: ..." as read, "data: " + the
// payload), and the blank separator lines not sent at all.
func codexLineChunks(events []string) []string {
	var chunks []string
	for _, event := range events {
		chunks = append(chunks, "event: "+gjson.Get(event, "type").String(), "data: "+event)
	}
	return chunks
}

// checkArgumentStream scrubs a request, streams a Responses tool call whose
// arguments hold its placeholder through chunksFor (the shape a host
// executor hands the hooks), delivers each chunk, and checks the client
// stream: every event arrives, each frame is named after its data, and the
// arguments arrive restored.
func checkArgumentStream(t *testing.T, requestID string, chunksFor func(events []string) []string) {
	t.Helper()
	secret := `pa"ss`
	placeholder := placeholderOf(t, requestID, "PASSWORD='"+secret+"'")
	args, _ := json.Marshal(map[string]string{"password": placeholder})
	half := len(args) / 2
	first, _ := json.Marshal(string(args)[:half])
	second, _ := json.Marshal(string(args)[half:])
	whole, _ := json.Marshal(string(args))
	ids := `"item_id":"fc_1","output_index":0`
	events := []string{
		`{"type":"response.created","sequence_number":0,"response":{"id":"resp_1"}}`,
		`{"type":"response.function_call_arguments.delta","sequence_number":1,` + ids + `,"delta":` + string(first) + `}`,
		`{"type":"response.function_call_arguments.delta","sequence_number":2,` + ids + `,"delta":` + string(second) + `}`,
		`{"type":"response.function_call_arguments.done","sequence_number":3,` + ids + `,"arguments":` + string(whole) + `}`,
		`{"type":"response.completed","sequence_number":4,"response":{"id":"resp_1","status":"completed","output":[]}}`,
	}
	var client hostClient
	for i, chunk := range chunksFor(events) {
		client.write(deliver(t, requestID, i, chunk))
	}
	var types []string
	var delivered strings.Builder
	for _, e := range sseFrames(t, client.end()) {
		types = append(types, e.Get("type").String())
		if e.Get("type").String() == "response.function_call_arguments.delta" {
			delivered.WriteString(e.Get("delta").String())
		}
	}
	want := []string{"response.created", "response.function_call_arguments.delta", "response.function_call_arguments.delta",
		"response.function_call_arguments.done", "response.completed"}
	if strings.Join(types, ",") != strings.Join(want, ",") {
		t.Fatalf("events %v", types)
	}
	if gjson.Get(delivered.String(), "password").String() != secret {
		t.Fatalf("arguments %q", delivered.String())
	}
}

// Review 7: the native executor's line-per-chunk stream is delivered in
// full, a data: line completing its event: line's frame, and a flushed
// arguments delta is a frame of its own before the frame that ends the
// call.
func TestCodexLinePerChunkStreamIsDelivered(t *testing.T) {
	checkArgumentStream(t, "r7", codexLineChunks)
}

// Review 8, finding 1: SSE comments, with and without their newline, are
// sent at once and never swallow the events after them.
func TestSSECommentsAreNeverHeld(t *testing.T) {
	withComments := func(shape func([]string) []string) func([]string) []string {
		return func(events []string) []string {
			var chunks []string
			for _, chunk := range shape(events) {
				chunks = append(chunks, chunk)
				if strings.HasPrefix(chunk, "data:") || strings.HasSuffix(chunk, "\n\n") {
					chunks = append(chunks, ": keep-alive")
				}
			}
			return append([]string{": keep-alive"}, chunks...)
		}
	}
	checkArgumentStream(t, "r8-1a", withComments(codexLineChunks))
	checkArgumentStream(t, "r8-1b", withComments(func(events []string) []string {
		var chunks []string
		for _, event := range events {
			chunks = append(chunks, "event: "+gjson.Get(event, "type").String()+"\ndata: "+event+"\n\n")
		}
		return chunks
	}))
}

// Review 8, finding 2: every value is gone a TTL after its last use, also
// for a request that never got a reply or a completion event, and quiesce
// (the host retiring this instance) is accepted.
func TestEveryValueIsGoneATTLAfterItsLastUse(t *testing.T) {
	s := newScrubber()
	clock := time.Unix(0, 0)
	s.now = func() time.Time { return clock }
	s.Scrub("abandoned", []byte(userBody(anthropicKey)))
	clock = clock.Add(s.ttl + sweepInterval(s.ttl))
	s.Sweep()
	if len(s.values) != 0 || len(s.byValue) != 0 || len(s.requests) != 0 {
		t.Fatalf("after the TTL: %d values, %d requests", len(s.values), len(s.requests))
	}
	call[struct{}](t, pluginabi.MethodPluginQuiesce, struct{}{})
}

// Review 8, finding 3: only running scrubs of switched requests are
// tracked; traffic with scrubbing off records nothing, and each tracked
// entry goes at End.
func TestTrackingIsBoundedByScrubsInFlight(t *testing.T) {
	s := newScrubber()
	for i := 0; i < 100000; i++ {
		s.End("off-" + strconv.Itoa(i))
	}
	inFlight := -1
	s.afterScan = func(string) { inFlight = len(s.scrubbing) }
	for i := 0; i < 1000; i++ {
		id := "on-" + strconv.Itoa(i)
		s.Scrub(id, []byte(userBody(anthropicKey)))
		s.End(id)
	}
	if inFlight != 1 || len(s.scrubbing) != 0 || len(s.requests) != 0 {
		t.Fatalf("in flight %d, tracked %d, requests %d", inFlight, len(s.scrubbing), len(s.requests))
	}
}

// Review 8, finding 4: a value keeps its placeholder while remembered, even
// after a value that held an earlier colliding candidate expires.
func TestCollisionIDsStayStableAfterExpiry(t *testing.T) {
	s := newScrubber()
	clock := time.Unix(0, 0)
	s.now = func() time.Time { return clock }
	s.ttl = 24 * time.Hour
	s.id = func(key []byte, value string, attempt int) string {
		if attempt == 0 {
			return "deadbeef"
		}
		return hmacID(key, value, attempt)
	}
	other := "sk-proj-ZyXwVuTsRqPoNmLkJiHgFeDcBa9876543210"
	placeholderFor := func(value string) string {
		return placeholderPattern.FindString(decodedContent(t, string(s.Scrub("c", []byte(userBody(value))))))
	}
	placeholderFor(anthropicKey)
	before := placeholderFor(other)
	clock = clock.Add(15 * time.Hour)
	placeholderFor(other)
	clock = clock.Add(10 * time.Hour)
	s.Sweep()
	if after := placeholderFor(other); after != before {
		t.Fatalf("placeholder changed within the TTL: %s then %s", before, after)
	}
}

// codexLineChunksDataFirst is the native executor's shape for an upstream
// that writes each frame's data: line before its event: line. The executor
// stops reading at the reply's last event, so that event's event: line is
// never sent.
func codexLineChunksDataFirst(events []string) []string {
	var chunks []string
	for _, event := range events {
		chunks = append(chunks, "data: "+event)
		if streamTerminal(gjson.Get(event, "type").String()) {
			break
		}
		chunks = append(chunks, "event: "+gjson.Get(event, "type").String())
	}
	return chunks
}

// Review 9, finding 1: a frame is its fields in any order; a stream that
// puts data before event keeps every event named after its data.
func TestDataBeforeEventKeepsFramesTogether(t *testing.T) {
	checkArgumentStream(t, "r9-1", codexLineChunksDataFirst)
}

// Review 9, finding 2: a remembered value is a copy, not a slice of the
// request it was found in, so it does not keep that buffer alive.
func TestRememberedValuesDoNotShareTheRequestBuffer(t *testing.T) {
	s := newScrubber()
	text := strings.Repeat("x ", 1<<16) + anthropicKey + strings.Repeat(" y", 1<<16)
	found := make(map[string]string)
	s.scrubEdits(text, true, found)
	start := uintptr(unsafe.Pointer(unsafe.StringData(text)))
	inside := func(value string) bool {
		p := uintptr(unsafe.Pointer(unsafe.StringData(value)))
		return p >= start && p < start+uintptr(len(text))
	}
	if len(found) != 1 {
		t.Fatalf("found %d values", len(found))
	}
	for id, value := range found {
		if inside(value) || inside(s.values[id].value) {
			t.Fatal("remembered value shares the request buffer")
		}
	}
	for value := range s.byValue {
		if inside(value) {
			t.Fatal("value index shares the request buffer")
		}
	}
}

// escapedItemID writes the second arguments delta's item_id with a JSON
// escape, the same id as the other events.
func escapedItemID(chunksFor func([]string) []string) func([]string) []string {
	return func(events []string) []string {
		edited := append([]string(nil), events...)
		edited[2] = strings.Replace(edited[2], `"item_id":"fc_1"`, `"item_id":"fc\u005f1"`, 1)
		return chunksFor(edited)
	}
}

// Review 10, finding 1: identity fields are compared by decoded value, so
// one id written two ways is one call.
func TestIdentityIsTheDecodedValue(t *testing.T) {
	checkArgumentStream(t, "r10-1", escapedItemID(codexLineChunks))
}

// Review 8 finding 1, review 10 finding 2, review 13 finding 1: comments
// travel exactly as the host carries them, whole or in fragments, between
// frames or inside one: a comment line ends only at its newline, so a
// fragment's continuation stays part of the comment.
func TestCommentsTravelAsTheHostCarriesThem(t *testing.T) {
	created := `{"type":"response.created","response":{"id":"r"}}`
	cases := [][]string{
		{": keep-alive", "event: response.created", "data: " + created},
		{"event: response.created", ": keep-alive", "data: " + created},
		{": keepalive ", "da", "ta: " + created + "\n\n"},
		{": keep-alive\n\n", "event: response.created\ndata: " + created + "\n\n"},
	}
	text := ": keep-alive\nevent: response.created\n: inside\ndata: " + created + "\n\n: after\n\n"
	for k := 1; k < len(text); k++ {
		cases = append(cases, []string{text[:k], text[k:]})
	}
	for i, chunks := range cases {
		requestID := "r13-1-" + strconv.Itoa(i)
		placeholderOf(t, requestID, anthropicKey)
		var client hostClient
		for j, chunk := range chunks {
			client.write(deliver(t, requestID, j, chunk))
		}
		got, want := dispatch(client.end()), dispatch(hostFrames(chunks...))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("chunks %q: with plugin %+v, host alone %+v", chunks, got, want)
		}
	}
	checkArgumentStream(t, "r10-2b", func(events []string) []string {
		var chunks []string
		for _, chunk := range codexLineChunks(events) {
			chunks = append(chunks, chunk)
			if strings.HasPrefix(chunk, "event:") {
				chunks = append(chunks, ": keep-alive")
			}
		}
		return chunks
	})
}

// Review 12, finding 1: a switched request the scrubber cannot read whole
// (not JSON, or documents in its strings past the size bound) is rejected,
// never forwarded.
func TestUnscrubbableRequestIsRejected(t *testing.T) {
	malformed := `{"messages":[{"role":"user","content":"` + anthropicKey + `"},]}`
	resp := scrubbedRequest(t, "r12-1", malformed, true)
	if !resp.Terminate || resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(resp.ResponseBody), "secret_scrub_unparsable") {
		t.Fatalf("malformed request not rejected: %+v", resp)
	}
	defer func(old int) { maxNestedBytes = old }(maxNestedBytes)
	maxNestedBytes = 16
	nested, _ := json.Marshal(map[string]string{"k": anthropicKey})
	resp = scrubbedRequest(t, "r12-1b", userBody(string(nested)), true)
	if !resp.Terminate || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("request past the size bound not rejected: %+v", resp)
	}
}

// Review 12, finding 3: tool-call arguments split inside an escaped
// placeholder opener (after \, \u or \u0) are held and restored.
func TestArgumentsSplitInsideAnEscapedOpenerAreRestored(t *testing.T) {
	for i, cut := range []int{1, 2, 3} {
		requestID := "r12-3-" + strconv.Itoa(i)
		placeholder := placeholderOf(t, requestID, anthropicKey)
		escaped := "\\u003c" + strings.TrimSuffix(strings.TrimPrefix(placeholder, "<"), ">") + "\\u003e"
		args := `{"k":"` + escaped + `"}`
		at := strings.Index(args, "\\") + cut
		_, streamed := chatStream(t, requestID, []string{toolChunk(0, args[:at]), toolChunk(0, args[at:])})
		var parsed struct{ K string }
		if errUnmarshal := json.Unmarshal([]byte(streamed[0]), &parsed); errUnmarshal != nil || parsed.K != anthropicKey {
			t.Fatalf("split after %q: arguments %q (%v)", args[at-cut:at], streamed[0], errUnmarshal)
		}
	}
}

// dispatchedEvent is one event an SSE client dispatches.
type dispatchedEvent struct {
	Event, Data, ID, Retry string
}

// dispatch reads an SSE body as a client does (the WHATWG event stream
// rules): fields until a blank line, which dispatches when data was set;
// comments ignored; the last event ID carried on.
func dispatch(body string) []dispatchedEvent {
	var events []dispatchedEvent
	var event, retry, lastID string
	var data []string
	hasData := false
	body = strings.ReplaceAll(strings.ReplaceAll(body, "\r\n", "\n"), "\r", "\n")
	for _, line := range strings.Split(body, "\n") {
		if line == "" {
			if hasData {
				name := event
				if name == "" {
					name = "message"
				}
				events = append(events, dispatchedEvent{Event: name, Data: strings.Join(data, "\n"), ID: lastID, Retry: retry})
			}
			event, retry, data, hasData = "", "", nil, false
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			event = value
		case "data":
			data = append(data, value)
			hasData = true
		case "id":
			if !strings.Contains(value, "\x00") {
				lastID = value
			}
		case "retry":
			if _, errAtoi := strconv.Atoi(value); errAtoi == nil {
				retry = value
			}
		}
	}
	return events
}

// nestedArguments is tool-call arguments holding a JSON document whose
// quotes are written as \u0022 escapes, with the placeholder in it, and
// where the placeholder starts.
func nestedArguments(placeholder string) (string, int) {
	quote := `\u0022`
	args := `{"content":"{` + quote + `k` + quote + `:` + quote + placeholder + quote + `}"}`
	return args, strings.Index(args, "<")
}

// Review 13, finding 2: restoring a placeholder inside a JSON document held
// in a string changes only the placeholder's bytes, so the part of the
// arguments already sent (with its \u escapes) stays a prefix and the
// stream is restored.
func TestNestedArgumentsAreRestoredByteForByte(t *testing.T) {
	secret := `pa"ss`
	placeholder := placeholderOf(t, "r13-2", "PASSWORD='"+secret+"'")
	args, cut := nestedArguments(placeholder)
	_, streamed := chatStream(t, "r13-2", []string{toolChunk(0, args[:cut]), toolChunk(0, args[cut:])})
	if !strings.HasPrefix(streamed[0], args[:cut]) {
		t.Fatalf("sent prefix changed: %q", streamed[0])
	}
	var outer struct{ Content string }
	var inner struct{ K string }
	if json.Unmarshal([]byte(streamed[0]), &outer) != nil || json.Unmarshal([]byte(outer.Content), &inner) != nil || inner.K != secret {
		t.Fatalf("arguments %q", streamed[0])
	}
}

// escapeEvery writes every character of text as a \u escape.
func escapeEvery(text string) string {
	var b strings.Builder
	for _, r := range text {
		fmt.Fprintf(&b, `\u%04x`, r)
	}
	return b.String()
}

// Review 14, finding 1: secrets are found in fully decoded values at every
// depth, keys included, whatever their encoding.
func TestEncodedSecretsAreFoundAtEveryDepth(t *testing.T) {
	encoded := escapeEvery(anthropicKey)
	nested := `{"k":"` + encoded + `"}`
	inKey, _ := json.Marshal(map[string]string{nested: "value"})
	inValue, _ := json.Marshal(map[string]string{"doc": nested})
	deeper, _ := json.Marshal(map[string]string{"outer": string(inValue)})
	for name, content := range map[string]string{
		"escaped value":         `{"k":"` + encoded + `"}`,
		"escaped key":           `{"` + encoded + `":1}`,
		"document in a key":     string(inKey),
		"document in a value":   string(inValue),
		"two documents deep":    string(deeper),
		"escaped in plain text": "see " + anthropicKey,
	} {
		out, errScrub := scrub.ScrubChecked("r14-1", []byte(userBody(content)))
		if errScrub != nil {
			t.Fatalf("%s: %v", name, errScrub)
		}
		if strings.Contains(decodeAll(string(out)), anthropicKey) {
			t.Fatalf("%s: secret reached upstream: %s", name, out)
		}
	}
}

// decodeAll decodes JSON strings held in JSON strings, as deep as they go,
// and returns every string and key it finds, joined.
func decodeAll(doc string) string {
	var b strings.Builder
	var walk func(v any)
	walk = func(v any) {
		switch value := v.(type) {
		case string:
			b.WriteString(value + "\n")
			var inner any
			if json.Unmarshal([]byte(value), &inner) == nil {
				walk(inner)
			}
		case map[string]any:
			for k, child := range value {
				walk(k)
				walk(child)
			}
		case []any:
			for _, child := range value {
				walk(child)
			}
		}
	}
	var root any
	_ = json.Unmarshal([]byte(doc), &root)
	walk(root)
	return b.String()
}

// Review 14, finding 4: a placeholder in a JSON document held in an
// arguments key is restored at its depth, so the key stays valid JSON.
func TestPlaceholderInANestedKeyIsRestored(t *testing.T) {
	secret := `pa"ss`
	placeholder := placeholderOf(t, "r14-4", "PASSWORD='"+secret+"'")
	args, _ := json.Marshal(map[string]string{`{"k":"` + placeholder + `"}`: "value"})
	restored := scrub.state("r14-4").restoreArguments(string(args))
	var outer map[string]string
	if errUnmarshal := json.Unmarshal([]byte(restored), &outer); errUnmarshal != nil {
		t.Fatalf("arguments %q: %v", restored, errUnmarshal)
	}
	for key := range outer {
		var inner map[string]string
		if errUnmarshal := json.Unmarshal([]byte(key), &inner); errUnmarshal != nil || inner["k"] != secret {
			t.Fatalf("key %q: %v", key, errUnmarshal)
		}
	}
}

// Review 15, finding 1: a key that starts with ":" (or holds . * # and the
// like) is no path syntax: the placeholder is restored where it is, and
// the field beside it is not touched.
func TestOddKeysAreRestoredInPlace(t *testing.T) {
	placeholder := placeholderOf(t, "r15-1", anthropicKey)
	body := `{"choices":[{"message":{":content":"` + placeholder + `","content":"x","a.b":"` + placeholder + `","#":"` + placeholder + `"}}]}`
	want := strings.ReplaceAll(body, placeholder, anthropicKey)
	if got := reply(t, "r15-1", body); got != want {
		t.Fatalf("reply = %s", got)
	}
}

// Review 15, finding 2: scrubbing changes only the secret's bytes:
// whitespace and escapes elsewhere stay as they came.
func TestScrubKeepsEveryOtherByte(t *testing.T) {
	body := "{ \"messages\" : [ {\"role\":\"user\",  \"content\" : \"caf" + `\u00e9` + " key " + anthropicKey + "\" , \"name\":\"x" + `\/` + "y\" } ] }"
	out := upstream(t, "r15-2", body)
	placeholder := placeholderPattern.FindString(out)
	if placeholder == "" || out != strings.Replace(body, anthropicKey, placeholder, 1) {
		t.Fatalf("scrubbed %q", out)
	}
}

// Review 15, finding 3: placeholders in a reply's keys are restored too.
func TestReplyKeysAreRestored(t *testing.T) {
	placeholder := placeholderOf(t, "r15-3", anthropicKey)
	body := `{"choices":[{"message":{"content":"x","annotations":{"` + placeholder + `":1}}}]}`
	if got := reply(t, "r15-3", body); got != strings.ReplaceAll(body, placeholder, anthropicKey) {
		t.Fatalf("reply = %s", got)
	}
}

// propertySecrets are secrets of known shapes written with characters JSON
// does not escape, so each is the same bytes at every depth.
var propertySecrets = []string{
	anthropicKey,
	"ghp_0123456789abcdefghijABCDEFGHIJ0123456789",
	"AKIAIOSFODNN7EXAMPLE",
}

// propertyDocument builds a random JSON document: odd keys, whitespace,
// numbers and arrays, secrets in keys and values, and JSON documents held
// in strings (escaped once per level), at every depth.
func propertyDocument(r *rand.Rand, depth int) string {
	space := func() string { return []string{"", " ", "\n  ", "\t"}[r.Intn(4)] }
	keys := []string{":k", "a.b", "*", "#", "k", "x y", "@", "|", "?", `\u0061b`, ":0", "0"}
	words := []string{"alpha", "beta gamma", "hello world", "x", ""}
	var str func(int) string
	str = func(d int) string {
		switch r.Intn(5) {
		case 0:
			return propertySecrets[r.Intn(len(propertySecrets))]
		case 1:
			return "key " + propertySecrets[r.Intn(len(propertySecrets))] + " ok"
		case 2:
			if d > 0 {
				return propertyDocument(r, d-1)
			}
		}
		return words[r.Intn(len(words))]
	}
	var value func(int) string
	value = func(d int) string {
		switch r.Intn(6) {
		case 0:
			return strconv.Itoa(r.Intn(100))
		case 1:
			return "[" + space() + value(d-1) + space() + "," + space() + value(d-1) + "]"
		case 2:
			if d > 0 {
				return propertyObject(r, d-1, keys, space, str, value)
			}
		}
		return `"` + escapeJSON(str(d)) + `"`
	}
	return propertyObject(r, depth, keys, space, str, value)
}

func propertyObject(r *rand.Rand, depth int, keys []string, space func() string, str func(int) string, value func(int) string) string {
	var fields []string
	used := map[string]bool{}
	for n := 1 + r.Intn(4); len(fields) < n; {
		key := keys[r.Intn(len(keys))]
		if r.Intn(4) == 0 {
			key = escapeJSON(str(0))
		}
		if used[key] {
			continue
		}
		used[key] = true
		fields = append(fields, space()+`"`+key+`"`+space()+":"+space()+value(depth))
	}
	return "{" + strings.Join(fields, ",") + space() + "}"
}

// Review 15: for random documents, scrub and restore change only the
// substituted spans, scrub then restore gives back the original bytes, and
// no secret survives scrubbing in any decoded view.
func TestRewriteProperties(t *testing.T) {
	r := rand.New(rand.NewSource(15))
	withSecrets, nestedWithSecrets := 0, 0
	defer func() {
		if withSecrets < 100 || nestedWithSecrets < 50 {
			t.Fatalf("corpus too thin: %d documents with secrets, %d of them nested", withSecrets, nestedWithSecrets)
		}
	}()
	for i := 0; i < 400; i++ {
		doc := propertyDocument(r, 3)
		if !json.Valid([]byte(doc)) {
			t.Fatalf("generator made invalid JSON: %s", doc)
		}
		requestID := "r15-property-" + strconv.Itoa(i)
		scrubbed, errScrub := scrub.ScrubChecked(requestID, []byte(doc))
		if errScrub != nil {
			t.Fatalf("scrub: %v\n%s", errScrub, doc)
		}
		want := doc
		for _, secret := range propertySecrets {
			if strings.Contains(doc, secret) {
				scrub.mu.Lock()
				id := scrub.byValue[secret]
				scrub.mu.Unlock()
				want = strings.ReplaceAll(want, secret, "<SECRET_"+id+">")
			}
		}
		if string(scrubbed) != want {
			t.Fatalf("scrub changed more than the secrets\ndoc  %s\ngot  %s\nwant %s", doc, scrubbed, want)
		}
		for _, secret := range propertySecrets {
			if strings.Contains(decodeAll(string(scrubbed)), secret) {
				t.Fatalf("secret survived scrubbing: %s", scrubbed)
			}
		}
		if strings.Contains(string(scrubbed), "SECRET_") {
			withSecrets++
			if strings.Contains(doc, `\"`) {
				nestedWithSecrets++
			}
		}
		state := scrub.state(requestID)
		if state == nil {
			if string(scrubbed) != doc {
				t.Fatal("changed without state")
			}
			continue
		}
		if restored := state.restoreArguments(string(scrubbed)); restored != doc {
			t.Fatalf("restore did not give back the original\ndoc  %s\ngot  %s", doc, restored)
		}
		if restored := string(scrub.Restore(requestID, scrubbed)); restored != doc {
			t.Fatalf("reply restore did not give back the original\ndoc  %s\ngot  %s", doc, restored)
		}
		scrub.End(requestID)
	}
}

// argumentsWithOpener is tool-call arguments holding the placeholder at
// JSON depth depth (in a document held in a string, depth-1 times), with
// its "<" written as opener, one encoding of "<" at that depth.
func argumentsWithOpener(placeholder string, depth int, opener string) string {
	doc := `{"k":"@` + placeholder[1:] + `"}`
	for level := 1; level < depth; level++ {
		doc = `{"k":"` + escapeJSON(doc) + `"}`
	}
	return strings.Replace(doc, "@", opener, 1)
}

// valueAtDepth decodes arguments depth levels down and returns k there.
func valueAtDepth(args string, depth int) (string, bool) {
	text := args
	for level := 0; level < depth; level++ {
		var doc struct{ K string }
		if json.Unmarshal([]byte(text), &doc) != nil {
			return "", false
		}
		text = doc.K
	}
	return text, true
}

// Review 16, finding 3: every generated encoding of a placeholder's "<"
// (each level's backslashes as \\ or \u005c in either case, "<" as is or
// \u003c in either case), split after any of its bytes, is held and
// restored.
func TestEveryOpenerEncodingSplitIsRestored(t *testing.T) {
	placeholder := placeholderOf(t, "r16-3", anthropicKey)
	tested := 0
	// The forms the review split, then every generated one.
	explicit := []string{`\u005cu003c`, `\u005Cu003C`, `\\u003c`, `\u005c\u005cu003c`}
	for _, opener := range append(explicit, openerEncodings...) {
		depth := 1
		decoded := opener
		for decoded != "<" {
			var next string
			if json.Unmarshal([]byte(`"`+decoded+`"`), &next) != nil {
				t.Fatalf("encoding %q does not decode", opener)
			}
			decoded = next
			depth++
		}
		args := argumentsWithOpener(placeholder, depth, opener)
		if value, ok := valueAtDepth(args, depth); !ok || value != placeholder {
			t.Fatalf("encoding %q at depth %d: arguments %q do not hold the placeholder", opener, depth, args)
		}
		at := strings.Index(args, opener)
		for cut := at + 1; cut < at+len(opener); cut++ {
			scrub.ResetStream("r16-3")
			_, streamed := chatStream(t, "r16-3", []string{toolChunk(0, args[:cut]), toolChunk(0, args[cut:])})
			if value, ok := valueAtDepth(streamed[0], depth); !ok || value != anthropicKey {
				t.Fatalf("encoding %q split after %q: arguments %q", opener, args[at:cut], streamed[0])
			}
			tested++
		}
	}
	if tested < 100 {
		t.Fatalf("only %d splits tested", tested)
	}
}

// Review 16, finding 4: one size, the decoded bytes, decides whether
// arguments are restored, streamed or not: just under the cap they are
// restored everywhere, however long their JSON encoding; just over it,
// nowhere.
func TestArgumentsCapIsMeasuredOnDecodedBytes(t *testing.T) {
	defer func(old int) { maxArgumentsBytes = old }(maxArgumentsBytes)
	maxArgumentsBytes = 96
	placeholder := placeholderOf(t, "r16-4", anthropicKey)
	for _, c := range []struct {
		pad     int
		restore bool
	}{{4, true}, {40, false}} {
		inner := `{"q":"` + strings.Repeat(`"`, c.pad) + `","k":"` + placeholder + `"}`
		args, _ := json.Marshal(map[string]string{"doc": inner})
		if decoded := string(args); (len(decoded) <= maxArgumentsBytes) != c.restore {
			t.Fatalf("fixture: %d decoded bytes", len(decoded))
		}
		argsString, _ := json.Marshal(string(args))
		if len(argsString) <= maxArgumentsBytes && c.restore {
			t.Fatalf("fixture: encoded %d bytes is not over the cap", len(argsString))
		}
		restored := func(text string) bool { return strings.Contains(decodeAll(text), anthropicKey) }
		whole := reply(t, "r16-4", `{"choices":[{"message":{"tool_calls":[{"function":{"arguments":`+string(argsString)+`}}]}}]}`)
		scrub.ResetStream("r16-4")
		_, streamed := chatStream(t, "r16-4", []string{toolChunk(0, string(args)[:20]), toolChunk(0, string(args)[20:])})
		if restored(whole) != c.restore || restored(`"`+escapeJSON(streamed[0])+`"`) != c.restore {
			t.Fatalf("pad %d: whole restored %v, streamed restored %v, want %v", c.pad, restored(whole), restored(`"`+escapeJSON(streamed[0])+`"`), c.restore)
		}
	}
}

// accumulateChat is a client's view of a chat stream: every chunk's choices
// accumulated, as OpenAI clients accumulate deltas.
func accumulateChat(t *testing.T, chunks []string) any {
	t.Helper()
	var message any
	for _, chunk := range chunks {
		var parsed map[string]any
		if errUnmarshal := json.Unmarshal([]byte(chunk), &parsed); errUnmarshal != nil {
			t.Fatalf("chunk %q: %v", chunk, errUnmarshal)
		}
		message = clientAccumulate(message, parsed["choices"])
	}
	return message
}

// clientAccumulate is how an OpenAI client accumulates stream deltas
// (openai-python's accumulate_delta, written apart from the plugin's
// merge): a missing or null value takes the delta, a null delta changes
// nothing; index, type and finish_reason take the delta; strings join;
// objects accumulate field by field; arrays of objects accumulate by their
// index, other arrays extend; anything else takes the delta.
func clientAccumulate(acc, delta any) any {
	switch {
	case acc == nil:
		return delta
	case delta == nil:
		return acc
	}
	if a, ok := acc.(string); ok {
		if d, ok := delta.(string); ok {
			return a + d
		}
		return delta
	}
	if a, ok := acc.(map[string]any); ok {
		d, ok := delta.(map[string]any)
		if !ok {
			return delta
		}
		out := map[string]any{}
		for k, v := range a {
			out[k] = v
		}
		for k, v := range d {
			switch k {
			case "index", "type", "finish_reason":
				if v != nil {
					out[k] = v
				}
			default:
				out[k] = clientAccumulate(out[k], v)
			}
		}
		return out
	}
	if a, ok := acc.([]any); ok {
		d, ok := delta.([]any)
		if !ok {
			return delta
		}
		out := append([]any(nil), a...)
	entries:
		for _, entry := range d {
			if object, ok := entry.(map[string]any); ok {
				if index, ok := object["index"]; ok {
					for i, existing := range out {
						if e, ok := existing.(map[string]any); ok && e["index"] == index {
							out[i] = clientAccumulate(e, object)
							continue entries
						}
					}
				}
			}
			out = append(out, entry)
		}
		return out
	}
	return delta
}

// randomChatChunk is one chat completion chunk with a random delta: text,
// tool call fragments, a legacy function_call, reasoning block fragments.
func randomChatChunk(r *rand.Rand, first, last bool) string {
	words := []string{"a", "b c", `{"x":`, `1}`, "", "ok"}
	word := func() string { return words[r.Intn(len(words))] }
	delta := map[string]any{}
	if first {
		delta["role"] = "assistant"
	}
	if r.Intn(2) == 0 {
		delta["content"] = word()
	}
	if r.Intn(3) == 0 {
		call := map[string]any{"index": r.Intn(2), "function": map[string]any{"arguments": word()}}
		if r.Intn(3) == 0 {
			call["id"] = "call_" + strconv.Itoa(r.Intn(9))
			call["type"] = "function"
		}
		delta["tool_calls"] = []any{call}
	}
	if r.Intn(4) == 0 {
		delta["function_call"] = map[string]any{"arguments": word()}
	}
	if r.Intn(4) == 0 {
		delta["reasoning_blocks"] = []any{map[string]any{"index": r.Intn(2), "type": "thinking", "thinking": word(), "signature": word()}}
	}
	choice := map[string]any{"index": r.Intn(2), "delta": delta, "finish_reason": nil}
	if last {
		choice["finish_reason"] = "stop"
	}
	raw, _ := json.Marshal(map[string]any{"id": "c1", "object": "chat.completion.chunk", "choices": []any{choice}})
	return string(raw)
}

// Review 17, finding 1: merging any run of consecutive chat chunks leaves
// a client with the same message as the chunks one by one: no fragment of
// any delta field is lost.
func TestChatChunkMergeIsLossless(t *testing.T) {
	r := rand.New(rand.NewSource(17))
	for i := 0; i < 500; i++ {
		n := 2 + r.Intn(8)
		chunks := make([]string, n)
		for j := range chunks {
			chunks[j] = randomChatChunk(r, j == 0, j == n-1)
		}
		from := r.Intn(n - 1)
		to := from + 2 + r.Intn(n-from-1)
		merged := append(append(append([]string(nil), chunks[:from]...), mergeChatChunks(chunks[from:to])), chunks[to:]...)
		want, got := accumulateChat(t, chunks), accumulateChat(t, merged)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("merging chunks %d..%d changed the message\nchunks %q\nwant %v\ngot  %v", from, to, chunks, want, got)
		}
	}
}

// Review 17, finding 1: held text delays a legacy function_call and
// reasoning blocks; their later fragments survive the merge.
func TestMergedChatChunksKeepEveryFragment(t *testing.T) {
	placeholderOf(t, "r17-1", anthropicKey)
	chunks := []string{
		`{"choices":[{"index":0,"delta":{"content":"a <"},"finish_reason":null}]}`,
		`{"choices":[{"index":0,"delta":{"function_call":{"name":"f","arguments":"{\"a\":"},"reasoning_blocks":[{"index":0,"type":"thinking","thinking":"o","signature":"s1"}]},"finish_reason":null}]}`,
		`{"choices":[{"index":0,"delta":{"function_call":{"arguments":"1}"},"reasoning_blocks":[{"index":0,"thinking":"k","signature":"s2"}]},"finish_reason":null}]}`,
		`{"choices":[{"index":0,"delta":{},"finish_reason":"function_call"}]}`,
	}
	var delivered []string
	for i, chunk := range chunks {
		if out := deliver(t, "r17-1", i, chunk); out != "" {
			delivered = append(delivered, out)
		}
	}
	got := accumulateChat(t, delivered)
	if want := accumulateChat(t, chunks); !reflect.DeepEqual(want, got) {
		t.Fatalf("message changed\nwant %v\ngot  %v", want, got)
	}
}

// Review 17, finding 2: a first arguments delta past the cap goes on at
// once, unrestored, even when it may hold a placeholder.
func TestOversizedFirstArgumentsDeltaIsNotHeld(t *testing.T) {
	defer func(old int) { maxArgumentsBytes = old }(maxArgumentsBytes)
	maxArgumentsBytes = 64
	placeholder := placeholderOf(t, "r17-2", anthropicKey)
	chunk := toolChunk(0, `{"pad":"`+strings.Repeat("x", 80)+`","k":"`+placeholder+`"}`)
	if got := deliver(t, "r17-2", 0, chunk); got != chunk {
		t.Fatalf("oversized delta held or changed: %q", got)
	}
}

// Review 17, finding 3: arguments restore the same streamed as whole, at
// nested-budget caps just around what they need.
func TestNestedBudgetIsChargedTheSameStreamedAndWhole(t *testing.T) {
	defer func(old int) { maxNestedBytes = old }(maxNestedBytes)
	placeholder := placeholderOf(t, "r17-3", anthropicKey)
	inner, _ := json.Marshal(map[string]string{"k": placeholder})
	args, _ := json.Marshal(map[string]string{"doc": string(inner)})
	argsString, _ := json.Marshal(string(args))
	need := len(args) + len(inner)
	sawBoth := map[bool]bool{}
	for budget := need - 3; budget <= need+3; budget++ {
		maxNestedBytes = budget
		whole := reply(t, "r17-3", `{"choices":[{"message":{"tool_calls":[{"function":{"arguments":`+string(argsString)+`}}]}}]}`)
		scrub.ResetStream("r17-3")
		_, streamed := chatStream(t, "r17-3", []string{toolChunk(0, string(args)[:10]), toolChunk(0, string(args)[10:])})
		wholeRestored := strings.Contains(decodeAll(whole), anthropicKey)
		streamedRestored := strings.Contains(decodeAll(`"`+escapeJSON(streamed[0])+`"`), anthropicKey)
		if wholeRestored != streamedRestored {
			t.Fatalf("budget %d: whole restored %v, streamed %v", budget, wholeRestored, streamedRestored)
		}
		sawBoth[wholeRestored] = true
	}
	if !sawBoth[true] || !sawBoth[false] {
		t.Fatalf("the caps tried do not straddle the boundary (needed %d): %v", need, sawBoth)
	}
}

// logprobChunk is a chat chunk whose choice carries content and logprob
// token records.
func logprobChunk(content string, tokens []string, finish bool) string {
	var records []any
	for _, token := range tokens {
		records = append(records, map[string]any{"token": token, "logprob": -0.5})
	}
	choice := map[string]any{"index": 0, "delta": map[string]any{"content": content}, "logprobs": map[string]any{"content": records}, "finish_reason": nil}
	if finish {
		choice["finish_reason"] = "stop"
	}
	raw, _ := json.Marshal(map[string]any{"choices": []any{choice}})
	return string(raw)
}

// tokensOf is the logprob token records a client accumulates, in order.
func tokensOf(message any) []string {
	var tokens []string
	choices, _ := message.([]any)
	for _, raw := range choices {
		choice, _ := raw.(map[string]any)
		logprobs, _ := choice["logprobs"].(map[string]any)
		records, _ := logprobs["content"].([]any)
		for _, record := range records {
			entry, _ := record.(map[string]any)
			token, _ := entry["token"].(string)
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// Review 18, finding 1: a logprob token is a record of its own chunk; the
// halves of a placeholder in two chunks' records are not joined into one,
// and nothing is restored across them.
func TestLogprobRecordsAreNotJoined(t *testing.T) {
	placeholder := placeholderOf(t, "r18-1", anthropicKey)
	chunks := []string{
		logprobChunk("", []string{placeholder[:4], "x"}, false),
		logprobChunk("", []string{placeholder[4:]}, false),
		logprobChunk("", nil, true),
	}
	var delivered []string
	for i, chunk := range chunks {
		if out := deliver(t, "r18-1", i, chunk); out != "" {
			delivered = append(delivered, out)
		}
	}
	if got, want := tokensOf(accumulateChat(t, delivered)), tokensOf(accumulateChat(t, chunks)); !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens %q, want %q", got, want)
	}
}

// Review 18, finding 1: for random chat streams with placeholders split
// across chunks both in content and in logprob tokens, the content arrives
// restored and the token records exactly as they came.
func TestOnlyContinuingStringsJoinAcrossChunks(t *testing.T) {
	r := rand.New(rand.NewSource(18))
	for i := 0; i < 200; i++ {
		requestID := "r18-p-" + strconv.Itoa(i)
		placeholder := placeholderOf(t, requestID, anthropicKey)
		text := "a " + placeholder + " b " + placeholder + " c"
		var chunks []string
		var tokens []string
		for at := 0; at < len(text); {
			n := min(1+r.Intn(12), len(text)-at)
			piece := text[at : at+n]
			at += n
			record := []string{piece}
			if r.Intn(2) == 0 {
				record = append(record, "t"+strconv.Itoa(i))
			}
			tokens = append(tokens, record...)
			chunks = append(chunks, logprobChunk(piece, record, false))
		}
		chunks = append(chunks, logprobChunk("", nil, true))
		var delivered []string
		for j, chunk := range chunks {
			if out := deliver(t, requestID, j, chunk); out != "" {
				delivered = append(delivered, out)
			}
		}
		message := accumulateChat(t, delivered)
		choices, _ := message.([]any)
		choice, _ := choices[0].(map[string]any)
		delta, _ := choice["delta"].(map[string]any)
		if content, _ := delta["content"].(string); content != strings.ReplaceAll(text, placeholder, anthropicKey) {
			t.Fatalf("content %q", content)
		}
		if got := tokensOf(message); !reflect.DeepEqual(got, tokens) {
			t.Fatalf("tokens %q, want %q", got, tokens)
		}
		scrub.End(requestID)
	}
}

// looseArguments restores tool-call arguments that are not JSON both ways
// a reply carries them, whole and streamed in two chunks, and returns both.
func looseArguments(t *testing.T, requestID, args string, cut int) (string, string) {
	t.Helper()
	argsString, _ := json.Marshal(args)
	whole := reply(t, requestID, `{"choices":[{"message":{"tool_calls":[{"function":{"arguments":`+string(argsString)+`}}]}}]}`)
	scrub.ResetStream(requestID)
	_, streamed := chatStream(t, requestID, []string{toolChunk(0, args[:cut]), toolChunk(0, args[cut:])})
	return gjson.Get(whole, "choices.0.message.tool_calls.0.function.arguments").String(), streamed[0]
}

// Review 19: in arguments that are not JSON, issued placeholders are
// located (in any encoding) and classified by a lexical scan: a literal
// SECRET_ word is no placeholder, and an escaped placeholder in a string
// still counts as in a string.
func TestLooseArgumentsLocatePlaceholders(t *testing.T) {
	placeholder := placeholderOf(t, "r19-1", anthropicKey)
	args := "echo SECRET_notes " + placeholder
	whole, streamed := looseArguments(t, "r19-1", args, 5)
	if want := "echo SECRET_notes " + anthropicKey; whole != want || streamed != want {
		t.Fatalf("whole %q, streamed %q", whole, streamed)
	}
	escaped := strings.Replace(placeholder, "E", `\u0045`, 1)
	args = `{"k":"` + escaped + `",}`
	whole, streamed = looseArguments(t, "r19-1", args, 3)
	if whole != args || streamed != args {
		t.Fatalf("placeholder in a string restored: whole %q, streamed %q", whole, streamed)
	}
}

// encodeRandomly writes text as JSON string content one level up, each
// character as is or as a \u00XX escape at random, a backslash as \\ or
// \u005c, a quote as \".
func encodeRandomly(r *rand.Rand, text string) string {
	var b strings.Builder
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c == '\\':
			b.WriteString([]string{`\\`, `\u005c`}[r.Intn(2)])
		case c == '"':
			b.WriteString(`\"`)
		case r.Intn(3) == 0:
			fmt.Fprintf(&b, `\u%04x`, c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// Review 19: random arguments that are not JSON, mixing words, literal
// SECRET_ words and issued placeholders in random encodings, in and out of
// quoted strings: a placeholder in a string leaves the text as it is;
// otherwise every placeholder becomes its value and nothing else changes.
// Whole and streamed agree.
func TestLooseArgumentsProperty(t *testing.T) {
	r := rand.New(rand.NewSource(19))
	placeholder := placeholderOf(t, "r19-p", anthropicKey)
	restoredCases, quotedCases := 0, 0
	defer func() {
		if restoredCases < 30 || quotedCases < 30 {
			t.Fatalf("corpus too thin: %d restored, %d left in a string", restoredCases, quotedCases)
		}
	}()
	for i := 0; i < 300; i++ {
		var segments, expected []string
		anyQuoted := false
		for n := 1 + r.Intn(5); n > 0; n-- {
			switch r.Intn(4) {
			case 0:
				word := []string{"echo", "SECRET_notes", "x", "SECRET_", "k="}[r.Intn(5)]
				segments, expected = append(segments, word), append(expected, word)
			case 1:
				depth := r.Intn(3)
				text, value := placeholder, anthropicKey
				for level := 0; level < depth; level++ {
					text, value = encodeRandomly(r, text), escapeJSON(value)
				}
				segments, expected = append(segments, text), append(expected, value)
			default:
				content := placeholder
				if r.Intn(2) == 0 {
					content = encodeRandomly(r, placeholder)
				}
				quoted := `"` + content + ` ok"`
				segments, expected = append(segments, quoted), append(expected, quoted)
				anyQuoted = true
			}
		}
		args := strings.Join(segments, " ") + " ,}"
		if json.Valid([]byte(args)) {
			continue
		}
		want := strings.Join(expected, " ") + " ,}"
		switch {
		case anyQuoted:
			want = args
			quotedCases++
		case want != args:
			restoredCases++
		}
		whole, streamed := looseArguments(t, "r19-p", args, 1+r.Intn(len(args)-1))
		if whole != want || streamed != want {
			t.Fatalf("arguments %q\nwhole    %q\nstreamed %q\nwant     %q", args, whole, streamed, want)
		}
	}
}
