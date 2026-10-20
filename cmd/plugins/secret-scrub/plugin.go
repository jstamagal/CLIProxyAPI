package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

const pluginVersion = "0.1.0"

var (
	scrub      = newScrubber()
	configMu   sync.RWMutex
	switchName = "X-JS-Scrub"
)

type pluginConfig struct {
	Header  string `yaml:"header"`
	TTL     string `yaml:"ttl"`
	Entropy *bool  `yaml:"entropy"`
}

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type lifecycleRequest struct {
	ConfigYAML    []byte `json:"config_yaml"`
	SchemaVersion uint32 `json:"schema_version"`
}

type registration struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Metadata      pluginapi.Metadata     `json:"metadata"`
	Capabilities  registrationCapability `json:"capabilities"`
}

type registrationCapability struct {
	RequestInterceptor     bool `json:"request_interceptor"`
	RequestLifecyclePlugin bool `json:"request_lifecycle_plugin"`
	ResponseInterceptor    bool `json:"response_interceptor"`
	StreamChunkInterceptor bool `json:"response_stream_interceptor"`
}

func main() {}

func handleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginQuiesce:
		// The host is retiring this instance and may keep it loaded. Its
		// sweeper keeps running, so what it remembers still expires.
		return okEnvelope(struct{}{})
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		if errConfigure := configure(request); errConfigure != nil {
			return nil, errConfigure
		}
		return okEnvelope(pluginRegistration())
	case pluginabi.MethodRequestInterceptBefore:
		return interceptBeforeAuth(request)
	case pluginabi.MethodRequestInterceptAfter:
		return okEnvelope(pluginapi.RequestInterceptResponse{})
	case pluginabi.MethodResponseInterceptAfter:
		return interceptResponse(request)
	case pluginabi.MethodResponseInterceptStreamChunk:
		return interceptStreamChunk(request)
	case pluginabi.MethodRequestComplete:
		return completeRequest(request)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

func configure(raw []byte) error {
	var req lifecycleRequest
	if len(raw) > 0 {
		if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
			return errUnmarshal
		}
	}
	var cfg pluginConfig
	if len(req.ConfigYAML) > 0 {
		if errUnmarshal := yaml.Unmarshal(req.ConfigYAML, &cfg); errUnmarshal != nil {
			return errUnmarshal
		}
	}
	ttl := 24 * time.Hour
	if strings.TrimSpace(cfg.TTL) != "" {
		parsed, errParse := time.ParseDuration(strings.TrimSpace(cfg.TTL))
		if errParse != nil || parsed <= 0 {
			return fmt.Errorf("ttl must be a positive duration such as 24h: %q", cfg.TTL)
		}
		ttl = parsed
	}
	header := "X-JS-Scrub"
	if strings.TrimSpace(cfg.Header) != "" {
		header = strings.TrimSpace(cfg.Header)
	}
	configMu.Lock()
	switchName = header
	configMu.Unlock()
	scrub.mu.Lock()
	scrub.ttl = ttl
	scrub.entropy = cfg.Entropy == nil || *cfg.Entropy
	scrub.mu.Unlock()
	scrub.startSweeper(sweepInterval(ttl))
	return nil
}

// sweepInterval is how often remembered values are checked for expiry: a
// quarter of the TTL, between a second and a minute.
func sweepInterval(ttl time.Duration) time.Duration {
	return min(max(ttl/4, time.Second), time.Minute)
}

// shutdownPlugin stops the sweeper and forgets every secret. The host may
// keep the library image loaded after shutdown, so nothing is left behind.
func shutdownPlugin() {
	scrub.Reset()
}

func pluginRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             "secret-scrub",
			Version:          pluginVersion,
			Author:           "jstamagal",
			GitHubRepository: "https://github.com/jstamagal/CLIProxyAPI",
			ConfigFields: []pluginapi.ConfigField{
				{Name: "header", Type: pluginapi.ConfigFieldTypeString,
					Description: "Request header that switches scrubbing on with the value on. Default X-JS-Scrub."},
				{Name: "ttl", Type: pluginapi.ConfigFieldTypeString,
					Description: "How long a secret is remembered after it was last seen. Default 24h."},
				{Name: "entropy", Type: pluginapi.ConfigFieldTypeBoolean,
					Description: "Also scrub high-entropy tokens of 24 or more characters. Default true."},
			},
		},
		Capabilities: registrationCapability{
			RequestInterceptor:     true,
			RequestLifecyclePlugin: true,
			ResponseInterceptor:    true,
			StreamChunkInterceptor: true,
		},
	}
}

// switchedOn reports whether headers carry the header name set to on.
func switchedOn(headers http.Header, name string) bool {
	return strings.EqualFold(strings.TrimSpace(headers.Get(name)), "on")
}

// unscrubbableRejection is the error a switched request gets when its body
// cannot be scrubbed whole.
const unscrubbableRejection = `{"error":{"type":"invalid_request_error","code":"secret_scrub_unparsable","message":"secret-scrub: the request body is not JSON the scrubber can read whole, so it is not sent upstream"}}`

// websocketRejection is the error a scrubbed WebSocket request gets.
const websocketRejection = `{"error":{"type":"invalid_request_error","code":"secret_scrub_websocket","message":"secret-scrub: scrubbing is not available over the Responses WebSocket, whose later turns can reach upstream without passing the scrubber; use HTTP"}}`

// websocketUpgrade reports whether headers are those of a WebSocket
// upgrade. Over the Responses WebSocket with response steering on, later
// response.create frames go upstream on a duplex channel that no request
// hook sees, so a scrubbed request must not use that transport. The plugin
// cannot see or change whether steering is on, so it refuses every
// scrubbed WebSocket request.
func websocketUpgrade(headers http.Header) bool {
	for _, value := range headers.Values("Upgrade") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "websocket") {
				return true
			}
		}
	}
	return false
}

func switchHeader() string {
	configMu.RLock()
	defer configMu.RUnlock()
	return switchName
}

func interceptBeforeAuth(raw []byte) ([]byte, error) {
	var req pluginapi.RequestInterceptRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	// One read of the name, so a reload between the check and the clear
	// cannot clear a different header than the one that switched scrubbing on.
	name := switchHeader()
	if !switchedOn(req.Headers, name) {
		return okEnvelope(pluginapi.RequestInterceptResponse{})
	}
	if !supportedFormat(req.SourceFormat) {
		log.WithFields(log.Fields{"request_id": req.RequestID, "format": req.SourceFormat}).Info("secret-scrub: format not scrubbed; request passed through untouched")
		return okEnvelope(pluginapi.RequestInterceptResponse{})
	}
	if websocketUpgrade(req.Headers) {
		log.WithField("request_id", req.RequestID).Warn("secret-scrub: scrubbing asked over the Responses WebSocket; request rejected")
		return okEnvelope(pluginapi.RequestInterceptResponse{
			Terminate:       true,
			StatusCode:      http.StatusBadRequest,
			ResponseHeaders: http.Header{"Content-Type": {"application/json"}},
			ResponseBody:    []byte(websocketRejection),
		})
	}
	body, errScrub := scrub.ScrubChecked(req.RequestID, req.Body)
	if errScrub != nil {
		// Fail closed: what the scrubber cannot read whole, a translator
		// may still read, secrets and all.
		log.WithField("request_id", req.RequestID).WithError(errScrub).Warn("secret-scrub: request rejected")
		return okEnvelope(pluginapi.RequestInterceptResponse{
			Terminate:       true,
			StatusCode:      http.StatusBadRequest,
			ResponseHeaders: http.Header{"Content-Type": {"application/json"}},
			ResponseBody:    []byte(unscrubbableRejection),
		})
	}
	scrub.MarkCodexClient(req.RequestID, codexClientRequest(req.Headers))
	resp := pluginapi.RequestInterceptResponse{ClearHeaders: []string{name}}
	if !bytes.Equal(body, req.Body) {
		resp.Body = body
	}
	return okEnvelope(resp)
}

func interceptResponse(raw []byte) ([]byte, error) {
	var req pluginapi.ResponseInterceptRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	restored := scrub.Restore(req.RequestID, req.Body)
	if bytes.Equal(restored, req.Body) {
		return okEnvelope(pluginapi.ResponseInterceptResponse{})
	}
	return okEnvelope(pluginapi.ResponseInterceptResponse{Body: restored})
}

func interceptStreamChunk(raw []byte) ([]byte, error) {
	var req pluginapi.StreamChunkInterceptRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	if req.ChunkIndex == pluginapi.StreamChunkHeaderInitIndex {
		// The host announces each attempt of the stream this way, the
		// first and every retry: start the attempt afresh.
		scrub.ResetStream(req.RequestID)
		return okEnvelope(pluginapi.StreamChunkInterceptResponse{})
	}
	if req.ChunkIndex < 0 || len(req.Body) == 0 {
		return okEnvelope(pluginapi.StreamChunkInterceptResponse{})
	}
	restored := scrub.RestoreChunk(req.RequestID, req.Body)
	if restored == nil {
		// Part of an SSE frame, held until the frame is complete.
		return okEnvelope(pluginapi.StreamChunkInterceptResponse{DropChunk: true})
	}
	if bytes.Equal(restored, req.Body) {
		return okEnvelope(pluginapi.StreamChunkInterceptResponse{})
	}
	return okEnvelope(pluginapi.StreamChunkInterceptResponse{Body: restored})
}

func completeRequest(raw []byte) ([]byte, error) {
	var completion pluginapi.RequestCompletion
	if errUnmarshal := json.Unmarshal(raw, &completion); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	scrub.End(completion.RequestID)
	return okEnvelope(struct{}{})
}

func okEnvelope(v any) ([]byte, error) {
	raw, errMarshal := json.Marshal(v)
	if errMarshal != nil {
		return nil, errMarshal
	}
	return json.Marshal(envelope{OK: true, Result: raw})
}

func errorEnvelope(code, message string) []byte {
	raw, errMarshal := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	if errMarshal != nil {
		return []byte(`{"ok":false,"error":{"code":"plugin_error","message":"encode error"}}`)
	}
	return raw
}
