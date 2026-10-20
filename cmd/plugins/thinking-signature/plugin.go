package main

import (
	"encoding/json"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

const pluginVersion = "0.1.0"

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type registration struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Metadata      pluginapi.Metadata     `json:"metadata"`
	Capabilities  registrationCapability `json:"capabilities"`
}

type registrationCapability struct {
	RequestInterceptor       bool `json:"request_interceptor"`
	RequestNormalizer        bool `json:"request_normalizer"`
	ResponseBeforeTranslator bool `json:"response_before_translator"`
	ResponseAfterTranslator  bool `json:"response_after_translator"`
}

func main() {}

func handleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		return okEnvelope(pluginRegistration())
	case pluginabi.MethodRequestInterceptBefore:
		return okEnvelope(pluginapi.RequestInterceptResponse{})
	case pluginabi.MethodRequestInterceptAfter:
		return interceptAfterAuth(request)
	case pluginabi.MethodRequestNormalize:
		return normalizeRequest(request)
	case pluginabi.MethodResponseNormalizeBefore:
		return normalizeResponse(request, beforeTranslation)
	case pluginabi.MethodResponseNormalizeAfter:
		return normalizeResponse(request, afterTranslation)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

func pluginRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             "thinking-signature",
			Version:          pluginVersion,
			Author:           "jstamagal",
			GitHubRepository: "https://github.com/jstamagal/CLIProxyAPI",
		},
		Capabilities: registrationCapability{
			RequestInterceptor:       true,
			RequestNormalizer:        true,
			ResponseBeforeTranslator: true,
			ResponseAfterTranslator:  true,
		},
	}
}

// interceptAfterAuth runs once the upstream format is known, before the
// native translation.
func interceptAfterAuth(raw []byte) ([]byte, error) {
	var req pluginapi.RequestInterceptRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	if req.SourceFormat != formatOpenAI {
		return okEnvelope(pluginapi.RequestInterceptResponse{})
	}
	return okEnvelope(pluginapi.RequestInterceptResponse{Body: prepareRequest(req.Body, req.ToFormat == formatClaude)})
}

func normalizeRequest(raw []byte) ([]byte, error) {
	var req pluginapi.RequestTransformRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	if req.FromFormat != formatOpenAI || req.ToFormat != formatClaude {
		return okEnvelope(pluginapi.PayloadResponse{})
	}
	return okEnvelope(pluginapi.PayloadResponse{Body: attachBlocks(req.Body)})
}

type responseStage int

const (
	beforeTranslation responseStage = iota
	afterTranslation
)

func normalizeResponse(raw []byte, stage responseStage) ([]byte, error) {
	var req pluginapi.ResponseTransformRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	if req.FromFormat != formatClaude || req.ToFormat != formatOpenAI {
		return okEnvelope(pluginapi.PayloadResponse{})
	}
	var out []byte
	if stage == beforeTranslation {
		out = markBlocks(req.Body)
	} else {
		out = liftBlocks(req.Body)
	}
	return okEnvelope(pluginapi.PayloadResponse{Body: out})
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
