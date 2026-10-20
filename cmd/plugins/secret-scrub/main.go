// Command secret-scrub is a CLIProxyAPI dynamic-library plugin that keeps
// secrets out of upstream model requests.
//
// A request that carries the switch header (X-JS-Scrub: on by default) has
// every secret in its body replaced by a placeholder <SECRET_xxxxxxxx>
// before it goes upstream; xxxxxxxx is a short HMAC of the decoded value, so
// one value gets one placeholder across requests. The header is removed. In
// the reply (non-streaming body and stream chunks) each placeholder of a
// secret found in that same request is replaced by the real value again, so
// the client sees the value and the upstream model never does. Complete
// tool-call arguments are restored byte for byte inside their JSON (and
// inside JSON documents held in their strings, at any depth): only a
// placeholder's bytes change, so they stay valid. When the
// request asked for JSON output (chat response_format or Responses
// text.format of type json_object or json_schema), the message text is
// restored the same way. Other text is restored in place; a placeholder
// written there with \u00XX escapes gets the value JSON escaped. Requests
// without the header pass unchanged.
//
// Streams reach the client with the same events as without the plugin:
// the plugin frames a Responses stream exactly as the host does (its
// validator, then its framer), and a frame goes on unchanged unless a
// secret is restored in it. A delta that may end in part of a placeholder,
// and a tool call's arguments from the first delta that may hold one, wait
// (with every frame after them, so the order stays) until it is known;
// the first waiting delta then carries the restored text and the others
// none, or, when nothing was restored, all go on as they came.
//
// Supported formats: the client side of OpenAI chat completions
// (/v1/chat/completions) and the OpenAI Responses API (/v1/responses), in
// requests, non-streaming replies and stream chunks. A switched request in
// any other format (Claude messages, Gemini, image or plugin streams) is
// passed through untouched, header included, and one line is logged for it.
//
// A switched request whose body the scrubber cannot read whole (not JSON,
// or JSON documents in its strings past the size bound) is rejected with
// HTTP 400 and never forwarded: a translator could still read the secrets
// in it.
//
// A scrubbed request over the Responses WebSocket is rejected with HTTP 400:
// with response steering, its later turns would reach upstream on a duplex
// channel the request hooks never see. Use HTTP with scrubbing.
//
// Known limit: the restore state of a request lives in this library
// instance. Replacing the plugin (a service restart) while a request is in
// flight loses that state, and that reply keeps its placeholders.
//
// Detection, from bead js-y3c.11: known key shapes; .env NAME=value whose
// name holds KEY, TOKEN, SECRET, PASSWORD, PASSWD, PASS or AUTH (a bare
// value runs to the end of the line, # after whitespace starts a comment);
// runs of 24 or more non-whitespace characters (quotes, brackets and
// trailing punctuation trimmed) with at least 4 bits of entropy per
// character, except runs that are paths. Overlapping matches are scrubbed as
// their union. Signed reasoning (chat reasoning_blocks, Responses
// encrypted_content) is neither scrubbed nor restored.
//
// Build (Linux):
//
//	go build -buildmode=c-shared -o plugins/linux/amd64/secret-scrub.so ./cmd/plugins/secret-scrub
package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	void* call;
	void* free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);
*/
import "C"
import (
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
)

//export cliproxy_plugin_init
func cliproxy_plugin_init(_ *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required"))
		return 1
	}
	var requestBytes []byte
	if request != nil && requestLen > 0 {
		requestBytes = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, errHandle := handleMethod(C.GoString(method), requestBytes)
	if errHandle != nil {
		writeResponse(response, errorEnvelope("plugin_error", errHandle.Error()))
		return 1
	}
	writeResponse(response, raw)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() { shutdownPlugin() }

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}
