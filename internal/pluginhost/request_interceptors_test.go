package pluginhost

import (
	"context"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestReloadedRequestCompletesThroughItsOriginalInterceptor(t *testing.T) {
	host := New()
	completed := make(chan string, 1)
	record := func(version string) capabilityRecord {
		plugin := validTestPlugin("scrubber")
		plugin.Capabilities.RequestInterceptor = requestInterceptorFunc(func(context.Context, pluginapi.RequestInterceptRequest) (pluginapi.RequestInterceptResponse, error) {
			return pluginapi.RequestInterceptResponse{}, nil
		})
		plugin.Capabilities.RequestLifecyclePlugin = requestLifecyclePluginFunc(func(context.Context, pluginapi.RequestCompletion) {
			completed <- version
		})
		return normalizeTestCapabilityRecord(capabilityRecord{id: "scrubber", path: "/test/" + version, version: version, plugin: plugin})
	}
	old, replacement := record("1.0.0"), record("2.0.0")
	setHostSnapshotForTest(host, true, old)
	host.InterceptRequestBeforeAuth(t.Context(), pluginapi.RequestInterceptRequest{RequestID: "in-flight"})
	host.mu.Lock()
	host.retired[old.id] = []*loadedPlugin{{path: old.path, version: old.version}}
	host.mu.Unlock()
	setHostSnapshotForTest(host, true, replacement)
	host.CompleteRequest(t.Context(), pluginapi.RequestCompletion{RequestID: "in-flight"})
	if got := <-completed; got != old.version {
		t.Errorf("completion instance = %q, want original %q", got, old.version)
	}
	host.mu.Lock()
	bound := len(host.requestInterceptors)
	host.mu.Unlock()
	if bound != 0 {
		t.Errorf("request bindings after completion = %d, want 0", bound)
	}
	host.InterceptRequestBeforeAuth(t.Context(), pluginapi.RequestInterceptRequest{RequestID: "fresh"})
	host.CompleteRequest(t.Context(), pluginapi.RequestCompletion{RequestID: "fresh"})
	if got := <-completed; got != replacement.version {
		t.Errorf("fresh request completion instance = %q, want replacement %q", got, replacement.version)
	}
}
