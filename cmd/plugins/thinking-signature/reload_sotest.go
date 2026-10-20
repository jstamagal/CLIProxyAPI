//go:build sotest

package main

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} so_buffer;

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	void* call;
	void* free_buffer;
} so_host_api;

typedef int (*so_call_fn)(char*, uint8_t*, size_t, so_buffer*);
typedef void (*so_free_fn)(void*, size_t);
typedef void (*so_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	so_call_fn call;
	so_free_fn free_buffer;
	so_shutdown_fn shutdown;
} so_plugin_api;

typedef int (*so_init_fn)(so_host_api*, so_plugin_api*);

static int so_open(const char* path, so_plugin_api* api) {
	void* handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
	if (handle == NULL) {
		return 1;
	}
	so_init_fn init = (so_init_fn)dlsym(handle, "cliproxy_plugin_init");
	if (init == NULL) {
		return 2;
	}
	so_host_api host = {0};
	return init(&host, api) == 0 ? 0 : 3;
}

static int so_call(so_plugin_api* api, char* method, uint8_t* req, size_t len, so_buffer* out) {
	return api->call(method, req, len, out);
}

static void so_free(so_plugin_api* api, so_buffer* buf) {
	if (buf->ptr != NULL) {
		api->free_buffer(buf->ptr, buf->len);
	}
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// loadedPlugin is a copy of this plugin's shared library loaded through
// its C ABI, as the host loads it. It exists for the reload test only.
type loadedPlugin struct{ api C.so_plugin_api }

func loadPlugin(path string) (*loadedPlugin, error) {
	p := &loadedPlugin{}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if code := C.so_open(cPath, &p.api); code != 0 {
		return nil, fmt.Errorf("load %s: %d: %s", path, code, C.GoString(C.dlerror()))
	}
	return p, nil
}

func (p *loadedPlugin) call(method string, request []byte) ([]byte, error) {
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	cRequest := C.CBytes(request)
	defer C.free(cRequest)
	var out C.so_buffer
	code := C.so_call(&p.api, cMethod, (*C.uint8_t)(cRequest), C.size_t(len(request)), &out)
	defer C.so_free(&p.api, &out)
	raw := C.GoBytes(out.ptr, C.int(out.len))
	if code != 0 {
		return nil, fmt.Errorf("%s: %s", method, raw)
	}
	return raw, nil
}
