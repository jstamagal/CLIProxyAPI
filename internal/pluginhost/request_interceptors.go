package pluginhost

// rememberRequestInterceptor binds a stateful interceptor to the request
// before it can retain secrets or partial stream frames. A hot reload keeps
// its old library resident, so this request can finish through that instance.
func (h *Host) rememberRequestInterceptor(requestID string, record capabilityRecord) {
	if requestID == "" || record.plugin.Capabilities.RequestLifecyclePlugin == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, bound := range h.requestInterceptors[requestID] {
		if bound.id == record.id {
			return
		}
	}
	if h.requestInterceptors == nil {
		h.requestInterceptors = make(map[string][]capabilityRecord)
	}
	h.requestInterceptors[requestID] = append(h.requestInterceptors[requestID], record)
}

// requestInterceptorRecords substitutes the instance that began each stateful
// interception for its replacement. Stateless hooks keep following config.
func (h *Host) requestInterceptorRecords(requestID string) []capabilityRecord {
	records := h.activeRecords()
	if h == nil || requestID == "" {
		return records
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, bound := range h.requestInterceptors[requestID] {
		if !h.retiredInterceptorLocked(bound) {
			continue
		}
		replaced := false
		for i := range records {
			if records[i].id == bound.id {
				records[i] = bound
				replaced = true
				break
			}
		}
		if !replaced {
			records = append(records, bound)
		}
	}
	sortRecords(records)
	return records
}

func (h *Host) requestInterceptorCurrent(requestID string, record capabilityRecord) bool {
	if h.recordCurrent(record) {
		return true
	}
	if h == nil || requestID == "" {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, bound := range h.requestInterceptors[requestID] {
		if bound.id == record.id && bound.path == record.path && bound.version == record.version {
			return h.retiredInterceptorLocked(bound)
		}
	}
	return false
}

// Explicit unload/disable removes retired entries too, so a binding never
// revives a shut-down plugin. Call with h.mu held.
func (h *Host) retiredInterceptorLocked(record capabilityRecord) bool {
	for _, loaded := range h.retired[record.id] {
		if loaded != nil && loaded.path == record.path && loaded.version == record.version {
			return true
		}
	}
	return false
}
