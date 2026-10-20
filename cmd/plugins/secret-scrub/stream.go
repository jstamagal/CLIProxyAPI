package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// streamQueue is a streamed reply's frames on their way to the host. A
// frame is passed on unchanged unless a secret is restored in it; a frame
// whose delta may hold part of a placeholder waits until that is known,
// and every frame after it waits behind it, so the order never changes.
type streamQueue struct {
	frames []*queuedFrame
	holds  map[string]*deltaHold
	// argumentsSent is, per tool call (or structured text), what was passed
	// on before any of it was held.
	argumentsSent map[string]string
	// unbounded marks the calls whose arguments outgrew the buffer; they go
	// on unrestored, and so do their snapshots.
	unbounded map[string]bool
}

// queuedFrame is one SSE frame, or one bare chat chunk.
type queuedFrame struct {
	frame   string // as received
	suffix  string // what the host's writer adds after it
	sent    string // the start of frame that went on already
	payload string // its data, as the client reads it
	offsets []int  // for each byte of payload, its offset in frame
	edits   []textEdit
	waiting int // holds on it not yet resolved
}

// deltaHold is the deltas of one logical string held back: text that may
// end in part of a placeholder, or the arguments of a tool call (or the
// text of structured output) from the first delta that may hold one.
type deltaHold struct {
	arguments bool
	steps     []step
	sites     []deltaSite
	text      string
	sent      string
}

type deltaSite struct {
	frame *queuedFrame
	token gjson.Result // the delta's string token in the frame's payload
}

// edit adds edits to the frame's data, in payload offsets.
func (f *queuedFrame) edit(edits []textEdit, at int) {
	f.edits = append(f.edits, shiftEdits(edits, at)...)
}

// render is the frame as it goes to the host, its edits applied to its own
// bytes: what of it did not go on already.
func (f *queuedFrame) render() string {
	frame := f.frame
	if len(f.edits) > 0 {
		var edits []textEdit
		for _, e := range f.edits {
			start, end := f.frameOffset(e.start), f.frameOffset(e.end)
			if start < 0 || end < 0 || start < len(f.sent) {
				log.Warn("secret-scrub: a restored span lies outside what can still be sent; placeholders left")
				edits = nil
				break
			}
			edits = append(edits, textEdit{start, end, e.text})
		}
		frame = applyEdits(frame, edits)
	}
	return frame[len(f.sent):] + f.suffix
}

// frameOffset maps a payload offset (end offsets too) to the frame.
func (f *queuedFrame) frameOffset(at int) int {
	switch {
	case at < len(f.offsets):
		return f.offsets[at]
	case at == len(f.offsets) && at > 0 && f.offsets[at-1] >= 0:
		return f.offsets[at-1] + 1
	}
	return -1
}

func (q *streamQueue) init() {
	if q.holds == nil {
		q.holds = make(map[string]*deltaHold)
		q.argumentsSent = make(map[string]string)
		q.unbounded = make(map[string]bool)
	}
}

// RestoreChunk restores placeholders in one stream chunk of requestID: a
// bare chat completion chunk, or SSE text of Responses events.
//
// SSE text is framed exactly as the host's Responses framer frames it
// (hostFramer), and each frame goes to the host as it came unless a secret
// is restored in it, so every event reaches the client with the same
// fields as it would without the plugin. A frame of only data lines, which
// the host keeps until the next data line or the end of the stream, is
// passed on for the host to keep when it ends the reply.
//
// A delta that may end in part of a placeholder waits, with the frames
// after it, until the next delta of the same string (same choice, tool
// call, output item or content part) shows whether a placeholder formed:
// if one did, the first waiting delta carries the restored text and the
// others carry none; if not, all go on unchanged. A tool call's arguments
// (and the text of structured output) go on unchanged until a delta may
// hold a placeholder; from there the call waits until it ends and is then
// restored whole. Waiting text that its part's end, its choice's finish or
// the reply's end finds is resolved then. A chunk that sends nothing
// returns nil, to be dropped.
func (s *scrubber) RestoreChunk(requestID string, chunk []byte) []byte {
	state := s.state(requestID)
	if state == nil {
		return chunk
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.stream.init()
	text := string(chunk)
	if !state.sseKnown {
		state.sseKnown = true
		state.sse = !strings.HasPrefix(strings.TrimLeft(text, " \t\r\n"), "{")
	}
	if !state.sse {
		state.queueFrame(text, "", true, "")
		out := state.releaseFrames(true)
		if out == "" {
			return nil
		}
		if out == text {
			return chunk
		}
		return []byte(out)
	}
	f := &state.framer
	if f.done || state.validator.failed {
		return chunk // the host has ended the stream and drops the rest
	}
	// The host validates the stream's frames before it frames them; the
	// framer sees what the validator passes on.
	validated, errValidate := state.validator.add(text)
	if errValidate != nil {
		// The host fails the stream at this frame: hand it everything held
		// and then the failing frame, so it fails on the same frame after
		// the same output.
		state.validator.failed = true
		return []byte(state.failStream())
	}
	if validated == "" {
		return nil
	}
	released := f.released
	var b strings.Builder
	for i, frame := range f.write(validated) {
		skip := 0
		if i == 0 {
			skip = min(released, len(frame))
		}
		suffix := sseFrameSuffix(frame)
		if skip == len(frame) {
			// All of it went on already, and the host ends it by its own
			// rule: nothing more to send.
			suffix = ""
		}
		state.queueFrame(frame, suffix, false, frame[:skip])
	}
	b.WriteString(state.releaseFrames(false))
	if !f.done && f.pending != "" && f.released < len(f.pending) &&
		sseHasField(f.pending, "data:") && !sseHasField(f.pending, "event:") && sseDataLinesValid(f.pending) &&
		sseFrameTerminal(f.pending, state.codexClient) {
		// The reply's last event without its event: line (the native
		// executor stops after it): the host keeps it until the end of the
		// stream, then sends it. It settles every hold; pass it on, after
		// the frames that waited, for the host to keep.
		state.queueFrame(f.pending, "", false, f.pending[:f.released])
		b.WriteString(state.releaseFrames(false))
		f.released = len(f.pending)
	}
	if !f.done && len(state.stream.frames) == 0 && f.released < len(f.pending) {
		// Nothing waits, and what the host holds of a frame not yet
		// complete cannot be (part of) a placeholder: no "<" and no "\\"
		// starts one. Pass it on as it came, so the host sees the same
		// chunks as without the plugin.
		if rest := f.pending[f.released:]; !strings.ContainsAny(rest, "<\\") && !strings.Contains(rest, "ECRET_") {
			b.WriteString(rest)
			f.released = len(f.pending)
		}
	}
	if state.validator.pendingErr != nil {
		// The host fails the stream at the next chunk, after this output.
		state.validator.failed = true
		b.WriteString(state.failStream())
	}
	if b.Len() == 0 {
		return nil
	}
	if b.String() == text {
		return chunk
	}
	return []byte(b.String())
}

// failStream is what the host must get when its validator fails the
// stream: every held frame (resolved), the text its framer holds (ended,
// so the framer sends it as it would at the error), and the failing frame.
func (st *requestState) failStream() string {
	var b strings.Builder
	for key, h := range st.stream.holds {
		st.resolve(key, h)
	}
	b.WriteString(st.releaseFrames(false))
	f := &st.framer
	if rest := f.pending[min(f.released, len(f.pending)):]; strings.TrimSpace(rest) != "" {
		b.WriteString(rest + "\n\n")
	}
	f.pending, f.released = "", 0
	b.WriteString(st.validator.failedFrame)
	return b.String()
}

// hostValidator is the host's check of a Responses stream before its
// framer (sseJSONValidationState in sdk/api/handlers): it normalizes line
// ends, joins a data: or event: line that arrives as its own chunk with a
// line break, and passes on whole frames, and text after them whose data
// is valid JSON; a frame whose data is not valid JSON fails the stream.
type hostValidator struct {
	pending    string
	prevCR     bool
	pendingErr error
	// failedFrame is the frame whose data was not valid JSON.
	failedFrame string
	// failed is set once the plugin has handed the host the failing frame;
	// from then on the host fails the stream, and chunks pass as they came.
	failed bool
}

// add is the host's AddChunk: a frame whose data is not valid JSON fails
// the call, unless frames before it in the same chunk passed, in which
// case those pass and the next call fails.
func (v *hostValidator) add(chunk string) (string, error) {
	if v.pendingErr != nil {
		errPending := v.pendingErr
		v.pendingErr = nil
		return "", errPending
	}
	if chunk == "" {
		return "", nil
	}
	if v.prevCR {
		chunk = strings.TrimPrefix(chunk, "\n")
		v.prevCR = false
	}
	if chunk == "" {
		return "", nil
	}
	v.prevCR = strings.HasSuffix(chunk, "\r")
	chunk = strings.ReplaceAll(strings.ReplaceAll(chunk, "\r\n", "\n"), "\r", "\n")
	if v.pending != "" && !strings.HasSuffix(v.pending, "\n") && !strings.HasPrefix(chunk, "\n") {
		first, _, _ := strings.Cut(chunk, "\n")
		first = strings.TrimSpace(first)
		if strings.HasPrefix(first, "data:") || strings.HasPrefix(first, "event:") {
			v.pending += "\n"
		}
	}
	v.pending += chunk
	var out strings.Builder
	for {
		end := strings.Index(v.pending, "\n\n")
		if end < 0 {
			break
		}
		frame := v.pending[:end+2]
		if !sseDataLinesValid(frame) {
			v.failedFrame = frame
			if out.Len() > 0 {
				v.pending = ""
				v.pendingErr = errInvalidFrame
				return out.String(), nil
			}
			return "", errInvalidFrame
		}
		out.WriteString(frame)
		v.pending = v.pending[end+2:]
	}
	if strings.TrimSpace(v.pending) == "" {
		v.pending = ""
		return out.String(), nil
	}
	if sseDataLinesValid(v.pending) {
		out.WriteString(v.pending)
		v.pending = ""
	}
	return out.String(), nil
}

var errInvalidFrame = errors.New("invalid SSE data JSON")

// releaseFrames sends the frames at the head of the queue that no longer
// wait. Bare chat chunks sent together are joined as the chat handler
// writes one chunk: "data: " before and a blank line after each.
func (st *requestState) releaseFrames(bare bool) string {
	q := &st.stream
	var parts []string
	n := 0
	for n < len(q.frames) && q.frames[n].waiting == 0 {
		parts = append(parts, q.frames[n].render())
		n++
	}
	q.frames = q.frames[n:]
	if bare && len(parts) > 1 {
		return mergeChatChunks(parts)
	}
	return strings.Join(parts, "")
}

// mergeChatChunks makes one chat completion chunk of several that go out
// together: the chat handler reads one JSON object per chunk (its
// finish_reason among others). The choices' deltas merge as an OpenAI
// client accumulates deltas (mergeDelta), so a client that accumulates the
// merged chunk ends with the same message as one that accumulates the
// chunks one by one. The last usage wins; other top-level fields come from
// the first chunk.
func mergeChatChunks(parts []string) string {
	var merged map[string]any
	for _, part := range parts {
		var chunk map[string]any
		if json.Unmarshal([]byte(part), &chunk) != nil {
			return strings.Join(parts, "")
		}
		if merged == nil {
			merged = chunk
			continue
		}
		for key, value := range chunk {
			switch key {
			case "choices":
				merged[key] = mergeDelta(merged[key], value)
			case "usage":
				if value != nil {
					merged[key] = value
				}
			default:
				if _, ok := merged[key]; !ok {
					merged[key] = value
				}
			}
		}
	}
	if merged == nil {
		return strings.Join(parts, "")
	}
	var b bytes.Buffer
	encoder := json.NewEncoder(&b)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(merged) != nil {
		return strings.Join(parts, "")
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// mergeDelta merges delta into acc as an OpenAI client accumulates stream
// deltas: strings concatenate; objects merge field by field; arrays of
// objects merge by their index field (an entry with a new index is added),
// other arrays extend; index, type and finish_reason take the later value;
// null never
// replaces a value; other scalars take the later value. It associates, so
// merging a run of deltas first changes nothing for the client.
func mergeDelta(acc, delta any) any {
	if acc == nil {
		return delta
	}
	if delta == nil {
		return acc
	}
	switch a := acc.(type) {
	case string:
		if d, ok := delta.(string); ok {
			return a + d
		}
	case map[string]any:
		d, ok := delta.(map[string]any)
		if !ok {
			break
		}
		out := make(map[string]any, len(a)+len(d))
		for k, v := range a {
			out[k] = v
		}
		for k, v := range d {
			if k == "index" || k == "type" || k == "finish_reason" {
				if v != nil {
					out[k] = v
				}
				continue
			}
			out[k] = mergeDelta(out[k], v)
		}
		return out
	case []any:
		d, ok := delta.([]any)
		if !ok {
			break
		}
		out := append([]any(nil), a...)
		for _, entry := range d {
			object, isObject := entry.(map[string]any)
			index, hasIndex := object["index"]
			if !isObject || !hasIndex {
				out = append(out, entry)
				continue
			}
			found := false
			for i, existing := range out {
				if e, ok := existing.(map[string]any); ok && e["index"] == index {
					out[i] = mergeDelta(e, object)
					found = true
					break
				}
			}
			if !found {
				out = append(out, object)
			}
		}
		return out
	}
	return delta
}

// queueFrame reads one frame, restores what can be restored in it now,
// holds its deltas that may hold part of a placeholder, resolves the holds
// its event ends, and queues it.
func (st *requestState) queueFrame(frame, suffix string, bare bool, sent string) {
	q := &st.stream
	qf := &queuedFrame{frame: frame, suffix: suffix, sent: sent}
	q.frames = append(q.frames, qf)
	if bare {
		qf.payload = strings.TrimSpace(frame)
		lead := strings.Index(frame, qf.payload)
		qf.offsets = make([]int, len(qf.payload))
		for i := range qf.offsets {
			qf.offsets[i] = lead + i
		}
	} else {
		qf.payload, qf.offsets = payloadMap(frame)
	}
	// The host's check: the data, trimmed, is valid JSON (sseDataLinesValid);
	// it may start with blank data lines or spaces.
	payload := strings.TrimSpace(qf.payload)
	if payload == "" || !json.Valid([]byte(payload)) {
		return
	}
	if lead := strings.Index(qf.payload, payload); lead > 0 {
		qf.offsets = qf.offsets[lead:]
		qf.payload = payload
	}
	root := gjson.Parse(payload)
	// The event's type as the host reads it: the payload type, else the
	// event: name.
	eventType := root.Get("type").String()
	if eventType == "" && !bare {
		eventType = sseEventName(frame)
	}
	incremental := deltaEvent(eventType)
	signed := signedReplyPaths(root)
	rootIdent := identityOf(root)
	walkTokens(payload, func(site tokenSite) {
		if site.key {
			qf.edit(st.restoreToken(site.token, false, true), site.token.Index)
			return
		}
		if signed[site.path] {
			return
		}
		whole := argumentsString(site.steps, site.parent, site.name, eventType) ||
			(st.structured && structuredText(site.steps, site.parent, site.name, eventType))
		key := renderSteps(site.steps)
		continuing := incremental && continuingString(site.steps, eventType)
		switch {
		case continuing && whole:
			st.holdArguments(qf, key, site.steps, site.token)
		case continuing:
			st.holdText(qf, key, site.steps, site.token)
		case whole && (st.overflowedCall(root, site.path) || st.stream.unbounded[key]):
			// Streaming sent this call unrestored; its snapshots match.
		default:
			qf.edit(st.restoreToken(site.token, whole, false), site.token.Index)
		}
	})
	st.resolveEnded(root, eventType, rootIdent, bare, !bare && sseFrameTerminal(frame, st.codexClient))
}

// continuingString reports whether a string of a stream event continues
// across events, so that a placeholder may be split between them: in a
// chat chunk's choice delta, content, reasoning_content, refusal, a tool
// call's function.arguments, function_call.arguments and a reasoning
// block's thinking, signature and data; in a Responses *.delta event, its
// delta. Every other string (a logprob token, an id, a name) is a record
// of its own event, restored within it.
func continuingString(steps []step, eventType string) bool {
	if eventType != "" {
		return strings.HasSuffix(eventType, ".delta") && len(steps) == 2 && steps[1].name == "delta"
	}
	if len(steps) < 5 || steps[1].name != "choices" || !steps[2].elem || steps[3].name != "delta" {
		return false
	}
	rest := steps[4:]
	switch len(rest) {
	case 1:
		switch rest[0].name {
		case "content", "reasoning_content", "refusal":
			return true
		}
	case 2:
		return rest[0].name == "function_call" && rest[1].name == "arguments"
	case 3:
		if rest[0].name == "reasoning_blocks" && rest[1].elem {
			switch rest[2].name {
			case "thinking", "signature", "data":
				return true
			}
		}
	case 4:
		return rest[0].name == "tool_calls" && rest[1].elem && rest[2].name == "function" && rest[3].name == "arguments"
	}
	return false
}

// holdText restores a text delta, or holds it when it ends in what may be
// the start of a placeholder.
func (st *requestState) holdText(qf *queuedFrame, key string, steps []step, token gjson.Result) {
	q := &st.stream
	value := token.String()
	if h := q.holds[key]; h != nil {
		h.sites = append(h.sites, deltaSite{qf, token})
		h.text += value
		qf.waiting++
		if st.partialTail(h.text) == "" && !endsWithOpenerPrefix(h.text) {
			st.resolve(key, h)
		}
		return
	}
	if st.partialTail(value) != "" || endsWithOpenerPrefix(value) {
		q.holds[key] = &deltaHold{steps: steps, sites: []deltaSite{{qf, token}}, text: value}
		qf.waiting++
		return
	}
	qf.edit(st.restoreToken(token, false, false), token.Index)
}

// holdArguments passes a tool call's arguments delta on, or holds it, and
// every later delta of the call, from the first that may hold a
// placeholder.
func (st *requestState) holdArguments(qf *queuedFrame, key string, steps []step, token gjson.Result) {
	q := &st.stream
	if q.unbounded[key] {
		return
	}
	value := token.String()
	if h := q.holds[key]; h != nil {
		h.sites = append(h.sites, deltaSite{qf, token})
		h.text += value
		qf.waiting++
		if len(h.sent)+len(h.text) > maxArgumentsBytes {
			log.WithField("limit_bytes", maxArgumentsBytes).Warn("secret-scrub: streamed tool-call arguments over the limit; sent unrestored")
			st.markUnbounded(key, qf)
			st.release(key, h)
		}
		return
	}
	// The cap first: arguments past it go on as they came, never held.
	sent := q.argumentsSent[key] + value
	if len(sent) > maxArgumentsBytes {
		log.WithField("limit_bytes", maxArgumentsBytes).Warn("secret-scrub: streamed tool-call arguments over the limit; sent unrestored")
		st.markUnbounded(key, qf)
		return
	}
	if containsOpener(value) || endsWithOpenerPrefix(value) {
		q.holds[key] = &deltaHold{arguments: true, steps: steps, sites: []deltaSite{{qf, token}}, text: value, sent: q.argumentsSent[key]}
		qf.waiting++
		return
	}
	q.argumentsSent[key] = sent
}

// maxEscapeDepth is how many levels of JSON escaping a placeholder opener
// is recognized at.
const maxEscapeDepth = 4

// openerEncodings are the ways a placeholder's "<" is written, generated
// level by level: text one level down becomes JSON string content one
// level up, where "<" may be written as is or as \u003c / \u003C, and a
// backslash as \\, \u005c or \u005C. Every form, at every depth up to
// maxEscapeDepth, is in the list.
var openerEncodings = func() []string {
	level := []string{"<"}
	all := []string{"<"}
	seen := map[string]bool{"<": true}
	for depth := 1; depth <= maxEscapeDepth; depth++ {
		var next []string
		for _, text := range level {
			for _, encoded := range encodeEveryWay(text) {
				if !seen[encoded] {
					seen[encoded] = true
					next = append(next, encoded)
					all = append(all, encoded)
				}
			}
		}
		level = next
	}
	return all
}()

// encodeEveryWay is every JSON string content that decodes to text, where
// only "<" and backslashes take escaped forms.
func encodeEveryWay(text string) []string {
	out := []string{""}
	for _, r := range text {
		var options []string
		switch r {
		case '<':
			options = []string{"<", `\u003c`, `\u003C`}
		case '\\':
			options = []string{`\\`, `\u005c`, `\u005C`}
		default:
			options = []string{string(r)}
		}
		next := make([]string, 0, len(out)*len(options))
		for _, prefix := range out {
			for _, option := range options {
				next = append(next, prefix+option)
			}
		}
		out = next
	}
	return out
}

// openerPrefixes are the proper prefixes of every opener encoding: what a
// split may leave at the end of a delta.
var openerPrefixes, longestOpener = func() (map[string]bool, int) {
	prefixes := map[string]bool{}
	longest := 0
	for _, encoding := range openerEncodings {
		longest = max(longest, len(encoding))
		for n := 1; n < len(encoding); n++ {
			prefixes[encoding[:n]] = true
		}
	}
	return prefixes, longest
}()

// containsOpener reports whether text holds a placeholder opener in any
// encoding (each holds "<" or u003c in either case), or a placeholder's
// name.
func containsOpener(text string) bool {
	return strings.Contains(text, "<") || strings.Contains(text, "u003c") || strings.Contains(text, "u003C") ||
		strings.Contains(text, "ECRET_")
}

// endsWithOpenerPrefix reports whether text ends in a proper prefix of any
// encoding of a placeholder opener.
func endsWithOpenerPrefix(text string) bool {
	for n := 1; n < longestOpener && n <= len(text); n++ {
		if openerPrefixes[text[len(text)-n:]] {
			return true
		}
	}
	return false
}

// markUnbounded records a call whose arguments go on unrestored, also by
// the Responses item, so its snapshots stay unrestored too.
func (st *requestState) markUnbounded(key string, qf *queuedFrame) {
	st.stream.unbounded[key] = true
	delete(st.stream.argumentsSent, key)
	if item := gjson.Get(qf.payload, "item_id").String(); item != "" {
		st.overflowItems[item] = true
	}
	if output := gjson.Get(qf.payload, "output_index"); output.Exists() {
		st.overflowOutputs[output.Int()] = true
	}
}

// resolve settles a hold: the restored text goes in its first delta and
// the others carry none; when nothing was restored, every delta goes on as
// it came.
func (st *requestState) resolve(key string, h *deltaHold) {
	var restored string
	if h.arguments {
		whole := h.sent + h.text
		restored = st.restoreArguments(whole)
		if restored != whole && !strings.HasPrefix(restored, h.sent) {
			log.Warn("secret-scrub: restored tool-call arguments no longer start as sent; placeholders left")
			restored = whole
		}
		restored = restored[len(h.sent):]
	} else {
		restored = st.restoreText(h.text)
	}
	if restored != h.text {
		// Only the changed end of each delta's string is written again.
		for i, site := range h.sites {
			value := ""
			if i == 0 {
				value = restored
			}
			site.frame.edit(replaceToken(site.token.Raw, value), site.token.Index)
		}
	}
	st.release(key, h)
}

// release lets a hold's frames go.
func (st *requestState) release(key string, h *deltaHold) {
	for _, site := range h.sites {
		site.frame.waiting--
	}
	delete(st.stream.holds, key)
	if h.arguments {
		delete(st.stream.argumentsSent, key)
	}
}

// resolveEnded settles the holds whose string the event ends: the reply's
// end, as the host classifies the frame (sseFrameTerminal: its event:
// name, payload type or error envelope), settles all; a Responses part or
// item end (*.done) settles the holds of the same item, part and output
// index; a chat choice that finishes settles the holds of that choice.
func (st *requestState) resolveEnded(root gjson.Result, eventType string, ident []idField, bare, terminal bool) {
	q := &st.stream
	var ended []string
	switch {
	case bare:
		pos := 0
		root.Get("choices").ForEach(func(_, item gjson.Result) bool {
			if reason := item.Get("finish_reason"); reason.Type == gjson.String && reason.String() != "" {
				choice := renderSteps([]step{{elem: true, pos: pos, ident: identityOf(item)}})
				for key, h := range q.holds {
					if len(h.steps) >= 3 && h.steps[1].name == "choices" && renderSteps(h.steps[2:3]) == choice {
						ended = append(ended, key)
					}
				}
			}
			pos++
			return true
		})
	case terminal:
		for key := range q.holds {
			ended = append(ended, key)
		}
	case strings.HasSuffix(eventType, ".done"):
		for key, h := range q.holds {
			if identityMatches(ident, h.steps[0].ident) {
				ended = append(ended, key)
			}
		}
	}
	for _, key := range ended {
		if h := q.holds[key]; h != nil {
			st.resolve(key, h)
		}
	}
}
