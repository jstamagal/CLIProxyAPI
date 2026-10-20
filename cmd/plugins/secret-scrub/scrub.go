package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// Formats the plugin scrubs and restores: OpenAI chat completions and the
// OpenAI Responses API, in requests, replies and stream chunks.
const (
	formatChat      = "openai"
	formatResponses = "openai-response"
)

func supportedFormat(format string) bool {
	return format == formatChat || format == formatResponses
}

// placeholderPattern matches a placeholder in a decoded string.
var placeholderPattern = regexp.MustCompile(`<SECRET_([0-9a-f]{8})>`)

// writtenPlaceholderPattern matches a placeholder as it can appear in text
// that holds JSON: any of its characters may be written as a \u00XX
// escape.
var writtenPlaceholderPattern = regexp.MustCompile(escapable("<SECRET_") + `((?:[0-9a-f]|` + `\\` + `u003[0-9]|` + `\\` + `u006[1-6]){8})` + escapable(">"))

// escapable is a pattern for text whose characters may each be written as
// a \u00XX escape.
func escapable(text string) string {
	var b strings.Builder
	for i := 0; i < len(text); i++ {
		code := fmt.Sprintf("%02x", text[i])
		b.WriteString(`(?:` + regexp.QuoteMeta(text[i:i+1]) + `|` + `\\` + `u00` + caseless(code) + `)`)
	}
	return b.String()
}

func caseless(hexDigits string) string {
	var b strings.Builder
	for _, c := range hexDigits {
		if c >= 'a' && c <= 'f' {
			b.WriteString("[" + string(c) + strings.ToUpper(string(c)) + "]")
		} else {
			b.WriteRune(c)
		}
	}
	return b.String()
}

// keyShapes are known secret formats. Each match is a secret.
var keyShapes = []*regexp.Regexp{
	regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`),
	regexp.MustCompile(`sk-ant-[A-Za-z0-9_\-]{20,}`),
	regexp.MustCompile(`sk-[A-Za-z0-9_\-]{20,}`),
	regexp.MustCompile(`(?:AKIA|ASIA)[0-9A-Z]{16}\b`),
	regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{36,}`),
	regexp.MustCompile(`github_pat_[A-Za-z0-9_]{22,}`),
	regexp.MustCompile(`xox[abprs]-[A-Za-z0-9\-]{10,}`),
	regexp.MustCompile(`AIza[0-9A-Za-z_\-]{35}`),
	regexp.MustCompile(`hf_[A-Za-z0-9]{30,}`),
	regexp.MustCompile(`glpat-[A-Za-z0-9_\-]{20,}`),
}

// envAssignment is a .env style NAME=value whose name holds KEY, TOKEN,
// SECRET, PASSWORD, PASSWD, PASS or AUTH, case-insensitively. The value is group 2 (double
// quoted), 3 (single quoted) or 4 (bare). A quoted value may span lines and
// runs to the first quote not escaped by a backslash. A bare value runs to
// the end of the line; dotenvValue cuts its comment.
var envAssignment = regexp.MustCompile(`(?:^|[^A-Za-z0-9_])([A-Za-z0-9_]*(?i:KEY|TOKEN|SECRET|PASSWORD|PASSWD|PASS|AUTH)[A-Za-z0-9_]*)[ \t]*=[ \t]*(?:"((?:[^"\\]|\\[\s\S])+)"|'((?:[^'\\]|\\[\s\S])+)'|([^\n]*))`)

// envComment starts a comment in a bare .env value: # after whitespace.
var envComment = regexp.MustCompile(`[ \t]#`)

// variableReference is a bare value that only names another variable:
// $NAME or ${NAME}. ${NAME:-fallback} and the like hold a literal and are
// scrubbed.
var variableReference = regexp.MustCompile(`^\$(?:[A-Za-z_][A-Za-z0-9_]*|\{[A-Za-z_][A-Za-z0-9_]*\})$`)

// candidateRun is a maximal run of non-whitespace, the unit the
// high-entropy rule looks at.
var candidateRun = regexp.MustCompile(`\S+`)

// assignmentPrefix is a NAME= in front of a value in one run; the = is
// followed by more than padding.
var assignmentPrefix = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=[^=]`)

// lowerWord is a path segment: lower case letters, - and _ only.
var lowerWord = regexp.MustCompile(`^[a-z_\-]{4,}$`)

const (
	entropyMinBits = 4.0
	// minTokenChars is the shortest run the high-entropy rule considers,
	// padding included.
	minTokenChars = 24
	// blobMinChars is the length from which a string that is all base64 is
	// file data.
	blobMinChars = 512
	// tailWindow is how far back from the end of a stream string a split
	// placeholder can start.
	tailWindow = 128
)

// maxNestedBytes bounds the JSON documents held in strings that one
// request or one tool call's arguments may open, at any depth.
var maxNestedBytes = 64 << 20

// maxArgumentsBytes bounds one streamed tool call's buffered arguments.
// Arguments beyond it go to the client unrestored.
var maxArgumentsBytes = 64 << 20

// identityFields name the object fields that tell parallel streams apart:
// a choice, a tool call, an output item, a content part, a summary part.
var identityFields = []string{"index", "output_index", "content_index", "item_id", "summary_index"}

type secretEntry struct {
	value string
	seen  time.Time
}

// idField is one identity field of an object: key is its decoded value,
// what identities are compared by ("m1" and "m\u0031" are one id); raw is
// its JSON text, for writing it back.
type idField struct{ name, key, raw string }

// step is one step of a logical path through a stream event: an object key,
// an array position, or an array element picked by its identity.
type step struct {
	name  string
	elem  bool
	pos   int
	ident []idField // identity of the object this step reaches
}

// requestState is what one scrubbed request may restore and what its stream
// holds back.
type requestState struct {
	mu      sync.Mutex
	touched time.Time
	values  map[string]string // placeholder id -> value found in this request
	stream  streamQueue       // a streamed reply's frames, held and passed on in order
	// overflowItems are the Responses items whose streamed arguments outgrew
	// the buffer; their snapshots are left unrestored too.
	overflowItems map[string]bool
	// overflowOutputs are the Responses output indexes of those items.
	overflowOutputs map[int64]bool
	// structured is set when the request asked for JSON output; its message
	// text is then restored like tool-call arguments.
	structured bool
	// sse is set once the stream is SSE (Responses), not bare chat chunks;
	// sseKnown once that is decided. framer follows the host's framing of
	// an SSE stream.
	sse      bool
	sseKnown bool
	framer   hostFramer
	// codexClient is set when the client is an official Codex client, for
	// which the host keeps most codex.* events.
	codexClient bool
	// validator follows the host's validation of the stream, which comes
	// before its framing.
	validator hostValidator
}

type scrubber struct {
	mu       sync.Mutex
	key      []byte
	ttl      time.Duration
	entropy  bool
	now      func() time.Time
	id       func(key []byte, value string, attempt int) string
	values   map[string]*secretEntry  // placeholder id -> value
	requests map[string]*requestState // request ID -> state
	byValue  map[string]string        // value -> placeholder id
	// scrubbing holds the request IDs whose Scrub is running; End removes
	// one, so a Scrub that finishes after its request ended keeps no state.
	scrubbing map[string]bool
	// afterScan, when set, runs between a Scrub's scan and its recording.
	afterScan func(requestID string)
	stop      chan struct{} // closes the running sweeper
}

func newScrubber() *scrubber {
	key := make([]byte, 32)
	if _, errRead := rand.Read(key); errRead != nil {
		panic(errRead)
	}
	return &scrubber{
		key:      key,
		ttl:      24 * time.Hour,
		entropy:  true,
		now:      time.Now,
		id:       hmacID,
		values:   make(map[string]*secretEntry),
		requests: make(map[string]*requestState),
		byValue:  make(map[string]string),

		scrubbing: make(map[string]bool),
	}
}

// hmacID is the placeholder id of value: the first 8 hex digits of its HMAC.
// A later attempt, used when an earlier id belongs to another value, adds
// the attempt number to the HMAC input.
func hmacID(key []byte, value string, attempt int) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(value))
	if attempt > 0 {
		mac.Write([]byte{0})
		mac.Write([]byte(strconv.Itoa(attempt)))
	}
	return hex.EncodeToString(mac.Sum(nil))[:8]
}

// placeholderID returns the id of value: the id it already has while it is
// remembered, else the first candidate no other remembered value holds.
func (s *scrubber) placeholderID(value string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if id, ok := s.byValue[value]; ok {
		s.values[id].seen = now
		return id
	}
	for attempt := 0; ; attempt++ {
		id := s.id(s.key, value, attempt)
		if s.values[id] == nil {
			s.values[id] = &secretEntry{value: value, seen: now}
			s.byValue[value] = id
			return id
		}
	}
}

type span struct{ start, end int }

// findSecrets returns the spans of text that are secrets, sorted; spans
// that overlap are merged into their union. text is a decoded string. A
// span that is only placeholders this plugin issued is not a secret.
// entropy switches the high-entropy rule.
func findSecrets(text string, entropy bool) []span {
	var found []span
	for _, shape := range keyShapes {
		for _, loc := range shape.FindAllStringIndex(text, -1) {
			found = append(found, span{loc[0], loc[1]})
		}
	}
	for _, loc := range envAssignment.FindAllStringSubmatchIndex(text, -1) {
		for group := 2; group <= 4; group++ {
			start, end := loc[2*group], loc[2*group+1]
			if start < 0 {
				continue
			}
			if group == 4 {
				end = start + len(dotenvValue(text[start:end]))
				if end == start || variableReference.MatchString(text[start:end]) {
					break
				}
			}
			found = append(found, span{start, end})
			break
		}
	}
	if entropy {
		for _, loc := range candidateRun.FindAllStringIndex(text, -1) {
			start, end := trimRun(text, loc[0], loc[1])
			if utf8.RuneCountInString(text[start:end]) < minTokenChars || looksLikePath(text[start:end]) {
				continue
			}
			if prefix := assignmentPrefix.FindString(text[start:end]); prefix != "" {
				start += len(prefix) - 1
			}
			if utf8.RuneCountInString(text[start:end]) >= minTokenChars && highEntropy(text[start:end]) {
				found = append(found, span{start, end})
			}
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].start < found[j].start })
	var merged []span
	for _, sp := range found {
		if len(merged) > 0 && sp.start < merged[len(merged)-1].end {
			last := &merged[len(merged)-1]
			last.end = max(last.end, sp.end)
			continue
		}
		merged = append(merged, sp)
	}
	out := merged[:0]
	for _, sp := range merged {
		if placeholderPattern.ReplaceAllString(text[sp.start:sp.end], "") != "" {
			out = append(out, sp)
		}
	}
	return out
}

// dotenvValue is a bare .env value as dotenv reads it: up to a # that
// follows whitespace, without trailing whitespace.
func dotenvValue(raw string) string {
	if loc := envComment.FindStringIndex(raw); loc != nil {
		raw = raw[:loc[0]]
	}
	return strings.TrimRight(raw, " \t\r")
}

// trimRun narrows text[start:end] by the quotes and brackets around it and
// the sentence punctuation after it.
func trimRun(text string, start, end int) (int, int) {
	for start < end && strings.IndexByte("\"'`([{<", text[start]) >= 0 {
		start++
	}
	for end > start && strings.IndexByte("\"'`)]}>.,;:!?", text[end-1]) >= 0 {
		end--
	}
	return start, end
}

// looksLikePath reports whether a run with / in it is a file path or URL:
// it starts with /, ./, ../ or ~/, holds ://, or has a segment that is a
// lower case word.
func looksLikePath(run string) bool {
	if !strings.Contains(run, "/") {
		return false
	}
	for _, prefix := range []string{"/", "./", "../", "~/"} {
		if strings.HasPrefix(run, prefix) {
			return true
		}
	}
	if strings.Contains(run, "://") {
		return true
	}
	for _, segment := range strings.Split(run, "/") {
		if lowerWord.MatchString(segment) {
			return true
		}
	}
	return false
}

// highEntropy reports whether token looks like a random credential: enough
// entropy per character, counted in runes. Hex digests and UUIDs stay under
// the threshold.
func highEntropy(token string) bool {
	counts := make(map[rune]int)
	runes := 0
	for _, r := range token {
		counts[r]++
		runes++
	}
	bits := 0.0
	n := float64(runes)
	for _, c := range counts {
		p := float64(c) / n
		bits -= p * math.Log2(p)
	}
	return bits >= entropyMinBits
}

// isBlob reports whether s is file data: a data URI (image_url, input_image,
// file_data) or a long run of base64. File data is searched with the key
// shape and .env rules but not the high-entropy rule, which random base64
// would always meet.
func isBlob(s string) bool {
	head := s
	if len(head) > 256 {
		head = head[:256]
	}
	if strings.HasPrefix(head, "data:") && strings.Contains(head, ";base64,") {
		return true
	}
	return len(s) >= blobMinChars && isBase64(s)
}

func isBase64(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '+' || c == '/' || c == '=' || c == '-' || c == '_' || c == '\n' || c == '\r':
		default:
			return false
		}
	}
	return true
}

// structuredOutput reports whether a request asks for JSON output: chat
// response_format, or Responses text.format, of type json_object or
// json_schema.
func structuredOutput(root gjson.Result) bool {
	for _, path := range []string{"response_format.type", "text.format.type"} {
		switch root.Get(path).String() {
		case "json_object", "json_schema":
			return true
		}
	}
	return false
}

// signedRequestPaths are the location paths of the strings in a request that
// carry signed model reasoning: the thinking, signature and data of each
// block in an assistant message's reasoning_blocks (chat completions), and
// the encrypted_content of each reasoning input item (Responses). Reasoning
// comes from upstream, which only ever saw placeholders, and a signature
// covers the exact bytes, so these are neither scrubbed nor restored.
func signedRequestPaths(root gjson.Result) map[string]bool {
	paths := make(map[string]bool)
	index := 0
	root.Get("messages").ForEach(func(_, message gjson.Result) bool {
		if message.Get("role").String() == "assistant" {
			addBlockPaths(paths, "messages."+strconv.Itoa(index)+".reasoning_blocks", message.Get("reasoning_blocks"))
		}
		index++
		return true
	})
	index = 0
	root.Get("input").ForEach(func(_, item gjson.Result) bool {
		if item.Get("type").String() == "reasoning" {
			paths["input."+strconv.Itoa(index)+".encrypted_content"] = true
		}
		index++
		return true
	})
	return paths
}

func addBlockPaths(paths map[string]bool, base string, blocks gjson.Result) {
	position := 0
	blocks.ForEach(func(_, block gjson.Result) bool {
		for _, field := range []string{"thinking", "signature", "data"} {
			paths[base+"."+strconv.Itoa(position)+"."+field] = true
		}
		position++
		return true
	})
}

// signedReplyPaths are the location paths of signed reasoning in a reply
// document: each choice's message or delta reasoning_blocks (chat
// completions), and the encrypted_content of every reasoning item
// (Responses, wherever the item sits in the document).
func signedReplyPaths(root gjson.Result) map[string]bool {
	paths := make(map[string]bool)
	index := 0
	root.Get("choices").ForEach(func(_, choice gjson.Result) bool {
		for _, holder := range []string{"message", "delta"} {
			base := "choices." + strconv.Itoa(index) + "." + holder + ".reasoning_blocks"
			addBlockPaths(paths, base, choice.Get(holder+".reasoning_blocks"))
		}
		index++
		return true
	})
	var walk func(path string, value gjson.Result)
	walk = func(path string, value gjson.Result) {
		switch {
		case value.IsObject():
			if value.Get("type").String() == "reasoning" {
				paths[joinPath(path, "encrypted_content")] = true
			}
			value.ForEach(func(k, child gjson.Result) bool {
				walk(joinPath(path, escapePathKey(k.String())), child)
				return true
			})
		case value.IsArray():
			i := 0
			value.ForEach(func(_, child gjson.Result) bool {
				walk(joinPath(path, strconv.Itoa(i)), child)
				i++
				return true
			})
		}
	}
	walk("", root)
	return paths
}

func joinPath(path, part string) string {
	if path == "" {
		return part
	}
	return path + "." + part
}

func escapePathKey(key string) string {
	replacer := strings.NewReplacer(`\`, `\\`, ".", `\.`, "*", `\*`, "?", `\?`, "|", `\|`, "#", `\#`, "@", `\@`)
	return replacer.Replace(key)
}

// Scrub replaces every secret in body, the JSON request requestID, with its
// placeholder, and records the values that request's reply may restore.
// Secrets are searched in each decoded JSON string and object key, so the
// value hashed is the value, whatever the JSON escaping. A body that is not
// JSON is returned as it is.
func (s *scrubber) Scrub(requestID string, body []byte) []byte {
	out, _ := s.ScrubChecked(requestID, body)
	return out
}

// scrubEdits are the edits that put a placeholder in place of each secret
// in decoded text, recording the values found. File data skips the
// high-entropy rule.
func (s *scrubber) scrubEdits(decoded string, entropy bool, found map[string]string) []textEdit {
	spans := findSecrets(decoded, entropy && !isBlob(decoded))
	edits := make([]textEdit, 0, len(spans))
	for _, sp := range spans {
		// A copy, so the remembered value does not keep the request's
		// buffer alive.
		value := strings.Clone(decoded[sp.start:sp.end])
		id := s.placeholderID(value)
		found[id] = value
		edits = append(edits, textEdit{sp.start, sp.end, "<SECRET_" + id + ">"})
	}
	return edits
}

// errUnscrubbable is a request body the scrubber cannot read whole: not
// JSON, or JSON documents held in its strings beyond the size bound.
var errUnscrubbable = errors.New("request body cannot be scrubbed whole")

// ScrubChecked is Scrub, with errUnscrubbable when the body cannot be read
// whole; the body returned then must not go upstream.
func (s *scrubber) ScrubChecked(requestID string, body []byte) ([]byte, error) {
	s.mu.Lock()
	entropy := s.entropy
	s.scrubbing[requestID] = true
	s.mu.Unlock()
	finished := false
	defer func() {
		if !finished {
			s.mu.Lock()
			delete(s.scrubbing, requestID)
			s.mu.Unlock()
		}
	}()

	text := string(body)
	if !gjson.Valid(text) {
		return body, errUnscrubbable
	}
	found := make(map[string]string)
	root := gjson.Parse(text)
	signed := signedRequestPaths(root)
	rewriter := newRewriter(func(decoded string) []textEdit { return s.scrubEdits(decoded, entropy, found) })
	var edits []textEdit
	walkTokens(text, func(site tokenSite) {
		if !site.key && signed[site.path] {
			return
		}
		edits = append(edits, shiftEdits(rewriter.token(site.token.Raw, true), site.token.Index)...)
	})
	if rewriter.failed {
		return body, errUnscrubbable
	}
	out := applyEdits(text, edits)
	if s.afterScan != nil {
		s.afterScan(requestID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	finished = true
	_, live := s.scrubbing[requestID]
	delete(s.scrubbing, requestID)
	if len(found) > 0 && live {
		state := s.requests[requestID]
		if state == nil {
			state = &requestState{
				values: make(map[string]string),

				overflowItems:   make(map[string]bool),
				overflowOutputs: make(map[int64]bool),
			}
			s.requests[requestID] = state
		}
		state.touched = s.now()
		state.structured = structuredOutput(root)
		for id, value := range found {
			state.values[id] = value
		}
	}
	if out == text {
		return body, nil
	}
	return []byte(out), nil
}

// state returns the state of requestID for its reply, or nil when the
// request has nothing to restore. Reply data has now arrived.
func (s *scrubber) state(requestID string) *requestState {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.requests[requestID]
	if state != nil {
		state.touched = s.now()
	}
	return state
}

// Active reports whether requestID has values to restore.
func (s *scrubber) Active(requestID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[requestID] != nil
}

// End forgets requestID. A Scrub of it still running keeps no state when
// it finishes. End of a request that was never scrubbed records nothing.
func (s *scrubber) End(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.requests, requestID)
	delete(s.scrubbing, requestID)
}

// sweepLocked drops values unseen for the TTL, and requests unused for the
// TTL: no Scrub and no reply data. With the sweeper running, every value is
// gone within the TTL plus one sweep interval of its last use, also in a
// library instance the host replaced and keeps loaded.
func (s *scrubber) sweepLocked() {
	cutoff := s.now().Add(-s.ttl)
	for id, entry := range s.values {
		if entry.seen.Before(cutoff) {
			delete(s.values, id)
			delete(s.byValue, entry.value)
		}
	}
	for id, state := range s.requests {
		if state.touched.Before(cutoff) {
			delete(s.requests, id)
		}
	}
}

// Sweep runs one sweep now.
func (s *scrubber) Sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked()
}

// startSweeper runs sweepLocked every interval until stopSweeper, so values
// expire without traffic. A running sweeper is replaced.
func (s *scrubber) startSweeper(interval time.Duration) {
	s.stopSweeper()
	stop := make(chan struct{})
	s.mu.Lock()
	s.stop = stop
	s.mu.Unlock()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				s.mu.Lock()
				s.sweepLocked()
				s.mu.Unlock()
			}
		}
	}()
}

func (s *scrubber) stopSweeper() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop != nil {
		close(s.stop)
		s.stop = nil
	}
}

// Reset stops the sweeper and forgets every value and request.
func (s *scrubber) Reset() {
	s.stopSweeper()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values = make(map[string]*secretEntry)
	s.requests = make(map[string]*requestState)
	s.byValue = make(map[string]string)
	s.scrubbing = make(map[string]bool)
}

// restoreText replaces the placeholders of this request in text, a decoded
// string. A placeholder written as is becomes the value; one written with
// \u00XX escapes sits in JSON text and becomes the value JSON escaped.
func (st *requestState) restoreText(text string) string {
	return applyEdits(text, st.restoreEdits(text))
}

// restoreEdits are the edits that put each placeholder's value in place of
// the placeholder in decoded text.
func (st *requestState) restoreEdits(decoded string) []textEdit {
	if !strings.Contains(decoded, "ECRET_") && !strings.Contains(decoded, `\u00`) {
		return nil
	}
	var edits []textEdit
	for _, loc := range writtenPlaceholderPattern.FindAllStringIndex(decoded, -1) {
		match := decoded[loc[0]:loc[1]]
		if restored := st.replaceWritten(match, false); restored != match {
			edits = append(edits, textEdit{loc[0], loc[1], restored})
		}
	}
	return edits
}

// restoreToken returns the edits, in the token's bytes, that restore one
// string token of a reply. whole is for tool-call arguments (and the text
// of structured output): JSON text restored inside, at every depth; keys
// are opened the same way; other text is restored as text.
func (st *requestState) restoreToken(token gjson.Result, whole, key bool) []textEdit {
	if whole {
		// Sizes are measured on the decoded bytes, here as for streamed
		// deltas and restoreArguments.
		if len(token.String()) > maxArgumentsBytes {
			log.WithField("limit_bytes", maxArgumentsBytes).Warn("secret-scrub: tool-call arguments over the limit; sent unrestored")
			return nil
		}
		if decoded := token.String(); !gjson.Valid(decoded) {
			// Arguments that are not JSON: the one rule restoreArguments
			// uses too (restoreLooseArguments).
			return replaceToken(token.Raw, st.restoreLooseArguments(decoded))
		}
	}
	rewriter := newRewriter(st.restoreEdits)
	edits := rewriter.token(token.Raw, whole || key)
	if rewriter.failed {
		log.Warn("secret-scrub: a reply string could not be read whole; placeholders left")
		return nil
	}
	return edits
}

// replaceWritten replaces the placeholders of this request in text, as is
// or written with escapes. escaped is for text that is itself the inside of
// a JSON string token, where every value is written JSON escaped.
func (st *requestState) replaceWritten(text string, escaped bool) string {
	if !strings.Contains(text, "ECRET_") && !strings.Contains(text, `\u00`) {
		return text
	}
	return writtenPlaceholderPattern.ReplaceAllStringFunc(text, func(match string) string {
		var decoded string
		if errUnmarshal := json.Unmarshal([]byte(`"`+match+`"`), &decoded); errUnmarshal != nil {
			return match
		}
		value, ok := st.values[decoded[len("<SECRET_"):len(decoded)-1]]
		if !ok {
			return match
		}
		if escaped || strings.Contains(match, `\`) {
			return escapeJSON(value)
		}
		return value
	})
}

// restoreArguments returns complete tool-call arguments, decoded JSON
// text, with this request's placeholders replaced byte for byte (see
// spanRewriter). Arguments that are not JSON are restored as text only
// when no placeholder sits inside a quoted string; otherwise, and past the
// size bounds, they are returned as they are.
func (st *requestState) restoreArguments(text string) string {
	if len(text) > maxArgumentsBytes {
		log.WithField("limit_bytes", maxArgumentsBytes).Warn("secret-scrub: tool-call arguments over the limit; sent unrestored")
		return text
	}
	if gjson.Valid(text) {
		// The same entry as a whole token's (spanRewriter.text), so the
		// nested budget is charged the same, streamed or whole.
		rewriter := newRewriter(st.restoreEdits)
		edits := rewriter.text(text, true)
		if rewriter.failed {
			log.Warn("secret-scrub: tool-call arguments too large to restore; placeholders left")
			return text
		}
		return applyEdits(text, edits)
	}
	return st.restoreLooseArguments(text)
}

// placeholderSpot is an issued placeholder found in raw text: its bytes,
// the JSON escaping depth it is written at, whether it lies inside a JSON
// string token, and its value.
type placeholderSpot struct {
	start, end int
	depth      int
	quoted     bool
	value      string
}

// issuedPlaceholders locates this request's placeholders in text, in any
// encoding: at each "<" or backslash it reads the placeholder's characters
// through JSON string unescaping at each depth up to maxEscapeDepth (the
// depths the opener encodings cover), and keeps those that spell an
// issued placeholder. Whether each lies in a JSON string comes from a
// lexical scan of the raw bytes (quotes, and the escapes inside them).
func (st *requestState) issuedPlaceholders(text string) []placeholderSpot {
	quoted := quotedBytes(text)
	var spots []placeholderSpot
	for i := 0; i < len(text); i++ {
		if text[i] != '<' && text[i] != '\\' {
			continue
		}
		for depth := 0; depth <= maxEscapeDepth; depth++ {
			spelled, end, ok := readEscaped(text, i, depth, len("<SECRET_00000000>"))
			if !ok || !placeholderPattern.MatchString(spelled) || len(spelled) != len("<SECRET_00000000>") {
				continue
			}
			value, issued := st.values[spelled[len("<SECRET_"):len(spelled)-1]]
			if !issued {
				continue
			}
			spots = append(spots, placeholderSpot{start: i, end: end, depth: depth, quoted: quoted[i], value: value})
			i = end - 1
			break
		}
	}
	return spots
}

// restoreLooseArguments restores tool-call arguments that are not valid
// JSON: when any issued placeholder lies inside a JSON string, where the
// value's escaping cannot be told, they go on as they are; otherwise each
// placeholder becomes its value, escaped once for each level it was
// written at.
func (st *requestState) restoreLooseArguments(text string) string {
	spots := st.issuedPlaceholders(text)
	if len(spots) == 0 {
		return text
	}
	var edits []textEdit
	for _, spot := range spots {
		if spot.quoted {
			log.Warn("secret-scrub: tool-call arguments are not JSON and hold a placeholder in a string; placeholders left")
			return text
		}
		value := spot.value
		for level := 0; level < spot.depth; level++ {
			value = escapeJSON(value)
		}
		edits = append(edits, textEdit{spot.start, spot.end, value})
	}
	return applyEdits(text, edits)
}

// quotedBytes reports, for each byte of text, whether it lies inside a
// JSON string token: a lexical scan that follows quotes and, inside them,
// backslash escapes.
func quotedBytes(text string) []bool {
	quoted := make([]bool, len(text))
	inside := false
	for i := 0; i < len(text); i++ {
		if !inside {
			if text[i] == '"' {
				inside = true
			}
			continue
		}
		quoted[i] = true
		switch text[i] {
		case '\\':
			if i+1 < len(text) {
				quoted[i+1] = true
				i++
			}
		case '"':
			quoted[i] = false
			inside = false
		}
	}
	return quoted
}

// readEscaped reads count characters of text starting at pos, as they read
// after depth levels of JSON string unescaping, and returns them and where
// they end in text. ok is false when the bytes there are not that many
// characters at that depth.
func readEscaped(text string, pos, depth, count int) (string, int, bool) {
	var out []byte
	for len(out) < count {
		c, next, ok := readAtDepth(text, pos, depth)
		if !ok {
			return "", 0, false
		}
		out = append(out, c)
		pos = next
	}
	return string(out), pos, true
}

// readAtDepth reads one byte of the text that depth levels of unescaping
// make of text from pos. Escapes for characters beyond one byte read as
// 0xff, which no placeholder holds.
func readAtDepth(text string, pos, depth int) (byte, int, bool) {
	if depth == 0 {
		if pos >= len(text) {
			return 0, 0, false
		}
		return text[pos], pos + 1, true
	}
	c, next, ok := readAtDepth(text, pos, depth-1)
	if !ok {
		return 0, 0, false
	}
	if c != '\\' {
		return c, next, true
	}
	e, next, ok := readAtDepth(text, next, depth-1)
	if !ok {
		return 0, 0, false
	}
	switch e {
	case '"', '\\', '/':
		return e, next, true
	case 'b':
		return '\b', next, true
	case 'f':
		return '\f', next, true
	case 'n':
		return '\n', next, true
	case 'r':
		return '\r', next, true
	case 't':
		return '\t', next, true
	case 'u':
		var hex [4]byte
		for k := range hex {
			hex[k], next, ok = readAtDepth(text, next, depth-1)
			if !ok {
				return 0, 0, false
			}
		}
		code, errParse := strconv.ParseUint(string(hex[:]), 16, 32)
		if errParse != nil {
			return 0, 0, false
		}
		if code > 0x7f {
			return 0xff, next, true
		}
		return byte(code), next, true
	}
	return 0, 0, false
}

// partialTail is the longest end of text that is the start, but not the
// whole, of a placeholder of this request, as is or with escapes.
func (st *requestState) partialTail(text string) string {
	for i := max(0, len(text)-tailWindow); i < len(text); i++ {
		if c := text[i]; (c == '<' || c == '\\') && st.placeholderPrefix(text[i:]) {
			return text[i:]
		}
	}
	return ""
}

// placeholderPrefix reports whether s is a proper prefix of a placeholder
// of one of this request's ids, each character as is or as a \u00XX escape.
func (st *requestState) placeholderPrefix(s string) bool {
	for id := range st.values {
		if writtenPrefix(s, "<SECRET_"+id+">") {
			return true
		}
	}
	return false
}

// writtenPrefix reports whether s is a proper prefix of target written with
// any of its characters as \u00XX escapes.
func writtenPrefix(s, target string) bool {
	j := 0
	for i := 0; i < len(target); i++ {
		if j == len(s) {
			return true
		}
		if s[j] == target[i] {
			j++
			continue
		}
		escape := fmt.Sprintf(`\u00%02x`, target[i])
		rest := s[j:]
		if len(rest) < len(escape) {
			return strings.EqualFold(rest, escape[:len(rest)])
		}
		if !strings.EqualFold(rest[:len(escape)], escape) {
			return false
		}
		j += len(escape)
	}
	return false
}

// structuredText reports whether the string at name inside parent of a
// reply event of eventType is model message text: chat message or delta
// content, Responses output text.
func structuredText(steps []step, parent gjson.Result, name, eventType string) bool {
	switch {
	case name == "content" && len(steps) >= 4 && steps[1].name == "choices" &&
		(steps[len(steps)-2].name == "message" || steps[len(steps)-2].name == "delta"):
		return true
	case eventType == "response.output_text.delta":
		return name == "delta"
	case eventType == "response.output_text.done":
		return name == "text"
	}
	return name == "text" && parent.Get("type").String() == "output_text"
}

// argumentsString reports whether the string at name inside parent of a
// reply event of eventType is tool-call arguments.
func argumentsString(steps []step, parent gjson.Result, name, eventType string) bool {
	switch {
	case name == "arguments" && len(steps) >= 2 && (steps[len(steps)-2].name == "function" || steps[len(steps)-2].name == "function_call"):
		return true // chat tool_calls[].function.arguments, legacy function_call.arguments
	case name == "arguments" && parent.Get("type").String() == "function_call":
		return true // Responses function_call item
	case strings.HasPrefix(eventType, "response.function_call_arguments."):
		return name == "delta" || name == "arguments"
	}
	return false
}

// identityKey is the decoded value of an identity field: a string's text,
// a number's value.
func identityKey(field gjson.Result) string {
	switch field.Type {
	case gjson.String:
		return "s:" + field.String()
	case gjson.Number:
		return "n:" + strconv.FormatFloat(field.Float(), 'g', -1, 64)
	}
	return field.Raw
}

func identityOf(object gjson.Result) []idField {
	var ident []idField
	for _, name := range identityFields {
		if field := object.Get(name); field.Exists() && !field.IsObject() && !field.IsArray() {
			ident = append(ident, idField{name: name, key: identityKey(field), raw: field.Raw})
		}
	}
	return ident
}

func renderIdentity(ident []idField) string {
	if len(ident) == 0 {
		return ""
	}
	parts := make([]string, len(ident))
	for i, f := range ident {
		parts[i] = f.name + "=" + f.key
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func renderSteps(steps []step) string {
	var b strings.Builder
	for _, st := range steps {
		b.WriteByte('/')
		switch {
		case st.elem && len(st.ident) > 0:
			b.WriteString("[*]")
		case st.elem:
			b.WriteString("[" + strconv.Itoa(st.pos) + "]")
		default:
			b.WriteString(st.name)
		}
		b.WriteString(renderIdentity(st.ident))
	}
	return b.String()
}

// identityMatches reports whether every field of want has the same value in
// have.
func identityMatches(want, have []idField) bool {
	for _, w := range want {
		matched := false
		for _, h := range have {
			if h.name == w.name && h.key == w.key {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// deltaEvent reports whether a stream event of eventType carries an
// increment of its strings, which may split a placeholder: every chat
// completion chunk, and a Responses *.delta event. Other Responses events
// (*.done, response.completed) are snapshots of the whole text.
func deltaEvent(eventType string) bool {
	return eventType == "" || strings.HasSuffix(eventType, ".delta")
}

// overflowedCall reports whether the whole-JSON string at path of the
// Responses event root belongs to an output item whose streamed text was
// sent unrestored: by the event's item_id or output_index, or by the
// output.N position of a snapshot.
func (st *requestState) overflowedCall(root gjson.Result, path string) bool {
	if len(st.overflowItems) == 0 && len(st.overflowOutputs) == 0 {
		return false
	}
	if st.overflowItems[root.Get("item_id").String()] {
		return true
	}
	if output := root.Get("output_index"); output.Exists() && st.overflowOutputs[output.Int()] {
		return true
	}
	if m := outputPosition.FindStringSubmatch(path); m != nil {
		n, _ := strconv.ParseInt(m[1], 10, 64)
		return st.overflowOutputs[n]
	}
	return false
}

// outputPosition finds the output item of a snapshot path.
var outputPosition = regexp.MustCompile(`^(?:response\.)?output\.(\d+)\.`)

// Restore returns body, the non-streaming reply of requestID, with the
// request's placeholders replaced by their values.
func (s *scrubber) Restore(requestID string, body []byte) []byte {
	state := s.state(requestID)
	if state == nil || !gjson.ValidBytes(body) {
		return body
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	text := string(body)
	root := gjson.Parse(text)
	signed := signedReplyPaths(root)
	eventType := root.Get("type").String()
	var edits []textEdit
	walkTokens(text, func(site tokenSite) {
		if !site.key && signed[site.path] {
			return
		}
		whole := !site.key && (argumentsString(site.steps, site.parent, site.name, eventType) ||
			(state.structured && structuredText(site.steps, site.parent, site.name, eventType)))
		edits = append(edits, shiftEdits(state.restoreToken(site.token, whole, site.key), site.token.Index)...)
	})
	out := applyEdits(text, edits)
	if out == text {
		return body
	}
	return []byte(out)
}

// hostFramer is the host's Responses SSE framer (responsesSSEFramer in
// sdk/api/handlers/openai) as far as framing goes: which text makes one
// frame and when the frame is sent. pending is the text the host holds;
// released is how much of it the plugin has already passed on.
type hostFramer struct {
	pending     string
	released    int
	done        bool
	codexClient bool
}

// write adds one chunk and returns the frames the host sends for it, in
// the host's order and with the host's rules: a data line after a frame of
// only valid data lines starts a new frame; a field line arriving as its
// own chunk gets a line break; a blank line ends a frame; a frame without
// one is sent once it holds an event: and valid data lines. After the
// reply's terminal event the host sends nothing more.
func (f *hostFramer) write(chunk string) []string {
	if chunk == "" || f.done {
		return nil
	}
	var frames []string
	emit := func(frame string) bool {
		frames = append(frames, frame)
		if sseFrameTerminal(frame, f.codexClient) {
			f.done = true
			f.pending, f.released = "", 0
			return true
		}
		return false
	}
	if sseStartsNewDataFrame(f.pending, chunk) {
		pending := f.pending
		f.pending, f.released = "", 0
		if emit(pending) {
			return frames
		}
	}
	if sseNeedsLineBreak(f.pending, chunk) {
		f.pending += "\n"
	}
	f.pending += chunk
	for {
		n := sseFrameLen(f.pending)
		if n == 0 {
			break
		}
		// What was passed on lies in the first frame cut: it has no blank
		// line, or it would have been cut before.
		frame := f.pending[:n]
		f.pending, f.released = f.pending[n:], 0
		if emit(frame) {
			return frames
		}
	}
	if strings.TrimSpace(f.pending) == "" {
		f.pending, f.released = "", 0
		return frames
	}
	if sseCanEmitWithoutDelimiter(f.pending) {
		pending := f.pending
		f.pending, f.released = "", 0
		emit(pending)
	}
	return frames
}

// sseFrameSuffix is what the host's writer adds after a frame that does
// not end with a blank line.
func sseFrameSuffix(frame string) string {
	switch {
	case strings.HasSuffix(frame, "\n\n"), strings.HasSuffix(frame, "\r\n\r\n"):
		return ""
	case strings.HasSuffix(frame, "\r\n"):
		return "\r\n"
	case strings.HasSuffix(frame, "\n"):
		return "\n"
	}
	return "\n\n"
}

func sseFrameLen(text string) int {
	lf := strings.Index(text, "\n\n")
	crlf := strings.Index(text, "\r\n\r\n")
	switch {
	case lf < 0 && crlf < 0:
		return 0
	case lf < 0:
		return crlf + 4
	case crlf < 0 || lf < crlf:
		return lf + 2
	}
	return crlf + 4
}

func sseHasField(text, prefix string) bool {
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			return true
		}
	}
	return false
}

// sseDataPayload is a frame's data lines joined, as SSE joins them.
func sseDataPayload(frame string) (string, bool) {
	var parts []string
	for _, line := range strings.Split(frame, "\n") {
		trimmed := strings.TrimSpace(strings.TrimRight(line, "\r"))
		if strings.HasPrefix(trimmed, "data:") {
			parts = append(parts, strings.TrimSpace(trimmed[len("data:"):]))
		}
	}
	return strings.Join(parts, "\n"), len(parts) > 0
}

func sseDataLinesValid(text string) bool {
	payload, found := sseDataPayload(text)
	if !found {
		return true
	}
	payload = strings.TrimSpace(payload)
	return payload == "" || payload == "[DONE]" || json.Valid([]byte(payload))
}

func sseNeedsMoreData(text string) bool {
	trimmed := strings.TrimSpace(text)
	return trimmed != "" && sseHasField(trimmed, "event:") && !sseHasField(trimmed, "data:")
}

func sseCanEmitWithoutDelimiter(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || sseNeedsMoreData(trimmed) || !sseHasField(trimmed, "event:") || !sseHasField(trimmed, "data:") {
		return false
	}
	return sseDataLinesValid(trimmed)
}

func sseStartsNewDataFrame(pending, chunk string) bool {
	trimmed := strings.TrimSpace(pending)
	if trimmed == "" || sseHasField(trimmed, "event:") || !sseHasField(trimmed, "data:") || !sseDataLinesValid(trimmed) {
		return false
	}
	return strings.HasPrefix(strings.TrimLeft(chunk, " \t\r\n"), "data:")
}

func sseNeedsLineBreak(pending, chunk string) bool {
	if pending == "" || chunk == "" || strings.HasSuffix(pending, "\n") || strings.HasSuffix(pending, "\r") ||
		chunk[0] == '\n' || chunk[0] == '\r' {
		return false
	}
	trimmed := strings.TrimLeft(chunk, " \t")
	if trimmed == "" {
		return false
	}
	for _, prefix := range []string{"data:", "event:", "id:", "retry:", ":"} {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	return false
}

// sseEventName is a frame's event: name.
func sseEventName(frame string) string {
	for _, line := range strings.Split(frame, "\n") {
		trimmed := strings.TrimSpace(strings.TrimRight(line, "\r"))
		if strings.HasPrefix(trimmed, "event:") {
			return strings.TrimSpace(trimmed[len("event:"):])
		}
	}
	return ""
}

// sseFrameTerminal reports whether the host ends the stream at frame: an
// error, or a terminal event named by its event: line or payload type.
func sseFrameTerminal(frame string, codexClient bool) bool {
	payload, ok := sseDataPayload(frame)
	if !ok || payload == "" || payload == "[DONE]" || !json.Valid([]byte(payload)) {
		return false
	}
	streamEvent := sseEventName(frame)
	payloadType := gjson.Get(payload, "type").String()
	if ssePrivateEvent(streamEvent, codexClient) || ssePrivateEvent(payloadType, codexClient) {
		return false
	}
	if sseErrorEvent(payloadType) || ssePayloadHasError(payload) {
		return true
	}
	eventType := payloadType
	if streamTerminal(streamEvent) {
		eventType = streamEvent
	} else if eventType == "" {
		eventType = streamEvent
	}
	return sseErrorEvent(eventType) || streamTerminal(eventType)
}

// ssePrivateEvent reports whether the host filters an event out of the
// stream (shouldFilterPrivateEvent), so it cannot end it: internal
// telemetry always; for an official Codex client only codex.rate_limits,
// for any other client every codex.* event.
func ssePrivateEvent(name string, codexClient bool) bool {
	name = strings.TrimSpace(name)
	if name == "" || sseErrorEvent(name) {
		return false
	}
	if strings.HasPrefix(name, "responsesapi.") || name == "response.subscription_usage" {
		return true
	}
	if codexClient {
		return name == "codex.rate_limits"
	}
	return strings.HasPrefix(name, "codex.")
}

// codexClientRequest is the host's isCodexResponsesClientRequest: the
// client's User-Agent or Originator names an official Codex client.
func codexClientRequest(headers http.Header) bool {
	userAgent := strings.TrimSpace(headers.Get("User-Agent"))
	if strings.HasPrefix(userAgent, "Codex Desktop/") || strings.HasPrefix(userAgent, "codex-tui/") ||
		userAgent == "codex_cli_rs" || strings.HasPrefix(userAgent, "codex_cli_rs/") || strings.HasPrefix(userAgent, "codex_exec/") {
		return true
	}
	switch originator := strings.ToLower(strings.TrimSpace(headers.Get("Originator"))); originator {
	case "codex desktop", "codex-tui", "codex_cli_rs":
		return true
	default:
		return strings.HasPrefix(originator, "codex desktop/") || strings.HasPrefix(originator, "codex-tui/") || strings.HasPrefix(originator, "codex_cli_rs/")
	}
}

// MarkCodexClient records whether requestID comes from an official Codex
// client, which changes which events the host's framer keeps.
func (s *scrubber) MarkCodexClient(requestID string, codex bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.requests[requestID]; state != nil {
		state.codexClient = codex
		state.framer.codexClient = codex
	}
}

// ResetStream starts a new attempt of requestID's stream (the host retries
// a stream that failed before its first delivered chunk): everything the
// earlier attempt held or framed is dropped; the request's secrets stay.
func (s *scrubber) ResetStream(requestID string) {
	state := s.state(requestID)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.stream = streamQueue{}
	state.framer = hostFramer{codexClient: state.codexClient}
	state.validator = hostValidator{}
	state.sse, state.sseKnown = false, false
	state.overflowItems = make(map[string]bool)
	state.overflowOutputs = make(map[int64]bool)
}

func sseErrorEvent(eventType string) bool {
	switch eventType {
	case "response.failed", "response.error", "error":
		return true
	}
	return false
}

func ssePayloadHasError(payload string) bool {
	for _, path := range []string{"error", "response.error"} {
		if result := gjson.Get(payload, path); result.Exists() && result.Type != gjson.Null {
			return true
		}
	}
	return gjson.Get(payload, "code").Exists() && gjson.Get(payload, "message").Exists()
}

// streamTerminal reports whether a Responses event ends the reply. It is
// the host's list of terminal events.
func streamTerminal(eventType string) bool {
	switch eventType {
	case "response.completed", "response.incomplete", "response.failed", "response.done", "response.error", "error":
		return true
	}
	return false
}
