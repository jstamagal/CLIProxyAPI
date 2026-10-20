package main

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

// textEdit replaces text[start:end] with text.
type textEdit struct {
	start, end int
	text       string
}

func applyEdits(text string, edits []textEdit) string {
	sorted := append([]textEdit(nil), edits...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].start > sorted[j].start })
	for _, e := range sorted {
		text = text[:e.start] + e.text + text[e.end:]
	}
	return text
}

func shiftEdits(edits []textEdit, by int) []textEdit {
	for i := range edits {
		edits[i].start += by
		edits[i].end += by
	}
	return edits
}

// substitution returns, for decoded plain text, the spans to replace and
// their replacements, also decoded.
type substitution func(decoded string) []textEdit

// spanRewriter rewrites JSON text byte for byte, in both directions: scrub
// (secret to placeholder) and restore (placeholder to secret) differ only
// in the substitution. Every string token, key or value, is decoded with a
// map from each decoded byte back to its raw bytes; when the decoded text
// is itself a JSON document it is opened in turn, at any depth; at the
// innermost level the substitution names the spans to change. Each span is
// mapped back out through every level, and only its raw bytes are
// replaced, by the replacement JSON escaped once per level. Nothing is
// written again, so every other byte (whitespace, escapes, key order)
// stays as it was.
type spanRewriter struct {
	sub    substitution
	budget int
	// failed is set when a JSON document held in a string outgrew the
	// budget, or a token did not decode as encoding/json decodes it: the
	// text was not read whole.
	failed bool
}

func newRewriter(sub substitution) *spanRewriter {
	return &spanRewriter{sub: sub, budget: maxNestedBytes}
}

// document returns the edits for every string token of a JSON document,
// keys and values, at every depth.
func (r *spanRewriter) document(doc string) []textEdit {
	var edits []textEdit
	eachString(gjson.Parse(doc), func(token gjson.Result) {
		edits = append(edits, shiftEdits(r.token(token.Raw, true), token.Index)...)
	})
	return edits
}

// eachString calls fn for every string token in v, keys included.
func eachString(v gjson.Result, fn func(gjson.Result)) {
	switch {
	case v.IsObject():
		v.ForEach(func(k, child gjson.Result) bool {
			fn(k)
			eachString(child, fn)
			return true
		})
	case v.IsArray():
		v.ForEach(func(_, child gjson.Result) bool {
			eachString(child, fn)
			return true
		})
	case v.Type == gjson.String:
		fn(v)
	}
}

// token returns the edits, in raw's own bytes, for one JSON string token
// (quotes included). nested opens the decoded text when it is a JSON
// document.
func (r *spanRewriter) token(raw string, nested bool) []textEdit {
	if r.failed {
		return nil
	}
	decoded, offsets, ok := decodeToken(raw)
	if !ok {
		r.failed = true
		return nil
	}
	inner := r.text(decoded, nested)
	edits := make([]textEdit, 0, len(inner))
	for _, e := range inner {
		edits = append(edits, textEdit{offsets[e.start], offsets[e.end], escapeJSON(e.text)})
	}
	return edits
}

// text returns the edits for decoded text: those of its strings when it is
// a JSON document (object, array or string) and nested, else the
// substitution's.
func (r *spanRewriter) text(decoded string, nested bool) []textEdit {
	if nested {
		trimmed := strings.TrimSpace(decoded)
		if trimmed != "" && strings.IndexByte("{[\"", trimmed[0]) >= 0 && gjson.Valid(trimmed) {
			if len(decoded) > r.budget {
				r.failed = true
				return nil
			}
			r.budget -= len(decoded)
			lead := strings.Index(decoded, trimmed)
			if trimmed[0] == '"' {
				return shiftEdits(r.token(trimmed, true), lead)
			}
			return shiftEdits(r.document(trimmed), lead)
		}
	}
	return r.sub(decoded)
}

// decodeToken decodes a JSON string token (quotes included) and returns,
// for each decoded byte and for the end, the offset in raw where it
// starts. ok is false when the token does not decode as encoding/json
// decodes it.
func decodeToken(raw string) (string, []int, bool) {
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return "", nil, false
	}
	var b strings.Builder
	offsets := make([]int, 0, len(raw))
	emit := func(text string, at int) {
		for i := 0; i < len(text); i++ {
			offsets = append(offsets, at)
		}
		b.WriteString(text)
	}
	for i := 1; i < len(raw)-1; {
		c := raw[i]
		if c != '\\' {
			_, size := utf8.DecodeRuneInString(raw[i:])
			emit(raw[i:i+size], i)
			i += size
			continue
		}
		if i+1 >= len(raw)-1 {
			return "", nil, false
		}
		switch raw[i+1] {
		case '"', '\\', '/':
			emit(raw[i+1:i+2], i)
			i += 2
		case 'b':
			emit("\b", i)
			i += 2
		case 'f':
			emit("\f", i)
			i += 2
		case 'n':
			emit("\n", i)
			i += 2
		case 'r':
			emit("\r", i)
			i += 2
		case 't':
			emit("\t", i)
			i += 2
		case 'u':
			if i+6 > len(raw)-1 {
				return "", nil, false
			}
			code, errParse := strconv.ParseUint(raw[i+2:i+6], 16, 32)
			if errParse != nil {
				return "", nil, false
			}
			r, width := rune(code), 6
			if utf16.IsSurrogate(r) {
				r = utf8.RuneError
				if i+12 <= len(raw)-1 && raw[i+6] == '\\' && raw[i+7] == 'u' {
					if low, errLow := strconv.ParseUint(raw[i+8:i+12], 16, 32); errLow == nil {
						if pair := utf16.DecodeRune(rune(code), rune(low)); pair != utf8.RuneError {
							r, width = pair, 12
						}
					}
				}
			}
			emit(string(r), i)
			i += width
		default:
			return "", nil, false
		}
	}
	offsets = append(offsets, len(raw)-1)
	decoded := b.String()
	var check string
	if json.Unmarshal([]byte(raw), &check) != nil || check != decoded {
		return "", nil, false
	}
	return decoded, offsets, true
}

// escapeJSON is value as the inside of a JSON string, escaped as
// encoding/json escapes, < > and & left as they are.
func escapeJSON(value string) string {
	var b bytes.Buffer
	encoder := json.NewEncoder(&b)
	encoder.SetEscapeHTML(false)
	if errEncode := encoder.Encode(value); errEncode != nil {
		return value
	}
	out := strings.TrimSuffix(b.String(), "\n")
	return out[1 : len(out)-1]
}

// replaceToken returns the edit, in raw's bytes, that makes the string
// token raw decode to value: only the bytes from the first decoded byte
// that changes to the end of the content are written again.
func replaceToken(raw, value string) []textEdit {
	decoded, offsets, ok := decodeToken(raw)
	if !ok {
		return []textEdit{{0, len(raw), `"` + escapeJSON(value) + `"`}}
	}
	if decoded == value {
		return nil
	}
	k := 0
	for k < len(decoded) && k < len(value) && decoded[k] == value[k] {
		k++
	}
	for k > 0 && k < len(decoded) && !utf8.RuneStart(decoded[k]) {
		k--
	}
	return []textEdit{{offsets[k], len(raw) - 1, escapeJSON(value[k:])}}
}

// tokenSite is a string token found walking a document, with where it sits.
type tokenSite struct {
	token  gjson.Result
	path   string // location path, for matching known locations only
	steps  []step
	parent gjson.Result
	name   string
	key    bool
}

// walkTokens calls visit for every string token at the top level of doc's
// structure, keys and values, with its location.
func walkTokens(doc string, visit func(tokenSite)) {
	root := gjson.Parse(doc)
	var walk func(path string, steps []step, value, parent gjson.Result, name string)
	walk = func(path string, steps []step, value, parent gjson.Result, name string) {
		switch {
		case value.IsObject():
			value.ForEach(func(k, child gjson.Result) bool {
				childPath := joinPath(path, escapePathKey(k.String()))
				next := append(append([]step(nil), steps...), step{name: k.String(), ident: identityOf(child)})
				visit(tokenSite{token: k, path: childPath, steps: next, parent: value, name: k.String(), key: true})
				walk(childPath, next, child, value, k.String())
				return true
			})
		case value.IsArray():
			index := 0
			value.ForEach(func(_, child gjson.Result) bool {
				next := append(append([]step(nil), steps...), step{elem: true, pos: index, ident: identityOf(child)})
				walk(joinPath(path, strconv.Itoa(index)), next, child, value, name)
				index++
				return true
			})
		case value.Type == gjson.String:
			visit(tokenSite{token: value, path: path, steps: steps, parent: parent, name: name})
		}
	}
	walk("", []step{{ident: identityOf(root)}}, root, gjson.Result{}, "")
}

// payloadMap is a frame's data (its data lines' contents, trimmed and
// joined by line breaks, as SSE reads them) with, for each byte, its
// offset in the frame; the joining line breaks have none (-1).
func payloadMap(frame string) (string, []int) {
	var b strings.Builder
	var offsets []int
	found := false
	lineStart := 0
	for _, line := range strings.SplitAfter(frame, "\n") {
		start := lineStart
		lineStart += len(line)
		body := strings.TrimRight(line, "\r\n")
		lead := len(body) - len(strings.TrimLeftFunc(body, unicode.IsSpace))
		trimmed := strings.TrimSpace(body)
		if !strings.HasPrefix(trimmed, "data:") {
			continue
		}
		content := trimmed[len("data:"):]
		contentLead := len(content) - len(strings.TrimLeftFunc(content, unicode.IsSpace))
		content = strings.TrimSpace(content)
		at := start + lead + len("data:") + contentLead
		if found {
			b.WriteByte('\n')
			offsets = append(offsets, -1)
		}
		for i := 0; i < len(content); i++ {
			offsets = append(offsets, at+i)
		}
		b.WriteString(content)
		found = true
	}
	return b.String(), offsets
}
