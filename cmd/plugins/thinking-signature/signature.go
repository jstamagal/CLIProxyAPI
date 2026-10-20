package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	formatOpenAI = "openai"
	formatClaude = "claude"

	// blocksField is the OpenAI-wire field that carries Claude's reasoning
	// blocks, next to reasoning_content.
	blocksField = "reasoning_blocks"

	// blockMarkerEnd ends a piece after the carrier marker.
	blockMarkerEnd = "\x00"
)

// nonceEnv overrides the carrier nonce when set.
const nonceEnv = "CPA_THINKING_SIGNATURE_NONCE"

// nonceFileName is the file, under ~/.cli-proxy-api, that holds the carrier
// nonce. Each loaded copy of this library has its own Go runtime and its own
// environment snapshot, so the nonce is shared through the file system:
// every instance, across reloads and processes, reads the same value, and a
// carrier one instance makes is read by another.
const nonceFileName = "thinking-signature.nonce"

var (
	carrierOnce sync.Once
	nonce       string
	marker      string
)

// carrier returns the process nonce and the marker that starts one JSON
// reasoning-block piece carried through a native translator inside text it
// copies verbatim: thinking text on the way out, an assistant text part on
// the way in. Clients and upstreams never see the nonce, so text they write
// cannot pass for a carrier.
func carrier() (string, string) {
	carrierOnce.Do(func() {
		nonce = processNonce()
		marker = "\x00cpa-reasoning-block:" + nonce + ":"
	})
	return nonce, marker
}

// processNonce returns the nonce in nonceEnv, else the one in the nonce
// file, creating that file with a fresh random value when it is missing.
// When the file cannot be used, the nonce is random for this instance.
func processNonce() string {
	if value := os.Getenv(nonceEnv); value != "" {
		return value
	}
	home, errHome := os.UserHomeDir()
	if errHome != nil {
		log.Warnf("thinking-signature: no home directory for the nonce file: %v", errHome)
		return randomNonce()
	}
	value, errFile := sharedNonce(filepath.Join(home, ".cli-proxy-api", nonceFileName))
	if errFile != nil {
		log.Warnf("thinking-signature: nonce file: %v", errFile)
		return randomNonce()
	}
	return value
}

// sharedNonce reads the nonce at path, or creates the file with a random
// value. The file appears whole or not at all: it is written under a
// temporary name and linked into place, which fails when another instance
// got there first, and then that instance's value is read.
func sharedNonce(path string) (string, error) {
	if value, errRead := readNonce(path); errRead == nil {
		return value, nil
	} else if !errors.Is(errRead, fs.ErrNotExist) {
		return "", errRead
	}
	dir := filepath.Dir(path)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		return "", errMkdir
	}
	temp, errTemp := os.CreateTemp(dir, ".thinking-signature.nonce-*")
	if errTemp != nil {
		return "", errTemp
	}
	defer func() {
		if errRemove := os.Remove(temp.Name()); errRemove != nil && !errors.Is(errRemove, fs.ErrNotExist) {
			log.Warnf("thinking-signature: remove %s: %v", temp.Name(), errRemove)
		}
	}()
	value := randomNonce()
	_, errWrite := temp.WriteString(value + "\n")
	errSync := temp.Sync()
	errClose := temp.Close()
	if errWrite != nil || errSync != nil || errClose != nil {
		return "", errors.Join(errWrite, errSync, errClose)
	}
	if errLink := os.Link(temp.Name(), path); errLink != nil {
		if errors.Is(errLink, fs.ErrExist) {
			return readNonce(path)
		}
		return "", errLink
	}
	return value, nil
}

// readNonce reads the nonce file: 32 hex digits.
func readNonce(path string) (string, error) {
	raw, errRead := os.ReadFile(path)
	if errRead != nil {
		return "", errRead
	}
	value := strings.TrimSpace(string(raw))
	if decoded, errDecode := hex.DecodeString(value); errDecode != nil || len(decoded) != 16 {
		return "", fmt.Errorf("%s does not hold a 128-bit hex nonce", path)
	}
	return value, nil
}

func randomNonce() string {
	raw := make([]byte, 16)
	if _, errRead := rand.Read(raw); errRead != nil {
		panic(errRead)
	}
	return hex.EncodeToString(raw)
}

func marked(piece []byte) string {
	_, blockMarker := carrier()
	return blockMarker + string(piece) + blockMarkerEnd
}

// piece is one reasoning-block fragment: the Claude content block index,
// its type, and the parts of its fields this fragment carries.
type piece struct {
	Index     *int    `json:"index,omitempty"`
	Type      string  `json:"type"`
	Thinking  *string `json:"thinking,omitempty"`
	Signature *string `json:"signature,omitempty"`
	Data      *string `json:"data,omitempty"`
}

func strPtr(s string) *string { return &s }

func encodePiece(p piece) []byte {
	raw, _ := json.Marshal(p)
	return raw
}

// markBlocks rewrites a Claude response before the native translator sees
// it, so every reasoning-block fragment reaches reasoning_content as a
// marked piece: the start of a thinking or redacted_thinking block, each
// thinking_delta and each signature_delta. It returns nil when nothing
// changed.
func markBlocks(body []byte) []byte {
	trimmed := bytes.TrimSpace(body)
	if bytes.HasPrefix(trimmed, []byte("{")) && gjson.GetBytes(trimmed, "content").IsArray() {
		return markMessage(body)
	}
	lines := bytes.Split(body, []byte("\n"))
	changed := false
	for i, line := range lines {
		prefix, payload := []byte(nil), line
		if bytes.HasPrefix(line, []byte("data:")) {
			prefix, payload = []byte("data: "), bytes.TrimSpace(line[len("data:"):])
		}
		if !bytes.HasPrefix(payload, []byte("{")) {
			continue
		}
		event := gjson.ParseBytes(payload)
		index := int(event.Get("index").Int())
		var p *piece
		switch event.Get("type").String() {
		case "content_block_start":
			block := event.Get("content_block")
			switch block.Get("type").String() {
			case "thinking":
				p = &piece{Index: &index, Type: "thinking", Thinking: strPtr(block.Get("thinking").String()), Signature: strPtr(block.Get("signature").String())}
			case "redacted_thinking":
				p = &piece{Index: &index, Type: "redacted_thinking", Data: strPtr(block.Get("data").String())}
			}
		case "content_block_delta":
			delta := event.Get("delta")
			switch delta.Get("type").String() {
			case "thinking_delta":
				p = &piece{Index: &index, Type: "thinking", Thinking: strPtr(delta.Get("thinking").String())}
			case "signature_delta":
				p = &piece{Index: &index, Type: "thinking", Signature: strPtr(delta.Get("signature").String())}
			}
		}
		if p == nil {
			continue
		}
		replacement := []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":""}}`)
		replacement, _ = sjson.SetBytes(replacement, "index", index)
		replacement, _ = sjson.SetBytes(replacement, "delta.thinking", marked(encodePiece(*p)))
		lines[i] = append(prefix, replacement...)
		changed = true
	}
	if !changed {
		return nil
	}
	return bytes.Join(lines, []byte("\n"))
}

// markMessage turns each thinking and redacted_thinking block of a whole
// Claude message into a thinking block whose text is the marked block.
func markMessage(body []byte) []byte {
	out := body
	changed := false
	index := 0
	gjson.GetBytes(body, "content").ForEach(func(_, block gjson.Result) bool {
		i := index
		index++
		var p *piece
		switch block.Get("type").String() {
		case "thinking":
			p = &piece{Index: &i, Type: "thinking", Thinking: strPtr(block.Get("thinking").String()), Signature: strPtr(block.Get("signature").String())}
		case "redacted_thinking":
			p = &piece{Index: &i, Type: "redacted_thinking", Data: strPtr(block.Get("data").String())}
		default:
			return true
		}
		replacement := []byte(`{"type":"thinking","thinking":""}`)
		replacement, _ = sjson.SetBytes(replacement, "thinking", marked(encodePiece(*p)))
		if updated, errSet := sjson.SetRawBytes(out, "content."+strconv.Itoa(i), replacement); errSet == nil {
			out = updated
			changed = true
		}
		return true
	})
	if !changed {
		return nil
	}
	return out
}

// splitPieces splits reasoning text into the plain text around markers and
// the marked pieces, in order.
func splitPieces(reasoning string) (string, []piece) {
	_, blockMarker := carrier()
	var text strings.Builder
	var pieces []piece
	rest := reasoning
	for {
		start := strings.Index(rest, blockMarker)
		if start < 0 {
			text.WriteString(rest)
			break
		}
		text.WriteString(rest[:start])
		body, after, found := strings.Cut(rest[start+len(blockMarker):], blockMarkerEnd)
		var p piece
		if found && json.Unmarshal([]byte(body), &p) == nil {
			pieces = append(pieces, p)
			if p.Thinking != nil {
				text.WriteString(*p.Thinking)
			}
		}
		if !found {
			break
		}
		rest = after
	}
	return text.String(), pieces
}

// mergePieces joins the pieces of each block, by index and in order of first
// appearance, into whole blocks without an index.
func mergePieces(pieces []piece) []piece {
	var merged []piece
	position := make(map[int]int)
	for _, p := range pieces {
		index := -1 - len(merged)
		if p.Index != nil {
			index = *p.Index
		}
		at, ok := position[index]
		if !ok {
			at = len(merged)
			position[index] = at
			block := piece{Type: p.Type}
			if p.Type == "redacted_thinking" {
				block.Data = strPtr("")
			} else {
				block.Thinking, block.Signature = strPtr(""), strPtr("")
			}
			merged = append(merged, block)
		}
		block := &merged[at]
		if p.Thinking != nil && block.Thinking != nil {
			*block.Thinking += *p.Thinking
		}
		if p.Signature != nil && block.Signature != nil {
			*block.Signature += *p.Signature
		}
		if p.Data != nil && block.Data != nil {
			*block.Data += *p.Data
		}
	}
	return merged
}

// liftBlocks rewrites a translated OpenAI response so the marked pieces in
// reasoning_content become reasoning_blocks. A stream delta carries its
// pieces as they are, each with its block index, for the client to join by
// index; a whole message carries the joined blocks. reasoning_content keeps
// the thinking text. It returns nil when nothing changed.
func liftBlocks(body []byte) []byte {
	markerNonce, blockMarker := carrier()
	if !bytes.Contains(body, []byte(markerNonce)) {
		return nil
	}
	lines := bytes.Split(body, []byte("\n"))
	changed := false
	for i, line := range lines {
		prefix, payload := []byte(nil), line
		if bytes.HasPrefix(line, []byte("data:")) {
			prefix, payload = []byte("data: "), bytes.TrimSpace(line[len("data:"):])
		}
		if !bytes.HasPrefix(bytes.TrimSpace(payload), []byte("{")) {
			continue
		}
		updated := payload
		index := 0
		gjson.GetBytes(payload, "choices").ForEach(func(_, choice gjson.Result) bool {
			for _, holder := range []string{"delta", "message"} {
				reasoning := choice.Get(holder + ".reasoning_content")
				if reasoning.Type != gjson.String || !strings.Contains(reasoning.String(), blockMarker) {
					continue
				}
				base := "choices." + strconv.Itoa(index) + "." + holder
				text, pieces := splitPieces(reasoning.String())
				if holder == "message" {
					pieces = mergePieces(pieces)
				}
				if text == "" {
					updated, _ = sjson.DeleteBytes(updated, base+".reasoning_content")
				} else {
					updated, _ = sjson.SetBytes(updated, base+".reasoning_content", text)
				}
				if len(pieces) > 0 {
					raw, _ := json.Marshal(pieces)
					updated, _ = sjson.SetRawBytes(updated, base+"."+blocksField, raw)
				}
			}
			index++
			return true
		})
		if !bytes.Equal(updated, payload) {
			lines[i] = append(prefix, updated...)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return bytes.Join(lines, []byte("\n"))
}

// claudeBlocks is the reasoning_blocks of one assistant message as Claude
// content blocks, copied field for field: thinking blocks with their text
// and signature, redacted_thinking blocks with their data.
func claudeBlocks(blocks gjson.Result) []byte {
	var out [][]byte
	blocks.ForEach(func(_, block gjson.Result) bool {
		var raw []byte
		switch block.Get("type").String() {
		case "thinking":
			raw = []byte(`{"type":"thinking","thinking":"","signature":""}`)
			raw, _ = sjson.SetBytes(raw, "thinking", block.Get("thinking").String())
			raw, _ = sjson.SetBytes(raw, "signature", block.Get("signature").String())
		case "redacted_thinking":
			raw = []byte(`{"type":"redacted_thinking","data":""}`)
			raw, _ = sjson.SetBytes(raw, "data", block.Get("data").String())
		default:
			return true
		}
		out = append(out, raw)
		return true
	})
	if len(out) == 0 {
		return nil
	}
	return append(append([]byte{'['}, bytes.Join(out, []byte{','})...), ']')
}

// prepareRequest rewrites an OpenAI chat request after the upstream is
// chosen. For a Claude upstream, each assistant message's reasoning_blocks
// become a marked text part first in its content, which the native
// translator copies into the Claude message for attachBlocks to turn back
// into blocks. For any other upstream reasoning_blocks is removed. It
// returns nil when nothing changed.
func prepareRequest(body []byte, toClaude bool) []byte {
	messages := gjson.GetBytes(body, "messages")
	if !messages.IsArray() {
		return nil
	}
	out := body
	changed := false
	index := 0
	messages.ForEach(func(_, message gjson.Result) bool {
		base := "messages." + strconv.Itoa(index)
		index++
		blocks := message.Get(blocksField)
		if !blocks.Exists() {
			return true
		}
		if updated, errDelete := sjson.DeleteBytes(out, base+"."+blocksField); errDelete == nil {
			out = updated
			changed = true
		}
		if !toClaude || message.Get("role").String() != "assistant" {
			return true
		}
		claude := claudeBlocks(blocks)
		if claude == nil {
			return true
		}
		// The blocks are the reasoning; a translator that turns
		// reasoning_content into a thinking block of its own (is-compat)
		// would send it twice.
		if updated, errDelete := sjson.DeleteBytes(out, base+".reasoning_content"); errDelete == nil {
			out = updated
		}
		part := []byte(`{"type":"text","text":""}`)
		part, _ = sjson.SetBytes(part, "text", marked(claude))
		parts := [][]byte{part}
		content := message.Get("content")
		switch {
		case content.IsArray():
			content.ForEach(func(_, existing gjson.Result) bool {
				parts = append(parts, []byte(existing.Raw))
				return true
			})
		case content.Type == gjson.String && content.String() != "":
			text := []byte(`{"type":"text","text":""}`)
			text, _ = sjson.SetBytes(text, "text", content.String())
			parts = append(parts, text)
		}
		joined := append(append([]byte{'['}, bytes.Join(parts, []byte{','})...), ']')
		if updated, errSet := sjson.SetRawBytes(out, base+".content", joined); errSet == nil {
			out = updated
		}
		return true
	})
	if !changed {
		return nil
	}
	return out
}

// attachBlocks replaces each marked text block of a translated Claude
// request with the reasoning blocks it carries, in place. It returns nil
// when nothing changed.
func attachBlocks(body []byte) []byte {
	markerNonce, blockMarker := carrier()
	if !bytes.Contains(body, []byte(markerNonce)) {
		return nil
	}
	out := body
	changed := false
	index := 0
	gjson.GetBytes(body, "messages").ForEach(func(_, message gjson.Result) bool {
		base := "messages." + strconv.Itoa(index)
		index++
		content := message.Get("content")
		if !content.IsArray() {
			return true
		}
		var blocks [][]byte
		found := false
		content.ForEach(func(_, block gjson.Result) bool {
			text := block.Get("text").String()
			if block.Get("type").String() == "text" && strings.HasPrefix(text, blockMarker) && strings.HasSuffix(text, blockMarkerEnd) {
				found = true
				inner := gjson.Parse(strings.TrimSuffix(strings.TrimPrefix(text, blockMarker), blockMarkerEnd))
				inner.ForEach(func(_, b gjson.Result) bool {
					blocks = append(blocks, []byte(b.Raw))
					return true
				})
				return true
			}
			blocks = append(blocks, []byte(block.Raw))
			return true
		})
		if !found {
			return true
		}
		joined := append(append([]byte{'['}, bytes.Join(blocks, []byte{','})...), ']')
		if updated, errSet := sjson.SetRawBytes(out, base+".content", joined); errSet == nil {
			out = updated
			changed = true
		}
		return true
	})
	if !changed {
		return nil
	}
	return out
}
