package main

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestEnvCredentialNamesAreCaseInsensitive(t *testing.T) {
	for _, name := range []string{"api_key", "Auth_Token", "postgres_password", "ssh_passwd", "private_secret", "auth", "pass"} {
		t.Run(name, func(t *testing.T) {
			s := newScrubber()
			s.entropy = false
			const value = "a familiar passphrase"
			out, err := s.ScrubChecked("env", []byte(userBody(name+"="+value)))
			if err != nil {
				t.Fatal(err)
			}
			content := gjson.GetBytes(out, "messages.0.content").String()
			placeholder := placeholderPattern.FindString(content)
			if strings.Contains(content, value) || placeholder == "" {
				t.Fatalf("scrubbed %s assignment = %q, want credential replaced", name, content)
			}
			restored := s.Restore("env", []byte(`{"choices":[{"message":{"content":"`+placeholder+`"}}]}`))
			if got := gjson.GetBytes(restored, "choices.0.message.content").String(); got != value {
				t.Errorf("restored %s credential = %q, want %q", name, got, value)
			}
		})
	}
}
