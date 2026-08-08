package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersionCommand(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run([]string{"version"}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("exit %d, stderr=%q", code, stderr.String())
	}
	got := strings.TrimSpace(stdout.String())
	if got != version {
		t.Fatalf("version output %q, want %q", got, version)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr %q", stderr.String())
	}
}

func TestVersionFlag(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run([]string{"--version"}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("exit %d, stderr=%q", code, stderr.String())
	}
	got := strings.TrimSpace(stdout.String())
	if got != version {
		t.Fatalf("version output %q, want %q", got, version)
	}
}

func TestUnknownCommandExit2(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run([]string{"nope"}, &stdout, &stderr, nil)
	if code != 2 {
		t.Fatalf("exit %d, want 2; stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("stderr missing unknown-command notice: %q", stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Fatalf("stderr missing usage: %q", stderr.String())
	}
}

func TestNoCommandExit2(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run(nil, &stdout, &stderr, nil)
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Fatalf("stderr missing usage: %q", stderr.String())
	}
}

func TestResolveDSN(t *testing.T) {
	t.Parallel()
	t.Run("flag wins", func(t *testing.T) {
		t.Parallel()
		got, err := resolveDSN("postgres://flag", func(string) string {
			return "postgres://env"
		})
		if err != nil {
			t.Fatal(err)
		}
		if got != "postgres://flag" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("env fallback", func(t *testing.T) {
		t.Parallel()
		got, err := resolveDSN("", func(k string) string {
			if k == "DATABASE_URL" {
				return "postgres://env"
			}
			return ""
		})
		if err != nil {
			t.Fatal(err)
		}
		if got != "postgres://env" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("missing", func(t *testing.T) {
		t.Parallel()
		_, err := resolveDSN("", func(string) string { return "" })
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestPeelGlobalsJSONAnywhere(t *testing.T) {
	t.Parallel()
	cfg, rest, err := peelGlobals([]string{"stats", "--json"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.json {
		t.Fatal("expected --json peeled")
	}
	if len(rest) != 1 || rest[0] != "stats" {
		t.Fatalf("rest=%v", rest)
	}
}
