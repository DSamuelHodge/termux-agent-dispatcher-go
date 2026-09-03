package tiera_test

import (
	"strings"
	"testing"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/catalog"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/tiera"
)

func f64(v float64) *float64 { return &v }

func verb(name, parser string, cmd []string, args []string, timeout *float64) *catalog.Verb {
	return &catalog.Verb{
		Name:      name,
		Direction: "perceive",
		Tier:      "A",
		Risk:      "none",
		Command:   cmd,
		Args:      args,
		Parser:    parser,
		Timeout:   timeout,
	}
}

func TestTierAJsonOK(t *testing.T) {
	v := verb("demo", "json", []string{"echo", `{"a":1}`}, nil, f64(5))
	result, err := tiera.Run(v, map[string]any{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result["ok"] != true {
		t.Errorf("ok = %v", result["ok"])
	}
	data, ok := result["data"].(map[string]any)
	if !ok || data["a"] != float64(1) {
		t.Errorf("data = %v", result["data"])
	}
}

func TestTierATextAndNone(t *testing.T) {
	vText := verb("demo", "text", []string{"echo", "hi"}, nil, f64(5))
	r, err := tiera.Run(vText, map[string]any{})
	if err != nil || r["data"] != "hi" {
		t.Errorf("text parser: got %v, %v", r, err)
	}

	vNone := verb("demo", "none", []string{"true"}, nil, f64(5))
	r, err = tiera.Run(vNone, map[string]any{})
	if err != nil || r["ok"] != true {
		t.Errorf("none parser: got %v, %v", r, err)
	}
	if _, hasData := r["data"]; hasData {
		t.Errorf("none parser should not return data, got %v", r)
	}
}

func TestTierAEmptyJson(t *testing.T) {
	v := verb("demo", "json", []string{"true"}, nil, f64(5))
	r, err := tiera.Run(v, map[string]any{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r["ok"] != true || r["data"] != nil {
		t.Errorf("empty json: got %v", r)
	}
}

func TestTierATimeout(t *testing.T) {
	v := verb("demo", "json", []string{"sleep", "10"}, nil, f64(0.05))
	_, err := tiera.Run(v, map[string]any{})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	execErr, ok := err.(*tiera.ExecutionError)
	if !ok {
		t.Fatalf("expected *ExecutionError, got %T", err)
	}
	if !strings.Contains(execErr.Message, "timed out") {
		t.Errorf("expected 'timed out' in message, got %q", execErr.Message)
	}
}

func TestTierACommandNotFound(t *testing.T) {
	v := verb("demo", "json", []string{"/nonexistent/binary/xyz"}, nil, f64(5))
	_, err := tiera.Run(v, map[string]any{})
	if err == nil {
		t.Fatal("expected command-not-found error")
	}
	execErr, ok := err.(*tiera.ExecutionError)
	if !ok {
		t.Fatalf("expected *ExecutionError, got %T", err)
	}
	if !strings.Contains(execErr.Message, "command not found") {
		t.Errorf("expected 'command not found', got %q", execErr.Message)
	}
}

func TestTierABadExitCode(t *testing.T) {
	v := verb("demo", "json", []string{"false"}, nil, f64(5))
	_, err := tiera.Run(v, map[string]any{})
	if err == nil {
		t.Fatal("expected exit code error")
	}
	execErr, ok := err.(*tiera.ExecutionError)
	if !ok {
		t.Fatalf("expected *ExecutionError, got %T", err)
	}
	if !strings.Contains(execErr.Message, "exit code") {
		t.Errorf("expected 'exit code', got %q", execErr.Message)
	}
}

func TestTierABadJson(t *testing.T) {
	v := verb("demo", "json", []string{"echo", "not-json"}, nil, f64(5))
	_, err := tiera.Run(v, map[string]any{})
	if err == nil {
		t.Fatal("expected JSON parse error")
	}
	execErr, ok := err.(*tiera.ExecutionError)
	if !ok {
		t.Fatalf("expected *ExecutionError, got %T", err)
	}
	if !strings.Contains(execErr.Message, "unparseable") {
		t.Errorf("expected 'unparseable', got %q", execErr.Message)
	}
}

func TestTierARejectsTierB(t *testing.T) {
	v := verb("demo", "json", []string{"true"}, nil, nil)
	v.Tier = "B"
	_, err := tiera.Run(v, map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "not a Tier A") {
		t.Errorf("expected 'not a Tier A' error, got %v", err)
	}
}

func TestTierARejectsBadParser(t *testing.T) {
	v := verb("demo", "bogus", []string{"true"}, nil, f64(1))
	_, err := tiera.Run(v, map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "parser") {
		t.Errorf("expected parser error, got %v", err)
	}
}
