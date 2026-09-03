package tierb_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/catalog"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/tierb"
)

func tierBVerb(name string, cmd []string, args []string, parser string) *catalog.Verb {
	return &catalog.Verb{
		Name:      name,
		Direction: "perceive",
		Tier:      "B",
		Risk:      "low",
		Command:   cmd,
		Args:      args,
		Parser:    parser,
		Timeout:   nil,
	}
}

func TestStartPollStop(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pids.json")
	tierb.SetPIDFile(pidFile)

	// Use a short-lived producer: yes outputs lines until killed.
	v := tierBVerb("sensor.stream",
		[]string{"sh", "-c", `echo '{"n":1}'; echo 'not-json'; sleep 0.1`},
		[]string{}, "json_stream")

	mgr := tierb.NewManager()
	subID, err := mgr.Start(v, map[string]any{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if subID == "" {
		t.Fatal("expected non-empty subID")
	}

	// Wait for items to arrive.
	deadline := time.Now().Add(2 * time.Second)
	var polled map[string]any
	for time.Now().Before(deadline) {
		polled, err = mgr.Poll(subID, 50)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if items, ok := polled["items"].([]any); ok && len(items) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	items, _ := polled["items"].([]any)
	if len(items) == 0 {
		t.Fatal("expected items from poll")
	}

	// Stop.
	verbName, err := mgr.Stop(subID)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if verbName != "sensor.stream" {
		t.Errorf("Stop returned verb %q, want sensor.stream", verbName)
	}

	// Poll after stop: stopped=true.
	after, err := mgr.Poll(subID, 50)
	if err != nil {
		t.Fatalf("Poll after stop: %v", err)
	}
	if after["stopped"] != true {
		t.Errorf("expected stopped=true after stop, got %v", after["stopped"])
	}
}

func TestStopUnknownID(t *testing.T) {
	mgr := tierb.NewManager()
	_, err := mgr.Stop("nonexistent")
	if err == nil {
		t.Error("expected error for unknown subscription ID")
	}
}

func TestPollUnknownID(t *testing.T) {
	mgr := tierb.NewManager()
	_, err := mgr.Poll("nonexistent", 10)
	if err == nil {
		t.Error("expected error for unknown subscription ID")
	}
}

func TestListActive(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pids.json")
	tierb.SetPIDFile(pidFile)

	mgr := tierb.NewManager()
	if ids := mgr.ListActive(); len(ids) != 0 {
		t.Errorf("expected 0 active subs, got %v", ids)
	}

	v := tierBVerb("sensor.stream",
		[]string{"sh", "-c", "sleep 0.2"},
		[]string{}, "json_stream")
	subID, err := mgr.Start(v, map[string]any{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if ids := mgr.ListActive(); len(ids) != 1 || ids[0] != subID {
		t.Errorf("expected [%s] active, got %v", subID, ids)
	}
	mgr.Stop(subID)
	if ids := mgr.ListActive(); len(ids) != 0 {
		t.Errorf("expected 0 active after stop, got %v", ids)
	}
}

func TestStartRejectsTierA(t *testing.T) {
	v := &catalog.Verb{
		Name: "battery.status", Direction: "perceive", Tier: "A",
		Risk: "none", Command: []string{"true"}, Args: []string{},
		Parser: "json", Timeout: float64Ptr(5),
	}
	mgr := tierb.NewManager()
	_, err := mgr.Start(v, map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "not a Tier B") {
		t.Errorf("expected 'not a Tier B' error, got %v", err)
	}
}

func TestRecoverOrphansEmpty(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pids.json")
	tierb.SetPIDFile(pidFile)
	// Empty PID file — nothing to recover.
	killed := tierb.RecoverOrphans()
	if len(killed) != 0 {
		t.Errorf("expected no kills from empty pid file, got %v", killed)
	}
}

func TestRecoverOrphansCleansPIDFile(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pids.json")
	tierb.SetPIDFile(pidFile)
	// Write PIDs that don't exist.
	os.WriteFile(pidFile, []byte("[999999, 999998]"), 0o644)
	killed := tierb.RecoverOrphans()
	// Non-existent PIDs are skipped.
	if len(killed) != 0 {
		t.Errorf("expected no kills for non-existent PIDs, got %v", killed)
	}
	// PID file should be cleared after recovery.
	data, _ := os.ReadFile(pidFile)
	if strings.TrimSpace(string(data)) != "[]" {
		t.Errorf("expected pid file cleared to [], got %q", data)
	}
}

func float64Ptr(v float64) *float64 { return &v }
