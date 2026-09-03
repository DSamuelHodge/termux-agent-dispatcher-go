package riskgate_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/catalog"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/riskgate"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/store"
)

// setupTestEnv wires an isolated store and audit log for each test.
func setupTestEnv(t *testing.T) (auditPath string) {
	t.Helper()
	tmp := t.TempDir()
	auditPath = filepath.Join(tmp, "audit.log")
	riskgate.SetLogPath(auditPath)

	dbPath := filepath.Join(tmp, "agent.db")
	s, err := store.New(&store.Config{
		Path:         dbPath,
		FailureLimit: store.DefaultFailureLimit,
		WindowS:      store.DefaultWindowS,
		CooldownS:    store.DefaultCooldownS,
	})
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	store.Reset(s)
	t.Cleanup(func() { store.Reset(nil) })

	// Reset mode to gated after each test.
	t.Cleanup(func() { riskgate.SetMode(riskgate.ModeGated) })
	// Reset confirm func.
	t.Cleanup(func() { riskgate.SetConfirmFunc(nil) })

	return auditPath
}

func realCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	p := "/Users/derrickhodge/termux-agent-dispatcher/verbs.yaml"
	if _, err := os.Stat(p); err != nil {
		t.Skipf("shared verbs.yaml not found: %v", err)
	}
	c, err := catalog.Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return c
}

// auditStages reads the audit log and returns the stage of each event.
func auditStages(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	var stages []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("bad audit line %q: %v", line, err)
		}
		if s, ok := ev["stage"].(string); ok {
			stages = append(stages, s)
		}
	}
	return stages
}

func containsStage(stages []string, want string) bool {
	for _, s := range stages {
		if s == want {
			return true
		}
	}
	return false
}

// ── Gated mode (default) ──────────────────────────────────────────────────────

func TestGatedLowRiskNoDialog(t *testing.T) {
	auditPath := setupTestEnv(t)
	cat := realCatalog(t)
	// toast.show is risk:none — no confirmation needed in any mode.
	if err := riskgate.Check(cat, "toast.show", map[string]any{"text": "x"}); err != nil {
		t.Fatalf("Check toast.show: %v", err)
	}
	stages := auditStages(t, auditPath)
	if !containsStage(stages, "requested") {
		t.Errorf("expected 'requested' in audit, got %v", stages)
	}
	// Audit parity with Python: only confirmation-gated calls record a
	// decision line. A verb below the gate goes requested → executed with
	// no "approved" in between.
	if containsStage(stages, "approved") {
		t.Errorf("unexpected 'approved' for non-gated verb, got %v", stages)
	}
	if containsStage(stages, "denied") {
		t.Errorf("unexpected 'denied' for non-gated verb, got %v", stages)
	}
}

func TestGatedHighRiskApproved(t *testing.T) {
	auditPath := setupTestEnv(t)
	cat := realCatalog(t)
	riskgate.SetConfirmFunc(func(string, map[string]any) bool { return true })
	// sms.send is risk:high — confirmation required in gated mode.
	if err := riskgate.Check(cat, "sms.send", map[string]any{"number": "1", "text": "x"}); err != nil {
		t.Fatalf("Check sms.send approved: %v", err)
	}
	stages := auditStages(t, auditPath)
	if !containsStage(stages, "approved") {
		t.Errorf("expected 'approved' in audit, got %v", stages)
	}
}

func TestGatedHighRiskDenied(t *testing.T) {
	setupTestEnv(t)
	cat := realCatalog(t)
	riskgate.SetConfirmFunc(func(string, map[string]any) bool { return false })
	err := riskgate.Check(cat, "sms.send", map[string]any{"number": "1", "text": "x"})
	if err == nil {
		t.Fatal("expected Denied")
	}
	if _, ok := err.(*riskgate.Denied); !ok {
		t.Errorf("expected *Denied, got %T: %v", err, err)
	}
}

func TestGatedHighRiskNoConfirmFuncIsDeny(t *testing.T) {
	setupTestEnv(t)
	cat := realCatalog(t)
	// No confirm func installed — must deny (fail-closed).
	err := riskgate.Check(cat, "sms.send", map[string]any{"number": "1", "text": "x"})
	if err == nil {
		t.Fatal("expected Denied when no confirm func installed")
	}
}

// ── Manual mode ───────────────────────────────────────────────────────────────

func TestManualMediumRiskRequiresConfirm(t *testing.T) {
	setupTestEnv(t)
	cat := realCatalog(t)
	riskgate.SetMode(riskgate.ModeManual)
	// location.get is risk:medium — no confirm in gated, confirm in manual.
	riskgate.SetConfirmFunc(func(string, map[string]any) bool { return false })
	err := riskgate.Check(cat, "location.get", map[string]any{"provider": "gps"})
	if err == nil {
		t.Fatal("expected Denied for medium-risk verb in manual mode")
	}
	if _, ok := err.(*riskgate.Denied); !ok {
		t.Errorf("expected *Denied, got %T: %v", err, err)
	}
}

func TestManualLowRiskNoConfirm(t *testing.T) {
	setupTestEnv(t)
	cat := realCatalog(t)
	riskgate.SetMode(riskgate.ModeManual)
	// torch.toggle is risk:low — no confirm even in manual mode.
	if err := riskgate.Check(cat, "torch.toggle", map[string]any{"state": "on"}); err != nil {
		t.Fatalf("Check torch.toggle in manual: %v", err)
	}
}

// ── Full mode ─────────────────────────────────────────────────────────────────

func TestFullModeSkipsConfirmation(t *testing.T) {
	auditPath := setupTestEnv(t)
	cat := realCatalog(t)
	riskgate.SetMode(riskgate.ModeFull)
	// sms.send is risk:high — no confirmation in full mode.
	// No confirm func installed; if full mode still called it, we'd get a deny.
	if err := riskgate.Check(cat, "sms.send", map[string]any{"number": "1", "text": "x"}); err != nil {
		t.Fatalf("Check sms.send in full mode: %v", err)
	}
	stages := auditStages(t, auditPath)
	// Audit is unconditional: requested and approved both written.
	if !containsStage(stages, "requested") {
		t.Errorf("expected 'requested' in full mode audit, got %v", stages)
	}
	if !containsStage(stages, "approved") {
		t.Errorf("expected 'approved' in full mode audit, got %v", stages)
	}
	if containsStage(stages, "denied") {
		t.Errorf("unexpected 'denied' in full mode audit, got %v", stages)
	}
}

func TestFullModeCircuitBreakerStillApplies(t *testing.T) {
	setupTestEnv(t)
	cat := realCatalog(t)
	riskgate.SetMode(riskgate.ModeFull)

	// Trip the circuit breaker for battery.status directly in the store.
	st, _ := store.Get()
	errStr := "exec failed"
	for i := 0; i < store.DefaultFailureLimit; i++ {
		st.RecordOutcome("battery.status", "failed", "none", nil, &errStr)
	}

	// Even in full mode, circuit-open must block execution.
	err := riskgate.Check(cat, "battery.status", map[string]any{})
	if err == nil {
		t.Fatal("expected CircuitOpen in full mode")
	}
	if _, ok := err.(*store.CircuitOpen); !ok {
		t.Errorf("expected *store.CircuitOpen, got %T: %v", err, err)
	}
}

// ── Audit ordering ────────────────────────────────────────────────────────────

func TestAuditBeforeGuard(t *testing.T) {
	auditPath := setupTestEnv(t)
	cat := realCatalog(t)

	// Trip circuit for camera.photo.
	st, _ := store.Get()
	errStr := "timeout"
	for i := 0; i < store.DefaultFailureLimit; i++ {
		st.RecordOutcome("camera.photo", "timeout", "high", nil, &errStr)
	}

	riskgate.SetConfirmFunc(func(string, map[string]any) bool { return true })
	// camera.photo is risk:high and circuit is open.
	// Audit "requested" must be written before Guard raises.
	riskgate.Check(cat, "camera.photo", map[string]any{"camera_id": "0", "outfile": "/tmp/x.jpg"})

	stages := auditStages(t, auditPath)
	if len(stages) == 0 || stages[0] != "requested" {
		t.Errorf("expected first audit stage to be 'requested', got %v", stages)
	}
}

// ── Mode config file ──────────────────────────────────────────────────────────

func TestLoadModeFromFile(t *testing.T) {
	tmp := t.TempDir()

	// Missing file defaults to gated.
	m, err := riskgate.LoadModeFromFile(filepath.Join(tmp, "nonexistent"))
	if err != nil || m != riskgate.ModeGated {
		t.Errorf("missing file: got %q, %v", m, err)
	}

	// Valid modes.
	for _, want := range []riskgate.AutonomyMode{riskgate.ModeManual, riskgate.ModeGated, riskgate.ModeFull} {
		p := filepath.Join(tmp, "mode")
		os.WriteFile(p, []byte(string(want)+"\n"), 0o644)
		got, err := riskgate.LoadModeFromFile(p)
		if err != nil || got != want {
			t.Errorf("mode %q: got %q, %v", want, got, err)
		}
	}

	// Invalid mode returns error.
	p := filepath.Join(tmp, "mode")
	os.WriteFile(p, []byte("bogus\n"), 0o644)
	_, err = riskgate.LoadModeFromFile(p)
	if err == nil {
		t.Error("expected error for unknown mode")
	}
}

// ── GetMode / SetMode ─────────────────────────────────────────────────────────

func TestSetAndGetMode(t *testing.T) {
	riskgate.SetMode(riskgate.ModeManual)
	if got := riskgate.GetMode(); got != riskgate.ModeManual {
		t.Errorf("GetMode = %q, want manual", got)
	}
	riskgate.SetMode(riskgate.ModeGated) // restore
}
