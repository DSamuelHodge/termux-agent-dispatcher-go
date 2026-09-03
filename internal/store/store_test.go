package store_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "agent.db")
	s, err := store.New(&store.Config{
		Path:         dbPath,
		FailureLimit: store.DefaultFailureLimit,
		WindowS:      store.DefaultWindowS,
		CooldownS:    store.DefaultCooldownS,
	})
	if err != nil {
		t.Fatalf("New store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestNewDurability(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "agent.db")
	s, err := store.New(&store.Config{
		Path:         dbPath,
		FailureLimit: store.DefaultFailureLimit,
		WindowS:      store.DefaultWindowS,
		CooldownS:    store.DefaultCooldownS,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()
	// WAL+FULL is verified inside New — if it returned without error, the
	// PRAGMAs are correct. This test just confirms New succeeds.
}

func TestAppendAndRecent(t *testing.T) {
	s := newTestStore(t)
	id, err := s.Append("verb.test", "requested", "low", map[string]any{"k": "v"}, nil, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if id <= 0 {
		t.Errorf("expected positive rowid, got %d", id)
	}
	events, err := s.Recent("verb.test", "requested", 0, 10)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Verb != "verb.test" || events[0].Stage != "requested" {
		t.Errorf("unexpected event: %+v", events[0])
	}
}

func TestAppendUnknownStage(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Append("verb.test", "bogus_stage", "low", nil, nil, nil)
	if err == nil {
		t.Error("expected error for unknown stage")
	}
}

func TestGuardClosed(t *testing.T) {
	s := newTestStore(t)
	// Circuit not open — Guard should return nil.
	if err := s.Guard("verb.test", "low", nil); err != nil {
		t.Errorf("Guard on closed circuit: %v", err)
	}
}

func TestCircuitBreakerTripAndGuard(t *testing.T) {
	s := newTestStore(t)
	// Trip the breaker: insert FailureLimit failures inside the window.
	errStr := "exec failed"
	for i := 0; i < store.DefaultFailureLimit; i++ {
		_, err := s.RecordOutcome("verb.flaky", "failed", "low", nil, &errStr)
		if err != nil {
			t.Fatalf("RecordOutcome %d: %v", i, err)
		}
	}
	open, err := s.IsOpen("verb.flaky")
	if err != nil {
		t.Fatalf("IsOpen: %v", err)
	}
	if !open {
		t.Error("expected circuit to be open after threshold failures")
	}
	// Guard should raise CircuitOpen.
	err = s.Guard("verb.flaky", "low", nil)
	if err == nil {
		t.Error("expected CircuitOpen from Guard")
	}
	if _, ok := err.(*store.CircuitOpen); !ok {
		t.Errorf("expected *CircuitOpen, got %T: %v", err, err)
	}
}

func TestCircuitBreakerCooldown(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "agent.db")
	s, err := store.New(&store.Config{
		Path:         dbPath,
		FailureLimit: 5,
		WindowS:      60.0,
		CooldownS:    0.001, // 1ms cooldown — should expire almost immediately
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	errStr := "timeout"
	for i := 0; i < 5; i++ {
		s.RecordOutcome("verb.flaky", "failed", "low", nil, &errStr)
	}
	open, _ := s.IsOpen("verb.flaky")
	if !open {
		t.Fatal("circuit should be open")
	}
	time.Sleep(5 * time.Millisecond)
	open, _ = s.IsOpen("verb.flaky")
	if open {
		t.Error("circuit should be closed after cooldown")
	}
}

func TestFailureCountWindow(t *testing.T) {
	s := newTestStore(t)
	errStr := "fail"
	// Insert failures with timestamps outside the window.
	oldTS := float64(time.Now().Unix()) - store.DefaultWindowS - 10
	for i := 0; i < 10; i++ {
		s.Append("verb.old", "failed", "low", nil, &errStr, &oldTS)
	}
	n, err := s.FailureCount("verb.old")
	if err != nil {
		t.Fatalf("FailureCount: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 failures inside window, got %d", n)
	}
	// Insert inside the window.
	for i := 0; i < 3; i++ {
		s.Append("verb.old", "failed", "low", nil, &errStr, nil)
	}
	n, _ = s.FailureCount("verb.old")
	if n != 3 {
		t.Errorf("expected 3 failures inside window, got %d", n)
	}
}

func TestRecordOutcomeTripsAtThreshold(t *testing.T) {
	s := newTestStore(t)
	errStr := "exec failed"
	// Insert FailureLimit-1 failures — not enough to trip.
	for i := 0; i < store.DefaultFailureLimit-1; i++ {
		s.RecordOutcome("verb.partial", "failed", "low", nil, &errStr)
	}
	open, _ := s.IsOpen("verb.partial")
	if open {
		t.Error("circuit should not be open below threshold")
	}
	// One more failure crosses the threshold.
	s.RecordOutcome("verb.partial", "failed", "low", nil, &errStr)
	open, _ = s.IsOpen("verb.partial")
	if !open {
		t.Error("circuit should be open at threshold")
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "agent.db")
	cfg := &store.Config{Path: dbPath, FailureLimit: 5, WindowS: 60.0, CooldownS: 30.0}

	// First open: record failures and trip the breaker.
	s1, err := store.New(cfg)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	errStr := "timeout"
	for i := 0; i < 5; i++ {
		s1.RecordOutcome("verb.persist", "timeout", "medium", nil, &errStr)
	}
	open, _ := s1.IsOpen("verb.persist")
	if !open {
		t.Fatal("circuit should be open after threshold")
	}
	s1.Close()

	// Second open: circuit should still be open (persisted in SQLite).
	s2, err := store.New(cfg)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer s2.Close()
	open, _ = s2.IsOpen("verb.persist")
	if !open {
		t.Error("circuit should survive process restart (persisted to SQLite)")
	}
	// Guard should raise on the reopened store.
	err = s2.Guard("verb.persist", "medium", nil)
	if _, ok := err.(*store.CircuitOpen); !ok {
		t.Errorf("expected CircuitOpen after restart, got %T: %v", err, err)
	}
}
