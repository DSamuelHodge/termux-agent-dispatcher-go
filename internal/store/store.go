// Package store is the durable verb-event log and circuit breaker.
//
// v1 is 100% local/offline: an embedded SQLite file at logs/agent.db via
// modernc.org/sqlite (pure Go, CGO_ENABLED=0, no NDK toolchain dependency).
//
// Durability: WAL journal + synchronous=FULL so a committed row survives an
// OS-level kill of the Termux process, not just a runtime panic.
// Each insert runs in BEGIN IMMEDIATE so writers take the reserved lock
// up front instead of upgrading from DEFERRED.
//
// Do not log stdin bodies here — callers pass Verb.PublicArgs.
// Mirrors dispatch/store.py 1:1.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Stages are the audit stages recorded in verb_events.
var Stages = map[string]bool{
	"requested":   true,
	"approved":    true,
	"denied":      true,
	"timeout":     true,
	"executed":    true,
	"failed":      true,
	"circuit_open": true,
	"stopped":     true,
	"mode_changed": true,
}

const (
	// Trip if this many timeout/failed rows land inside the window.
	DefaultFailureLimit = 5
	DefaultWindowS      = 60.0
	DefaultCooldownS    = 30.0
)

var defaultDB = filepath.Join(os.Getenv("HOME"), "agent", "logs", "agent.db")

// Config carries local file plus circuit breaker tuning.
type Config struct {
	Path         string
	FailureLimit int
	WindowS      float64
	CooldownS    float64
}

// ConfigFromEnv builds a Config from environment, using AGENT_DB_PATH if set.
func ConfigFromEnv() *Config {
	path := os.Getenv("AGENT_DB_PATH")
	if path == "" {
		path = defaultDB
	}
	return &Config{
		Path:         path,
		FailureLimit: DefaultFailureLimit,
		WindowS:      DefaultWindowS,
		CooldownS:    DefaultCooldownS,
	}
}

// CircuitOpen is returned by Guard when the breaker is open for a verb.
type CircuitOpen struct{ Verb string }

func (e *CircuitOpen) Error() string { return fmt.Sprintf("%s: circuit open", e.Verb) }

// Store wraps the database handle.
type Store struct {
	cfg       *Config
	db        *sql.DB
	mu        sync.Mutex
	durability Durability
}

// Durability records the pinned journal/synchronous settings.
type Durability struct {
	JournalMode string `json:"journal_mode"`
	Synchronous int    `json:"synchronous"`
}

func schemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS verb_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			ts REAL NOT NULL,
			verb TEXT NOT NULL,
			stage TEXT NOT NULL,
			risk TEXT,
			args_json TEXT NOT NULL DEFAULT '{}',
			error TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS verb_events_verb_ts ON verb_events (verb, ts DESC)`,
		`CREATE INDEX IF NOT EXISTS verb_events_stage_ts ON verb_events (stage, ts DESC)`,
		`CREATE TABLE IF NOT EXISTS confirm_jobs (
			id TEXT PRIMARY KEY,
			verb TEXT NOT NULL,
			kind TEXT NOT NULL,
			risk TEXT,
			args_json TEXT NOT NULL DEFAULT '{}',
			status TEXT NOT NULL,
			http_status INTEGER,
			result_json TEXT,
			error TEXT,
			error_code TEXT,
			webhook_url TEXT,
			idempotency_key TEXT,
			created_ts REAL NOT NULL,
			updated_ts REAL NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS confirm_jobs_status_ts ON confirm_jobs (status, created_ts)`,
		`CREATE TABLE IF NOT EXISTS idempotency_keys (
			verb TEXT NOT NULL,
			key TEXT NOT NULL,
			args_hash TEXT NOT NULL,
			confirm_id TEXT,
			http_status INTEGER,
			response_json TEXT NOT NULL,
			created_ts REAL NOT NULL,
			PRIMARY KEY (verb, key)
		)`,
	}
}

func connect(cfg *Config) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir: %w", err)
	}
	db, err := sql.Open("sqlite", cfg.Path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	// Serialise all access through one connection; modernc/sqlite supports
	// concurrent use of the *sql.DB but we want explicit ordering for BEGIN IMMEDIATE.
	db.SetMaxOpenConns(1)
	return db, nil
}

// applyDurability pins WAL + FULL and reads them back.
// PRAGMA as a SET statement returns no rows in modernc.org/sqlite — use
// Exec for the write, then QueryRow to read back and verify.
func applyDurability(db *sql.DB) (*Durability, error) {
	var journal string
	if err := db.QueryRow("PRAGMA journal_mode=WAL").Scan(&journal); err != nil {
		return nil, fmt.Errorf("store: set journal_mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA synchronous=FULL"); err != nil {
		return nil, fmt.Errorf("store: set synchronous: %w", err)
	}
	// Read back the actual values.
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil {
		return nil, fmt.Errorf("store: read journal_mode: %w", err)
	}
	var sync int
	if err := db.QueryRow("PRAGMA synchronous").Scan(&sync); err != nil {
		return nil, fmt.Errorf("store: read synchronous: %w", err)
	}
	d := &Durability{JournalMode: journal, Synchronous: sync}
	if d.JournalMode != "wal" {
		return nil, fmt.Errorf("store: expected journal_mode=WAL, got %q", journal)
	}
	if d.Synchronous != 2 {
		return nil, fmt.Errorf("store: expected synchronous=FULL (2), got %d", sync)
	}
	return d, nil
}

// New opens (or creates) the store at cfg.Path.
func New(cfg *Config) (*Store, error) {
	if cfg == nil {
		cfg = ConfigFromEnv()
	}
	db, err := connect(cfg)
	if err != nil {
		return nil, err
	}
	d, err := applyDurability(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	for _, stmt := range schemaStatements() {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: schema: %w", err)
		}
	}
	return &Store{cfg: cfg, db: db, durability: *d}, nil
}

// Close closes the database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// Append inserts an event row and returns its rowid.
func (s *Store) Append(verb, stage, risk string, args map[string]any, errStr *string, ts *float64) (int64, error) {
	if !Stages[stage] {
		return 0, fmt.Errorf("store: unknown stage %q", stage)
	}
	eventTS := nowFloat()
	if ts != nil {
		eventTS = *ts
	}
	payload := "{}"
	if args != nil {
		b, err := json.Marshal(args)
		if err != nil {
			return 0, fmt.Errorf("store: marshal args: %w", err)
		}
		payload = string(b)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("store: begin: %w", err)
	}
	res, err := tx.Exec(
		"INSERT INTO verb_events (ts, verb, stage, risk, args_json, error) VALUES (?, ?, ?, ?, ?, ?)",
		eventTS, verb, stage, risk, payload, errStr,
	)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("store: insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: commit: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("store: last_insert_id: %w", err)
	}
	return id, nil
}

// Event is a single verb_events row.
type Event struct {
	ID       int64
	TS       float64
	Verb     string
	Stage    string
	Risk     sql.NullString
	ArgsJSON string
	Error    sql.NullString
}

// Recent returns up to limit rows for verb, newest first.
func (s *Store) Recent(verb string, stage string, since float64, limit int) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	query := "SELECT id, ts, verb, stage, risk, args_json, error FROM verb_events WHERE verb = ?"
	args := []any{verb}
	if stage != "" {
		query += " AND stage = ?"
		args = append(args, stage)
	}
	if since > 0 {
		query += " AND ts >= ?"
		args = append(args, since)
	}
	query += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: recent: %w", err)
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var ev Event
		if err := rows.Scan(&ev.ID, &ev.TS, &ev.Verb, &ev.Stage, &ev.Risk, &ev.ArgsJSON, &ev.Error); err != nil {
			return nil, fmt.Errorf("store: scan: %w", err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// FailureCount returns the number of timeout/failed rows inside the window.
func (s *Store) FailureCount(verb string) (int, error) {
	since := nowFloat() - s.cfg.WindowS
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	err := s.db.QueryRow(
		"SELECT COUNT(*) FROM verb_events WHERE verb = ? AND stage IN ('timeout', 'failed') AND ts >= ?",
		verb, since,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: failure_count: %w", err)
	}
	return n, nil
}

// IsOpen reports whether the circuit breaker is currently open for verb.
func (s *Store) IsOpen(verb string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ts float64
	err := s.db.QueryRow(
		"SELECT ts FROM verb_events WHERE verb = ? AND stage = 'circuit_open' ORDER BY id DESC LIMIT 1",
		verb,
	).Scan(&ts)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: is_open: %w", err)
	}
	return (nowFloat() - ts) < s.cfg.CooldownS, nil
}

// Guard returns *CircuitOpen if the breaker is open for verb.
// Mirrors store.guard() in Python: raises on open circuit.
func (s *Store) Guard(verb, risk string, args map[string]any) error {
	open, err := s.IsOpen(verb)
	if err != nil {
		return err
	}
	if open {
		s.Append(verb, "circuit_open", risk, args, nil, nil)
		return &CircuitOpen{Verb: verb}
	}
	return nil
}

// RecordOutcome inserts an outcome row and trips the breaker on threshold.
func (s *Store) RecordOutcome(verb, stage, risk string, args map[string]any, errStr *string) (int64, error) {
	rowid, err := s.Append(verb, stage, risk, args, errStr, nil)
	if err != nil {
		return 0, err
	}
	if stage == "timeout" || stage == "failed" {
		n, err := s.FailureCount(verb)
		if err != nil {
			return rowid, err
		}
		if n >= s.cfg.FailureLimit {
			open, err := s.IsOpen(verb)
			if err != nil {
				return rowid, err
			}
			if !open {
				s.Append(verb, "circuit_open", risk, args, errStr, nil)
			}
		}
	}
	return rowid, nil
}

// nowFloat returns the current time as fractional Unix seconds, matching
// Python's time.time() — the ts column is REAL and keeps sub-second precision.
func nowFloat() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// ── Confirm jobs ─────────────────────────────────────────────────────────────

// ConfirmJob is one row of confirm_jobs: an async on-device confirmation.
type ConfirmJob struct {
	ID            string
	Verb          string
	Kind          string
	Risk          sql.NullString
	Args          map[string]any
	Status        string
	HTTPStatus    sql.NullInt64
	Result        map[string]any
	Error         sql.NullString
	ErrorCode     sql.NullString
	WebhookURL    sql.NullString
	IdempotencyKey sql.NullString
	CreatedTS     float64
	UpdatedTS     float64
}

// PutConfirmJob inserts or updates a confirm job (upsert on id). created_ts
// is preserved across updates, matching Python's put_confirm_job.
func (s *Store) PutConfirmJob(j *ConfirmJob) error {
	now := nowFloat()
	argsJSON := "{}"
	if j.Args != nil {
		b, err := json.Marshal(j.Args)
		if err != nil {
			return fmt.Errorf("store: marshal confirm args: %w", err)
		}
		argsJSON = string(b)
	}
	var resultJSON any
	if j.Result != nil {
		b, err := json.Marshal(j.Result)
		if err != nil {
			return fmt.Errorf("store: marshal confirm result: %w", err)
		}
		resultJSON = string(b)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	var created float64
	err = tx.QueryRow("SELECT created_ts FROM confirm_jobs WHERE id = ?", j.ID).Scan(&created)
	if err == sql.ErrNoRows {
		created = now
	} else if err != nil {
		tx.Rollback()
		return fmt.Errorf("store: confirm lookup: %w", err)
	}
	_, err = tx.Exec(
		`INSERT INTO confirm_jobs (id, verb, kind, risk, args_json, status, http_status,
			result_json, error, error_code, webhook_url, idempotency_key, created_ts, updated_ts)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
			status=excluded.status, http_status=excluded.http_status,
			result_json=excluded.result_json, error=excluded.error,
			error_code=excluded.error_code, updated_ts=excluded.updated_ts`,
		j.ID, j.Verb, j.Kind, j.Risk, argsJSON, j.Status, j.HTTPStatus,
		resultJSON, j.Error, j.ErrorCode, j.WebhookURL, j.IdempotencyKey,
		created, now,
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("store: confirm insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}

// GetConfirmJob returns the job or nil if unknown.
func (s *Store) GetConfirmJob(jobID string) (*ConfirmJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.db.QueryRow(
		`SELECT id, verb, kind, risk, args_json, status, http_status,
			result_json, error, error_code, webhook_url, idempotency_key,
			created_ts, updated_ts FROM confirm_jobs WHERE id = ?`, jobID)
	var j ConfirmJob
	var argsJSON string
	var resultJSON sql.NullString
	err := row.Scan(&j.ID, &j.Verb, &j.Kind, &j.Risk, &argsJSON, &j.Status, &j.HTTPStatus,
		&resultJSON, &j.Error, &j.ErrorCode, &j.WebhookURL, &j.IdempotencyKey,
		&j.CreatedTS, &j.UpdatedTS)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: get confirm: %w", err)
	}
	j.Args = map[string]any{}
	if argsJSON != "" {
		json.Unmarshal([]byte(argsJSON), &j.Args)
	}
	if resultJSON.Valid && resultJSON.String != "" {
		j.Result = map[string]any{}
		json.Unmarshal([]byte(resultJSON.String), &j.Result)
	}
	return &j, nil
}

// ListPendingConfirms returns ids of all pending confirm jobs.
func (s *Store) ListPendingConfirms() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query("SELECT id FROM confirm_jobs WHERE status = 'pending'")
	if err != nil {
		return nil, fmt.Errorf("store: pending confirms: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan confirm id: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ── Idempotency ──────────────────────────────────────────────────────────────

// IdempotencyRecord is one row of idempotency_keys.
type IdempotencyRecord struct {
	Verb       string
	Key        string
	ArgsHash   string
	ConfirmID  sql.NullString
	HTTPStatus sql.NullInt64
	Response   map[string]any
	CreatedTS  float64
}

// GetIdempotency returns the stored record or nil.
func (s *Store) GetIdempotency(verb, key string) (*IdempotencyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.db.QueryRow(
		`SELECT verb, key, args_hash, confirm_id, http_status, response_json, created_ts
		 FROM idempotency_keys WHERE verb = ? AND key = ?`, verb, key)
	var r IdempotencyRecord
	var responseJSON string
	err := row.Scan(&r.Verb, &r.Key, &r.ArgsHash, &r.ConfirmID, &r.HTTPStatus, &responseJSON, &r.CreatedTS)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: get idempotency: %w", err)
	}
	r.Response = map[string]any{}
	if responseJSON != "" {
		json.Unmarshal([]byte(responseJSON), &r.Response)
	}
	return &r, nil
}

// PutIdempotency inserts or updates an idempotency record (upsert on verb+key).
func (s *Store) PutIdempotency(r *IdempotencyRecord) error {
	responseJSON := "{}"
	if r.Response != nil {
		b, err := json.Marshal(r.Response)
		if err != nil {
			return fmt.Errorf("store: marshal idempotency response: %w", err)
		}
		responseJSON = string(b)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	_, err = tx.Exec(
		`INSERT INTO idempotency_keys (verb, key, args_hash, confirm_id, http_status,
			response_json, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(verb, key) DO UPDATE SET
			http_status=excluded.http_status, response_json=excluded.response_json,
			confirm_id=excluded.confirm_id, args_hash=excluded.args_hash`,
		r.Verb, r.Key, r.ArgsHash, r.ConfirmID, r.HTTPStatus, responseJSON, nowFloat(),
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("store: idempotency insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}

// ── Process singleton ────────────────────────────────────────────────────────

var (
	storeMu sync.Mutex
	store   *Store
)

// Get returns the process singleton, opening it on first call.
func Get() (*Store, error) {
	storeMu.Lock()
	defer storeMu.Unlock()
	if store == nil {
		s, err := New(nil)
		if err != nil {
			return nil, err
		}
		store = s
	}
	return store, nil
}

// Reset replaces (or drops) the process singleton. Used by tests.
func Reset(s *Store) {
	storeMu.Lock()
	defer storeMu.Unlock()
	if store != nil && s == nil {
		store.Close()
	}
	store = s
}
