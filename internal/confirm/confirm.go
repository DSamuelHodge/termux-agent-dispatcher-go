// Package confirm implements async on-device confirmation.
//
// High-risk verbs return a pending confirm_id instead of blocking the MCP
// request on termux-dialog. The phone user still taps Yes/No; the brain
// polls the confirm.poll MCP tool. Application handles live in logs/agent.db
// — MCP itself stays stateless (2026-07-28): the client passes confirm_id
// back as an ordinary argument.
//
// termux-dialog on the physical device is the sole approval surface. The
// elicitation result shown to the MCP client is never itself the approve/
// deny decision — that would collapse requester and approver onto the same
// channel and quietly remove the physical-presence property the
// confirmation step exists for. Mirrors dispatch/confirm.py 1:1.
package confirm

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/catalog"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/errs"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/riskgate"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/store"
)

const webhookTimeout = 5 * time.Second

// ExecuteFn runs the verb after approval. kind is "perceive", "act",
// "watch", or "mode". Returns the HTTP-equivalent status and payload that
// get persisted with the job.
type ExecuteFn func(kind string, verb *catalog.Verb, args map[string]any) (int, map[string]any)

// Manager owns in-flight and completed confirm jobs.
type Manager struct {
	execute  ExecuteFn
	mu       sync.Mutex
	inFlight map[string]bool
}

// NewManager builds a Manager around an execute callback.
func NewManager(execute ExecuteFn) *Manager {
	return &Manager{execute: execute, inFlight: map[string]bool{}}
}

// jobPayload is the client-visible shape of a confirm job (Python public_job).
func jobPayload(j *store.ConfirmJob) map[string]any {
	out := map[string]any{
		"confirm_id": j.ID,
		"verb":       j.Verb,
		"kind":       j.Kind,
		"status":     j.Status,
		"poll":       fmt.Sprintf("/confirm/%s", j.ID),
	}
	if j.Risk.Valid && j.Risk.String != "" {
		out["risk"] = j.Risk.String
	}
	if j.Status == "pending" {
		out["code"] = errs.ConfirmPending
	}
	if j.Result != nil {
		out["result"] = j.Result
	}
	if j.Error.Valid && j.Error.String != "" {
		out["error"] = j.Error.String
		if j.ErrorCode.Valid && j.ErrorCode.String != "" {
			out["code"] = j.ErrorCode.String
		}
	}
	return out
}

// Get returns the public payload for a job, or nil if unknown.
func (m *Manager) Get(jobID string) (map[string]any, error) {
	st, err := store.Get()
	if err != nil {
		return nil, err
	}
	j, err := st.GetConfirmJob(jobID)
	if err != nil {
		return nil, err
	}
	if j == nil {
		return nil, nil
	}
	return jobPayload(j), nil
}

// Poll returns the public payload for a job plus its stored verb HTTP
// status (0 if unknown job). Mirrors engine.poll_confirm.
func (m *Manager) Poll(jobID string) (map[string]any, int, error) {
	st, err := store.Get()
	if err != nil {
		return nil, 0, err
	}
	j, err := st.GetConfirmJob(jobID)
	if err != nil {
		return nil, 0, err
	}
	if j == nil {
		return nil, 0, nil
	}
	return jobPayload(j), int(j.HTTPStatus.Int64), nil
}

// FailOrphans marks pending rows from a previous process as failed —
// a pending dialog cannot resume across a daemon restart.
func (m *Manager) FailOrphans() (int, error) {
	st, err := store.Get()
	if err != nil {
		return 0, err
	}
	ids, err := st.ListPendingConfirms()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		j, err := st.GetConfirmJob(id)
		if err != nil || j == nil {
			continue
		}
		deniedPayload := errs.Payload(
			errs.ExecutionFailed,
			"confirm job orphaned on daemon restart",
			nil,
		)
		updated := &store.ConfirmJob{
			ID:             j.ID,
			Verb:           j.Verb,
			Kind:           j.Kind,
			Risk:           j.Risk,
			Args:           j.Args,
			Status:         "failed",
			HTTPStatus:     sqlNullInt64(500),
			Result:         deniedPayload,
			Error:          sqlNullString(deniedPayload["error"].(string)),
			ErrorCode:      sqlNullString(errs.ExecutionFailed),
			WebhookURL:     j.WebhookURL,
			IdempotencyKey: j.IdempotencyKey,
		}
		if err := st.PutConfirmJob(updated); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// Submit persists a pending job and spawns the confirm goroutine.
// Returns the public payload the tool call hands back immediately.
func (m *Manager) Submit(verb *catalog.Verb, args map[string]any, kind, webhookURL, idempotencyKey string) (map[string]any, error) {
	jobID, err := newID()
	if err != nil {
		return nil, err
	}
	st, err := store.Get()
	if err != nil {
		return nil, err
	}
	logged := verb.PublicArgs(args)
	job := &store.ConfirmJob{
		ID:             jobID,
		Verb:           verb.Name,
		Kind:           kind,
		Risk:           sqlNullString(verb.Risk),
		Args:           logged,
		Status:         "pending",
		HTTPStatus:     sqlNullInt64(202),
		WebhookURL:     sqlNullString(webhookURL),
		IdempotencyKey: sqlNullString(idempotencyKey),
	}
	if err := st.PutConfirmJob(job); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.inFlight[jobID] = true
	m.mu.Unlock()
	go m.runJob(jobID, verb, args, kind, webhookURL)
	j, err := st.GetConfirmJob(jobID)
	if err != nil {
		return nil, err
	}
	return jobPayload(j), nil
}

// runJob is the goroutine behind one confirm: dialog on device → decision →
// execute or deny → persist → webhook.
func (m *Manager) runJob(jobID string, verb *catalog.Verb, args map[string]any, kind, webhookURL string) {
	defer func() {
		m.mu.Lock()
		delete(m.inFlight, jobID)
		m.mu.Unlock()
	}()
	st, err := store.Get()
	if err != nil {
		log.Printf("confirm: store unavailable for job %s: %v", jobID, err)
		return
	}
	logged := verb.PublicArgs(args)

	approved := riskgate.ConfirmDeviceFn(verb.Name, logged, verb.Args)
	riskgate.RecordDecision(verb.Name, verb.Risk, logged, approved)

	if !approved {
		denied := errs.Payload(
			errs.ConfirmDenied,
			fmt.Sprintf("%s: declined on-device (risk=%s)", verb.Name, verb.Risk),
			nil,
		)
		// webhook_url / idempotency_key are preserved by the upsert —
		// only the decision fields change.
		j := &store.ConfirmJob{
			ID:         jobID,
			Verb:       verb.Name,
			Kind:       kind,
			Risk:       sqlNullString(verb.Risk),
			Args:       logged,
			Status:     "denied",
			HTTPStatus: sqlNullInt64(403),
			Result:     denied,
			Error:      sqlNullString(denied["error"].(string)),
			ErrorCode:  sqlNullString(errs.ConfirmDenied),
		}
		st.PutConfirmJob(j)
		m.notify(jobID)
		return
	}

	httpStatus, payload := m.execute(kind, verb, args)
	failed := httpStatus >= 400
	j := &store.ConfirmJob{
		ID:         jobID,
		Verb:       verb.Name,
		Kind:       kind,
		Risk:       sqlNullString(verb.Risk),
		Args:       logged,
		Result:     payload,
		WebhookURL: sqlNullString(webhookURL),
	}
	if failed {
		j.Status = "failed"
		j.Error = sqlNullString(fmt.Sprintf("%v", payload["error"]))
		if code, ok := payload["code"].(string); ok {
			j.ErrorCode = sqlNullString(code)
		}
	} else {
		j.Status = "executed"
	}
	j.HTTPStatus = sqlNullInt64(int64(httpStatus))
	st.PutConfirmJob(j)
	m.notify(jobID)
}

// notify posts the final job payload to the webhook, if any.
func (m *Manager) notify(jobID string) {
	st, err := store.Get()
	if err != nil {
		return
	}
	j, err := st.GetConfirmJob(jobID)
	if err != nil || j == nil || !j.WebhookURL.Valid || j.WebhookURL.String == "" {
		return
	}
	postWebhook(j.WebhookURL.String, jobPayload(j))
}

// webhookOK validates an optional webhook URL: must be http(s) with a host.
func webhookOK(raw string) bool {
	if raw == "" {
		return true
	}
	if len(raw) > 2048 {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	return u.Host != ""
}

// WebhookOK is the exported validation used by the dispatcher before submit.
func WebhookOK(raw string) bool { return webhookOK(raw) }

func postWebhook(raw string, payload map[string]any) {
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, raw, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: webhookTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// newID returns a 16-hex-char job id (Python: uuid4().hex[:16]).
func newID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("confirm: generate id: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// ── sql null helpers ─────────────────────────────────────────────────────────

func sqlNullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func sqlNullInt64(n int64) sql.NullInt64 {
	return sql.NullInt64{Int64: n, Valid: true}
}
