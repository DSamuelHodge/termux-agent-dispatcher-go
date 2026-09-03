package mcpserver_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/catalog"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/confirm"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/mcpserver"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/riskgate"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/store"
)

// testCatalogYAML uses only commands that exist on any POSIX system so Tier A
// execution tests run on macOS/Linux CI as well as Termux.
const testCatalogYAML = `
verbs:
  test.echo:
    direction: perceive
    tier: A
    risk: none
    command: ['echo', '{"ok":true}']
    args: []
    parser: json
    timeout: 5
  test.echo_args:
    direction: act
    tier: A
    risk: low
    command: ['echo', '{text}']
    args: [text]
    parser: text
    timeout: 5
  test.fail:
    direction: perceive
    tier: A
    risk: none
    command: ['false']
    args: []
    parser: none
    timeout: 5
  test.high_risk:
    direction: act
    tier: A
    risk: high
    command: ['echo', 'done']
    args: []
    parser: text
    timeout: 5
  test.stream:
    direction: perceive
    tier: B
    risk: low
    command: ['sh', '-c', 'echo "{\"n\":1}"; sleep 0.05']
    args: []
    parser: json_stream
    timeout: null
confirmation_required_for: [high]
`

// fixture wires a Server with isolated store, audit log, token, and catalog.
type fixture struct {
	srv       *mcpserver.Server
	token     string
	auditPath string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	tmp := t.TempDir()

	// Isolated store.
	st, err := store.New(&store.Config{
		Path:         filepath.Join(tmp, "agent.db"),
		FailureLimit: store.DefaultFailureLimit,
		WindowS:      store.DefaultWindowS,
		CooldownS:    store.DefaultCooldownS,
	})
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	store.Reset(st)
	t.Cleanup(func() { store.Reset(nil) })

	// Isolated audit log.
	auditPath := filepath.Join(tmp, "audit.log")
	riskgate.SetLogPath(auditPath)

	// Deterministic token + gated mode restored after each test.
	t.Setenv("AGENT_TOKEN", "test-token-not-for-production")
	t.Cleanup(func() { riskgate.SetMode(riskgate.ModeGated) })

	// Stub the on-device dialog by default so no test ever blocks on a
	// real termux-dialog. Individual tests override the decision.
	riskgate.ConfirmDeviceFn = func(string, map[string]any, []string) bool {
		return false // fail-closed default
	}
	t.Cleanup(func() { riskgate.ConfirmDeviceFn = riskgate.ConfirmOnDevice })

	catalogPath := filepath.Join(tmp, "verbs.yaml")
	if err := os.WriteFile(catalogPath, []byte(testCatalogYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	srv, err := mcpserver.New(&mcpserver.Config{
		CatalogPath: catalogPath,
		TokenPath:   filepath.Join(tmp, ".agent-token"),
		ModePath:    filepath.Join(tmp, ".autonomy-mode"),
	})
	if err != nil {
		t.Fatalf("mcpserver.New: %v", err)
	}
	return &fixture{srv: srv, token: "test-token-not-for-production", auditPath: auditPath}
}

// connectMCP builds an in-memory client↔server pair. No elicitation handler
// is ever installed: the dispatcher must not depend on client elicitation
// for approval (the on-device dialog is the sole approval surface).
func (f *fixture) connectMCP(t *testing.T) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := f.srv.MCPServer().Connect(ctx, serverT, nil); err != nil {
		t.Fatalf("server Connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.1"}, nil)
	session, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

// auditStages returns every stage value in the fixture's audit log, in order.
func (f *fixture) auditStages(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(f.auditPath)
	if err != nil {
		return nil
	}
	var stages []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err == nil {
			if s, ok := ev["stage"].(string); ok {
				stages = append(stages, s)
			}
		}
	}
	return stages
}

func resultText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if res == nil || len(res.Content) == 0 {
		return ""
	}
	if tc, ok := res.Content[0].(*mcp.TextContent); ok {
		return tc.Text
	}
	return ""
}

// resultPayload parses the tool result text as JSON.
func resultPayload(t *testing.T, res *mcp.CallToolResult) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(resultText(t, res)), &payload); err != nil {
		t.Fatalf("result JSON: %v (raw %q)", err, resultText(t, res))
	}
	return payload
}

func hasStage(stages []string, want string) bool {
	for _, s := range stages {
		if s == want {
			return true
		}
	}
	return false
}

// callTool is a small helper for a single tools/call.
func callTool(t *testing.T, session *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool %s: %v", name, err)
	}
	return res
}

// pollConfirm calls confirm.poll until the job leaves pending, returning
// the final payload.
func pollConfirm(t *testing.T, session *mcp.ClientSession, confirmID string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		res := callTool(t, session, "confirm.poll", map[string]any{"confirm_id": confirmID})
		payload := resultPayload(t, res)
		if status, _ := payload["status"].(string); status != "pending" {
			return payload
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("confirm job never left pending")
	return nil
}

// ── Auth + health (HTTP layer) ────────────────────────────────────────────────

func TestHealthEndpoint(t *testing.T) {
	f := newFixture(t)
	srv := httptest.NewServer(f.srv.Handler())
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/health", nil)
	req.Header.Set("X-Agent-Token", f.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("health status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("health JSON: %v", err)
	}
	if payload["ok"] != true {
		t.Errorf("health ok = %v", payload["ok"])
	}
	if payload["verbs"].(float64) != 5 {
		t.Errorf("health verbs = %v, want 5", payload["verbs"])
	}
	if payload["autonomy_mode"] != "gated" {
		t.Errorf("health autonomy_mode = %v, want gated", payload["autonomy_mode"])
	}
}

func TestHealthRequiresAuth(t *testing.T) {
	f := newFixture(t)
	srv := httptest.NewServer(f.srv.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("health without token: status = %d, want 401", resp.StatusCode)
	}
}

func TestMCPRequiresAuth(t *testing.T) {
	f := newFixture(t)
	srv := httptest.NewServer(f.srv.Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/mcp", "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`))
	if err != nil {
		t.Fatalf("POST /mcp: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("mcp without token: status = %d, want 401", resp.StatusCode)
	}
}

// ── Tier A via MCP ────────────────────────────────────────────────────────────

func TestTierACall(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)
	res := callTool(t, session, "test.echo", map[string]any{})
	if res.IsError {
		t.Fatalf("test.echo returned error: %s", resultText(t, res))
	}
	payload := resultPayload(t, res)
	if payload["ok"] != true {
		t.Errorf("payload ok = %v", payload["ok"])
	}
	stages := f.auditStages(t)
	if !hasStage(stages, "requested") || !hasStage(stages, "executed") {
		t.Errorf("expected requested+executed in audit, got %v", stages)
	}
	if hasStage(stages, "approved") {
		t.Errorf("unexpected 'approved' for risk:none verb, got %v", stages)
	}
}

func TestTierAUnknownTool(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)
	_, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "test.nonexistent",
		Arguments: map[string]any{},
	})
	if err == nil {
		t.Error("expected error for unknown tool")
	}
}

func TestTierAArgValidationBeforeGate(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)
	res := callTool(t, session, "test.echo_args", map[string]any{}) // missing required 'text'
	if !res.IsError {
		t.Fatal("expected IsError for missing args")
	}
	if !strings.Contains(resultText(t, res), "missing required args") {
		t.Errorf("expected 'missing required args', got %q", resultText(t, res))
	}
	// Validation happens before the gate: no audit should be written.
	if stages := f.auditStages(t); len(stages) != 0 {
		t.Errorf("no audit expected for validation failure, got %v", stages)
	}
}

func TestTierAExecutionFailure(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)
	res := callTool(t, session, "test.fail", map[string]any{})
	if !res.IsError {
		t.Fatal("expected IsError for failing command")
	}
	if !strings.Contains(resultText(t, res), "exit code") {
		t.Errorf("expected 'exit code', got %q", resultText(t, res))
	}
	if !hasStage(f.auditStages(t), "failed") {
		t.Errorf("expected 'failed' in audit, got %v", f.auditStages(t))
	}
}

// ── Async on-device confirmation ──────────────────────────────────────────────

// TestHighRiskPendingConfirm: a high-risk act returns a pending confirm_id
// immediately, does not execute, and does not depend on client elicitation.
func TestHighRiskPendingConfirm(t *testing.T) {
	f := newFixture(t)
	// Block the dialog so the job stays pending while we assert.
	release := make(chan struct{})
	var once sync.Once
	releaseFn := func() { once.Do(func() { close(release) }) }
	riskgate.ConfirmDeviceFn = func(string, map[string]any, []string) bool {
		<-release
		return false
	}
	t.Cleanup(releaseFn)
	session := f.connectMCP(t)

	res := callTool(t, session, "test.high_risk", map[string]any{"idempotency_key": "k1"})
	if res.IsError {
		t.Fatalf("pending confirm must be a success result, got %q", resultText(t, res))
	}
	payload := resultPayload(t, res)
	confirmID, _ := payload["confirm_id"].(string)
	if confirmID == "" {
		t.Fatalf("no confirm_id in %v", payload)
	}
	if payload["status"] != "pending" {
		t.Errorf("status = %v, want pending", payload["status"])
	}
	if payload["code"] != "CONFIRM_PENDING" {
		t.Errorf("code = %v, want CONFIRM_PENDING", payload["code"])
	}
	if payload["poll"] == nil {
		t.Errorf("expected poll handle in %v", payload)
	}

	// Nothing executed yet; only "requested" is audited.
	stages := f.auditStages(t)
	if !hasStage(stages, "requested") {
		t.Errorf("expected 'requested', got %v", stages)
	}
	if hasStage(stages, "executed") || hasStage(stages, "denied") {
		t.Errorf("verb must not resolve yet, got %v", stages)
	}

	// Poll before resolution: stays pending.
	pollRes := callTool(t, session, "confirm.poll", map[string]any{"confirm_id": confirmID})
	if pollRes.IsError {
		t.Fatalf("pending poll should not be an error: %q", resultText(t, pollRes))
	}
	if got := resultPayload(t, pollRes)["status"]; got != "pending" {
		t.Errorf("poll status = %v, want pending", got)
	}

	// Release the dialog (deny) — job resolves to denied.
	releaseFn()
	final := pollConfirm(t, session, confirmID)
	if final["status"] != "denied" {
		t.Fatalf("after release, status = %v (payload %v)", final["status"], final)
	}
}

// TestHighRiskApprovedViaDialog: the on-device dialog approves → the job
// resolves to executed with the verb's result.
func TestHighRiskApprovedViaDialog(t *testing.T) {
	f := newFixture(t)
	riskgate.ConfirmDeviceFn = func(string, map[string]any, []string) bool { return true }
	session := f.connectMCP(t)

	res := callTool(t, session, "test.high_risk", map[string]any{"idempotency_key": "k2"})
	confirmID := resultPayload(t, res)["confirm_id"].(string)

	final := pollConfirm(t, session, confirmID)
	if final["status"] != "executed" {
		t.Fatalf("status = %v, want executed (payload %v)", final["status"], final)
	}
	// The verb result is embedded in the job payload.
	if inner, ok := final["result"].(map[string]any); ok && inner["data"] != "done" {
		t.Errorf("unexpected result %v", final["result"])
	}
	stages := f.auditStages(t)
	if !hasStage(stages, "requested") || !hasStage(stages, "approved") || !hasStage(stages, "executed") {
		t.Errorf("expected requested+approved+executed, got %v", stages)
	}
}

// TestHighRiskDeniedViaDialog: the on-device dialog denies → the job
// resolves to denied, the verb never executes.
func TestHighRiskDeniedViaDialog(t *testing.T) {
	f := newFixture(t)
	// ConfirmDeviceFn already stubbed to false (fail-closed) by the fixture.
	session := f.connectMCP(t)

	res := callTool(t, session, "test.high_risk", map[string]any{"idempotency_key": "k3"})
	confirmID := resultPayload(t, res)["confirm_id"].(string)

	final := pollConfirm(t, session, confirmID)
	if final["status"] != "denied" {
		t.Fatalf("status = %v, want denied (payload %v)", final["status"], final)
	}
	if final["code"] != "CONFIRM_DENIED" {
		t.Errorf("code = %v, want CONFIRM_DENIED", final["code"])
	}
	stages := f.auditStages(t)
	if !hasStage(stages, "denied") {
		t.Errorf("expected 'denied' in audit, got %v", stages)
	}
	if hasStage(stages, "executed") {
		t.Errorf("verb must not execute after denial, got %v", stages)
	}
}

// TestHighRiskFailClosedNoDialogBinary: ConfirmOnDevice with no
// termux-dialog binary (macOS CI) counts as denial, never approval.
func TestHighRiskFailClosedNoDialogBinary(t *testing.T) {
	f := newFixture(t)
	// Use the REAL dialog path: termux-dialog does not exist here.
	riskgate.ConfirmDeviceFn = riskgate.ConfirmOnDevice
	session := f.connectMCP(t)

	res := callTool(t, session, "test.high_risk", map[string]any{"idempotency_key": "k4"})
	confirmID := resultPayload(t, res)["confirm_id"].(string)

	final := pollConfirm(t, session, confirmID)
	if final["status"] != "denied" {
		t.Fatalf("missing dialog binary must deny, got %v", final)
	}
}

// TestConfirmPollUnknownID: polling a bogus handle is CONFIRM_NOT_FOUND.
func TestConfirmPollUnknownID(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)
	res := callTool(t, session, "confirm.poll", map[string]any{"confirm_id": "bogus"})
	if !res.IsError {
		t.Fatal("expected IsError for unknown confirm id")
	}
	if !strings.Contains(resultText(t, res), "CONFIRM_NOT_FOUND") {
		t.Errorf("expected CONFIRM_NOT_FOUND, got %q", resultText(t, res))
	}
}

// ── Idempotency ───────────────────────────────────────────────────────────────

func TestHighRiskRequiresIdempotencyKey(t *testing.T) {
	f := newFixture(t)
	riskgate.ConfirmDeviceFn = func(string, map[string]any, []string) bool { return true }
	session := f.connectMCP(t)

	res := callTool(t, session, "test.high_risk", map[string]any{})
	if !res.IsError {
		t.Fatal("expected IsError without idempotency_key")
	}
	if !strings.Contains(resultText(t, res), "MISSING_IDEMPOTENCY_KEY") {
		t.Errorf("expected MISSING_IDEMPOTENCY_KEY, got %q", resultText(t, res))
	}
}

func TestIdempotencyReplayAfterExecution(t *testing.T) {
	f := newFixture(t)
	riskgate.ConfirmDeviceFn = func(string, map[string]any, []string) bool { return true }
	session := f.connectMCP(t)

	args := map[string]any{"idempotency_key": "replay-1"}
	res1 := callTool(t, session, "test.high_risk", args)
	confirmID := resultPayload(t, res1)["confirm_id"].(string)
	pollConfirm(t, session, confirmID)

	// Same key + same args → the resolved job payload, no second execution.
	res2 := callTool(t, session, "test.high_risk", args)
	if res2.IsError {
		t.Fatalf("replay must succeed: %q", resultText(t, res2))
	}
	replay := resultPayload(t, res2)
	if replay["status"] != "executed" {
		t.Errorf("replay should return the executed job, got %v", replay)
	}
	// Exactly one dialog decision + one execution in the audit.
	stages := f.auditStages(t)
	if n := countStage(stages, "executed"); n != 1 {
		t.Errorf("executed count = %d, want 1 (stages %v)", n, stages)
	}
	if n := countStage(stages, "approved"); n != 1 {
		t.Errorf("approved count = %d, want 1 (stages %v)", n, stages)
	}
}

func TestIdempotencyConflict(t *testing.T) {
	f := newFixture(t)
	riskgate.ConfirmDeviceFn = func(string, map[string]any, []string) bool { return true }
	session := f.connectMCP(t)

	res := callTool(t, session, "test.echo_args", map[string]any{"text": "a", "idempotency_key": "c1"})
	if res.IsError {
		t.Fatalf("first call: %q", resultText(t, res))
	}
	// Same key, different args → conflict.
	res2 := callTool(t, session, "test.echo_args", map[string]any{"text": "b", "idempotency_key": "c1"})
	if !res2.IsError {
		t.Fatal("expected IsError for idempotency conflict")
	}
	if !strings.Contains(resultText(t, res2), "IDEMPOTENCY_CONFLICT") {
		t.Errorf("expected IDEMPOTENCY_CONFLICT, got %q", resultText(t, res2))
	}
}

func countStage(stages []string, want string) int {
	n := 0
	for _, s := range stages {
		if s == want {
			n++
		}
	}
	return n
}

// ── Dry run ───────────────────────────────────────────────────────────────────

func TestDryRun(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)

	res := callTool(t, session, "test.high_risk", map[string]any{"dry_run": true})
	if res.IsError {
		t.Fatalf("dry_run: %q", resultText(t, res))
	}
	payload := resultPayload(t, res)
	if payload["dry_run"] != true || payload["ok"] != true {
		t.Errorf("unexpected dry_run payload %v", payload)
	}
	argv, _ := payload["argv"].([]any)
	if len(argv) != 2 || argv[1] != "done" {
		t.Errorf("argv = %v, want [echo done]", argv)
	}
	if payload["confirmation_required"] != true {
		t.Errorf("confirmation_required = %v, want true", payload["confirmation_required"])
	}
	if payload["idempotency_required"] != true {
		t.Errorf("idempotency_required = %v, want true", payload["idempotency_required"])
	}
	// dry_run runs before the gate: no audit, no execution.
	if stages := f.auditStages(t); len(stages) != 0 {
		t.Errorf("no audit expected for dry_run, got %v", stages)
	}
}

// ── Tier B watch.* ────────────────────────────────────────────────────────────

func TestWatchStartPollStop(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)

	startRes := callTool(t, session, "watch.start", map[string]any{"verb": "test.stream"})
	if startRes.IsError {
		t.Fatalf("watch.start error: %s", resultText(t, startRes))
	}
	startPayload := resultPayload(t, startRes)
	subID, _ := startPayload["id"].(string)
	if subID == "" {
		t.Fatalf("no subscription id in %v", startPayload)
	}

	// Poll until items arrive.
	var pollPayload map[string]any
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		pollRes := callTool(t, session, "watch.poll", map[string]any{"id": subID})
		pollPayload = resultPayload(t, pollRes)
		if items, ok := pollPayload["items"].([]any); ok && len(items) > 0 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if items, _ := pollPayload["items"].([]any); len(items) == 0 {
		t.Fatal("expected items from watch.poll")
	}

	stopRes := callTool(t, session, "watch.stop", map[string]any{"id": subID})
	if stopRes.IsError {
		t.Fatalf("watch.stop error: %s", resultText(t, stopRes))
	}
	if !hasStage(f.auditStages(t), "stopped") {
		t.Errorf("expected 'stopped' in audit, got %v", f.auditStages(t))
	}
}

func TestWatchStartRejectsTierA(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)
	res := callTool(t, session, "watch.start", map[string]any{"verb": "test.echo"})
	if !res.IsError {
		t.Fatal("expected IsError for Tier A verb on watch.start")
	}
	if !strings.Contains(resultText(t, res), "does not support route kind") {
		t.Errorf("unexpected message: %q", resultText(t, res))
	}
}

func TestWatchPollUnknownID(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)
	res := callTool(t, session, "watch.poll", map[string]any{"id": "nonexistent"})
	if !res.IsError {
		t.Fatal("expected IsError for unknown subscription")
	}
}

// ── autonomy.set_mode ─────────────────────────────────────────────────────────

func TestSetModeGatedToFull(t *testing.T) {
	f := newFixture(t)
	riskgate.ConfirmDeviceFn = func(name string, _ map[string]any, _ []string) bool {
		return name == "autonomy.set_mode" // approve only the mode switch
	}
	session := f.connectMCP(t)

	res := callTool(t, session, "autonomy.set_mode", map[string]any{"mode": "full", "idempotency_key": "m1"})
	if res.IsError {
		t.Fatalf("set_mode error: %s", resultText(t, res))
	}
	confirmID := resultPayload(t, res)["confirm_id"].(string)
	final := pollConfirm(t, session, confirmID)
	if final["status"] != "executed" {
		t.Fatalf("mode switch status = %v (payload %v)", final["status"], final)
	}
	if riskgate.GetMode() != riskgate.ModeFull {
		t.Errorf("mode = %q, want full", riskgate.GetMode())
	}
	if !hasStage(f.auditStages(t), "mode_changed") {
		t.Errorf("expected 'mode_changed' in audit, got %v", f.auditStages(t))
	}

	// In full mode, the high-risk verb executes without a dialog — but the
	// approved line is still written (audit unconditional in every mode).
	res2 := callTool(t, session, "test.high_risk", map[string]any{"idempotency_key": "m2"})
	if res2.IsError {
		t.Fatalf("high_risk in full mode: %s", resultText(t, res2))
	}
	stages := f.auditStages(t)
	if !hasStage(stages, "requested") || !hasStage(stages, "approved") || !hasStage(stages, "executed") {
		t.Errorf("expected full-mode audit requested+approved+executed, got %v", stages)
	}
}

func TestSetModeRequiresConfirmEvenInFull(t *testing.T) {
	f := newFixture(t)
	riskgate.ConfirmDeviceFn = func(name string, _ map[string]any, _ []string) bool {
		return name == "autonomy.set_mode"
	}
	session := f.connectMCP(t)

	res := callTool(t, session, "autonomy.set_mode", map[string]any{"mode": "full", "idempotency_key": "f1"})
	confirmID := resultPayload(t, res)["confirm_id"].(string)
	pollConfirm(t, session, confirmID)
	if riskgate.GetMode() != riskgate.ModeFull {
		t.Fatalf("mode = %q, want full", riskgate.GetMode())
	}

	// Switching back to gated still goes through the dialog. Deny it:
	// ConfirmDeviceFn only approves the FIRST call (the switch to full).
	riskgate.ConfirmDeviceFn = func(string, map[string]any, []string) bool { return false }
	res2 := callTool(t, session, "autonomy.set_mode", map[string]any{"mode": "gated", "idempotency_key": "f2"})
	confirmID2 := resultPayload(t, res2)["confirm_id"].(string)
	final := pollConfirm(t, session, confirmID2)
	if final["status"] != "denied" {
		t.Fatalf("denied switch status = %v (payload %v)", final["status"], final)
	}
	if riskgate.GetMode() != riskgate.ModeFull {
		t.Errorf("mode should remain full after denied switch, got %q", riskgate.GetMode())
	}
}

func TestSetModeDeniedKeepsMode(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)

	res := callTool(t, session, "autonomy.set_mode", map[string]any{"mode": "full", "idempotency_key": "d1"})
	confirmID := resultPayload(t, res)["confirm_id"].(string)
	final := pollConfirm(t, session, confirmID)
	if final["status"] != "denied" {
		t.Fatalf("status = %v, want denied (payload %v)", final["status"], final)
	}
	if riskgate.GetMode() != riskgate.ModeGated {
		t.Errorf("mode should remain gated after denial, got %q", riskgate.GetMode())
	}
	if !hasStage(f.auditStages(t), "denied") {
		t.Errorf("expected 'denied' in audit, got %v", f.auditStages(t))
	}
}

func TestSetModeInvalidValue(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)
	res := callTool(t, session, "autonomy.set_mode", map[string]any{"mode": "bogus", "idempotency_key": "i1"})
	if !res.IsError {
		t.Fatal("expected IsError for invalid mode")
	}
	if !strings.Contains(resultText(t, res), "invalid mode") {
		t.Errorf("unexpected message: %q", resultText(t, res))
	}
}

func TestSetModeRequiresIdempotencyKey(t *testing.T) {
	f := newFixture(t)
	session := f.connectMCP(t)
	res := callTool(t, session, "autonomy.set_mode", map[string]any{"mode": "full"})
	if !res.IsError {
		t.Fatal("expected IsError without idempotency_key")
	}
	if !strings.Contains(resultText(t, res), "MISSING_IDEMPOTENCY_KEY") {
		t.Errorf("expected MISSING_IDEMPOTENCY_KEY, got %q", resultText(t, res))
	}
}

// ── Orphan recovery ───────────────────────────────────────────────────────────

// TestFailOrphans: pending confirm rows from a previous process are marked
// failed on boot — a dialog cannot resume across a restart.
func TestFailOrphans(t *testing.T) {
	_ = newFixture(t) // isolate store + audit + ConfirmDeviceFn
	st, _ := store.Get()
	if err := st.PutConfirmJob(&store.ConfirmJob{
		ID:     "orphan1",
		Verb:   "test.high_risk",
		Kind:   "act",
		Status: "pending",
	}); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}

	m := confirm.NewManager(func(kind string, v *catalog.Verb, args map[string]any) (int, map[string]any) {
		return 200, nil
	})
	n, err := m.FailOrphans()
	if err != nil {
		t.Fatalf("FailOrphans: %v", err)
	}
	if n != 1 {
		t.Fatalf("failed orphans = %d, want 1", n)
	}
	job, _ := st.GetConfirmJob("orphan1")
	if job == nil || job.Status != "failed" {
		t.Fatalf("orphan status = %+v, want failed", job)
	}
}
