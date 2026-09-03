// Package mcpserver implements the MCP 2026-07-28 stateless server.
//
// Each verbs.yaml entry becomes one MCP tool (Tier A) or is reached through
// the three watch.* tools (Tier B). The autonomy.set_mode synthetic tool and
// the confirm.poll tool are registered here directly, not from verbs.yaml.
//
// Auth: HTTP middleware wraps StreamableHTTPHandler and checks X-Agent-Token
// before any MCP dispatch. Timing-safe compare via crypto/subtle.
//
// Health: GET /health is a plain HTTP endpoint outside the MCP surface.
// It is not a token-gated MCP tool; it uses the same auth middleware but
// returns a simple JSON body for shell smoke tests.
//
// Confirmation: high-risk verbs do NOT block the tool call and do NOT ask
// the MCP client for approval. The call returns immediately with a pending
// confirm_id; a goroutine runs termux-dialog on the device (the sole
// approval surface); the client polls confirm.poll. See internal/confirm.
package mcpserver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/catalog"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/confirm"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/errs"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/riskgate"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/store"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/tiera"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/tierb"
)

const (
	defaultHost = "127.0.0.1"
	defaultPort = 8477
	maxBody     = 1 << 20 // 1 MiB — stdin payloads are text; refuse unbounded reads
)

// envelopeKeys are stripped from tool arguments before verb arg validation.
// Mirrors catalog.ENVELOPE_KEYS in Python.
var envelopeKeys = map[string]bool{
	"dry_run":         true,
	"idempotency_key": true,
	"webhook_url":     true,
}

// autonomyVerb is the synthetic pseudo-verb behind autonomy.set_mode.
// Always risk:high — switching what "are you sure" means is itself the one
// call that must never slip through unconfirmed.
func autonomyVerb() *catalog.Verb {
	return &catalog.Verb{
		Name:      "autonomy.set_mode",
		Direction: "act",
		Tier:      "A",
		Risk:      "high",
		Command:   []string{"autonomy.set_mode"},
		Args:      []string{"mode"},
		Parser:    "none",
	}
}

// Server is the top-level MCP daemon server.
type Server struct {
	cat       *catalog.Catalog
	subs      *tierb.Manager
	confirms  *confirm.Manager
	token     string
	modePath  string
	startedAt time.Time
	mcpServer *mcp.Server
}

// Config carries construction-time dependencies.
type Config struct {
	CatalogPath string // path to verbs.yaml
	TokenPath   string // path to .agent-token
	ModePath    string // path to .autonomy-mode
	Host        string // listen address, default 127.0.0.1
	Port        int    // listen port, default 8477
}

// New builds a Server. Loads the catalog, the auth token, and the autonomy
// mode config; opens the durable store; fails orphaned confirm jobs.
func New(cfg *Config) (*Server, error) {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.Host == "" {
		cfg.Host = defaultHost
	}
	if cfg.Port == 0 {
		cfg.Port = defaultPort
	}

	cat, err := catalog.Load(cfg.CatalogPath)
	if err != nil {
		return nil, fmt.Errorf("mcpserver: load catalog: %w", err)
	}

	token, err := loadOrCreateToken(cfg.TokenPath)
	if err != nil {
		return nil, fmt.Errorf("mcpserver: load token: %w", err)
	}

	// Read autonomy mode from config file; defaults to gated.
	mode, err := riskgate.LoadModeFromFile(cfg.ModePath)
	if err != nil {
		log.Printf("mcpserver: autonomy mode file: %v — defaulting to gated", err)
		mode = riskgate.ModeGated
	}
	riskgate.SetMode(mode)

	// Open the durable store now — confirm jobs and the circuit breaker
	// depend on it; better to fail at boot than on first call.
	if _, err := store.Get(); err != nil {
		return nil, fmt.Errorf("mcpserver: store: %w", err)
	}

	s := &Server{
		cat:       cat,
		subs:      tierb.NewManager(),
		token:     token,
		modePath:  cfg.ModePath,
		startedAt: time.Now(),
	}
	s.confirms = confirm.NewManager(s.executeKind)

	// Pending rows from a previous process cannot resume a dialog.
	if n, err := s.confirms.FailOrphans(); err != nil {
		log.Printf("mcpserver: fail orphaned confirms: %v", err)
	} else if n > 0 {
		log.Printf("mcpserver: failed %d orphaned confirm job(s) from previous run", n)
	}

	s.mcpServer = s.buildMCPServer()
	return s, nil
}

// MCPServer exposes the underlying *mcp.Server. Used by tests to connect
// in-memory transports; production code should use Handler/ListenAndServe.
func (s *Server) MCPServer() *mcp.Server { return s.mcpServer }

// Handler returns the root http.Handler: auth middleware → /health → MCP.
func (s *Server) Handler() http.Handler {
	mcpHandler := mcp.NewStreamableHTTPHandler(
		func(r *http.Request) *mcp.Server { return s.mcpServer },
		&mcp.StreamableHTTPOptions{
			Stateless:    true,
			JSONResponse: true,
		},
	)

	mux := http.NewServeMux()
	mux.Handle("/health", s.authMiddleware(http.HandlerFunc(s.handleHealth)))
	mux.Handle("/mcp", s.authMiddleware(mcpHandler))
	return mux
}

// ListenAndServe binds and serves. Blocks until the server exits.
func (s *Server) ListenAndServe() error {
	addr := fmt.Sprintf("%s:%d", defaultHost, defaultPort)
	log.Printf("agent daemon listening on %s — %d verbs loaded (mode: %s)",
		addr, len(s.cat.Verbs), riskgate.GetMode())
	return http.ListenAndServe(addr, s.Handler())
}

// ── MCP server construction ───────────────────────────────────────────────────

func (s *Server) buildMCPServer() *mcp.Server {
	srv := mcp.NewServer(
		&mcp.Implementation{
			Name:    "termux-agent-dispatcher",
			Version: "1.0.0",
		},
		nil,
	)

	// Tier A verbs: one tool per verb.
	for _, verb := range s.cat.Verbs {
		if verb.Tier != "A" {
			continue
		}
		s.registerTierATool(srv, verb)
	}

	// Tier B: three generic watch.* tools.
	s.registerTierBTools(srv)

	// confirm.poll — how the brain resolves an async on-device confirm.
	s.registerConfirmPollTool(srv)

	// Synthetic autonomy.set_mode tool — always risk:high confirmation,
	// regardless of current mode. Not from verbs.yaml.
	s.registerAutonomyTool(srv)

	return srv
}

// registerTierATool adds one MCP tool for a Tier A verb.
func (s *Server) registerTierATool(srv *mcp.Server, verb *catalog.Verb) {
	description := fmt.Sprintf(
		"direction:%s tier:A risk:%s parser:%s timeout:%v args:%v",
		verb.Direction, verb.Risk, verb.Parser, verb.Timeout, verb.Args,
	)
	if s.cat.ConfirmationRequiredFor[verb.Risk] {
		description += " High-risk: returns a pending confirm_id; poll confirm.poll. " +
			"On-device Yes/No; does not block the MCP request."
	}
	mcp.AddTool(srv, &mcp.Tool{
		Name:        verb.Name,
		Description: description,
	}, func(ctx context.Context, req *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		args, envelope := splitEnvelope(input)
		isErr, payload := s.dispatch(verb.Name, args, verb.Direction, envelope)
		return payloadResult(payload, isErr)
	})
}

// registerTierBTools adds watch.start / watch.poll / watch.stop.
func (s *Server) registerTierBTools(srv *mcp.Server) {
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "watch.start",
		Description: "Start a Tier B subscription. Args: verb (string, required) + verb-specific args.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		verbName, _ := input["verb"].(string)
		if verbName == "" {
			return payloadResult(errs.Payload(errs.InvalidArgs, "watch.start: missing required argument 'verb'", nil), true)
		}
		if _, err := s.cat.Get(verbName); err != nil {
			return payloadResult(errs.Payload(errs.UnknownVerb, err.Error(), nil), true)
		}
		// Strip the meta 'verb' key, then split the envelope.
		rest := make(map[string]any, len(input)-1)
		for k, v := range input {
			if k != "verb" {
				rest[k] = v
			}
		}
		args, envelope := splitEnvelope(rest)
		isErr, payload := s.dispatch(verbName, args, "watch", envelope)
		return payloadResult(payload, isErr)
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "watch.poll",
		Description: "Poll a Tier B subscription. Args: id (string, required), max_items (int, optional).",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		subID, _ := input["id"].(string)
		if subID == "" {
			return payloadResult(errs.Payload(errs.InvalidArgs, "watch.poll: missing required argument 'id'", nil), true)
		}
		maxItems := 50
		if v, ok := input["max_items"].(float64); ok {
			maxItems = int(v)
		}
		result, err := s.subs.Poll(subID, maxItems)
		if err != nil {
			return payloadResult(errs.Payload(errs.InvalidArgs, err.Error(), nil), true)
		}
		return payloadResult(result, false)
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "watch.stop",
		Description: "Stop a Tier B subscription. Args: id (string, required).",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		subID, _ := input["id"].(string)
		if subID == "" {
			return payloadResult(errs.Payload(errs.InvalidArgs, "watch.stop: missing required argument 'id'", nil), true)
		}
		verbName, err := s.subs.Stop(subID)
		if err != nil {
			return payloadResult(errs.Payload(errs.InvalidArgs, err.Error(), nil), true)
		}
		riskgate.Audit(riskgate.AuditEvent{
			Verb:         verbName,
			Stage:        "stopped",
			Subscription: subID,
		})
		return payloadResult(map[string]any{"ok": true}, false)
	})
}

// registerConfirmPollTool adds confirm.poll — the stateless handle the brain
// passes back to resolve an async on-device confirmation.
func (s *Server) registerConfirmPollTool(srv *mcp.Server) {
	mcp.AddTool(srv, &mcp.Tool{
		Name: "confirm.poll",
		Description: "Poll an async on-device confirm job by confirm_id. " +
			"MCP is stateless; pass the handle from the original tools/call.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		confirmID, _ := input["confirm_id"].(string)
		if confirmID == "" {
			return payloadResult(errs.Payload(errs.InvalidArgs, "confirm_id required", nil), true)
		}
		payload, verbStatus, err := s.confirms.Poll(confirmID)
		if err != nil {
			return payloadResult(errs.Payload(errs.ExecutionFailed, err.Error(), nil), true)
		}
		if payload == nil {
			return payloadResult(errs.Payload(errs.ConfirmNotFound, fmt.Sprintf("unknown confirm id: %s", confirmID), nil), true)
		}
		// Poll stays a success while pending; a denied or failed terminal
		// state surfaces as a tool error so clients can stop looping.
		status, _ := payload["status"].(string)
		isErr := status == "denied" || status == "failed"
		if verbStatus > 0 {
			payload["verb_http_status"] = verbStatus
		}
		return payloadResult(payload, isErr)
	})
}

// registerAutonomyTool adds the autonomy.set_mode synthetic tool.
// Always requires an on-device confirm regardless of current mode — this is
// the one moment where "are you sure" still matters, precisely because it is
// what turns "are you sure" off for everything downstream.
func (s *Server) registerAutonomyTool(srv *mcp.Server) {
	mcp.AddTool(srv, &mcp.Tool{
		Name: "autonomy.set_mode",
		Description: "Set the dispatcher autonomy mode. " +
			"mode must be one of: manual (confirm on risk:medium+), " +
			"gated (confirm on risk:high), full (no confirmation). " +
			"Returns a pending confirm_id; the on-device dialog is the sole " +
			"approval surface. Poll confirm.poll. idempotency_key required.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		modeStr, _ := input["mode"].(string)
		switch riskgate.AutonomyMode(modeStr) {
		case riskgate.ModeManual, riskgate.ModeGated, riskgate.ModeFull:
			// valid
		default:
			return payloadResult(errs.Payload(errs.InvalidArgs,
				fmt.Sprintf("autonomy.set_mode: invalid mode %q (expected manual|gated|full)", modeStr), nil), true)
		}
		args := map[string]any{"mode": modeStr}
		idem, _ := input["idempotency_key"].(string)
		if idem == "" {
			return payloadResult(errs.Payload(errs.MissingIdempotencyKey,
				"autonomy.set_mode: idempotency_key is required for risk=high act", nil), true)
		}
		isErr, payload := s.dispatchSynthetic(autonomyVerb(), args, "mode", idem)
		return payloadResult(payload, isErr)
	})
}

// ── Dispatch (mirrors dispatch/engine.py) ─────────────────────────────────────

// dispatch is the single entry point for verb calls. Ordering is fixed:
// unknown verb → route/kind contract → malformed payload → webhook check →
// dry_run → idempotency → risk gate (audit + breaker) → async confirm or
// execution. Returns (isError, payload).
func (s *Server) dispatch(verbName string, args map[string]any, kind string, envelope map[string]any) (bool, map[string]any) {
	verb, err := s.cat.Get(verbName)
	if err != nil {
		return true, errs.Payload(errs.UnknownVerb, err.Error(), nil)
	}
	return s.dispatchVerb(verb, args, kind, envelope)
}

// dispatchSynthetic runs the same pipeline for the autonomy.set_mode
// pseudo-verb (idempotency without webhook/dry_run).
func (s *Server) dispatchSynthetic(verb *catalog.Verb, args map[string]any, kind, idem string) (bool, map[string]any) {
	if _, err := verb.BuildArgv(args); err != nil {
		return true, errs.Payload(errs.InvalidArgs, err.Error(), nil)
	}
	if idem != "" {
		if done, isErr, payload := s.idempotencyReplay(verb.Name, idem, args); done {
			return isErr, payload
		}
	}
	if err := s.precheckSynthetic(verb, args); err != nil {
		return gateError(err)
	}
	payload, err := s.confirms.Submit(verb, args, kind, "", idem)
	if err != nil {
		return true, errs.Payload(errs.ExecutionFailed, err.Error(), nil)
	}
	if idem != "" {
		s.idempotencyRecord(verb.Name, idem, args, 202, payload, payload["confirm_id"])
	}
	return false, payload // 202 pending is a successful MCP result with a handle
}

func (s *Server) dispatchVerb(verb *catalog.Verb, args map[string]any, kind string, envelope map[string]any) (bool, map[string]any) {
	// Route/kind contract.
	switch kind {
	case "perceive", "act":
		if verb.Tier != "A" || verb.Direction != kind {
			return true, errs.Payload(errs.InvalidRoute,
				fmt.Sprintf("%s: tier %s direction %q does not support route kind %q",
					verb.Name, verb.Tier, verb.Direction, kind), nil)
		}
	case "watch":
		if verb.Tier != "B" {
			return true, errs.Payload(errs.InvalidRoute,
				fmt.Sprintf("%s: tier %s does not support route kind %q", verb.Name, verb.Tier, kind), nil)
		}
	default:
		return true, errs.Payload(errs.InvalidRoute, "not found", nil)
	}

	// Validate BEFORE gating: a human should never be prompted to approve
	// a call the machine can already prove is broken.
	if _, err := verb.BuildArgv(args); err != nil {
		return true, errs.Payload(errs.InvalidArgs, err.Error(), nil)
	}
	if _, err := verb.StdinPayload(args); err != nil {
		return true, errs.Payload(errs.InvalidArgs, err.Error(), nil)
	}

	webhook, _ := envelope["webhook_url"].(string)
	if webhook != "" && !confirm.WebhookOK(webhook) {
		return true, errs.Payload(errs.InvalidBody, "webhook_url must be http(s)", nil)
	}

	if dryRun, _ := envelope["dry_run"].(bool); dryRun {
		payload, err := s.dryRunPayload(verb, args)
		if err != nil {
			return true, errs.Payload(errs.InvalidArgs, err.Error(), nil)
		}
		return false, payload
	}

	idem, _ := envelope["idempotency_key"].(string)
	if idempotencyRequired(verb) && idem == "" {
		return true, errs.Payload(errs.MissingIdempotencyKey,
			fmt.Sprintf("%s: idempotency_key is required for risk=%s act/watch", verb.Name, verb.Risk), nil)
	}
	if idem != "" && (len(idem) < 1 || len(idem) > 256) {
		return true, errs.Payload(errs.InvalidBody, "idempotency_key must be 1-256 chars", nil)
	}
	if idem != "" {
		if done, isErr, payload := s.idempotencyReplay(verb.Name, idem, args); done {
			return isErr, payload
		}
	}

	// Risk gate: audit "requested" + circuit breaker. Unconditional.
	if err := riskgate.Precheck(s.cat, verb.Name, args); err != nil {
		return gateError(err)
	}

	// Full mode removes the dialog but not the audit trail: a
	// confirmation-tier verb still records "approved" (no dialog in
	// between) before execution.
	if riskgate.GetMode() == riskgate.ModeFull && s.cat.ConfirmationRequiredFor[verb.Risk] {
		riskgate.Audit(riskgate.AuditEvent{
			Verb:  verb.Name,
			Risk:  verb.Risk,
			Args:  verb.PublicArgs(args),
			Stage: "approved",
		})
	}

	// Mode-aware confirmation. The dialog is on-device; the tool call
	// returns the pending handle immediately.
	if s.confirmNeeded(verb) {
		payload, err := s.confirms.Submit(verb, args, kind, webhook, idem)
		if err != nil {
			return true, errs.Payload(errs.ExecutionFailed, err.Error(), nil)
		}
		if idem != "" {
			s.idempotencyRecord(verb.Name, idem, args, 202, payload, payload["confirm_id"])
		}
		return false, payload // 202 pending is a successful MCP result with a handle
	}

	status, payload := s.executeKind(kind, verb, args)
	if idem != "" {
		s.idempotencyRecord(verb.Name, idem, args, status, payload, nil)
	}
	return status >= 400, payload
}

// gateError maps a riskgate failure to a payload.
func gateError(err error) (bool, map[string]any) {
	switch e := err.(type) {
	case *store.CircuitOpen:
		return true, errs.Payload(errs.CircuitOpen, e.Error(), nil)
	case *riskgate.Denied:
		return true, errs.Payload(errs.ConfirmDenied, e.Error(), nil)
	default:
		return true, errs.Payload(errs.ExecutionFailed, err.Error(), nil)
	}
}

// precheckSynthetic is Precheck for a verb not in the catalog (the
// autonomy.set_mode pseudo-verb): audit "requested" + circuit guard.
func (s *Server) precheckSynthetic(verb *catalog.Verb, args map[string]any) error {
	needsConfirmation := true // always
	loggedArgs := verb.PublicArgs(args)
	riskgate.Audit(riskgate.AuditEvent{
		Verb:                 verb.Name,
		Risk:                 verb.Risk,
		Args:                 loggedArgs,
		ConfirmationRequired: &needsConfirmation,
		Stage:                "requested",
	})
	st, err := store.Get()
	if err != nil {
		return fmt.Errorf("mcpserver: store unavailable: %w", err)
	}
	return st.Guard(verb.Name, verb.Risk, loggedArgs)
}

// confirmNeeded reports whether the CURRENT autonomy mode requires an
// on-device confirm for this verb. Full mode never confirms; manual
// confirms on medium+; gated follows the catalog's confirmation_required_for.
func (s *Server) confirmNeeded(verb *catalog.Verb) bool {
	switch riskgate.GetMode() {
	case riskgate.ModeFull:
		return false
	case riskgate.ModeManual:
		return verb.Risk == "medium" || verb.Risk == "high"
	default: // gated
		return s.cat.ConfirmationRequiredFor[verb.Risk]
	}
}

// executeKind runs an approved call. kind is "perceive", "act", "watch",
// or "mode". Audits the outcome and returns the HTTP-equivalent status.
func (s *Server) executeKind(kind string, verb *catalog.Verb, args map[string]any) (int, map[string]any) {
	if kind == "mode" {
		return s.executeSetMode(args)
	}
	logged := verb.PublicArgs(args)

	if kind == "watch" {
		subID, err := s.subs.Start(verb, args)
		if err != nil {
			riskgate.Audit(riskgate.AuditEvent{
				Verb: verb.Name, Risk: verb.Risk, Args: logged,
				Stage: "failed", Error: err.Error(),
			})
			return 500, errs.Payload(errs.ExecutionFailed, fmt.Sprintf("%s: %v", verb.Name, err), nil)
		}
		riskgate.Audit(riskgate.AuditEvent{
			Verb: verb.Name, Risk: verb.Risk, Args: logged,
			Stage: "executed", Subscription: subID,
		})
		return 200, map[string]any{"id": subID}
	}

	result, err := tiera.Run(verb, args)
	if err != nil {
		stage := "failed"
		code := errs.ExecutionFailed
		var stderr string
		msg := err.Error()
		if execErr, ok := err.(*tiera.ExecutionError); ok {
			stderr = execErr.Stderr
			if strings.Contains(execErr.Message, "timed out") {
				stage = "timeout"
				code = errs.Timeout
			}
			msg = execErr.Message
		}
		riskgate.Audit(riskgate.AuditEvent{
			Verb: verb.Name, Risk: verb.Risk, Args: logged,
			Stage: stage, Error: msg, Stderr: stderr,
		})
		extra := map[string]any{}
		if stderr != "" {
			extra["stderr"] = stderr
		}
		return 500, errs.Payload(code, msg, extra)
	}
	riskgate.Audit(riskgate.AuditEvent{
		Verb: verb.Name, Risk: verb.Risk, Args: logged, Stage: "executed",
	})
	return 200, result
}

// executeSetMode applies an approved mode switch and persists it.
func (s *Server) executeSetMode(args map[string]any) (int, map[string]any) {
	modeStr, _ := args["mode"].(string)
	mode := riskgate.AutonomyMode(modeStr)
	from := riskgate.GetMode()
	riskgate.SetMode(mode)
	s.persistMode(mode)
	riskgate.Audit(riskgate.AuditEvent{
		Verb: "autonomy.set_mode", Risk: "high",
		Stage: "mode_changed", FromMode: string(from), ToMode: string(mode),
	})
	return 200, map[string]any{"ok": true, "mode": string(mode), "previous_mode": string(from)}
}

// persistMode writes the mode file so a reboot keeps the choice. Failure is
// logged, not fatal — the live mode is already switched.
func (s *Server) persistMode(mode riskgate.AutonomyMode) {
	if s.modePath == "" {
		return
	}
	if err := os.WriteFile(s.modePath, []byte(string(mode)+"\n"), 0o600); err != nil {
		log.Printf("mcpserver: persist autonomy mode: %v", err)
	}
}

// ── Idempotency ───────────────────────────────────────────────────────────────

// idempotencyRequired mirrors Python's _idempotency_required: risk:high
// act or watch calls must carry an idempotency key.
func idempotencyRequired(verb *catalog.Verb) bool {
	return verb.Risk == "high" && (verb.Direction == "act" || verb.Tier == "B")
}

// argsHash digests the verb args (Go's json.Marshal sorts map keys).
func argsHash(args map[string]any) string {
	blob, err := json.Marshal(args)
	if err != nil {
		blob = []byte("{}")
	}
	sum := sha256.Sum256(blob)
	return fmt.Sprintf("%x", sum)
}

// idempotencyReplay returns (done, isErr, payload) for a repeated key.
// done=false means the key is fresh and the call should proceed.
func (s *Server) idempotencyReplay(verbName, key string, args map[string]any) (bool, bool, map[string]any) {
	st, err := store.Get()
	if err != nil {
		return true, true, errs.Payload(errs.ExecutionFailed, err.Error(), nil)
	}
	rec, err := st.GetIdempotency(verbName, key)
	if err != nil {
		return true, true, errs.Payload(errs.ExecutionFailed, err.Error(), nil)
	}
	if rec == nil {
		return false, false, nil
	}
	digest := argsHash(args)
	if rec.ArgsHash != digest {
		return true, true, errs.Payload(errs.IdempotencyConflict,
			fmt.Sprintf("%s: idempotency key reused with different args", verbName), nil)
	}
	// A stored confirm handle replays the JOB, not the stored response —
	// the job keeps updating while the stored response is frozen at 202.
	if rec.ConfirmID.Valid && rec.ConfirmID.String != "" {
		if payload, err := s.confirms.Get(rec.ConfirmID.String); err == nil && payload != nil {
			status, _ := payload["status"].(string)
			if status == "pending" {
				// Still waiting on the dialog: hand the same handle back.
				return true, false, payload
			}
			// Resolved: the job's terminal payload is the response.
			return true, int(rec.HTTPStatus.Int64) >= 400, payload
		}
	}
	httpStatus := int(rec.HTTPStatus.Int64)
	return true, httpStatus >= 400, rec.Response
}

// idempotencyRecord stores the response of a completed or submitted call.
func (s *Server) idempotencyRecord(verbName, key string, args map[string]any, httpStatus int, payload map[string]any, confirmID any) {
	st, err := store.Get()
	if err != nil {
		return
	}
	rec := &store.IdempotencyRecord{
		Verb:       verbName,
		Key:        key,
		ArgsHash:   argsHash(args),
		HTTPStatus: nullInt64(httpStatus),
		Response:   payload,
	}
	if cid, ok := confirmID.(string); ok && cid != "" {
		rec.ConfirmID = nullString(cid)
	}
	st.PutIdempotency(rec)
}

// ── Envelope / dry run ────────────────────────────────────────────────────────

// splitEnvelope separates verb args from dry_run / idempotency_key / webhook_url.
func splitEnvelope(input map[string]any) (map[string]any, map[string]any) {
	args := make(map[string]any, len(input))
	envelope := map[string]any{}
	for k, v := range input {
		if envelopeKeys[k] {
			envelope[k] = v
		} else {
			args[k] = v
		}
	}
	return args, envelope
}

// dryRunPayload validates and returns argv without executing or confirming.
func (s *Server) dryRunPayload(verb *catalog.Verb, args map[string]any) (map[string]any, error) {
	argv, err := verb.BuildArgv(args)
	if err != nil {
		return nil, err
	}
	stdin := any(nil)
	if verb.Stdin != "" {
		stdin = verb.PublicArgs(args)[verb.Stdin]
	}
	needsConfirm := s.cat.ConfirmationRequiredFor[verb.Risk]
	route := verb.Direction
	if verb.Tier == "B" {
		route = "watch"
	}
	return map[string]any{
		"ok":                   true,
		"dry_run":              true,
		"verb":                 verb.Name,
		"direction":            verb.Direction,
		"tier":                 verb.Tier,
		"risk":                 verb.Risk,
		"route":                route,
		"argv":                 argv,
		"stdin":                stdin,
		"confirmation_required": needsConfirm,
		"idempotency_required": idempotencyRequired(verb),
	}, nil
}

// ── Auth middleware ───────────────────────────────────────────────────────────

// authMiddleware rejects requests without a valid X-Agent-Token header.
// Timing-safe compare via crypto/subtle.ConstantTimeCompare.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		supplied := r.Header.Get("X-Agent-Token")
		if subtle.ConstantTimeCompare([]byte(supplied), []byte(s.token)) != 1 {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		next.ServeHTTP(w, r)
	})
}

// ── Health ────────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	_, err := exec.LookPath("termux-battery-status")
	termuxAPI := err == nil

	payload := map[string]any{
		"ok":            true,
		"pid":           os.Getpid(),
		"uptime_s":      time.Since(s.startedAt).Seconds(),
		"host":          defaultHost,
		"port":          defaultPort,
		"verbs":         len(s.cat.Verbs),
		"watches":       s.subs.ListActive(),
		"termux_api":    termuxAPI,
		"autonomy_mode": string(riskgate.GetMode()),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payload)
}

// ── Token ─────────────────────────────────────────────────────────────────────

// loadOrCreateToken reads the auth token from AGENT_TOKEN env, or from the
// token file, or generates a new one and writes it to the token file.
func loadOrCreateToken(tokenPath string) (string, error) {
	if env := os.Getenv("AGENT_TOKEN"); env != "" {
		return env, nil
	}
	if tokenPath == "" {
		exe, err := os.Executable()
		if err != nil {
			return "", fmt.Errorf("mcpserver: cannot determine token path: %w", err)
		}
		tokenPath = filepath.Join(filepath.Dir(exe), ".agent-token")
	}
	if data, err := os.ReadFile(tokenPath); err == nil {
		if tok := strings.TrimRight(string(data), "\n"); tok != "" {
			return tok, nil
		}
	}
	// Generate 64 hex chars (32 bytes), same as Python's secrets.token_hex(32).
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("mcpserver: generate token: %w", err)
	}
	token := fmt.Sprintf("%x", b)
	if err := os.MkdirAll(filepath.Dir(tokenPath), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("mcpserver: write token: %w", err)
	}
	return token, nil
}

// ── Result helpers ────────────────────────────────────────────────────────────

// nullString / nullInt64 wrap values for the store.
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func nullInt64(n int) sql.NullInt64 {
	if n == 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(n), Valid: true}
}

// payloadResult wraps a payload as a CallToolResult. MCP convention:
// tool-level failures go in Content with IsError, not as protocol-level
// errors (which the SDK may surface as Go errors).
func payloadResult(payload map[string]any, isErr bool) (*mcp.CallToolResult, any, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return payloadResult(errs.Payload(errs.ExecutionFailed, fmt.Sprintf("marshal result: %v", err), nil), true)
	}
	return &mcp.CallToolResult{
		IsError:           isErr,
		Content:           []mcp.Content{&mcp.TextContent{Text: string(b)}},
		StructuredContent: payload,
	}, nil, nil
}
