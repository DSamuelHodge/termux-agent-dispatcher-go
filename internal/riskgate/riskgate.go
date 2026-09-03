// Package riskgate sits in front of every verb execution. It is deliberately
// NOT something the brain can route around — it's middleware the dispatcher
// calls unconditionally, keyed off the catalog's own risk field.
//
// For risk levels in `confirmation_required_for`, the gate blocks and shows
// a confirmation prompt (MCP elicitation in this Go port). Only an explicit
// human "yes" lets execution proceed. Every attempt — approved, denied, or
// bypassed by a lower risk tier — is written to the audit log first, before
// the verb runs, so a crash mid-execution still leaves a record of intent.
//
// AutonomyMode is a first-class concept here: the gate checks the current
// mode before deciding whether to invoke confirmation at all. The circuit
// breaker (store.guard()) is a reliability mechanism, not a trust mechanism,
// and stays live in every mode including full.
//
// Mirrors dispatch/risk_gate.py 1:1.
package riskgate

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/catalog"
	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/store"
)

// ── AutonomyMode ─────────────────────────────────────────────────────────────

// AutonomyMode controls whether the risk gate fires confirmation prompts.
type AutonomyMode string

const (
	// ModeManual confirms on risk:medium and risk:high.
	ModeManual AutonomyMode = "manual"
	// ModeGated confirms on risk:high only — the default, matching
	// confirmation_required_for: [high] in verbs.yaml.
	ModeGated AutonomyMode = "gated"
	// ModeFull skips all confirmation prompts. The circuit breaker still
	// applies — it is a reliability mechanism, not a trust gate.
	ModeFull AutonomyMode = "full"
)

// validModes is the set of accepted mode strings.
var validModes = map[AutonomyMode]bool{
	ModeManual: true,
	ModeGated:  true,
	ModeFull:   true,
}

// ── Denied ────────────────────────────────────────────────────────────────────

// Denied is returned by Check when a confirmation-gated verb is declined.
type Denied struct {
	Verb string
	Risk string
}

func (e *Denied) Error() string {
	return fmt.Sprintf("%s: declined (risk=%s)", e.Verb, e.Risk)
}

// ── Audit log ─────────────────────────────────────────────────────────────────

// LogPath is the JSON-lines audit log. It is resolved once at init from the
// executable's directory; tests override it via SetLogPath.
var logPath = defaultLogPath()

var logMu sync.Mutex

func defaultLogPath() string {
	exe, err := os.Executable()
	if err != nil {
		return filepath.Join(os.Getenv("HOME"), "agent", "logs", "audit.log")
	}
	return filepath.Join(filepath.Dir(exe), "logs", "audit.log")
}

// SetLogPath overrides the audit log path. Used by tests.
func SetLogPath(p string) {
	logMu.Lock()
	defer logMu.Unlock()
	logPath = p
}

// AuditEvent is one line in the audit log. Field order matches the Python
// implementation: ts is always present; other fields are set as applicable.
type AuditEvent struct {
	TS                   float64        `json:"ts"`
	Verb                 string         `json:"verb,omitempty"`
	Risk                 string         `json:"risk,omitempty"`
	Args                 map[string]any `json:"args,omitempty"`
	ConfirmationRequired *bool          `json:"confirmation_required,omitempty"`
	Stage                string         `json:"stage,omitempty"`
	Error                string         `json:"error,omitempty"`
	Stderr               string         `json:"stderr,omitempty"`
	Subscription         string         `json:"subscription,omitempty"`
	FromMode             string         `json:"from_mode,omitempty"`
	ToMode               string         `json:"to_mode,omitempty"`
}

// Audit writes one event line to the audit log and mirrors it into the store.
// This is the audit-before-execute invariant: always call Audit before the
// corresponding action, so a crash mid-execution still leaves a record of intent.
func Audit(event AuditEvent) {
	if event.TS == 0 {
		event.TS = float64(time.Now().UnixNano()) / 1e9
	}
	logMu.Lock()
	p := logPath
	os.MkdirAll(filepath.Dir(p), 0o755)
	line, _ := json.Marshal(event)
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err == nil {
		f.Write(append(line, '\n'))
		f.Close()
	}
	logMu.Unlock()

	// Mirror into the store if the stage is one of the tracked ones.
	if event.Stage != "" {
		if st, err := store.Get(); err == nil {
			var errStr *string
			if event.Error != "" {
				errStr = &event.Error
			}
			st.RecordOutcome(
				event.Verb,
				event.Stage,
				event.Risk,
				event.Args,
				errStr,
			)
		}
	}
}

// ── Mode state ────────────────────────────────────────────────────────────────

// currentMode is the live autonomy mode. Updated atomically by SetMode.
var currentMode atomic.Value // stores AutonomyMode

func init() {
	currentMode.Store(ModeGated)
}

// GetMode returns the current autonomy mode.
func GetMode() AutonomyMode {
	v := currentMode.Load()
	if v == nil {
		return ModeGated
	}
	return v.(AutonomyMode)
}

// SetMode updates the live autonomy mode. Called by the autonomy.set_mode tool
// after its own hardcoded confirmation has passed.
func SetMode(m AutonomyMode) {
	currentMode.Store(m)
}

// LoadModeFromFile reads the mode config file (plain text, one word).
// Missing file or empty content defaults to ModeGated. Read at boot only.
func LoadModeFromFile(path string) (AutonomyMode, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return ModeGated, nil
	}
	if err != nil {
		return ModeGated, fmt.Errorf("riskgate: read mode file: %w", err)
	}
	m := AutonomyMode(strings.TrimSpace(string(data)))
	if !validModes[m] {
		return ModeGated, fmt.Errorf("riskgate: unknown mode %q in %s (expected manual|gated|full)", m, path)
	}
	return m, nil
}

// ── Confirmation hook ─────────────────────────────────────────────────────────

// ConfirmFunc is the pluggable blocking confirmation transport (used by the
// synchronous Check path). The default in production is ConfirmOnDevice —
// a termux-dialog confirm prompt on the phone itself. Tests install stubs.
// Returns true if approved, false if denied.
type ConfirmFunc func(verbName string, args map[string]any) bool

var confirmFn ConfirmFunc

// SetConfirmFunc installs the confirmation transport. Called once at startup
// by mcpserver; called per-test by riskgate tests.
func SetConfirmFunc(fn ConfirmFunc) {
	confirmFn = fn
}

// ── On-device confirm dialog ─────────────────────────────────────────────────

// Copy limits for the confirm dialog, matching Python's risk_gate.py.
const (
	valueMax = 80
	hintMax  = 400
	titleMax = 60
	footer   = "Yes allows this. No denies it."
)

// intent is the human-readable action name shown in the dialog title.
var intent = map[string]string{
	"sms.send":          "Send an SMS",
	"call.place":        "Place a phone call",
	"camera.photo":      "Take a photo",
	"microphone.record": "Record from the microphone",
	"fingerprint.auth":  "Use the fingerprint sensor",
	"keystore.list":     "List hardware keystore keys",
	"keystore.generate": "Create a hardware keystore key",
	"keystore.delete":   "Delete a hardware keystore key",
	"keystore.sign":     "Sign data with a keystore key",
	"keystore.verify":   "Verify a keystore signature",
	// Synthetic tool (registered in mcpserver, not verbs.yaml). Switching
	// autonomy mode is always risk:high — the one moment "are you sure"
	// still matters, because it is what turns "are you sure" off.
	"autonomy.set_mode": "Change agent autonomy mode",
}

// argLabel maps arg names to human labels per verb.
var argLabel = map[string]map[string]string{
	"sms.send":          {"number": "To", "text": "Message"},
	"call.place":        {"number": "Number"},
	"camera.photo":      {"camera_id": "Camera", "outfile": "Save as"},
	"microphone.record": {"outfile": "File", "seconds": "Seconds"},
	"keystore.generate": {"alias": "Alias"},
	"keystore.delete":   {"alias": "Alias"},
	"keystore.sign":     {"alias": "Alias", "algorithm": "Algorithm", "data": "Data"},
	"keystore.verify": {
		"alias": "Alias", "algorithm": "Algorithm",
		"signature": "Signature", "data": "Data",
	},
	"autonomy.set_mode": {"mode": "Mode"},
}

func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func fallbackIntent(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

// FormatConfirmCopy builds the dialog title and body for a verb call.
// Mirrors Python's format_confirm_copy.
func FormatConfirmCopy(verbName string, publicArgs map[string]any, argNames []string) (string, string) {
	in := intent[verbName]
	if in == "" {
		in = fallbackIntent(verbName)
	}
	title := truncate(fmt.Sprintf("Allow: %s?", in), titleMax)
	lead := fmt.Sprintf("The agent wants to %s.", strings.ToLower(in[:1])+in[1:])
	labels := argLabel[verbName]
	lines := []string{}
	for _, key := range argNames {
		val, ok := publicArgs[key]
		if !ok {
			continue
		}
		label := key
		if l, ok := labels[key]; ok {
			label = l
		}
		lines = append(lines, fmt.Sprintf("%s: %s", label, truncate(fmt.Sprintf("%v", val), valueMax)))
	}
	hint := truncate(strings.Join(append([]string{lead}, append(lines, footer)...), "\n"), hintMax)
	return title, hint
}

// ConfirmOnDevice blocks on a termux-dialog confirm prompt shown on the
// device itself. This is the sole approval surface for confirmation-gated
// verbs: a human physically holding the phone taps yes/no.
//
// Fail-closed: timeout (120s), missing termux-dialog binary, non-zero exit,
// or a JSON decode failure all count as denial — never approval.
func ConfirmOnDevice(verbName string, publicArgs map[string]any, argNames []string) bool {
	title, hint := FormatConfirmCopy(verbName, publicArgs, argNames)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	proc := exec.CommandContext(ctx, "termux-dialog", "confirm", "-t", title, "-i", hint)
	out, err := proc.Output()
	if err != nil {
		return false // non-zero exit, binary missing, or 120s timeout
	}
	var result struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		return false
	}
	return result.Text == "yes"
}

// ConfirmDeviceFn is the on-device confirm transport used by the async
// ConfirmManager. Defaults to the real termux-dialog ConfirmOnDevice;
// tests override it.
var ConfirmDeviceFn = ConfirmOnDevice

// RecordDecision writes the approved/denied audit line for a confirmation.
func RecordDecision(verbName, risk string, args map[string]any, approved bool) {
	stage := "denied"
	if approved {
		stage = "approved"
	}
	Audit(AuditEvent{Verb: verbName, Risk: risk, Args: args, Stage: stage})
}

// ── Check ─────────────────────────────────────────────────────────────────────

// Precheck audits "requested" and trips the circuit breaker. Does not
// confirm. Mirrors Python's risk_gate.precheck — the HTTP/MCP paths use
// Precheck + the async ConfirmManager instead of the blocking Check.
func Precheck(cat *catalog.Catalog, verbName string, args map[string]any) error {
	verb, err := cat.Get(verbName)
	if err != nil {
		return err
	}
	needsConfirmation, err := cat.RequiresConfirmation(verbName)
	if err != nil {
		return err
	}
	loggedArgs := verb.PublicArgs(args)

	Audit(AuditEvent{
		Verb:                 verbName,
		Risk:                 verb.Risk,
		Args:                 loggedArgs,
		ConfirmationRequired: &needsConfirmation,
		Stage:                "requested",
	})

	st, err := store.Get()
	if err != nil {
		return fmt.Errorf("riskgate: store unavailable: %w", err)
	}
	return st.Guard(verbName, verb.Risk, loggedArgs) // *store.CircuitOpen if tripped
}

// Check is the risk gate entry point. It returns nil if execution may proceed,
// or *Denied / *store.CircuitOpen if it may not. Always audits first.
// Blocking confirm is kept for unit tests and callers that opt in — the
// HTTP/MCP paths use Precheck + the async ConfirmManager instead.
//
// Ordering (matches Python + the handoff's mode rules):
//  1. audit("requested") — unconditional, before anything else
//  2. store.guard()      — circuit breaker; stays live in all modes
//  3. mode decides the confirmation set:
//     full → no dialog; "approved" written only for confirmation-tier verbs
//     manual → confirm on medium+; gated → confirm on the catalog's set
//  4. confirm via confirmFn; fail-closed (nil transport = deny)
//  5. audit("approved" | "denied") → nil | *Denied
func Check(cat *catalog.Catalog, verbName string, args map[string]any) error {
	verb, err := cat.Get(verbName)
	if err != nil {
		return err
	}
	needsConfirmation, err := cat.RequiresConfirmation(verbName)
	if err != nil {
		return err
	}
	loggedArgs := verb.PublicArgs(args)
	mode := GetMode()

	// Step 1: audit "requested" — unconditional.
	Audit(AuditEvent{
		Verb:                 verbName,
		Risk:                 verb.Risk,
		Args:                 loggedArgs,
		ConfirmationRequired: &needsConfirmation,
		Stage:                "requested",
	})

	// Step 2: circuit breaker — unconditional in all modes.
	st, err := store.Get()
	if err != nil {
		return fmt.Errorf("riskgate: store unavailable: %w", err)
	}
	if err := st.Guard(verbName, verb.Risk, loggedArgs); err != nil {
		return err // *store.CircuitOpen
	}

	// Step 3: manual mode adds medium to the confirmation set.
	effectiveNeedsConfirmation := needsConfirmation
	if mode == ModeManual && verb.Risk == "medium" {
		effectiveNeedsConfirmation = true
	}

	// Full mode removes the dialog but not the audit trail: a
	// confirmation-tier verb still records "approved" (no dialog in
	// between). Verbs below the gate record no decision line at all.
	if mode == ModeFull {
		if needsConfirmation {
			Audit(AuditEvent{
				Verb:  verbName,
				Risk:  verb.Risk,
				Args:  loggedArgs,
				Stage: "approved",
			})
		}
		return nil
	}

	// Verb needs no confirmation under the current mode — no "approved"
	// line is written (audit parity with Python: only confirmation-gated
	// calls record a decision).
	if !effectiveNeedsConfirmation {
		return nil
	}

	// Step 4: call the confirmation transport. Fail-closed: no transport
	// installed means deny (never proceed without an explicit human yes).
	approved := false
	if confirmFn != nil {
		approved = confirmFn(verbName, loggedArgs)
	}

	Audit(AuditEvent{
		Verb:  verbName,
		Risk:  verb.Risk,
		Args:  loggedArgs,
		Stage: map[bool]string{true: "approved", false: "denied"}[approved],
	})

	if !approved {
		return &Denied{Verb: verbName, Risk: verb.Risk}
	}
	return nil
}
