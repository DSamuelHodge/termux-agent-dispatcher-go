package catalog_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/catalog"
)

// realVerbsYAML returns the path to the shared verbs.yaml from the Python repo.
func realVerbsYAML(t *testing.T) string {
	t.Helper()
	p := "/Users/derrickhodge/termux-agent-dispatcher/verbs.yaml"
	if _, err := os.Stat(p); err != nil {
		t.Skipf("shared verbs.yaml not found at %s: %v", p, err)
	}
	return p
}

func newTestVerb(name, dir, tier, risk string, cmd []string, args []string, parser string, timeout *float64, stdin string) *catalog.Verb {
	return &catalog.Verb{
		Name:      name,
		Direction: dir,
		Tier:      tier,
		Risk:      risk,
		Command:   cmd,
		Args:      args,
		Parser:    parser,
		Timeout:   timeout,
		Stdin:     stdin,
	}
}

func f64(v float64) *float64 { return &v }

// ── Load real catalog ─────────────────────────────────────────────────────────

func TestLoadRealCatalog(t *testing.T) {
	c, err := catalog.Load(realVerbsYAML(t))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.Verbs) != 73 {
		t.Errorf("expected 73 verbs, got %d", len(c.Verbs))
	}
	v, err := c.Get("battery.status")
	if err != nil {
		t.Fatalf("Get battery.status: %v", err)
	}
	if v.Tier != "A" || v.Direction != "perceive" || v.Risk != "none" {
		t.Errorf("battery.status: unexpected fields %+v", v)
	}
	if v.Parser != "json" {
		t.Errorf("battery.status parser = %q, want json", v.Parser)
	}
}

func TestLoadTierBVerbs(t *testing.T) {
	c, err := catalog.Load(realVerbsYAML(t))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// job.schedule has timeout:10 in verbs.yaml despite being tier B —
	// check tier only for that one; all others must have nil timeout.
	tierBNilTimeout := []string{"nfc.read", "sensor.stream", "location.watch", "microphone.record", "dialog.show", "stt.listen"}
	for _, name := range tierBNilTimeout {
		v, err := c.Get(name)
		if err != nil {
			t.Errorf("Get %s: %v", name, err)
			continue
		}
		if v.Tier != "B" {
			t.Errorf("%s: expected tier B, got %q", name, v.Tier)
		}
		if v.Timeout != nil {
			t.Errorf("%s: expected nil timeout, got %v", name, *v.Timeout)
		}
	}
	// job.schedule: tier B, timeout set (special case — bounded job submission).
	js, err := c.Get("job.schedule")
	if err != nil {
		t.Fatalf("Get job.schedule: %v", err)
	}
	if js.Tier != "B" {
		t.Errorf("job.schedule: expected tier B, got %q", js.Tier)
	}
}

func TestPublicSpecRoute(t *testing.T) {
	c, err := catalog.Load(realVerbsYAML(t))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	bat, _ := c.Get("battery.status")
	spec := bat.PublicSpec()
	if spec["route"] != "perceive" {
		t.Errorf("battery.status route = %v, want perceive", spec["route"])
	}
	if spec["parser"] != "json" {
		t.Errorf("battery.status parser = %v, want json", spec["parser"])
	}
	stream, _ := c.Get("sensor.stream")
	wspec := stream.PublicSpec()
	if wspec["route"] != "watch" {
		t.Errorf("sensor.stream route = %v, want watch", wspec["route"])
	}
	sign, _ := c.Get("keystore.sign")
	sspec := sign.PublicSpec()
	if sspec["stdin"] != "data" {
		t.Errorf("keystore.sign stdin = %v, want data", sspec["stdin"])
	}
}

// ── BuildArgv ─────────────────────────────────────────────────────────────────

func TestBuildArgvOK(t *testing.T) {
	v := newTestVerb("t", "act", "A", "none", []string{"cmd", "{x}"}, []string{"x"}, "none", f64(1), "")
	argv, err := v.BuildArgv(map[string]any{"x": "1"})
	if err != nil {
		t.Fatalf("BuildArgv: %v", err)
	}
	if len(argv) != 2 || argv[0] != "cmd" || argv[1] != "1" {
		t.Errorf("argv = %v", argv)
	}
}

func TestBuildArgvMissing(t *testing.T) {
	v := newTestVerb("t", "act", "A", "none", []string{"cmd", "{x}"}, []string{"x"}, "none", f64(1), "")
	_, err := v.BuildArgv(map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Errorf("expected missing error, got %v", err)
	}
}

func TestBuildArgvExtra(t *testing.T) {
	v := newTestVerb("t", "act", "A", "none", []string{"cmd", "{x}"}, []string{"x"}, "none", f64(1), "")
	_, err := v.BuildArgv(map[string]any{"x": "1", "y": "2"})
	if err == nil || !strings.Contains(err.Error(), "unexpected") {
		t.Errorf("expected unexpected error, got %v", err)
	}
}

// ── StdinPayload / PublicArgs ─────────────────────────────────────────────────

func TestStdinPayload(t *testing.T) {
	v := newTestVerb("t", "act", "A", "high", []string{"cmd"}, []string{"data"}, "text", f64(1), "data")
	s, err := v.StdinPayload(map[string]any{"data": "hello"})
	if err != nil || s != "hello" {
		t.Errorf("StdinPayload = %q, %v", s, err)
	}
	s, err = v.StdinPayload(map[string]any{"data": nil})
	if err != nil || s != "" {
		t.Errorf("StdinPayload(nil) = %q, %v", s, err)
	}
	_, err = v.StdinPayload(map[string]any{"data": 42})
	if err == nil || !strings.Contains(err.Error(), "must be a string") {
		t.Errorf("expected type error, got %v", err)
	}
}

func TestPublicArgsRedaction(t *testing.T) {
	v := newTestVerb("t", "act", "A", "high", []string{"cmd"}, []string{"data"}, "text", f64(1), "data")
	pub := v.PublicArgs(map[string]any{"data": "secret"})
	if pub["data"] != "<6 chars>" {
		t.Errorf("PublicArgs[data] = %v, want <6 chars>", pub["data"])
	}
}

func TestPublicArgsNoStdin(t *testing.T) {
	v := newTestVerb("t", "act", "A", "none", []string{"cmd"}, []string{"x"}, "none", f64(1), "")
	pub := v.PublicArgs(map[string]any{"x": "val"})
	if pub["x"] != "val" {
		t.Errorf("PublicArgs[x] = %v, want val", pub["x"])
	}
}

// ── Load validation errors ────────────────────────────────────────────────────

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "v.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadRejectsBadTier(t *testing.T) {
	p := writeYAML(t, `verbs:
  bad:
    direction: perceive
    tier: C
    risk: none
    command: ["x"]
    args: []
    parser: json
    timeout: 1
`)
	_, err := catalog.Load(p)
	if err == nil || !strings.Contains(err.Error(), "invalid tier") {
		t.Errorf("expected invalid tier error, got %v", err)
	}
}

func TestLoadRejectsBadDirection(t *testing.T) {
	p := writeYAML(t, `verbs:
  bad:
    direction: nope
    tier: A
    risk: none
    command: ["x"]
    args: []
    parser: json
    timeout: 1
`)
	_, err := catalog.Load(p)
	if err == nil || !strings.Contains(err.Error(), "invalid direction") {
		t.Errorf("expected invalid direction error, got %v", err)
	}
}

func TestLoadRejectsBadRisk(t *testing.T) {
	p := writeYAML(t, `verbs:
  bad:
    direction: perceive
    tier: A
    risk: nuclear
    command: ["x"]
    args: []
    parser: json
    timeout: 1
`)
	_, err := catalog.Load(p)
	if err == nil || !strings.Contains(err.Error(), "invalid risk") {
		t.Errorf("expected invalid risk error, got %v", err)
	}
}

func TestLoadRejectsBadParser(t *testing.T) {
	p := writeYAML(t, `verbs:
  bad:
    direction: perceive
    tier: A
    risk: none
    command: ["x"]
    args: []
    parser: xml
    timeout: 1
`)
	_, err := catalog.Load(p)
	if err == nil || !strings.Contains(err.Error(), "invalid parser") {
		t.Errorf("expected invalid parser error, got %v", err)
	}
}

func TestLoadRejectsJsonStreamTierA(t *testing.T) {
	p := writeYAML(t, `verbs:
  bad:
    direction: perceive
    tier: A
    risk: none
    command: ["x"]
    args: []
    parser: json_stream
    timeout: 1
`)
	_, err := catalog.Load(p)
	if err == nil || !strings.Contains(err.Error(), "json_stream is Tier B only") {
		t.Errorf("expected json_stream Tier B only error, got %v", err)
	}
}

func TestLoadRejectsEmptyCommand(t *testing.T) {
	p := writeYAML(t, `verbs:
  bad:
    direction: perceive
    tier: A
    risk: none
    command: []
    args: []
    parser: json
    timeout: 1
`)
	_, err := catalog.Load(p)
	if err == nil || !strings.Contains(err.Error(), "command must be") {
		t.Errorf("expected command error, got %v", err)
	}
}

func TestLoadRejectsStdinNotInArgs(t *testing.T) {
	p := writeYAML(t, `verbs:
  bad:
    direction: act
    tier: A
    risk: none
    command: ["cmd"]
    args: ["a"]
    stdin: missing
    parser: text
    timeout: 1
`)
	_, err := catalog.Load(p)
	if err == nil || !strings.Contains(err.Error(), "stdin") {
		t.Errorf("expected stdin error, got %v", err)
	}
}

func TestLoadRejectsBadConfirmationRisks(t *testing.T) {
	p := writeYAML(t, `verbs:
  ok:
    direction: perceive
    tier: A
    risk: none
    command: ["x"]
    args: []
    parser: json
    timeout: 1
confirmation_required_for: ["bogus"]
`)
	_, err := catalog.Load(p)
	if err == nil || !strings.Contains(err.Error(), "unknown risks") {
		t.Errorf("expected unknown risks error, got %v", err)
	}
}

// ── Get / RequiresConfirmation ────────────────────────────────────────────────

func TestUnknownVerb(t *testing.T) {
	c, err := catalog.Load(realVerbsYAML(t))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	_, err = c.Get("nope.verb")
	if err == nil {
		t.Error("expected error for unknown verb")
	}
}

func TestRequiresConfirmation(t *testing.T) {
	c, err := catalog.Load(realVerbsYAML(t))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	yes, err := c.RequiresConfirmation("sms.send")
	if err != nil || !yes {
		t.Errorf("sms.send requires confirmation: got %v, %v", yes, err)
	}
	no, err := c.RequiresConfirmation("toast.show")
	if err != nil || no {
		t.Errorf("toast.show requires confirmation: got %v, %v", no, err)
	}
}
