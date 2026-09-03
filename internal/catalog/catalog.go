// Package catalog loads verbs.yaml into typed Verb values and gives the
// rest of the dispatcher a single source of truth: add a verb to the YAML,
// it is live everywhere (tool registration, risk gating, execution) with
// no new code. Mirrors dispatch/catalog.py 1:1.
package catalog

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	validTiers      = map[string]bool{"A": true, "B": true} // Tier C is not representable here — see tier_c.py
	validDirections = map[string]bool{"perceive": true, "act": true}
	validRisks      = map[string]bool{"none": true, "low": true, "medium": true, "high": true}
	validParsers    = map[string]bool{"json": true, "text": true, "none": true, "json_stream": true}
)

// Verb is one catalog row.
type Verb struct {
	Name    string   `yaml:"-"`
	Direction string `yaml:"direction"` // perceive | act
	Tier      string `yaml:"tier"`      // A | B
	Risk      string `yaml:"risk"`      // none | low | medium | high
	Command   []string `yaml:"command"` // argv template
	Args      []string `yaml:"args"`    // required arg names, in order
	Parser    string   `yaml:"parser"`  // json | text | none | json_stream
	Timeout   *float64 `yaml:"timeout"` // seconds, or nil for long-running Tier B
	Stdin     string   `yaml:"stdin"`   // args[] name whose value is piped to process stdin
	                   // (official scripts that read the payload from stdin:
	                   // termux-saf-write, termux-keystore sign/verify)
}

// yamlVerb mirrors the YAML row shape. Verb.Name is filled from the map key.
type yamlVerbs struct {
	Verbs                  map[string]*Verb `yaml:"verbs"`
	ConfirmationRequiredFor []string        `yaml:"confirmation_required_for"`
}

// BuildArgv expands the command template with supplied args.
// Raises (returns error) on missing required args or unexpected extra args.
func (v *Verb) BuildArgv(supplied map[string]any) ([]string, error) {
	missing := []string{}
	for _, a := range v.Args {
		if _, ok := supplied[a]; !ok {
			missing = append(missing, a)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s: missing required args %v", v.Name, missing)
	}
	extra := []string{}
	for k := range supplied {
		found := false
		for _, a := range v.Args {
			if a == k {
				found = true
				break
			}
		}
		if !found {
			extra = append(extra, k)
		}
	}
	if len(extra) > 0 {
		return nil, fmt.Errorf("%s: unexpected args %v", v.Name, extra)
	}
	argv := make([]string, len(v.Command))
	for i, part := range v.Command {
		argv[i] = expandTemplate(part, supplied)
	}
	return argv, nil
}

// expandTemplate replaces {name} placeholders with string values from supplied.
func expandTemplate(part string, supplied map[string]any) string {
	for name, val := range supplied {
		placeholder := "{" + name + "}"
		if strings.Contains(part, placeholder) {
			part = strings.ReplaceAll(part, placeholder, fmt.Sprintf("%v", val))
		}
	}
	return part
}

// StdinPayload returns the value to pipe to the subprocess, or "" if this
// verb has no stdin hook. Returns an error if the value is not a string.
func (v *Verb) StdinPayload(supplied map[string]any) (string, error) {
	if v.Stdin == "" {
		return "", nil
	}
	val, ok := supplied[v.Stdin]
	if !ok {
		return "", fmt.Errorf("%s: stdin arg %q is missing", v.Name, v.Stdin)
	}
	if val == nil {
		return "", nil
	}
	s, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("%s: stdin arg %q must be a string, got %T", v.Name, v.Stdin, val)
	}
	return s, nil
}

// PublicArgs returns args safe to write to the audit log / confirm dialog
// (stdin body redacted as "<N chars>").
func (v *Verb) PublicArgs(supplied map[string]any) map[string]any {
	if v.Stdin == "" {
		return supplied
	}
	if _, ok := supplied[v.Stdin]; !ok {
		return supplied
	}
	out := make(map[string]any, len(supplied))
	for k, val := range supplied {
		out[k] = val
	}
	if raw, ok := out[v.Stdin].(string); ok {
		out[v.Stdin] = fmt.Sprintf("<%d chars>", len(raw))
	} else {
		out[v.Stdin] = "<0 chars>"
	}
	return out
}

// PublicSpec returns the catalog row the brain can cache: direction, tier,
// risk, args, parser, timeout, route, and optional stdin field name.
func (v *Verb) PublicSpec() map[string]any {
	route := v.Direction
	if v.Tier == "B" {
		route = "watch"
	}
	spec := map[string]any{
		"direction": v.Direction,
		"tier":      v.Tier,
		"risk":      v.Risk,
		"args":      v.Args,
		"parser":    v.Parser,
		"timeout":   v.Timeout,
		"route":     route,
	}
	if v.Stdin != "" {
		spec["stdin"] = v.Stdin
	}
	return spec
}

// Catalog is the loaded verb store.
type Catalog struct {
	Verbs                  map[string]*Verb
	ConfirmationRequiredFor map[string]bool
}

// Load reads and validates a verbs.yaml file.
func Load(path string) (*Catalog, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("catalog: read %s: %w", path, err)
	}
	var yv yamlVerbs
	if err := yaml.Unmarshal(raw, &yv); err != nil {
		return nil, fmt.Errorf("catalog: parse %s: %w", path, err)
	}
	verbs := make(map[string]*Verb, len(yv.Verbs))
	for name, spec := range yv.Verbs {
		spec.Name = name
		if !validTiers[spec.Tier] {
			return nil, fmt.Errorf("%s: invalid tier %q (dispatcher only handles A/B)", name, spec.Tier)
		}
		if !validDirections[spec.Direction] {
			return nil, fmt.Errorf("%s: invalid direction %q", name, spec.Direction)
		}
		if !validRisks[spec.Risk] {
			return nil, fmt.Errorf("%s: invalid risk %q", name, spec.Risk)
		}
		if !validParsers[spec.Parser] {
			return nil, fmt.Errorf("%s: invalid parser %q", name, spec.Parser)
		}
		if spec.Tier == "A" && spec.Parser == "json_stream" {
			return nil, fmt.Errorf("%s: json_stream is Tier B only", name)
		}
		if len(spec.Command) == 0 {
			return nil, fmt.Errorf("%s: command must be a non-empty list of strings", name)
		}
		for _, p := range spec.Command {
			if p == "" {
				return nil, fmt.Errorf("%s: command must be a non-empty list of strings", name)
			}
		}
		if spec.Stdin != "" {
			found := false
			for _, a := range spec.Args {
				if a == spec.Stdin {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("%s: stdin %q is not in args %v", name, spec.Stdin, spec.Args)
			}
		}
		verbs[name] = spec
	}
	confirm := make(map[string]bool, len(yv.ConfirmationRequiredFor))
	for _, r := range yv.ConfirmationRequiredFor {
		if !validRisks[r] {
			return nil, fmt.Errorf("confirmation_required_for has unknown risks [%s]", r)
		}
		confirm[r] = true
	}
	return &Catalog{Verbs: verbs, ConfirmationRequiredFor: confirm}, nil
}

// Get returns the named verb or an error.
func (c *Catalog) Get(name string) (*Verb, error) {
	v, ok := c.Verbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown verb: %s", name)
	}
	return v, nil
}

// RequiresConfirmation reports whether this verb needs an on-device confirm.
func (c *Catalog) RequiresConfirmation(name string) (bool, error) {
	v, err := c.Get(name)
	if err != nil {
		return false, err
	}
	return c.ConfirmationRequiredFor[v.Risk], nil
}
