// Package tiera implements Tier A: stateless request/response. Build argv
// from the template, run it, parse stdout, return a typed result. Every one
// of these commands has a real success/failure verdict, so a failure means
// something failed, not that the read went stale. Mirrors dispatch/tier_a.py.
package tiera

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/catalog"
)

// ExecutionError is returned when the subprocess fails.
type ExecutionError struct {
	VerbName string
	Message  string
	Stderr   string
}

func (e *ExecutionError) Error() string {
	return fmt.Sprintf("%s: %s", e.VerbName, e.Message)
}

// Run executes a Tier A verb. Returns a result map: {"ok": true, ...}.
func Run(verb *catalog.Verb, args map[string]any) (map[string]any, error) {
	if verb.Tier != "A" {
		return nil, fmt.Errorf("%s: not a Tier A verb (tier=%s)", verb.Name, verb.Tier)
	}
	argv, err := verb.BuildArgv(args)
	if err != nil {
		return nil, err
	}
	stdinData, err := verb.StdinPayload(args)
	if err != nil {
		return nil, err
	}
	timeout := 30.0
	if verb.Timeout != nil {
		timeout = *verb.Timeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout*float64(time.Second)))
	defer cancel()

	var cmd *exec.Cmd
	if stdinData != "" {
		cmd = exec.CommandContext(ctx, argv[0], argv[1:]...)
		cmd.Stdin = strings.NewReader(stdinData)
	} else {
		cmd = exec.CommandContext(ctx, argv[0], argv[1:]...)
	}

	var stdoutBuf, stderrBuf strings.Builder
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err = cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return nil, &ExecutionError{
			VerbName: verb.Name,
			Message:  fmt.Sprintf("timed out after %gs", timeout),
		}
	}
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, &ExecutionError{
				VerbName: verb.Name,
				Message:  fmt.Sprintf("exit code %d", exitErr.ExitCode()),
				Stderr:   stderrBuf.String(),
			}
		}
		// exec.ErrNotFound wraps as *exec.Error
		if strings.Contains(err.Error(), "executable file not found") ||
			strings.Contains(err.Error(), "no such file or directory") {
			return nil, &ExecutionError{
				VerbName: verb.Name,
				Message:  fmt.Sprintf("command not found: %s (is Termux:API installed?)", argv[0]),
			}
		}
		return nil, &ExecutionError{
			VerbName: verb.Name,
			Message:  err.Error(),
			Stderr:   stderrBuf.String(),
		}
	}

	stdout := strings.TrimSpace(stdoutBuf.String())

	switch verb.Parser {
	case "none":
		return map[string]any{"ok": true}, nil
	case "text":
		return map[string]any{"ok": true, "data": stdout}, nil
	case "json":
		if stdout == "" {
			return map[string]any{"ok": true, "data": nil}, nil
		}
		var data any
		if err := json.Unmarshal([]byte(stdout), &data); err != nil {
			return nil, &ExecutionError{
				VerbName: verb.Name,
				Message:  "expected JSON stdout, got unparseable output",
				Stderr:   stdout[:min(500, len(stdout))],
			}
		}
		return map[string]any{"ok": true, "data": data}, nil
	default:
		return nil, fmt.Errorf("%s: parser %q is not valid for Tier A", verb.Name, verb.Parser)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
