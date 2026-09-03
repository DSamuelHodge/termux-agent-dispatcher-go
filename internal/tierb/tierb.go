// Package tierb implements Tier B: stateful or streaming verbs —
// termux-sensor without -n, termux-location -r updates, termux-microphone-record
// for its duration. These need an explicit start/stop lifecycle rather than a
// single call, so the daemon doesn't leave termux-* processes running
// unmonitored forever, and so a caller can drain results as they arrive
// instead of blocking until the process exits. Mirrors dispatch/tier_b.py.
package tierb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/DSamuelHodge/termux-agent-dispatcher-go/internal/catalog"
)

const (
	// QueueMax is the bounded per-subscription buffer. If the brain polls
	// slower than the stream produces, the oldest items are dropped instead
	// of growing RAM without limit; Poll reports the drop count so the
	// brain can react (poll faster, re-subscribe, etc.).
	QueueMax = 500

	// ReapGraceS is how long a stopped subscription stays poll-able (final
	// drain) before the reaper removes it from the registry.
	ReapGraceS = 60.0
	// ReapIntervalS is how often the reaper checks for expired stopped subs.
	ReapIntervalS = 30.0
)

// PIDFile is the crash-recovery PID registry. Resolved relative to the
// executable at package init; tests override it via SetPIDFile.
var pidFile = defaultPIDFile()

var pidFileMu sync.Mutex

func defaultPIDFile() string {
	exe, err := os.Executable()
	if err != nil {
		return filepath.Join(os.Getenv("HOME"), "agent", "logs", "subscriptions.pids")
	}
	return filepath.Join(filepath.Dir(exe), "logs", "subscriptions.pids")
}

// SetPIDFile overrides the PID file path. Used by tests.
func SetPIDFile(p string) {
	pidFileMu.Lock()
	defer pidFileMu.Unlock()
	pidFile = p
}

// Subscription tracks one running Tier B process.
type Subscription struct {
	ID         string
	VerbName   string
	Process    *exec.Cmd
	Queue      chan any
	Dropped    int
	Stopped    bool
	StoppedAt  float64
	readerDone chan struct{}

	mu sync.Mutex // guards Dropped, Stopped, StoppedAt
}

// Manager owns all active subscriptions.
type Manager struct {
	subs map[string]*Subscription
	mu   sync.Mutex
}

// NewManager creates a Manager and starts the reaper goroutine.
func NewManager() *Manager {
	m := &Manager{subs: make(map[string]*Subscription)}
	go m.reapLoop()
	return m
}

// Start spawns the Tier B verb's command, registers a Subscription, and
// returns the subscription ID.
func (m *Manager) Start(verb *catalog.Verb, args map[string]any) (string, error) {
	if verb.Tier != "B" {
		return "", fmt.Errorf("%s: not a Tier B verb (tier=%s)", verb.Name, verb.Tier)
	}
	argv, err := verb.BuildArgv(args)
	if err != nil {
		return "", err
	}
	stdinData, err := verb.StdinPayload(args)
	if err != nil {
		return "", err
	}

	subID := newUUID()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stderr = nil // unread PIPE stderr can deadlock a noisy child

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("%s: stdout pipe: %w", verb.Name, err)
	}

	if stdinData != "" {
		cmd.Stdin = strings.NewReader(stdinData)
	}

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("%s: start: %w", verb.Name, err)
	}

	sub := &Subscription{
		ID:         subID,
		VerbName:   verb.Name,
		Process:    cmd,
		Queue:      make(chan any, QueueMax),
		readerDone: make(chan struct{}),
	}

	m.mu.Lock()
	m.subs[subID] = sub
	m.mu.Unlock()

	m.pidsAdd(cmd.Process.Pid)

	// Reader goroutine: line-buffered, drop-oldest on full queue.
	go func() {
		defer close(sub.readerDone)
		defer m.markStopped(sub)
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			var item any
			if verb.Parser == "json_stream" {
				var parsed any
				if err := json.Unmarshal([]byte(line), &parsed); err != nil {
					item = map[string]any{"_raw": line, "_parse_error": true}
				} else {
					item = parsed
				}
			} else {
				item = line
			}
			// Bounded queue, drop-oldest (single producer: this goroutine).
			enqueued := false
			for !enqueued {
				select {
				case sub.Queue <- item:
					enqueued = true
				default:
					// Queue full — drop oldest.
					select {
					case <-sub.Queue:
						sub.mu.Lock()
						sub.Dropped++
						sub.mu.Unlock()
					default:
					}
				}
			}
		}
		// Wait must be called after the scanner finishes so the pipe is fully
		// drained before Wait closes it; otherwise Wait can race with Scan.
		cmd.Wait()
	}()

	return subID, nil
}

// Poll drains up to maxItems from the subscription's queue.
// Returns items, stopped flag, and drop count.
func (m *Manager) Poll(subID string, maxItems int) (map[string]any, error) {
	sub, err := m.get(subID)
	if err != nil {
		return nil, err
	}
	sub.mu.Lock()
	dropped := sub.Dropped
	stopped := sub.Stopped
	sub.mu.Unlock()

	items := []any{}
	for len(items) < maxItems {
		select {
		case item := <-sub.Queue:
			items = append(items, item)
		default:
			return map[string]any{"items": items, "stopped": stopped, "dropped": dropped}, nil
		}
	}
	return map[string]any{"items": items, "stopped": stopped, "dropped": dropped}, nil
}

// Stop terminates the subscription's process and returns the verb name.
func (m *Manager) Stop(subID string) (string, error) {
	m.mu.Lock()
	sub, ok := m.subs[subID]
	m.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("unknown subscription: %s", subID)
	}

	if sub.Process.ProcessState == nil || !sub.Process.ProcessState.Exited() {
		sub.Process.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			sub.Process.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			sub.Process.Process.Kill()
			<-done
		}
	}

	m.markStopped(sub)
	m.pidsRemove(sub.Process.Process.Pid)
	return sub.VerbName, nil
}

// ListActive returns the IDs of all non-stopped subscriptions.
func (m *Manager) ListActive() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []string{}
	for id, sub := range m.subs {
		sub.mu.Lock()
		if !sub.Stopped {
			out = append(out, id)
		}
		sub.mu.Unlock()
	}
	return out
}

// ── Internals ─────────────────────────────────────────────────────────────────

func (m *Manager) markStopped(sub *Subscription) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	if !sub.Stopped {
		sub.Stopped = true
		sub.StoppedAt = float64(time.Now().UnixNano()) / 1e9
	}
}

func (m *Manager) get(subID string) (*Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sub, ok := m.subs[subID]
	if !ok {
		return nil, fmt.Errorf("unknown subscription: %s", subID)
	}
	return sub, nil
}

func (m *Manager) reapLoop() {
	for {
		time.Sleep(ReapIntervalS * time.Second)
		now := float64(time.Now().UnixNano()) / 1e9
		m.mu.Lock()
		dead := []string{}
		for id, sub := range m.subs {
			sub.mu.Lock()
			if sub.Stopped && sub.StoppedAt > 0 && now-sub.StoppedAt > ReapGraceS {
				dead = append(dead, id)
			}
			sub.mu.Unlock()
		}
		for _, id := range dead {
			delete(m.subs, id)
		}
		m.mu.Unlock()
	}
}

// ── PID tracking (crash recovery) ────────────────────────────────────────────

func pidsLoad() ([]int, error) {
	pidFileMu.Lock()
	p := pidFile
	pidFileMu.Unlock()
	data, err := os.ReadFile(p)
	if err != nil {
		return []int{}, nil
	}
	var pids []int
	if err := json.Unmarshal(data, &pids); err != nil {
		return []int{}, nil
	}
	return pids, nil
}

func pidsSave(pids []int) error {
	pidFileMu.Lock()
	p := pidFile
	pidFileMu.Unlock()
	os.MkdirAll(filepath.Dir(p), 0o755)
	data, err := json.Marshal(pids)
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

func (m *Manager) pidsAdd(pid int) {
	pids, _ := pidsLoad()
	pids = append(pids, pid)
	pidsSave(pids)
}

func (m *Manager) pidsRemove(pid int) {
	pids, _ := pidsLoad()
	out := []int{}
	for _, p := range pids {
		if p != pid {
			out = append(out, p)
		}
	}
	pidsSave(out)
}

// isOurOrphan reports whether pid is a termux-* tool (guard against PID reuse).
// PID file entries can be stale; only kill processes whose cmdline names a
// termux-* binary — nothing else in this deployment should be spawning those.
//
// On Termux every termux-* tool is a bash script, so /proc/<pid>/cmdline's
// argv[0] is the interpreter (bash), not the tool. Python's reference
// implementation checks argv[0] only and therefore never matches its own
// wrapped children. Deliberate deviation: scan every argv element's basename
// so both direct binaries (argv[0] = termux-xxx) and script wrappers
// (argv[1] = termux-xxx) are recognized. The match set is unchanged — still
// only termux-* tools — so the pid-reuse guard is not weakened.
func isOurOrphan(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil || len(data) == 0 {
		return false
	}
	for _, part := range strings.Split(string(data), "\x00") {
		if part == "" {
			continue
		}
		if strings.HasPrefix(filepath.Base(part), "termux-") {
			return true
		}
	}
	return false
}

// RecoverOrphans is called once at daemon startup: terminate any termux-*
// processes left running by a previous crash (tracked in subscriptions.pids).
// Returns the list of PIDs actually killed.
func RecoverOrphans() []int {
	killed := []int{}
	pids, _ := pidsLoad()
	for _, pid := range pids {
		// Check if alive.
		if err := syscall.Kill(pid, 0); err != nil {
			continue // already gone
		}
		if !isOurOrphan(pid) {
			continue // pid reused by something we don't own
		}
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
			continue
		}
		time.Sleep(500 * time.Millisecond)
		// If still alive after SIGTERM, escalate to SIGKILL.
		if err := syscall.Kill(pid, 0); err == nil {
			syscall.Kill(pid, syscall.SIGKILL)
		}
		killed = append(killed, pid)
	}
	pidsSave([]int{})
	return killed
}

// ── UUID ─────────────────────────────────────────────────────────────────────

var uuidMu sync.Mutex
var uuidCounter uint64

// newUUID returns a 12-char hex string. Collision probability is negligible
// for a single-device daemon; matches Python's uuid.uuid4().hex[:12].
func newUUID() string {
	uuidMu.Lock()
	defer uuidMu.Unlock()
	uuidCounter++
	now := time.Now().UnixNano()
	return fmt.Sprintf("%08x%04x", uint32(now>>32)^uint32(now), uint16(uuidCounter))
}
