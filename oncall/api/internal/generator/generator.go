// Package generator starts and stops the repo's own load generator (scripts/data_generator.py) as a child
// process, so the admin API can create traffic without a second implementation of the workload.
package generator

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var (
	ErrAlreadyRunning = errors.New("generator already running")
	ErrNotRunning     = errors.New("generator is not running")
)

type Status struct {
	Running   bool      `json:"running"`
	Rate      int       `json:"rate"`
	StartedAt time.Time `json:"started_at,omitempty"`
	PID       int       `json:"pid,omitempty"`
}

// Runner is what the API uses (and tests fake).
type Runner interface {
	Start(rate int) (Status, error)
	Stop() (Status, error)
	Status() Status
}

type Exec struct {
	Dir     string // repo root
	Python  string
	Script  string
	LogFile string

	mu     sync.Mutex
	cmd    *exec.Cmd
	status Status
	done   chan struct{}
}

func NewExec(dir, python, script, logFile string) *Exec {
	return &Exec{Dir: dir, Python: python, Script: script, LogFile: logFile}
}

func (e *Exec) Start(rate int) (Status, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cmd != nil {
		return e.status, ErrAlreadyRunning
	}
	py := e.Python
	if !filepath.IsAbs(py) {
		py = filepath.Join(e.Dir, py)
	}
	cmd := exec.Command(py, e.Script, "--rate", fmt.Sprint(rate), "--duration", "86400")
	cmd.Dir = e.Dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if e.LogFile != "" {
		_ = os.MkdirAll(filepath.Dir(filepath.Join(e.Dir, e.LogFile)), 0o755)
		if f, err := os.OpenFile(filepath.Join(e.Dir, e.LogFile), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			cmd.Stdout, cmd.Stderr = f, f
		}
	}
	if err := cmd.Start(); err != nil {
		return e.status, fmt.Errorf("start generator: %w", err)
	}
	e.cmd = cmd
	e.done = make(chan struct{})
	e.status = Status{Running: true, Rate: rate, StartedAt: time.Now().UTC(), PID: cmd.Process.Pid}
	go func(c *exec.Cmd, done chan struct{}) {
		_ = c.Wait()
		e.mu.Lock()
		if e.cmd == c {
			e.cmd = nil
			e.status = Status{}
		}
		e.mu.Unlock()
		close(done)
	}(cmd, e.done)
	return e.status, nil
}

// Stop sends SIGTERM (the script finishes its current batch) and escalates to SIGKILL after 10 s.
func (e *Exec) Stop() (Status, error) {
	e.mu.Lock()
	cmd, done := e.cmd, e.done
	e.mu.Unlock()
	if cmd == nil {
		return Status{}, ErrNotRunning
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
	return Status{}, nil
}

func (e *Exec) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.status
}

// Fake is an in-memory Runner for tests.
type Fake struct {
	mu     sync.Mutex
	status Status
	Starts []int
}

func (f *Fake) Start(rate int) (Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.status.Running {
		return f.status, ErrAlreadyRunning
	}
	f.Starts = append(f.Starts, rate)
	f.status = Status{Running: true, Rate: rate, StartedAt: time.Now().UTC(), PID: 1}
	return f.status, nil
}

func (f *Fake) Stop() (Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.status.Running {
		return Status{}, ErrNotRunning
	}
	f.status = Status{}
	return f.status, nil
}

func (f *Fake) Status() Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}
