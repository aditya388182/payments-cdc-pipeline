package generator

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func script(t *testing.T, body string) (dir, name string) {
	t.Helper()
	dir = t.TempDir()
	name = "fake_generator.sh"
	if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir, name
}

func TestStartStopLifecycleWithAProcessGroup(t *testing.T) {
	dir, name := script(t, `trap 'exit 0' TERM; sleep 60 & wait`)
	e := NewExec(dir, "/bin/sh", name, "gen.log")
	st, err := e.Start(25)
	if err != nil || !st.Running || st.Rate != 25 || st.PID == 0 {
		t.Fatalf("start: %+v %v", st, err)
	}
	if _, err := e.Start(10); err != ErrAlreadyRunning {
		t.Fatalf("second start err = %v", err)
	}
	if got := e.Status(); !got.Running || got.Rate != 25 {
		t.Fatalf("status = %+v", got)
	}
	t0 := time.Now()
	if _, err := e.Stop(); err != nil {
		t.Fatal(err)
	}
	if time.Since(t0) > 5*time.Second {
		t.Fatalf("stop took %v", time.Since(t0))
	}
	if e.Status().Running {
		t.Fatal("still running after stop")
	}
	if _, err := e.Stop(); err != ErrNotRunning {
		t.Fatalf("second stop err = %v", err)
	}
}

func TestAProcessThatExitsOnItsOwnClearsTheStatus(t *testing.T) {
	dir, name := script(t, `exit 0`)
	e := NewExec(dir, "/bin/sh", name, "")
	if _, err := e.Start(5); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.Status().Running && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if e.Status().Running {
		t.Fatal("status still running after the process exited")
	}
	if _, err := e.Start(5); err != nil { // can be started again
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for e.Status().Running && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMissingInterpreterIsAnError(t *testing.T) {
	e := NewExec(t.TempDir(), "/nonexistent/python", "x.py", "")
	if _, err := e.Start(5); err == nil {
		t.Fatal("expected an error")
	}
}
