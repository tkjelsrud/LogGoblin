package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var builtBinaryPath string

func TestMain(m *testing.M) {
	buildDir, err := os.MkdirTemp("", "loggoblin-e2e-build-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mktemp failed:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(buildDir)

	builtBinaryPath = filepath.Join(buildDir, "loggoblin-test-bin")
	cmd := exec.Command("go", "build", "-o", builtBinaryPath, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintln(os.Stderr, "build failed:", err)
		fmt.Fprintln(os.Stderr, string(out))
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestOneShotInfluxBinaryRun(t *testing.T) {
	var mu sync.Mutex
	var reqBodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if !strings.HasPrefix(r.URL.Path, "/api/v2/write") {
			t.Fatalf("unexpected write path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Token test-token" {
			t.Fatalf("unexpected auth header: %s", r.Header.Get("Authorization"))
		}
		if got := r.URL.Query().Get("org"); got != "test-org" {
			t.Fatalf("unexpected org query: %s", got)
		}
		if got := r.URL.Query().Get("bucket"); got != "test-bucket" {
			t.Fatalf("unexpected bucket query: %s", got)
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		reqBodies = append(reqBodies, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	tmp := t.TempDir()
	logsDir := filepath.Join(tmp, "logs")
	if err := copyDir(filepath.Join("testdata", "fauxlogs"), logsDir); err != nil {
		t.Fatalf("copy fixtures: %v", err)
	}

	now := time.Now()
	setMTime := func(name string, delta time.Duration) {
		p := filepath.Join(logsDir, name)
		tm := now.Add(delta)
		if err := os.Chtimes(p, tm, tm); err != nil {
			t.Fatalf("chtimes %s: %v", name, err)
		}
	}
	setMTime("app1.log", -1*time.Minute)
	setMTime("app2.log", -2*time.Minute)
	setMTime("app3.log", -3*time.Minute)
	setMTime("app4.log", -4*time.Minute)

	cfg := Config{
		LogDirs:           []string{logsDir},
		FindNewestInDirs:  true,
		MaxFilesPerDir:    3,
		MatchPattern:      "(?i)error",
		ContextLines:      0,
		Hostname:          "test-host",
		OutputMode:        "influx",
		InfluxURL:         server.URL,
		InfluxToken:       "test-token",
		InfluxOrg:         "test-org",
		InfluxBucket:      "test-bucket",
		InfluxInsecure:    true,
		InfluxMeasurement: "loggoblin_events",
	}

	configPath := filepath.Join(tmp, "config.json")
	statePath := filepath.Join(tmp, "state.json")
	if err := writeJSON(configPath, cfg); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// First run establishes checkpoints (first-seen starts at EOF).
	runBinary(t, "--mode", "oneshot", "--config", configPath, "--state", statePath, "--lock-stale-after", "1h")
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("expected state file: %v", err)
	}
	mu.Lock()
	if len(reqBodies) != 0 {
		t.Fatalf("expected no writes on first run, got %d", len(reqBodies))
	}
	mu.Unlock()

	if err := appendLine(filepath.Join(logsDir, "app1.log"), "2026-03-05T12:00:00Z ERROR app1\n"); err != nil {
		t.Fatalf("append app1: %v", err)
	}
	if err := appendLine(filepath.Join(logsDir, "app2.log"), "2026-03-05T12:00:00Z ERROR app2\n"); err != nil {
		t.Fatalf("append app2: %v", err)
	}
	if err := appendLine(filepath.Join(logsDir, "app3.log"), "2026-03-05T12:00:00Z ERROR app3\n"); err != nil {
		t.Fatalf("append app3: %v", err)
	}
	if err := appendLine(filepath.Join(logsDir, "app4.log"), "2026-03-05T12:00:00Z ERROR app4\n"); err != nil {
		t.Fatalf("append app4: %v", err)
	}

	// Re-assert newest ordering so top-3 selection remains app1/app2/app3.
	now2 := time.Now()
	setMTime2 := func(name string, delta time.Duration) {
		p := filepath.Join(logsDir, name)
		tm := now2.Add(delta)
		if err := os.Chtimes(p, tm, tm); err != nil {
			t.Fatalf("chtimes2 %s: %v", name, err)
		}
	}
	setMTime2("app1.log", -1*time.Minute)
	setMTime2("app2.log", -2*time.Minute)
	setMTime2("app3.log", -3*time.Minute)
	setMTime2("app4.log", -4*time.Minute)

	runBinary(t, "--mode", "oneshot", "--config", configPath, "--state", statePath)

	mu.Lock()
	if len(reqBodies) != 1 {
		t.Fatalf("expected 1 write batch after second run, got %d", len(reqBodies))
	}
	batch := reqBodies[0]
	mu.Unlock()

	lines := splitNonEmptyLines(batch)
	if len(lines) != 3 {
		t.Fatalf("expected 3 events in batch, got %d\n%s", len(lines), batch)
	}
	if !strings.Contains(batch, escapeTagValue(filepath.Join(logsDir, "app1.log"))) ||
		!strings.Contains(batch, escapeTagValue(filepath.Join(logsDir, "app2.log"))) ||
		!strings.Contains(batch, escapeTagValue(filepath.Join(logsDir, "app3.log"))) {
		t.Fatalf("missing expected top-3 files in payload:\n%s", batch)
	}
	if strings.Contains(batch, escapeTagValue(filepath.Join(logsDir, "app4.log"))) {
		t.Fatalf("unexpected app4.log in top-3 payload:\n%s", batch)
	}
	if !strings.Contains(batch, "source_utc_offset_minutes=") {
		t.Fatalf("payload missing source_utc_offset_minutes field:\n%s", batch)
	}
	if !strings.Contains(batch, "host_local_time=") {
		t.Fatalf("payload missing host_local_time field:\n%s", batch)
	}
	if !strings.Contains(batch, "source_timezone=") {
		t.Fatalf("payload missing source_timezone tag:\n%s", batch)
	}

	// Third run with no new lines should not write additional points.
	runBinary(t, "--mode", "oneshot", "--config", configPath, "--state", statePath)
	mu.Lock()
	defer mu.Unlock()
	if len(reqBodies) != 1 {
		t.Fatalf("expected still 1 total write batch after third run, got %d", len(reqBodies))
	}
}

func runBinary(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command(builtBinaryPath, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("binary run failed: %v\nargs: %v\nstderr:\n%s\nstdout:\n%s", err, args, stderr.String(), stdout.String())
	}
}

func splitNonEmptyLines(s string) []string {
	parts := strings.Split(s, "\n")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func copyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		in := filepath.Join(src, e.Name())
		out := filepath.Join(dst, e.Name())
		data, err := os.ReadFile(in)
		if err != nil {
			return err
		}
		if err := os.WriteFile(out, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func appendLine(path, line string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(line)
	return err
}
