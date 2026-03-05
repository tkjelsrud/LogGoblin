package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewestLogFilesTop3(t *testing.T) {
	dir := t.TempDir()
	files := []string{"a.log", "b.log", "c.log", "d.log", "ignore.txt"}
	for _, name := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("line\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	now := time.Now()
	setMTime := func(name string, delta time.Duration) {
		p := filepath.Join(dir, name)
		tm := now.Add(delta)
		if err := os.Chtimes(p, tm, tm); err != nil {
			t.Fatalf("chtimes %s: %v", name, err)
		}
	}

	setMTime("a.log", -1*time.Minute)
	setMTime("b.log", -2*time.Minute)
	setMTime("c.log", -3*time.Minute)
	setMTime("d.log", -4*time.Minute)
	setMTime("ignore.txt", -10*time.Minute)

	got, err := newestLogFiles(dir, 3)
	if err != nil {
		t.Fatalf("newestLogFiles: %v", err)
	}

	var gotBase []string
	for _, p := range got {
		gotBase = append(gotBase, filepath.Base(p))
	}

	want := []string{"a.log", "b.log", "c.log"}
	if !reflect.DeepEqual(gotBase, want) {
		t.Fatalf("unexpected files: got %v want %v", gotBase, want)
	}
}

func TestResolveStartOffset(t *testing.T) {
	tests := []struct {
		name   string
		size   int64
		cp     FileCheckpoint
		repl   bool
		wanted int64
	}{
		{name: "no checkpoint", size: 100, cp: FileCheckpoint{}, wanted: 100},
		{name: "valid checkpoint", size: 100, cp: FileCheckpoint{Offset: 25}, wanted: 25},
		{name: "checkpoint beyond file", size: 100, cp: FileCheckpoint{Offset: 150}, wanted: 100},
		{name: "negative checkpoint", size: 100, cp: FileCheckpoint{Offset: -1}, wanted: 100},
		{name: "replaced file starts at beginning", size: 100, cp: FileCheckpoint{Offset: 80}, repl: true, wanted: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveStartOffset(tt.size, tt.cp, tt.repl); got != tt.wanted {
				t.Fatalf("resolveStartOffset() = %d, want %d", got, tt.wanted)
			}
		})
	}
}

func TestMergeEntriesDedupesByStableKey(t *testing.T) {
	existing := []LogEntry{
		{Timestamp: "2026-03-05T10:00:00Z", Message: "ERROR 123 happened", Filename: "a.log", Hostname: "vm1"},
	}
	incoming := []LogEntry{
		{Timestamp: "2026-03-05T10:01:00Z", Message: "ERROR 456 happened", Filename: "a.log", Hostname: "vm1"},
		{Timestamp: "2026-03-05T10:02:00Z", Message: "ERROR unique", Filename: "b.log", Hostname: "vm1"},
	}

	merged := mergeEntries(existing, incoming)
	if len(merged) != 2 {
		t.Fatalf("mergeEntries length = %d, want 2", len(merged))
	}

	if merged[0].Filename != "a.log" || merged[1].Filename != "b.log" {
		t.Fatalf("merge order/content unexpected: %#v", merged)
	}
}

func TestAcquireLockAndStaleReclaim(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "app.lock")

	first, err := acquireLock(lockPath, 0)
	if err != nil {
		t.Fatalf("acquire first lock: %v", err)
	}
	defer releaseLock(first, lockPath)

	if _, err := acquireLock(lockPath, 0); err == nil {
		t.Fatal("expected second lock attempt to fail")
	}

	releaseLock(first, lockPath)

	if err := os.WriteFile(lockPath, []byte("stale\n"), 0o644); err != nil {
		t.Fatalf("write stale lock: %v", err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatalf("chtimes stale lock: %v", err)
	}

	second, err := acquireLock(lockPath, time.Hour)
	if err != nil {
		t.Fatalf("expected stale lock reclaim, got: %v", err)
	}
	releaseLock(second, lockPath)
}

func TestFileLooksReplaced(t *testing.T) {
	t.Run("legacy checkpoint without identity", func(t *testing.T) {
		replaced := fileLooksReplaced(FileCheckpoint{Offset: 300}, FileIdentity{Size: 350, HeadHash: "abc"})
		if replaced {
			t.Fatal("expected legacy checkpoint to not force replacement")
		}
	})

	t.Run("changed head hash", func(t *testing.T) {
		cp := FileCheckpoint{Offset: 300, Size: 300, HeadHash: "old", ModTimeUnixNano: 10}
		cur := FileIdentity{Size: 320, HeadHash: "new", ModTimeUnixNano: 20}
		if !fileLooksReplaced(cp, cur) {
			t.Fatal("expected replacement when head hash changed")
		}
	})

	t.Run("shrunken file", func(t *testing.T) {
		cp := FileCheckpoint{Offset: 300, Size: 300, HeadHash: "same", ModTimeUnixNano: 10}
		cur := FileIdentity{Size: 120, HeadHash: "same", ModTimeUnixNano: 20}
		if !fileLooksReplaced(cp, cur) {
			t.Fatal("expected replacement when file shrank")
		}
	})

	t.Run("append-only growth", func(t *testing.T) {
		cp := FileCheckpoint{Offset: 300, Size: 300, HeadHash: "same", ModTimeUnixNano: 10}
		cur := FileIdentity{Size: 500, HeadHash: "same", ModTimeUnixNano: 20}
		if fileLooksReplaced(cp, cur) {
			t.Fatal("did not expect replacement on append-only growth")
		}
	})
}

func TestComputeFileIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	if err := os.WriteFile(path, []byte("first line\nsecond line\n"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat test file: %v", err)
	}

	id1, err := computeFileIdentity(path, info)
	if err != nil {
		t.Fatalf("compute identity: %v", err)
	}
	if id1.Size != info.Size() {
		t.Fatalf("identity size mismatch: got %d want %d", id1.Size, info.Size())
	}
	if id1.HeadHash == "" {
		t.Fatal("expected non-empty head hash")
	}

	// Overwrite same path with new content and ensure head hash changes.
	if err := os.WriteFile(path, []byte("changed line\nsecond line\n"), 0o644); err != nil {
		t.Fatalf("rewrite test file: %v", err)
	}
	info2, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat rewritten file: %v", err)
	}
	id2, err := computeFileIdentity(path, info2)
	if err != nil {
		t.Fatalf("compute identity rewritten: %v", err)
	}
	if id1.HeadHash == id2.HeadHash {
		t.Fatal("expected head hash change after overwrite")
	}
}

func TestResolveOutputMode(t *testing.T) {
	got, err := resolveOutputMode("")
	if err != nil {
		t.Fatalf("resolveOutputMode empty: %v", err)
	}
	if got != "influx" {
		t.Fatalf("resolveOutputMode empty = %q, want influx", got)
	}

	got, err = resolveOutputMode("jsonbin")
	if err != nil || got != "jsonbin" {
		t.Fatalf("resolveOutputMode jsonbin failed: got=%q err=%v", got, err)
	}

	if _, err := resolveOutputMode("badmode"); err == nil {
		t.Fatal("expected invalid output mode error")
	}
}

func TestCompileRegexList(t *testing.T) {
	regexes, err := compileRegexList([]string{"(?i)health check", "", `\\s+error\\s+`})
	if err != nil {
		t.Fatalf("compileRegexList valid: %v", err)
	}
	if len(regexes) != 2 {
		t.Fatalf("regex count = %d, want 2", len(regexes))
	}

	if _, err := compileRegexList([]string{"["}); err == nil {
		t.Fatal("expected invalid regex error")
	}
}

func TestProcessLogRespectsIgnorePatterns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	content := strings.Join([]string{
		"[2026-03-03] error: Health check instance_ready with status Unhealthy completed after 32ms with message 'Elasticsearch is not responding'",
		"[2026-03-03] error: Real processing failure happened",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	mainRe := regexp.MustCompile(`(?i)error`)
	ignore, err := compileRegexList([]string{`(?i)Health check .*Elasticsearch is not responding`})
	if err != nil {
		t.Fatalf("compile ignore: %v", err)
	}

	entries, newOffset, err := processLog(path, mainRe, ignore, 0, 0, "vm1")
	if err != nil {
		t.Fatalf("processLog: %v", err)
	}
	if newOffset <= 0 {
		t.Fatalf("expected new offset > 0, got %d", newOffset)
	}
	if len(entries) != 1 {
		t.Fatalf("entries len = %d, want 1", len(entries))
	}
	if !strings.Contains(entries[0].Message, "Real processing failure") {
		t.Fatalf("unexpected remaining message: %q", entries[0].Message)
	}
}

func TestNewOutputSinkValidationAndFallback(t *testing.T) {
	cfg := Config{OutputMode: "influx"}
	if _, err := newOutputSink(cfg); err == nil {
		t.Fatal("expected influx sink validation error")
	}

	cfg = Config{
		OutputMode:       "jsonbin",
		JSONBinBucketID:  "bucket",
		JSONBinAPIKey:    "master",
		JSONBinAccessKey: "access",
	}
	sink, err := newOutputSink(cfg)
	if err != nil {
		t.Fatalf("expected jsonbin sink, got err: %v", err)
	}
	if _, ok := sink.(*JSONBinSink); !ok {
		t.Fatalf("expected *JSONBinSink, got %T", sink)
	}
}

func TestBuildInfluxPayloadContainsTimezoneAndUTC(t *testing.T) {
	entry := LogEntry{
		Timestamp:              "2026-03-05T10:00:00Z",
		Message:                "ERROR boom\nnext",
		Filename:               `C:\logs\app.log`,
		Hostname:               "vm-a",
		MessageHash:            "abc123",
		HostLocalTime:          "2026-03-05T11:00:00+01:00",
		SourceTimezone:         "Europe/Oslo",
		SourceUTCOffsetMinutes: 60,
	}
	payload := buildInfluxPayload("loggoblin_events", []LogEntry{entry}, RunContext{Pattern: "(?i)error", ContextLines: 2})
	if payload == "" {
		t.Fatal("expected non-empty payload")
	}
	if !strings.Contains(payload, "loggoblin_events") {
		t.Fatalf("payload missing measurement: %s", payload)
	}
	if !strings.Contains(payload, "source_timezone=Europe/Oslo") {
		t.Fatalf("payload missing source_timezone tag: %s", payload)
	}
	if !strings.Contains(payload, `host_local_time="2026-03-05T11:00:00+01:00"`) {
		t.Fatalf("payload missing host_local_time field: %s", payload)
	}
	if !strings.Contains(payload, "source_utc_offset_minutes=60i") {
		t.Fatalf("payload missing offset field: %s", payload)
	}
	if !strings.Contains(payload, `message_hash="abc123"`) {
		t.Fatalf("payload missing message_hash field: %s", payload)
	}
}

func TestInfluxSinkSendSuccess(t *testing.T) {
	var gotAuth string
	var gotPath string
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.String()
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sink, err := newInfluxSink(Config{
		InfluxURL:         server.URL,
		InfluxToken:       "tok",
		InfluxOrg:         "org",
		InfluxBucket:      "bucket",
		InfluxMeasurement: "loggoblin_events",
		InfluxInsecure:    true,
	})
	if err != nil {
		t.Fatalf("newInfluxSink: %v", err)
	}
	sink.BaseDelay = 1 * time.Millisecond

	entries := []LogEntry{{
		Timestamp:              "2026-03-05T10:00:00Z",
		Message:                "ERROR boom",
		Filename:               "a.log",
		Hostname:               "vm1",
		SourceTimezone:         "Europe/Oslo",
		SourceUTCOffsetMinutes: 60,
		HostLocalTime:          "2026-03-05T11:00:00+01:00",
	}}
	if err := sink.Send(entries, RunContext{Pattern: "(?i)error", ContextLines: 0}); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if gotAuth != "Token tok" {
		t.Fatalf("unexpected auth header: %q", gotAuth)
	}
	if !strings.Contains(gotPath, "/api/v2/write?") || !strings.Contains(gotPath, "org=org") || !strings.Contains(gotPath, "bucket=bucket") {
		t.Fatalf("unexpected path/query: %s", gotPath)
	}
	if !strings.Contains(gotBody, "loggoblin_events") {
		t.Fatalf("expected measurement in body, got: %s", gotBody)
	}
}

func TestInfluxSinkRetryThenSuccess(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("try again"))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sink, err := newInfluxSink(Config{InfluxURL: server.URL, InfluxToken: "tok", InfluxOrg: "org", InfluxBucket: "bucket", InfluxInsecure: true})
	if err != nil {
		t.Fatalf("newInfluxSink: %v", err)
	}
	sink.BaseDelay = 1 * time.Millisecond
	sink.MaxRetries = 3

	entries := []LogEntry{{Timestamp: "2026-03-05T10:00:00Z", Message: "ERROR x", Filename: "a.log", Hostname: "vm1", SourceTimezone: "UTC", HostLocalTime: "2026-03-05T10:00:00Z"}}
	if err := sink.Send(entries, RunContext{Pattern: "error", ContextLines: 0}); err != nil {
		t.Fatalf("expected retry success, got: %v", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestInfluxSinkPermanentFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("no"))
	}))
	defer server.Close()

	sink, err := newInfluxSink(Config{InfluxURL: server.URL, InfluxToken: "tok", InfluxOrg: "org", InfluxBucket: "bucket", InfluxInsecure: true})
	if err != nil {
		t.Fatalf("newInfluxSink: %v", err)
	}
	sink.BaseDelay = 1 * time.Millisecond
	sink.MaxRetries = 2

	entries := []LogEntry{{Timestamp: "2026-03-05T10:00:00Z", Message: "ERROR x", Filename: "a.log", Hostname: "vm1", SourceTimezone: "UTC", HostLocalTime: "2026-03-05T10:00:00Z"}}
	if err := sink.Send(entries, RunContext{Pattern: "error", ContextLines: 0}); err == nil {
		t.Fatal("expected permanent failure")
	}
}

func TestSourceTimeMetadataFallback(t *testing.T) {
	zone, offset := sourceTimeMetadata(time.Now())
	if zone == "" {
		t.Fatal("expected non-empty timezone label")
	}
	_ = offset
}

func TestConcurrentInfluxRequestCaptureHelper(t *testing.T) {
	// Guards helper behavior used in e2e and demonstrates payload splitting assumptions.
	var mu sync.Mutex
	var payloads []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		payloads = append(payloads, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sink, err := newInfluxSink(Config{InfluxURL: server.URL, InfluxToken: "tok", InfluxOrg: "org", InfluxBucket: "bucket", InfluxInsecure: true})
	if err != nil {
		t.Fatalf("newInfluxSink: %v", err)
	}
	sink.BaseDelay = time.Millisecond

	entries := []LogEntry{{Timestamp: "2026-03-05T10:00:00Z", Message: "ERROR x", Filename: "a.log", Hostname: "vm1", SourceTimezone: "UTC", HostLocalTime: "2026-03-05T10:00:00Z"}}
	if err := sink.Send(entries, RunContext{Pattern: "error", ContextLines: 0}); err != nil {
		t.Fatalf("send: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(payloads) != 1 {
		t.Fatalf("payload count = %d, want 1", len(payloads))
	}
}
