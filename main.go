package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Configuration struct for JSON file
type Config struct {
	JSONBinBucketID   string   `json:"jsonBinBucketID"`
	JSONBinAPIKey     string   `json:"jsonBinAPIKey"`
	JSONBinAccessKey  string   `json:"jsonBinAccessKey"`
	JSONBinVersioning bool     `json:"jsonBinVersioning"`
	LogFiles          []string `json:"logFiles"`
	LogDirs           []string `json:"logDirs"`
	FindNewestInDirs  bool     `json:"findNewestInDirs"`
	MaxFilesPerDir    int      `json:"maxFilesPerDir"`
	MatchPattern      string   `json:"matchPattern"`
	IgnorePatterns    []string `json:"ignorePatterns"`
	ContextLines      int      `json:"contextLines"`
	Hostname          string   `json:"hostname"`
	OutputMode        string   `json:"outputMode"`
	InfluxURL         string   `json:"influxURL"`
	InfluxToken       string   `json:"influxToken"`
	InfluxOrg         string   `json:"influxOrg"`
	InfluxBucket      string   `json:"influxBucket"`
	InfluxInsecure    bool     `json:"influxInsecure"`
	InfluxMeasurement string   `json:"influxMeasurement"`
}

// Log entry struct
type LogEntry struct {
	Timestamp              string `json:"timestamp"`
	Message                string `json:"message"`
	Filename               string `json:"filename"`
	Hostname               string `json:"hostname"`
	MessageHash            string `json:"messageHash,omitempty"`
	HostLocalTime          string `json:"hostLocalTime"`
	SourceTimezone         string `json:"sourceTimezone"`
	SourceUTCOffsetMinutes int    `json:"sourceUtcOffsetMinutes"`
}

type FileCheckpoint struct {
	Offset          int64  `json:"offset"`
	UpdatedAt       string `json:"updatedAt"`
	Size            int64  `json:"size,omitempty"`
	ModTimeUnixNano int64  `json:"modTimeUnixNano,omitempty"`
	HeadHash        string `json:"headHash,omitempty"`
}

type CheckpointState struct {
	Files map[string]FileCheckpoint `json:"files"`
}

type FileIdentity struct {
	Size            int64
	ModTimeUnixNano int64
	HeadHash        string
}

type RuntimeOptions struct {
	Mode               string
	ConfigPath         string
	StatePath          string
	LockPath           string
	NoUpload           bool
	LockStaleAfter     time.Duration
	OutputModeOverride string
}

type RunSummary struct {
	Mode         string     `json:"mode"`
	FilesScanned []string   `json:"filesScanned"`
	Matches      int        `json:"matches"`
	Entries      []LogEntry `json:"entries"`
}

type RunContext struct {
	Pattern      string
	ContextLines int
}

type OutputSink interface {
	Send(entries []LogEntry, ctx RunContext) error
}

type InfluxSink struct {
	URL         string
	Token       string
	Org         string
	Bucket      string
	Measurement string
	Insecure    bool
	Client      *http.Client
	MaxRetries  int
	BaseDelay   time.Duration
}

type JSONBinSink struct {
	Config Config
	Client *http.Client
}

var (
	jsonBinURL = "https://api.jsonbin.io/v3/b/%s"
	errLocked  = errors.New("lock already held")
)

const (
	checkInterval            = 5 * time.Second
	defaultMaxFilesDir       = 3
	maxUploadRetries         = 5
	defaultInfluxMeasure     = "loggoblin_events"
	defaultOutputMode        = "influx"
	defaultInfluxBaseDelay   = 300 * time.Millisecond
	defaultHTTPClientTimeout = 20 * time.Second
	fileHeadHashBytes        = 4096
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func run() error {
	debug := flag.Bool("debug", false, "Enable debug output")
	verboseShort := flag.Bool("v", false, "Verbose output")
	verboseLong := flag.Bool("verbose", false, "Verbose output")
	pattern := flag.String("r", "", "Regex pattern to search for")
	logFile := flag.String("f", "", "Log file to scan")
	logDir := flag.String("d", "", "Log directory to scan")
	configPath := flag.String("config", "config.json", "Path to config file")
	mode := flag.String("mode", "oneshot", "Run mode: oneshot or daemon")
	statePathFlag := flag.String("state", "", "Path to checkpoint state file")
	lockPathFlag := flag.String("lock", "", "Path to lock file")
	lockStaleAfter := flag.Duration("lock-stale-after", 48*time.Hour, "Reclaim lock file automatically when older than this duration")
	noUpload := flag.Bool("no-upload", false, "Skip sink upload and print deterministic summary JSON")
	outputMode := flag.String("output-mode", "", "Output sink mode: influx or jsonbin")
	influxURL := flag.String("influx-url", "", "InfluxDB URL")
	influxToken := flag.String("influx-token", "", "InfluxDB token")
	influxOrg := flag.String("influx-org", "", "InfluxDB org")
	influxBucket := flag.String("influx-bucket", "", "InfluxDB bucket")
	influxMeasurement := flag.String("influx-measurement", "", "InfluxDB measurement")
	influxInsecure := flag.String("influx-insecure", "", "Override Influx TLS verify mode (true/false)")
	flag.Parse()
	verbose := *debug || *verboseShort || *verboseLong

	runtimeMode := strings.ToLower(strings.TrimSpace(*mode))
	if runtimeMode != "oneshot" && runtimeMode != "daemon" {
		return fmt.Errorf("invalid --mode %q (must be oneshot or daemon)", runtimeMode)
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if *pattern != "" {
		cfg.MatchPattern = *pattern
	}
	if *logFile != "" {
		cfg.LogFiles = []string{*logFile}
	}
	if *logDir != "" {
		cfg.LogDirs = []string{*logDir}
		cfg.FindNewestInDirs = true
	}
	if cfg.MaxFilesPerDir <= 0 {
		cfg.MaxFilesPerDir = defaultMaxFilesDir
	}
	if cfg.InfluxMeasurement == "" {
		cfg.InfluxMeasurement = defaultInfluxMeasure
	}

	if strings.TrimSpace(*outputMode) != "" {
		cfg.OutputMode = strings.TrimSpace(*outputMode)
	}
	if strings.TrimSpace(*influxURL) != "" {
		cfg.InfluxURL = strings.TrimSpace(*influxURL)
	}
	if strings.TrimSpace(*influxToken) != "" {
		cfg.InfluxToken = strings.TrimSpace(*influxToken)
	}
	if strings.TrimSpace(*influxOrg) != "" {
		cfg.InfluxOrg = strings.TrimSpace(*influxOrg)
	}
	if strings.TrimSpace(*influxBucket) != "" {
		cfg.InfluxBucket = strings.TrimSpace(*influxBucket)
	}
	if strings.TrimSpace(*influxMeasurement) != "" {
		cfg.InfluxMeasurement = strings.TrimSpace(*influxMeasurement)
	}
	if strings.TrimSpace(*influxInsecure) != "" {
		parsed, err := strconv.ParseBool(strings.TrimSpace(*influxInsecure))
		if err != nil {
			return fmt.Errorf("invalid --influx-insecure value: %w", err)
		}
		cfg.InfluxInsecure = parsed
	}

	resolvedOutputMode, err := resolveOutputMode(cfg.OutputMode)
	if err != nil {
		return err
	}
	cfg.OutputMode = resolvedOutputMode

	resolvedStatePath := strings.TrimSpace(*statePathFlag)
	if resolvedStatePath == "" {
		configDir := filepath.Dir(*configPath)
		resolvedStatePath = filepath.Join(configDir, "loggoblin.state.json")
	}
	resolvedLockPath := strings.TrimSpace(*lockPathFlag)
	if resolvedLockPath == "" {
		resolvedLockPath = resolvedStatePath + ".lock"
	}

	opts := RuntimeOptions{
		Mode:               runtimeMode,
		ConfigPath:         *configPath,
		StatePath:          resolvedStatePath,
		LockPath:           resolvedLockPath,
		NoUpload:           *noUpload,
		LockStaleAfter:     *lockStaleAfter,
		OutputModeOverride: strings.TrimSpace(*outputMode),
	}

	lockFile, err := acquireLock(opts.LockPath, opts.LockStaleAfter)
	if err != nil {
		if errors.Is(err, errLocked) {
			fmt.Fprintln(os.Stderr, "Another instance is running; skipping this run")
			return nil
		}
		return fmt.Errorf("acquire lock: %w", err)
	}
	defer releaseLock(lockFile, opts.LockPath)

	state, err := loadCheckpointState(opts.StatePath)
	if err != nil {
		return fmt.Errorf("load checkpoint state: %w", err)
	}

	var sink OutputSink
	if !opts.NoUpload {
		sink, err = newOutputSink(cfg)
		if err != nil {
			return fmt.Errorf("configure output sink: %w", err)
		}
	}

	if opts.Mode == "oneshot" {
		_, err := runScanCycle(cfg, state, opts, sink, verbose)
		return err
	}

	for {
		if _, err := runScanCycle(cfg, state, opts, sink, verbose); err != nil {
			fmt.Fprintln(os.Stderr, "Cycle error:", err)
		}
		time.Sleep(checkInterval)
	}
}

func runScanCycle(cfg Config, state *CheckpointState, opts RuntimeOptions, sink OutputSink, debug bool) (RunSummary, error) {
	summary := RunSummary{Mode: opts.Mode, FilesScanned: []string{}, Entries: []LogEntry{}}
	files, err := gatherTargetFiles(cfg)
	if err != nil {
		return summary, err
	}

	re, err := regexp.Compile(cfg.MatchPattern)
	if err != nil {
		return summary, fmt.Errorf("invalid regex pattern: %w", err)
	}
	ignoreRegexes, err := compileRegexList(cfg.IgnorePatterns)
	if err != nil {
		return summary, fmt.Errorf("invalid ignore pattern: %w", err)
	}

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = cfg.Hostname
	}

	updates := make(map[string]FileCheckpoint)
	entries := make([]LogEntry, 0)

	for _, file := range files {
		fileInfo, err := os.Stat(file)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Skipping file stat error:", file, err)
			continue
		}
		identity, err := computeFileIdentity(file, fileInfo)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Skipping file identity error:", file, err)
			continue
		}

		prev := state.Files[file]
		replaced := fileLooksReplaced(prev, identity)
		startOffset := resolveStartOffset(fileInfo.Size(), prev, replaced)
		matched, newOffset, err := processLog(file, re, ignoreRegexes, cfg.ContextLines, startOffset, hostname)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Skipping file read error:", file, err)
			continue
		}

		summary.FilesScanned = append(summary.FilesScanned, file)
		updates[file] = FileCheckpoint{
			Offset:          newOffset,
			Size:            maxInt64(identity.Size, newOffset),
			ModTimeUnixNano: identity.ModTimeUnixNano,
			HeadHash:        identity.HeadHash,
		}
		entries = append(entries, matched...)

		if debug {
			fmt.Fprintf(os.Stderr, "Scanned %s from %d to %d (%d matches, replaced=%v)\n", file, startOffset, newOffset, len(matched), replaced)
		}
	}

	summary.Entries = entries
	summary.Matches = len(entries)

	if len(entries) > 0 && !opts.NoUpload {
		if sink == nil {
			return summary, errors.New("output sink is nil")
		}
		runCtx := RunContext{Pattern: cfg.MatchPattern, ContextLines: cfg.ContextLines}
		if err := sink.Send(entries, runCtx); err != nil {
			return summary, err
		}
	}

	applyOffsets(state, updates, time.Now().UTC())
	if err := saveCheckpointState(opts.StatePath, state); err != nil {
		return summary, fmt.Errorf("save checkpoint state: %w", err)
	}

	if opts.NoUpload {
		if err := emitSummary(summary); err != nil {
			return summary, fmt.Errorf("emit summary: %w", err)
		}
	}
	if opts.Mode == "oneshot" || debug {
		printCycleSummary(summary, files, cfg.OutputMode, opts.NoUpload)
	}

	return summary, nil
}

func emitSummary(summary RunSummary) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	return enc.Encode(summary)
}

func loadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()

	cfg := Config{
		MaxFilesPerDir:    defaultMaxFilesDir,
		OutputMode:        defaultOutputMode,
		InfluxInsecure:    true,
		InfluxMeasurement: defaultInfluxMeasure,
	}

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func resolveOutputMode(mode string) (string, error) {
	resolved := strings.ToLower(strings.TrimSpace(mode))
	if resolved == "" {
		resolved = defaultOutputMode
	}
	switch resolved {
	case "influx", "jsonbin":
		return resolved, nil
	default:
		return "", fmt.Errorf("invalid output mode %q (must be influx or jsonbin)", mode)
	}
}

func gatherTargetFiles(cfg Config) ([]string, error) {
	seen := make(map[string]struct{})
	files := make([]string, 0, len(cfg.LogFiles)+len(cfg.LogDirs)*cfg.MaxFilesPerDir)

	for _, file := range cfg.LogFiles {
		clean := filepath.Clean(file)
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		files = append(files, clean)
	}

	if cfg.FindNewestInDirs {
		limit := cfg.MaxFilesPerDir
		if limit <= 0 {
			limit = defaultMaxFilesDir
		}
		for _, dir := range cfg.LogDirs {
			selected, err := newestLogFiles(dir, limit)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Directory scan warning:", err)
				continue
			}
			for _, file := range selected {
				if _, ok := seen[file]; ok {
					continue
				}
				seen[file] = struct{}{}
				files = append(files, file)
			}
		}
	}

	return files, nil
}

func newestLogFiles(dir string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("invalid limit %d for dir %s", limit, dir)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	type candidate struct {
		path string
		name string
		mod  time.Time
	}

	candidates := make([]candidate, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.EqualFold(filepath.Ext(e.Name()), ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		candidates = append(candidates, candidate{
			path: filepath.Join(dir, e.Name()),
			name: e.Name(),
			mod:  info.ModTime(),
		})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no .log files found in %s", dir)
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].mod.Equal(candidates[j].mod) {
			return candidates[i].name < candidates[j].name
		}
		return candidates[i].mod.After(candidates[j].mod)
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.path)
	}
	return out, nil
}

func processLog(filePath string, re *regexp.Regexp, ignore []*regexp.Regexp, contextLines int, startOffset int64, hostname string) ([]LogEntry, int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	if _, err := file.Seek(startOffset, io.SeekStart); err != nil {
		return nil, 0, err
	}

	reader := bufio.NewReader(file)
	entries := make([]LogEntry, 0)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				if line == "" {
					break
				}
				// Process trailing line without newline.
			} else {
				return nil, 0, err
			}
		}

		if re.MatchString(line) {
			if matchesAnyRegex(ignore, line) {
				if errors.Is(err, io.EOF) {
					break
				}
				continue
			}
			var buffer bytes.Buffer
			buffer.WriteString(line)
			for i := 0; i < contextLines; i++ {
				extraLine, extraErr := reader.ReadString('\n')
				if extraLine != "" {
					buffer.WriteString(extraLine)
				}
				if extraErr != nil {
					break
				}
			}

			nowLocal := time.Now()
			tzLabel, tzOffsetMin := sourceTimeMetadata(nowLocal)
			msg := buffer.String()
			entries = append(entries, LogEntry{
				Timestamp:              nowLocal.UTC().Format(time.RFC3339Nano),
				Message:                msg,
				Filename:               filePath,
				Hostname:               hostname,
				MessageHash:            hashMessage(msg),
				HostLocalTime:          nowLocal.Format(time.RFC3339Nano),
				SourceTimezone:         tzLabel,
				SourceUTCOffsetMinutes: tzOffsetMin,
			})
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	newPos, err := currentReaderOffset(file, reader)
	if err != nil {
		return nil, 0, err
	}

	return entries, newPos, nil
}

func compileRegexList(patterns []string) ([]*regexp.Regexp, error) {
	out := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", p, err)
		}
		out = append(out, re)
	}
	return out, nil
}

func matchesAnyRegex(regexes []*regexp.Regexp, line string) bool {
	for _, re := range regexes {
		if re.MatchString(line) {
			return true
		}
	}
	return false
}

func currentReaderOffset(file *os.File, reader *bufio.Reader) (int64, error) {
	pos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return pos - int64(reader.Buffered()), nil
}

func resolveStartOffset(fileSize int64, cp FileCheckpoint, replaced bool) int64 {
	if replaced {
		return 0
	}
	if cp.Offset <= 0 {
		return fileSize
	}
	if cp.Offset > fileSize {
		return fileSize
	}
	return cp.Offset
}

func loadCheckpointState(path string) (*CheckpointState, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &CheckpointState{Files: make(map[string]FileCheckpoint)}, nil
		}
		return nil, err
	}
	defer file.Close()

	var state CheckpointState
	if err := json.NewDecoder(file).Decode(&state); err != nil {
		return nil, err
	}
	if state.Files == nil {
		state.Files = make(map[string]FileCheckpoint)
	}
	return &state, nil
}

func applyOffsets(state *CheckpointState, offsets map[string]FileCheckpoint, now time.Time) {
	if state.Files == nil {
		state.Files = make(map[string]FileCheckpoint)
	}
	stamp := now.Format(time.RFC3339)
	for file, cp := range offsets {
		cp.UpdatedAt = stamp
		state.Files[file] = cp
	}
}

func fileLooksReplaced(cp FileCheckpoint, current FileIdentity) bool {
	// Legacy state without identity metadata falls back to offset-only logic.
	if cp.HeadHash == "" && cp.Size == 0 && cp.ModTimeUnixNano == 0 {
		return false
	}
	if cp.HeadHash != "" && current.HeadHash != "" && cp.HeadHash != current.HeadHash {
		return true
	}
	if cp.Size > current.Size {
		return true
	}
	return false
}

func computeFileIdentity(path string, info os.FileInfo) (FileIdentity, error) {
	file, err := os.Open(path)
	if err != nil {
		return FileIdentity{}, err
	}
	defer file.Close()

	buf := make([]byte, fileHeadHashBytes)
	n, err := file.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return FileIdentity{}, err
	}
	head := sha256.Sum256(buf[:n])

	return FileIdentity{
		Size:            info.Size(),
		ModTimeUnixNano: info.ModTime().UnixNano(),
		HeadHash:        hex.EncodeToString(head[:]),
	}, nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func saveCheckpointState(path string, state *CheckpointState) error {
	if state.Files == nil {
		state.Files = make(map[string]FileCheckpoint)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(path)
		if err := os.Rename(tmpPath, path); err != nil {
			return err
		}
	}
	return nil
}

func acquireLock(path string, staleAfter time.Duration) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	f, err := tryCreateLock(path)
	if err == nil {
		return f, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	if staleAfter <= 0 {
		return nil, errLocked
	}

	info, statErr := os.Stat(path)
	if statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			// Another process released lock between create and stat.
			return acquireLock(path, staleAfter)
		}
		return nil, errLocked
	}
	if time.Since(info.ModTime()) <= staleAfter {
		return nil, errLocked
	}

	if removeErr := os.Remove(path); removeErr != nil {
		if !errors.Is(removeErr, os.ErrNotExist) {
			return nil, errLocked
		}
	}

	f, err = tryCreateLock(path)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, errLocked
		}
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "Reclaimed stale lock %s (older than %s)\n", path, staleAfter)
	return f, nil
}

func tryCreateLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	_, _ = fmt.Fprintf(f, "%d\n", os.Getpid())
	return f, nil
}

func releaseLock(file *os.File, path string) {
	if file != nil {
		_ = file.Close()
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintln(os.Stderr, "Lock cleanup warning:", err)
	}
}

func newOutputSink(cfg Config) (OutputSink, error) {
	mode, err := resolveOutputMode(cfg.OutputMode)
	if err != nil {
		return nil, err
	}
	switch mode {
	case "influx":
		return newInfluxSink(cfg)
	case "jsonbin":
		if strings.TrimSpace(cfg.JSONBinBucketID) == "" || strings.TrimSpace(cfg.JSONBinAPIKey) == "" || strings.TrimSpace(cfg.JSONBinAccessKey) == "" {
			return nil, errors.New("jsonbin mode requires jsonBinBucketID, jsonBinAPIKey, and jsonBinAccessKey")
		}
		return &JSONBinSink{
			Config: cfg,
			Client: &http.Client{Timeout: defaultHTTPClientTimeout},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported output mode %q", mode)
	}
}

func newInfluxSink(cfg Config) (*InfluxSink, error) {
	missing := make([]string, 0)
	if strings.TrimSpace(cfg.InfluxURL) == "" {
		missing = append(missing, "influxURL")
	}
	if strings.TrimSpace(cfg.InfluxToken) == "" {
		missing = append(missing, "influxToken")
	}
	if strings.TrimSpace(cfg.InfluxOrg) == "" {
		missing = append(missing, "influxOrg")
	}
	if strings.TrimSpace(cfg.InfluxBucket) == "" {
		missing = append(missing, "influxBucket")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("influx mode missing required config: %s", strings.Join(missing, ", "))
	}
	if _, err := url.ParseRequestURI(strings.TrimSpace(cfg.InfluxURL)); err != nil {
		return nil, fmt.Errorf("invalid influxURL: %w", err)
	}
	measurement := strings.TrimSpace(cfg.InfluxMeasurement)
	if measurement == "" {
		measurement = defaultInfluxMeasure
	}

	transport := &http.Transport{}
	if cfg.InfluxInsecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	return &InfluxSink{
		URL:         strings.TrimRight(strings.TrimSpace(cfg.InfluxURL), "/"),
		Token:       strings.TrimSpace(cfg.InfluxToken),
		Org:         strings.TrimSpace(cfg.InfluxOrg),
		Bucket:      strings.TrimSpace(cfg.InfluxBucket),
		Measurement: measurement,
		Insecure:    cfg.InfluxInsecure,
		Client:      &http.Client{Timeout: defaultHTTPClientTimeout, Transport: transport},
		MaxRetries:  maxUploadRetries,
		BaseDelay:   defaultInfluxBaseDelay,
	}, nil
}

func (s *InfluxSink) Send(entries []LogEntry, ctx RunContext) error {
	if len(entries) == 0 {
		return nil
	}
	payload := buildInfluxPayload(s.Measurement, entries, ctx)
	if payload == "" {
		return nil
	}

	baseDelay := s.BaseDelay
	if baseDelay <= 0 {
		baseDelay = defaultInfluxBaseDelay
	}
	maxRetries := s.MaxRetries
	if maxRetries <= 0 {
		maxRetries = maxUploadRetries
	}

	endpoint := fmt.Sprintf("%s/api/v2/write?org=%s&bucket=%s&precision=ns",
		s.URL,
		url.QueryEscape(s.Org),
		url.QueryEscape(s.Bucket),
	)

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Token "+s.Token)
		req.Header.Set("Content-Type", "text/plain; charset=utf-8")

		resp, err := s.Client.Do(req)
		if err == nil {
			func() {
				defer resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					lastErr = nil
					return
				}
				body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
				lastErr = fmt.Errorf("influx write status %s: %s", resp.Status, strings.TrimSpace(string(body)))
			}()
			if lastErr == nil {
				return nil
			}
		} else {
			lastErr = err
		}

		if attempt == maxRetries {
			break
		}
		jitter := time.Duration(rand.Intn(200)) * time.Millisecond
		time.Sleep(baseDelay*time.Duration(1<<(attempt-1)) + jitter)
	}

	if lastErr == nil {
		lastErr = errors.New("unknown influx write failure")
	}
	return fmt.Errorf("influx upload failed after %d attempts: %w", maxRetries, lastErr)
}

func (s *JSONBinSink) Send(entries []LogEntry, _ RunContext) error {
	return appendJSONBinWithRetry(s.Client, s.Config, entries)
}

func appendJSONBinWithRetry(client *http.Client, cfg Config, incoming []LogEntry) error {
	baseDelay := 300 * time.Millisecond
	var lastErr error

	for attempt := 1; attempt <= maxUploadRetries; attempt++ {
		existing, err := fetchExistingLogs(client, cfg)
		if err != nil {
			lastErr = err
		} else {
			merged := mergeEntries(existing, incoming)
			if err := putLogs(client, cfg, merged); err == nil {
				return nil
			} else {
				lastErr = err
			}
		}

		if attempt == maxUploadRetries {
			break
		}

		jitter := time.Duration(rand.Intn(200)) * time.Millisecond
		time.Sleep(baseDelay*time.Duration(1<<(attempt-1)) + jitter)
	}

	if lastErr == nil {
		lastErr = errors.New("unknown upload failure")
	}
	return fmt.Errorf("jsonbin upload failed after %d attempts: %w", maxUploadRetries, lastErr)
}

func fetchExistingLogs(client *http.Client, cfg Config) ([]LogEntry, error) {
	jsonBinEndpoint := fmt.Sprintf(jsonBinURL, cfg.JSONBinBucketID)
	req, err := http.NewRequest(http.MethodGet, jsonBinEndpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Master-Key", cfg.JSONBinAPIKey)
	req.Header.Set("X-Access-Key", cfg.JSONBinAccessKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("jsonbin GET status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var response struct {
		Record []LogEntry `json:"record"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}
	if response.Record == nil {
		response.Record = []LogEntry{}
	}
	return response.Record, nil
}

func putLogs(client *http.Client, cfg Config, entries []LogEntry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}

	jsonBinEndpoint := fmt.Sprintf(jsonBinURL, cfg.JSONBinBucketID)
	req, err := http.NewRequest(http.MethodPut, jsonBinEndpoint, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Master-Key", cfg.JSONBinAPIKey)
	req.Header.Set("X-Access-Key", cfg.JSONBinAccessKey)
	if cfg.JSONBinVersioning {
		req.Header.Set("X-Bin-Versioning", "true")
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("jsonbin PUT status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	return nil
}

func mergeEntries(existing []LogEntry, incoming []LogEntry) []LogEntry {
	merged := make([]LogEntry, 0, len(existing)+len(incoming))
	seen := make(map[string]struct{}, len(existing)+len(incoming))

	for _, entry := range existing {
		key := logEntryKey(entry)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, entry)
	}

	for _, entry := range incoming {
		key := logEntryKey(entry)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, entry)
	}

	return merged
}

func logEntryKey(entry LogEntry) string {
	return entry.Hostname + "|" + entry.Filename + "|" + normalizeLog(entry.Message)
}

func buildInfluxPayload(measurement string, entries []LogEntry, ctx RunContext) string {
	lines := make([]string, 0, len(entries))
	for _, entry := range entries {
		ts := time.Now().UTC()
		if entry.Timestamp != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, entry.Timestamp); err == nil {
				ts = parsed.UTC()
			}
		}
		hostLocalTime := entry.HostLocalTime
		if hostLocalTime == "" {
			hostLocalTime = ts.Local().Format(time.RFC3339Nano)
		}
		messageHash := entry.MessageHash
		if messageHash == "" {
			messageHash = hashMessage(entry.Message)
		}

		fields := map[string]any{
			"message":                   entry.Message,
			"message_hash":              messageHash,
			"context_lines":             ctx.ContextLines,
			"source_utc_offset_minutes": entry.SourceUTCOffsetMinutes,
			"host_local_time":           hostLocalTime,
		}
		tags := map[string]string{
			"host":            entry.Hostname,
			"source_file":     entry.Filename,
			"pattern":         ctx.Pattern,
			"source_timezone": entry.SourceTimezone,
		}
		line, ok := toLineProtocol(measurement, tags, fields, ts.UnixNano())
		if ok {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

func toLineProtocol(measurement string, tags map[string]string, fields map[string]any, tsNs int64) (string, bool) {
	measurement = strings.TrimSpace(measurement)
	if measurement == "" {
		return "", false
	}

	var b strings.Builder
	b.WriteString(escapeMeasurement(measurement))

	tagKeys := make([]string, 0, len(tags))
	for k, v := range tags {
		if strings.TrimSpace(v) == "" {
			continue
		}
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)
	for _, k := range tagKeys {
		b.WriteByte(',')
		b.WriteString(escapeTagKey(k))
		b.WriteByte('=')
		b.WriteString(escapeTagValue(tags[k]))
	}

	fieldKeys := make([]string, 0, len(fields))
	for k := range fields {
		fieldKeys = append(fieldKeys, k)
	}
	sort.Strings(fieldKeys)

	writtenFields := 0
	b.WriteByte(' ')
	for _, k := range fieldKeys {
		encoded, ok := influxFieldValue(fields[k])
		if !ok {
			continue
		}
		if writtenFields > 0 {
			b.WriteByte(',')
		}
		b.WriteString(escapeTagKey(k))
		b.WriteByte('=')
		b.WriteString(encoded)
		writtenFields++
	}
	if writtenFields == 0 {
		return "", false
	}

	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(tsNs, 10))
	return b.String(), true
}

func escapeMeasurement(v string) string {
	repl := strings.NewReplacer(
		"\\", "\\\\",
		",", "\\,",
		" ", "\\ ",
	)
	return repl.Replace(v)
}

func escapeTagKey(v string) string {
	repl := strings.NewReplacer(
		"\\", "\\\\",
		",", "\\,",
		" ", "\\ ",
		"=", "\\=",
	)
	return repl.Replace(v)
}

func escapeTagValue(v string) string {
	return escapeTagKey(v)
}

func escapeFieldString(v string) string {
	repl := strings.NewReplacer(
		"\\", "\\\\",
		"\"", "\\\"",
		"\n", "\\n",
	)
	return repl.Replace(v)
}

func influxFieldValue(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", escapeFieldString(t)), true
	case int:
		return fmt.Sprintf("%di", t), true
	case int8:
		return fmt.Sprintf("%di", t), true
	case int16:
		return fmt.Sprintf("%di", t), true
	case int32:
		return fmt.Sprintf("%di", t), true
	case int64:
		return fmt.Sprintf("%di", t), true
	case uint:
		return fmt.Sprintf("%du", t), true
	case uint8:
		return fmt.Sprintf("%du", t), true
	case uint16:
		return fmt.Sprintf("%du", t), true
	case uint32:
		return fmt.Sprintf("%du", t), true
	case uint64:
		return fmt.Sprintf("%du", t), true
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 64), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	case bool:
		if t {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}

func sourceTimeMetadata(t time.Time) (string, int) {
	_, offsetSec := t.Zone()
	offsetMin := offsetSec / 60
	zoneName := strings.TrimSpace(t.Location().String())
	if zoneName == "" || zoneName == "Local" {
		zoneName = formatUTCOffset(offsetMin)
	}
	return zoneName, offsetMin
}

func printCycleSummary(summary RunSummary, targetFiles []string, outputMode string, noUpload bool) {
	modeLabel := outputMode
	if noUpload {
		modeLabel = "no-upload"
	}
	fmt.Fprintf(
		os.Stderr,
		"Run summary: targets=%d scanned=%d matches=%d output=%s\n",
		len(targetFiles),
		len(summary.FilesScanned),
		summary.Matches,
		modeLabel,
	)
	if len(targetFiles) == 0 {
		fmt.Fprintln(os.Stderr, "  No log files resolved from configuration.")
		return
	}
	fmt.Fprintln(os.Stderr, "  Resolved files:")
	for _, f := range targetFiles {
		fmt.Fprintf(os.Stderr, "    - %s\n", f)
	}
}

func formatUTCOffset(offsetMinutes int) string {
	sign := "+"
	if offsetMinutes < 0 {
		sign = "-"
		offsetMinutes = -offsetMinutes
	}
	hours := offsetMinutes / 60
	mins := offsetMinutes % 60
	return fmt.Sprintf("UTC%s%02d:%02d", sign, hours, mins)
}

func normalizeLog(msg string) string {
	re := regexp.MustCompile(`\d+`)
	clean := re.ReplaceAllString(msg, "")
	return strings.TrimSpace(clean)
}

func hashMessage(msg string) string {
	normalized := normalizeLog(msg)
	hash := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(hash[:])
}
