package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
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
	MatchPattern      string   `json:"matchPattern"`
	ContextLines      int      `json:"contextLines"`
}

// Log entry struct
type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
	Filename  string `json:"filename"`
}

// Global variables
var (
	config        Config
	filePositions = make(map[string]int64) // Track last read position per file
	logQueue      []LogEntry               // Store logs before sending
	queueMutex    sync.Mutex
	jsonBinURL    = "https://api.jsonbin.io/v3/b/%s"
)

const (
	checkInterval = 5 * time.Second  // Check logs every 5 seconds
	batchInterval = 30 * time.Second // Send logs every 30 seconds
)

func main() {
	// Load configuration
	err := loadConfig()
	if err != nil {
		fmt.Println("Error loading config:", err)
		return
	}

	go batchSender() // sender til JSONBin i bakgrunnen

	fmt.Println("Monitoring log files:", config.LogFiles, "and directories:", config.LogDirs)

	for {
		// Eksakte filer
		for _, file := range config.LogFiles {
			processLog(file, config.MatchPattern, config.ContextLines)
		}

		// Mapper med søk etter nyeste
		if config.FindNewestInDirs {
			for _, dir := range config.LogDirs {
				filePath, err := newestLogFile(dir)
				if err != nil {
					fmt.Println("Error finding newest log in", dir, ":", err)
					continue
				}
				processLog(filePath, config.MatchPattern, config.ContextLines)
			}
		}

		time.Sleep(checkInterval)
	}
}

// Load configuration from config.json
func loadConfig() error {
	file, err := os.Open("config.json")
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		return err
	}

	fmt.Println("Config loaded successfully.")
	return nil
}

func processLog(filePath string, pattern string, contextLines int) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	errorPattern, err := regexp.Compile(pattern)
	if err != nil {
		fmt.Println("Invalid regex pattern:", err)
		return
	}

	lastPos, exists := filePositions[filePath]
	fmt.Printf("%v %v, lastPos=%d\n", filePath, exists, lastPos)

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return
	}
	currentSize := fileInfo.Size()

	if lastPos > currentSize {
		lastPos = 0 // loggrotasjon
	}

	if !exists || lastPos == 0 {
		lastPos = findLastNLinesOffset(file, 100)
		fmt.Printf("First read: jumping to last 100 lines (offset %d)\n", lastPos)
	}

	_, err = file.Seek(lastPos, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking in file:", err)
		return
	}

	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading file:", err)
			return
		}

		if errorPattern.MatchString(line) {
			var buffer bytes.Buffer
			buffer.WriteString(line)

			// Legg til N ekstra linjer
			for i := 0; i < contextLines; i++ {
				extraLine, err := reader.ReadString('\n')
				if err != nil {
					break
				}
				buffer.WriteString(extraLine)
			}

			logEntry := LogEntry{
				Timestamp: time.Now().Format(time.RFC3339),
				Message:   buffer.String(),
				Filename:  filePath,
			}
			queueMutex.Lock()
			logQueue = append(logQueue, logEntry)
			queueMutex.Unlock()
			fmt.Printf("Matched: %s", line) // Debug
		}
	}

	newPos, _ := file.Seek(0, io.SeekCurrent)
	filePositions[filePath] = newPos
}

func newestLogFile(dir string) (string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	var newestFile string
	var newestTime time.Time

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if strings.HasSuffix(f.Name(), ".log") {
			info, err := f.Info()
			if err != nil {
				continue
			}
			if info.ModTime().After(newestTime) {
				newestTime = info.ModTime()
				newestFile = filepath.Join(dir, f.Name())
			}
		}
	}

	if newestFile == "" {
		return "", fmt.Errorf("no .log files found in %s", dir)
	}
	return newestFile, nil
}

// Hjelpefunksjon for å finne startposisjon slik at vi får ca N siste linjer
func findLastNLinesOffset(file *os.File, n int) int64 {
	const chunkSize = 4096
	stat, _ := file.Stat()
	size := stat.Size()
	var offset int64
	var lines int
	var buf []byte

	for {
		if size-int64(chunkSize) < 0 {
			offset = 0
		} else {
			offset = size - int64(chunkSize)
		}

		buf = make([]byte, size-offset)
		_, err := file.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			return 0 // fallback til start hvis lesefeil
		}

		lines = bytes.Count(buf, []byte{'\n'})
		if lines >= n || offset == 0 {
			break
		}
		size = offset
	}

	// Nå har buf siste chunk med minst N linjer (eller hele fila)
	idx := len(buf)
	for i := 0; i < n; i++ {
		prevIdx := bytes.LastIndex(buf[:idx], []byte{'\n'})
		if prevIdx == -1 {
			return offset // færre enn N linjer i hele fila
		}
		idx = prevIdx
	}

	return offset + int64(idx+1)
}

// Background function to send logs in batches
func batchSender() {
	for {
		time.Sleep(batchInterval)

		queueMutex.Lock()
		if len(logQueue) == 0 {
			queueMutex.Unlock()
			continue // Skip if no logs to send
		}

		// Copy logs and clear queue
		batch := logQueue
		logQueue = nil
		queueMutex.Unlock()

		sendToJSONBin(batch)
	}
}

func fetchExistingLogs() []LogEntry {
	jsonBinEndpoint := fmt.Sprintf(jsonBinURL, config.JSONBinBucketID)

	req, err := http.NewRequest("GET", jsonBinEndpoint, nil)
	if err != nil {
		fmt.Println("Error creating GET request:", err)
		return nil
	}

	// Add authentication headers
	req.Header.Set("X-Master-Key", config.JSONBinAPIKey)
	req.Header.Set("X-Access-Key", config.JSONBinAccessKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error fetching logs from JSONBin:", err)
		return nil
	}
	defer resp.Body.Close()

	// JSONBin wraps data inside "record", so we need a struct to match
	var response struct {
		Record []LogEntry `json:"record"`
	}

	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		fmt.Println("Error decoding JSON response:", err)
		return nil
	}

	return response.Record // Return only the log entries
}

// Send batched log entries to JSONBin with authentication headers
func sendToJSONBin(entries []LogEntry) {
	// Send kun nye entries
	data, err := json.Marshal(entries)
	if err != nil {
		fmt.Println("Error encoding JSON:", err)
		return
	}

	jsonBinEndpoint := fmt.Sprintf(jsonBinURL, config.JSONBinBucketID)
	req, err := http.NewRequest("PUT", jsonBinEndpoint, bytes.NewBuffer(data))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Master-Key", config.JSONBinAPIKey)
	req.Header.Set("X-Access-Key", config.JSONBinAccessKey)
	if config.JSONBinVersioning {
		req.Header.Set("X-Bin-Versioning", "true")
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	defer resp.Body.Close()

	fmt.Printf("Updated JSONBin with %d new entries. HTTP Status: %s\n", len(entries), resp.Status)
}
