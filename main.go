package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
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
}

// Log entry struct
type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
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

	// Start background process for batch sending
	go batchSender()

	fmt.Println("Monitoring log files:", config.LogFiles)

	// Main loop: Check logs every few seconds
	for {
		for _, file := range config.LogFiles {
			processLog(file)
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

// Read log file from last position and find errors
func processLog(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Get last read position (or start from 0)
	lastPos, exists := filePositions[filePath]
	if !exists {
		lastPos = 0
	}

	// Get current file size
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return
	}
	currentSize := fileInfo.Size()

	// Handle log rotation (if file was truncated, start over)
	if lastPos > currentSize {
		lastPos = 0
	}

	// Move to last known position
	_, err = file.Seek(lastPos, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking in file:", err)
		return
	}

	reader := bufio.NewReader(file)
	errorPattern := regexp.MustCompile(`(?i)ERROR`) // Case-insensitive match

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
			logEntry := LogEntry{
				Timestamp: time.Now().Format(time.RFC3339),
				Message:   line,
			}
			queueMutex.Lock()
			logQueue = append(logQueue, logEntry) // Add to queue
			queueMutex.Unlock()
		}
	}

	// Update last read position
	newPos, _ := file.Seek(0, io.SeekCurrent)
	filePositions[filePath] = newPos
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

// Send batched log entries to JSONBin with authentication headers
func sendToJSONBin(entries []LogEntry) {
	data, err := json.Marshal(entries)
	if err != nil {
		fmt.Println("Error encoding JSON:", err)
		return
	}

	jsonBinEndpoint := fmt.Sprintf(jsonBinURL, config.JSONBinBucketID)
	req, err := http.NewRequest("POST", jsonBinEndpoint, bytes.NewBuffer(data))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}

	// Add authentication headers
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

	fmt.Printf("Sent %d log entries to JSONBin: %s\n", len(entries), resp.Status)
}
