package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type statusRange struct {
	Start int
	End   int
}

type Config struct {
	URL               string
	Concurrency       int
	Duration          time.Duration
	Method            string
	Payload           string
	Headers           string
	SuccessStatusSpec string
	SuccessStatus     []statusRange
	RequestsPerSecond float64
	OutputFormats     []string
	OutputPrefix      string
	FromReport        string
	ServeWeb          bool
	WebAddr           string
	DashboardReport   string
}

type Result struct {
	Latency    time.Duration
	StatusCode int
	BytesRead  int64
	Error      error
}

type LatencyStats struct {
	Min time.Duration
	Max time.Duration
	Avg time.Duration
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
}

type Summary struct {
	TotalRequests       int
	FailedRequests      int
	TransportErrors     int
	HTTPFailures        int
	SuccessRequests     int
	SuccessRate         float64
	ErrorRate           float64
	RequestsPerSecond   float64
	LatencyStats        LatencyStats
	TotalDataMB         float64
	AvgThroughputMB     float64
	ActualDuration      time.Duration
	StatusCodes         map[int]int
	TransportErrorTypes map[string]int
}

type TestReport struct {
	StartTime         string         `json:"start_time"`
	URL               string         `json:"url"`
	Concurrency       int            `json:"concurrency"`
	Duration          string         `json:"duration"`
	Method            string         `json:"method"`
	Payload           string         `json:"payload"`
	Headers           []string       `json:"headers"`
	SuccessStatusSpec string         `json:"success_status"`
	RateLimitRPS      float64        `json:"rate_limit_rps"`
	OutputFormats     []string       `json:"output_formats"`
	StatusCodes       map[string]int `json:"status_codes"`
	ErrorTypes        map[string]int `json:"transport_error_types"`
	TotalRequests     int            `json:"total_requests"`
	FailedRequests    int            `json:"failed_requests"`
	TransportErrors   int            `json:"transport_errors"`
	HTTPFailures      int            `json:"http_failures"`
	SuccessRequests   int            `json:"success_requests"`
	SuccessRate       float64        `json:"success_rate"`
	ErrorRate         float64        `json:"error_rate"`
	RequestsPerSecond float64        `json:"requests_per_second"`
	MinLatency        string         `json:"min_latency"`
	MaxLatency        string         `json:"max_latency"`
	AverageLatency    string         `json:"average_latency"`
	P50Latency        string         `json:"p50_latency"`
	P95Latency        string         `json:"p95_latency"`
	P99Latency        string         `json:"p99_latency"`
	TotalDataMB       float64        `json:"total_data_mb"`
	AvgThroughputMB   float64        `json:"avg_throughput_mb_s"`
	ActualDuration    string         `json:"actual_duration"`
}

type countEntry struct {
	Key   string
	Value int
}

type chartBar struct {
	Label string
	Value string
	Width float64
	Tone  string
}

type htmlReportView struct {
	Report            TestReport
	StatusCodes       []countEntry
	ErrorTypes        []countEntry
	SuccessPct        float64
	FailurePct        float64
	TransportErrorPct float64
	HTTPFailurePct    float64
	LatencyBars       []chartBar
	StatusCodeBars    []chartBar
	ErrorBars         []chartBar
	SummaryHeadline   string
	SummaryParagraphs []string
	ReliabilityNote   string
	LatencyNote       string
	ThroughputNote    string
}

type rateLimiter interface {
	Wait(context.Context) error
	Stop()
}

type noopLimiter struct{}

func (noopLimiter) Wait(context.Context) error { return nil }
func (noopLimiter) Stop()                      {}

type tickerLimiter struct {
	tokens chan struct{}
	stop   chan struct{}
	done   chan struct{}
}

func newRateLimiter(rps float64) (rateLimiter, error) {
	if rps < 0 {
		return nil, errors.New("--rps must be 0 or greater")
	}
	if rps == 0 {
		return noopLimiter{}, nil
	}

	interval := time.Duration(float64(time.Second) / rps)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	limiter := &tickerLimiter{
		tokens: make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	limiter.tokens <- struct{}{}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer close(limiter.done)

		for {
			select {
			case <-ticker.C:
				select {
				case limiter.tokens <- struct{}{}:
				default:
				}
			case <-limiter.stop:
				return
			}
		}
	}()

	return limiter, nil
}

func (l *tickerLimiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.tokens:
		return nil
	}
}

func (l *tickerLimiter) Stop() {
	select {
	case <-l.stop:
		return
	default:
		close(l.stop)
		<-l.done
	}
}

func parseFlags() (Config, error) {
	urlFlag := flag.String("url", "", "Target URL to stress test (required)")
	concurrency := flag.Int("c", 10, "Number of concurrent workers (goroutines)")
	duration := flag.Int("d", 10, "Duration of test in seconds")
	method := flag.String("method", "GET", "HTTP method (GET or POST)")
	payload := flag.String("payload", "", "Request payload for POST requests")
	headers := flag.String("headers", "", "Custom headers in key:value format, separated by commas")
	successStatus := flag.String("success-status", "200-399", "Comma-separated successful status codes/ranges (example: 200-299,304)")
	rps := flag.Float64("rps", 0, "Global requests per second limit across all workers (0 disables limiting)")
	formats := flag.String("formats", "json", "Output formats: comma-separated json,csv,html")
	output := flag.String("output", "report", "Output file prefix without extension")
	fromReport := flag.String("from-report", "", "Render reports from an existing JSON report instead of running a new test")
	serveWeb := flag.Bool("serve-web", false, "Run the protected web dashboard")
	webAddr := flag.String("web-addr", ":8088", "Address for the web dashboard server")
	dashboardReport := flag.String("dashboard-report", "", "Load dashboard data from a JSON report file")
	flag.Parse()

	cfg := Config{
		URL:               strings.TrimSpace(*urlFlag),
		Concurrency:       *concurrency,
		Duration:          time.Duration(*duration) * time.Second,
		Method:            strings.ToUpper(strings.TrimSpace(*method)),
		Payload:           *payload,
		Headers:           strings.TrimSpace(*headers),
		SuccessStatusSpec: strings.TrimSpace(*successStatus),
		RequestsPerSecond: *rps,
		OutputPrefix:      strings.TrimSpace(*output),
		FromReport:        strings.TrimSpace(*fromReport),
		ServeWeb:          *serveWeb,
		WebAddr:           strings.TrimSpace(*webAddr),
		DashboardReport:   strings.TrimSpace(*dashboardReport),
	}

	parsedFormats, err := parseOutputFormats(*formats)
	if err != nil {
		return Config{}, err
	}
	cfg.OutputFormats = parsedFormats

	if cfg.FromReport != "" && cfg.OutputPrefix == "report" {
		cfg.OutputPrefix = strings.TrimSuffix(cfg.FromReport, filepath.Ext(cfg.FromReport))
	}

	return validateAndNormalizeConfig(cfg)
}

func validateAndNormalizeConfig(cfg Config) (Config, error) {
	if cfg.ServeWeb {
		if cfg.WebAddr == "" {
			return Config{}, errors.New("--web-addr must not be empty")
		}
		return cfg, nil
	}

	if cfg.FromReport != "" {
		if cfg.OutputPrefix == "" {
			return Config{}, errors.New("--output must not be empty")
		}
		if len(cfg.OutputFormats) == 0 {
			return Config{}, errors.New("--formats must include at least one format")
		}
		return cfg, nil
	}

	if cfg.URL == "" {
		return Config{}, errors.New("--url is required")
	}

	parsedURL, err := url.ParseRequestURI(cfg.URL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return Config{}, fmt.Errorf("invalid --url value %q", cfg.URL)
	}

	if cfg.Concurrency <= 0 {
		return Config{}, errors.New("--c must be greater than 0")
	}

	if cfg.Duration <= 0 {
		return Config{}, errors.New("--d must be greater than 0")
	}

	switch cfg.Method {
	case http.MethodGet, http.MethodPost:
	default:
		return Config{}, fmt.Errorf("--method must be GET or POST, got %q", cfg.Method)
	}

	if _, err := parseHeaders(cfg.Headers); err != nil {
		return Config{}, err
	}

	if cfg.Method == http.MethodGet && cfg.Payload != "" {
		return Config{}, errors.New("--payload is only supported with --method POST")
	}

	successRanges, err := parseStatusRanges(cfg.SuccessStatusSpec)
	if err != nil {
		return Config{}, err
	}
	cfg.SuccessStatus = successRanges

	if cfg.RequestsPerSecond < 0 {
		return Config{}, errors.New("--rps must be 0 or greater")
	}

	if cfg.OutputPrefix == "" {
		return Config{}, errors.New("--output must not be empty")
	}

	if len(cfg.OutputFormats) == 0 {
		return Config{}, errors.New("--formats must include at least one format")
	}

	return cfg, nil
}

func worker(client *http.Client, cfg Config, deadline time.Time, limiter rateLimiter, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	headers, _ := parseHeaders(cfg.Headers)

	for {
		if time.Now().After(deadline) {
			return
		}

		waitCtx, cancelWait := context.WithDeadline(context.Background(), deadline)
		err := limiter.Wait(waitCtx)
		cancelWait()
		if err != nil {
			return
		}

		req, cancelReq, err := newRequest(cfg, headers, deadline)
		if err != nil {
			results <- Result{Error: err}
			continue
		}

		start := time.Now()
		resp, err := client.Do(req)
		latency := time.Since(start)

		result := Result{Latency: latency}
		if err != nil {
			cancelReq()
			result.Error = err
			results <- result
			continue
		}

		n, readErr := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		cancelReq()

		result.StatusCode = resp.StatusCode
		result.BytesRead = n
		if readErr != nil {
			result.Error = readErr
		}

		results <- result
	}
}

func newRequest(cfg Config, headers http.Header, deadline time.Time) (*http.Request, context.CancelFunc, error) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	var body io.Reader
	if cfg.Method == http.MethodPost && cfg.Payload != "" {
		body = bytes.NewBufferString(cfg.Payload)
	}

	req, err := http.NewRequestWithContext(ctx, cfg.Method, cfg.URL, body)
	if err != nil {
		cancel()
		return nil, nil, err
	}

	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	if cfg.Method == http.MethodPost && cfg.Payload != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	return req, cancel, nil
}

func parseHeaders(headerString string) (http.Header, error) {
	headers := make(http.Header)
	if strings.TrimSpace(headerString) == "" {
		return headers, nil
	}

	headerPairs := strings.Split(headerString, ",")
	for _, pair := range headerPairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid header %q, expected key:value", pair)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" || value == "" {
			return nil, fmt.Errorf("invalid header %q, expected non-empty key and value", pair)
		}

		headers.Add(key, value)
	}

	return headers, nil
}

func parseStatusRanges(spec string) ([]statusRange, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, errors.New("--success-status must not be empty")
	}

	parts := strings.Split(spec, ",")
	ranges := make([]statusRange, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if strings.Contains(part, "-") {
			bounds := strings.SplitN(part, "-", 2)
			if len(bounds) != 2 {
				return nil, fmt.Errorf("invalid status range %q", part)
			}

			start, err := parseStatusCode(bounds[0])
			if err != nil {
				return nil, fmt.Errorf("invalid status range %q: %w", part, err)
			}
			end, err := parseStatusCode(bounds[1])
			if err != nil {
				return nil, fmt.Errorf("invalid status range %q: %w", part, err)
			}
			if start > end {
				return nil, fmt.Errorf("invalid status range %q: start must be <= end", part)
			}

			ranges = append(ranges, statusRange{Start: start, End: end})
			continue
		}

		code, err := parseStatusCode(part)
		if err != nil {
			return nil, fmt.Errorf("invalid status code %q: %w", part, err)
		}
		ranges = append(ranges, statusRange{Start: code, End: code})
	}

	if len(ranges) == 0 {
		return nil, errors.New("--success-status must include at least one code or range")
	}

	return ranges, nil
}

func parseStatusCode(value string) (int, error) {
	code, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, errors.New("must be an integer")
	}
	if code < 100 || code > 599 {
		return 0, errors.New("must be between 100 and 599")
	}
	return code, nil
}

func parseOutputFormats(value string) ([]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, errors.New("--formats must not be empty")
	}

	allowed := map[string]bool{
		"json": true,
		"csv":  true,
		"html": true,
	}

	var formats []string
	seen := make(map[string]bool)
	for _, part := range strings.Split(value, ",") {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		if !allowed[part] {
			return nil, fmt.Errorf("unsupported format %q, expected json,csv,html", part)
		}
		if seen[part] {
			continue
		}
		seen[part] = true
		formats = append(formats, part)
	}

	if len(formats) == 0 {
		return nil, errors.New("--formats must include at least one format")
	}

	return formats, nil
}

func runStressTest(cfg Config) error {
	limiter, err := newRateLimiter(cfg.RequestsPerSecond)
	if err != nil {
		return err
	}
	defer limiter.Stop()

	client := &http.Client{}

	var wg sync.WaitGroup
	results := make(chan Result, cfg.Concurrency*100)

	startTime := time.Now()
	deadline := startTime.Add(cfg.Duration)

	stopProgress := make(chan struct{})
	go showProgress(startTime, cfg.Duration, stopProgress)

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go worker(client, cfg, deadline, limiter, results, &wg)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	summary := Summary{
		StatusCodes:         make(map[int]int),
		TransportErrorTypes: make(map[string]int),
	}

	var (
		latencies  []time.Duration
		totalBytes int64
	)

	for result := range results {
		summary.TotalRequests++
		totalBytes += result.BytesRead

		if result.Error != nil {
			summary.FailedRequests++
			summary.TransportErrors++
			summary.TransportErrorTypes[classifyError(result.Error)]++
			continue
		}

		summary.StatusCodes[result.StatusCode]++
		if statusMatches(result.StatusCode, cfg.SuccessStatus) {
			summary.SuccessRequests++
			latencies = append(latencies, result.Latency)
			continue
		}

		summary.FailedRequests++
		summary.HTTPFailures++
	}

	close(stopProgress)

	elapsed := time.Since(startTime)
	summary.ActualDuration = elapsed
	summary.TotalDataMB, summary.AvgThroughputMB = ComputeThroughput(totalBytes, elapsed)
	summary.SuccessRate, summary.ErrorRate = computeRates(summary.SuccessRequests, summary.FailedRequests, summary.TotalRequests)
	if elapsed > 0 {
		summary.RequestsPerSecond = float64(summary.TotalRequests) / elapsed.Seconds()
	}

	if len(latencies) > 0 {
		summary.LatencyStats = ComputeLatencyStats(latencies)
	}

	return printAndSaveReport(cfg, startTime, summary)
}

func showProgress(start time.Time, duration time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(start)
			percent := float64(elapsed) / float64(duration) * 100
			if percent > 100 {
				percent = 100
			}
			fmt.Printf("\rTest Progress: %.1f%%", percent)
		case <-done:
			fmt.Printf("\rTest Completed: 100%%\n\n")
			return
		}
	}
}

func printAndSaveReport(cfg Config, startTime time.Time, summary Summary) error {
	headers, err := parseHeaders(cfg.Headers)
	if err != nil {
		return err
	}

	headersList := formatHeaders(headers)
	statusCodes := formatStatusCodes(summary.StatusCodes)

	fmt.Println("1. Test Configuration")
	fmt.Println("---------------------")
	fmt.Printf("Test Start Time:       %s\n", startTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("Target URL:            %s\n", cfg.URL)
	fmt.Printf("Concurrency Level:     %d\n", cfg.Concurrency)
	fmt.Printf("Test Duration:         %s\n", cfg.Duration)
	fmt.Printf("Request Method:        %s\n", cfg.Method)
	fmt.Printf("Payload/Body:          %s\n", safeDisplayValue(cfg.Payload))
	fmt.Printf("Headers Used:          %s\n", safeDisplayValue(strings.Join(headersList, ", ")))
	fmt.Printf("Success Statuses:      %s\n", cfg.SuccessStatusSpec)
	fmt.Printf("RPS Limit:             %s\n", formatRateLimit(cfg.RequestsPerSecond))
	fmt.Printf("Output Formats:        %s\n", strings.Join(cfg.OutputFormats, ", "))
	fmt.Printf("Output Prefix:         %s\n", cfg.OutputPrefix)

	fmt.Println()
	fmt.Println("2. Request Statistics")
	fmt.Println("---------------------")
	fmt.Printf("Total Requests Sent:   %10d\n", summary.TotalRequests)
	fmt.Printf("Failed Requests:       %10d\n", summary.FailedRequests)
	fmt.Printf("Transport Errors:      %10d\n", summary.TransportErrors)
	fmt.Printf("HTTP Failures:         %10d\n", summary.HTTPFailures)
	fmt.Printf("Success Requests:      %10d\n", summary.SuccessRequests)
	fmt.Printf("Success Rate:          %10.2f%%\n", summary.SuccessRate)
	fmt.Printf("Error Rate:            %10.2f%%\n", summary.ErrorRate)
	fmt.Printf("Requests per Second:   %10.2f\n", summary.RequestsPerSecond)
	fmt.Printf("Status Codes:          %s\n", safeDisplayValue(strings.Join(statusCodes, ", ")))

	if len(summary.TransportErrorTypes) > 0 {
		fmt.Printf("Transport Error Types: %s\n", strings.Join(formatStringCounts(summary.TransportErrorTypes), ", "))
	}

	if summary.SuccessRequests > 0 {
		fmt.Println()
		fmt.Println("3. Latency Statistics")
		fmt.Println("---------------------")
		fmt.Printf("Min Latency:           %15s\n", formatDuration(summary.LatencyStats.Min))
		fmt.Printf("Max Latency:           %15s\n", formatDuration(summary.LatencyStats.Max))
		fmt.Printf("Average Latency:       %15s\n", formatDuration(summary.LatencyStats.Avg))
		fmt.Printf("p50 Latency:           %15s\n", formatDuration(summary.LatencyStats.P50))
		fmt.Printf("p95 Latency:           %15s\n", formatDuration(summary.LatencyStats.P95))
		fmt.Printf("p99 Latency:           %15s\n", formatDuration(summary.LatencyStats.P99))
	}

	fmt.Println()
	fmt.Println("4. Throughput")
	fmt.Println("-------------")
	fmt.Printf("Total Data Transferred: %9.2f MB\n", summary.TotalDataMB)
	fmt.Printf("Average Throughput:     %9.2f MB/s\n", summary.AvgThroughputMB)
	fmt.Printf("Actual Duration:        %s\n", formatDuration(summary.ActualDuration))
	fmt.Println()

	report := buildReport(cfg, startTime, headersList, summary)
	files, err := writeReports(report, cfg.OutputPrefix, cfg.OutputFormats)
	if err != nil {
		return err
	}

	fmt.Printf("Saved Reports:         %s\n", strings.Join(files, ", "))
	return nil
}

func buildReport(cfg Config, startTime time.Time, headersList []string, summary Summary) TestReport {
	return TestReport{
		StartTime:         startTime.Format("2006-01-02 15:04:05"),
		URL:               cfg.URL,
		Concurrency:       cfg.Concurrency,
		Duration:          cfg.Duration.String(),
		Method:            cfg.Method,
		Payload:           cfg.Payload,
		Headers:           headersList,
		SuccessStatusSpec: cfg.SuccessStatusSpec,
		RateLimitRPS:      cfg.RequestsPerSecond,
		OutputFormats:     append([]string(nil), cfg.OutputFormats...),
		StatusCodes:       stringifyIntMap(summary.StatusCodes),
		ErrorTypes:        cloneStringMap(summary.TransportErrorTypes),
		TotalRequests:     summary.TotalRequests,
		FailedRequests:    summary.FailedRequests,
		TransportErrors:   summary.TransportErrors,
		HTTPFailures:      summary.HTTPFailures,
		SuccessRequests:   summary.SuccessRequests,
		SuccessRate:       summary.SuccessRate,
		ErrorRate:         summary.ErrorRate,
		RequestsPerSecond: summary.RequestsPerSecond,
		MinLatency:        formatDuration(summary.LatencyStats.Min),
		MaxLatency:        formatDuration(summary.LatencyStats.Max),
		AverageLatency:    formatDuration(summary.LatencyStats.Avg),
		P50Latency:        formatDuration(summary.LatencyStats.P50),
		P95Latency:        formatDuration(summary.LatencyStats.P95),
		P99Latency:        formatDuration(summary.LatencyStats.P99),
		TotalDataMB:       summary.TotalDataMB,
		AvgThroughputMB:   summary.AvgThroughputMB,
		ActualDuration:    formatDuration(summary.ActualDuration),
	}
}

func writeReports(report TestReport, prefix string, formats []string) ([]string, error) {
	var files []string

	for _, format := range formats {
		var (
			data []byte
			err  error
		)

		switch format {
		case "json":
			data, err = renderJSONReport(report)
		case "csv":
			data, err = renderCSVReport(report)
		case "html":
			data, err = renderHTMLReport(report)
		default:
			err = fmt.Errorf("unsupported format %q", format)
		}
		if err != nil {
			return nil, err
		}

		path := fmt.Sprintf("%s.%s", prefix, format)
		if err := ensureParentDir(path); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return nil, err
		}

		files = append(files, path)
	}

	return files, nil
}

func renderJSONReport(report TestReport) ([]byte, error) {
	return json.MarshalIndent(report, "", "  ")
}

func renderCSVReport(report TestReport) ([]byte, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)

	rows := [][]string{
		{"metric", "value"},
		{"start_time", report.StartTime},
		{"url", report.URL},
		{"concurrency", fmt.Sprintf("%d", report.Concurrency)},
		{"duration", report.Duration},
		{"method", report.Method},
		{"payload", report.Payload},
		{"headers", strings.Join(report.Headers, "; ")},
		{"success_status", report.SuccessStatusSpec},
		{"rate_limit_rps", fmt.Sprintf("%.2f", report.RateLimitRPS)},
		{"output_formats", strings.Join(report.OutputFormats, ",")},
		{"total_requests", fmt.Sprintf("%d", report.TotalRequests)},
		{"failed_requests", fmt.Sprintf("%d", report.FailedRequests)},
		{"transport_errors", fmt.Sprintf("%d", report.TransportErrors)},
		{"http_failures", fmt.Sprintf("%d", report.HTTPFailures)},
		{"success_requests", fmt.Sprintf("%d", report.SuccessRequests)},
		{"success_rate", fmt.Sprintf("%.2f", report.SuccessRate)},
		{"error_rate", fmt.Sprintf("%.2f", report.ErrorRate)},
		{"requests_per_second", fmt.Sprintf("%.2f", report.RequestsPerSecond)},
		{"min_latency", report.MinLatency},
		{"max_latency", report.MaxLatency},
		{"average_latency", report.AverageLatency},
		{"p50_latency", report.P50Latency},
		{"p95_latency", report.P95Latency},
		{"p99_latency", report.P99Latency},
		{"total_data_mb", fmt.Sprintf("%.2f", report.TotalDataMB)},
		{"avg_throughput_mb_s", fmt.Sprintf("%.2f", report.AvgThroughputMB)},
		{"actual_duration", report.ActualDuration},
	}

	for _, entry := range formatStringMapEntries("status_code", report.StatusCodes) {
		rows = append(rows, []string{entry[0], entry[1]})
	}
	for _, entry := range formatStringMapEntries("transport_error", report.ErrorTypes) {
		rows = append(rows, []string{entry[0], entry[1]})
	}

	if err := writer.WriteAll(rows); err != nil {
		return nil, err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func renderHTMLReport(report TestReport) ([]byte, error) {
	const reportTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>gostress report</title>
  <style>
    :root {
      --ink: #14304a;
      --muted: #5f7084;
      --line: #d7dee7;
      --paper: #fbfaf6;
      --panel: #ffffff;
      --accent: #2f6f62;
      --accent-soft: #dff3ed;
      --warn: #c76a28;
      --warn-soft: #fde9d8;
      --danger: #a63a3a;
      --danger-soft: #f9dede;
      --shadow: 0 14px 36px rgba(20, 48, 74, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 32px;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(47,111,98,0.10), transparent 28%),
        radial-gradient(circle at top right, rgba(199,106,40,0.10), transparent 24%),
        linear-gradient(180deg, #fcfbf8 0%, #f4efe7 100%);
    }
    .page { max-width: 1180px; margin: 0 auto; }
    h1, h2, h3 { color: #10273d; margin-top: 0; }
    p { line-height: 1.65; }
    .hero {
      background: linear-gradient(135deg, rgba(16,39,61,0.96), rgba(47,111,98,0.92));
      color: #f7fbff;
      border-radius: 24px;
      padding: 32px;
      box-shadow: var(--shadow);
      margin-bottom: 22px;
    }
    .hero h1 { color: #ffffff; font-size: 40px; margin-bottom: 8px; }
    .hero p { color: rgba(247,251,255,0.88); max-width: 900px; }
    .meta { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 18px; }
    .pill {
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(255,255,255,0.12);
      border: 1px solid rgba(255,255,255,0.16);
      font-size: 14px;
    }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 16px; margin-bottom: 22px; }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 20px;
      box-shadow: var(--shadow);
    }
    .kpi {
      font-size: 34px;
      line-height: 1;
      margin: 8px 0 4px;
      font-weight: bold;
    }
    .muted { color: var(--muted); }
    .section { margin-bottom: 22px; }
    .section-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 12px;
    }
    .prose {
      background: rgba(255,255,255,0.76);
      border: 1px solid rgba(215,222,231,0.8);
      border-radius: 18px;
      padding: 20px;
      box-shadow: var(--shadow);
    }
    .prose p:last-child { margin-bottom: 0; }
    .stack {
      width: 100%;
      height: 20px;
      display: flex;
      overflow: hidden;
      border-radius: 999px;
      background: #edf2f7;
      border: 1px solid #dde5ee;
      margin: 12px 0 10px;
    }
    .stack .success { background: linear-gradient(90deg, #2f6f62, #3e9e89); }
    .stack .http { background: linear-gradient(90deg, #c76a28, #e59a53); }
    .stack .transport { background: linear-gradient(90deg, #a63a3a, #d96a6a); }
    .legend { display: flex; flex-wrap: wrap; gap: 12px; font-size: 14px; color: var(--muted); }
    .legend span::before {
      content: "";
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      margin-right: 8px;
      vertical-align: middle;
    }
    .legend .success::before { background: #2f6f62; }
    .legend .http::before { background: #c76a28; }
    .legend .transport::before { background: #a63a3a; }
    .bar-group { display: grid; gap: 12px; margin-top: 14px; }
    .bar-row { display: grid; grid-template-columns: 130px 1fr 96px; gap: 12px; align-items: center; }
    .bar-track {
      width: 100%;
      height: 16px;
      background: #edf2f7;
      border-radius: 999px;
      overflow: hidden;
      border: 1px solid #dde5ee;
    }
    .bar-fill {
      height: 100%;
      border-radius: 999px;
      min-width: 6px;
    }
    .tone-teal { background: linear-gradient(90deg, #2f6f62, #61b8a7); }
    .tone-amber { background: linear-gradient(90deg, #c76a28, #f1b36d); }
    .tone-red { background: linear-gradient(90deg, #a63a3a, #e28787); }
    .tone-blue { background: linear-gradient(90deg, #255f8f, #6bb3e4); }
    .two-col { display: grid; grid-template-columns: 1.1fr 0.9fr; gap: 16px; }
    .notes { display: grid; gap: 14px; }
    .note {
      padding: 16px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: #fffdfa;
    }
    table { width: 100%; border-collapse: collapse; margin-top: 12px; background: #ffffff; border-radius: 14px; overflow: hidden; }
    th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid #e9ecef; }
    th { background: #f0f4f8; }
    @media (max-width: 860px) {
      body { padding: 18px; }
      .hero { padding: 22px; }
      .hero h1 { font-size: 30px; }
      .two-col { grid-template-columns: 1fr; }
      .bar-row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <p class="muted">Generated at {{.Report.StartTime}}</p>
      <h1>gostress HTML Report</h1>
      <p>{{.SummaryHeadline}}</p>
      <div class="meta">
        <span class="pill">Target: {{.Report.URL}}</span>
        <span class="pill">Method: {{.Report.Method}}</span>
        <span class="pill">Concurrency: {{.Report.Concurrency}}</span>
        <span class="pill">Duration: {{.Report.Duration}}</span>
        <span class="pill">Actual: {{.Report.ActualDuration}}</span>
      </div>
    </section>

    <section class="section prose">
      <div class="section-head">
        <h2>Executive Summary</h2>
        <span class="muted">Readable narrative for quick review</span>
      </div>
      {{range .SummaryParagraphs}}
      <p>{{.}}</p>
      {{end}}
    </section>

    <section class="section grid">
      <div class="card">
        <div class="muted">Total requests</div>
        <div class="kpi">{{.Report.TotalRequests}}</div>
        <div class="muted">Across {{.Report.ActualDuration}}</div>
      </div>
      <div class="card">
        <div class="muted">Success rate</div>
        <div class="kpi">{{printf "%.2f%%" .Report.SuccessRate}}</div>
        <div class="muted">{{.Report.SuccessRequests}} successful responses</div>
      </div>
      <div class="card">
        <div class="muted">Observed throughput</div>
        <div class="kpi">{{printf "%.2f" .Report.RequestsPerSecond}}</div>
        <div class="muted">Requests per second</div>
      </div>
      <div class="card">
        <div class="muted">Average latency</div>
        <div class="kpi">{{.Report.AverageLatency}}</div>
        <div class="muted">p95 {{.Report.P95Latency}} • p99 {{.Report.P99Latency}}</div>
      </div>
    </section>

    <section class="section two-col">
      <div class="card">
        <div class="section-head">
          <h2>Reliability Breakdown</h2>
          <span class="muted">Success vs failure mix</span>
        </div>
        <div class="stack">
          <div class="success" style="width: {{printf "%.2f" .SuccessPct}}%"></div>
          <div class="http" style="width: {{printf "%.2f" .HTTPFailurePct}}%"></div>
          <div class="transport" style="width: {{printf "%.2f" .TransportErrorPct}}%"></div>
        </div>
        <div class="legend">
          <span class="success">Success {{printf "%.2f%%" .SuccessPct}}</span>
          <span class="http">HTTP failures {{printf "%.2f%%" .HTTPFailurePct}}</span>
          <span class="transport">Transport errors {{printf "%.2f%%" .TransportErrorPct}}</span>
        </div>
        <p class="muted">{{.ReliabilityNote}}</p>
      </div>
      <div class="card">
        <div class="section-head">
          <h2>Configuration Snapshot</h2>
          <span class="muted">How this run was executed</span>
        </div>
        <p><strong>Success statuses:</strong> {{.Report.SuccessStatusSpec}}</p>
        <p><strong>RPS limit:</strong> {{formatRate .Report.RateLimitRPS}}</p>
        <p><strong>Output formats:</strong> {{join .Report.OutputFormats ", "}}</p>
        <p><strong>Headers:</strong> {{defaultText (join .Report.Headers ", ") "(none)"}}</p>
        <p><strong>Payload:</strong> {{defaultText .Report.Payload "(none)"}}</p>
      </div>
    </section>

    <section class="section two-col">
      <div class="card">
        <div class="section-head">
          <h2>Latency Chart</h2>
          <span class="muted">Relative to max latency</span>
        </div>
        <div class="bar-group">
          {{range .LatencyBars}}
          <div class="bar-row">
            <div><strong>{{.Label}}</strong></div>
            <div class="bar-track"><div class="bar-fill {{.Tone}}" style="width: {{printf "%.2f" .Width}}%"></div></div>
            <div class="muted">{{.Value}}</div>
          </div>
          {{end}}
        </div>
        <p class="muted">{{.LatencyNote}}</p>
      </div>
      <div class="notes">
        <div class="note">
          <h3>Throughput Note</h3>
          <p>{{.ThroughputNote}}</p>
        </div>
        <div class="note">
          <h3>Failure Note</h3>
          <p>{{.ReliabilityNote}}</p>
        </div>
      </div>
    </section>

    <section class="section two-col">
      <div class="card">
        <div class="section-head">
          <h2>Status Code Distribution</h2>
          <span class="muted">Observed HTTP responses</span>
        </div>
        <div class="bar-group">
          {{range .StatusCodeBars}}
          <div class="bar-row">
            <div><strong>{{.Label}}</strong></div>
            <div class="bar-track"><div class="bar-fill {{.Tone}}" style="width: {{printf "%.2f" .Width}}%"></div></div>
            <div class="muted">{{.Value}}</div>
          </div>
          {{else}}
          <p class="muted">No HTTP responses recorded.</p>
          {{end}}
        </div>
      </div>
      <div class="card">
        <div class="section-head">
          <h2>Transport Error Distribution</h2>
          <span class="muted">Network and timeout level failures</span>
        </div>
        <div class="bar-group">
          {{range .ErrorBars}}
          <div class="bar-row">
            <div><strong>{{.Label}}</strong></div>
            <div class="bar-track"><div class="bar-fill {{.Tone}}" style="width: {{printf "%.2f" .Width}}%"></div></div>
            <div class="muted">{{.Value}}</div>
          </div>
          {{else}}
          <p class="muted">No transport errors recorded.</p>
          {{end}}
        </div>
      </div>
    </section>

    <section class="section two-col">
      <div class="card">
        <h2>Status Codes Table</h2>
        <table>
          <thead><tr><th>Code</th><th>Count</th></tr></thead>
          <tbody>
            {{range .StatusCodes}}
            <tr><td>{{.Key}}</td><td>{{.Value}}</td></tr>
            {{else}}
            <tr><td colspan="2">No HTTP responses recorded</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
      <div class="card">
        <h2>Transport Errors Table</h2>
        <table>
          <thead><tr><th>Error</th><th>Count</th></tr></thead>
          <tbody>
            {{range .ErrorTypes}}
            <tr><td>{{.Key}}</td><td>{{.Value}}</td></tr>
            {{else}}
            <tr><td colspan="2">No transport errors recorded</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>
  </div>
</body>
</html>`

	tmpl, err := template.New("report").Funcs(template.FuncMap{
		"join":        strings.Join,
		"defaultText": defaultText,
		"formatRate":  formatRateLimit,
	}).Parse(reportTemplate)
	if err != nil {
		return nil, err
	}

	view := buildHTMLReportView(report)

	var buffer bytes.Buffer
	if err := tmpl.Execute(&buffer, view); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func buildHTMLReportView(report TestReport) htmlReportView {
	total := report.TotalRequests
	successPct := percentOf(report.SuccessRequests, total)
	httpFailurePct := percentOf(report.HTTPFailures, total)
	transportErrorPct := percentOf(report.TransportErrors, total)

	statusEntries := sortedStringMapEntries(report.StatusCodes)
	errorEntries := sortedStringMapEntries(report.ErrorTypes)

	return htmlReportView{
		Report:            report,
		StatusCodes:       statusEntries,
		ErrorTypes:        errorEntries,
		SuccessPct:        successPct,
		FailurePct:        percentOf(report.FailedRequests, total),
		TransportErrorPct: transportErrorPct,
		HTTPFailurePct:    httpFailurePct,
		LatencyBars: []chartBar{
			durationBar("p50", report.P50Latency, report.MaxLatency, "tone-blue"),
			durationBar("Average", report.AverageLatency, report.MaxLatency, "tone-teal"),
			durationBar("p95", report.P95Latency, report.MaxLatency, "tone-amber"),
			durationBar("p99", report.P99Latency, report.MaxLatency, "tone-red"),
			durationBar("Max", report.MaxLatency, report.MaxLatency, "tone-red"),
		},
		StatusCodeBars:    countBars(statusEntries, "tone-teal"),
		ErrorBars:         countBars(errorEntries, "tone-red"),
		SummaryHeadline:   buildSummaryHeadline(report),
		SummaryParagraphs: buildSummaryParagraphs(report),
		ReliabilityNote:   buildReliabilityNote(report),
		LatencyNote:       buildLatencyNote(report),
		ThroughputNote:    buildThroughputNote(report),
	}
}

func buildSummaryHeadline(report TestReport) string {
	return fmt.Sprintf(
		"This run sent %d requests to %s over %s, sustained %.2f requests per second, and finished with a %.2f%% success rate.",
		report.TotalRequests,
		report.URL,
		report.ActualDuration,
		report.RequestsPerSecond,
		report.SuccessRate,
	)
}

func buildSummaryParagraphs(report TestReport) []string {
	paragraphs := []string{
		fmt.Sprintf(
			"The test exercised %s with %d concurrent workers for a configured duration of %s. Success was defined by the status range %s, and the observed throughput reached %.2f requests per second while transferring %.2f MB in total.",
			report.URL,
			report.Concurrency,
			report.Duration,
			report.SuccessStatusSpec,
			report.RequestsPerSecond,
			report.TotalDataMB,
		),
		buildReliabilityNote(report),
		buildLatencyNote(report),
	}

	if note := buildThroughputNote(report); note != "" {
		paragraphs = append(paragraphs, note)
	}

	return paragraphs
}

func buildReliabilityNote(report TestReport) string {
	switch {
	case report.TotalRequests == 0:
		return "No requests were recorded, so there is not enough data yet to judge reliability."
	case report.FailedRequests == 0:
		return "No failures were observed. That points to a stable target for this test profile, with both application responses and transport behavior remaining healthy throughout the run."
	case report.ErrorRate < 0.1:
		return fmt.Sprintf("Failures were rare at %.4f%% of total traffic. The system looks broadly stable, with most of the risk concentrated in isolated outliers rather than a systemic collapse.", report.ErrorRate)
	case report.HTTPFailures > report.TransportErrors:
		return fmt.Sprintf("Most failures came back as HTTP responses rather than transport drops. That usually means the application stayed reachable but started pushing back or returning error responses under load.")
	default:
		return fmt.Sprintf("A noticeable portion of failures were transport-level issues. That often points to timeout pressure, connection churn, or an overloaded upstream path rather than only business-layer response errors.")
	}
}

func buildLatencyNote(report TestReport) string {
	maxLatency := parseDuration(report.MaxLatency)
	p99Latency := parseDuration(report.P99Latency)
	avgLatency := parseDuration(report.AverageLatency)

	switch {
	case maxLatency == 0 && avgLatency == 0:
		return "Latency data was not available for this run."
	case p99Latency > avgLatency*5 && avgLatency > 0:
		return fmt.Sprintf("Tail latency stretches well beyond the average. The average response completed in %s, but p99 reached %s, which suggests bursts of queueing or intermittent saturation under pressure.", report.AverageLatency, report.P99Latency)
	case p99Latency > avgLatency*2 && avgLatency > 0:
		return fmt.Sprintf("Latency stayed fairly controlled for most requests, though the tail still widened under load. p99 at %s is meaningfully above the %s average and is worth watching if user-facing SLAs matter.", report.P99Latency, report.AverageLatency)
	default:
		return fmt.Sprintf("Latency stayed relatively compact across the distribution. The average was %s, p95 was %s, and the worst observed request was %s.", report.AverageLatency, report.P95Latency, report.MaxLatency)
	}
}

func buildThroughputNote(report TestReport) string {
	if report.RateLimitRPS > 0 {
		return fmt.Sprintf("This run was rate-limited to %.2f requests per second. The observed %.2f requests per second should therefore be interpreted as a shaped workload rather than the absolute ceiling of the target.", report.RateLimitRPS, report.RequestsPerSecond)
	}
	return fmt.Sprintf("No explicit RPS cap was configured, so the observed %.2f requests per second reflects the combined effect of target responsiveness, client concurrency, and runtime overhead during the test window.", report.RequestsPerSecond)
}

func durationBar(label, value, maxValue, tone string) chartBar {
	maxDuration := parseDuration(maxValue)
	current := parseDuration(value)
	width := 0.0
	if maxDuration > 0 {
		width = (float64(current) / float64(maxDuration)) * 100
	}
	if width == 0 && current > 0 {
		width = 2
	}
	if width > 100 {
		width = 100
	}

	return chartBar{
		Label: label,
		Value: value,
		Width: width,
		Tone:  tone,
	}
}

func countBars(entries []countEntry, tone string) []chartBar {
	maxValue := 0
	for _, entry := range entries {
		if entry.Value > maxValue {
			maxValue = entry.Value
		}
	}

	bars := make([]chartBar, 0, len(entries))
	for _, entry := range entries {
		width := 0.0
		if maxValue > 0 {
			width = (float64(entry.Value) / float64(maxValue)) * 100
		}
		if width == 0 && entry.Value > 0 {
			width = 2
		}
		bars = append(bars, chartBar{
			Label: entry.Key,
			Value: fmt.Sprintf("%d", entry.Value),
			Width: width,
			Tone:  tone,
		})
	}

	return bars
}

func parseDuration(value string) time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return d
}

func percentOf(value, total int) float64 {
	if total == 0 {
		return 0
	}
	return (float64(value) / float64(total)) * 100
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func loadReportFromFile(path string) (TestReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return TestReport{}, err
	}

	var report TestReport
	if err := json.Unmarshal(data, &report); err != nil {
		return TestReport{}, err
	}

	return report, nil
}

func exportExistingReport(cfg Config) error {
	report, err := loadReportFromFile(cfg.FromReport)
	if err != nil {
		return err
	}

	files, err := writeReports(report, cfg.OutputPrefix, cfg.OutputFormats)
	if err != nil {
		return err
	}

	fmt.Printf("Rendered Reports:      %s\n", strings.Join(files, ", "))
	return nil
}

func main() {
	fmt.Printf("%s Running stress test suite\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf("%s Parsing input arguments\n", time.Now().Format("2006-01-02 15:04:05"))

	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		flag.Usage()
		os.Exit(2)
	}

	if cfg.FromReport != "" {
		fmt.Printf("%s Rendering report from %s\n", time.Now().Format("2006-01-02 15:04:05"), cfg.FromReport)
		if err := exportExistingReport(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if cfg.ServeWeb {
		fmt.Printf("%s Starting protected web dashboard on %s\n", time.Now().Format("2006-01-02 15:04:05"), cfg.WebAddr)
		if err := serveWeb(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("%s Begin stress test\n", time.Now().Format("2006-01-02 15:04:05"))
	if err := runStressTest(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func ComputePercentile(latencies []time.Duration, percentile float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	return percentileFromSorted(sorted, percentile)
}

func ComputeLatencyStats(latencies []time.Duration) LatencyStats {
	if len(latencies) == 0 {
		return LatencyStats{}
	}

	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	var total time.Duration
	for _, latency := range sorted {
		total += latency
	}

	return LatencyStats{
		Min: sorted[0],
		Max: sorted[len(sorted)-1],
		Avg: total / time.Duration(len(sorted)),
		P50: percentileFromSorted(sorted, 50),
		P95: percentileFromSorted(sorted, 95),
		P99: percentileFromSorted(sorted, 99),
	}
}

func percentileFromSorted(sorted []time.Duration, percent float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	index := int((percent / 100.0) * float64(len(sorted)))
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}

func ComputeThroughput(totalBytes int64, duration time.Duration) (float64, float64) {
	if duration <= 0 {
		return 0, 0
	}

	mbTransferred := float64(totalBytes) / (1024 * 1024)
	mbPerSecond := mbTransferred / duration.Seconds()
	return mbTransferred, mbPerSecond
}

func statusMatches(code int, ranges []statusRange) bool {
	for _, r := range ranges {
		if code >= r.Start && code <= r.End {
			return true
		}
	}
	return false
}

func computeRates(successes, failures, total int) (float64, float64) {
	if total == 0 {
		return 0, 0
	}

	return (float64(successes) / float64(total)) * 100, (float64(failures) / float64(total)) * 100
}

func classifyError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return err.Error()
}

func formatHeaders(headers http.Header) []string {
	if len(headers) == 0 {
		return nil
	}

	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	formatted := make([]string, 0, len(headers))
	for _, key := range keys {
		formatted = append(formatted, fmt.Sprintf("%s: %s", key, strings.Join(headers.Values(key), "; ")))
	}

	return formatted
}

func formatStatusCodes(statusCodes map[int]int) []string {
	if len(statusCodes) == 0 {
		return nil
	}

	keys := make([]int, 0, len(statusCodes))
	for code := range statusCodes {
		keys = append(keys, code)
	}
	sort.Ints(keys)

	formatted := make([]string, 0, len(keys))
	for _, code := range keys {
		formatted = append(formatted, fmt.Sprintf("%d=%d", code, statusCodes[code]))
	}

	return formatted
}

func formatStringCounts(counts map[string]int) []string {
	if len(counts) == 0 {
		return nil
	}

	entries := sortedStringMapEntries(counts)
	formatted := make([]string, 0, len(entries))
	for _, entry := range entries {
		formatted = append(formatted, fmt.Sprintf("%s=%d", entry.Key, entry.Value))
	}
	return formatted
}

func formatStringMapEntries(prefix string, values map[string]int) [][2]string {
	entries := sortedStringMapEntries(values)
	formatted := make([][2]string, 0, len(entries))
	for _, entry := range entries {
		formatted = append(formatted, [2]string{fmt.Sprintf("%s.%s", prefix, entry.Key), fmt.Sprintf("%d", entry.Value)})
	}
	return formatted
}

func sortedStringMapEntries(values map[string]int) []countEntry {
	if len(values) == 0 {
		return nil
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]countEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, countEntry{Key: key, Value: values[key]})
	}
	return entries
}

func stringifyIntMap(input map[int]int) map[string]int {
	if len(input) == 0 {
		return nil
	}

	output := make(map[string]int, len(input))
	for key, value := range input {
		output[fmt.Sprintf("%d", key)] = value
	}
	return output
}

func cloneStringMap(input map[string]int) map[string]int {
	if len(input) == 0 {
		return nil
	}

	output := make(map[string]int, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func safeDisplayValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "(none)"
	}
	return value
}

func defaultText(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	return d.String()
}

func formatRateLimit(rps float64) string {
	if rps <= 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%.2f", rps)
}
