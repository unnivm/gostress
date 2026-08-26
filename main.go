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
	RampDuration      time.Duration
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
	RampDuration      string         `json:"ramp_duration"`
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

type concurrencyRamp struct {
	mu         sync.RWMutex
	limit      int
	target     int
	done       chan struct{}
	stop       chan struct{}
	currentCap int
}

func newConcurrencyRamp(startConcurrency, targetConcurrency int, rampDuration time.Duration, testDeadline time.Time) *concurrencyRamp {
	r := &concurrencyRamp{
		limit:      startConcurrency,
		target:     targetConcurrency,
		done:       make(chan struct{}),
		stop:       make(chan struct{}),
		currentCap: startConcurrency,
	}

	if rampDuration <= 0 || targetConcurrency <= startConcurrency {
		r.limit = targetConcurrency
		r.currentCap = targetConcurrency
		close(r.done)
		return r
	}

	go r.run(startConcurrency, targetConcurrency, rampDuration, testDeadline)
	return r
}

func (r *concurrencyRamp) run(start, target int, rampDuration time.Duration, deadline time.Time) {
	defer close(r.done)

	startTime := deadline.Add(-rampDuration)
	if time.Now().After(startTime) {
		startTime = time.Now()
	}

	steps := target - start
	stepDuration := rampDuration / time.Duration(steps)

	for i := 1; i <= steps; i++ {
		select {
		case <-r.stop:
			return
		default:
		}

		newLimit := start + i
		stepTime := startTime.Add(stepDuration * time.Duration(i))

		waitDur := time.Until(stepTime)
		if waitDur > 0 {
			select {
			case <-r.stop:
				return
			case <-time.After(waitDur):
			}
		}

		r.mu.Lock()
		r.limit = newLimit
		r.currentCap = newLimit
		r.mu.Unlock()
	}
}

func (r *concurrencyRamp) Wait(ctx context.Context) error {
	for {
		r.mu.RLock()
		limit := r.limit
		r.mu.RUnlock()

		if limit > 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stop:
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (r *concurrencyRamp) CurrentLimit() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.limit
}

func (r *concurrencyRamp) StopAfter() {
	select {
	case <-r.stop:
		return
	default:
		close(r.stop)
	}
	<-r.done
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
	ramp := flag.Int("ramp", 0, "Ramp-up duration in seconds from 1 to --c concurrent workers (0 disables ramp)")
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
		RampDuration:      time.Duration(*ramp) * time.Second,
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

	if cfg.RampDuration < 0 {
		return Config{}, errors.New("--ramp must be 0 or greater")
	}

	if cfg.RampDuration > 0 && cfg.RampDuration >= cfg.Duration {
		return Config{}, errors.New("--ramp must be shorter than --d")
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

	var ramp *concurrencyRamp
	if cfg.RampDuration > 0 {
		ramp = newConcurrencyRamp(1, cfg.Concurrency, cfg.RampDuration, deadline)
	} else {
		ramp = newConcurrencyRamp(cfg.Concurrency, cfg.Concurrency, 0, deadline)
	}

	stopProgress := make(chan struct{})
	go showProgress(startTime, cfg.Duration, stopProgress, func() int {
		return ramp.CurrentLimit()
	})

	go func() {
		spawned := 0
		for {
			if time.Now().After(deadline) {
				break
			}
			curLimit := ramp.CurrentLimit()
			toSpawn := curLimit - spawned
			for i := 0; i < toSpawn; i++ {
				if time.Now().After(deadline) {
					break
				}
				wg.Add(1)
				go worker(client, cfg, deadline, limiter, results, &wg)
				spawned++
			}
			time.Sleep(100 * time.Millisecond)
		}
		ramp.StopAfter()
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

func showProgress(start time.Time, duration time.Duration, done <-chan struct{}, currentConcurrency func() int) {
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
			concurrency := currentConcurrency()
			fmt.Printf("\rTest Progress: %.1f%% | Concurrency: %d   ", percent, concurrency)
		case <-done:
			fmt.Printf("\rTest Completed: 100%%                        \n\n")
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
	if cfg.RampDuration > 0 {
		fmt.Printf("Ramp Duration:         %s (1 → %d workers)\n", cfg.RampDuration, cfg.Concurrency)
	}
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
		RampDuration:      formatRampDuration(cfg.RampDuration, cfg.Concurrency),
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
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>gostress — Load Test Report</title>
  <style>
    :root {
      --bg: #0c1117;
      --surface: #161b22;
      --surface2: #1c2333;
      --border: #30363d;
      --text: #e6edf3;
      --muted: #8b949e;
      --green: #3fb950;
      --green-dim: rgba(63,185,80,0.15);
      --amber: #d29922;
      --amber-dim: rgba(210,153,34,0.15);
      --red: #f85149;
      --red-dim: rgba(248,81,73,0.15);
      --blue: #58a6ff;
      --blue-dim: rgba(88,166,255,0.15);
      --purple: #bc8cff;
      --purple-dim: rgba(188,140,255,0.15);
      --cyan: #39d353;
      --radius: 16px;
      --radius-sm: 10px;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      padding: 32px;
      line-height: 1.6;
    }
    .container { max-width: 1200px; margin: 0 auto; }

    .hero {
      position: relative;
      background: linear-gradient(135deg, #161b22 0%, #0d1117 50%, #161b22 100%);
      border: 1px solid var(--border);
      border-radius: 24px;
      padding: 48px 40px;
      margin-bottom: 24px;
      overflow: hidden;
    }
    .hero::before {
      content: "";
      position: absolute;
      top: -50%;
      right: -20%;
      width: 500px;
      height: 500px;
      background: radial-gradient(circle, rgba(63,185,80,0.08) 0%, transparent 70%);
      pointer-events: none;
    }
    .hero::after {
      content: "";
      position: absolute;
      bottom: -40%;
      left: -10%;
      width: 400px;
      height: 400px;
      background: radial-gradient(circle, rgba(88,166,255,0.06) 0%, transparent 70%);
      pointer-events: none;
    }
    .hero .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: var(--green-dim);
      color: var(--green);
      padding: 6px 14px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 16px;
    }
    .hero .badge::before {
      content: "";
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--green);
      animation: pulse 2s infinite;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.4; }
    }
    .hero h1 {
      font-size: 42px;
      font-weight: 800;
      letter-spacing: -0.02em;
      margin-bottom: 12px;
      background: linear-gradient(135deg, #fff 0%, #8b949e 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }
    .hero .subtitle { color: var(--muted); font-size: 16px; margin-bottom: 20px; }
    .meta { display: flex; flex-wrap: wrap; gap: 10px; }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 7px 14px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 999px;
      font-size: 13px;
      color: var(--muted);
    }
    .pill b { color: var(--text); font-weight: 600; }

    .kpi-row {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
      margin-bottom: 24px;
    }
    .kpi-card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 24px;
      position: relative;
      overflow: hidden;
      transition: border-color 0.2s;
    }
    .kpi-card:hover { border-color: var(--blue); }
    .kpi-card .label { color: var(--muted); font-size: 13px; font-weight: 500; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.06em; }
    .kpi-card .value { font-size: 36px; font-weight: 700; letter-spacing: -0.02em; line-height: 1.1; }
    .kpi-card .sub { color: var(--muted); font-size: 13px; margin-top: 6px; }
    .kpi-card .accent { position: absolute; top: 0; right: 0; width: 80px; height: 80px; border-radius: 0 0 0 80px; opacity: 0.08; }

    .section-title {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 16px;
      margin-top: 8px;
    }
    .section-title h2 { font-size: 20px; font-weight: 700; }
    .section-title .tag {
      font-size: 11px;
      padding: 4px 10px;
      border-radius: 999px;
      background: var(--blue-dim);
      color: var(--blue);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }

    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }
    .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin-bottom: 24px; }

    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 28px;
    }

    .donut-section { display: flex; align-items: center; gap: 32px; }
    .donut-wrap { flex-shrink: 0; }
    .donut-legend { flex: 1; }
    .legend-item {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 10px 0;
      border-bottom: 1px solid var(--border);
    }
    .legend-item:last-child { border-bottom: none; }
    .legend-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
    .legend-label { color: var(--muted); font-size: 14px; flex: 1; }
    .legend-value { font-weight: 700; font-size: 15px; }

    .bar-chart { display: flex; flex-direction: column; gap: 14px; }
    .bar-item { display: grid; grid-template-columns: 120px 1fr 80px; gap: 12px; align-items: center; }
    .bar-item .label { font-size: 14px; font-weight: 600; }
    .bar-track {
      height: 24px;
      background: var(--surface2);
      border-radius: 8px;
      overflow: hidden;
      position: relative;
    }
    .bar-fill {
      height: 100%;
      border-radius: 8px;
      min-width: 4px;
      transition: width 0.6s cubic-bezier(0.22, 1, 0.36, 1);
    }
    .bar-item .count { font-size: 14px; font-weight: 600; text-align: right; color: var(--muted); }

    .gradient-green { background: linear-gradient(90deg, #238636, #3fb950); }
    .gradient-amber { background: linear-gradient(90deg, #9e6a03, #d29922); }
    .gradient-red { background: linear-gradient(90deg, #da3633, #f85149); }
    .gradient-blue { background: linear-gradient(90deg, #1f6feb, #58a6ff); }
    .gradient-purple { background: linear-gradient(90deg, #8957e5, #bc8cff); }
    .gradient-cyan { background: linear-gradient(90deg, #1a7f37, #3fb950); }

    .prose { color: var(--text); font-size: 16px; line-height: 1.9; }
    .prose p { margin-bottom: 14px; }
    .prose p:last-child { margin-bottom: 0; }

    .table-card { overflow: hidden; }
    .table-card table { width: 100%; border-collapse: collapse; }
    .table-card th, .table-card td { padding: 12px 16px; text-align: left; font-size: 14px; }
    .table-card th { color: var(--muted); font-weight: 600; text-transform: uppercase; font-size: 11px; letter-spacing: 0.08em; background: var(--surface2); border-bottom: 1px solid var(--border); }
    .table-card td { border-bottom: 1px solid var(--border); }
    .table-card tr:last-child td { border-bottom: none; }
    .table-card td:first-child { font-weight: 600; font-family: "SF Mono", Menlo, Monaco, monospace; font-size: 13px; }

    .note-card {
      background: var(--surface2);
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      padding: 16px 20px;
      margin-bottom: 12px;
    }
    .note-card:last-child { margin-bottom: 0; }
    .note-card h3 { font-size: 13px; font-weight: 600; color: var(--blue); margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.06em; }
    .note-card p { font-size: 14px; color: var(--muted); line-height: 1.7; }

    .config-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 24px; }
    .config-row { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--border); font-size: 14px; }
    .config-row:last-child { border-bottom: none; }
    .config-row .key { color: var(--muted); }
    .config-row .val { font-weight: 600; font-family: "SF Mono", Menlo, Monaco, monospace; font-size: 13px; }

    .footer {
      margin-top: 40px;
      padding-top: 20px;
      border-top: 1px solid var(--border);
      text-align: center;
      color: var(--muted);
      font-size: 13px;
    }

    @media (max-width: 900px) {
      body { padding: 16px; }
      .hero { padding: 28px 20px; }
      .hero h1 { font-size: 28px; }
      .kpi-row { grid-template-columns: 1fr 1fr; }
      .grid-2, .grid-3 { grid-template-columns: 1fr; }
      .donut-section { flex-direction: column; }
      .config-grid { grid-template-columns: 1fr; }
      .bar-item { grid-template-columns: 1fr; gap: 4px; }
    }
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <div class="badge">Load Test Complete</div>
      <h1>Performance Report</h1>
      <p class="subtitle">{{.SummaryHeadline}}</p>
      <div class="meta">
        <span class="pill"><b>{{.Report.Method}}</b> {{.Report.URL}}</span>
        <span class="pill"><b>{{.Report.Concurrency}}</b> workers</span>
        {{if .Report.RampDuration}}<span class="pill">Ramp <b>{{.Report.RampDuration}}</b></span>{{end}}
        <span class="pill"><b>{{.Report.Duration}}</b> target</span>
        <span class="pill">Actual <b>{{.Report.ActualDuration}}</b></span>
      </div>
    </section>

    <div class="kpi-row">
      <div class="kpi-card">
        <div class="accent" style="background:var(--blue)"></div>
        <div class="label">Total Requests</div>
        <div class="value" style="color:var(--blue)">{{.Report.TotalRequests}}</div>
        <div class="sub">Across {{.Report.ActualDuration}}</div>
      </div>
      <div class="kpi-card">
        <div class="accent" style="background:var(--green)"></div>
        <div class="label">Success Rate</div>
        <div class="value" style="color:var(--green)">{{printf "%.1f%%" .Report.SuccessRate}}</div>
        <div class="sub">{{.Report.SuccessRequests}} successful</div>
      </div>
      <div class="kpi-card">
        <div class="accent" style="background:var(--purple)"></div>
        <div class="label">Throughput</div>
        <div class="value" style="color:var(--purple)">{{printf "%.1f" .Report.RequestsPerSecond}}</div>
        <div class="sub">requests / second</div>
      </div>
      <div class="kpi-card">
        <div class="accent" style="background:var(--amber)"></div>
        <div class="label">Avg Latency</div>
        <div class="value" style="color:var(--amber)">{{.Report.AverageLatency}}</div>
        <div class="sub">p95 {{.Report.P95Latency}} · p99 {{.Report.P99Latency}}</div>
      </div>
    </div>

    <div class="grid-2">
      <div class="card">
        <div class="section-title">
          <h2>Reliability</h2>
          <span class="tag">Health</span>
        </div>
        <div class="donut-section">
          <div class="donut-wrap">
            <svg width="180" height="180" viewBox="0 0 180 180">
              <circle cx="90" cy="90" r="70" fill="none" stroke="var(--surface2)" stroke-width="16"/>
              <circle cx="90" cy="90" r="70" fill="none" stroke="var(--green)" stroke-width="16"
                stroke-dasharray="{{printf "%.1f" .SuccessPct}} {{printf "%.1f" (sub 100.0 .SuccessPct)}}"
                stroke-dashoffset="25" stroke-linecap="round" transform="rotate(-90 90 90)"/>
              {{if gt .HTTPFailurePct 0.0}}
              <circle cx="90" cy="90" r="70" fill="none" stroke="var(--amber)" stroke-width="16"
                stroke-dasharray="{{printf "%.1f" .HTTPFailurePct}} {{printf "%.1f" (sub 100.0 .HTTPFailurePct)}}"
                stroke-dashoffset="{{printf "%.1f" (sub 25.0 .SuccessPct)}}" stroke-linecap="round" transform="rotate(-90 90 90)"/>
              {{end}}
              {{if gt .TransportErrorPct 0.0}}
              <circle cx="90" cy="90" r="70" fill="none" stroke="var(--red)" stroke-width="16"
                stroke-dasharray="{{printf "%.1f" .TransportErrorPct}} {{printf "%.1f" (sub 100.0 .TransportErrorPct)}}"
                stroke-dashoffset="{{printf "%.1f" (sub 25.0 (add .SuccessPct .HTTPFailurePct))}}" stroke-linecap="round" transform="rotate(-90 90 90)"/>
              {{end}}
              <text x="90" y="86" text-anchor="middle" fill="var(--text)" font-size="28" font-weight="800">{{printf "%.1f%%" .Report.SuccessRate}}</text>
              <text x="90" y="106" text-anchor="middle" fill="var(--muted)" font-size="12">success</text>
            </svg>
          </div>
          <div class="donut-legend">
            <div class="legend-item">
              <div class="legend-dot" style="background:var(--green)"></div>
              <div class="legend-label">Success</div>
              <div class="legend-value" style="color:var(--green)">{{printf "%.1f%%" .SuccessPct}}</div>
            </div>
            <div class="legend-item">
              <div class="legend-dot" style="background:var(--amber)"></div>
              <div class="legend-label">HTTP Failures</div>
              <div class="legend-value" style="color:var(--amber)">{{printf "%.1f%%" .HTTPFailurePct}}</div>
            </div>
            <div class="legend-item">
              <div class="legend-dot" style="background:var(--red)"></div>
              <div class="legend-label">Transport Errors</div>
              <div class="legend-value" style="color:var(--red)">{{printf "%.1f%%" .TransportErrorPct}}</div>
            </div>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="section-title">
          <h2>Configuration</h2>
          <span class="tag">Setup</span>
        </div>
        <div class="config-grid">
          <div class="config-row"><span class="key">Success statuses</span><span class="val">{{.Report.SuccessStatusSpec}}</span></div>
          <div class="config-row"><span class="key">RPS limit</span><span class="val">{{formatRate .Report.RateLimitRPS}}</span></div>
          <div class="config-row"><span class="key">Concurrency</span><span class="val">{{.Report.Concurrency}}</span></div>
          <div class="config-row"><span class="key">Ramp</span><span class="val">{{defaultText .Report.RampDuration "(none)"}}</span></div>
          <div class="config-row"><span class="key">Duration</span><span class="val">{{.Report.Duration}}</span></div>
          <div class="config-row"><span class="key">Method</span><span class="val">{{.Report.Method}}</span></div>
          <div class="config-row"><span class="key">Headers</span><span class="val">{{defaultText (join .Report.Headers ", ") "(none)"}}</span></div>
          <div class="config-row"><span class="key">Payload</span><span class="val">{{defaultText .Report.Payload "(none)"}}</span></div>
        </div>
      </div>
    </div>

    <div class="grid-2">
      <div class="card">
        <div class="section-title">
          <h2>Latency Distribution</h2>
          <span class="tag">Timing</span>
        </div>
        <div class="bar-chart">
          {{range .LatencyBars}}
          <div class="bar-item">
            <div class="label">{{.Label}}</div>
            <div class="bar-track"><div class="bar-fill {{.Tone}}" style="width: {{printf "%.1f" .Width}}%"></div></div>
            <div class="count">{{.Value}}</div>
          </div>
          {{end}}
        </div>
      </div>

      <div class="card">
        <div class="section-title">
          <h2>Status Codes</h2>
          <span class="tag">HTTP</span>
        </div>
        <div class="bar-chart">
          {{range .StatusCodeBars}}
          <div class="bar-item">
            <div class="label">{{.Label}}</div>
            <div class="bar-track"><div class="bar-fill {{.Tone}}" style="width: {{printf "%.1f" .Width}}%"></div></div>
            <div class="count">{{.Value}}</div>
          </div>
          {{else}}
          <p style="color:var(--muted);font-size:14px;">No HTTP responses recorded.</p>
          {{end}}
        </div>
      </div>
    </div>

    <div class="grid-2">
      <div class="card">
        <div class="section-title">
          <h2>Transport Errors</h2>
          <span class="tag">Infra</span>
        </div>
        <div class="bar-chart">
          {{range .ErrorBars}}
          <div class="bar-item">
            <div class="label">{{.Label}}</div>
            <div class="bar-track"><div class="bar-fill {{.Tone}}" style="width: {{printf "%.1f" .Width}}%"></div></div>
            <div class="count">{{.Value}}</div>
          </div>
          {{else}}
          <p style="color:var(--muted);font-size:14px;">No transport errors recorded.</p>
          {{end}}
        </div>
      </div>

      <div class="card">
        <div class="section-title">
          <h2>Throughput</h2>
          <span class="tag">Data</span>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px;">
          <div class="note-card">
            <h3>Total Data</h3>
            <div style="font-size:28px;font-weight:700;color:var(--cyan);">{{printf "%.2f" .Report.TotalDataMB}} <span style="font-size:14px;color:var(--muted);">MB</span></div>
          </div>
          <div class="note-card">
            <h3>Avg Throughput</h3>
            <div style="font-size:28px;font-weight:700;color:var(--cyan);">{{printf "%.2f" .Report.AvgThroughputMB}} <span style="font-size:14px;color:var(--muted);">MB/s</span></div>
          </div>
        </div>
        <div class="note-card">
          <h3>Analysis</h3>
          <p>{{.ThroughputNote}}</p>
        </div>
      </div>
    </div>

    <div class="section-title" style="margin-top:8px;">
      <h2>Analysis & Insights</h2>
      <span class="tag">Narrative</span>
    </div>
    <div class="card" style="margin-bottom:24px;">
      <div class="prose">
        {{range .SummaryParagraphs}}
        <p>{{.}}</p>
        {{end}}
      </div>
    </div>

    <div class="section-title" style="margin-top:8px;">
      <h2>Transport Errors Explained</h2>
      <span class="tag">Reference</span>
    </div>
    <div class="card" style="margin-bottom:24px;">
      <div class="prose">
        <p><b>Transport errors</b> are network-level failures that occur <em>before</em> your application returns an HTTP response. Unlike HTTP failures (e.g. 500, 503), transport errors mean the request never reached the server or the response was never fully received. Common causes include:</p>
        <p><b>deadline_exceeded</b> — The request took longer than the allowed time. This usually means the server is overloaded, the database is slow, or there is a network bottleneck. Under high concurrency, this is often the first signal that your target is reaching capacity.</p>
        <p><b>connection refused</b> — The server actively rejected the TCP connection. This typically indicates the target service is down, not listening on the expected port, or has exhausted its connection pool.</p>
        <p><b>connection reset</b> — An established connection was abruptly terminated by the remote end. This can happen under heavy load when a server or load balancer drops connections to shed pressure.</p>
        <p><b>EOF / broken pipe</b> — The server closed the connection before sending a complete response. This often points to a crashed handler, a proxy timeout, or a server restart mid-request.</p>
        {{if gt .TransportErrorPct 0.0}}
        <p><b>In this test:</b> transport errors accounted for <b>{{printf "%.1f%%" .TransportErrorPct}}</b> of all requests. {{if gt .TransportErrorPct 20.0}}This is a significant portion and strongly suggests the target is under stress or experiencing infrastructure-level instability. Consider reducing concurrency or investigating upstream timeouts.{{else}}This is within a moderate range and may reflect occasional timeouts under load.{{end}}</p>
        {{end}}
      </div>
    </div>

    <div class="grid-2">
      <div class="card table-card">
        <div class="section-title">
          <h2>Status Code Table</h2>
        </div>
        <table>
          <thead><tr><th>Code</th><th>Count</th></tr></thead>
          <tbody>
            {{range .StatusCodes}}
            <tr><td>{{.Key}}</td><td>{{.Value}}</td></tr>
            {{else}}
            <tr><td colspan="2" style="color:var(--muted);">No HTTP responses recorded</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
      <div class="card table-card">
        <div class="section-title">
          <h2>Transport Error Table</h2>
        </div>
        <table>
          <thead><tr><th>Error</th><th>Count</th></tr></thead>
          <tbody>
            {{range .ErrorTypes}}
            <tr><td>{{.Key}}</td><td>{{.Value}}</td></tr>
            {{else}}
            <tr><td colspan="2" style="color:var(--muted);">No transport errors recorded</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </div>

    <div class="footer">
      Generated by <b>gostress</b> at {{.Report.StartTime}}
    </div>
  </div>
</body>
</html>`

	tmpl, err := template.New("report").Funcs(template.FuncMap{
		"join":        strings.Join,
		"defaultText": defaultText,
		"formatRate":  formatRateLimit,
		"sub":         func(a, b float64) float64 { return a - b },
		"add":         func(a, b float64) float64 { return a + b },
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
			durationBar("p50", report.P50Latency, report.MaxLatency, "gradient-blue"),
			durationBar("Average", report.AverageLatency, report.MaxLatency, "gradient-green"),
			durationBar("p95", report.P95Latency, report.MaxLatency, "gradient-amber"),
			durationBar("p99", report.P99Latency, report.MaxLatency, "gradient-red"),
			durationBar("Max", report.MaxLatency, report.MaxLatency, "gradient-red"),
		},
		StatusCodeBars:    countBars(statusEntries, "gradient-green"),
		ErrorBars:         countBars(errorEntries, "gradient-red"),
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

func formatRampDuration(d time.Duration, concurrency int) string {
	if d <= 0 {
		return ""
	}
	return fmt.Sprintf("%s (1 → %d workers)", d, concurrency)
}
