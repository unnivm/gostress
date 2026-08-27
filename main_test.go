package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateAndNormalizeConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "missing url",
			cfg:  Config{Concurrency: 1, Duration: time.Second, Method: http.MethodGet, SuccessStatusSpec: "200-399", OutputFormats: []string{"json"}, OutputPrefix: "report"},
		},
		{
			name: "bad url",
			cfg:  Config{URL: "localhost", Concurrency: 1, Duration: time.Second, Method: http.MethodGet, SuccessStatusSpec: "200-399", OutputFormats: []string{"json"}, OutputPrefix: "report"},
		},
		{
			name: "zero concurrency",
			cfg:  Config{URL: "http://example.com", Concurrency: 0, Duration: time.Second, Method: http.MethodGet, SuccessStatusSpec: "200-399", OutputFormats: []string{"json"}, OutputPrefix: "report"},
		},
		{
			name: "zero duration",
			cfg:  Config{URL: "http://example.com", Concurrency: 1, Duration: 0, Method: http.MethodGet, SuccessStatusSpec: "200-399", OutputFormats: []string{"json"}, OutputPrefix: "report"},
		},
		{
			name: "bad method",
			cfg:  Config{URL: "http://example.com", Concurrency: 1, Duration: time.Second, Method: "PUT", SuccessStatusSpec: "200-399", OutputFormats: []string{"json"}, OutputPrefix: "report"},
		},
		{
			name: "payload with get",
			cfg:  Config{URL: "http://example.com", Concurrency: 1, Duration: time.Second, Method: http.MethodGet, Payload: "{}", SuccessStatusSpec: "200-399", OutputFormats: []string{"json"}, OutputPrefix: "report"},
		},
		{
			name: "bad headers",
			cfg:  Config{URL: "http://example.com", Concurrency: 1, Duration: time.Second, Method: http.MethodGet, Headers: "Authorization", SuccessStatusSpec: "200-399", OutputFormats: []string{"json"}, OutputPrefix: "report"},
		},
		{
			name: "bad success status",
			cfg:  Config{URL: "http://example.com", Concurrency: 1, Duration: time.Second, Method: http.MethodGet, SuccessStatusSpec: "399-200", OutputFormats: []string{"json"}, OutputPrefix: "report"},
		},
		{
			name: "negative rps",
			cfg:  Config{URL: "http://example.com", Concurrency: 1, Duration: time.Second, Method: http.MethodGet, SuccessStatusSpec: "200-399", RequestsPerSecond: -1, OutputFormats: []string{"json"}, OutputPrefix: "report"},
		},
		{
			name: "empty output prefix",
			cfg:  Config{URL: "http://example.com", Concurrency: 1, Duration: time.Second, Method: http.MethodGet, SuccessStatusSpec: "200-399", OutputFormats: []string{"json"}, OutputPrefix: ""},
		},
		{
			name: "negative ramp",
			cfg:  Config{URL: "http://example.com", Concurrency: 1, Duration: time.Second, Method: http.MethodGet, SuccessStatusSpec: "200-399", OutputFormats: []string{"json"}, OutputPrefix: "report", RampDuration: -1 * time.Second},
		},
		{
			name: "ramp exceeds duration",
			cfg:  Config{URL: "http://example.com", Concurrency: 10, Duration: 5 * time.Second, Method: http.MethodGet, SuccessStatusSpec: "200-399", OutputFormats: []string{"json"}, OutputPrefix: "report", RampDuration: 10 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := validateAndNormalizeConfig(tt.cfg); err == nil {
				t.Fatalf("validateAndNormalizeConfig(%+v) returned nil error", tt.cfg)
			}
		})
	}
}

func TestParseHeaders(t *testing.T) {
	headers, err := parseHeaders("Authorization: Bearer token, X-Trace: abc123")
	if err != nil {
		t.Fatalf("parseHeaders returned error: %v", err)
	}

	if got := headers.Get("Authorization"); got != "Bearer token" {
		t.Fatalf("expected Authorization header, got %q", got)
	}

	if got := headers.Get("X-Trace"); got != "abc123" {
		t.Fatalf("expected X-Trace header, got %q", got)
	}
}

func TestParseHeadersRejectsMalformedInput(t *testing.T) {
	if _, err := parseHeaders("Authorization"); err == nil {
		t.Fatal("expected malformed header to fail")
	}
}

func TestParseStatusRanges(t *testing.T) {
	ranges, err := parseStatusRanges("200-299,304,429")
	if err != nil {
		t.Fatalf("parseStatusRanges returned error: %v", err)
	}

	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}

	if !statusMatches(204, ranges) || !statusMatches(304, ranges) || !statusMatches(429, ranges) {
		t.Fatalf("expected ranges to match configured codes: %+v", ranges)
	}

	if statusMatches(500, ranges) {
		t.Fatalf("did not expect 500 to match: %+v", ranges)
	}
}

func TestParseStatusRangesRejectsInvalidInput(t *testing.T) {
	for _, spec := range []string{"", "200-", "399-200", "abc"} {
		if _, err := parseStatusRanges(spec); err == nil {
			t.Fatalf("expected %q to be rejected", spec)
		}
	}
}

func TestParseOutputFormats(t *testing.T) {
	formats, err := parseOutputFormats("json, html, csv, json")
	if err != nil {
		t.Fatalf("parseOutputFormats returned error: %v", err)
	}

	expected := []string{"json", "html", "csv"}
	if len(formats) != len(expected) {
		t.Fatalf("expected %d formats, got %d", len(expected), len(formats))
	}

	for i := range expected {
		if formats[i] != expected[i] {
			t.Fatalf("expected format %q at index %d, got %q", expected[i], i, formats[i])
		}
	}
}

func TestParseOutputFormatsRejectsUnknownFormats(t *testing.T) {
	if _, err := parseOutputFormats("json,xml"); err == nil {
		t.Fatal("expected unknown format to fail")
	}
}

func TestNewRequestBuildsPOSTRequestWithDefaultContentType(t *testing.T) {
	cfg := Config{
		URL:               "http://example.com",
		Concurrency:       1,
		Duration:          time.Second,
		Method:            http.MethodPost,
		Payload:           `{"hello":"world"}`,
		SuccessStatusSpec: "200-399",
		OutputFormats:     []string{"json"},
		OutputPrefix:      "report",
	}

	headers := make(http.Header)
	deadline := time.Now().Add(time.Second)

	req, cancel, err := newRequest(cfg, headers, deadline)
	if err != nil {
		t.Fatalf("newRequest returned error: %v", err)
	}
	defer cancel()

	if req.Method != http.MethodPost {
		t.Fatalf("expected POST method, got %s", req.Method)
	}

	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected default content type, got %q", got)
	}
}

func TestComputeLatencyStats(t *testing.T) {
	stats := ComputeLatencyStats([]time.Duration{
		200 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
	})

	if stats.Min != 50*time.Millisecond {
		t.Fatalf("expected min 50ms, got %s", stats.Min)
	}

	if stats.Max != 200*time.Millisecond {
		t.Fatalf("expected max 200ms, got %s", stats.Max)
	}

	if stats.Avg != 116666666*time.Nanosecond {
		t.Fatalf("unexpected average: %s", stats.Avg)
	}
}

func TestComputePercentileDoesNotMutateInput(t *testing.T) {
	latencies := []time.Duration{
		300 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
	}

	_ = ComputePercentile(latencies, 95)

	if latencies[0] != 300*time.Millisecond || latencies[1] != 100*time.Millisecond || latencies[2] != 200*time.Millisecond {
		t.Fatalf("ComputePercentile mutated input: %v", latencies)
	}
}

func TestComputeThroughputHandlesZeroDuration(t *testing.T) {
	totalMB, throughput := ComputeThroughput(1024, 0)
	if totalMB != 0 || throughput != 0 {
		t.Fatalf("expected zero throughput for zero duration, got total=%f throughput=%f", totalMB, throughput)
	}
}

func TestComputeRatesHandlesZeroTotals(t *testing.T) {
	successRate, errorRate := computeRates(0, 0, 0)
	if successRate != 0 || errorRate != 0 {
		t.Fatalf("expected zero rates, got success=%f error=%f", successRate, errorRate)
	}
}

func TestWriteReportsCreatesRequestedFormats(t *testing.T) {
	dir := t.TempDir()
	prefix := filepath.Join(dir, "report")
	report := TestReport{
		StartTime:         "2026-04-21 12:00:00",
		URL:               "http://example.com",
		Concurrency:       10,
		Duration:          "5s",
		Method:            "GET",
		SuccessStatusSpec: "200-399",
		RateLimitRPS:      25,
		OutputFormats:     []string{"json", "csv", "html"},
		StatusCodes:       map[string]int{"200": 10, "500": 1},
		ErrorTypes:        map[string]int{"deadline_exceeded": 2},
		TotalRequests:     11,
		FailedRequests:    1,
		TransportErrors:   0,
		HTTPFailures:      1,
		SuccessRequests:   10,
		SuccessRate:       90.9,
		ErrorRate:         9.1,
		RequestsPerSecond: 20.0,
		MinLatency:        "10ms",
		MaxLatency:        "50ms",
		AverageLatency:    "20ms",
		P50Latency:        "18ms",
		P95Latency:        "45ms",
		P99Latency:        "49ms",
		TotalDataMB:       1.25,
		AvgThroughputMB:   0.25,
		ActualDuration:    "5s",
	}

	files, err := writeReports(report, prefix, []string{"json", "csv", "html"})
	if err != nil {
		t.Fatalf("writeReports returned error: %v", err)
	}

	if len(files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(files))
	}

	for _, path := range files {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("failed reading %s: %v", path, err)
		}
		if len(content) == 0 {
			t.Fatalf("expected %s to have content", path)
		}
	}

	csvContent, err := os.ReadFile(prefix + ".csv")
	if err != nil {
		t.Fatalf("failed reading csv file: %v", err)
	}
	if !strings.Contains(string(csvContent), "status_code.200") {
		t.Fatalf("expected csv output to contain status code rows, got %s", string(csvContent))
	}

	htmlContent, err := os.ReadFile(prefix + ".html")
	if err != nil {
		t.Fatalf("failed reading html file: %v", err)
	}
	if !strings.Contains(string(htmlContent), "gostress") {
		t.Fatalf("expected html output to contain title, got %s", string(htmlContent))
	}
	if !strings.Contains(string(htmlContent), "Analysis") {
		t.Fatalf("expected html output to contain narrative section, got %s", string(htmlContent))
	}
	if !strings.Contains(string(htmlContent), "Latency Distribution") {
		t.Fatalf("expected html output to contain chart section, got %s", string(htmlContent))
	}
}

func TestExportExistingReportRendersHTML(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.json")
	outputPrefix := filepath.Join(dir, "pretty-report")

	report := TestReport{
		StartTime:         "2026-04-21 12:00:00",
		URL:               "http://example.com",
		Concurrency:       4,
		Duration:          "3s",
		Method:            "GET",
		SuccessStatusSpec: "200-399",
		OutputFormats:     []string{"json"},
		StatusCodes:       map[string]int{"200": 12},
		ErrorTypes:        map[string]int{"deadline_exceeded": 1},
		TotalRequests:     13,
		FailedRequests:    1,
		TransportErrors:   1,
		HTTPFailures:      0,
		SuccessRequests:   12,
		SuccessRate:       92.3,
		ErrorRate:         7.7,
		RequestsPerSecond: 4.33,
		MinLatency:        "1ms",
		MaxLatency:        "10ms",
		AverageLatency:    "3ms",
		P50Latency:        "2ms",
		P95Latency:        "8ms",
		P99Latency:        "9ms",
		TotalDataMB:       0.8,
		AvgThroughputMB:   0.26,
		ActualDuration:    "3.01s",
	}

	data, err := renderJSONReport(report)
	if err != nil {
		t.Fatalf("renderJSONReport returned error: %v", err)
	}
	if err := os.WriteFile(source, data, 0o644); err != nil {
		t.Fatalf("failed writing source report: %v", err)
	}

	cfg := Config{
		FromReport:    source,
		OutputFormats: []string{"html"},
		OutputPrefix:  outputPrefix,
	}
	if err := exportExistingReport(cfg); err != nil {
		t.Fatalf("exportExistingReport returned error: %v", err)
	}

	htmlContent, err := os.ReadFile(outputPrefix + ".html")
	if err != nil {
		t.Fatalf("failed reading rendered html file: %v", err)
	}
	if !strings.Contains(string(htmlContent), "Reliability") {
		t.Fatalf("expected rendered html to contain richer sections, got %s", string(htmlContent))
	}
}

func TestConcurrencyRampReachesTarget(t *testing.T) {
	deadline := time.Now().Add(1 * time.Second)
	ramp := newConcurrencyRamp(1, 10, 800*time.Millisecond, deadline)
	defer ramp.StopAfter()

	time.Sleep(1100 * time.Millisecond)

	if got := ramp.CurrentLimit(); got != 10 {
		t.Fatalf("expected concurrency limit to reach 10, got %d", got)
	}
}

func TestConcurrencyRampNoRamp(t *testing.T) {
	deadline := time.Now().Add(2 * time.Second)
	ramp := newConcurrencyRamp(10, 10, 0, deadline)

	if got := ramp.CurrentLimit(); got != 10 {
		t.Fatalf("expected immediate concurrency 10, got %d", got)
	}
}

func TestConcurrencyRampWaitBlocks(t *testing.T) {
	deadline := time.Now().Add(2 * time.Second)
	ramp := newConcurrencyRamp(0, 5, 100*time.Millisecond, deadline)
	defer ramp.StopAfter()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := ramp.Wait(ctx)
	if err == nil {
		t.Fatal("expected Wait to block when limit is 0")
	}
}

func TestConcurrencyRampWaitPassesWhenLimitPositive(t *testing.T) {
	deadline := time.Now().Add(2 * time.Second)
	ramp := newConcurrencyRamp(1, 10, 100*time.Millisecond, deadline)
	defer ramp.StopAfter()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := ramp.Wait(ctx)
	if err != nil {
		t.Fatalf("expected Wait to succeed, got %v", err)
	}
}

func TestFormatRampDuration(t *testing.T) {
	got := formatRampDuration(60*time.Second, 500)
	expected := "1m0s (1 → 500 workers)"
	if got != expected {
		t.Fatalf("expected %q, got %q", expected, got)
	}

	got = formatRampDuration(0, 10)
	if got != "" {
		t.Fatalf("expected empty string for zero duration, got %q", got)
	}
}

func TestComputeThroughputPct(t *testing.T) {
	tests := []struct {
		observed float64
		limit    float64
		want     float64
	}{
		{200, 200, 100.0},
		{150, 200, 75.0},
		{0, 200, 0.0},
		{100, 0, 0.0},
		{50, 100, 50.0},
	}
	for _, tt := range tests {
		got := computeThroughputPct(tt.observed, tt.limit)
		if got != tt.want {
			t.Errorf("computeThroughputPct(%v, %v) = %v, want %v", tt.observed, tt.limit, got, tt.want)
		}
	}
}

func TestClassifyError(t *testing.T) {
	if got := classifyError(context.DeadlineExceeded); got != "deadline_exceeded" {
		t.Fatalf("expected deadline_exceeded, got %q", got)
	}

	if got := classifyError(errors.New("connection refused")); got != "connection refused" {
		t.Fatalf("expected failed error message, got %q", got)
	}
}

func TestClassifyErrorWrapsDeadline(t *testing.T) {
	wrapped := wrapErrDeadline()
	if got := classifyError(wrapped); got != "deadline_exceeded" {
		t.Fatalf("expected deadline_exceeded for wrapped error, got %q", got)
	}
}

func wrapErrDeadline() error {
	return errors.Join(context.DeadlineExceeded, errors.New("operation timed out"))
}

func TestFormatHeaders(t *testing.T) {
	if got := formatHeaders(nil); got != nil {
		t.Fatalf("expected nil for empty headers, got %v", got)
	}

	headers := http.Header{}
	headers.Set("Authorization", "Bearer token")
	headers.Set("X-Trace", "abc123")
	headers.Add("X-Multi", "one")

	got := formatHeaders(headers)
	if len(got) != 3 {
		t.Fatalf("expected 3 formatted headers, got %d", len(got))
	}

	if got[0] != "Authorization: Bearer token" {
		t.Fatalf("expected Authorization first, got %q", got[0])
	}
	if !strings.Contains(strings.Join(got, ","), "X-Multi: one") {
		t.Fatalf("expected X-Multi header, got %v", got)
	}

	empty := http.Header{}
	if got := formatHeaders(empty); got != nil {
		t.Fatalf("expected nil for empty http.Header, got %v", got)
	}
}

func TestFormatStatusCodes(t *testing.T) {
	if got := formatStatusCodes(nil); got != nil {
		t.Fatalf("expected nil for empty status codes, got %v", got)
	}

	got := formatStatusCodes(map[int]int{500: 2, 200: 10, 404: 5})
	if len(got) != 3 {
		t.Fatalf("expected 3 codes, got %d", len(got))
	}

	expected := []string{"200=10", "404=5", "500=2"}
	for i, e := range expected {
		if got[i] != e {
			t.Fatalf("expected %q, got %q", e, got[i])
		}
	}
}

func TestFormatStringCounts(t *testing.T) {
	if got := formatStringCounts(nil); got != nil {
		t.Fatalf("expected nil for empty counts, got %v", got)
	}

	got := formatStringCounts(map[string]int{"deadline_exceeded": 3, "connection_reset": 1})
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}

	if got[0] != "connection_reset=1" || got[1] != "deadline_exceeded=3" {
		t.Fatalf("expected sorted entries, got %v", got)
	}
}

func TestStringifyIntMap(t *testing.T) {
	if got := stringifyIntMap(nil); got != nil {
		t.Fatalf("expected nil for empty maps, got %v", got)
	}

	got := stringifyIntMap(map[int]int{200: 5, 500: 1})
	if got["200"] != 5 || got["500"] != 1 {
		t.Fatalf("expected stringified map, got %v", got)
	}

	original := map[int]int{200: 5}
	out := stringifyIntMap(original)
	original[200] = 99
	if out["200"] != 5 {
		t.Fatalf("expected stringifyIntMap to copy values, got %d", out["200"])
	}
}

func TestCloneStringMap(t *testing.T) {
	if got := cloneStringMap(nil); got != nil {
		t.Fatalf("expected nil for empty maps, got %v", got)
	}

	original := map[string]int{"200": 5, "500": 1}
	clone := cloneStringMap(original)
	original["200"] = 99

	if clone["200"] != 5 {
		t.Fatalf("expected clone to be independent, got %d", clone["200"])
	}
}

func TestSafeDisplayValue(t *testing.T) {
	if got := safeDisplayValue(""); got != "(none)" {
		t.Fatalf("expected (none) for empty, got %q", got)
	}
	if got := safeDisplayValue("   "); got != "(none)" {
		t.Fatalf("expected (none) for whitespace, got %q", got)
	}
	if got := safeDisplayValue("value"); got != "value" {
		t.Fatalf("expected value, got %q", got)
	}
}

func TestDefaultText(t *testing.T) {
	if got := defaultText("", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback for empty, got %q", got)
	}
	if got := defaultText("   ", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback for whitespace, got %q", got)
	}
	if got := defaultText("real", "fallback"); got != "real" {
		t.Fatalf("expected real value, got %q", got)
	}
}

func TestFormatDuration(t *testing.T) {
	if got := formatDuration(0); got != "0s" {
		t.Fatalf("expected 0s for zero, got %q", got)
	}
	if got := formatDuration(-time.Second); got != "0s" {
		t.Fatalf("expected 0s for negative, got %q", got)
	}
	if got := formatDuration(1500 * time.Millisecond); got != "1.5s" {
		t.Fatalf("expected 1.5s, got %q", got)
	}
}

func TestComputeThroughputMultipleValues(t *testing.T) {
	totalMB, throughput := ComputeThroughput(1_048_576, 2*time.Second) // 1 MB over 2s
	if totalMB != 1.0 {
		t.Fatalf("expected 1.0 MB total, got %f", totalMB)
	}
	if throughput != 0.5 {
		t.Fatalf("expected 0.5 MB/s, got %f", throughput)
	}
}

func TestBuildReliabilityNoteBranches(t *testing.T) {
	tests := []struct {
		name   string
		report TestReport
		check  string
	}{
		{
			name: "no requests",
			report: TestReport{TotalRequests: 0},
			check:  "No requests were recorded",
		},
		{
			name: "no failures",
			report: TestReport{TotalRequests: 100, FailedRequests: 0},
			check:  "No failures were observed",
		},
		{
			name: "rare failures",
			report: TestReport{TotalRequests: 100, FailedRequests: 1, ErrorRate: 0.01},
			check:  "broadly stable",
		},
		{
			name: "more http failures",
			report: TestReport{
				TotalRequests:   100,
				FailedRequests:  10,
				HTTPFailures:    8,
				TransportErrors: 2,
				ErrorRate:       10.0,
			},
			check: "Most failures came back as HTTP responses",
		},
		{
			name: "more transport errors",
			report: TestReport{
				TotalRequests:   100,
				FailedRequests:  10,
				HTTPFailures:    2,
				TransportErrors: 8,
				ErrorRate:       10.0,
			},
			check: "transport-level issues",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildReliabilityNote(tt.report)
			if !strings.Contains(got, tt.check) {
				t.Fatalf("expected note to contain %q, got %q", tt.check, got)
			}
		})
	}
}

func TestBuildLatencyNoteBranches(t *testing.T) {
	tests := []struct {
		name   string
		report TestReport
		check  string
	}{
		{
			name:   "no data",
			report: TestReport{},
			check:  "Latency data was not available",
		},
		{
			name: "large tail",
			report: TestReport{
				AverageLatency: "10ms",
				P99Latency:     "100ms",
				P95Latency:     "50ms",
				MaxLatency:     "150ms",
			},
			check: "Tail latency stretches well beyond the average",
		},
		{
			name: "moderate tail",
			report: TestReport{
				AverageLatency: "20ms",
				P99Latency:     "60ms",
				P95Latency:     "30ms",
				MaxLatency:     "80ms",
			},
			check: "worth watching if user-facing SLAs matter",
		},
		{
			name: "compact",
			report: TestReport{
				AverageLatency: "10ms",
				P99Latency:     "15ms",
				P95Latency:     "12ms",
				MaxLatency:     "18ms",
			},
			check: "Latency stayed relatively compact",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildLatencyNote(tt.report)
			if !strings.Contains(got, tt.check) {
				t.Fatalf("expected note to contain %q, got %q", tt.check, got)
			}
		})
	}
}

func TestBuildThroughputNoteBranches(t *testing.T) {
	limited := TestReport{RateLimitRPS: 200, RequestsPerSecond: 180}
	if got := buildThroughputNote(limited); !strings.Contains(got, "rate-limited") {
		t.Fatalf("expected rate-limited note, got %q", got)
	}

	unlimited := TestReport{RateLimitRPS: 0, RequestsPerSecond: 500}
	if got := buildThroughputNote(unlimited); !strings.Contains(got, "No explicit RPS cap") {
		t.Fatalf("expected unlimited note, got %q", got)
	}
}

func TestDurationBar(t *testing.T) {
	bar := durationBar("p50", "10ms", "100ms", "gradient-blue")
	if bar.Label != "p50" {
		t.Fatalf("expected label p50, got %q", bar.Label)
	}
	if bar.Width != 10.0 {
		t.Fatalf("expected width 10, got %f", bar.Width)
	}

	tiny := durationBar("min", "1ns", "1h", "gradient-green")
	if tiny.Width <= 0 {
		t.Fatalf("expected positive width for tiny values, got %f", tiny.Width)
	}

	empty := durationBar("x", "0s", "0s", "gradient-red")
	if empty.Width != 0 {
		t.Fatalf("expected zero width for zero values, got %f", empty.Width)
	}
}

func TestNewRateLimiter(t *testing.T) {
	noop, err := newRateLimiter(0)
	if err != nil {
		t.Fatalf("newRateLimiter(0) returned error: %v", err)
	}
	if _, ok := noop.(noopLimiter); !ok {
		t.Fatalf("expected noopLimiter for rps=0, got %T", noop)
	}

	limiter, err := newRateLimiter(1000)
	if err != nil {
		t.Fatalf("newRateLimiter(1000) returned error: %v", err)
	}
	defer limiter.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := limiter.Wait(ctx); err != nil {
		t.Fatalf("expected Wait to succeed, got %v", err)
	}

	if _, err := newRateLimiter(-1); err == nil {
		t.Fatal("expected negative rps to fail")
	}
}

func TestSortedStringMapEntries(t *testing.T) {
	if got := sortedStringMapEntries(nil); got != nil {
		t.Fatalf("expected nil for empty map, got %v", got)
	}

	got := sortedStringMapEntries(map[string]int{"b": 2, "a": 1, "c": 3})
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	if got[0].Key != "a" || got[1].Key != "b" || got[2].Key != "c" {
		t.Fatalf("expected sorted keys, got %v", got)
	}
}

func TestFormatStringMapEntries(t *testing.T) {
	got := formatStringMapEntries("status_code", map[string]int{"200": 5, "500": 1})
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0][0] != "status_code.200" || got[0][1] != "5" {
		t.Fatalf("expected prefixed entry, got %v", got[0])
	}
}

func TestDurationBarClampsOverflow(t *testing.T) {
	bar := durationBar("max", "200ms", "100ms", "gradient-red")
	if bar.Width > 100.0 {
		t.Fatalf("expected width clamped to <= 100, got %f", bar.Width)
	}
}

func TestRunStressTestEndToEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	successStatus, err := parseStatusRanges("200-399")
	if err != nil {
		t.Fatalf("parseStatusRanges returned error: %v", err)
	}

	outputPrefix := filepath.Join(t.TempDir(), "report")
	cfg := Config{
		URL:               srv.URL,
		Concurrency:       3,
		Duration:          200 * time.Millisecond,
		Method:            http.MethodGet,
		SuccessStatusSpec: "200-399",
		SuccessStatus:     successStatus,
		OutputFormats:     []string{"json", "csv"},
		OutputPrefix:      outputPrefix,
	}

	if err := runStressTest(cfg); err != nil {
		t.Fatalf("runStressTest returned error: %v", err)
	}

	data, err := os.ReadFile(outputPrefix + ".json")
	if err != nil {
		t.Fatalf("expected json report to be written: %v", err)
	}
	var report TestReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("failed to parse report json: %v", err)
	}

	if report.TotalRequests == 0 {
		t.Fatal("expected at least one request to be recorded")
	}
	if report.SuccessRate < 0.999 {
		t.Fatalf("expected ~100%% success rate, got %f", report.SuccessRate)
	}
	if report.URL != srv.URL {
		t.Fatalf("expected url %q, got %q", srv.URL, report.URL)
	}
	if _, ok := report.StatusCodes["200"]; !ok {
		t.Fatalf("expected status 200 in report, got %v", report.StatusCodes)
	}

	if _, err := os.Stat(outputPrefix + ".csv"); err != nil {
		t.Fatalf("expected csv report to be written: %v", err)
	}
}

func TestRunStressTestReportsTransportErrors(t *testing.T) {
	successStatus, err := parseStatusRanges("200-399")
	if err != nil {
		t.Fatalf("parseStatusRanges returned error: %v", err)
	}

	outputPrefix := filepath.Join(t.TempDir(), "report")
	cfg := Config{
		URL:               "http://127.0.0.1:1/",
		Concurrency:       1,
		Duration:          150 * time.Millisecond,
		Method:            http.MethodGet,
		SuccessStatusSpec: "200-399",
		SuccessStatus:     successStatus,
		OutputFormats:     []string{"json"},
		OutputPrefix:      outputPrefix,
	}

	if err := runStressTest(cfg); err != nil {
		t.Fatalf("runStressTest returned error: %v", err)
	}

	data, err := os.ReadFile(outputPrefix + ".json")
	if err != nil {
		t.Fatalf("expected json report to be written: %v", err)
	}
	var report TestReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("failed to parse report json: %v", err)
	}

	if report.TransportErrors == 0 {
		t.Fatal("expected transport errors to be recorded")
	}
	if len(report.ErrorTypes) == 0 {
		t.Fatal("expected transport error types to be recorded")
	}
}

func TestNoopLimiterStopAndWait(t *testing.T) {
	l := noopLimiter{}
	if err := l.Wait(context.Background()); err != nil {
		t.Fatalf("expected nil from Wait, got %v", err)
	}
	l.Stop()
}

func TestParseFlagsBuildsConfig(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet("stressy", flag.ExitOnError)
	os.Args = []string{
		"stressy",
		"--url", "http://example.com/api",
		"--c", "5",
		"--d", "3",
		"--method", "POST",
		"--payload", `{"a":1}`,
		"--headers", "X-Token:abc",
		"--success-status", "200,404",
		"--rps", "100",
		"--ramp", "2",
		"--formats", "json,csv",
		"--output", "/tmp/report-x",
	}

	cfg, err := parseFlags()
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}

	if cfg.URL != "http://example.com/api" {
		t.Fatalf("unexpected url: %q", cfg.URL)
	}
	if cfg.Concurrency != 5 {
		t.Fatalf("unexpected concurrency: %d", cfg.Concurrency)
	}
	if cfg.Duration != 3*time.Second {
		t.Fatalf("unexpected duration: %s", cfg.Duration)
	}
	if cfg.Method != http.MethodPost {
		t.Fatalf("unexpected method: %q", cfg.Method)
	}
	if cfg.Payload != `{"a":1}` {
		t.Fatalf("unexpected payload: %q", cfg.Payload)
	}
	if cfg.Headers != "X-Token:abc" {
		t.Fatalf("unexpected headers: %q", cfg.Headers)
	}
	if cfg.RequestsPerSecond != 100 {
		t.Fatalf("unexpected rps: %f", cfg.RequestsPerSecond)
	}
	if cfg.RampDuration != 2*time.Second {
		t.Fatalf("unexpected ramp: %s", cfg.RampDuration)
	}
	if len(cfg.OutputFormats) != 2 || cfg.OutputFormats[0] != "json" || cfg.OutputFormats[1] != "csv" {
		t.Fatalf("unexpected formats: %v", cfg.OutputFormats)
	}
	if len(cfg.SuccessStatus) != 2 {
		t.Fatalf("expected 2 success status ranges, got %d", len(cfg.SuccessStatus))
	}
}

func TestParseFlagsFromReportModeSetsPrefix(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet("stressy", flag.ExitOnError)
	os.Args = []string{"stressy", "--from-report", "/tmp/foo.json", "--formats", "html"}

	cfg, err := parseFlags()
	if err != nil {
		t.Fatalf("parseFlags returned error: %v", err)
	}
	if cfg.FromReport != "/tmp/foo.json" {
		t.Fatalf("unexpected from-report: %q", cfg.FromReport)
	}
	if cfg.OutputPrefix != "/tmp/foo" {
		t.Fatalf("expected output prefix derived from report, got %q", cfg.OutputPrefix)
	}
}

func TestMainFromReportModeWritesOutput(t *testing.T) {
	dir := t.TempDir()
	report := sampleWebReport()
	data, err := renderJSONReport(report)
	if err != nil {
		t.Fatalf("renderJSONReport returned error: %v", err)
	}
	source := filepath.Join(dir, "source.json")
	if err := os.WriteFile(source, data, 0o644); err != nil {
		t.Fatalf("failed to write source report: %v", err)
	}

	flag.CommandLine = flag.NewFlagSet("stressy", flag.ExitOnError)
	os.Args = []string{
		"stressy",
		"--from-report", source,
		"--formats", "html",
		"--output", filepath.Join(dir, "out"),
	}

	main()

	if _, err := os.Stat(filepath.Join(dir, "out.html")); err != nil {
		t.Fatalf("expected html output to be written: %v", err)
	}
}
