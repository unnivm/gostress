package main

import (
	"context"
	"net/http"
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
