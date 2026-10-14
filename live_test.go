package main

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func liveTestConfig() Config {
	statuses, err := parseStatusRanges("200-399")
	if err != nil {
		panic(err)
	}
	return Config{
		URL:               "http://example.com/",
		Concurrency:       10,
		Duration:          5 * time.Second,
		Method:            http.MethodGet,
		SuccessStatusSpec: "200-399",
		SuccessStatus:     statuses,
		OutputFormats:     []string{"json"},
		OutputPrefix:      "report",
	}
}

func liveTestRamp(cfg Config, deadline time.Time) *concurrencyRamp {
	return newConcurrencyRamp(cfg.Concurrency, cfg.Concurrency, 0, deadline)
}

func TestLiveReporterCountsAndRates(t *testing.T) {
	cfg := liveTestConfig()
	start := time.Now()
	deadline := start.Add(cfg.Duration)
	r := newLiveReporter(cfg, start, deadline, liveTestRamp(cfg, deadline))

	for i := 0; i < 90; i++ {
		r.sample(Result{StatusCode: 200, Latency: time.Millisecond, BytesRead: 100})
	}
	for i := 0; i < 5; i++ {
		r.sample(Result{StatusCode: 500, Latency: time.Millisecond})
	}
	for i := 0; i < 5; i++ {
		r.sample(Result{Error: errors.New("boom")})
	}

	s := r.snapshot()
	if s.TotalRequests != 100 {
		t.Fatalf("expected 100 total requests, got %d", s.TotalRequests)
	}
	if s.SuccessRequests != 90 {
		t.Fatalf("expected 90 success, got %d", s.SuccessRequests)
	}
	if s.FailedRequests != 10 {
		t.Fatalf("expected 10 failed, got %d", s.FailedRequests)
	}
	if s.TransportErrors != 5 {
		t.Fatalf("expected 5 transport errors, got %d", s.TransportErrors)
	}
	if s.HTTPFailures != 5 {
		t.Fatalf("expected 5 http failures, got %d", s.HTTPFailures)
	}
	if s.SuccessRate < 89.9 || s.SuccessRate > 90.1 {
		t.Fatalf("expected ~90%% success rate, got %f", s.SuccessRate)
	}
	if s.ErrorRate < 9.9 || s.ErrorRate > 10.1 {
		t.Fatalf("expected ~10%% error rate, got %f", s.ErrorRate)
	}
	if !s.Running || s.Finished {
		t.Fatal("expected reporter to be running")
	}
	if s.TargetConcurrency != 10 {
		t.Fatalf("expected concurrency 10, got %d", s.TargetConcurrency)
	}
	if s.RequestsPerSecond <= 0 {
		t.Fatalf("expected requests per second > 0, got %f", s.RequestsPerSecond)
	}
	if len(s.Timeline) == 0 {
		t.Fatal("expected at least one timeline point")
	}
	if s.TotalDataMB <= 0 {
		t.Fatalf("expected data transferred > 0, got %f", s.TotalDataMB)
	}
}

func TestLiveReporterFinishState(t *testing.T) {
	cfg := liveTestConfig()
	start := time.Now()
	deadline := start.Add(cfg.Duration)
	r := newLiveReporter(cfg, start, deadline, liveTestRamp(cfg, deadline))
	r.sample(Result{StatusCode: 200, Latency: time.Millisecond, BytesRead: 1024})

	r.finish()
	s0 := r.snapshot()
	if s0.Running {
		t.Fatal("expected reporter to be stopped after finish")
	}
	if !s0.Finished {
		t.Fatal("expected finished flag to be set")
	}
	if s0.RequestsPerSecond <= 0 {
		t.Fatalf("expected rps frozen at finish, got %f", s0.RequestsPerSecond)
	}
	if s0.AvgThroughputMBS <= 0 {
		t.Fatalf("expected throughput frozen at finish, got %f", s0.AvgThroughputMBS)
	}
	if s0.Percent != 100 {
		t.Fatalf("expected percent 100 at finish, got %f", s0.Percent)
	}
	if s0.Remaining < 0 || s0.Remaining >= 5 {
		t.Fatalf("expected remaining to stay inside the run window, got %f", s0.Remaining)
	}

	time.Sleep(30 * time.Millisecond)
	s1 := r.snapshot()
	if s1.RequestsPerSecond != s0.RequestsPerSecond {
		t.Fatalf("expected rps to stay frozen after finish, got %f vs %f", s1.RequestsPerSecond, s0.RequestsPerSecond)
	}
	if s1.AvgThroughputMBS != s0.AvgThroughputMBS {
		t.Fatalf("expected throughput to stay frozen after finish")
	}
	if s1.Remaining != s0.Remaining {
		t.Fatalf("expected remaining to stay frozen after finish, got %f vs %f", s1.Remaining, s0.Remaining)
	}
}

func TestLatencyHistogramStats(t *testing.T) {
	h := newLatencyHistogram()
	for i := 0; i < 9; i++ {
		h.add(10 * time.Millisecond)
	}
	h.add(100 * time.Millisecond)

	min, avg, p50, p95, p99, max := h.stats()
	if min != 10*time.Millisecond {
		t.Fatalf("expected min 10ms, got %s", min)
	}
	if max != 100*time.Millisecond {
		t.Fatalf("expected max 100ms, got %s", max)
	}
	if avg <= 10*time.Millisecond || avg >= 100*time.Millisecond {
		t.Fatalf("expected avg in (10ms,100ms), got %s", avg)
	}
	if p50 < 8*time.Millisecond || p50 > 15*time.Millisecond {
		t.Fatalf("expected p50 within a bucket of 10ms, got %s", p50)
	}
	for _, p := range []time.Duration{p95, p99} {
		if p < 50*time.Millisecond || p > 150*time.Millisecond {
			t.Fatalf("expected p95/p99 within a bucket of 100ms, got %s", p)
		}
	}
}

func TestLatencyHistogramEmpty(t *testing.T) {
	min, avg, p50, p95, p99, max := (&latencyHistogram{}).stats()
	if min != 0 || avg != 0 || p50 != 0 || p95 != 0 || p99 != 0 || max != 0 {
		t.Fatalf("expected zero stats for empty histogram, got %s %s %s %s %s %s", min, avg, p50, p95, p99, max)
	}
	if h := newLatencyHistogram(); h.percentile(0.5) != 0 {
		t.Fatal("expected 0 for empty percentile")
	}
}

func TestLiveReporterTimelineRates(t *testing.T) {
	cfg := liveTestConfig()
	start := time.Now()
	deadline := start.Add(cfg.Duration)
	r := newLiveReporter(cfg, start, deadline, liveTestRamp(cfg, deadline))

	r.sample(Result{StatusCode: 200, Latency: time.Millisecond, BytesRead: 8})
	r.sample(Result{StatusCode: 500, Latency: time.Millisecond})

	pt := r.snapshot().Timeline
	if len(pt) != 1 {
		t.Fatalf("expected 1 timeline point, got %d", len(pt))
	}
	if pt[0].Requests != 2 {
		t.Fatalf("expected 2 requests in timeline, got %d", pt[0].Requests)
	}
	if pt[0].SuccessRate != 50 {
		t.Fatalf("expected 50%% timeline success, got %f", pt[0].SuccessRate)
	}
	if pt[0].ErrorRate != 50 {
		t.Fatalf("expected 50%% timeline error rate, got %f", pt[0].ErrorRate)
	}
	if pt[0].AvgLatencyMs != 1 {
		t.Fatalf("expected 1ms average latency, got %f", pt[0].AvgLatencyMs)
	}
}

func TestLiveReporterThroughputPct(t *testing.T) {
	cfg := liveTestConfig()
	cfg.RequestsPerSecond = 1000
	start := time.Now()
	deadline := start.Add(cfg.Duration)
	r := newLiveReporter(cfg, start, deadline, liveTestRamp(cfg, deadline))
	for i := 0; i < 50; i++ {
		r.sample(Result{StatusCode: 200, Latency: time.Millisecond, BytesRead: 1})
	}
	s := r.snapshot()
	if s.RequestsPerSecond <= 0 {
		t.Fatalf("expected rps > 0, got %f", s.RequestsPerSecond)
	}
	want := (s.RequestsPerSecond / 1000) * 100
	if s.ThroughputPct < want-0.001 || s.ThroughputPct > want+0.001 {
		t.Fatalf("expected throughput pct %f, got %f", want, s.ThroughputPct)
	}

	cfgNoLimit := liveTestConfig()
	start2 := time.Now()
	deadline2 := start2.Add(cfgNoLimit.Duration)
	r2 := newLiveReporter(cfgNoLimit, start2, deadline2, liveTestRamp(cfgNoLimit, deadline2))
	for i := 0; i < 10; i++ {
		r2.sample(Result{StatusCode: 200, Latency: time.Millisecond})
	}
	pct, ceiling := r2.throughputPct(5000)
	if pct < 49.9 || pct > 50.1 {
		t.Fatalf("expected ~50%% auto throughput pct, got %f", pct)
	}
	if ceiling < 9990 || ceiling > 10010 {
		t.Fatalf("expected ceiling ~10000 rps, got %f", ceiling)
	}
	if got := r2.snapshot().ThroughputPct; got <= 0 {
		t.Fatalf("expected auto throughput pct > 0 without rps cap, got %f", got)
	}

	zero, _ := r2.throughputPct(0)
	if zero != 0 {
		t.Fatalf("expected 0 throughput pct for zero rps, got %f", zero)
	}
}

func TestServeLivePageAndJSON(t *testing.T) {
	cfg := liveTestConfig()
	start := time.Now()
	deadline := start.Add(cfg.Duration)
	r := newLiveReporter(cfg, start, deadline, liveTestRamp(cfg, deadline))
	r.sample(Result{StatusCode: 200, Latency: time.Millisecond})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/live", nil)
	serveLivePage(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for live page, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "live dashboard") {
		t.Fatalf("expected dashboard markup in page, got %s", rec.Body.String()[:200])
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/live", nil)
	r.serveJSON(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for api, got %d", rec.Code)
	}
	var snap liveSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("failed to parse snapshot: %v", err)
	}
	if snap.TotalRequests != 1 || snap.URL != "http://example.com/" {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/live", nil)
	r.serveJSON(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for POST, got %d", rec.Code)
	}
}

func TestLiveServerEndToEnd(t *testing.T) {
	cfg := liveTestConfig()
	start := time.Now()
	deadline := start.Add(cfg.Duration)
	r := newLiveReporter(cfg, start, deadline, liveTestRamp(cfg, deadline))
	r.sample(Result{StatusCode: 200, Latency: time.Millisecond})

	addr, srv, err := startLiveServer("127.0.0.1:0", r)
	if err != nil {
		t.Fatalf("startLiveServer returned error: %v", err)
	}
	defer stopLiveServer(srv)

	pageResp, err := http.Get("http://" + addr + "/live")
	if err != nil {
		t.Fatalf("failed to fetch live page: %v", err)
	}
	defer pageResp.Body.Close()
	if pageResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for page, got %d", pageResp.StatusCode)
	}

	jsonResp, err := http.Get("http://" + addr + "/api/live")
	if err != nil {
		t.Fatalf("failed to fetch live api: %v", err)
	}
	defer jsonResp.Body.Close()
	var snap liveSnapshot
	if err := json.NewDecoder(jsonResp.Body).Decode(&snap); err != nil {
		t.Fatalf("failed to decode snapshot: %v", err)
	}
	if snap.TotalRequests != 1 {
		t.Fatalf("expected 1 total request via http, got %d", snap.TotalRequests)
	}
}

func TestStartLiveServerReportsBindError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind: %v", err)
	}
	defer ln.Close()

	cfg := liveTestConfig()
	start := time.Now()
	deadline := start.Add(cfg.Duration)
	r := newLiveReporter(cfg, start, deadline, liveTestRamp(cfg, deadline))

	if _, _, err := startLiveServer(ln.Addr().String(), r); err == nil {
		t.Fatal("expected bind error for occupied address")
	}
}

func TestLiveTimelineCapWindow(t *testing.T) {
	cfg := liveTestConfig()
	start := time.Now().Add(-100 * time.Hour)
	deadline := start.Add(cfg.Duration)
	r := newLiveReporter(cfg, start, deadline, liveTestRamp(cfg, deadline))

	for i := 0; i < 500; i++ {
		r.sample(Result{StatusCode: 200, Latency: time.Millisecond})
	}
	s := r.snapshot()
	if len(s.Timeline) > liveTimelineWindow {
		t.Fatalf("expected timeline capped at %d, got %d", liveTimelineWindow, len(s.Timeline))
	}
	if s.Percent > 100 {
		t.Fatalf("expected percent clamped to 100, got %f", s.Percent)
	}
}

func TestValidateAndNormalizeConfigRejectsLiveWebConflicts(t *testing.T) {
	cfg := liveTestConfig() // valid base
	cfg.LiveWebAddr = ":8088"
	if _, err := validateAndNormalizeConfig(cfg); err != nil {
		t.Fatalf("expected live web run to be valid, got %v", err)
	}

	bad := liveTestConfig()
	bad.LiveWebAddr = ":8088"
	bad.ServeWeb = true
	if _, err := validateAndNormalizeConfig(bad); err == nil {
		t.Fatal("expected error combining --live-web with --serve-web")
	}

	bad = liveTestConfig()
	bad.LiveWebAddr = ":8088"
	bad.FromReport = "report.json"
	bad.OutputPrefix = "out"
	if _, err := validateAndNormalizeConfig(bad); err == nil {
		t.Fatal("expected error combining --live-web with --from-report")
	}
}
