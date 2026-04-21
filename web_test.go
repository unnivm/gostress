package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMemoryAuthStoreCreateUserAndAuthenticate(t *testing.T) {
	store := newMemoryAuthStore()

	if err := store.createUser("user@example.com", "password123"); err != nil {
		t.Fatalf("createUser returned error: %v", err)
	}

	if !store.authenticate("user@example.com", "password123") {
		t.Fatal("expected authenticate to succeed")
	}

	if store.authenticate("user@example.com", "wrong-password") {
		t.Fatal("expected authenticate to fail with wrong password")
	}
}

func TestProtectedDashboardRedirectsWhenLoggedOut(t *testing.T) {
	app := &webApp{auth: newMemoryAuthStore()}

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()

	app.requireAuth(app.handleDashboard).ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected redirect, got %d", rec.Code)
	}

	if location := rec.Header().Get("Location"); location != "/login" {
		t.Fatalf("expected redirect to /login, got %q", location)
	}
}

func TestDashboardImportUpdatesReport(t *testing.T) {
	app := &webApp{auth: newMemoryAuthStore()}
	if err := app.auth.createUser("user@example.com", "password123"); err != nil {
		t.Fatalf("createUser returned error: %v", err)
	}

	token, err := app.auth.createSession("user@example.com")
	if err != nil {
		t.Fatalf("createSession returned error: %v", err)
	}

	report := sampleWebReport()
	payload, err := renderJSONReport(report)
	if err != nil {
		t.Fatalf("renderJSONReport returned error: %v", err)
	}

	form := url.Values{}
	form.Set("report_json", string(payload))

	req := httptest.NewRequest(http.MethodPost, "/dashboard/import", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	rec := httptest.NewRecorder()

	app.requireAuth(app.handleDashboardImport).ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected redirect after import, got %d", rec.Code)
	}

	loaded, source := app.snapshotReport()
	if loaded == nil {
		t.Fatal("expected report to be loaded")
	}
	if loaded.URL != report.URL {
		t.Fatalf("expected report url %q, got %q", report.URL, loaded.URL)
	}
	if !strings.Contains(source, "Imported by user@example.com") {
		t.Fatalf("expected import source to mention user, got %q", source)
	}
}

func TestLoadInitialDashboardReport(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")

	report := sampleWebReport()
	data, err := renderJSONReport(report)
	if err != nil {
		t.Fatalf("renderJSONReport returned error: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("failed writing report: %v", err)
	}

	loaded, source, err := loadInitialDashboardReport(path)
	if err != nil {
		t.Fatalf("loadInitialDashboardReport returned error: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected loaded report")
	}
	if source != path {
		t.Fatalf("expected source %q, got %q", path, source)
	}
}

func sampleWebReport() TestReport {
	return TestReport{
		StartTime:         "2026-04-21 12:00:00",
		URL:               "http://example.com",
		Concurrency:       8,
		Duration:          "5s",
		Method:            "GET",
		SuccessStatusSpec: "200-399",
		StatusCodes:       map[string]int{"200": 100, "500": 2},
		ErrorTypes:        map[string]int{"deadline_exceeded": 1},
		TotalRequests:     103,
		FailedRequests:    3,
		TransportErrors:   1,
		HTTPFailures:      2,
		SuccessRequests:   100,
		SuccessRate:       97.08,
		ErrorRate:         2.91,
		RequestsPerSecond: 20.6,
		MinLatency:        "1ms",
		MaxLatency:        "22ms",
		AverageLatency:    "4ms",
		P50Latency:        "3ms",
		P95Latency:        "12ms",
		P99Latency:        "18ms",
		TotalDataMB:       2.4,
		AvgThroughputMB:   0.48,
		ActualDuration:    "5.02s",
	}
}
