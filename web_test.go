package main

import (
	"net"
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

func newTestWebApp() *webApp {
	app := &webApp{auth: newMemoryAuthStore()}
	_ = app.auth.createUser("user@example.com", "password123")
	return app
}

func loginAs(t *testing.T, app *webApp, email string) *http.Cookie {
	t.Helper()
	token, err := app.auth.createSession(email)
	if err != nil {
		t.Fatalf("createSession returned error: %v", err)
	}
	return &http.Cookie{Name: sessionCookieName, Value: token}
}

func TestHandleRootRedirectsLoggedOutUsersToLogin(t *testing.T) {
	app := newTestWebApp()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	app.handleRoot(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected redirect, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/login" {
		t.Fatalf("expected /login, got %q", loc)
	}
}

func TestHandleRootRedirectsLoggedInUsersToDashboard(t *testing.T) {
	app := newTestWebApp()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(loginAs(t, app, "user@example.com"))
	rec := httptest.NewRecorder()

	app.handleRoot(rec, req)

	if loc := rec.Header().Get("Location"); !strings.HasPrefix(loc, "/dashboard") {
		t.Fatalf("expected /dashboard redirect, got %q", loc)
	}
}

func TestHandleRootNotFoundForUnknownPaths(t *testing.T) {
	app := newTestWebApp()
	req := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	rec := httptest.NewRecorder()

	app.handleRoot(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandleSignupGetRendersForm(t *testing.T) {
	app := newTestWebApp()
	req := httptest.NewRequest(http.MethodGet, "/signup", nil)
	rec := httptest.NewRecorder()

	app.handleSignup(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Create Account") {
		t.Fatalf("expected signup page, got %s", rec.Body.String())
	}
}

func TestHandleSignupPostCreatesUserAndRedirects(t *testing.T) {
	app := newTestWebApp()
	form := url.Values{}
	form.Set("email", "newuser@example.com")
	form.Set("password", "password123")

	req := httptest.NewRequest(http.MethodPost, "/signup", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	app.handleSignup(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected redirect, got %d", rec.Code)
	}
	if !app.auth.authenticate("newuser@example.com", "password123") {
		t.Fatal("expected user to be creatable after signup")
	}
}

func TestHandleSignupPostRejectsDuplicateUser(t *testing.T) {
	app := newTestWebApp()
	form := url.Values{}
	form.Set("email", "user@example.com")
	form.Set("password", "password123")

	req := httptest.NewRequest(http.MethodPost, "/signup", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	app.handleSignup(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected form re-render, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "already exists") {
		t.Fatalf("expected error in response, got %s", rec.Body.String())
	}
}

func TestHandleLoginPostAuthenticatesAndRedirects(t *testing.T) {
	app := newTestWebApp()
	form := url.Values{}
	form.Set("email", "user@example.com")
	form.Set("password", "password123")

	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	app.handleLogin(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected redirect, got %d", rec.Code)
	}
	if !strings.Contains(rec.Result().Header.Get("Set-Cookie"), sessionCookieName) {
		t.Fatalf("expected session cookie to be set, got %q", rec.Result().Header.Get("Set-Cookie"))
	}
}

func TestHandleLoginPostRejectsBadCredentials(t *testing.T) {
	app := newTestWebApp()
	form := url.Values{}
	form.Set("email", "user@example.com")
	form.Set("password", "wrong-password")

	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	app.handleLogin(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected form re-render, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "invalid email or password") {
		t.Fatalf("expected login error, got %s", rec.Body.String())
	}
}

func TestHandleLoginGetRendersForm(t *testing.T) {
	app := newTestWebApp()
	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	rec := httptest.NewRecorder()

	app.handleLogin(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Sign in to the analytics dashboard") {
		t.Fatalf("expected login page, got %s", rec.Body.String())
	}
}

func TestHandleLogoutGetRendersConfirmation(t *testing.T) {
	app := newTestWebApp()
	req := httptest.NewRequest(http.MethodGet, "/logout", nil)
	req.AddCookie(loginAs(t, app, "user@example.com"))
	rec := httptest.NewRecorder()

	app.handleLogout(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Sign out of the dashboard") {
		t.Fatalf("expected logout page, got %s", rec.Body.String())
	}
}

func TestHandleLogoutPostClearsSessionAndRedirects(t *testing.T) {
	app := newTestWebApp()
	cookie := loginAs(t, app, "user@example.com")

	req := httptest.NewRequest(http.MethodPost, "/logout", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()

	app.handleLogout(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected redirect, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/login" {
		t.Fatalf("expected /login, got %q", loc)
	}

	token := cookie.Value
	if email, ok := app.auth.lookupSession(token); ok || email != "" {
		t.Fatal("expected session to be cleared after logout")
	}
}

func TestHandleDashboardRendersWithReport(t *testing.T) {
	app := newTestWebApp()
	app.replaceReport(sampleWebReport(), "test-source")
	cookie := loginAs(t, app, "user@example.com")

	req := httptest.NewRequest(http.MethodGet, "/dashboard?user=user@example.com", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()

	app.handleDashboard(rec, req, "user@example.com")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Dashboard") {
		t.Fatalf("expected dashboard content, got %s", rec.Body.String())
	}
}

func TestHandleDashboardRendersEmptyState(t *testing.T) {
	app := newTestWebApp()
	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	req.AddCookie(loginAs(t, app, "user@example.com"))
	rec := httptest.NewRecorder()

	app.handleDashboard(rec, req, "user@example.com")

	if !strings.Contains(rec.Body.String(), "No report loaded yet") {
		t.Fatalf("expected empty state, got %s", rec.Body.String())
	}
}

func TestHandleReportAPIReturnsJSON(t *testing.T) {
	app := newTestWebApp()
	app.replaceReport(sampleWebReport(), "test-source")
	cookie := loginAs(t, app, "user@example.com")

	req := httptest.NewRequest(http.MethodGet, "/api/report", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()

	app.handleReportAPI(rec, req, "user@example.com")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("expected JSON content type, got %q", rec.Header().Get("Content-Type"))
	}
	if !strings.Contains(rec.Body.String(), "example.com") {
		t.Fatalf("expected report URL in response, got %s", rec.Body.String())
	}
}

func TestHandleReportAPIDeleteMethodReturnsMethodNotAllowed(t *testing.T) {
	app := newTestWebApp()
	app.replaceReport(sampleWebReport(), "test-source")
	cookie := loginAs(t, app, "user@example.com")

	req := httptest.NewRequest(http.MethodDelete, "/api/report", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()

	app.handleReportAPI(rec, req, "user@example.com")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestHandleReportAPIPostImportsReportFromBody(t *testing.T) {
	app := newTestWebApp()
	cookie := loginAs(t, app, "user@example.com")

	data, err := renderJSONReport(sampleWebReport())
	if err != nil {
		t.Fatalf("renderJSONReport returned error: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/report", strings.NewReader(string(data)))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()

	app.handleReportAPI(rec, req, "user@example.com")

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	if loaded, _ := app.snapshotReport(); loaded == nil {
		t.Fatal("expected report to be imported")
	}
}

func TestHandleReportAPIPostRejectsInvalidBody(t *testing.T) {
	app := newTestWebApp()
	cookie := loginAs(t, app, "user@example.com")

	req := httptest.NewRequest(http.MethodPost, "/api/report", strings.NewReader("{invalid"))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()

	app.handleReportAPI(rec, req, "user@example.com")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleReportAPIReturns404WithoutReport(t *testing.T) {
	app := newTestWebApp()
	cookie := loginAs(t, app, "user@example.com")

	req := httptest.NewRequest(http.MethodGet, "/api/report", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()

	app.handleReportAPI(rec, req, "user@example.com")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandleDashboardImportRejectsInvalidJSON(t *testing.T) {
	app := newTestWebApp()
	cookie := loginAs(t, app, "user@example.com")

	form := url.Values{}
	form.Set("report_json", "{invalid json")

	req := httptest.NewRequest(http.MethodPost, "/dashboard/import", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()

	app.handleDashboardImport(rec, req, "user@example.com")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleDashboardImportRejectsNonPost(t *testing.T) {
	app := newTestWebApp()
	cookie := loginAs(t, app, "user@example.com")

	req := httptest.NewRequest(http.MethodGet, "/dashboard/import", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()

	app.handleDashboardImport(rec, req, "user@example.com")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestDeleteSessionRemovesActiveToken(t *testing.T) {
	store := newMemoryAuthStore()
	_ = store.createUser("user@example.com", "password123")
	token, err := store.createSession("user@example.com")
	if err != nil {
		t.Fatalf("createSession returned error: %v", err)
	}

	if email, ok := store.lookupSession(token); !ok || email != "user@example.com" {
		t.Fatal("expected session to exist before delete")
	}

	store.deleteSession(token)
	if email, ok := store.lookupSession(token); ok || email != "" {
		t.Fatal("expected session to be deleted")
	}
}

func TestDeleteSessionDoesNotPanicOnUnknownToken(t *testing.T) {
	store := newMemoryAuthStore()
	store.deleteSession("nonexistent-token")
}

func TestNormalizeEmailEdges(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"User@Example.COM", "user@example.com"},
		{"  spaced@example.com  ", "spaced@example.com"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := normalizeEmail(tt.input); got != tt.want {
			t.Errorf("normalizeEmail(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestLooksLikeEmail(t *testing.T) {
	if !looksLikeEmail("user@example.com") {
		t.Fatal("expected valid email to pass")
	}
	if looksLikeEmail("not-an-email") {
		t.Fatal("expected invalid email to fail")
	}
}

func TestHashPasswordRoundTrips(t *testing.T) {
	salt, hash, err := hashPassword("password123")
	if err != nil {
		t.Fatalf("hashPassword returned error: %v", err)
	}
	if salt == "" || hash == "" {
		t.Fatalf("expected salt and hash to be non-empty")
	}
	if !verifyPassword("password123", salt, hash) {
		t.Fatal("expected correct password to verify")
	}
	if verifyPassword("wrong-password", salt, hash) {
		t.Fatal("expected wrong password to fail verification")
	}
}

func TestLoadInitialDashboardReportReturnsNilWhenNoDefaultFile(t *testing.T) {
	dir := t.TempDir()
	wd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir returned error: %v", err)
	}
	defer os.Chdir(wd)

	loaded, source, err := loadInitialDashboardReport("")
	if err != nil {
		t.Fatalf("expected no error when no default file, got %v", err)
	}
	if loaded != nil || source != "" {
		t.Fatalf("expected nil loaded report, got %v %q", loaded, source)
	}
}

func TestLoadInitialDashboardReportDefaultsToReportJSON(t *testing.T) {
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

	wd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir returned error: %v", err)
	}
	defer os.Chdir(wd)

	loaded, source, err := loadInitialDashboardReport("")
	if err != nil {
		t.Fatalf("loadInitialDashboardReport returned error: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected report to be loaded from default path")
	}
	if source != "report.json" {
		t.Fatalf("expected default source report.json, got %q", source)
	}
}

func TestHandleSignupRejectsNonPostGet(t *testing.T) {
	app := newTestWebApp()
	req := httptest.NewRequest(http.MethodPut, "/signup", nil)
	rec := httptest.NewRecorder()

	app.handleSignup(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestRoutesServesAllEndpoints(t *testing.T) {
	app := newTestWebApp()
	handler := app.routes()

	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected login route to render, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/signup", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected signup route to render, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected dashboard to redirect when logged out, got %d", rec.Code)
	}
}

func TestServeWebReportsBindError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	err = serveWeb(Config{WebAddr: addr, DashboardReport: ""})
	if err == nil {
		t.Fatal("expected serveWeb to return an error for an occupied address")
	}
}

func TestServeWebRejectsUnreadableDashboardReport(t *testing.T) {
	err := serveWeb(Config{
		WebAddr:        "127.0.0.1:0",
		DashboardReport: filepath.Join(t.TempDir(), "missing.json"),
	})
	if err == nil {
		t.Fatal("expected serveWeb to return an error for a missing dashboard report")
	}
}
