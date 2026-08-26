package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const sessionCookieName = "gostress_session"

type userRecord struct {
	Email        string
	PasswordSalt string
	PasswordHash string
	CreatedAt    time.Time
}

type memoryAuthStore struct {
	mu       sync.RWMutex
	users    map[string]userRecord
	sessions map[string]string
}

type webApp struct {
	addr   string
	auth   *memoryAuthStore
	report *TestReport
	source string
	mu     sync.RWMutex
}

type authPageData struct {
	Title       string
	Heading     string
	Subheading  string
	Error       string
	Email       string
	Action      string
	AltCopy     string
	AltLink     string
	AltLabel    string
	IsLogout    bool
	Message     string
	CurrentUser string
}

type dashboardPageData struct {
	Title             string
	CurrentUser       string
	HasReport         bool
	Report            *TestReport
	ReportSource      string
	ImportedAt        string
	ReportJSON        string
	SuccessPct        float64
	HTTPFailurePct    float64
		TransportErrorPct float64
		ThroughputPct     float64
		SummaryHeadline   string
	SummaryParagraphs []string
	LatencyBars       []chartBar
	StatusCodeBars    []chartBar
	ErrorBars         []chartBar
	StatusCodes       []countEntry
	ErrorTypes        []countEntry
	ReliabilityNote   string
	LatencyNote       string
	ThroughputNote    string
	ChartCardTone     string
}

var webTemplates = template.Must(template.New("web").Funcs(template.FuncMap{
	"join":        strings.Join,
	"defaultText": defaultText,
	"formatFloat": func(v float64) string { return fmt.Sprintf("%.2f", v) },
	"add":         func(a, b float64) float64 { return a + b },
	"formatTime": func(v string) string {
		if strings.TrimSpace(v) == "" {
			return "(not available)"
		}
		return v
	},
}).Parse(webTemplateSource))

func serveWeb(cfg Config) error {
	report, source, err := loadInitialDashboardReport(cfg.DashboardReport)
	if err != nil {
		return err
	}

	app := &webApp{
		addr:   cfg.WebAddr,
		auth:   newMemoryAuthStore(),
		report: report,
		source: source,
	}

	server := &http.Server{
		Addr:              cfg.WebAddr,
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	fmt.Printf("Web dashboard ready at http://localhost%s\n", cfg.WebAddr)
	return server.ListenAndServe()
}

func newMemoryAuthStore() *memoryAuthStore {
	return &memoryAuthStore{
		users:    make(map[string]userRecord),
		sessions: make(map[string]string),
	}
}

func (s *memoryAuthStore) createUser(email, password string) error {
	email = normalizeEmail(email)
	if !looksLikeEmail(email) {
		return errors.New("enter a valid email address")
	}
	if len(password) < 8 {
		return errors.New("password must be at least 8 characters")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[email]; exists {
		return errors.New("an account with this email already exists")
	}

	salt, hash, err := hashPassword(password)
	if err != nil {
		return err
	}

	s.users[email] = userRecord{
		Email:        email,
		PasswordSalt: salt,
		PasswordHash: hash,
		CreatedAt:    time.Now(),
	}

	return nil
}

func (s *memoryAuthStore) authenticate(email, password string) bool {
	email = normalizeEmail(email)

	s.mu.RLock()
	user, exists := s.users[email]
	s.mu.RUnlock()
	if !exists {
		return false
	}

	return verifyPassword(password, user.PasswordSalt, user.PasswordHash)
}

func (s *memoryAuthStore) createSession(email string) (string, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}

	token := hex.EncodeToString(tokenBytes)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[token] = normalizeEmail(email)
	return token, nil
}

func (s *memoryAuthStore) lookupSession(token string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	email, ok := s.sessions[token]
	return email, ok
}

func (s *memoryAuthStore) deleteSession(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, token)
}

func (a *webApp) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", a.handleRoot)
	mux.HandleFunc("/signup", a.handleSignup)
	mux.HandleFunc("/login", a.handleLogin)
	mux.HandleFunc("/logout", a.handleLogout)
	mux.HandleFunc("/dashboard", a.requireAuth(a.handleDashboard))
	mux.HandleFunc("/dashboard/import", a.requireAuth(a.handleDashboardImport))
	mux.HandleFunc("/api/report", a.requireAuth(a.handleReportAPI))
	return mux
}

func (a *webApp) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	if email, ok := a.currentUser(r); ok {
		http.Redirect(w, r, "/dashboard?user="+email, http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (a *webApp) handleSignup(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.render(w, "signup", authPageData{
			Title:      "Create Account",
			Heading:    "Create your dashboard account",
			Subheading: "This Go implementation keeps signup data in memory for the current process, matching the in-memory behavior you asked for.",
			Action:     "/signup",
			AltCopy:    "Already have an account?",
			AltLink:    "/login",
			AltLabel:   "Log in",
		})
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			http.Error(w, "invalid form submission", http.StatusBadRequest)
			return
		}

		email := strings.TrimSpace(r.FormValue("email"))
		password := r.FormValue("password")

		if err := a.auth.createUser(email, password); err != nil {
			a.render(w, "signup", authPageData{
				Title:      "Create Account",
				Heading:    "Create your dashboard account",
				Subheading: "Use an email address and a password with at least 8 characters.",
				Error:      err.Error(),
				Email:      email,
				Action:     "/signup",
				AltCopy:    "Already have an account?",
				AltLink:    "/login",
				AltLabel:   "Log in",
			})
			return
		}

		if err := a.loginUser(w, email); err != nil {
			http.Error(w, "failed to create session", http.StatusInternalServerError)
			return
		}

		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *webApp) handleLogin(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.render(w, "login", authPageData{
			Title:      "Login",
			Heading:    "Sign in to the analytics dashboard",
			Subheading: "Your session unlocks the protected charts, report viewer, and JSON import flow.",
			Action:     "/login",
			AltCopy:    "Need an account first?",
			AltLink:    "/signup",
			AltLabel:   "Sign up",
		})
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			http.Error(w, "invalid form submission", http.StatusBadRequest)
			return
		}

		email := strings.TrimSpace(r.FormValue("email"))
		password := r.FormValue("password")
		if !a.auth.authenticate(email, password) {
			a.render(w, "login", authPageData{
				Title:      "Login",
				Heading:    "Sign in to the analytics dashboard",
				Subheading: "Check your email and password, then try again.",
				Error:      "invalid email or password",
				Email:      email,
				Action:     "/login",
				AltCopy:    "Need an account first?",
				AltLink:    "/signup",
				AltLabel:   "Sign up",
			})
			return
		}

		if err := a.loginUser(w, email); err != nil {
			http.Error(w, "failed to create session", http.StatusInternalServerError)
			return
		}

		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *webApp) handleLogout(w http.ResponseWriter, r *http.Request) {
	email, _ := a.currentUser(r)

	switch r.Method {
	case http.MethodGet:
		a.render(w, "logout", authPageData{
			Title:       "Logout",
			Heading:     "Sign out of the dashboard",
			Subheading:  "This clears the current browser session and returns you to the login page.",
			Action:      "/logout",
			IsLogout:    true,
			Message:     "You are currently signed in.",
			CurrentUser: email,
		})
	case http.MethodPost:
		a.clearSession(w, r)
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *webApp) handleDashboard(w http.ResponseWriter, r *http.Request, email string) {
	report, source := a.snapshotReport()
	data := dashboardPageData{
		Title:       "Gostress Analytics Dashboard",
		CurrentUser: email,
		HasReport:   report != nil,
		ReportSource: func() string {
			if source == "" {
				return "No report loaded yet"
			}
			return source
		}(),
		ChartCardTone: "sea",
	}

	if report != nil {
		view := buildHTMLReportView(*report)
		reportJSON, _ := renderJSONReport(*report)
		data.Report = report
		data.ImportedAt = report.StartTime
		data.ReportJSON = string(reportJSON)
		data.SuccessPct = view.SuccessPct
		data.HTTPFailurePct = view.HTTPFailurePct
		data.TransportErrorPct = view.TransportErrorPct
		data.ThroughputPct = view.ThroughputPct
		data.SummaryHeadline = view.SummaryHeadline
		data.SummaryParagraphs = view.SummaryParagraphs
		data.LatencyBars = view.LatencyBars
		data.StatusCodeBars = view.StatusCodeBars
		data.ErrorBars = view.ErrorBars
		data.StatusCodes = view.StatusCodes
		data.ErrorTypes = view.ErrorTypes
		data.ReliabilityNote = view.ReliabilityNote
		data.LatencyNote = view.LatencyNote
		data.ThroughputNote = view.ThroughputNote
	}

	a.render(w, "dashboard", data)
}

func (a *webApp) handleDashboardImport(w http.ResponseWriter, r *http.Request, email string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form submission", http.StatusBadRequest)
		return
	}

	raw := r.FormValue("report_json")
	var report TestReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		http.Error(w, "invalid report JSON", http.StatusBadRequest)
		return
	}

	a.replaceReport(report, fmt.Sprintf("Imported by %s at %s", email, time.Now().Format("2006-01-02 15:04:05")))
	http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
}

func (a *webApp) handleReportAPI(w http.ResponseWriter, r *http.Request, email string) {
	switch r.Method {
	case http.MethodGet:
		report, _ := a.snapshotReport()
		if report == nil {
			http.Error(w, "no report loaded", http.StatusNotFound)
			return
		}

		data, err := renderJSONReport(*report)
		if err != nil {
			http.Error(w, "failed to render report", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	case http.MethodPost:
		defer r.Body.Close()
		var report TestReport
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			http.Error(w, "invalid report JSON", http.StatusBadRequest)
			return
		}
		a.replaceReport(report, fmt.Sprintf("API import by %s at %s", email, time.Now().Format("2006-01-02 15:04:05")))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *webApp) replaceReport(report TestReport, source string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.report = &report
	a.source = source
}

func (a *webApp) snapshotReport() (*TestReport, string) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.report == nil {
		return nil, a.source
	}

	copyReport := *a.report
	return &copyReport, a.source
}

func (a *webApp) requireAuth(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		email, ok := a.currentUser(r)
		if !ok {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		next(w, r, email)
	}
}

func (a *webApp) currentUser(r *http.Request) (string, bool) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || cookie.Value == "" {
		return "", false
	}
	return a.auth.lookupSession(cookie.Value)
}

func (a *webApp) loginUser(w http.ResponseWriter, email string) error {
	token, err := a.auth.createSession(email)
	if err != nil {
		return err
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   86400,
	})

	return nil
}

func (a *webApp) clearSession(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(sessionCookieName)
	if err == nil && cookie.Value != "" {
		a.auth.deleteSession(cookie.Value)
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
}

func (a *webApp) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := webTemplates.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func loadInitialDashboardReport(path string) (*TestReport, string, error) {
	candidates := make([]string, 0, 2)
	if strings.TrimSpace(path) != "" {
		candidates = append(candidates, path)
	} else if _, err := os.Stat("report.json"); err == nil {
		candidates = append(candidates, "report.json")
	}

	for _, candidate := range candidates {
		report, err := loadReportFromFile(candidate)
		if err != nil {
			if path != "" {
				return nil, "", err
			}
			continue
		}
		return &report, candidate, nil
	}

	return nil, "", nil
}

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func looksLikeEmail(email string) bool {
	return strings.Contains(email, "@") && strings.Contains(email, ".")
}

func hashPassword(password string) (string, string, error) {
	saltBytes := make([]byte, 16)
	if _, err := rand.Read(saltBytes); err != nil {
		return "", "", err
	}

	salt := hex.EncodeToString(saltBytes)
	sum := sha256.Sum256([]byte(salt + password))
	return salt, hex.EncodeToString(sum[:]), nil
}

func verifyPassword(password, salt, expected string) bool {
	sum := sha256.Sum256([]byte(salt + password))
	actual := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) == 1
}

const webTemplateSource = `
{{define "shell_start"}}
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{.Title}}</title>
  <style>
    :root {
      --ink: #0f2233;
      --muted: #5c7086;
      --line: #d6dee7;
      --bg: #f4f0e8;
      --panel: #ffffff;
      --sea: #0f766e;
      --sea-soft: #daf0ed;
      --ember: #b45309;
      --ember-soft: #fde6cf;
      --rose: #b42318;
      --rose-soft: #fddfdb;
      --midnight: #11243a;
      --shadow: 0 18px 48px rgba(15, 34, 51, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.10), transparent 28%),
        radial-gradient(circle at top right, rgba(180,83,9,0.08), transparent 24%),
        linear-gradient(180deg, #faf7f1 0%, #f1e8dc 100%);
    }
    a { color: inherit; }
    .container { max-width: 1380px; margin: 0 auto; padding: 28px; }
    .hero {
      background: linear-gradient(135deg, rgba(17,36,58,0.96), rgba(15,118,110,0.92));
      color: #f8fbff;
      border-radius: 28px;
      padding: 28px 30px;
      box-shadow: var(--shadow);
      margin-bottom: 22px;
    }
    .hero h1 { margin: 0 0 8px; font-size: 40px; color: #fff; }
    .hero p { margin: 0; max-width: 860px; color: rgba(248,251,255,0.86); line-height: 1.6; }
    .grid { display: grid; gap: 18px; }
    .auth-grid { grid-template-columns: minmax(0, 420px) minmax(0, 1fr); align-items: stretch; }
    .panel {
      background: rgba(255,255,255,0.95);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 24px;
      box-shadow: var(--shadow);
    }
    .panel h2, .panel h3 { margin-top: 0; color: var(--midnight); }
    .muted { color: var(--muted); }
    .field { margin-bottom: 16px; }
    label { display: block; margin-bottom: 8px; font-size: 14px; color: var(--muted); }
    input, textarea {
      width: 100%;
      border: 1px solid #cbd6e2;
      border-radius: 14px;
      padding: 14px 16px;
      font: inherit;
      background: #fffdf9;
    }
    textarea { min-height: 260px; resize: vertical; font-family: Menlo, Monaco, monospace; font-size: 13px; }
    .btn-row { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
    .btn {
      border: none;
      border-radius: 999px;
      padding: 13px 18px;
      font: inherit;
      cursor: pointer;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }
    .btn-primary { background: linear-gradient(90deg, var(--sea), #11998e); color: #fff; }
    .btn-secondary { background: #fff; border: 1px solid var(--line); color: var(--ink); }
    .error {
      padding: 12px 14px;
      border-radius: 14px;
      background: var(--rose-soft);
      color: var(--rose);
      border: 1px solid #f2b8b1;
      margin-bottom: 14px;
    }
    .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; }
    .kpi {
      padding: 18px;
      border-radius: 20px;
      background: #fff;
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }
    .kpi .value { font-size: 34px; font-weight: bold; margin: 8px 0 6px; }
    .dashboard-grid { display: grid; grid-template-columns: 1.2fr 0.8fr; gap: 18px; }
    .stack {
      width: 100%;
      height: 18px;
      display: flex;
      overflow: hidden;
      border-radius: 999px;
      border: 1px solid #dae3eb;
      background: #eef3f7;
      margin: 14px 0 10px;
    }
    .stack .success { background: linear-gradient(90deg, var(--sea), #4fc3b7); }
    .stack .http { background: linear-gradient(90deg, var(--ember), #f59e0b); }
    .stack .transport { background: linear-gradient(90deg, var(--rose), #f87171); }
    .legend { display: flex; flex-wrap: wrap; gap: 12px; font-size: 14px; color: var(--muted); }
    .legend span::before {
      content: "";
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      margin-right: 8px;
    }
    .legend .success::before { background: var(--sea); }
    .legend .http::before { background: var(--ember); }
    .legend .transport::before { background: var(--rose); }
    .chart {
      display: grid;
      gap: 14px;
    }
    .chart-row {
      display: grid;
      grid-template-columns: 130px 1fr 110px;
      gap: 12px;
      align-items: center;
    }
    .track {
      background: #edf2f7;
      border-radius: 999px;
      border: 1px solid #dde6ef;
      overflow: hidden;
      height: 16px;
    }
    .fill { height: 100%; border-radius: 999px; min-width: 6px; }
    .tone-sea { background: linear-gradient(90deg, var(--sea), #53c5bb); }
    .tone-ember { background: linear-gradient(90deg, var(--ember), #f6b453); }
    .tone-rose { background: linear-gradient(90deg, var(--rose), #ef7f7f); }
    .tone-blue { background: linear-gradient(90deg, #1d4d8f, #6fb5ed); }
    .donut-wrap { display: grid; grid-template-columns: 220px 1fr; gap: 18px; align-items: center; }
    .donut {
      width: 220px;
      height: 220px;
      border-radius: 50%;
      position: relative;
      border: 1px solid var(--line);
      box-shadow: inset 0 0 0 1px rgba(255,255,255,0.6);
    }
    .donut::after {
      content: "";
      position: absolute;
      inset: 28px;
      background: #fffdf9;
      border-radius: 50%;
      box-shadow: inset 0 0 0 1px rgba(214,222,231,0.8);
    }
    .donut-center {
      position: absolute;
      inset: 0;
      display: grid;
      place-items: center;
      z-index: 1;
      text-align: center;
      padding: 42px;
    }
    .donut-center strong { font-size: 34px; display: block; }
    .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
    .prose p { line-height: 1.7; }
    .toolbar {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 18px;
    }
    .meta-strip {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 14px;
    }
    .tag {
      display: inline-flex;
      padding: 7px 11px;
      border-radius: 999px;
      background: rgba(255,255,255,0.82);
      border: 1px solid var(--line);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      background: #fff;
      border-radius: 18px;
      overflow: hidden;
    }
    th, td {
      text-align: left;
      padding: 10px 12px;
      border-bottom: 1px solid #e5ebf1;
    }
    th { background: #f4f7fa; }
    .empty {
      padding: 28px;
      border: 1px dashed #c7d3df;
      border-radius: 20px;
      background: rgba(255,255,255,0.72);
    }
    code {
      background: rgba(15,34,51,0.05);
      border-radius: 8px;
      padding: 2px 6px;
      font-family: Menlo, Monaco, monospace;
      font-size: 12px;
    }
    .admin-shell {
      display: grid;
      grid-template-columns: 280px minmax(0, 1fr);
      gap: 22px;
      align-items: start;
    }
    .sidebar {
      position: sticky;
      top: 22px;
      background: linear-gradient(180deg, rgba(17,36,58,0.98), rgba(12,94,87,0.96));
      color: #f8fbff;
      border-radius: 28px;
      padding: 24px 20px;
      box-shadow: var(--shadow);
      min-height: calc(100vh - 56px);
    }
    .brand {
      display: grid;
      gap: 6px;
      padding-bottom: 18px;
      border-bottom: 1px solid rgba(255,255,255,0.12);
      margin-bottom: 18px;
    }
    .brand-mark {
      width: 52px;
      height: 52px;
      border-radius: 16px;
      display: grid;
      place-items: center;
      background: linear-gradient(135deg, rgba(255,255,255,0.18), rgba(255,255,255,0.06));
      font-weight: bold;
      letter-spacing: 0.08em;
    }
    .brand h1 {
      margin: 0;
      font-size: 24px;
      color: #ffffff;
    }
    .brand p {
      margin: 0;
      color: rgba(248,251,255,0.72);
      font-size: 14px;
      line-height: 1.5;
    }
    .nav-block { margin-top: 18px; }
    .nav-label {
      font-size: 12px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: rgba(248,251,255,0.48);
      margin-bottom: 10px;
    }
    .side-nav {
      display: grid;
      gap: 8px;
    }
    .side-link {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      text-decoration: none;
      padding: 12px 14px;
      border-radius: 16px;
      color: rgba(248,251,255,0.9);
      background: rgba(255,255,255,0.05);
      border: 1px solid rgba(255,255,255,0.06);
      transition: transform 120ms ease, background 120ms ease;
    }
    .side-link:hover {
      transform: translateX(2px);
      background: rgba(255,255,255,0.11);
    }
    .side-link small {
      color: rgba(248,251,255,0.56);
      font-size: 12px;
    }
    .side-card {
      margin-top: 22px;
      padding: 16px;
      border-radius: 18px;
      background: rgba(255,255,255,0.08);
      border: 1px solid rgba(255,255,255,0.08);
    }
    .side-card h3 {
      margin: 0 0 8px;
      color: #fff;
      font-size: 16px;
    }
    .side-card p {
      margin: 0;
      font-size: 14px;
      line-height: 1.6;
      color: rgba(248,251,255,0.72);
    }
    .side-actions {
      display: grid;
      gap: 10px;
      margin-top: 16px;
    }
    .side-actions .btn {
      width: 100%;
      justify-content: space-between;
    }
    .main-stage {
      display: grid;
      gap: 20px;
    }
    .masthead {
      background: linear-gradient(135deg, rgba(255,255,255,0.95), rgba(255,250,244,0.88));
      border: 1px solid rgba(214,222,231,0.9);
      border-radius: 28px;
      box-shadow: var(--shadow);
      padding: 24px 26px;
      position: relative;
      overflow: hidden;
    }
    .masthead::before {
      content: "";
      position: absolute;
      right: -60px;
      top: -60px;
      width: 220px;
      height: 220px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(15,118,110,0.13), transparent 68%);
    }
    .masthead-grid {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 18px;
      align-items: start;
      position: relative;
      z-index: 1;
    }
    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 7px 12px;
      background: var(--sea-soft);
      color: var(--sea);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      margin-bottom: 12px;
    }
    .masthead h1 {
      margin: 0 0 10px;
      font-size: 38px;
      color: var(--midnight);
    }
    .masthead p {
      margin: 0;
      max-width: 760px;
      line-height: 1.75;
      color: var(--muted);
    }
    .masthead-actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .status-strip {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
    }
    .status-card {
      background: rgba(255,255,255,0.95);
      border: 1px solid rgba(214,222,231,0.92);
      border-radius: 20px;
      padding: 18px;
      box-shadow: var(--shadow);
    }
    .status-card .meta {
      color: var(--muted);
      font-size: 13px;
      margin: 0 0 8px;
    }
    .status-card strong {
      display: block;
      font-size: 32px;
      color: var(--midnight);
      margin-bottom: 6px;
    }
    .admin-grid {
      display: grid;
      grid-template-columns: 1.35fr 0.95fr;
      gap: 18px;
    }
    .insight-card {
      background: rgba(255,255,255,0.95);
      border: 1px solid rgba(214,222,231,0.92);
      border-radius: 24px;
      padding: 22px;
      box-shadow: var(--shadow);
    }
    .insight-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 12px;
      margin-bottom: 18px;
    }
    .insight-head h2, .insight-head h3 {
      margin: 0;
    }
    .insight-head p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }
    .insight-badge {
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 12px;
      background: #eef5fb;
      color: #28527a;
      white-space: nowrap;
    }
    .mini-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .mini-stat {
      padding: 16px;
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(250,251,253,1), rgba(243,247,251,0.92));
      border: 1px solid #e1e7ee;
    }
    .mini-stat strong {
      display: block;
      margin-top: 8px;
      font-size: 24px;
      color: var(--midnight);
    }
    .mini-stat span {
      font-size: 13px;
      color: var(--muted);
    }
    .sparkline {
      margin-top: 12px;
      display: flex;
      align-items: end;
      gap: 8px;
      height: 140px;
      padding: 14px 10px 10px;
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(224,244,240,0.72), rgba(255,255,255,0.92));
      border: 1px solid #dde7e5;
    }
    .sparkline .spark-bar {
      flex: 1;
      border-radius: 12px 12px 4px 4px;
      min-height: 10px;
      background: linear-gradient(180deg, #0f766e, #49c5b8);
      box-shadow: inset 0 -10px 16px rgba(255,255,255,0.12);
    }
    .chart-surface {
      padding: 16px;
      border-radius: 20px;
      background: linear-gradient(180deg, rgba(247,250,252,0.98), rgba(255,255,255,0.92));
      border: 1px solid #e0e8ef;
    }
    .section-anchor {
      scroll-margin-top: 24px;
    }
    @media (max-width: 960px) {
      .auth-grid, .dashboard-grid, .two-col, .donut-wrap, .admin-grid, .status-strip, .masthead-grid, .admin-shell { grid-template-columns: 1fr; }
      .chart-row { grid-template-columns: 1fr; }
      .donut { margin: 0 auto; }
      .sidebar {
        position: static;
        min-height: auto;
      }
      .masthead-actions {
        justify-content: flex-start;
      }
    }
  </style>
</head>
<body>
<div class="container">
{{end}}

{{define "shell_end"}}
</div>
</body>
</html>
{{end}}

{{define "signup"}}
{{template "shell_start" .}}
<section class="hero">
  <h1>{{.Heading}}</h1>
  <p>{{.Subheading}}</p>
</section>
<section class="grid auth-grid">
  <div class="panel">
    <h2>Sign Up</h2>
    {{if .Error}}<div class="error">{{.Error}}</div>{{end}}
    <form method="post" action="{{.Action}}">
      <div class="field">
        <label for="email">Email address</label>
        <input id="email" name="email" type="email" value="{{.Email}}" required>
      </div>
      <div class="field">
        <label for="password">Password</label>
        <input id="password" name="password" type="password" minlength="8" required>
      </div>
      <div class="btn-row">
        <button class="btn btn-primary" type="submit">Create Account</button>
        <a class="btn btn-secondary" href="{{.AltLink}}">{{.AltLabel}}</a>
      </div>
    </form>
    <p class="muted">{{.AltCopy}} <a href="{{.AltLink}}">{{.AltLabel}}</a></p>
  </div>
  <div class="panel prose">
    <h2>What You Get</h2>
    <p>The protected web support includes a signup page, login page, logout flow, JSON import, and a professional analytics dashboard layered on top of the current stress-test report format.</p>
    <p>This implementation keeps account records in memory for the lifetime of the Go process so you can move quickly in local development. If you want a literal H2-backed Java companion later, we can add that as a second step without throwing this UI away.</p>
    <p>The dashboard is protected by an authenticated session cookie, and every chart view pulls from the same report JSON model used by the CLI and HTML exporter.</p>
  </div>
</section>
{{template "shell_end" .}}
{{end}}

{{define "login"}}
{{template "shell_start" .}}
<section class="hero">
  <h1>{{.Heading}}</h1>
  <p>{{.Subheading}}</p>
</section>
<section class="grid auth-grid">
  <div class="panel">
    <h2>Login</h2>
    {{if .Error}}<div class="error">{{.Error}}</div>{{end}}
    <form method="post" action="{{.Action}}">
      <div class="field">
        <label for="email">Email address</label>
        <input id="email" name="email" type="email" value="{{.Email}}" required>
      </div>
      <div class="field">
        <label for="password">Password</label>
        <input id="password" name="password" type="password" required>
      </div>
      <div class="btn-row">
        <button class="btn btn-primary" type="submit">Sign In</button>
        <a class="btn btn-secondary" href="{{.AltLink}}">{{.AltLabel}}</a>
      </div>
    </form>
    <p class="muted">{{.AltCopy}} <a href="{{.AltLink}}">{{.AltLabel}}</a></p>
  </div>
  <div class="panel prose">
    <h2>Protected Views</h2>
    <p>Once you log in, the dashboard unlocks reliability visuals, latency distribution charts, status-code analysis, transport error panels, and an inline JSON import tool.</p>
    <p>The same account can sign in repeatedly while the process stays alive, and logout clears the active browser session cleanly.</p>
  </div>
</section>
{{template "shell_end" .}}
{{end}}

{{define "logout"}}
{{template "shell_start" .}}
<section class="hero">
  <h1>{{.Heading}}</h1>
  <p>{{.Subheading}}</p>
</section>
<section class="grid auth-grid">
  <div class="panel">
    <h2>Logout</h2>
    <p class="muted">{{.Message}} {{if .CurrentUser}}Signed in as <strong>{{.CurrentUser}}</strong>.{{end}}</p>
    <form method="post" action="{{.Action}}">
      <div class="btn-row">
        <button class="btn btn-primary" type="submit">Log Out</button>
        <a class="btn btn-secondary" href="/dashboard">Back to Dashboard</a>
      </div>
    </form>
  </div>
  <div class="panel prose">
    <h2>Session Handling</h2>
    <p>Logging out deletes the current session cookie and removes the in-memory session mapping from the running process, which keeps the protected dashboard inaccessible until the next login.</p>
  </div>
</section>
{{template "shell_end" .}}
{{end}}

{{define "dashboard"}}
{{template "shell_start" .}}
<div class="admin-shell">
  <aside class="sidebar">
    <div class="brand">
      <div class="brand-mark">GS</div>
      <h1>Gostress</h1>
      <p>Operational analytics console for protected report review, imports, and performance storytelling.</p>
    </div>

    <div class="nav-block">
      <div class="nav-label">Navigation</div>
      <nav class="side-nav">
        <a class="side-link" href="#overview"><span>Overview</span><small>KPI cards</small></a>
        <a class="side-link" href="#reliability"><span>Reliability</span><small>Success mix</small></a>
        <a class="side-link" href="#latency"><span>Latency</span><small>Distribution</small></a>
        <a class="side-link" href="#status-codes"><span>Status Codes</span><small>HTTP map</small></a>
        <a class="side-link" href="#data-ops"><span>Data Ops</span><small>JSON import</small></a>
      </nav>
    </div>

    <div class="nav-block">
      <div class="nav-label">Context</div>
      <div class="side-card">
        <h3>Signed In</h3>
        <p>{{.CurrentUser}}</p>
      </div>
      <div class="side-card">
        <h3>Report Source</h3>
        <p>{{.ReportSource}}</p>
      </div>
      <div class="side-card">
        <h3>Imported At</h3>
        <p>{{defaultText .ImportedAt "(not available)"}}</p>
      </div>
    </div>

    <div class="side-actions">
      <a class="btn btn-secondary" href="/api/report">Open JSON API</a>
      <a class="btn btn-secondary" href="/logout">Logout</a>
    </div>
  </aside>

  <main class="main-stage">
    <section class="masthead">
      <div class="masthead-grid">
        <div>
          <span class="eyebrow">Admin Dashboard</span>
          <h1>Gostress Analytics Dashboard</h1>
          <p>{{if .HasReport}}{{.SummaryHeadline}}{{else}}Sign-in protection is active. Load or paste report JSON to populate the dashboard charts, executive summary, and operational scorecards.{{end}}</p>
        </div>
        <div class="masthead-actions">
          <span class="tag">Protected session</span>
          <span class="tag">{{if .HasReport}}Live report loaded{{else}}Waiting for report{{end}}</span>
        </div>
      </div>
    </section>

    {{if .HasReport}}
    <section id="overview" class="section-anchor status-strip">
      <div class="status-card">
        <div class="meta">Total Requests</div>
        <strong>{{.Report.TotalRequests}}</strong>
        <div class="muted">Across {{.Report.ActualDuration}}</div>
      </div>
      <div class="status-card">
        <div class="meta">Success Rate</div>
        <strong>{{printf "%.2f%%" .Report.SuccessRate}}</strong>
        <div class="muted">{{.Report.SuccessRequests}} successful responses</div>
      </div>
      <div class="status-card">
        <div class="meta">Request Throughput</div>
        <strong>{{formatFloat .Report.RequestsPerSecond}}</strong>
        <div class="muted">Requests per second observed</div>
      </div>
      <div class="status-card">
        <div class="meta">Average Latency</div>
        <strong>{{.Report.AverageLatency}}</strong>
        <div class="muted">p95 {{.Report.P95Latency}} • p99 {{.Report.P99Latency}}</div>
      </div>
    </section>

    <section class="admin-grid">
      <section class="insight-card">
        <div class="insight-head">
          <div>
            <h2>Executive Summary</h2>
            <p>High-signal narrative for engineering, SRE, and product review.</p>
          </div>
          <span class="insight-badge">Narrative Analysis</span>
        </div>
        <div class="prose">
          {{range .SummaryParagraphs}}<p>{{.}}</p>{{end}}
        </div>
        <div class="sparkline" aria-hidden="true">
          {{range .LatencyBars}}
          <div class="spark-bar" style="height: {{printf "%.2f" .Width}}%;"></div>
          {{end}}
          {{range .StatusCodeBars}}
          <div class="spark-bar" style="height: {{printf "%.2f" .Width}}%; background: linear-gradient(180deg, #245b93, #80baf1);"></div>
          {{end}}
        </div>
      </section>

      <section id="reliability" class="insight-card section-anchor">
        <div class="insight-head">
          <div>
            <h2>Reliability Center</h2>
            <p>Failure composition, transport pressure, and response health.</p>
          </div>
          <span class="insight-badge">Health Mix</span>
        </div>
        <div class="donut-wrap">
          <div class="donut" style="background: conic-gradient(var(--sea) 0 {{printf "%.2f" .SuccessPct}}%, var(--ember) {{printf "%.2f" .SuccessPct}}% {{printf "%.2f" (add .SuccessPct .HTTPFailurePct)}}%, var(--rose) {{printf "%.2f" (add .SuccessPct .HTTPFailurePct)}}% 100%);">
            <div class="donut-center">
              <div>
                <strong>{{printf "%.2f%%" .Report.SuccessRate}}</strong>
                <span class="muted">success</span>
              </div>
            </div>
          </div>
          <div>
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
        </div>
        <div class="mini-grid" style="margin-top:16px;">
          <div class="mini-stat">
            <span>HTTP failures</span>
            <strong>{{.Report.HTTPFailures}}</strong>
          </div>
          <div class="mini-stat">
            <span>Transport errors</span>
            <strong>{{.Report.TransportErrors}}</strong>
          </div>
          <div class="mini-stat">
            <span>Success policy</span>
            <strong><code>{{.Report.SuccessStatusSpec}}</code></strong>
          </div>
          <div class="mini-stat">
            <span>Rate shaping</span>
            <strong>{{if gt .Report.RateLimitRPS 0.0}}{{formatFloat .Report.RateLimitRPS}} RPS{{else}}Unlimited{{end}}</strong>
          </div>
        </div>
      </section>
    </section>

    <section class="admin-grid">
      <section id="latency" class="insight-card section-anchor">
        <div class="insight-head">
          <div>
            <h2>Latency Observatory</h2>
            <p>Percentile spread plotted against the worst observed request.</p>
          </div>
          <span class="insight-badge">Tail Behavior</span>
        </div>
        <div class="chart-surface">
          <div class="chart">
            {{range .LatencyBars}}
            <div class="chart-row">
              <div><strong>{{.Label}}</strong></div>
              <div class="track"><div class="fill {{.Tone}}" style="width: {{printf "%.2f" .Width}}%"></div></div>
              <div class="muted">{{.Value}}</div>
            </div>
            {{end}}
          </div>
        </div>
        <p class="muted" style="margin-top:16px;">{{.LatencyNote}}</p>
      </section>

      <section class="insight-card">
        <div class="insight-head">
          <div>
            <h2>Performance Brief</h2>
            <p>Throughput, transfer volume, and operating context at a glance.</p>
          </div>
          <span class="insight-badge">Ops Notes</span>
        </div>
        <div class="mini-grid">
          <div class="mini-stat">
            <span>Total data moved</span>
            <strong>{{formatFloat .Report.TotalDataMB}} MB</strong>
          </div>
          <div class="mini-stat">
            <span>Average throughput</span>
            <strong>{{formatFloat .Report.AvgThroughputMB}} MB/s</strong>
          </div>
          <div class="mini-stat">
            <span>RPS utilization</span>
            <strong>{{if gt .Report.ThroughputPct 0.0}}{{formatFloat .Report.ThroughputPct}}%{{else}}N/A{{end}}</strong>
          </div>
          <div class="mini-stat">
            <span>Configured duration</span>
            <strong>{{.Report.Duration}}</strong>
          </div>
          <div class="mini-stat">
            <span>Actual duration</span>
            <strong>{{.Report.ActualDuration}}</strong>
          </div>
        </div>
        <div class="prose" style="margin-top:16px;">
          <p>{{.ThroughputNote}}</p>
          <p><strong>Source:</strong> {{.ReportSource}}</p>
          <p><strong>Imported at:</strong> {{defaultText .ImportedAt "(not available)"}}</p>
        </div>
      </section>
    </section>

    <section class="admin-grid">
      <section id="status-codes" class="insight-card section-anchor">
        <div class="insight-head">
          <div>
            <h2>Status Code Distribution</h2>
            <p>Professional breakdown of the response mix returned by the target.</p>
          </div>
          <span class="insight-badge">HTTP Map</span>
        </div>
        <div class="chart-surface">
          <div class="chart">
            {{range .StatusCodeBars}}
            <div class="chart-row">
              <div><strong>{{.Label}}</strong></div>
              <div class="track"><div class="fill tone-sea" style="width: {{printf "%.2f" .Width}}%"></div></div>
              <div class="muted">{{.Value}}</div>
            </div>
            {{else}}
            <div class="empty">No HTTP response data is available yet.</div>
            {{end}}
          </div>
        </div>
        <table style="margin-top:18px;">
          <thead><tr><th>Status</th><th>Count</th></tr></thead>
          <tbody>
            {{range .StatusCodes}}
            <tr><td>{{.Key}}</td><td>{{.Value}}</td></tr>
            {{else}}
            <tr><td colspan="2">No status code data loaded</td></tr>
            {{end}}
          </tbody>
        </table>
      </section>

      <section class="insight-card">
        <div class="insight-head">
          <div>
            <h2>Transport Error Distribution</h2>
            <p>Network-level failures isolated from application response codes.</p>
          </div>
          <span class="insight-badge">Infra Signals</span>
        </div>
        <div class="chart-surface">
          <div class="chart">
            {{range .ErrorBars}}
            <div class="chart-row">
              <div><strong>{{.Label}}</strong></div>
              <div class="track"><div class="fill tone-rose" style="width: {{printf "%.2f" .Width}}%"></div></div>
              <div class="muted">{{.Value}}</div>
            </div>
            {{else}}
            <div class="empty">No transport errors were recorded for this report.</div>
            {{end}}
          </div>
        </div>
        <table style="margin-top:18px;">
          <thead><tr><th>Error</th><th>Count</th></tr></thead>
          <tbody>
            {{range .ErrorTypes}}
            <tr><td>{{.Key}}</td><td>{{.Value}}</td></tr>
            {{else}}
            <tr><td colspan="2">No transport errors loaded</td></tr>
            {{end}}
          </tbody>
        </table>
      </section>
    </section>

    <section id="data-ops" class="insight-card section-anchor">
      <div class="insight-head">
        <div>
          <h2>Data Operations</h2>
          <p>Paste raw report JSON here to refresh the charts, summary, and tables without leaving the dashboard.</p>
        </div>
        <span class="insight-badge">Import Flow</span>
      </div>
      <form method="post" action="/dashboard/import">
        <div class="field">
          <label for="report_json">Report JSON</label>
          <textarea id="report_json" name="report_json">{{.ReportJSON}}</textarea>
        </div>
        <div class="btn-row">
          <button class="btn btn-primary" type="submit">Update Dashboard</button>
          <a class="btn btn-secondary" href="/api/report">Inspect JSON API</a>
        </div>
      </form>
    </section>
    {{else}}
    <section class="masthead">
      <div class="masthead-grid">
        <div>
          <span class="eyebrow">Admin Dashboard</span>
          <h1>Gostress Analytics Dashboard</h1>
          <p>Sign-in protection is active. Load a report to unlock the admin-style KPI strip, reliability center, latency observatory, and import workflow.</p>
        </div>
      </div>
    </section>

    <section class="insight-card empty">
      <h2>No report loaded yet</h2>
      <p>Start the web app with <code>--dashboard-report report.json</code>, or paste report JSON below once you have a report available.</p>
    </section>

    <section id="data-ops" class="insight-card section-anchor">
      <div class="insight-head">
        <div>
          <h2>Import Report JSON</h2>
          <p>Paste a full report payload to populate the dashboard immediately.</p>
        </div>
        <span class="insight-badge">Import Flow</span>
      </div>
      <form method="post" action="/dashboard/import">
        <div class="field">
          <label for="report_json">Report JSON</label>
          <textarea id="report_json" name="report_json" placeholder='{"start_time":"2026-04-21 12:00:00", ...}'></textarea>
        </div>
        <div class="btn-row">
          <button class="btn btn-primary" type="submit">Load Dashboard</button>
        </div>
      </form>
    </section>
    {{end}}
  </main>
</div>
{{template "shell_end" .}}
{{end}}
`
