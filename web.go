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
		Title:       "Dashboard",
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
    .container { max-width: 1220px; margin: 0 auto; padding: 28px; }
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
    @media (max-width: 960px) {
      .auth-grid, .dashboard-grid, .two-col, .donut-wrap { grid-template-columns: 1fr; }
      .chart-row { grid-template-columns: 1fr; }
      .donut { margin: 0 auto; }
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
<section class="hero">
  <h1>Protected Analytics Dashboard</h1>
  <p>{{if .HasReport}}{{.SummaryHeadline}}{{else}}Sign-in protection is active. Load or paste report JSON to populate the dashboard charts and executive summary.{{end}}</p>
</section>

<div class="toolbar">
  <div class="meta-strip">
    <span class="tag">Signed in as {{.CurrentUser}}</span>
    <span class="tag">Source: {{.ReportSource}}</span>
    <span class="tag">Imported at: {{defaultText .ImportedAt "(not available)"}}</span>
  </div>
  <div class="btn-row">
    <a class="btn btn-secondary" href="/api/report">JSON API</a>
    <a class="btn btn-secondary" href="/logout">Logout</a>
  </div>
</div>

{{if .HasReport}}
<section class="kpi-grid">
  <div class="kpi">
    <div class="muted">Total requests</div>
    <div class="value">{{.Report.TotalRequests}}</div>
    <div class="muted">Observed over {{.Report.ActualDuration}}</div>
  </div>
  <div class="kpi">
    <div class="muted">Success rate</div>
    <div class="value">{{printf "%.2f%%" .Report.SuccessRate}}</div>
    <div class="muted">{{.Report.SuccessRequests}} successful requests</div>
  </div>
  <div class="kpi">
    <div class="muted">Requests per second</div>
    <div class="value">{{formatFloat .Report.RequestsPerSecond}}</div>
    <div class="muted">Throughput at application edge</div>
  </div>
  <div class="kpi">
    <div class="muted">Average latency</div>
    <div class="value">{{.Report.AverageLatency}}</div>
    <div class="muted">p95 {{.Report.P95Latency}} • p99 {{.Report.P99Latency}}</div>
  </div>
</section>

<section class="grid dashboard-grid" style="margin-top:18px;">
  <div class="panel">
    <h2>Executive Summary</h2>
    <div class="prose">
      {{range .SummaryParagraphs}}<p>{{.}}</p>{{end}}
    </div>
  </div>
  <div class="panel">
    <h2>Reliability Donut</h2>
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
  </div>
</section>

<section class="grid two-col" style="margin-top:18px;">
  <div class="panel">
    <h2>Latency Ladder</h2>
    <div class="chart">
      {{range .LatencyBars}}
      <div class="chart-row">
        <div><strong>{{.Label}}</strong></div>
        <div class="track"><div class="fill {{.Tone}}" style="width: {{printf "%.2f" .Width}}%"></div></div>
        <div class="muted">{{.Value}}</div>
      </div>
      {{end}}
    </div>
    <p class="muted">{{.LatencyNote}}</p>
  </div>
  <div class="panel">
    <h2>Performance Notes</h2>
    <div class="prose">
      <p>{{.ThroughputNote}}</p>
      <p><strong>Total data moved:</strong> {{formatFloat .Report.TotalDataMB}} MB</p>
      <p><strong>Average throughput:</strong> {{formatFloat .Report.AvgThroughputMB}} MB/s</p>
      <p><strong>Success status policy:</strong> <code>{{.Report.SuccessStatusSpec}}</code></p>
      <p><strong>Rate limit:</strong> {{if gt .Report.RateLimitRPS 0.0}}{{formatFloat .Report.RateLimitRPS}} RPS{{else}}unlimited{{end}}</p>
    </div>
  </div>
</section>

<section class="grid two-col" style="margin-top:18px;">
  <div class="panel">
    <h2>Status Code Distribution</h2>
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
  <div class="panel">
    <h2>Transport Error Distribution</h2>
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
</section>

<section class="grid two-col" style="margin-top:18px;">
  <div class="panel">
    <h2>Status Table</h2>
    <table>
      <thead><tr><th>Status</th><th>Count</th></tr></thead>
      <tbody>
        {{range .StatusCodes}}
        <tr><td>{{.Key}}</td><td>{{.Value}}</td></tr>
        {{else}}
        <tr><td colspan="2">No status code data loaded</td></tr>
        {{end}}
      </tbody>
    </table>
  </div>
  <div class="panel">
    <h2>Transport Error Table</h2>
    <table>
      <thead><tr><th>Error</th><th>Count</th></tr></thead>
      <tbody>
        {{range .ErrorTypes}}
        <tr><td>{{.Key}}</td><td>{{.Value}}</td></tr>
        {{else}}
        <tr><td colspan="2">No transport errors loaded</td></tr>
        {{end}}
      </tbody>
    </table>
  </div>
</section>

<section class="panel" style="margin-top:18px;">
  <h2>Import Report JSON</h2>
  <p class="muted">Paste a full JSON report here to update the protected dashboard immediately, or use the authenticated API endpoint at <code>/api/report</code>.</p>
  <form method="post" action="/dashboard/import">
    <div class="field">
      <label for="report_json">Report JSON</label>
      <textarea id="report_json" name="report_json">{{.ReportJSON}}</textarea>
    </div>
    <div class="btn-row">
      <button class="btn btn-primary" type="submit">Update Dashboard</button>
    </div>
  </form>
</section>
{{else}}
<section class="panel empty">
  <h2>No report loaded yet</h2>
  <p>Start the web app with <code>--dashboard-report report.json</code>, or paste report JSON into the import form once you have a report available.</p>
</section>

<section class="panel" style="margin-top:18px;">
  <h2>Import Report JSON</h2>
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
{{template "shell_end" .}}
{{end}}
`
