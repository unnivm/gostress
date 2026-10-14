package main

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	liveTimelineCap    = 3600
	liveTimelineWindow = 300
	liveHistCount      = 64
	liveHistMinNs      = 1e3
	liveHistMaxNs      = 1e10
	liveHistLogFactor  = 0.251845224
)

type livePoint struct {
	T            int64   `json:"t"`
	Requests     int64   `json:"requests"`
	SuccessRate  float64 `json:"success_rate"`
	ErrorRate    float64 `json:"error_rate"`
	AvgLatencyMs float64 `json:"average_latency_ms"`
}

type liveSnapshot struct {
	Running           bool        `json:"running"`
	Finished          bool        `json:"finished"`
	Elapsed           float64     `json:"elapsed_seconds"`
	Remaining         float64     `json:"remaining_seconds"`
	Percent           float64     `json:"percent"`
	Concurrency       int         `json:"concurrency"`
	TargetConcurrency int         `json:"target_concurrency"`
	TotalRequests     int64       `json:"total_requests"`
	SuccessRequests   int64       `json:"success_requests"`
	FailedRequests    int64       `json:"failed_requests"`
	TransportErrors   int64       `json:"transport_errors"`
	HTTPFailures      int64       `json:"http_failures"`
	RequestsPerSecond float64     `json:"requests_per_second"`
	SuccessRate       float64     `json:"success_rate"`
	ErrorRate         float64     `json:"error_rate"`
	TotalDataMB       float64     `json:"total_data_mb"`
	AvgThroughputMBS  float64     `json:"avg_throughput_mb_s"`
	ThroughputPct     float64     `json:"throughput_pct"`
	ThroughputCeiling float64     `json:"throughput_ceiling_rps"`
	MinLatency        string      `json:"min_latency"`
	MaxLatency        string      `json:"max_latency"`
	AvgLatency        string      `json:"average_latency"`
	P50Latency        string      `json:"p50_latency"`
	P95Latency        string      `json:"p95_latency"`
	P99Latency        string      `json:"p99_latency"`
	URL               string      `json:"url"`
	Method            string      `json:"method"`
	Duration          string      `json:"duration"`
	RateLimitRPS      float64     `json:"rate_limit_rps"`
	RampDuration      string      `json:"ramp_duration"`
	Timeline          []livePoint `json:"timeline"`
}

type secondBucket struct {
	t         atomic.Int64
	requests  atomic.Int64
	success   atomic.Int64
	failed    atomic.Int64
	transport atomic.Int64
	httpFail  atomic.Int64
	bytes     atomic.Int64
	latencyNs atomic.Int64
	latencyN  atomic.Int64
}

func resetSecondBucket(b *secondBucket, t int64) {
	b.t.Store(t)
	b.requests.Store(0)
	b.success.Store(0)
	b.failed.Store(0)
	b.transport.Store(0)
	b.httpFail.Store(0)
	b.bytes.Store(0)
	b.latencyNs.Store(0)
	b.latencyN.Store(0)
}

type latencyHistogram struct {
	buckets [liveHistCount]atomic.Int64
	count   atomic.Int64
	sumNs   atomic.Int64
	minNs   atomic.Int64
	maxNs   atomic.Int64
}

func newLatencyHistogram() *latencyHistogram {
	h := &latencyHistogram{}
	h.minNs.Store(math.MaxInt64)
	return h
}

func bucketIndex(ns int64) int {
	if ns <= liveHistMinNs {
		return 0
	}
	if ns >= liveHistMaxNs {
		return liveHistCount - 1
	}
	idx := int(math.Log(float64(ns)/liveHistMinNs) / liveHistLogFactor)
	if idx < 0 {
		idx = 0
	}
	if idx >= liveHistCount {
		idx = liveHistCount - 1
	}
	return idx
}

func bucketValue(idx int) int64 {
	return int64(liveHistMinNs * math.Pow(10, float64(idx)*liveHistLogFactor/math.Log(10)))
}

func (h *latencyHistogram) add(d time.Duration) {
	ns := int64(d)
	if ns <= 0 {
		ns = 1
	}
	if h.count.Load() == 0 || ns < h.minNs.Load() {
		h.minNs.Store(ns)
	}
	if ns > h.maxNs.Load() {
		h.maxNs.Store(ns)
	}
	h.count.Add(1)
	h.sumNs.Add(ns)
	h.buckets[bucketIndex(ns)].Add(1)
}

func (h *latencyHistogram) percentile(p float64) time.Duration {
	count := h.count.Load()
	if count == 0 {
		return 0
	}
	target := float64(count) * p
	var cumulative int64
	for i := range h.buckets {
		b := h.buckets[i].Load()
		cumulative += b
		if float64(cumulative) >= target {
			if b == 0 {
				return time.Duration(bucketValue(i))
			}
			below := float64(cumulative - b)
			frac := (target - below) / float64(b)
			lo := float64(bucketValue(i))
			if i >= liveHistCount-1 {
				return time.Duration(h.maxNs.Load())
			}
			hi := float64(bucketValue(i + 1))
			return time.Duration(lo + frac*(hi-lo))
		}
	}
	return time.Duration(h.maxNs.Load())
}

func (h *latencyHistogram) stats() (min, avg, p50, p95, p99, max time.Duration) {
	count := h.count.Load()
	if count == 0 {
		return
	}
	min = time.Duration(h.minNs.Load())
	max = time.Duration(h.maxNs.Load())
	avg = time.Duration(h.sumNs.Load() / count)
	p50 = h.percentile(0.5)
	p95 = h.percentile(0.95)
	p99 = h.percentile(0.99)
	return
}

type liveReporter struct {
	ringMu    sync.Mutex
	cfg       Config
	start     time.Time
	deadline  time.Time
	ramp      *concurrencyRamp
	hist      latencyHistogram
	seconds   []secondBucket
	ringNext  int
	ringFull  bool
	curSecond atomic.Int64
	total     atomic.Int64
	success   atomic.Int64
	failed    atomic.Int64
	transport atomic.Int64
	httpFail  atomic.Int64
	bytes     atomic.Int64
	running   atomic.Bool
	finished  atomic.Bool
	endNS     atomic.Int64
}

func newLiveReporter(cfg Config, start, deadline time.Time, ramp *concurrencyRamp) *liveReporter {
	r := &liveReporter{
		cfg:      cfg,
		start:    start,
		deadline: deadline,
		ramp:     ramp,
		seconds:  make([]secondBucket, liveTimelineCap),
	}
	r.curSecond.Store(-1)
	r.running.Store(true)
	return r
}

func (r *liveReporter) sample(res Result) {
	idx := int64(time.Since(r.start) / time.Second)
	if idx != r.curSecond.Load() {
		r.advanceLocked(idx)
	}
	b := &r.seconds[r.ringNext]
	b.requests.Add(1)
	r.total.Add(1)
	if res.Error != nil {
		b.transport.Add(1)
		b.failed.Add(1)
		r.transport.Add(1)
		r.failed.Add(1)
		return
	}

	b.bytes.Add(int64(res.BytesRead))
	r.bytes.Add(int64(res.BytesRead))
	if statusMatches(res.StatusCode, r.cfg.SuccessStatus) {
		b.success.Add(1)
		r.success.Add(1)
		b.latencyNs.Add(int64(res.Latency))
		b.latencyN.Add(1)
		r.hist.add(res.Latency)
		return
	}

	b.httpFail.Add(1)
	b.failed.Add(1)
	r.httpFail.Add(1)
	r.failed.Add(1)
}

func (r *liveReporter) advanceLocked(idx int64) {
	r.ringMu.Lock()
	defer r.ringMu.Unlock()

	cur := r.curSecond.Load()
	if idx-cur > liveTimelineCap {
		r.ringNext = 0
		r.ringFull = true
		r.curSecond.Store(idx)
		resetSecondBucket(&r.seconds[0], idx)
		return
	}
	for cur < idx {
		r.ringNext = (r.ringNext + 1) % liveTimelineCap
		if r.ringNext == 0 {
			r.ringFull = true
		}
		resetSecondBucket(&r.seconds[r.ringNext], idx)
		cur++
	}
	r.curSecond.Store(idx)
}

func (r *liveReporter) finish() {
	r.running.Store(false)
	r.finished.Store(true)
	r.endNS.Store(time.Now().UnixNano())
}

func (r *liveReporter) endTime() time.Time {
	if ns := r.endNS.Load(); ns > 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

func (r *liveReporter) snapshot() liveSnapshot {
	finished := r.finished.Load()
	end := r.endTime()

	elapsed := time.Since(r.start)
	if finished && !end.IsZero() {
		elapsed = end.Sub(r.start)
	}
	if elapsed < 0 {
		elapsed = 0
	}

	snap := liveSnapshot{
		Running:           r.running.Load(),
		Finished:          finished,
		Elapsed:           elapsed.Seconds(),
		Concurrency:       r.ramp.CurrentLimit(),
		TargetConcurrency: r.cfg.Concurrency,
		TotalRequests:     r.total.Load(),
		SuccessRequests:   r.success.Load(),
		FailedRequests:    r.failed.Load(),
		TransportErrors:   r.transport.Load(),
		HTTPFailures:      r.httpFail.Load(),
		TotalDataMB:       float64(r.bytes.Load()) / (1024 * 1024),
		URL:               r.cfg.URL,
		Method:            r.cfg.Method,
		Duration:          r.cfg.Duration.String(),
		RateLimitRPS:      r.cfg.RequestsPerSecond,
		RampDuration:      formatRampDuration(r.cfg.RampDuration, r.cfg.Concurrency),
	}

	durationSec := r.cfg.Duration.Seconds()
	if finished && !end.IsZero() {
		remaining := r.deadline.Sub(end)
		if remaining < 0 {
			remaining = 0
		}
		snap.Remaining = remaining.Seconds()
	} else {
		snap.Remaining = time.Until(r.deadline).Seconds()
		if snap.Remaining < 0 {
			snap.Remaining = 0
		}
	}
	if finished {
		snap.Percent = 100
	} else if durationSec > 0 {
		pct := elapsed.Seconds() / durationSec * 100
		if pct > 100 {
			pct = 100
		}
		snap.Percent = pct
	}
	if elapsed.Seconds() > 0 {
		snap.RequestsPerSecond = float64(r.total.Load()) / elapsed.Seconds()
		snap.AvgThroughputMBS = snap.TotalDataMB / elapsed.Seconds()
		snap.ThroughputPct, snap.ThroughputCeiling = r.throughputPct(snap.RequestsPerSecond)
	}
	snap.SuccessRate, snap.ErrorRate = computeRates(int(r.success.Load()), int(r.failed.Load()), int(r.total.Load()))

	min, avg, p50, p95, p99, max := r.hist.stats()
	snap.MinLatency = formatDuration(min)
	snap.AvgLatency = formatDuration(avg)
	snap.P50Latency = formatDuration(p50)
	snap.P95Latency = formatDuration(p95)
	snap.P99Latency = formatDuration(p99)
	snap.MaxLatency = formatDuration(max)

	snap.Timeline = r.timelineLocked()
	return snap
}

func (r *liveReporter) throughputPct(rps float64) (float64, float64) {
	if r.cfg.RequestsPerSecond > 0 {
		return computeThroughputPct(rps, r.cfg.RequestsPerSecond), r.cfg.RequestsPerSecond
	}
	_, avg, _, _, _, _ := r.hist.stats()
	if avg <= 0 {
		return 0, 0
	}
	ceiling := float64(r.ramp.CurrentLimit()) / (float64(avg) / float64(time.Second))
	if ceiling <= 0 {
		return 0, 0
	}
	return rps / ceiling * 100, ceiling
}

func (r *liveReporter) timelineLocked() []livePoint {
	r.ringMu.Lock()
	defer r.ringMu.Unlock()

	start := 0
	if r.ringFull {
		start = r.ringNext + 1
	}
	collected := make([]livePoint, 0, liveTimelineCap)
	for i := 0; i < liveTimelineCap; i++ {
		b := &r.seconds[(start+i)%liveTimelineCap]
		requests := b.requests.Load()
		if requests == 0 {
			continue
		}
		pt := livePoint{T: b.t.Load(), Requests: requests}
		pt.SuccessRate = float64(b.success.Load()) / float64(requests) * 100
		pt.ErrorRate = float64(b.failed.Load()) / float64(requests) * 100
		if n := b.latencyN.Load(); n > 0 {
			pt.AvgLatencyMs = float64(b.latencyNs.Load()/n) / 1e6
		}
		collected = append(collected, pt)
	}
	if len(collected) > liveTimelineWindow {
		collected = collected[len(collected)-liveTimelineWindow:]
	}
	return collected
}

func (r *liveReporter) serveJSON(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(r.snapshot())
}

func serveLivePage(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, livePageHTML)
}

func startLiveServer(addr string, live *liveReporter) (string, *http.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", serveLivePage)
	mux.HandleFunc("/live", serveLivePage)
	mux.HandleFunc("/api/live", live.serveJSON)
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = srv.Serve(ln)
	}()
	return ln.Addr().String(), srv, nil
}

func stopLiveServer(srv *http.Server) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func liveURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr + "/live"
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return "http://localhost:" + port + "/live"
	}
	return "http://" + net.JoinHostPort(host, port) + "/live"
}

const livePageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>gostress · live</title>
<style>
:root{
  --bg:#0d1620; --panel:#13202c; --line:#22313f;
  --text:#e8eef4; --muted:#8aa0b4;
  --sea:#34d399; --ember:#fbbf24; --rose:#f87171;
  --font:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  --mono:ui-monospace,SFMono-Regular,Menlo,monospace;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:var(--font)}
.wrap{max-width:1080px;margin:0 auto;padding:24px}
.head{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;margin-bottom:20px}
.title{font-size:22px;font-weight:700;letter-spacing:.04em}
.title b{color:var(--sea)}
.sub{color:var(--muted);font-size:13px;margin-top:6px;word-break:break-all;font-family:var(--mono)}
.state{display:inline-flex;align-items:center;gap:8px;border-radius:999px;padding:8px 14px;font-size:13px;background:var(--panel);border:1px solid var(--line)}
.dot{width:9px;height:9px;border-radius:50%;background:var(--sea);animation:pulse 1.4s infinite}
@keyframes pulse{50%{opacity:.35}}
.grid{display:grid;grid-template-columns:repeat(6,1fr);gap:14px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:16px}
.card .k{color:var(--muted);font-size:12px;letter-spacing:.08em;text-transform:uppercase}
.card .v{font-size:26px;font-weight:700;margin-top:8px;font-variant-numeric:tabular-nums}
.card .v small{font-size:14px;color:var(--muted);font-weight:500}
.row{display:grid;grid-template-columns:2fr 1.2fr;gap:14px;margin-top:14px}
.track{height:12px;border-radius:999px;background:#1a2836;overflow:hidden;margin-top:12px}
.bar{height:100%;width:0;border-radius:999px;background:linear-gradient(90deg,var(--sea),#0ea5e9);transition:width .8s ease}
.donut-wrap{display:flex;align-items:center;justify-content:center;flex-direction:column;gap:12px}
.donut{width:120px;height:120px;transform:rotate(-90deg)}
.donut circle{fill:none;stroke-width:14;stroke-linecap:round}
.legend{display:grid;gap:6px;font-size:13px;color:var(--muted)}
.legend span::before{content:"";display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:8px}
.legend .ok::before{background:var(--sea)}
.legend .http::before{background:var(--ember)}
.legend .tr::before{background:var(--rose)}
.chart-card{margin-top:14px}
canvas{width:100%;height:260px;display:block}
.banner{display:none;margin-top:14px;padding:14px 16px;border-radius:14px;background:#0b3b2e;border:1px solid #1d6f52;color:#b9f5dc;font-size:14px}
@media(max-width:820px){.grid{grid-template-columns:repeat(3,1fr)}.row{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="wrap">
  <div class="head">
    <div>
      <div class="title">gostress <b>·</b> live dashboard</div>
      <div class="sub" id="meta">connecting…</div>
    </div>
    <div class="state"><span class="dot"></span><span id="status">Running</span></div>
  </div>

  <div class="grid">
    <div class="card"><div class="k">Requests / s</div><div class="v" id="rps">–</div></div>
    <div class="card"><div class="k">Success</div><div class="v" id="succ">–</div></div>
    <div class="card"><div class="k">p95 latency</div><div class="v" id="p95">–</div></div>
    <div class="card"><div class="k">Elapsed</div><div class="v" id="el">–</div></div>
    <div class="card"><div class="k">Concurrency</div><div class="v" id="conc">–</div></div>
    <div class="card"><div class="k">Throughput</div><div class="v" id="tp">–</div></div>
  </div>

  <div class="row">
    <div class="card">
      <div class="k">Test progress</div>
      <div class="v" id="pctText">0%</div>
      <div class="track"><div class="bar" id="bar"></div></div>
      <div style="margin-top:12px;color:var(--muted);font-size:12px">total requests: <b style="color:var(--text)" id="total">0</b> &nbsp;·&nbsp; transport errors: <b style="color:var(--text)" id="err">0</b></div>
    </div>
    <div class="card">
      <div class="k">Reliability</div>
      <div class="donut-wrap">
        <svg class="donut" viewBox="0 0 120 120">
          <circle cx="60" cy="60" r="52" stroke="#1a2836" stroke-width="14"></circle>
          <circle id="arcOk" cx="60" cy="60" r="52" stroke="var(--sea)" stroke-width="14" stroke-dasharray="0 327"></circle>
          <circle id="arcHttp" cx="60" cy="60" r="52" stroke="var(--ember)" stroke-width="14" stroke-dasharray="0 0"></circle>
          <circle id="arcTr" cx="60" cy="60" r="52" stroke="var(--rose)" stroke-width="14" stroke-dasharray="0 0"></circle>
        </svg>
        <div class="legend">
          <span class="ok" id="lgOk">success 0%</span>
          <span class="http" id="lgHttp">http failures 0%</span>
          <span class="tr" id="lgTr">transport 0%</span>
        </div>
      </div>
    </div>
  </div>

  <div class="card chart-card">
    <div class="k">Traffic timeline (per second)</div>
    <canvas id="chart"></canvas>
  </div>

  <div class="banner" id="banner">Test complete — final report saved. The dashboard now shows final aggregates.</div>
</div>
<script>
var CIRC=327;
function $(id){return document.getElementById(id)}
function setupChart(){
  var c=$('chart');var ctx=c.getContext('2d');
  function resize(){
    var dpr=window.devicePixelRatio||1;
    var w=c.clientWidth||640;
    c.width=w*dpr;c.height=260*dpr;
    ctx.setTransform(dpr,0,0,dpr,0,0);
  }
  resize();
  window.addEventListener('resize',resize);
  return ctx;
}
var ctx=setupChart();
function draw(timeline){
  var W=Math.max(100,$('chart').clientWidth||640);var H=258;
  ctx.clearRect(0,0,W,H);
  if(!timeline||!timeline.length)return;
  var maxR=1,maxL=1,maxE=1;
  for(var i=0;i<timeline.length;i++){
    var p=timeline[i];
    if(p.requests>maxR)maxR=p.requests;
    if(p.average_latency_ms>maxL)maxL=p.average_latency_ms;
    if(p.error_rate>maxE)maxE=p.error_rate;
  }
  var padB=4;
  function yA(v,m){return padB+(H-padB)*(1-v/m)}
  var step=W/(timeline.length-1||1);
  ctx.beginPath();
  ctx.moveTo(0,H);
  for(var i=0;i<timeline.length;i++)ctx.lineTo(i*step,yA(timeline[i].requests,maxR));
  ctx.lineTo((timeline.length-1)*step,H);
  ctx.closePath();
  ctx.fillStyle='rgba(52,211,153,0.16)';
  ctx.fill();
  ctx.beginPath();
  for(var i=0;i<timeline.length;i++){var X=i*step;var Y=yA(timeline[i].requests,maxR);if(i===0)ctx.moveTo(X,Y);else ctx.lineTo(X,Y);}
  ctx.strokeStyle='#34d399';ctx.lineWidth=2;ctx.stroke();
  ctx.beginPath();
  for(var i=0;i<timeline.length;i++){var X=i*step;var Y=yA(timeline[i].average_latency_ms,maxL);if(i===0)ctx.moveTo(X,Y);else ctx.lineTo(X,Y);}
  ctx.strokeStyle='#38bdf8';ctx.lineWidth=1.5;ctx.stroke();
  ctx.beginPath();
  for(var i=0;i<timeline.length;i++){var X=i*step;var Y=yA(timeline[i].error_rate,maxE);if(i===0)ctx.moveTo(X,Y);else ctx.lineTo(X,Y);}
  ctx.strokeStyle='#f87171';ctx.lineWidth=1.5;ctx.stroke();
}
function renderDonut(ok,http,tr){
  var tot=ok+http+tr;
  if(tot<=0){tot=1}
  var okP=ok/tot,httpP=http/tot;
  $('arcOk').setAttribute('stroke-dasharray',(CIRC*okP).toFixed(1)+' '+(CIRC-CIRC*okP).toFixed(1));
  var off=CIRC*okP;
  $('arcHttp').setAttribute('stroke-dasharray','0 '+off.toFixed(1)+' '+(CIRC*httpP).toFixed(1)+' '+(CIRC-CIRC*httpP-off).toFixed(1));
  var off2=off+CIRC*httpP;
  $('arcTr').setAttribute('stroke-dasharray','0 '+off2.toFixed(1)+' '+(CIRC*(tr/tot)).toFixed(1)+' '+(CIRC-CIRC*(tr/tot)-off2).toFixed(1));
  $('lgOk').textContent='success '+Math.round(okP*100)+'%';
  $('lgHttp').textContent='http failures '+Math.round(httpP*100)+'%';
  $('lgTr').textContent='transport '+Math.round(tr/tot*100)+'%';
}
function metaText(d){
  var s=d.method+' '+d.url+' · concurrency '+d.target_concurrency;
  if(d.rate_limit_rps>0)s=s+' · rps '+d.rate_limit_rps;
  s=s+' · '+d.duration;
  if(d.ramp_duration)s=s+' · ramp '+d.ramp_duration;
  return s;
}
var prev={finished:false,elapsed:0};
async function tick(){
  try{
    var res=await fetch('/api/live',{cache:'no-store'});
    if(!res.ok)return;
    var d=await res.json();
    if(d.running&&(prev.finished||d.elapsed_seconds<prev.elapsed)){
      $('status').textContent='Running';
      $('banner').style.display='none';
      $('total').textContent='0';
      $('err').textContent='0';
    }
    $('rps').textContent=d.requests_per_second.toFixed(1);
    $('succ').textContent=d.success_rate.toFixed(1)+'%';
    $('p95').textContent=d.p95_latency;
    $('el').textContent=Math.round(d.elapsed_seconds)+'s / '+d.duration;
    $('conc').textContent=d.concurrency;
    $('tp').textContent=d.throughput_pct.toFixed(1)+'%';
    $('pctText').textContent=Math.round(d.percent)+'%';
    $('bar').style.width=d.percent+'%';
    $('total').textContent=d.total_requests;
    $('err').textContent=d.transport_errors;
    $('meta').textContent=metaText(d);
    renderDonut(d.success_requests,d.http_failures,d.transport_errors);
    draw(d.timeline);
    if(d.finished){
      $('status').textContent='Completed';
      $('banner').style.display='block';
      $('bar').style.width='100%';
      $('pctText').textContent='100%';
    }
    prev.finished=d.finished;
    prev.elapsed=d.elapsed_seconds;
  }catch(e){}
}
var timer=setInterval(tick,1000);
tick();
</script>
</body>
</html>`
