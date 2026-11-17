---
title: "A Feature Tour of GoStress: HTTP Load Testing in Go"
published: false
description: "Explore GoStress traffic controls, latency and error metrics, the dynamic dashboard, report formats, and CI performance gates."
tags: go, testing, performance, opensource
---

From a focused HTTP load test to a live view, polished report, or automated performance gate, GoStress brings the core testing workflow into one Go tool.

Performance testing should answer practical questions: How many requests can an endpoint handle? Does latency stay acceptable under load? Are failures coming from HTTP responses or the network? Can a team catch regressions before release? GoStress is a lightweight HTTP stress-testing tool written in Go. This tour explains how its traffic controls, measurements, dashboards, exports, and thresholds fit together.

## Shape the traffic you want to test

GoStress sends concurrent HTTP `GET` or `POST` requests to a target URL. For POST tests, provide a payload; custom headers can carry authentication, content negotiation, or tracing information. GET is the default method. If a POST payload has no content type, GoStress sends it as JSON.

Set the worker count with `--c` and the test duration with `--d`. Workers generate concurrent requests until the run reaches its deadline. Concurrency answers how many requests can be in flight at once; it does not by itself define a fixed request rate. For rate-controlled tests, `--rps` applies one global requests-per-second limit across all workers. This lets you adjust concurrency and offered request rate independently, which is valuable when you want to compare system behavior at a controlled traffic level.

For capacity exploration, `--ramp` gradually raises concurrency from one worker toward the target. This can reveal when latency rises or failures begin, instead of starting at peak load. Ramp duration must be shorter than the test. These controls support smoke checks, staged experiments, and longer runs.

## Define what success means

Applications do not always use the same status codes to signal an acceptable outcome. By default, GoStress treats HTTP responses in the 200–399 range as successful. Use `--success-status` to supply codes or ranges that fit your endpoint, such as `200-299,409` when a conflict response is expected and acceptable for the test.

This choice affects the success and error rates, as well as the requests included in latency statistics. GoStress separates HTTP failures—responses with a status outside the configured success ranges—from transport errors such as timeouts, connection failures, or interrupted reads. That distinction helps narrow down whether the target is returning an application-level response or whether a request failed before a usable response was received. Status code counts and transport error categories add more detail to the overall totals.

## Measure throughput and latency

GoStress summarizes request totals, success and error rates, achieved requests per second, transferred data, and average throughput. Latency statistics include minimum, maximum, average, and p50, p95, and p99 percentiles for successful responses. p50 represents the middle request; p95 and p99 expose slower tail behavior that an average can hide.

## Watch the run with the dynamic dashboard

When a test lasts several minutes, waiting for the final report can make it difficult to notice when behavior changed. Add `--live-web :8088` to start the dynamic dashboard alongside the load generator. GoStress prints a URL to open in a browser. While the test runs, summary cards update with request rate, data throughput, total requests, success rate, average and p99 latency, transport errors, and HTTP failures. A timeline charts requests per second against average and p99 latency.

The browser polls a JSON snapshot once per second. A fixed-size latency histogram and bounded timeline keep retained monitoring data independent of request count. Live percentiles are estimates, and per-second p99 uses a capped sample. Follow trends on the dashboard, then review the completed report. The live page stays available until the process stops.

## Choose the report workflow that fits

GoStress writes JSON, CSV, and HTML reports. JSON preserves structured results for tools, CSV is convenient for spreadsheets, and HTML presents a narrative summary with reliability, latency, status code, and transport error charts. Select formats with `--formats` and set a filename prefix with `--output`.

If a JSON report already exists, `--from-report` can render it into another supported format without repeating the load test. For browser-based review, `--serve-web` starts the protected report dashboard. It supports sign-up and login, can preload a report using `--dashboard-report`, and provides an import form for report JSON. The web dashboard is intended for reviewing completed report data; `--live-web` is the separate mode for observing a running test.

## Turn performance expectations into a gate

For teams that run load checks in CI, a report alone does not decide whether a build should pass. Optional thresholds let the test return a failing exit status when measured performance misses an agreed limit. `--max-p95 300ms` sets a maximum p95 latency; `--max-error-rate 1` caps the error rate at one percent; and `--min-rps 500` requires at least 500 requests per second. Combine the flags to express several requirements in one run.

GoStress prints the result and still writes the report when a gate fails. A failed threshold returns exit status 1, which CI systems can use to mark a job unsuccessful. Thresholds are optional. Choose realistic limits because shared runners, network variation, and changing test data affect measurements.

## Start small, then build a repeatable check

A straightforward run can begin with a URL and duration:

```bash
go run . --url http://localhost:8080/api/health --c 50 --d 60 \
  --ramp 15 --formats json,html --output reports/health-run
```

Add request headers or a POST payload when the endpoint requires them. Use `--rps` when you need a global rate cap, and configure success statuses to match expected application behavior. Add `--live-web` to watch a longer run, or add thresholds to turn a repeatable check into a CI performance gate. Afterward, compare reports from equivalent runs and investigate changes alongside metrics from the service being tested.

GoStress combines traffic controls, useful measurements, multiple report formats, a live dashboard, a protected report viewer, and threshold-based pass/fail checks in one tool. Each feature serves a different point in the workflow: shaping traffic, observing a run, understanding its result, and deciding whether performance meets expectations. Explore the [usage guide](https://github.com/unnivm/gostress#usage) for the available flags, or browse the [GoStress blog](https://github.com/unnivm/gostress/tree/main/blog) for focused guides.

> **Tip:** Keep test settings consistent when comparing runs. Use the same endpoint, request shape, success criteria, and load controls, then compare the reports and the target system’s own metrics.
