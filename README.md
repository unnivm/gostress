# gostress

`gostress` is a lightweight HTTP stress-testing tool written in Go.

## What it does

- Sends concurrent `GET` or `POST` requests to a target URL
- Supports a global requests-per-second cap
- Lets you define which HTTP status codes count as success
- Tracks throughput and latency percentiles
- Counts HTTP status codes separately from transport errors
- Writes reports in `json`, `csv`, and/or `html`

## Usage

```bash
go run . --url http://localhost:8080
```

```bash
go run . \
  --url http://localhost:8080/api/orders \
  --c 50 \
  --d 30 \
  --method POST \
  --payload '{"customerId":123}' \
  --headers 'Authorization: Bearer token, X-Trace: demo' \
  --success-status '200-299,409' \
  --rps 200 \
  --formats 'json,csv,html' \
  --output reports/run-01
```

```bash
go run . --from-report report.json --formats html --output reports/run-01
```

```bash
go run . --serve-web --web-addr :8088 --dashboard-report report.json
```

## Flags

- `--url`: target URL, required
- `--c`: concurrency level, default `10`
- `--d`: duration in seconds, default `10`
- `--method`: `GET` or `POST`, default `GET`
- `--payload`: request body for `POST`
- `--headers`: comma-separated `key:value` pairs
- `--success-status`: comma-separated successful status codes or ranges, default `200-399`
- `--rps`: global requests-per-second cap across all workers, default `0` for unlimited
- `--formats`: output formats to write, comma-separated `json,csv,html`, default `json`
- `--output`: output file prefix without extension, default `report`
- `--from-report`: render output files from an existing JSON report instead of running a new test
- `--serve-web`: start the protected web dashboard instead of running a stress test
- `--web-addr`: bind address for the web dashboard, default `:8088`
- `--dashboard-report`: preload the dashboard from a JSON report file such as `report.json`

## Notes

- By default, HTTP `2xx` and `3xx` responses are treated as successful requests.
- You can override that with `--success-status`, for example `200-299,429` if `429` is acceptable for your test case.
- The `--rps` limit is shared across all workers, so concurrency and request rate can be tuned independently.
- Output files are written as `<prefix>.json`, `<prefix>.csv`, and `<prefix>.html` depending on `--formats`.
- The HTML report includes a narrative summary plus built-in charts for reliability, latency, status codes, and transport errors.
- `--from-report` is handy when you already have `report.json` and only want to re-render it as polished HTML.
- `--serve-web` adds a sign-up page, login page, logout page, a protected dashboard, and authenticated JSON import/API routes.
- The dashboard can load report data from `--dashboard-report`, from the protected `/api/report` endpoint, or from the JSON paste form in the UI.
- Account storage in this Go implementation is in-memory for the lifetime of the running process.
- Transport-level problems such as timeouts or connection failures are counted separately.
- Duration values in the saved report are written in human-readable Go duration format such as `125ms` or `10.2s`.
