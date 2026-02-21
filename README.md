# Sitemap Tester

`sitemap-tester` is an installable CLI utility for checking website URLs listed in sitemap files.

What it does:
- Accepts a base URL (auto-uses `/sitemap.xml`) or a direct sitemap URL.
- Crawls both sitemap index files and URL set files.
- Supports both plain XML and `.xml.gz` sitemap files.
- Tests every discovered URL.
- Handles slow/cold Drupal responses with retries, backoff, timeout control, and bounded concurrency.
- Includes a cache-warm mode that forces `GET` requests (useful for Varnish warmup).
- Reports HTTP failures and network/timeouts with a summary.

## Project Structure

```text
.
├── pyproject.toml
├── README.md
└── src/
    └── sitemap_tester/
        ├── __init__.py
        └── cli.py
```

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

## Install (uv)

Create environment and sync dependencies:

```bash
uv sync
```

Run without installing globally:

```bash
uv run sitemap-tester https://example.com
```

Install as a CLI tool into your user environment:

```bash
uv tool install .
```

Then run:

```bash
sitemap-tester https://example.com
```

## Usage

Basic:

```bash
uv run sitemap-tester https://example.com
```

Direct sitemap:

```bash
uv run sitemap-tester https://example.com/sitemap.xml
```

Tuned for slow Drupal instances:

```bash
uv run sitemap-tester https://example.com \
  --connect-timeout 8 \
  --read-timeout 30 \
  --workers 6 \
  --retries 4 \
  --backoff 1.5 \
  --rps 2 \
  --jitter-max 0.25
```

Warm Varnish cache using GET requests:

```bash
uv run sitemap-tester https://example.com \
  --warm-cache \
  --workers 4 \
  --rps 2
```

Warm cache only (no pass/fail validation):

```bash
uv run sitemap-tester https://example.com \
  --warm-only \
  --workers 4 \
  --rps 2
```

JSON output to stdout (CI-friendly):

```bash
uv run sitemap-tester https://example.com --output json
```

CSV output to file:

```bash
uv run sitemap-tester https://example.com --output csv --output-file results.csv
```

## Command Line Switches

- `url` (positional): Base site URL (for example `https://example.com`) or direct sitemap URL.
- `--timeout FLOAT`: Legacy timeout that sets both connect and read timeouts.
- `--connect-timeout FLOAT`: Connect timeout in seconds. Default: `5`.
- `--read-timeout FLOAT`: Read timeout in seconds. Default: `20`.
- `--workers INT`: Concurrent worker threads used to check URLs. Default: `8`.
- `--retries INT`: Retry attempts for transient HTTP/network failures. Default: `3`.
- `--backoff FLOAT`: Retry backoff factor. Default: `1.0`.
- `--max-sitemaps INT`: Maximum number of sitemap files to traverse as a safety guard. Default: `200`.
- `--method auto|head|get`: Request strategy for URL checks. Default: `auto`.
- `--warm-cache`: Force `GET` requests for cache warmup (overrides `--method`).
- `--warm-only`: Warm cache without pass/fail validation. Forces `GET` and exits `0` unless setup/sitemap loading fails.
- `--rps FLOAT`: Maximum requests per second per host. Default: disabled.
- `--delay FLOAT`: Minimum delay in seconds between requests per host. Default: `0`.
- `--jitter-max FLOAT`: Maximum random delay (seconds) before each URL check to reduce request bursts. Default: `0.15`.
- `--show-failures INT`: Maximum number of failed URLs to print. Default: `50`.
- `--user-agent STRING`: Custom User-Agent header. Default: `sitemap-link-tester/1.0`.
- `--output text|json|csv`: Output format. Default: `text`.
- `--output-file PATH`: Optional path to write output; otherwise writes to stdout.

## Exit Codes

- `0`: All links passed.
- `1`: One or more links failed.
- `2`: Sitemap load/parse/configuration error.
