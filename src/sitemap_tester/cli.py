"""Crawl a site's sitemap and test all discovered URLs."""

from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import json
import random
import sys
import threading
import time
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass
from typing import Iterable, List, Set
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SITEMAP_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"


@dataclass
class LinkResult:
    url: str
    ok: bool
    status_code: int | None
    elapsed_seconds: float
    error: str | None
    method: str


class SitemapCrawlerError(RuntimeError):
    pass


class HostRateLimiter:
    def __init__(self, min_interval_seconds: float) -> None:
        self.min_interval_seconds = max(0.0, min_interval_seconds)
        self._next_allowed_by_host: dict[str, float] = {}
        self._lock = threading.Lock()

    def acquire(self, url: str) -> None:
        if self.min_interval_seconds <= 0:
            return

        host = urlparse(url).netloc or "_default"
        while True:
            with self._lock:
                now = time.monotonic()
                next_allowed = self._next_allowed_by_host.get(host, 0.0)
                if now >= next_allowed:
                    self._next_allowed_by_host[host] = now + self.min_interval_seconds
                    return
                sleep_for = next_allowed - now
            time.sleep(sleep_for)


def normalize_base_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    if not parsed.scheme:
        base_url = f"https://{base_url}"
        parsed = urlparse(base_url)

    if not parsed.netloc:
        raise SitemapCrawlerError(f"Invalid URL: {base_url}")

    return f"{parsed.scheme}://{parsed.netloc}"


def build_session(retries: int, backoff_factor: float, user_agent: str) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        allowed_methods=("GET", "HEAD"),
        status_forcelist=(429, 500, 502, 503, 504),
        backoff_factor=backoff_factor,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": user_agent})
    return session


def decode_maybe_gzip(url: str, content_type: str, payload: bytes) -> bytes:
    is_gzip = url.endswith(".gz") or "gzip" in content_type.lower() or payload[:2] == b"\x1f\x8b"
    if not is_gzip:
        return payload

    try:
        return gzip.decompress(payload)
    except OSError:
        return payload


def get_xml(
    session: requests.Session,
    url: str,
    timeout: tuple[float, float],
    rate_limiter: HostRateLimiter,
) -> ET.Element:
    rate_limiter.acquire(url)
    try:
        response = session.get(url, timeout=timeout)
    except requests.RequestException as exc:
        raise SitemapCrawlerError(f"Failed to fetch sitemap '{url}': {exc}") from exc

    if response.status_code >= 400:
        raise SitemapCrawlerError(
            f"Failed to fetch sitemap '{url}': HTTP {response.status_code}"
        )

    xml_payload = decode_maybe_gzip(
        url=url,
        content_type=response.headers.get("Content-Type", ""),
        payload=response.content,
    )

    try:
        return ET.fromstring(xml_payload)
    except ET.ParseError as exc:
        raise SitemapCrawlerError(f"Invalid XML in sitemap '{url}': {exc}") from exc


def extract_text_items(root: ET.Element, tag: str) -> List[str]:
    items: List[str] = []
    for element in root.findall(f".//{SITEMAP_NS}{tag}"):
        if element.text:
            text = element.text.strip()
            if text:
                items.append(text)

    # Fallback for namespaces that are omitted or non-standard.
    if not items:
        for element in root.findall(f".//{tag}"):
            if element.text:
                text = element.text.strip()
                if text:
                    items.append(text)

    return items


def is_sitemap_index(root: ET.Element) -> bool:
    local_name = root.tag.rsplit("}", maxsplit=1)[-1]
    return local_name == "sitemapindex"


def crawl_sitemaps(
    session: requests.Session,
    start_sitemap_url: str,
    timeout: tuple[float, float],
    max_sitemaps: int,
    rate_limiter: HostRateLimiter,
) -> List[str]:
    queue = [start_sitemap_url]
    visited: Set[str] = set()
    urls: List[str] = []

    while queue:
        sitemap_url = queue.pop(0)
        if sitemap_url in visited:
            continue

        visited.add(sitemap_url)
        if len(visited) > max_sitemaps:
            raise SitemapCrawlerError(
                f"Exceeded max sitemaps ({max_sitemaps}). "
                "Increase --max-sitemaps if needed."
            )

        root = get_xml(session, sitemap_url, timeout, rate_limiter)

        if is_sitemap_index(root):
            child_sitemaps = extract_text_items(root, "loc")
            queue.extend(child_sitemaps)
            continue

        page_urls = extract_text_items(root, "loc")
        urls.extend(page_urls)

    seen = set()
    deduped = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)

    return deduped


def test_url(
    session: requests.Session,
    url: str,
    timeout: tuple[float, float],
    method: str,
    jitter_max: float,
    rate_limiter: HostRateLimiter,
) -> LinkResult:
    if jitter_max > 0:
        time.sleep(random.uniform(0, jitter_max))

    start = time.perf_counter()
    used_method = method
    try:
        if method == "get":
            rate_limiter.acquire(url)
            response = session.get(url, allow_redirects=True, timeout=timeout)
        elif method == "head":
            rate_limiter.acquire(url)
            response = session.head(url, allow_redirects=True, timeout=timeout)
        else:
            # Auto mode: use HEAD for lightweight checks and fallback to GET when needed.
            used_method = "head"
            rate_limiter.acquire(url)
            response = session.head(url, allow_redirects=True, timeout=timeout)
            if response.status_code >= 500 or response.status_code == 405:
                used_method = "get"
                rate_limiter.acquire(url)
                response = session.get(url, allow_redirects=True, timeout=timeout)

        elapsed = time.perf_counter() - start
        ok = 200 <= response.status_code < 400
        return LinkResult(
            url=url,
            ok=ok,
            status_code=response.status_code,
            elapsed_seconds=elapsed,
            error=None,
            method=used_method,
        )
    except requests.RequestException as exc:
        elapsed = time.perf_counter() - start
        return LinkResult(
            url=url,
            ok=False,
            status_code=None,
            elapsed_seconds=elapsed,
            error=str(exc),
            method=used_method,
        )


def run_checks(
    urls: Iterable[str],
    session: requests.Session,
    timeout: tuple[float, float],
    workers: int,
    method: str,
    jitter_max: float,
    rate_limiter: HostRateLimiter,
) -> List[LinkResult]:
    results: List[LinkResult] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                test_url,
                session,
                url,
                timeout,
                method,
                jitter_max,
                rate_limiter,
            )
            for url in urls
        ]

        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    return results


def print_summary(results: List[LinkResult], show_failures: int) -> int:
    total = len(results)
    failures = [result for result in results if not result.ok]
    successes = total - len(failures)

    status_counter = Counter(result.status_code for result in results if result.status_code is not None)
    network_errors = [result for result in failures if result.status_code is None]

    print(f"Checked URLs: {total}")
    print(f"Successes:    {successes}")
    print(f"Failures:     {len(failures)}")

    if status_counter:
        print("Status counts:")
        for code, count in sorted(status_counter.items()):
            print(f"  {code}: {count}")

    if failures:
        print(f"\nFirst {min(show_failures, len(failures))} failures:")
        for result in failures[:show_failures]:
            status = str(result.status_code) if result.status_code is not None else "ERR"
            error_text = f" ({result.error})" if result.error else ""
            print(f"  [{status}] {result.url}{error_text}")

    if network_errors:
        print(f"\nNetwork/timeout errors: {len(network_errors)}")

    return 0 if not failures else 1


def print_warm_summary(results: List[LinkResult], show_failures: int) -> int:
    total = len(results)
    status_counter = Counter(
        result.status_code for result in results if result.status_code is not None
    )
    network_errors = [result for result in results if result.status_code is None]

    print(f"Warmed URLs:   {total}")
    print(f"HTTP results:  {total - len(network_errors)}")
    print(f"Request errors:{len(network_errors)}")

    if status_counter:
        print("Status counts:")
        for code, count in sorted(status_counter.items()):
            print(f"  {code}: {count}")

    if network_errors:
        print(f"\nFirst {min(show_failures, len(network_errors))} request errors:")
        for result in network_errors[:show_failures]:
            error_text = f" ({result.error})" if result.error else ""
            print(f"  [ERR] {result.url}{error_text}")

    return 0


def summary_data(results: List[LinkResult], warm_only: bool) -> dict[str, object]:
    total = len(results)
    failures = [result for result in results if not result.ok]
    successes = total - len(failures)
    status_counter = Counter(
        result.status_code for result in results if result.status_code is not None
    )
    network_errors = sum(1 for result in failures if result.status_code is None)

    return {
        "mode": "warm-only" if warm_only else "validate",
        "checked_urls": total,
        "successes": successes,
        "failures": len(failures),
        "network_errors": network_errors,
        "status_counts": {str(code): count for code, count in sorted(status_counter.items())},
    }


def render_json(results: List[LinkResult], warm_only: bool) -> str:
    payload = {
        "summary": summary_data(results, warm_only=warm_only),
        "results": [
            {
                "url": result.url,
                "ok": result.ok,
                "status_code": result.status_code,
                "elapsed_seconds": result.elapsed_seconds,
                "error": result.error,
                "method": result.method,
            }
            for result in results
        ],
    }
    return json.dumps(payload, indent=2)


def render_csv(results: List[LinkResult]) -> str:
    rows = []
    rows.append("url,method,ok,status_code,elapsed_seconds,error")
    for result in results:
        row = (
            result.url,
            result.method,
            str(result.ok).lower(),
            "" if result.status_code is None else str(result.status_code),
            f"{result.elapsed_seconds:.6f}",
            "" if result.error is None else result.error,
        )
        rows.append(",".join(csv_quote(value) for value in row))
    return "\n".join(rows)


def csv_quote(value: str) -> str:
    needs_quotes = any(char in value for char in [",", "\"", "\n", "\r"])
    if not needs_quotes:
        return value
    return "\"" + value.replace("\"", "\"\"") + "\""


def emit_output(content: str, output_file: str | None) -> None:
    if output_file:
        with open(output_file, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            if not content.endswith("\n"):
                handle.write("\n")
        return
    print(content)


def log(message: str, output_format: str) -> None:
    if output_format == "text":
        print(message)
        return
    print(message, file=sys.stderr)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl sitemap.xml and test all listed URLs."
    )
    parser.add_argument(
        "url",
        help="Base site URL (e.g. https://example.com) or direct sitemap URL.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        help="Legacy timeout in seconds for both connect and read phases.",
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=5.0,
        help="Connect timeout in seconds (default: 5).",
    )
    parser.add_argument(
        "--read-timeout",
        type=float,
        default=20.0,
        help="Read timeout in seconds (default: 20).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Concurrent worker threads for URL tests (default: 8).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP retry attempts for transient errors (default: 3).",
    )
    parser.add_argument(
        "--backoff",
        type=float,
        default=1.0,
        help="Retry backoff factor (default: 1.0).",
    )
    parser.add_argument(
        "--max-sitemaps",
        type=int,
        default=200,
        help="Safety limit for number of sitemap files traversed (default: 200).",
    )
    parser.add_argument(
        "--jitter-max",
        type=float,
        default=0.15,
        help="Max random sleep (seconds) before each URL check to reduce bursts.",
    )
    parser.add_argument(
        "--show-failures",
        type=int,
        default=50,
        help="Max number of failed URLs printed (default: 50).",
    )
    parser.add_argument(
        "--user-agent",
        default="sitemap-link-tester/1.0",
        help="Custom User-Agent header.",
    )
    parser.add_argument(
        "--method",
        choices=("auto", "head", "get"),
        default="auto",
        help="Request method strategy for URL checks (default: auto).",
    )
    parser.add_argument(
        "--warm-cache",
        action="store_true",
        help="Force GET requests to warm upstream/Varnish cache.",
    )
    parser.add_argument(
        "--warm-only",
        action="store_true",
        help="Warm cache without pass/fail validation; always exits 0 unless setup fails.",
    )
    parser.add_argument(
        "--rps",
        type=float,
        default=0.0,
        help="Max requests per second per host (default: disabled).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Minimum delay in seconds between requests per host (default: 0).",
    )
    parser.add_argument(
        "--output",
        choices=("text", "json", "csv"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--output-file",
        help="Optional file path for output. Defaults to stdout.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    base = normalize_base_url(args.url)
    sitemap_url = args.url if args.url.endswith(".xml") else urljoin(f"{base}/", "sitemap.xml")

    connect_timeout = args.connect_timeout
    read_timeout = args.read_timeout
    if args.timeout is not None:
        connect_timeout = args.timeout
        read_timeout = args.timeout
    timeout = (max(0.1, connect_timeout), max(0.1, read_timeout))

    rps_interval = 1.0 / args.rps if args.rps and args.rps > 0 else 0.0
    min_interval = max(max(0.0, args.delay), rps_interval)
    rate_limiter = HostRateLimiter(min_interval)

    session = build_session(args.retries, args.backoff, args.user_agent)
    method = "get" if (args.warm_cache or args.warm_only) else args.method

    log(f"Loading sitemap: {sitemap_url}", args.output)
    try:
        urls = crawl_sitemaps(
            session=session,
            start_sitemap_url=sitemap_url,
            timeout=timeout,
            max_sitemaps=args.max_sitemaps,
            rate_limiter=rate_limiter,
        )
    except SitemapCrawlerError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if not urls:
        print("No URLs found in sitemap.")
        return 2

    mode_text = f"method={method}"
    if args.warm_only:
        mode_text += ", warm-only"
    log(
        f"Found {len(urls)} URLs. Running checks with {args.workers} workers ({mode_text})...",
        args.output,
    )
    results = run_checks(
        urls=urls,
        session=session,
        timeout=timeout,
        workers=max(1, args.workers),
        method=method,
        jitter_max=max(0.0, args.jitter_max),
        rate_limiter=rate_limiter,
    )
    results = sorted(results, key=lambda result: result.url)

    if args.output == "text":
        if args.warm_only:
            return print_warm_summary(results, max(1, args.show_failures))
        return print_summary(results, max(1, args.show_failures))

    if args.output == "json":
        emit_output(render_json(results, warm_only=args.warm_only), args.output_file)
    else:
        emit_output(render_csv(results), args.output_file)

    if args.warm_only:
        return 0
    return 0 if all(result.ok for result in results) else 1


def main_entry() -> None:
    raise SystemExit(main())
