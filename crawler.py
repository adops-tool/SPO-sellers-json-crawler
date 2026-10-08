"""Check a site list for sellers.json, with optional SPO discovery.

Python 3.11+. Run ``python crawler.py --help`` for configuration.
"""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import logging
import math
import os
import random
import re
import signal
from contextlib import ExitStack, asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable, TextIO
from urllib.parse import urljoin, urlsplit, urlunsplit

import aiohttp


PROJECT_DIR = Path(__file__).resolve().parent
ADS_PATHS = ("/ads.txt", "/app-ads.txt")
BAD_CONTENT_TYPES = {"text/html", "application/xhtml+xml", "application/xml", "text/xml"}
RETRY_STATUSES = {408, 429, 500, 502, 503, 504}
LABEL_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
USER_AGENT = "SellersJsonCrawler/2.0"
log = logging.getLogger("crawler")


@dataclass(frozen=True)
class Config:
    input_file: Path = PROJECT_DIR / "publishers.txt"
    output_file: Path = PROJECT_DIR / "exchanges.txt"
    discovered_file: Path = PROJECT_DIR / "discovered_publishers.txt"
    concurrency: int = 10
    timeout: float = 20.0
    retries: int = 2  # Additional attempts after the initial request.
    retry_backoff: float = 1.0
    max_retry_delay: float = 60.0
    max_sellers_bytes: int = 50 * 1024 * 1024
    max_ads_bytes: int = 5 * 1024 * 1024
    max_domains: int = 100_000
    discover: bool = False
    recursive: bool = False
    https_only: bool = False
    progress_interval: float = 15.0
    verbose: bool = False

    def validate(self) -> None:
        for name in ("concurrency", "timeout", "max_sellers_bytes", "max_ads_bytes",
                     "max_domains", "progress_interval", "max_retry_delay"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a finite positive number.")
        if self.retries < 0 or not math.isfinite(self.retry_backoff) or self.retry_backoff < 0:
            raise ValueError("Retries and retry backoff must be non-negative.")
        # Also protect the input against collisions with generated lock files.
        paths = [self.input_file, self.output_file, lock_path(self.output_file)]
        if self.recursive:
            paths.extend((self.discovered_file, lock_path(self.discovered_file)))
        resolved = [os.path.normcase(str(path.resolve())) for path in paths]
        if len(resolved) != len(set(resolved)):
            raise ValueError("Input, output, discovery, and lock paths must be different.")
        existing = [path for path in paths if path.exists()]
        for index, path in enumerate(existing):
            if any(path.samefile(other) for other in existing[index + 1:]):
                raise ValueError("Input, output, discovery, and lock files must be different.")


@dataclass(frozen=True)
class Target:
    domain: str
    endpoint: str = "/sellers.json"

    def urls(self, https_only: bool = False) -> Iterable[str]:
        for scheme in (("https",) if https_only else ("https", "http")):
            yield f"{scheme}://{self.domain}{self.endpoint}"


@dataclass
class Stats:
    targets_checked: int = 0
    sellers_found: int = 0
    new_urls: int = 0
    ads_checked: int = 0
    new_domains: int = 0
    requests: int = 0
    retries: int = 0
    limit_reached: bool = False


def clean_publisher(raw: object) -> str | None:
    """Normalize a public hostname, preserving www and converting IDNs to ASCII."""
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value or value.startswith("#") or any(ch.isspace() for ch in value):
        return None
    try:
        parsed = urlsplit(value if "://" in value else "//" + value)
        if parsed.scheme and parsed.scheme.lower() not in {"http", "https"}:
            return None
        if parsed.username is not None or parsed.password is not None:
            return None
        if parsed.port is not None and parsed.port != {"http": 80, "https": 443}.get(parsed.scheme):
            return None
        host = parsed.hostname
        if not host:
            return None
        domain = host.rstrip(".").encode("idna").decode("ascii").lower()
        if len(domain) > 253:
            return None
        labels = domain.split(".")
        if len(labels) < 2 or any(not LABEL_RE.fullmatch(label) for label in labels):
            return None
        tld = labels[-1]
        if not (re.fullmatch(r"[a-z]{2,63}", tld) or (tld.startswith("xn--") and len(tld) > 4)):
            return None
        try:
            ipaddress.ip_address(domain)
        except ValueError:
            return domain
        return None
    except (ValueError, UnicodeError):
        return None


def clean_ad_system(raw: object) -> str | None:
    if not isinstance(raw, str) or "=" in raw:
        return None
    return clean_publisher(raw)


def parse_target(raw: str) -> Target | None:
    """Keep an explicitly supplied sellers.json path; site URLs use the root path."""
    domain = clean_publisher(raw)
    if domain is None:
        return None
    parsed = urlsplit(raw.strip() if "://" in raw else "//" + raw.strip())
    if parsed.path.lower().endswith("/sellers.json"):
        endpoint = parsed.path
        if parsed.query:
            endpoint += "?" + parsed.query
        return Target(domain, endpoint)
    return Target(domain)


def parse_ads_txt(body: str) -> list[str]:
    domains: dict[str, None] = {}
    for raw in body.lstrip("\ufeff").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if "=" in line:
            key, value = line.split("=", 1)
            if key.strip().upper() in {"OWNERDOMAIN", "MANAGERDOMAIN"}:
                domain = clean_publisher(value.split(",", 1)[0])
                if domain:
                    domains[domain] = None
            continue
        columns = [part.strip() for part in line.split(",")]
        if len(columns) < 3 or not columns[1] or columns[2].upper() not in {"DIRECT", "RESELLER"}:
            continue
        domain = clean_ad_system(columns[0])
        if domain:
            domains[domain] = None
    return list(domains)


def extract_publishers_from_sellers(data: dict) -> Iterable[str]:
    for seller in data["sellers"]:
        if isinstance(seller, dict):
            domain = clean_publisher(seller.get("domain"))
            if domain:
                yield domain


def parse_sellers_json(body: bytes) -> dict:
    def reject_constant(value: str) -> None:
        raise ValueError(f"Invalid JSON constant: {value}")

    data = json.loads(body.decode("utf-8-sig"), parse_constant=reject_constant)
    if not isinstance(data, dict) or not isinstance(data.get("sellers"), list):
        raise ValueError("Expected a JSON object containing a sellers array.")
    if any(not isinstance(seller, dict) for seller in data["sellers"]):
        raise ValueError("Every sellers array entry must be an object.")
    return data


def load_targets(paths: Iterable[Path], max_domains: int) -> list[Target]:
    targets: dict[Target, None] = {}
    for path in paths:
        invalid = 0
        with path.open("r", encoding="utf-8-sig") as stream:
            for line in stream:
                value = line.split("#", 1)[0].strip()
                if not value:
                    continue
                target = parse_target(value)
                if target is None:
                    invalid += 1
                    continue
                targets[target] = None
                if len(targets) > max_domains:
                    raise ValueError(f"Input exceeds the {max_domains:,} target limit; increase --max-domains.")
        if invalid:
            log.warning("Ignored %d invalid input lines in %s.", invalid, path)
    if not targets:
        raise ValueError("No valid sites found in the input file.")
    return list(targets)


def canonical_url(value: str) -> str | None:
    """Normalize URL host and scheme without changing case-sensitive paths."""
    domain = clean_publisher(value)
    if domain is None:
        return None
    parsed = urlsplit(value.strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    return urlunsplit((parsed.scheme.lower(), domain, parsed.path or "/", parsed.query, ""))


def lock_path(path: Path) -> Path:
    return path.with_name(path.name + ".lock")


class OutputLock:
    """An OS lock prevents concurrent writers and is released on process exit."""

    def __init__(self, path: Path) -> None:
        self.path = lock_path(path)
        self.stream = None

    def __enter__(self) -> OutputLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("a+b")
        try:
            if self.path.stat().st_size == 0:
                self.stream.write(b"\0")
                self.stream.flush()
            self.stream.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self.stream.close()
            raise OSError(f"Cannot lock {self.path}; another crawler may be using this output.") from error
        return self

    def __exit__(self, *args) -> None:
        self.stream.close()


class LineStore:
    """Append distinct lines, flushing and syncing each batch before returning."""

    def __init__(self, path: Path, *, urls: bool = False) -> None:
        self.path = path
        self.urls = urls
        self.seen: set[str] = set()
        self.stream: TextIO | None = None

    def __enter__(self) -> LineStore:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            invalid = 0
            with self.path.open("r", encoding="utf-8-sig") as stream:
                for line in stream:
                    value = line.strip()
                    normalized = canonical_url(value) if self.urls else clean_publisher(value)
                    if normalized:
                        self.seen.add(normalized)
                    elif value:
                        invalid += 1
            if invalid:
                log.warning("Ignored %d invalid existing lines in %s.", invalid, self.path)
        # Repair a missing trailing newline without joining the next result to it.
        needs_newline = False
        if self.path.exists() and self.path.stat().st_size:
            with self.path.open("rb") as stream:
                stream.seek(-1, os.SEEK_END)
                needs_newline = stream.read(1) != b"\n"
        self.stream = self.path.open("a", encoding="utf-8", newline="\n")
        try:
            if needs_newline:
                self.stream.write("\n")
                self.stream.flush()
                os.fsync(self.stream.fileno())
        except BaseException:
            self.stream.close()
            raise
        return self

    def append(self, values: Iterable[str]) -> int:
        fresh = list(dict.fromkeys(value for value in values if value not in self.seen))
        if not fresh:
            return 0
        self.stream.write("".join(value + "\n" for value in fresh))
        self.stream.flush()
        os.fsync(self.stream.fileno())
        self.seen.update(fresh)
        return len(fresh)

    def __exit__(self, *args) -> None:
        self.stream.close()


def retry_delay(header: str | None, attempt: int, config: Config) -> float:
    if header:
        try:
            delay = float(header)
        except ValueError:
            try:
                date = parsedate_to_datetime(header)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=timezone.utc)
                delay = (date - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                delay = -1
        if math.isfinite(delay) and delay >= 0:
            return min(delay, config.max_retry_delay)
    base = min(config.max_retry_delay, config.retry_backoff * (2 ** min(attempt, 16)))
    return min(config.max_retry_delay, base * random.uniform(1.0, 1.25))


@asynccontextmanager
async def open_response(session: aiohttp.ClientSession, url: str, config: Config, accept: str):
    """Follow bounded redirects, rejecting HTTPS downgrades before issuing them."""
    history = []
    # The deadline includes the complete redirect chain and body consumption.
    async with asyncio.timeout(config.timeout):
        for redirect in range(6):
            async with session.get(url, headers={"Accept": accept}, allow_redirects=False) as response:
                location = response.headers.get("Location") or response.headers.get("URI")
                if response.status not in {301, 302, 303, 307, 308} or not location:
                    yield response
                    return
                if redirect == 5:
                    raise aiohttp.TooManyRedirects(response.request_info, tuple(history))
                try:
                    next_url = urljoin(str(response.url), location)
                    scheme = urlsplit(next_url).scheme.lower()
                except ValueError as error:
                    raise aiohttp.InvalidURL(location) from error
                if scheme not in {"http", "https"} or (config.https_only and scheme != "https"):
                    raise aiohttp.InvalidURL(next_url, "Redirect protocol is not allowed.")
                history.append(response)
                url = next_url


async def request_body(
    session: aiohttp.ClientSession, url: str, cap_bytes: int, config: Config, stats: Stats,
    *, accept: str = "application/json, text/plain;q=0.9, */*;q=0.1",
) -> tuple[str, bytes] | None:
    for attempt in range(config.retries + 1):
        delay = None
        stats.requests += 1
        try:
            async with open_response(session, url, config, accept) as response:
                if config.https_only and (
                    response.url.scheme != "https"
                    or any(item.url.scheme != "https" for item in response.history)
                ):
                    log.debug("Rejected HTTP redirect for %s.", url)
                    return None
                if response.status == 200:
                    content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                    if content_type in BAD_CONTENT_TYPES:
                        log.debug("Rejected %s content at %s.", content_type, url)
                        return None
                    if (not response.headers.get("Content-Encoding")
                            and response.content_length is not None
                            and response.content_length > cap_bytes):
                        log.debug("Response exceeds the %d-byte limit: %s.", cap_bytes, url)
                        return None
                    body = bytearray()
                    async for chunk in response.content.iter_chunked(64 * 1024):
                        if len(body) + len(chunk) > cap_bytes:
                            log.debug("Decoded response exceeds the %d-byte limit: %s.", cap_bytes, url)
                            return None
                        body.extend(chunk)
                    return str(response.url), bytes(body)
                log.debug("HTTP %d: %s.", response.status, url)
                if response.status not in RETRY_STATUSES or attempt == config.retries:
                    return None
                delay = retry_delay(response.headers.get("Retry-After"), attempt, config)
        except (aiohttp.ClientConnectorCertificateError, aiohttp.ClientConnectorSSLError,
                aiohttp.InvalidURL, aiohttp.TooManyRedirects) as error:
            log.debug("Cannot fetch %s: %s.", url, error)
            return None
        except (aiohttp.ClientError, TimeoutError, OSError) as error:
            log.debug("Request failed for %s: %s.", url, error)
            if attempt == config.retries:
                return None
            delay = retry_delay(None, attempt, config)
        # Release the response/connection before sleeping.
        stats.retries += 1
        await asyncio.sleep(delay)
    return None


async def fetch_sellers_json(
    session: aiohttp.ClientSession, target: Target, config: Config, stats: Stats,
) -> tuple[str, dict] | None:
    for url in target.urls(config.https_only):
        result = await request_body(session, url, config.max_sellers_bytes, config, stats)
        if result is None:
            continue
        final_url, body = result
        try:
            data = await asyncio.to_thread(parse_sellers_json, body)
        except (ValueError, UnicodeError, RecursionError) as error:
            log.debug("Invalid sellers.json at %s: %s.", final_url, error)
            continue
        normalized = canonical_url(final_url)
        if normalized:
            return normalized, data
    return None


class Crawler:
    """One queue tracks both direct probes and optional discovery jobs."""

    def __init__(self, config: Config, output: LineStore, discovered: LineStore | None) -> None:
        self.config = config
        self.output = output
        self.discovered = discovered
        self.stats = Stats()
        self.queue: asyncio.Queue[tuple[str, Target]] = asyncio.Queue()
        self.scheduled: set[Target] = set()
        self.ads_scheduled: set[str] = set()
        self.known_domains: set[str] = set()

    def schedule(self, target: Target, *, scan_ads: bool = False) -> bool:
        if target not in self.scheduled:
            if len(self.scheduled) >= self.config.max_domains:
                if not self.stats.limit_reached:
                    log.warning("Reached the %d target limit; further discoveries will be skipped.",
                                self.config.max_domains)
                    self.stats.limit_reached = True
                return False
            self.scheduled.add(target)
            self.known_domains.add(target.domain)
            self.queue.put_nowait(("sellers", target))
        if scan_ads and target.domain not in self.ads_scheduled:
            self.ads_scheduled.add(target.domain)
            self.queue.put_nowait(("ads", target))
        return True

    async def check_sellers(self, session: aiohttp.ClientSession, target: Target) -> None:
        result = await fetch_sellers_json(session, target, self.config, self.stats)
        self.stats.targets_checked += 1
        if result is None:
            return
        url, data = result
        self.stats.new_urls += self.output.append([url])
        self.stats.sellers_found += 1
        if not self.config.recursive:
            return
        new_domains = []
        for domain in extract_publishers_from_sellers(data):
            is_new = domain not in self.known_domains
            if self.schedule(Target(domain), scan_ads=True) and is_new:
                new_domains.append(domain)
        self.stats.new_domains += len(new_domains)
        self.discovered.append(new_domains)

    async def check_ads(self, session: aiohttp.ClientSession, target: Target) -> None:
        for path in ADS_PATHS:
            for url in Target(target.domain, path).urls(self.config.https_only):
                result = await request_body(session, url, self.config.max_ads_bytes,
                                            self.config, self.stats, accept="text/plain, */*;q=0.1")
                if result is None:
                    continue
                body = result[1].decode("utf-8-sig", errors="replace")
                # Some soft-404 pages are incorrectly labelled text/plain.
                if body.lstrip().startswith("<"):
                    continue
                for domain in parse_ads_txt(body):
                    self.schedule(Target(domain))
                break
        self.stats.ads_checked += 1

    async def worker(self, session: aiohttp.ClientSession) -> None:
        while True:
            kind, target = await self.queue.get()
            try:
                if kind == "sellers":
                    await self.check_sellers(session, target)
                else:
                    await self.check_ads(session, target)
            finally:
                # Children are enqueued before the parent's completion.
                self.queue.task_done()

    def report(self) -> None:
        log.info("Checked %d/%d targets; found %d valid files (%d new URLs); "
                 "%d ads scans; %d discovered domains; %d requests (%d retries); %d queued jobs.",
                 self.stats.targets_checked, len(self.scheduled), self.stats.sellers_found,
                 self.stats.new_urls, self.stats.ads_checked, self.stats.new_domains,
                 self.stats.requests, self.stats.retries, self.queue.qsize())

    async def progress(self) -> None:
        while True:
            await asyncio.sleep(self.config.progress_interval)
            self.report()

    async def crawl(self, session: aiohttp.ClientSession, targets: list[Target]) -> Stats:
        for target in targets:
            self.schedule(target, scan_ads=self.config.discover or self.config.recursive)
        try:
            async with asyncio.TaskGroup() as group:
                workers = [group.create_task(self.worker(session), name=f"crawler-{index}")
                           for index in range(self.config.concurrency)]
                progress = group.create_task(self.progress(), name="crawler-progress")
                await self.queue.join()
                for task in (*workers, progress):
                    task.cancel()
        finally:
            self.report()
        return self.stats


async def run(config: Config | None = None) -> Stats:
    config = config or Config()
    config = replace(config, input_file=config.input_file.resolve(),
                     output_file=config.output_file.resolve(),
                     discovered_file=config.discovered_file.resolve())
    config.validate()
    with ExitStack() as stack:
        stack.enter_context(OutputLock(config.output_file))
        if config.recursive:
            stack.enter_context(OutputLock(config.discovered_file))
        input_paths = [config.input_file]
        if config.recursive and config.discovered_file.exists():
            input_paths.append(config.discovered_file)
        targets = load_targets(input_paths, config.max_domains)
        output = stack.enter_context(LineStore(config.output_file, urls=True))
        discovered = stack.enter_context(LineStore(config.discovered_file)) if config.recursive else None
        log.info("Starting with %d targets, %d workers, %d existing URLs. Output: %s.",
                 len(targets), config.concurrency, len(output.seen), config.output_file)
        timeout = aiohttp.ClientTimeout(total=config.timeout, connect=min(10.0, config.timeout),
                                        sock_read=config.timeout)
        # Explicit threaded DNS works on Windows without deprecated loop policies.
        connector = aiohttp.TCPConnector(limit=config.concurrency, limit_per_host=2,
                                         ttl_dns_cache=300, resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(timeout=timeout, connector=connector,
                                         headers={"User-Agent": USER_AGENT},
                                         cookie_jar=aiohttp.DummyCookieJar(), trust_env=True) as session:
            stats = await Crawler(config, output, discovered).crawl(session, targets)
        log.info("Finished. %d unique URLs saved in %s.", len(output.seen), config.output_file)
        return stats


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Find valid sellers.json URLs from a site list.")
    parser.add_argument("--input", type=Path, default=Config.input_file, help="Site list (default: publishers.txt beside this script).")
    parser.add_argument("--output", type=Path, default=Config.output_file, help="Append-only URL list (default: exchanges.txt beside this script).")
    parser.add_argument("--concurrency", type=int, default=10, help="Maximum concurrent workers (default: 10).")
    parser.add_argument("--timeout", type=float, default=20, help="Total timeout per request in seconds (default: 20).")
    parser.add_argument("--retries", type=int, default=2, help="Additional attempts for transient failures (default: 2).")
    parser.add_argument("--max-sellers-mb", type=float, default=50, help="Maximum decoded sellers.json size in MiB (default: 50).")
    parser.add_argument("--max-ads-mb", type=float, default=5, help="Maximum decoded ads.txt size in MiB (default: 5).")
    parser.add_argument("--max-domains", type=int, default=100_000, help="Maximum unique sellers targets (default: 100000).")
    parser.add_argument("--discover", action="store_true", help="Also find ad systems in seed ads.txt and app-ads.txt files.")
    parser.add_argument("--recursive", action="store_true", help="Also follow sellers[].domain and scan discovered publisher ads files.")
    parser.add_argument("--discovered-file", type=Path, default=Config.discovered_file, help="Recursive state file (default: discovered_publishers.txt beside this script).")
    parser.add_argument("--https-only", action="store_true", help="Accept only HTTPS results; disable HTTP fallback.")
    parser.add_argument("--progress-interval", type=float, default=15, help="Seconds between progress logs (default: 15).")
    parser.add_argument("--verbose", action="store_true", help="Log individual HTTP, parsing, and size failures.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = argument_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    try:
        for name in ("max_sellers_mb", "max_ads_mb"):
            value = getattr(args, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"--{name.replace('_', '-')} must be finite and positive.")
        config = Config(input_file=args.input.resolve(), output_file=args.output.resolve(),
                        discovered_file=args.discovered_file.resolve(), concurrency=args.concurrency,
                        timeout=args.timeout, retries=args.retries,
                        max_sellers_bytes=int(args.max_sellers_mb * 1024 * 1024),
                        max_ads_bytes=int(args.max_ads_mb * 1024 * 1024),
                        max_domains=args.max_domains, discover=args.discover, recursive=args.recursive,
                        https_only=args.https_only, progress_interval=args.progress_interval,
                        verbose=args.verbose)
        config.validate()
    except (ValueError, OSError, OverflowError) as error:
        parser.error(str(error))

    # asyncio.Runner installs Ctrl+C cancellation and cleans up tasks and sockets.
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    try:
        with asyncio.Runner() as runner:
            async def execute() -> Stats:
                task = asyncio.current_task()

                def stop(signum, frame) -> None:
                    runner.get_loop().call_soon_threadsafe(task.cancel)

                signal.signal(signal.SIGTERM, stop)
                return await run(config)

            runner.run(execute())
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.warning("Interrupted. Saved results are available; rerun to check sites again.")
        return 130
    except Exception as error:
        log.error("Crawler failed: %s", error, exc_info=args.verbose or isinstance(error, ExceptionGroup))
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
