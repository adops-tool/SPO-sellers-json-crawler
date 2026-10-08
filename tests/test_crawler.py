"""Reliability tests using local HTTP responses and isolated temporary files."""

import asyncio
import gzip
import os
import socket
import ssl
import tempfile
import unittest
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
from aiohttp import web
from aiohttp.client_reqrep import ConnectionKey
from yarl import URL

import crawler


VALID_BODY = b'{"version":"1.0","sellers":[]}'


class InputAndStorageTests(unittest.TestCase):
    def test_hostname_normalization(self):
        examples = {
            " HTTPS://WWW.Example.COM/news?x=1 ": "www.example.com",
            "example.com.": "example.com",
            "b\u00fccher.de": "xn--bcher-kva.de",
            "xn--e1afmkfd.xn--p1ai": "xn--e1afmkfd.xn--p1ai",
            "https://example.com:443/": "example.com",
        }
        for raw, expected in examples.items():
            with self.subTest(raw=raw):
                self.assertEqual(crawler.clean_publisher(raw), expected)

    def test_invalid_domains_and_non_string_seller_values(self):
        for raw in (None, 42, {}, [], True, "", "# comment", "localhost", "127.0.0.1",
                    "https://user:password@example.com/", "ftp://example.com", "a..com",
                    "-bad.com", "example.123", "example.com:9999", "https://example.com:80",
                    "example .com", "https://example.com:wrong", "OWNERDOMAIN=example.com"):
            with self.subTest(raw=raw):
                self.assertIsNone(crawler.clean_publisher(raw))

    def test_input_dedupe_bom_comments_and_custom_endpoints(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sites.txt"
            content = "\ufeff# Sites\nExample.COM\nhttps://example.com/news\n\ninvalid\nhttps://cdn.example.org/Case/sellers.json?Key=A # endpoint\n"
            path.write_text(content, encoding="utf-8")
            before = path.read_bytes()
            with self.assertLogs("crawler", level="WARNING"):
                targets = crawler.load_targets([path], 10)
            self.assertEqual(targets, [crawler.Target("example.com"),
                                     crawler.Target("cdn.example.org", "/Case/sellers.json?Key=A")])
            self.assertEqual(path.read_bytes(), before)
            with self.assertRaisesRegex(ValueError, "limit"):
                crawler.load_targets([path], 1)

    def test_empty_input_is_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sites.txt"
            path.write_text("# Empty\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "No valid sites"):
                crawler.load_targets([path], 10)

    def test_ads_rows_variables_and_deduplication(self):
        body = """\ufeffexchange.example, 123, DIRECT # comment
        exchange.example, 456, RESELLER
        www.partner.example, 1, direct
        OWNERDOMAIN = owner.example
        MANAGERDOMAIN=manager.example, exchange.example
        contact=support@example.com
        invalid.example
        malformed.example, , DIRECT
        bogus.example, 123, UNKNOWN
        """
        self.assertEqual(crawler.parse_ads_txt(body), ["exchange.example", "www.partner.example",
                                                     "owner.example", "manager.example"])

    def test_sellers_structure_and_bom(self):
        self.assertEqual(crawler.parse_sellers_json(b"\xef\xbb\xbf" + VALID_BODY)["sellers"], [])
        for body in (b"{}", b"[]", b'{"sellers":{}}', b'{"sellers":[1]}',
                     b'{"sellers":[],"bad":NaN}', b'{"sellers":[],"bad":Infinity}',
                     b'{"sellers":', b'{"sellers":[],"name":"\xff"}'):
            with self.subTest(body=body), self.assertRaises((ValueError, UnicodeError)):
                crawler.parse_sellers_json(body)
        data = {"sellers": [{"domain": "example.com"}, {"domain": 123}, {"domain": None}, {}]}
        self.assertEqual(list(crawler.extract_publishers_from_sellers(data)), ["example.com"])

    def test_retry_after_seconds_dates_and_delay_cap(self):
        config = crawler.Config(retry_backoff=0, max_retry_delay=60)
        self.assertEqual(crawler.retry_delay("0", 0, config), 0)
        self.assertEqual(crawler.retry_delay("99999", 0, config), 60)
        future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True)
        self.assertGreater(crawler.retry_delay(future, 0, config), 27)
        for header in ("invalid", "nan", "inf", "-1"):
            self.assertEqual(crawler.retry_delay(header, 0, config), 0)

    def test_config_rejects_collisions_and_nonfinite_values(self):
        for config in (crawler.Config(concurrency=0), crawler.Config(timeout=float("nan")),
                       crawler.Config(timeout=float("inf")), crawler.Config(retries=-1),
                       crawler.Config(input_file=Path("same.txt"), output_file=Path("same.txt")),
                       crawler.Config(input_file=Path("result.txt.lock"), output_file=Path("result.txt")),
                       crawler.Config(output_file=Path("same.txt"), discovered_file=Path("same.txt"), recursive=True)):
            with self.subTest(config=config), self.assertRaises(ValueError):
                config.validate()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "input.txt"
            alias = Path(directory) / "output.txt"
            source.write_text("example.com\n", encoding="utf-8")
            os.link(source, alias)
            with self.assertRaises(ValueError):
                crawler.Config(input_file=source, output_file=alias).validate()

    def test_output_dedupe_preserves_path_case_and_repairs_newline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.txt"
            path.write_text("HTTPS://EXAMPLE.COM/Case/sellers.json", encoding="utf-8")
            with crawler.LineStore(path, urls=True) as store:
                self.assertEqual(store.append(["https://example.com/Case/sellers.json"]), 0)
                self.assertEqual(store.append(["https://example.com/case/sellers.json"] * 2), 1)
            self.assertEqual(path.read_text(encoding="utf-8").splitlines(),
                             ["HTTPS://EXAMPLE.COM/Case/sellers.json", "https://example.com/case/sellers.json"])

    def test_failed_fsync_does_not_acknowledge_result(self):
        with tempfile.TemporaryDirectory() as directory:
            with crawler.LineStore(Path(directory) / "results.txt", urls=True) as store:
                with patch("crawler.os.fsync", side_effect=OSError("Disk full")):
                    with self.assertRaisesRegex(OSError, "Disk full"):
                        store.append(["https://example.com/sellers.json"])
                self.assertEqual(store.seen, set())

    def test_output_lock_rejects_another_writer_and_releases(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.txt"
            with crawler.OutputLock(path):
                with self.assertRaisesRegex(OSError, "another crawler"):
                    with crawler.OutputLock(path):
                        self.fail("The second writer should not acquire the lock.")
            with crawler.OutputLock(path):
                pass

    @patch("crawler.logging.basicConfig")
    def test_cli_uses_runner_and_reports_errors(self, basic_config):
        with patch("crawler.run", new=AsyncMock(return_value=crawler.Stats())):
            self.assertEqual(crawler.main([]), 0)
        with patch("crawler.run", new=AsyncMock(side_effect=OSError("Disk full"))):
            with self.assertLogs("crawler", level="ERROR") as messages:
                self.assertEqual(crawler.main([]), 1)
            self.assertIn("Disk full", messages.output[0])


class LoopbackResolver(aiohttp.abc.AbstractResolver):
    """Resolve test hostnames to an ephemeral local server, including its port."""

    def __init__(self, port):
        self.port = port

    async def resolve(self, host, port=0, family=socket.AF_INET):
        return [{"hostname": host, "host": "127.0.0.1", "port": self.port,
                 "family": socket.AF_INET, "proto": 0, "flags": 0}]

    async def close(self):
        pass


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.calls = Counter()
        app = web.Application()
        app.router.add_get("/{path:.*}", self.respond)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1))
        self.config = crawler.Config(retries=2, retry_backoff=0)
        self.stats = crawler.Stats()

    async def asyncTearDown(self):
        await self.session.close()
        await self.runner.cleanup()

    async def respond(self, request):
        path = request.path
        self.calls[path] += 1
        if path == "/chunked":
            response = web.StreamResponse(headers={"Content-Type": "application/json"})
            await response.prepare(request)
            for part in (b'{"version":"1.0",', b'"sellers":', b'[]}'):
                await response.write(part)
                await asyncio.sleep(0.01)
            await response.write_eof()
            return response
        if path == "/redirect":
            raise web.HTTPFound("/Case/sellers.json")
        if path == "/redirect-loop":
            raise web.HTTPFound("/redirect-loop")
        if path in {"/retry", "/rate-limit"} and self.calls[path] < 3:
            return web.Response(status=429 if path == "/rate-limit" else 503,
                                headers={"Retry-After": "0"})
        if path == "/always-fails":
            return web.Response(status=503)
        if path == "/missing":
            return web.Response(status=404)
        if path == "/html":
            return web.Response(text="<html>Not found</html>", content_type="text/html")
        if path == "/compressed":
            return web.Response(body=gzip.compress(b"x" * 1000),
                                headers={"Content-Encoding": "gzip", "Content-Type": "application/json"})
        if path == "/oversized":
            return web.Response(body=b"x" * 1000)
        if path == "/slow":
            await asyncio.sleep(0.1)
        if path == "/incomplete":
            response = web.StreamResponse(headers={"Content-Length": "1000"})
            await response.prepare(request)
            await response.write(b"short")
            request.transport.close()
            return response
        return web.Response(body=VALID_BODY, content_type="application/json")

    async def fetch(self, path, cap=2048):
        return await crawler.request_body(self.session, self.base + path, cap, self.config, self.stats)

    async def test_reads_chunked_body_until_eof(self):
        result = await self.fetch("/chunked")
        self.assertEqual(crawler.parse_sellers_json(result[1])["sellers"], [])

    async def test_records_final_redirect_url_with_original_path_case(self):
        result = await self.fetch("/redirect")
        self.assertEqual(result[0], self.base + "/Case/sellers.json")

    async def test_retries_transient_status_and_retry_after(self):
        for path in ("/retry", "/rate-limit"):
            self.assertIsNotNone(await self.fetch(path))
            self.assertEqual(self.calls[path], 3)
        self.assertEqual(self.stats.retries, 4)

    async def test_retry_budget_includes_initial_attempt(self):
        self.assertIsNone(await self.fetch("/always-fails"))
        self.assertEqual(self.calls["/always-fails"], 3)

    async def test_missing_html_and_oversized_bodies_are_not_retried(self):
        for path in ("/missing", "/html", "/oversized", "/compressed"):
            self.assertIsNone(await self.fetch(path, cap=100))
            self.assertEqual(self.calls[path], 1)

    async def test_redirect_loop_is_not_retried(self):
        self.assertIsNone(await self.fetch("/redirect-loop"))
        self.assertEqual(self.stats.retries, 0)

    async def test_timeout_is_bounded_and_retried(self):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.02)) as session:
            self.assertIsNone(await crawler.request_body(session, self.base + "/slow", 100,
                                                        self.config, self.stats))
        self.assertEqual(self.stats.requests, 3)
        self.assertEqual(self.stats.retries, 2)

    async def test_incomplete_payload_is_retried_and_never_accepted(self):
        self.assertIsNone(await self.fetch("/incomplete"))
        self.assertEqual(self.calls["/incomplete"], 3)

    async def test_certificate_failure_is_not_retried(self):
        key = ConnectionKey("example.com", 443, True, True, None, None, None)
        error = aiohttp.ClientConnectorCertificateError(key, ssl.CertificateError("Invalid certificate"))
        session = Mock()
        session.get.side_effect = error
        self.assertIsNone(await crawler.request_body(session, "https://example.com/sellers.json", 100,
                                                    self.config, self.stats))
        self.assertEqual(session.get.call_count, 1)

    async def test_https_failure_falls_back_to_http(self):
        with patch("crawler.request_body", new=AsyncMock(side_effect=[None, ("http://example.com/Case/sellers.json", VALID_BODY)])) as request:
            result = await crawler.fetch_sellers_json(self.session, crawler.Target("example.com"),
                                                     self.config, self.stats)
        self.assertEqual(result[0], "http://example.com/Case/sellers.json")
        self.assertEqual([call.args[1] for call in request.await_args_list],
                         ["https://example.com/sellers.json", "http://example.com/sellers.json"])

    async def test_unrelated_json_is_not_accepted(self):
        with patch("crawler.request_body", new=AsyncMock(return_value=("https://example.com/sellers.json", b"{}"))):
            self.assertIsNone(await crawler.fetch_sellers_json(self.session, crawler.Target("example.com"),
                                                              self.config, self.stats))

    async def test_https_only_does_not_try_http_fallback(self):
        with patch("crawler.request_body", new=AsyncMock(return_value=None)) as request:
            self.assertIsNone(await crawler.fetch_sellers_json(self.session, crawler.Target("example.com"),
                                                              replace(self.config, https_only=True), self.stats))
        self.assertEqual(request.await_count, 1)

    async def test_https_only_blocks_downgrade_before_the_http_request(self):
        response = Mock(status=302, url=URL("https://example.com/sellers.json"),
                        headers={"Location": "http://example.com/sellers.json"})
        context = AsyncMock()
        context.__aenter__.return_value = response
        session = Mock()
        session.get.return_value = context
        result = await crawler.request_body(session, "https://example.com/sellers.json", 100,
                                            replace(self.config, https_only=True), self.stats)
        self.assertIsNone(result)
        self.assertEqual(session.get.call_count, 1)
        self.assertEqual(self.stats.retries, 0)


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.seed_path = self.directory / "sites.txt"
        self.output_path = self.directory / "results.txt"
        self.discovery_path = self.directory / "discovered.txt"
        self.seed_path.write_text("seed.example\n", encoding="utf-8")
        self.calls = Counter()
        app = web.Application()
        app.router.add_get("/{path:.*}", self.respond)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]
        self.config = crawler.Config(input_file=self.seed_path, output_file=self.output_path,
                                     discovered_file=self.discovery_path, concurrency=1,
                                     retries=0, timeout=1, progress_interval=60)

    async def asyncTearDown(self):
        await self.runner.cleanup()
        self.temporary.cleanup()

    async def respond(self, request):
        domain = request.host
        self.calls[domain, request.path] += 1
        if request.path == "/sellers.json":
            graph = {"seed.example": ["second.example", "second.example", "seed.example"],
                     "second.example": ["seed.example", "exchange.example"],
                     "exchange.example": []}
            sellers = [{"domain": domain, "seller_id": str(index), "seller_type": "PUBLISHER"}
                       for index, domain in enumerate(graph.get(domain, []))]
            return web.json_response({"version": "1.0", "sellers": sellers})
        if request.path == "/ads.txt":
            return web.Response(text="exchange.example, 1, DIRECT\nexchange.example, 2, RESELLER\n",
                                content_type="text/plain")
        return web.Response(status=404)

    async def local_crawl(self, config):
        connector = aiohttp.TCPConnector(resolver=LoopbackResolver(self.port), use_dns_cache=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            with crawler.LineStore(self.output_path, urls=True) as output:
                with crawler.LineStore(self.discovery_path) as discovered:
                    instance = crawler.Crawler(config, output, discovered if config.recursive else None)
                    # HTTP transport is local; fallback and TLS errors are tested separately.
                    with patch.object(crawler.Target, "urls", lambda target, https_only=False:
                                      iter([f"http://{target.domain}{target.endpoint}"])):
                        stats = await asyncio.wait_for(instance.crawl(session, [crawler.Target("seed.example")]), 3)
        return instance, stats

    async def test_default_checks_only_input_sites_and_rechecks_without_duplicates(self):
        _, stats = await self.local_crawl(self.config)
        self.assertEqual(stats.targets_checked, 1)
        self.assertEqual(stats.ads_checked, 0)
        self.assertEqual(self.calls, Counter({("seed.example", "/sellers.json"): 1}))
        _, second = await self.local_crawl(self.config)
        self.assertEqual(second.targets_checked, 1)
        self.assertEqual(second.new_urls, 0)
        self.assertEqual(len(self.output_path.read_text().splitlines()), 1)
        self.assertEqual(self.seed_path.read_text(), "seed.example\n")

    async def test_discovery_follows_ads_but_not_seller_domains(self):
        instance, stats = await self.local_crawl(replace(self.config, discover=True))
        self.assertEqual({target.domain for target in instance.scheduled}, {"seed.example", "exchange.example"})
        self.assertEqual(stats.ads_checked, 1)
        self.assertEqual(stats.targets_checked, 2)

    async def test_recursive_cycle_completes_with_one_worker_and_no_duplicates(self):
        instance, stats = await self.local_crawl(replace(self.config, recursive=True))
        self.assertEqual(stats.targets_checked, 3)
        self.assertEqual(stats.ads_checked, 3)
        self.assertEqual(instance.queue.qsize(), 0)
        self.assertTrue(all(count == 1 for count in self.calls.values()))
        self.assertEqual(len(self.output_path.read_text().splitlines()), 3)
        self.assertIn("second.example", self.discovery_path.read_text().splitlines())
        self.assertEqual(self.seed_path.read_text(), "seed.example\n")

    async def test_target_limit_stops_growth_without_hanging(self):
        with self.assertLogs("crawler", level="WARNING"):
            instance, stats = await self.local_crawl(replace(self.config, recursive=True, max_domains=2))
        self.assertEqual(len(instance.scheduled), 2)
        self.assertEqual(stats.targets_checked, 2)
        self.assertTrue(stats.limit_reached)

    async def test_full_run_loads_recursive_state_and_preserves_input(self):
        self.discovery_path.write_text("saved.example\n", encoding="utf-8")
        before = self.seed_path.read_bytes()
        with patch("crawler.aiohttp.ThreadedResolver", side_effect=lambda: LoopbackResolver(self.port)):
            with patch.object(crawler.Target, "urls", lambda target, https_only=False:
                              iter([f"http://{target.domain}{target.endpoint}"])):
                with patch.dict(os.environ, {"NO_PROXY": "*", "no_proxy": "*"}):
                    stats = await asyncio.wait_for(crawler.run(replace(self.config, recursive=True)), 3)
        self.assertEqual(stats.targets_checked, 4)
        self.assertEqual(self.seed_path.read_bytes(), before)
        self.assertIn(("saved.example", "/sellers.json"), self.calls)
        with crawler.OutputLock(self.output_path):
            pass

    async def test_output_failure_propagates_instead_of_deadlocking(self):
        output = Mock()
        output.append.side_effect = OSError("Disk full")
        instance = crawler.Crawler(self.config, output, None)
        result = ("https://seed.example/sellers.json", {"sellers": []})
        with patch("crawler.fetch_sellers_json", new=AsyncMock(return_value=result)):
            with self.assertRaises(ExceptionGroup) as error:
                await asyncio.wait_for(instance.crawl(Mock(), [crawler.Target("seed.example")]), 1)
        self.assertIsInstance(error.exception.exceptions[0], OSError)
        self.assertFalse(any(task.get_name().startswith("crawler-") for task in asyncio.all_tasks()))

    async def test_cancellation_keeps_saved_results_and_cleans_up_workers(self):
        blocked = asyncio.Event()

        async def fetch(session, target, config, stats):
            if target.domain == "seed.example":
                return "https://seed.example/sellers.json", {"sellers": []}
            blocked.set()
            await asyncio.Event().wait()

        with crawler.LineStore(self.output_path, urls=True) as output:
            instance = crawler.Crawler(self.config, output, None)
            with patch("crawler.fetch_sellers_json", side_effect=fetch):
                task = asyncio.create_task(instance.crawl(Mock(), [crawler.Target("seed.example"),
                                                                  crawler.Target("slow.example")]))
                await asyncio.wait_for(blocked.wait(), 1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        self.assertEqual(self.output_path.read_text().splitlines(), ["https://seed.example/sellers.json"])
        self.assertFalse(any(task.get_name().startswith("crawler-") for task in asyncio.all_tasks()))


if __name__ == "__main__":
    unittest.main()
