# Architecture and Reliability

## Crawl modes

Each input entry becomes a `Target(domain, endpoint)`. Ordinary site URLs use
`/sellers.json`; explicitly supplied sellers.json paths and query strings are
retained. Hostnames preserve `www`, normalize case, and support IDNA. Input is
deduplicated before workers start. Invalid lines are counted and reported.

The default mode schedules one sellers check per target. `--discover` also
schedules an ads scan per input hostname; that scan reads ads.txt and app-ads.txt
and schedules the referenced sellers targets. `--recursive` additionally
schedules domains in valid sellers arrays and scans their ads files. Recursive
state lives in a separate discovery file and is loaded on subsequent recursive
runs. The original seed list is never opened for writing.

## One queue and structured task lifetime

One `asyncio.Queue` contains both `sellers` and `ads` jobs. Workers schedule all
derived jobs before calling `task_done()` for their current job. Consequently,
`queue.join()` returns only when all current and derived work is complete,
including cycles. A separate global work counter is unnecessary.

Targets and ads scans are deduplicated before enqueueing. No await separates
the membership test from set insertion, so concurrent coroutines cannot schedule
duplicates. The number of sellers targets is capped by `--max-domains`, and
there is at most one additional ads job per hostname. The queue is unbounded
within this explicit ceiling so workers never deadlock while producing children.

`asyncio.TaskGroup` owns workers and the progress logger. An unexpected worker
exception cancels its siblings and propagates to the caller. Successful queue
drain cancels idle workers. Cancellation also cleans up the session, files,
and locks. Results are written directly in the worker, so there is no separate
writer task whose failure could strand queued output.

## HTTP handling

All workers share one `aiohttp.ClientSession`, with a connection limit equal to
the worker count, at most two connections per host, and a 300-second DNS cache.
The session uses threaded DNS for portability, no cookie persistence, verified
TLS, and environment proxy settings. `asyncio.Runner` manages the event loop
without deprecated Windows event loop policy overrides.

Every candidate starts with HTTPS and optionally falls back to HTTP. Redirects
are bounded to five, with a timeout covering the complete chain and body.
In HTTPS-only mode, HTTP redirects are blocked before sending a request.
The resolved URL is saved without lowercasing its path.
HTTP 200 alone is insufficient: HTML/XML media types are rejected, and JSON
must be valid UTF-8 with a top-level object containing an array of seller
objects. UTF-8 BOMs and empty seller lists are accepted. NaN, Infinity, malformed
JSON, unrelated JSON, and non-object seller rows are rejected. This is structural
validation, not a full compliance audit of required IAB seller fields.

Bodies are read in chunks until EOF; a single `read(n)` is insufficient because
it may return before the whole response arrives. Size limits are enforced on
decoded bytes while streaming, including chunked and compressed responses.
An oversized Content-Length is rejected early for uncompressed responses.
Large JSON parsing runs in a thread to keep the event loop responsive. Parsed
objects and in-flight bodies still use memory; the body ceiling is not a cap on
total process memory.

HTTP 408, 429, 500, 502, 503, and 504, connection failures, incomplete payloads,
and timeouts receive up to `1 + retries` attempts per URL. Backoff is exponential
with jitter. `Retry-After` supports seconds and HTTP dates, capped at 60 seconds.
Responses are released before retry delays. Certificate failures, invalid URLs,
redirect loops, permanent statuses, invalid JSON, and oversized bodies are not
retried on the same candidate. They may still lead to HTTP fallback. Cancellation
is allowed to propagate instead of being treated as an ordinary fetch failure.

Ads parsing accepts complete rows with a non-empty seller ID and a DIRECT or
RESELLER relationship, plus OWNERDOMAIN and MANAGERDOMAIN declarations. Other
variable lines and malformed rows do not become requests. HTML-looking ads
responses are rejected even if mislabeled as text/plain.

## Persistence

`LineStore` loads prior lines into its dedupe set, appends new lines, flushes,
and calls `os.fsync()` before acknowledging the append. Duplicate lines already
present in old files are not rewritten or removed. A missing trailing newline
is repaired before appending. Invalid historical lines are reported and ignored
for deduplication, while their original contents are preserved.

Results are cumulative and all input sites are rechecked on every run; previous
success does not prevent current verification. Recursive discovery batches are
synced before a worker finishes. On an interruption, saved recursive state can
be loaded again. Abrupt termination during a write may still leave a partial
last line; flush and fsync do not make multi-byte appends atomic.

OS file locks serialize access to output and recursive state, including across
processes. Closing the process releases the lock automatically; lock files are
not deleted, avoiding lock-inode races. File paths, hard links, and generated
lock paths are checked for collisions so output cannot overwrite the input.

## Configuration

Run `python crawler.py --help` for all options. Defaults:

| Setting | Default |
| --- | --- |
| Input | `publishers.txt` beside crawler.py |
| Output | `exchanges.txt` beside crawler.py |
| Recursive state | `discovered_publishers.txt` beside crawler.py |
| Concurrent workers | 10 |
| Total timeout per request | 20 seconds |
| Additional retry attempts | 2 |
| Initial backoff | 1 second plus jitter |
| Maximum retry delay | 60 seconds |
| Sellers decoded body limit | 50 MiB |
| Ads decoded body limit | 5 MiB |
| Unique sellers target limit | 100,000 |
| Progress interval | 15 seconds |

Numeric arguments must be finite and positive, except retries which may be
zero. Runtime failure and unexpected task exceptions produce a nonzero exit.

References: [aiohttp streaming behavior](https://docs.aiohttp.org/en/stable/streams.html),
[aiohttp client configuration](https://docs.aiohttp.org/en/stable/client_advanced.html),
and the [IAB sellers.json specification](https://iabtechlab.com/wp-content/uploads/2019/07/Sellers.json_Final.pdf).
