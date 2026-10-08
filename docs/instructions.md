# Operations Guide

## Setup

Install Python 3.11+ and create a virtual environment in the project folder.
Only `aiohttp` is required; standard threaded DNS works on Windows and Linux.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

On Linux/macOS, use `python3` and `.venv/bin/python` instead. If an environment
was copied from another computer and reports a missing Python executable,
recreate it with `python -m venv .venv` and reinstall the requirements.

## Prepare the list

Edit `publishers.txt` with one domain or site URL per line. Blank lines and
comments are supported. Explicit sellers.json URLs preserve their endpoint.
The existing `ALLSSP.md` is a plain list of endpoints and can be used as input.

```powershell
.\.venv\Scripts\python.exe crawler.py --input publishers.txt --output exchanges.txt
```

By default, the crawler does not expand the list. Use `--discover` to also scan
the input domains' `/ads.txt` and `/app-ads.txt` files. Use `--recursive` to
follow seller domains, scan their ads files, and save recursive state separately:

```powershell
.\.venv\Scripts\python.exe crawler.py --recursive --discovered-file campaign_domains.txt
```

The seed list is read-only. Existing recursive discoveries are loaded again
only in recursive mode. A separate output and discovery file per campaign keeps
results and crawl state independent.

## Monitor and stop

Startup, periodic progress, and completion appear in English logs. Progress
counts targets checked, valid files, new URLs, ads scans, discovered domains,
request attempts, retries, and queued jobs. A checked target may have failed;
the count of valid files shows successful checks.

Press `Ctrl+C` once. On Unix, `SIGTERM` also cancels the crawl cleanly. The
process returns 130 after interruption, 1 for a runtime failure, 2 for invalid
arguments, and 0 for a completed scan. A site returning 404 or invalid JSON is
a failed individual check and does not make the whole run fail.

Windows Task Manager force termination, Unix `SIGKILL`, and power loss cannot
run cleanup handlers. Successfully synced batches remain on disk, but an
interrupted write can leave a partial last line. Invalid existing lines are
reported and ignored for deduplication; they are not automatically deleted.

To collect logs in PowerShell:

```powershell
.\.venv\Scripts\python.exe crawler.py 2>&1 | Tee-Object -FilePath crawler.log
```

To keep a Unix crawl running after disconnecting, use a terminal session manager:

```bash
tmux new -s spo-crawler
.venv/bin/python crawler.py --recursive
# Detach with Ctrl+B, then D. Reattach with:
tmux attach -t spo-crawler
```

## Rerun and fresh snapshots

Run the same command again to recheck sites and append only new URLs. Historical
output is cumulative: it is not a guarantee that every saved URL still works.
For a fresh scan, choose a new `--output` path. Recursive reruns also load the
discovery state file, but completed/failed requests are not individually cached.

## Troubleshooting

- **Nothing found:** use a small list and `--verbose`. Many publisher sites do
  not host sellers.json themselves; `--discover` can find their ad systems.
- **Timeouts or rate limits:** decrease `--concurrency` or increase `--timeout`.
  Temporary failures are retried twice by default; `Retry-After` waits are capped
  at 60 seconds. Each HTTPS/HTTP candidate has its own retry budget.
- **Large feeds rejected:** increase `--max-sellers-mb`. Limits apply to decoded
  bytes, including compressed responses. JSON objects can use several times
  the body size in memory, so reduce concurrency for large feeds.
- **Target limit reached:** increase `--max-domains` if you intend a wider crawl.
  A target is a unique hostname plus sellers endpoint; each can also produce
  one ads scan per hostname. An input above the limit is rejected explicitly.
- **Certificate errors:** fix your local certificate trust configuration or
  the server certificate. HTTPS certificate verification stays enabled.
- **Cannot lock output:** another process may be using the same collection, or
  the lock file is not writable. Choose a different output or stop the other
  process. A leftover `.lock` filename does not mean its OS lock is held.
- **Disk/permission errors:** fix the output directory or available space. These
  failures are fatal and do not leave an orphaned writer waiting forever.
- **Proxy configuration:** the shared session uses `trust_env=True`, so standard
  `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` variables are honored by aiohttp.

## Test the installation

```powershell
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe crawler.py --help
```
