# fandom-cli

Helper scripts for downloading structured page and media data from Fandom / MediaWiki wikis.

## Usage

The CLI lives in `fandom.py` and exposes three sub-commands: `all-pages`, `all-media`, and `download-media`. The easiest way to run it is through `uv`, which will automatically install the dependencies declared in `pyproject.toml`:

```bash
uv run fandom.py --help
uv run fandom.py all-pages rezero               # write fandom-data/rezero/all_page_urls.json
uv run fandom.py all-media rezero --limit 250   # chunk media metadata under fandom-data/rezero
uv run fandom.py download-media rezero          # fetch files into fandom-data/rezero/media
```

Each command expects the wiki subdomain (e.g., `rezero`, `marvelstudios`, etc.). Results are written under `fandom-data/<wiki>/` so you can resume work or inspect the JSON artifacts later.

You can also invoke each command with `--limit` to cap processing during development, and `all-media` must be run before `download-media` so that the manifest exists.

Set request pauses and a download bandwidth cap explicitly for a slow archive:

```bash
uv run fandom.py all-media popnmusic --sleep-min 5 --sleep-max 15
uv run fandom.py download-media popnmusic --sleep-min 5 --sleep-max 20 --max-bytes-per-second 5000000
```

Downloads are sequential. The rate cap uses bytes per second (5 MB/s is
5,000,000 bytes/s), with reads paced in blocks of at most 64 KiB. Existing
completed files are skipped on restart; interrupted `.part` files are retried.
Fandom CDN requests explicitly select the original format to avoid automatic
WebP conversion. Slashes in wiki filenames are replaced with underscores.
Per-asset HTTP 400, 404 and 410 errors are recorded in the manifest and skipped;
temporary server/network errors retain exponential backoff.
