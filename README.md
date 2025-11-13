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
