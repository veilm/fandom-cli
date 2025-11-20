#!/usr/bin/env python3
"""Minimal CLI helpers for working with Fandom / MediaWiki APIs."""

from __future__ import annotations

import argparse
import json
import random
import shutil
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Iterator, List

import httpx

API_TIMEOUT = 30
REQUEST_DELAY = 0.2  # seconds between paged requests to stay polite
MEDIA_DELAY_RANGE = (1.0, 10.0)
DOWNLOAD_DELAY_RANGE = (1.0, 20.0)
DOWNLOAD_LOG_INTERVAL = 50
MAX_BACKOFF_SECONDS = 2 * 60 * 60  # 2 hours
MIN_FREE_BYTES = 10 * 1024**3  # 10 GB


class DownloadNotFoundError(Exception):
    """Raised when the remote server reports the asset does not exist."""

    def __init__(self, url: str) -> None:
        super().__init__(f"Asset not found: {url}")
        self.url = url


def iter_all_pages(wiki: str, client: httpx.Client) -> Iterator[Dict[str, str]]:
    params: Dict[str, str] = {
        "action": "query",
        "format": "json",
        "list": "allpages",
        "aplimit": "max",
        "apnamespace": "0",
    }
    cont: Dict[str, str] = {}
    while True:
        resp = client.get(
            f"https://{wiki}.fandom.com/api.php",
            params={**params, **cont},
        )
        resp.raise_for_status()
        payload = resp.json()
        for page in payload["query"]["allpages"]:
            yield page
        if "continue" not in payload:
            break
        cont = payload["continue"]
        time.sleep(REQUEST_DELAY)


def command_all_pages(args: argparse.Namespace) -> None:
    print(
        f"[all-pages] Fetching namespace 0 pages for {args.wiki}. "
        "Logs will appear roughly every request."
    )
    pages: List[Dict[str, str]] = []
    headers = {"User-Agent": "fandom-cli/0.1 (+https://github.com/user/project)"}
    with httpx.Client(timeout=API_TIMEOUT, headers=headers) as client:
        for entry in iter_all_pages(args.wiki, client):
            title = entry["title"]
            slug = title.replace(" ", "_")
            entry["url"] = (
                f"https://{args.wiki}.fandom.com/wiki/"
                f"{urllib.parse.quote(slug, safe=':/%')}"
            )
            pages.append(entry)
    out_dir = Path("fandom-data") / args.wiki
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "all_page_urls.json"
    out_file.write_text(json.dumps(pages, indent=2), encoding="utf-8")
    print(f"Wrote {len(pages)} pages to {out_file}")


def command_all_media(args: argparse.Namespace) -> None:
    print(
        f"[all-media] Fetching all files for {args.wiki}. "
        "Writing chunks to disk and logging every 10 chunks."
    )
    headers = {"User-Agent": "fandom-cli/0.1 (+https://github.com/user/project)"}
    out_dir = Path("fandom-data") / args.wiki
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = out_dir / ".media-chunks"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    params: Dict[str, str] = {
        "action": "query",
        "format": "json",
        "list": "allimages",
        "aiprop": "url|mime|size|sha1|timestamp|user|comment",
        "ailimit": "max",
    }
    cont: Dict[str, str] = {}
    chunk_idx = 0
    total = 0
    completed = False

    with httpx.Client(timeout=API_TIMEOUT, headers=headers) as client:
        while True:
            resp = client.get(
                f"https://{args.wiki}.fandom.com/api.php",
                params={**params, **cont},
            )
            resp.raise_for_status()
            payload = resp.json()
            images = payload["query"]["allimages"]

            if args.limit is not None:
                remaining = args.limit - total
                if remaining <= 0:
                    completed = True
                    break
                images = images[:remaining]

            if not images:
                completed = "continue" not in payload
                break

            for entry in images:
                entry["descriptionurl"] = entry.get(
                    "descriptionurl",
                    f"https://{args.wiki}.fandom.com/wiki/"
                    f"{urllib.parse.quote(entry['title'].replace(' ', '_'), safe=':/%')}",
                )

            chunk_path = tmp_dir / f"chunk-{chunk_idx:05d}.json"
            chunk_path.write_text(json.dumps(images, indent=2), encoding="utf-8")
            chunk_idx += 1
            total += len(images)
            if chunk_idx % 10 == 0:
                print(f"...chunk {chunk_idx} written ({total} media so far)")

            if args.limit is not None and total >= args.limit:
                completed = True
                break

            if "continue" not in payload:
                completed = True
                break

            cont = payload["continue"]
            time.sleep(random.uniform(*MEDIA_DELAY_RANGE))

    if not completed:
        print(
            f"Stopped after {total} media entries; partial chunks left in {tmp_dir} for inspection."
        )
        return

    media: List[Dict[str, str]] = []
    for chunk_file in sorted(tmp_dir.glob("chunk-*.json")):
        media.extend(json.loads(chunk_file.read_text(encoding="utf-8")))

    out_file = out_dir / "all_media_urls.json"
    out_file.write_text(json.dumps(media, indent=2), encoding="utf-8")
    shutil.rmtree(tmp_dir)
    print(f"Wrote {len(media)} media entries to {out_file}")


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for file in path.rglob("*"):
        if file.is_file():
            total += file.stat().st_size
    return total


def _human_bytes(num: int) -> str:
    step = 1024
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num < step or unit == "TB":
            return f"{num:.2f} {unit}"
        num /= step
    return f"{num:.2f} TB"


def _format_eta(seconds: float) -> str:
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _download_file(client: httpx.Client, url: str, dest: Path) -> int:
    tmp_path = dest.with_suffix(dest.suffix + ".part")
    with client.stream("GET", url, follow_redirects=True) as resp:
        resp.raise_for_status()
        with tmp_path.open("wb") as fh:
            for chunk in resp.iter_bytes():
                fh.write(chunk)
    tmp_path.replace(dest)
    return dest.stat().st_size


def _download_with_backoff(client: httpx.Client, url: str, dest: Path) -> int:
    delay = 5.0
    attempt = 1
    while True:
        try:
            return _download_file(client, url, dest)
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status == 404:
                raise DownloadNotFoundError(url) from None
            msg = f"HTTP {status}"
        except httpx.HTTPError as exc:
            msg = f"Network error: {exc}"
        except Exception as exc:  # pylint: disable=broad-exception-caught
            msg = f"Unexpected error: {exc}"
        print(
            f"[download-media] Download failed ({msg}) on attempt {attempt}. "
            f"Next retry in {int(delay)}s."
        )
        if delay >= MAX_BACKOFF_SECONDS:
            raise RuntimeError(
                "Backoff exceeded 2 hours between attempts; aborting downloads."
            ) from None
        time.sleep(delay)
        delay = min(delay * 2, MAX_BACKOFF_SECONDS)
        attempt += 1


def _destination_for_entry(media_dir: Path, entry: Dict[str, Any]) -> Path:
    name = entry.get("name") or entry.get("title", "file")
    sanitized = str(name).replace(" ", "_")
    sha1 = entry.get("sha1")
    filename = f"{sha1}_{sanitized}" if sha1 else sanitized
    return media_dir / filename


def _get_next_pending_entry(
    media_entries: List[Dict[str, Any]], media_dir: Path
) -> tuple[Dict[str, Any], Path] | None:
    for entry in media_entries:
        url = entry.get("url")
        if not url:
            continue
        if entry.get("failure") is not None:
            continue
        dest = _destination_for_entry(media_dir, entry)
        if dest.exists():
            continue
        return entry, dest
    return None


def _log_download_progress(
    media_dir: Path,
    completed_entries: int,
    total_entries: int,
    bytes_on_disk: int,
) -> None:
    percent = (completed_entries / total_entries) * 100 if total_entries else 0.0
    eta_seconds = max(total_entries - completed_entries, 0) * 11
    free_bytes = shutil.disk_usage(media_dir).free
    print(
        f"[download-media] {completed_entries}/{total_entries} entries ({percent:.2f}%); "
        f"{_human_bytes(bytes_on_disk)} stored in {media_dir}; "
        f"ETA ~{_format_eta(eta_seconds)}; free space {_human_bytes(free_bytes)}."
    )
    if free_bytes < MIN_FREE_BYTES:
        raise RuntimeError(
            f"Less than {_human_bytes(MIN_FREE_BYTES)} free on the target filesystem; stopping downloads."
        )


def command_download_media(args: argparse.Namespace) -> None:
    wiki = args.wiki
    base_dir = Path("fandom-data") / wiki
    manifest = base_dir / "all_media_urls.json"
    if not manifest.exists():
        print(
            f"[download-media] Manifest not found at {manifest}. "
            "Run 'all-media' first."
        )
        raise SystemExit(1)

    media = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(media, list) or not media:
        print(f"[download-media] Manifest at {manifest} is empty; nothing to download.")
        return
    if args.limit:
        media = media[: args.limit]

    total_entries = len(media)
    media_dir = base_dir / "media"
    media_dir.mkdir(parents=True, exist_ok=True)
    bytes_on_disk = _dir_size_bytes(media_dir)
    print(
        f"[download-media] Starting download for {total_entries} files on {wiki}. "
        f"Random delay {DOWNLOAD_DELAY_RANGE[0]:.0f}-{DOWNLOAD_DELAY_RANGE[1]:.0f}s between downloads; "
        f"logging every {DOWNLOAD_LOG_INTERVAL} downloads. Saving under {media_dir}."
    )

    headers = {"User-Agent": "fandom-cli/0.1 (+https://github.com/user/project)"}
    completed_entries = 0
    downloaded_files = 0
    downloads_since_log = 0
    client_timeout = httpx.Timeout(120.0, connect=30.0)

    try:
        with httpx.Client(timeout=client_timeout, headers=headers) as client:
            for entry in media:
                completed_entries += 1
                url = entry.get("url")
                if not url:
                    continue
                if entry.get("failure") is not None:
                    continue
                dest = _destination_for_entry(media_dir, entry)
                if dest.exists():
                    continue
                if downloaded_files > 0:
                    time.sleep(random.uniform(*DOWNLOAD_DELAY_RANGE))
                try:
                    size = _download_with_backoff(client, url, dest)
                except DownloadNotFoundError:
                    entry["failure"] = 404
                    manifest.write_text(json.dumps(media, indent=2), encoding="utf-8")
                    print(
                        "[download-media] Recorded 404 for this entry; it will be skipped:"
                    )
                    print(json.dumps(entry, indent=2, sort_keys=True))
                    print(f"[download-media] Intended destination: {dest}")
                    continue
                except RuntimeError as exc:
                    if "aborting downloads" in str(exc).lower():
                        print(
                            "[download-media] Download aborted while fetching this entry:"
                        )
                        print(json.dumps(entry, indent=2, sort_keys=True))
                        print(f"[download-media] Intended destination: {dest}")
                    print(f"[download-media] {exc}")
                    return
                downloaded_files += 1
                downloads_since_log += 1
                bytes_on_disk += size
                if downloads_since_log >= DOWNLOAD_LOG_INTERVAL:
                    try:
                        _log_download_progress(
                            media_dir, completed_entries, total_entries, bytes_on_disk
                        )
                    except RuntimeError as exc:
                        print(f"[download-media] {exc}")
                        return
                    downloads_since_log = 0
    finally:
        if downloads_since_log:
            try:
                _log_download_progress(
                    media_dir, completed_entries, total_entries, bytes_on_disk
                )
            except RuntimeError as exc:
                print(f"[download-media] {exc}")
                return

    print(
        f"[download-media] Completed. {downloaded_files} new files ensured in {media_dir} "
        f"({completed_entries}/{total_entries} entries processed). "
        f"Current storage usage: {_human_bytes(bytes_on_disk)}."
    )


def command_view_next_download(args: argparse.Namespace) -> None:
    wiki = args.wiki
    base_dir = Path("fandom-data") / wiki
    manifest = base_dir / "all_media_urls.json"
    if not manifest.exists():
        print(
            f"[view-next-download] Manifest not found at {manifest}. "
            "Run 'all-media' first."
        )
        raise SystemExit(1)

    media = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(media, list) or not media:
        print(
            f"[view-next-download] Manifest at {manifest} is empty; nothing to inspect."
        )
        return
    if args.limit:
        media = media[: args.limit]

    media_dir = base_dir / "media"
    media_dir.mkdir(parents=True, exist_ok=True)

    pending = _get_next_pending_entry(media, media_dir)
    if not pending:
        print(
            f"[view-next-download] All entries up to current limit already exist in {media_dir}."
        )
        return

    entry, dest = pending
    print(
        "[view-next-download] Next pending download (dry run; no network requests performed):"
    )
    print(json.dumps(entry, indent=2, sort_keys=True))
    print(f"[view-next-download] Intended destination: {dest}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Small helper CLI for Fandom APIs.")
    sub = parser.add_subparsers(dest="command", required=True)

    all_pages = sub.add_parser("all-pages", help="Fetch every content page URL for a wiki.")
    all_pages.add_argument("wiki", help="Subdomain of the Fandom wiki, e.g. 'rezero'")
    all_pages.set_defaults(func=command_all_pages)

    all_media = sub.add_parser(
        "all-media", help="Fetch metadata + URLs for every uploaded file on a wiki."
    )
    all_media.add_argument("wiki", help="Subdomain of the Fandom wiki, e.g. 'rezero'")
    all_media.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of media records (useful for testing).",
    )
    all_media.set_defaults(func=command_all_media)

    download_media = sub.add_parser(
        "download-media",
        help="Download every media asset listed in all_media_urls.json for a wiki.",
    )
    download_media.add_argument("wiki", help="Subdomain of the Fandom wiki, e.g. 'rezero'")
    download_media.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of manifest entries to download.",
    )
    download_media.set_defaults(func=command_download_media)

    view_next = sub.add_parser(
        "view-next-download",
        help="Print the next manifest entry download-media would process, without downloading.",
    )
    view_next.add_argument("wiki", help="Subdomain of the Fandom wiki, e.g. 'rezero'")
    view_next.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap mirroring download-media's --limit for inspection.",
    )
    view_next.set_defaults(func=command_view_next_download)

    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except httpx.HTTPStatusError as exc:
        parser.error(f"HTTP {exc.response.status_code}: {exc.request.url}")
    except httpx.HTTPError as exc:
        parser.error(f"Network error: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
