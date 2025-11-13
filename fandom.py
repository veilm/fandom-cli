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
from typing import Dict, Iterator, List

import httpx

API_TIMEOUT = 30
REQUEST_DELAY = 0.2  # seconds between paged requests to stay polite
MEDIA_DELAY_RANGE = (1.0, 10.0)


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
