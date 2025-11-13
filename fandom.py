#!/usr/bin/env python3
"""Minimal CLI helpers for working with Fandom / MediaWiki APIs."""

from __future__ import annotations

import argparse
import json
import random
import time
import urllib.parse
from pathlib import Path
from typing import Dict, Iterator, List, Optional

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


def iter_all_images(
    wiki: str,
    client: httpx.Client,
    max_items: Optional[int] = None,
) -> Iterator[Dict[str, str]]:
    params: Dict[str, str] = {
        "action": "query",
        "format": "json",
        "list": "allimages",
        "aiprop": "url|mime|size|sha1|timestamp|user|comment",
        "ailimit": "max",
    }
    cont: Dict[str, str] = {}
    yielded = 0
    while True:
        resp = client.get(
            f"https://{wiki}.fandom.com/api.php",
            params={**params, **cont},
        )
        resp.raise_for_status()
        payload = resp.json()
        for image in payload["query"]["allimages"]:
            yield image
            yielded += 1
            if max_items is not None and yielded >= max_items:
                return
        if "continue" not in payload:
            break
        cont = payload["continue"]
        time.sleep(random.uniform(*MEDIA_DELAY_RANGE))


def command_all_pages(args: argparse.Namespace) -> None:
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
    media: List[Dict[str, str]] = []
    headers = {"User-Agent": "fandom-cli/0.1 (+https://github.com/user/project)"}
    with httpx.Client(timeout=API_TIMEOUT, headers=headers) as client:
        for entry in iter_all_images(args.wiki, client, args.limit):
            # Titles are already unique (File:Name.ext). Include canonical file page URL.
            entry["descriptionurl"] = entry.get(
                "descriptionurl",
                f"https://{args.wiki}.fandom.com/wiki/{urllib.parse.quote(entry['title'].replace(' ', '_'), safe=':/%')}",
            )
            media.append(entry)
    out_dir = Path("fandom-data") / args.wiki
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "all_media_urls.json"
    out_file.write_text(json.dumps(media, indent=2), encoding="utf-8")
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
