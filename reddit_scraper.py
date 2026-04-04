#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import requests


DEFAULT_QUERIES = [
    '"Anthropic"',
    '"Anthropic Claude"',
    '"Claude"',
    '"Claude AI"',
    '"Claude Code"',
    '"Claude API"',
    '"Claude Sonnet"',
    '"Claude Haiku"',
    '"Claude Opus"',
    '"Claude Max"',
    '"Model Context Protocol"',
    '"MCP Anthropic"',
]

PRODUCT_KEYWORDS = [
    "anthropic",
    "claude",
    "claude ai",
    "claude code",
    "claude api",
    "claude sonnet",
    "claude haiku",
    "claude opus",
    "claude max",
    "claude md",
    "artifacts",
    "model context protocol",
    "mcp",
]

STRONG_RELEVANCE_TERMS = [
    "anthropic",
    "claude ai",
    "claude code",
    "claude api",
    "claude sonnet",
    "claude haiku",
    "claude opus",
    "claude max",
    "claude md",
    "anthropic api",
    "model context protocol",
    "artifacts",
    "mcp server",
]

CLAUDE_CONTEXT_TERMS = [
    "ai",
    "llm",
    "model",
    "models",
    "prompt",
    "prompts",
    "api",
    "code",
    "coding",
    "sonnet",
    "haiku",
    "opus",
    "max",
    "context",
    "token",
    "tokens",
    "artifact",
    "artifacts",
    "mcp",
    "agent",
    "agents",
    "chatbot",
    "subscription",
    "benchmark",
    "gpt",
]

SEARCH_URL = "https://www.reddit.com/search.json"


@dataclass
class RedditPost:
    post_id: str
    subreddit: str
    title: str
    author: str
    created_utc: int
    created_iso: str
    score: int
    num_comments: int
    upvote_ratio: float | str
    is_self: bool
    permalink: str
    external_url: str
    matched_query: str
    matched_products: str
    selftext: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape recent Reddit posts mentioning Anthropic or Claude-related "
            "products and save the result as a CSV dataset."
        )
    )
    parser.add_argument(
        "--output",
        default="data/reddit_anthropic_last_week.csv",
        help="Path to the output CSV file.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Lookback window in days. Defaults to 7.",
    )
    parser.add_argument(
        "--limit-per-query",
        type=int,
        default=100,
        help="Maximum posts to keep per query after filtering. Defaults to 100.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Number of results requested per Reddit API page. Max 100.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Maximum number of pages to request per query.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Delay between paginated Reddit requests.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP request timeout.",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help="Add a custom Reddit search query. Can be repeated.",
    )
    return parser


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "linux:anthropic-reddit-scraper:v1.0 "
                "(by /u/research-dataset-bot)"
            ),
            "Accept": "application/json",
        }
    )
    return session


def fetch_search_page(
    session: requests.Session,
    query: str,
    page_size: int,
    time_filter: str,
    after: str | None,
    timeout_seconds: float,
) -> dict:
    params = {
        "q": query,
        "sort": "new",
        "t": time_filter,
        "limit": min(page_size, 100),
        "type": "link",
        "raw_json": 1,
        "include_over_18": "on",
    }
    if after:
        params["after"] = after

    response = session.get(SEARCH_URL, params=params, timeout=timeout_seconds)

    if response.status_code == 429:
        raise RuntimeError(
            "Reddit rate-limited the request. Increase --sleep-seconds and retry."
        )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Reddit request failed with status {response.status_code}: "
            f"{response.text[:200]}"
        )

    return response.json()


def detect_products(text: str) -> list[str]:
    lowered = text.lower()
    matches = [keyword for keyword in PRODUCT_KEYWORDS if keyword in lowered]
    return sorted(set(matches))


def is_relevant_post(title: str, selftext: str) -> bool:
    text = f"{title}\n{selftext}".lower()
    if any(term in text for term in STRONG_RELEVANCE_TERMS):
        return True

    if "claude" in text and any(term in text for term in CLAUDE_CONTEXT_TERMS):
        return True

    return False


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return value.replace("\x00", "").strip()


def normalize_post(item: dict, matched_query: str) -> RedditPost:
    data = item["data"]
    created_utc = int(data["created_utc"])
    title = normalize_text(data.get("title"))
    selftext = normalize_text(data.get("selftext"))
    joined_text = f"{title}\n{selftext}"
    matched_products = ", ".join(detect_products(joined_text))

    return RedditPost(
        post_id=str(data["id"]),
        subreddit=str(data.get("subreddit", "")),
        title=title,
        author=str(data.get("author", "")),
        created_utc=created_utc,
        created_iso=datetime.fromtimestamp(
            created_utc, tz=timezone.utc
        ).isoformat(),
        score=int(data.get("score", 0)),
        num_comments=int(data.get("num_comments", 0)),
        upvote_ratio=data.get("upvote_ratio", ""),
        is_self=bool(data.get("is_self", False)),
        permalink=f"https://www.reddit.com{data.get('permalink', '')}",
        external_url=str(data.get("url", "")),
        matched_query=matched_query,
        matched_products=matched_products,
        selftext=selftext,
    )


def scrape_query(
    session: requests.Session,
    query: str,
    cutoff_utc: int,
    limit_per_query: int,
    page_size: int,
    max_pages: int,
    sleep_seconds: float,
    timeout_seconds: float,
) -> list[RedditPost]:
    results: list[RedditPost] = []
    after: str | None = None

    for page_number in range(max_pages):
        payload = fetch_search_page(
            session=session,
            query=query,
            page_size=page_size,
            time_filter="week",
            after=after,
            timeout_seconds=timeout_seconds,
        )
        page = payload.get("data", {})
        children = page.get("children", [])
        if not children:
            break

        for item in children:
            post = normalize_post(item, matched_query=query)
            if post.created_utc < cutoff_utc:
                continue
            if not is_relevant_post(post.title, post.selftext):
                continue
            results.append(post)
            if len(results) >= limit_per_query:
                return results

        after = page.get("after")
        if not after:
            break

        if page_number < max_pages - 1:
            time.sleep(sleep_seconds)

    return results


def deduplicate_posts(posts: Iterable[RedditPost]) -> list[RedditPost]:
    deduped: dict[str, RedditPost] = {}
    for post in posts:
        existing = deduped.get(post.post_id)
        if existing is None:
            deduped[post.post_id] = post
            continue

        merged_queries = sorted(
            {part.strip() for part in (existing.matched_query + "," + post.matched_query).split(",") if part.strip()}
        )
        merged_products = sorted(
            {part.strip() for part in (existing.matched_products + "," + post.matched_products).split(",") if part.strip()}
        )
        existing.matched_query = ", ".join(merged_queries)
        existing.matched_products = ", ".join(merged_products)

    return sorted(deduped.values(), key=lambda post: post.created_utc, reverse=True)


def write_csv(output_path: Path, posts: list[RedditPost]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "post_id",
        "subreddit",
        "title",
        "author",
        "created_utc",
        "created_iso",
        "score",
        "num_comments",
        "upvote_ratio",
        "is_self",
        "permalink",
        "external_url",
        "matched_query",
        "matched_products",
        "selftext",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for post in posts:
            writer.writerow(post.__dict__)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.days <= 0:
        parser.error("--days must be greater than 0.")
    if args.limit_per_query <= 0:
        parser.error("--limit-per-query must be greater than 0.")
    if args.page_size <= 0:
        parser.error("--page-size must be greater than 0.")
    if args.max_pages <= 0:
        parser.error("--max-pages must be greater than 0.")

    queries = args.queries or DEFAULT_QUERIES
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=args.days)
    cutoff_utc = int(cutoff.timestamp())

    session = make_session()
    all_posts: list[RedditPost] = []

    for query in queries:
        print(f"Searching Reddit for {query} ...", file=sys.stderr)
        posts = scrape_query(
            session=session,
            query=query,
            cutoff_utc=cutoff_utc,
            limit_per_query=args.limit_per_query,
            page_size=args.page_size,
            max_pages=args.max_pages,
            sleep_seconds=args.sleep_seconds,
            timeout_seconds=args.timeout_seconds,
        )
        print(
            f"Collected {len(posts)} posts for query {query}.",
            file=sys.stderr,
        )
        all_posts.extend(posts)

    deduped_posts = deduplicate_posts(all_posts)
    output_path = Path(args.output)
    write_csv(output_path, deduped_posts)

    print(
        f"Wrote {len(deduped_posts)} unique posts to {output_path.resolve()}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
