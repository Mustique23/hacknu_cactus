#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from requests import RequestException

BASE_URL = "https://www.reddit.com"
DEFAULT_START_DATE = "2023-01-01"
DEFAULT_OUTPUT = "data/reddit_anthropic_discussions_since_2023.csv"
DEFAULT_REPORT_OUTPUT = "data/reddit_anthropic_discussions_since_2023_report.md"
MAX_REQUEST_RETRIES = 6

DEFAULT_QUERIES = [
    '"Anthropic"',
    '"Anthropic Claude"',
    '"Anthropic API"',
    '"Claude"',
    '"Claude AI"',
    '"Claude API"',
    '"Claude Code"',
    '"Claude Sonnet"',
    '"Claude Haiku"',
    '"Claude Opus"',
    '"Claude Max"',
    '"Claude 3"',
    '"Claude 3.5"',
    '"Claude 3.7"',
    '"Claude 4"',
    '"Artifacts Claude"',
    '"Model Context Protocol"',
    '"MCP Anthropic"',
]

DEFAULT_SUBREDDIT_QUERIES = [
    '"Anthropic Claude"',
    '"Anthropic API"',
    '"Claude API"',
    '"Claude Code"',
    '"Claude Sonnet"',
    '"Claude Opus"',
    '"Claude 4"',
    '"Model Context Protocol"',
]

DEFAULT_SEARCH_SUBREDDITS = [
    "Anthropic",
    "ClaudeAI",
    "LocalLLaMA",
    "singularity",
    "OpenAI",
    "ChatGPT",
    "artificial",
    "ArtificialInteligence",
    "MachineLearning",
    "PromptEngineering",
    "cursor",
    "selfhosted",
    "webdev",
    "programming",
    "coding",
]

DEFAULT_DIRECT_CRAWL_SUBREDDITS = [
    "Anthropic",
    "ClaudeAI",
]

PRODUCT_KEYWORDS = [
    "anthropic",
    "anthropic api",
    "claude",
    "claude ai",
    "claude api",
    "claude code",
    "claude sonnet",
    "claude haiku",
    "claude opus",
    "claude max",
    "claude 3",
    "claude 3.5",
    "claude 3.7",
    "claude 4",
    "claude md",
    "artifacts",
    "model context protocol",
    "mcp",
]

STRONG_RELEVANCE_TERMS = [
    "anthropic",
    "anthropic api",
    "claude ai",
    "claude api",
    "claude code",
    "claude sonnet",
    "claude haiku",
    "claude opus",
    "claude max",
    "claude 3",
    "claude 3.5",
    "claude 3.7",
    "claude 4",
    "claude md",
    "artifacts",
    "model context protocol",
    "mcp server",
]

CLAUDE_CONTEXT_TERMS = [
    "ai",
    "agent",
    "agents",
    "api",
    "artifact",
    "artifacts",
    "benchmark",
    "chatbot",
    "code",
    "coding",
    "context",
    "gpt",
    "haiku",
    "llm",
    "mcp",
    "model",
    "models",
    "opus",
    "prompt",
    "prompts",
    "sonnet",
    "subscription",
    "token",
    "tokens",
]

SEARCH_URL = f"{BASE_URL}/search.json"
SUBREDDIT_SEARCH_URL = f"{BASE_URL}/r/{{subreddit}}/search.json"
SUBREDDIT_NEW_URL = f"{BASE_URL}/r/{{subreddit}}/new.json"
COMMENTS_URL = f"{BASE_URL}/comments/{{post_id}}.json"
REMOVED_TEXT_VALUES = {"", "[deleted]", "[removed]"}


@dataclass
class DatasetRow:
    row_id: str
    row_type: str
    source_strategy: str
    subreddit: str
    post_id: str
    parent_id: str
    thread_root_id: str
    thread_root_title: str
    title: str
    body: str
    author: str
    created_utc: int
    created_iso: str
    score: int
    num_comments: int | str
    permalink: str
    external_url: str
    matched_query: str
    matched_products: str
    context_reason: str
    is_self: bool | str


@dataclass
class ScrapeStats:
    requests_sent: int = 0
    request_retries: int = 0
    rate_limit_hits: int = 0
    server_errors: int = 0
    request_failures: int = 0
    global_search_pages: int = 0
    subreddit_search_pages: int = 0
    subreddit_new_pages: int = 0
    posts_seen: int = 0
    posts_collected: int = 0
    comments_seen: int = 0
    comments_collected: int = 0
    comment_threads_fetched: int = 0
    comment_threads_skipped: int = 0
    comment_tree_truncations: int = 0
    duplicates_merged: int = 0
    skipped_before_cutoff: int = 0
    skipped_after_target: int = 0
    skipped_irrelevant_posts: int = 0
    skipped_irrelevant_comments: int = 0
    missing_authors: int = 0
    missing_bodies: int = 0
    deleted_or_removed_comments: int = 0
    issues: list[str] = field(default_factory=list)

    def add_issue(self, message: str) -> None:
        if len(self.issues) < 40:
            self.issues.append(message)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape Reddit posts and comment discussions mentioning Anthropic "
            "or Claude-related products, then export the dataset to CSV."
        )
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Path to the output CSV file.",
    )
    parser.add_argument(
        "--report-output",
        default=DEFAULT_REPORT_OUTPUT,
        help="Path to the run report file.",
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Inclusive UTC start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--target-date",
        "--end-date",
        dest="target_date",
        help=(
            "Inclusive UTC target/end date in YYYY-MM-DD format. "
            "Defaults to the current UTC date."
        ),
    )
    parser.add_argument(
        "--limit-per-query",
        type=int,
        default=5000,
        help="Maximum posts to keep per query after filtering.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Number of results requested per Reddit page. Max 100.",
    )
    parser.add_argument(
        "--max-pages-per-query",
        type=int,
        default=12,
        help="Maximum pages to request for each query search.",
    )
    parser.add_argument(
        "--max-pages-per-subreddit",
        type=int,
        default=40,
        help="Maximum pages to request for each direct subreddit crawl.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.5,
        help="Delay between successful paginated requests.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP request timeout.",
    )
    parser.add_argument(
        "--max-comment-posts",
        type=int,
        default=250,
        help="Fetch comment threads for up to this many matched posts.",
    )
    parser.add_argument(
        "--max-comments-per-post",
        type=int,
        default=200,
        help="Maximum comments requested from each Reddit thread JSON response.",
    )
    parser.add_argument(
        "--comment-depth",
        type=int,
        default=8,
        help="Depth passed to Reddit's thread JSON endpoint.",
    )
    parser.add_argument(
        "--skip-comments",
        action="store_true",
        help="Skip harvesting comments from matched posts.",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help=(
            "Add a custom Reddit search query. Can be repeated. "
            "When provided alone, these queries are also used for subreddit-scoped search."
        ),
    )
    parser.add_argument(
        "--subreddit-query",
        action="append",
        dest="subreddit_queries",
        help=(
            "Add a custom query for subreddit-scoped search only. Can be repeated. "
            "Defaults to a compact subreddit query set for speed."
        ),
    )
    parser.add_argument(
        "--subreddit",
        action="append",
        dest="search_subreddits",
        help="Add a subreddit to subreddit-scoped search. Can be repeated.",
    )
    parser.add_argument(
        "--crawl-subreddit",
        action="append",
        dest="crawl_subreddits",
        help="Add a subreddit for direct /new crawling. Can be repeated.",
    )
    return parser


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "linux:anthropic-reddit-scraper:v2.0 "
                "(by /u/research-dataset-bot)"
            ),
            "Accept": "application/json",
        }
    )
    return session


def parse_retry_after(header_value: str | None) -> float | None:
    if not header_value:
        return None
    try:
        return max(float(header_value), 1.0)
    except ValueError:
        return None


def request_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    timeout_seconds: float,
    stats: ScrapeStats,
    context: str,
) -> Any | None:
    for attempt in range(1, MAX_REQUEST_RETRIES + 1):
        stats.requests_sent += 1
        try:
            response = session.get(url, params=params, timeout=timeout_seconds)
        except RequestException as exc:
            if attempt == MAX_REQUEST_RETRIES:
                stats.request_failures += 1
                stats.add_issue(f"{context}: request exception after retries: {exc}")
                return None
            stats.request_retries += 1
            time.sleep(min(2**attempt, 30))
            continue

        if response.status_code == 429:
            stats.rate_limit_hits += 1
            if attempt == MAX_REQUEST_RETRIES:
                stats.request_failures += 1
                stats.add_issue(f"{context}: rate limited after retries.")
                return None
            stats.request_retries += 1
            wait_seconds = parse_retry_after(response.headers.get("Retry-After"))
            time.sleep(wait_seconds or min(2**attempt, 60))
            continue

        if response.status_code >= 500:
            stats.server_errors += 1
            if attempt == MAX_REQUEST_RETRIES:
                stats.request_failures += 1
                stats.add_issue(
                    f"{context}: server error {response.status_code} after retries."
                )
                return None
            stats.request_retries += 1
            time.sleep(min(2**attempt, 30))
            continue

        if response.status_code in (403, 404):
            stats.request_failures += 1
            stats.add_issue(f"{context}: endpoint returned {response.status_code}.")
            return None

        if response.status_code >= 400:
            stats.request_failures += 1
            stats.add_issue(f"{context}: request failed with {response.status_code}.")
            return None

        try:
            return response.json()
        except ValueError:
            if attempt == MAX_REQUEST_RETRIES:
                stats.request_failures += 1
                stats.add_issue(f"{context}: invalid JSON after retries.")
                return None
            stats.request_retries += 1
            time.sleep(min(2**attempt, 30))

    stats.request_failures += 1
    stats.add_issue(f"{context}: exhausted retries.")
    return None


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\x00", "").strip()


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def isoformat_from_utc(created_utc: int) -> str:
    return datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat()


def split_merged_values(value: str) -> set[str]:
    return {part.strip() for part in value.split(",") if part.strip()}


def merge_csv_values(existing: str, new_value: str) -> str:
    merged = split_merged_values(existing) | split_merged_values(new_value)
    return ", ".join(sorted(merged))


def detect_products(text: str) -> list[str]:
    lowered = text.lower()
    matches = [keyword for keyword in PRODUCT_KEYWORDS if keyword in lowered]
    return sorted(set(matches))


def is_relevant_text(text: str) -> tuple[bool, str]:
    lowered = text.lower()
    strong_hits = [term for term in STRONG_RELEVANCE_TERMS if term in lowered]
    if strong_hits:
        return True, "strong_terms"

    if "claude" in lowered:
        context_hits = [term for term in CLAUDE_CONTEXT_TERMS if term in lowered]
        if context_hits:
            return True, "claude_with_context"

    return False, ""


def normalize_author(author: Any, stats: ScrapeStats) -> str:
    author_text = normalize_text(author)
    if author_text:
        return author_text
    stats.missing_authors += 1
    return "[unknown]"


def post_from_child(
    child: dict[str, Any],
    source_strategy: str,
    matched_query: str,
    stats: ScrapeStats,
) -> DatasetRow:
    data = child.get("data", {})
    created_utc = safe_int(data.get("created_utc"))
    title = normalize_text(data.get("title"))
    body = normalize_text(data.get("selftext"))
    joined_text = "\n".join(part for part in [title, body] if part)
    matched_products = ", ".join(detect_products(joined_text))
    author = normalize_author(data.get("author"), stats)
    if not body:
        stats.missing_bodies += 1

    post_id = normalize_text(data.get("id"))
    return DatasetRow(
        row_id=f"t3_{post_id}",
        row_type="post",
        source_strategy=source_strategy,
        subreddit=normalize_text(data.get("subreddit")),
        post_id=post_id,
        parent_id="",
        thread_root_id=f"t3_{post_id}",
        thread_root_title=title,
        title=title,
        body=body,
        author=author,
        created_utc=created_utc,
        created_iso=isoformat_from_utc(created_utc),
        score=safe_int(data.get("score")),
        num_comments=safe_int(data.get("num_comments")),
        permalink=f"{BASE_URL}{normalize_text(data.get('permalink'))}",
        external_url=normalize_text(data.get("url")),
        matched_query=matched_query,
        matched_products=matched_products,
        context_reason="post_match",
        is_self=bool(data.get("is_self", False)),
    )


def comment_from_child(
    child: dict[str, Any],
    post: DatasetRow,
    stats: ScrapeStats,
) -> DatasetRow | None:
    data = child.get("data", {})
    body = normalize_text(data.get("body"))
    stats.comments_seen += 1

    if body.lower() in REMOVED_TEXT_VALUES:
        stats.deleted_or_removed_comments += 1
        return None

    joined_text = "\n".join(part for part in [post.thread_root_title, body] if part)
    comment_relevant, reason = is_relevant_text(joined_text)
    if not comment_relevant:
        stats.skipped_irrelevant_comments += 1
        return None

    comment_id = normalize_text(data.get("id"))
    created_utc = safe_int(data.get("created_utc"))
    author = normalize_author(data.get("author"), stats)
    matched_products = ", ".join(detect_products(joined_text))
    parent_id = normalize_text(data.get("parent_id"))

    return DatasetRow(
        row_id=f"t1_{comment_id}",
        row_type="comment",
        source_strategy="comments",
        subreddit=post.subreddit,
        post_id=post.post_id,
        parent_id=parent_id,
        thread_root_id=post.thread_root_id,
        thread_root_title=post.thread_root_title,
        title=post.thread_root_title,
        body=body,
        author=author,
        created_utc=created_utc,
        created_iso=isoformat_from_utc(created_utc),
        score=safe_int(data.get("score")),
        num_comments="",
        permalink=f"{BASE_URL}{normalize_text(data.get('permalink'))}",
        external_url=post.external_url,
        matched_query=post.matched_query,
        matched_products=matched_products,
        context_reason=reason or "thread_context",
        is_self="",
    )


def extract_listing_children(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data", {})
    children = data.get("children", [])
    if not isinstance(children, list):
        return []
    return [child for child in children if isinstance(child, dict)]


def collect_posts_from_listing(
    children: list[dict[str, Any]],
    source_strategy: str,
    matched_query: str,
    cutoff_utc: int,
    target_utc_exclusive: int,
    stats: ScrapeStats,
) -> tuple[list[DatasetRow], bool]:
    results: list[DatasetRow] = []
    hit_cutoff = False

    for child in children:
        if child.get("kind") != "t3":
            continue
        row = post_from_child(child, source_strategy, matched_query, stats)
        stats.posts_seen += 1
        if row.created_utc >= target_utc_exclusive:
            stats.skipped_after_target += 1
            continue
        if row.created_utc < cutoff_utc:
            stats.skipped_before_cutoff += 1
            hit_cutoff = True
            continue

        relevant, reason = is_relevant_text("\n".join([row.title, row.body]))
        if not relevant:
            stats.skipped_irrelevant_posts += 1
            continue

        row.context_reason = reason
        if not row.matched_products:
            row.matched_products = ", ".join(detect_products("\n".join([row.title, row.body])))
        results.append(row)

    return results, hit_cutoff


def search_global_query(
    session: requests.Session,
    query: str,
    progress_label: str,
    cutoff_utc: int,
    target_utc_exclusive: int,
    page_size: int,
    max_pages: int,
    sleep_seconds: float,
    timeout_seconds: float,
    stats: ScrapeStats,
    limit_per_query: int,
) -> list[DatasetRow]:
    results: list[DatasetRow] = []
    after: str | None = None

    for page_number in range(max_pages):
        payload = request_json(
            session=session,
            url=SEARCH_URL,
            params={
                "q": query,
                "sort": "new",
                "t": "all",
                "limit": min(page_size, 100),
                "type": "link",
                "raw_json": 1,
                "include_over_18": "on",
            }
            | ({"after": after} if after else {}),
            timeout_seconds=timeout_seconds,
            stats=stats,
            context=f'global search {query}',
        )
        if payload is None:
            break

        stats.global_search_pages += 1
        children = extract_listing_children(payload)
        if not children:
            break

        page_results, hit_cutoff = collect_posts_from_listing(
            children=children,
            source_strategy="global_search",
            matched_query=query,
            cutoff_utc=cutoff_utc,
            target_utc_exclusive=target_utc_exclusive,
            stats=stats,
        )
        results.extend(page_results)
        print(
            f"    {progress_label} page {page_number + 1}/{max_pages}: "
            f"{len(page_results)} kept, {len(results)} total",
            file=sys.stderr,
        )
        if len(results) >= limit_per_query or hit_cutoff:
            break

        after = payload.get("data", {}).get("after")
        if not after:
            break

        if page_number < max_pages - 1:
            time.sleep(sleep_seconds)

    return results[:limit_per_query]


def search_query_in_subreddit(
    session: requests.Session,
    subreddit: str,
    query: str,
    progress_label: str,
    cutoff_utc: int,
    target_utc_exclusive: int,
    page_size: int,
    max_pages: int,
    sleep_seconds: float,
    timeout_seconds: float,
    stats: ScrapeStats,
    limit_per_query: int,
) -> list[DatasetRow]:
    results: list[DatasetRow] = []
    after: str | None = None
    url = SUBREDDIT_SEARCH_URL.format(subreddit=subreddit)

    for page_number in range(max_pages):
        payload = request_json(
            session=session,
            url=url,
            params={
                "q": query,
                "restrict_sr": "on",
                "sort": "new",
                "t": "all",
                "limit": min(page_size, 100),
                "type": "link",
                "raw_json": 1,
                "include_over_18": "on",
            }
            | ({"after": after} if after else {}),
            timeout_seconds=timeout_seconds,
            stats=stats,
            context=f'subreddit search r/{subreddit} {query}',
        )
        if payload is None:
            break

        stats.subreddit_search_pages += 1
        children = extract_listing_children(payload)
        if not children:
            break

        page_results, hit_cutoff = collect_posts_from_listing(
            children=children,
            source_strategy=f"subreddit_search:r/{subreddit}",
            matched_query=query,
            cutoff_utc=cutoff_utc,
            target_utc_exclusive=target_utc_exclusive,
            stats=stats,
        )
        results.extend(page_results)
        print(
            f"    {progress_label} page {page_number + 1}/{max_pages}: "
            f"{len(page_results)} kept, {len(results)} total",
            file=sys.stderr,
        )
        if len(results) >= limit_per_query or hit_cutoff:
            break

        after = payload.get("data", {}).get("after")
        if not after:
            break

        if page_number < max_pages - 1:
            time.sleep(sleep_seconds)

    return results[:limit_per_query]


def crawl_subreddit_new(
    session: requests.Session,
    subreddit: str,
    progress_label: str,
    cutoff_utc: int,
    target_utc_exclusive: int,
    page_size: int,
    max_pages: int,
    sleep_seconds: float,
    timeout_seconds: float,
    stats: ScrapeStats,
) -> list[DatasetRow]:
    results: list[DatasetRow] = []
    after: str | None = None
    url = SUBREDDIT_NEW_URL.format(subreddit=subreddit)

    for page_number in range(max_pages):
        payload = request_json(
            session=session,
            url=url,
            params={
                "limit": min(page_size, 100),
                "raw_json": 1,
                "include_over_18": "on",
            }
            | ({"after": after} if after else {}),
            timeout_seconds=timeout_seconds,
            stats=stats,
            context=f"subreddit crawl r/{subreddit}",
        )
        if payload is None:
            break

        stats.subreddit_new_pages += 1
        children = extract_listing_children(payload)
        if not children:
            break

        page_results, hit_cutoff = collect_posts_from_listing(
            children=children,
            source_strategy=f"subreddit_new:r/{subreddit}",
            matched_query=f"direct_crawl:r/{subreddit}",
            cutoff_utc=cutoff_utc,
            target_utc_exclusive=target_utc_exclusive,
            stats=stats,
        )
        results.extend(page_results)
        print(
            f"    {progress_label} page {page_number + 1}/{max_pages}: "
            f"{len(page_results)} kept, {len(results)} total",
            file=sys.stderr,
        )
        if hit_cutoff:
            break

        after = payload.get("data", {}).get("after")
        if not after:
            break

        if page_number < max_pages - 1:
            time.sleep(sleep_seconds)

    return results


def walk_comment_nodes(
    nodes: list[dict[str, Any]],
    post: DatasetRow,
    cutoff_utc: int,
    target_utc_exclusive: int,
    stats: ScrapeStats,
) -> list[DatasetRow]:
    collected: list[DatasetRow] = []
    stack = [node for node in reversed(nodes) if isinstance(node, dict)]

    while stack:
        node = stack.pop()
        kind = node.get("kind")
        data = node.get("data", {})

        if kind == "more":
            stats.comment_tree_truncations += 1
            continue

        if kind != "t1":
            continue

        created_utc = safe_int(data.get("created_utc"))
        if created_utc >= target_utc_exclusive:
            stats.skipped_after_target += 1
        elif created_utc < cutoff_utc:
            stats.skipped_before_cutoff += 1
        else:
            row = comment_from_child(node, post, stats)
            if row is not None:
                collected.append(row)

        replies = data.get("replies")
        if isinstance(replies, dict):
            reply_children = extract_listing_children(replies)
            for reply in reversed(reply_children):
                stack.append(reply)

    return collected


def fetch_comments_for_post(
    session: requests.Session,
    post: DatasetRow,
    cutoff_utc: int,
    target_utc_exclusive: int,
    max_comments_per_post: int,
    comment_depth: int,
    timeout_seconds: float,
    stats: ScrapeStats,
) -> list[DatasetRow]:
    payload = request_json(
        session=session,
        url=COMMENTS_URL.format(post_id=post.post_id),
        params={
            "limit": max_comments_per_post,
            "depth": comment_depth,
            "sort": "new",
            "raw_json": 1,
        },
        timeout_seconds=timeout_seconds,
        stats=stats,
        context=f"comments for post {post.post_id}",
    )
    if payload is None:
        stats.comment_threads_skipped += 1
        return []

    if not isinstance(payload, list) or len(payload) < 2:
        stats.comment_threads_skipped += 1
        stats.add_issue(f"comments for post {post.post_id}: unexpected payload shape.")
        return []

    stats.comment_threads_fetched += 1
    comment_listing = payload[1]
    comment_children = extract_listing_children(comment_listing)
    return walk_comment_nodes(
        nodes=comment_children,
        post=post,
        cutoff_utc=cutoff_utc,
        target_utc_exclusive=target_utc_exclusive,
        stats=stats,
    )


def deduplicate_rows(rows: Iterable[DatasetRow], stats: ScrapeStats) -> list[DatasetRow]:
    deduped: dict[str, DatasetRow] = {}

    for row in rows:
        existing = deduped.get(row.row_id)
        if existing is None:
            deduped[row.row_id] = row
            continue

        stats.duplicates_merged += 1
        existing.source_strategy = merge_csv_values(existing.source_strategy, row.source_strategy)
        existing.matched_query = merge_csv_values(existing.matched_query, row.matched_query)
        existing.matched_products = merge_csv_values(
            existing.matched_products, row.matched_products
        )
        existing.context_reason = merge_csv_values(
            existing.context_reason, row.context_reason
        )

    return sorted(deduped.values(), key=lambda row: row.created_utc, reverse=True)


def write_csv(output_path: Path, rows: list[DatasetRow]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "row_id",
        "row_type",
        "source_strategy",
        "subreddit",
        "post_id",
        "parent_id",
        "thread_root_id",
        "thread_root_title",
        "title",
        "body",
        "author",
        "created_utc",
        "created_iso",
        "score",
        "num_comments",
        "permalink",
        "external_url",
        "matched_query",
        "matched_products",
        "context_reason",
        "is_self",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def write_report(
    report_path: Path,
    output_path: Path,
    rows: list[DatasetRow],
    stats: ScrapeStats,
    start_date: str,
    target_date: str,
    queries: list[str],
    subreddit_queries: list[str],
    search_subreddits: list[str],
    crawl_subreddits: list[str],
    skip_comments: bool,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    total_rows = len(rows)
    total_posts = sum(1 for row in rows if row.row_type == "post")
    total_comments = sum(1 for row in rows if row.row_type == "comment")
    oldest_row = rows[-1].created_iso if rows else "n/a"
    newest_row = rows[0].created_iso if rows else "n/a"

    report_lines = [
        "# Reddit Scrape Report",
        "",
        f"- Generated at: `{datetime.now(tz=timezone.utc).isoformat()}`",
        f"- Output CSV: `{output_path}`",
        f"- Start date: `{start_date}`",
        f"- Target date: `{target_date}`",
        f"- Rows written: `{total_rows}`",
        f"- Post rows: `{total_posts}`",
        f"- Comment rows: `{total_comments}`",
        f"- Newest row timestamp: `{newest_row}`",
        f"- Oldest row timestamp: `{oldest_row}`",
        f"- Comment harvesting enabled: `{'no' if skip_comments else 'yes'}`",
        "",
        "## Collection Strategy",
        "",
        "- Global Reddit search across Anthropic and Claude query variants.",
        "- Subreddit-scoped search across AI and developer communities.",
        "- Direct `/new` crawling for Anthropic-centric subreddits to work around shallow global search coverage.",
        "- Comment harvesting from matched posts to capture discussion rows, not just submissions.",
        "",
        "## Runtime Counters",
        "",
        f"- Requests sent: `{stats.requests_sent}`",
        f"- Retries performed: `{stats.request_retries}`",
        f"- Rate-limit responses: `{stats.rate_limit_hits}`",
        f"- Server errors: `{stats.server_errors}`",
        f"- Request failures left unresolved: `{stats.request_failures}`",
        f"- Global search pages: `{stats.global_search_pages}`",
        f"- Subreddit search pages: `{stats.subreddit_search_pages}`",
        f"- Direct subreddit pages: `{stats.subreddit_new_pages}`",
        f"- Posts seen: `{stats.posts_seen}`",
        f"- Posts collected: `{stats.posts_collected}`",
        f"- Comments seen: `{stats.comments_seen}`",
        f"- Comments collected: `{stats.comments_collected}`",
        f"- Comment threads fetched: `{stats.comment_threads_fetched}`",
        f"- Comment threads skipped: `{stats.comment_threads_skipped}`",
        f"- Comment tree truncations (`more` objects): `{stats.comment_tree_truncations}`",
        f"- Duplicates merged: `{stats.duplicates_merged}`",
        f"- Rows skipped before cutoff: `{stats.skipped_before_cutoff}`",
        f"- Rows skipped after target date: `{stats.skipped_after_target}`",
        f"- Irrelevant posts skipped: `{stats.skipped_irrelevant_posts}`",
        f"- Irrelevant comments skipped: `{stats.skipped_irrelevant_comments}`",
        f"- Missing authors normalized: `{stats.missing_authors}`",
        f"- Missing bodies encountered: `{stats.missing_bodies}`",
        f"- Deleted or removed comments skipped: `{stats.deleted_or_removed_comments}`",
        "",
        "## What Broke And How It Was Handled",
        "",
        "- Global Reddit search was too shallow to backfill all the way to 2023 on its own. The scraper now combines global search with subreddit-scoped search and direct subreddit crawling.",
        "- Reddit intermittently returned `429` or `5xx` responses. The scraper retries with exponential backoff and honors `Retry-After` when available.",
        "- Some subreddit endpoints can return `403` or `404`. Those failures are logged in the report and the run continues instead of aborting.",
        "- Thread JSON can contain `more` placeholders instead of full comment trees. The scraper keeps the comments Reddit returned directly, counts each truncation, and documents the partial coverage.",
        "- Missing authors and empty bodies are normalized instead of crashing serialization. Removed comments are skipped because they do not contain usable text.",
        "",
        "## Run Configuration",
        "",
        f"- Global queries: `{len(queries)}`",
        f"- Subreddit queries: `{len(subreddit_queries)}`",
        f"- Search subreddits: `{', '.join(search_subreddits)}`",
        f"- Direct crawl subreddits: `{', '.join(crawl_subreddits)}`",
        "",
        "## Sample Issues",
        "",
    ]

    if stats.issues:
        report_lines.extend([f"- {issue}" for issue in stats.issues])
    else:
        report_lines.append("- No unresolved endpoint issues were recorded in this run.")

    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.limit_per_query <= 0:
        parser.error("--limit-per-query must be greater than 0.")
    if args.page_size <= 0:
        parser.error("--page-size must be greater than 0.")
    if args.max_pages_per_query <= 0:
        parser.error("--max-pages-per-query must be greater than 0.")
    if args.max_pages_per_subreddit <= 0:
        parser.error("--max-pages-per-subreddit must be greater than 0.")
    if args.max_comment_posts < 0:
        parser.error("--max-comment-posts cannot be negative.")
    if args.max_comments_per_post <= 0:
        parser.error("--max-comments-per-post must be greater than 0.")
    if args.comment_depth <= 0:
        parser.error("--comment-depth must be greater than 0.")

    try:
        cutoff = datetime.strptime(args.start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        parser.error(f"--start-date must be YYYY-MM-DD: {exc}")

    target_date_value = args.target_date or datetime.now(tz=timezone.utc).strftime(
        "%Y-%m-%d"
    )
    try:
        target_date = datetime.strptime(target_date_value, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        parser.error(f"--target-date must be YYYY-MM-DD: {exc}")

    now_utc = datetime.now(tz=timezone.utc)
    if cutoff > now_utc:
        parser.error("--start-date cannot be in the future.")
    if cutoff > target_date:
        parser.error("--start-date cannot be after --target-date.")

    queries = args.queries or DEFAULT_QUERIES
    subreddit_queries = (
        args.subreddit_queries or args.queries or DEFAULT_SUBREDDIT_QUERIES
    )
    search_subreddits = args.search_subreddits or DEFAULT_SEARCH_SUBREDDITS
    crawl_subreddits = args.crawl_subreddits or DEFAULT_DIRECT_CRAWL_SUBREDDITS
    cutoff_utc = int(cutoff.timestamp())
    target_utc_exclusive = int((target_date + timedelta(days=1)).timestamp())

    session = make_session()
    stats = ScrapeStats()
    all_rows: list[DatasetRow] = []

    print("Running global query search...", file=sys.stderr)
    for query in queries:
        print(f"  query {query}", file=sys.stderr)
        query_rows = search_global_query(
            session=session,
            query=query,
            progress_label=f'query {query}',
            cutoff_utc=cutoff_utc,
            target_utc_exclusive=target_utc_exclusive,
            page_size=args.page_size,
            max_pages=args.max_pages_per_query,
            sleep_seconds=args.sleep_seconds,
            timeout_seconds=args.timeout_seconds,
            stats=stats,
            limit_per_query=args.limit_per_query,
        )
        stats.posts_collected += len(query_rows)
        all_rows.extend(query_rows)

    print("Running subreddit-scoped search...", file=sys.stderr)
    for subreddit in search_subreddits:
        for query in subreddit_queries:
            print(f"  r/{subreddit} {query}", file=sys.stderr)
            query_rows = search_query_in_subreddit(
                session=session,
                subreddit=subreddit,
                query=query,
                progress_label=f'r/{subreddit} {query}',
                cutoff_utc=cutoff_utc,
                target_utc_exclusive=target_utc_exclusive,
                page_size=args.page_size,
                max_pages=args.max_pages_per_query,
                sleep_seconds=args.sleep_seconds,
                timeout_seconds=args.timeout_seconds,
                stats=stats,
                limit_per_query=args.limit_per_query,
            )
            stats.posts_collected += len(query_rows)
            all_rows.extend(query_rows)

    print("Running direct subreddit crawl...", file=sys.stderr)
    for subreddit in crawl_subreddits:
        print(f"  crawl r/{subreddit}", file=sys.stderr)
        crawl_rows = crawl_subreddit_new(
            session=session,
            subreddit=subreddit,
            progress_label=f"crawl r/{subreddit}",
            cutoff_utc=cutoff_utc,
            target_utc_exclusive=target_utc_exclusive,
            page_size=args.page_size,
            max_pages=args.max_pages_per_subreddit,
            sleep_seconds=args.sleep_seconds,
            timeout_seconds=args.timeout_seconds,
            stats=stats,
        )
        stats.posts_collected += len(crawl_rows)
        all_rows.extend(crawl_rows)

    deduped_posts = deduplicate_rows(all_rows, stats)
    posts_only = [row for row in deduped_posts if row.row_type == "post"]
    final_rows = list(deduped_posts)

    if args.skip_comments:
        print("Skipping comment harvest.", file=sys.stderr)
    else:
        print("Harvesting comments from matched posts...", file=sys.stderr)
        candidate_posts = sorted(
            (
                post
                for post in posts_only
                if safe_int(post.num_comments) > 0 and post.permalink
            ),
            key=lambda post: (safe_int(post.num_comments), post.created_utc),
            reverse=True,
        )[: args.max_comment_posts]

        for index, post in enumerate(candidate_posts, start=1):
            print(
                f"  comments {index}/{len(candidate_posts)} for post {post.post_id}",
                file=sys.stderr,
            )
            comment_rows = fetch_comments_for_post(
                session=session,
                post=post,
                cutoff_utc=cutoff_utc,
                target_utc_exclusive=target_utc_exclusive,
                max_comments_per_post=args.max_comments_per_post,
                comment_depth=args.comment_depth,
                timeout_seconds=args.timeout_seconds,
                stats=stats,
            )
            stats.comments_collected += len(comment_rows)
            final_rows.extend(comment_rows)
            time.sleep(args.sleep_seconds)

    deduped_rows = deduplicate_rows(final_rows, stats)
    output_path = Path(args.output)
    report_path = Path(args.report_output)
    write_csv(output_path, deduped_rows)
    write_report(
        report_path=report_path,
        output_path=output_path,
        rows=deduped_rows,
        stats=stats,
        start_date=args.start_date,
        target_date=target_date_value,
        queries=queries,
        subreddit_queries=subreddit_queries,
        search_subreddits=search_subreddits,
        crawl_subreddits=crawl_subreddits,
        skip_comments=args.skip_comments,
    )

    print(
        f"Wrote {len(deduped_rows)} rows to {output_path.resolve()}",
        file=sys.stderr,
    )
    print(
        f"Wrote run report to {report_path.resolve()}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
