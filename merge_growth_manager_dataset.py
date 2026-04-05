#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable


DEFAULT_YOUTUBE_GLOBS = ["data/youtube*.csv"]
DEFAULT_REDDIT_GLOBS = ["data/reddit*_clean.csv", "data/reddit*.csv"]
DEFAULT_OUTPUT = "data/growth_manager_historical_timeline_merged.csv"
DEFAULT_REPORT = "data/growth_manager_historical_timeline_merged_report.md"

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

PROMO_REGEX = re.compile(
    "|".join(
        [
            r"40%\s+below\s+anthropic\s+pricing",
            r"\bbelow\s+anthropic\s+pricing\b",
            r"\bcheck\s+bio\b",
            r"\bclaim\s+your\s+free\b",
            r"\bdm\s+me\b",
            r"\bdm\s+please\b",
            r"\bfor\s+sale\b",
            r"\bfree\s+(credits|usage|trial)\b",
            r"\bpromo\s+code\b",
            r"\btry\s+free\s+before\s+you\s+pay\b",
        ]
    ),
    re.IGNORECASE,
)


@dataclass
class MergeStats:
    youtube_input_files: int = 0
    reddit_input_files: int = 0
    youtube_input_rows: int = 0
    reddit_input_rows: int = 0
    youtube_kept_rows: int = 0
    reddit_kept_rows: int = 0
    merged_output_rows: int = 0
    youtube_dropped_irrelevant: int = 0
    youtube_dropped_promotional: int = 0
    youtube_dropped_duplicate_id: int = 0
    youtube_dropped_duplicate_content: int = 0
    reddit_dropped_irrelevant: int = 0
    reddit_dropped_promotional: int = 0
    reddit_dropped_bot_or_deleted: int = 0
    reddit_dropped_duplicate_id: int = 0
    reddit_dropped_duplicate_content: int = 0
    oldest_published_at: str = ""
    newest_published_at: str = ""
    examples: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    def remember(self, reason: str, example: str) -> None:
        if len(self.examples[reason]) < 8:
            self.examples[reason].append(example)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Merge YouTube and Reddit datasets into one cleaned chronological CSV "
            "that preserves the historical timeline."
        )
    )
    parser.add_argument(
        "--youtube-input",
        action="append",
        dest="youtube_inputs",
        help=(
            "YouTube CSV path or glob. Can be repeated. "
            "Defaults to all matching data/youtube*.csv files."
        ),
    )
    parser.add_argument(
        "--reddit-input",
        action="append",
        dest="reddit_inputs",
        help=(
            "Reddit CSV path or glob. Can be repeated. "
            "Defaults to cleaned Reddit CSVs in data/."
        ),
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--report-output", default=DEFAULT_REPORT)
    return parser


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalized_key(value: str) -> str:
    return normalize_text(value).lower()


def safe_int(value: str) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def safe_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_published_at(value: str) -> datetime:
    cleaned = (value or "").strip()
    if not cleaned:
        return datetime.min
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return datetime.min


def expand_input_paths(values: list[str] | None, default_globs: list[str]) -> list[Path]:
    patterns = values or default_globs
    paths: list[Path] = []
    seen: set[Path] = set()

    for pattern in patterns:
        matches = [Path(match) for match in sorted(glob.glob(pattern))]
        if not matches:
            matches = [Path(pattern)]
        for path in matches:
            resolved = path.resolve()
            if path.exists() and resolved not in seen:
                seen.add(resolved)
                paths.append(path)

    return sorted(paths)


def read_csv_rows(paths: list[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in paths:
        with path.open("r", newline="", encoding="utf-8") as handle:
            rows.extend(csv.DictReader(handle))
    return rows


def detect_products(text: str) -> list[str]:
    lowered = text.lower()
    return sorted({keyword for keyword in PRODUCT_KEYWORDS if keyword in lowered})


def is_relevant_text(text: str) -> bool:
    lowered = text.lower()
    if any(term in lowered for term in STRONG_RELEVANCE_TERMS):
        return True
    if re.search(r"\bclaude\b", lowered) and any(
        term in lowered for term in CLAUDE_CONTEXT_TERMS
    ):
        return True
    return False


def is_deleted_or_bot_author(author: str) -> bool:
    lowered = (author or "").strip().lower()
    if lowered in {"[deleted]", "[removed]", "[unknown]", "", "automoderator"}:
        return True
    if lowered == "claudeai-mod-bot":
        return True
    if "not_a_bot" in lowered:
        return False
    if lowered.endswith("bot"):
        return True
    if any(token in lowered for token in ["_bot", "-bot", "bot_", "bot-"]):
        return True
    return False


def is_promotional(text: str) -> bool:
    return bool(PROMO_REGEX.search(text))


def common_fieldnames() -> list[str]:
    return [
        "timeline_index",
        "record_id",
        "platform",
        "source_type",
        "source_id",
        "title",
        "body_text",
        "creator_name",
        "community_name",
        "published_at",
        "published_date",
        "published_year_month",
        "url",
        "search_context",
        "matched_topics",
        "content_format",
        "tags",
        "views",
        "likes",
        "comments",
        "score",
        "engagement_total",
        "engagement_rate",
        "quality_tier",
        "quality_notes",
    ]


def choose_best_youtube(rows: list[dict[str, str]]) -> dict[str, str]:
    return max(
        rows,
        key=lambda row: (
            safe_int(row.get("view_count", "")),
            safe_int(row.get("like_count", "")) + safe_int(row.get("comment_count", "")),
            row.get("publish_date", ""),
        ),
    )


def choose_best_reddit(rows: list[dict[str, str]]) -> dict[str, str]:
    return max(
        rows,
        key=lambda row: (
            safe_int(row.get("score", "")),
            safe_int(row.get("num_comments", "")),
            row.get("created_iso", ""),
        ),
    )


def clean_youtube_rows(rows: list[dict[str, str]], stats: MergeStats) -> list[dict[str, str]]:
    stats.youtube_input_rows = len(rows)
    deduped_by_id: dict[str, dict[str, str]] = {}
    content_groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)

    for row in rows:
        video_id = row.get("video_id", "")
        text = " ".join(
            [row.get("title", ""), row.get("description", ""), row.get("tags", "")]
        )
        example = f"youtube | {row.get('channel_name')} | {row.get('title', '')[:140]}"

        if not is_relevant_text(text):
            stats.youtube_dropped_irrelevant += 1
            stats.remember("youtube_irrelevant", example)
            continue

        if is_promotional(text):
            stats.youtube_dropped_promotional += 1
            stats.remember("youtube_promotional", example)
            continue

        if video_id in deduped_by_id:
            stats.youtube_dropped_duplicate_id += 1
            continue

        deduped_by_id[video_id] = row

    for row in deduped_by_id.values():
        key = (
            normalized_key(row.get("title", "")),
            normalized_key(row.get("channel_name", "")),
        )
        content_groups[key].append(row)

    cleaned: list[dict[str, str]] = []
    for group in content_groups.values():
        if len(group) > 1:
            stats.youtube_dropped_duplicate_content += len(group) - 1
        cleaned.append(choose_best_youtube(group))

    stats.youtube_kept_rows = len(cleaned)
    return cleaned


def clean_reddit_rows(rows: list[dict[str, str]], stats: MergeStats) -> list[dict[str, str]]:
    stats.reddit_input_rows = len(rows)
    deduped_by_id: dict[str, dict[str, str]] = {}
    content_groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)

    for row in rows:
        row_id = row.get("row_id", "") or row.get("post_id", "")
        body = row.get("body", "") or row.get("selftext", "")
        text = " ".join([row.get("title", ""), body])
        example = (
            f"reddit | r/{row.get('subreddit')} | {row.get('author')} | "
            f"{row.get('title', '')[:140]}"
        )

        if is_deleted_or_bot_author(row.get("author", "")):
            stats.reddit_dropped_bot_or_deleted += 1
            stats.remember("reddit_bot_or_deleted", example)
            continue

        if not is_relevant_text(text):
            stats.reddit_dropped_irrelevant += 1
            stats.remember("reddit_irrelevant", example)
            continue

        if is_promotional(text):
            stats.reddit_dropped_promotional += 1
            stats.remember("reddit_promotional", example)
            continue

        if row_id in deduped_by_id:
            stats.reddit_dropped_duplicate_id += 1
            continue

        deduped_by_id[row_id] = row

    for row in deduped_by_id.values():
        body = row.get("body", "") or row.get("selftext", "")
        key = (
            normalized_key(row.get("row_type", "post")),
            normalized_key(row.get("title", "")),
            normalized_key(body),
            normalized_key(row.get("author", "")),
        )
        content_groups[key].append(row)

    cleaned: list[dict[str, str]] = []
    for group in content_groups.values():
        if len(group) > 1:
            stats.reddit_dropped_duplicate_content += len(group) - 1
        cleaned.append(choose_best_reddit(group))

    stats.reddit_kept_rows = len(cleaned)
    return cleaned


def transform_youtube_row(row: dict[str, str]) -> dict[str, str]:
    text = " ".join([row.get("title", ""), row.get("description", ""), row.get("tags", "")])
    matched_topics = ", ".join(detect_products(text) or [row.get("search_query", "").lower()])
    likes = safe_int(row.get("like_count", ""))
    comments = safe_int(row.get("comment_count", ""))
    views = safe_int(row.get("view_count", ""))
    engagement_total = likes + comments
    duration_seconds = safe_int(row.get("duration_seconds", ""))
    published_at = row.get("publish_date", "")
    published_dt = parse_published_at(published_at)
    content_format = row.get("content_type", "") or (
        "short" if duration_seconds and duration_seconds <= 60 else "video"
    )

    quality_notes = []
    if views == 0:
        quality_notes.append("zero_views")
    if duration_seconds and duration_seconds < 15:
        quality_notes.append("ultra_short")

    return {
        "timeline_index": "",
        "record_id": f"youtube_{row.get('video_id', '')}",
        "platform": "youtube",
        "source_type": "video",
        "source_id": row.get("video_id", ""),
        "title": row.get("title", ""),
        "body_text": row.get("description", ""),
        "creator_name": row.get("channel_name", ""),
        "community_name": row.get("channel_name", ""),
        "published_at": published_at,
        "published_date": published_dt.date().isoformat() if published_dt != datetime.min else "",
        "published_year_month": published_dt.strftime("%Y-%m") if published_dt != datetime.min else "",
        "url": f"https://www.youtube.com/watch?v={row.get('video_id', '')}",
        "search_context": row.get("search_query", ""),
        "matched_topics": matched_topics,
        "content_format": content_format,
        "tags": row.get("tags", ""),
        "views": str(views),
        "likes": str(likes),
        "comments": str(comments),
        "score": "",
        "engagement_total": str(engagement_total),
        "engagement_rate": row.get("engagement_rate", ""),
        "quality_tier": "high" if views >= 1000 or engagement_total >= 50 else "standard",
        "quality_notes": ",".join(quality_notes),
    }


def transform_reddit_row(row: dict[str, str]) -> dict[str, str]:
    score = safe_int(row.get("score", ""))
    comments = safe_int(row.get("num_comments", ""))
    row_type = row.get("row_type", "post")
    body = row.get("body", "") or row.get("selftext", "")
    text = " ".join([row.get("title", ""), body])
    published_at = row.get("created_iso", "")
    published_dt = parse_published_at(published_at)
    matched_topics = ", ".join(
        detect_products(text) or [part.strip() for part in row.get("matched_products", "").split(",") if part.strip()]
    )
    quality_notes = []
    if row_type == "post" and not body.strip():
        quality_notes.append("link_post_or_empty_body")
    if row.get("subreddit", "").lower() not in {"anthropic", "claudeai", "claudecode", "localllama", "chatgpt", "openai", "cursor"}:
        quality_notes.append("general_subreddit")
    if row_type == "comment":
        quality_notes.append("comment_row")

    return {
        "timeline_index": "",
        "record_id": f"reddit_{row.get('row_id', '') or row.get('post_id', '')}",
        "platform": "reddit",
        "source_type": row_type,
        "source_id": row.get("row_id", "") or row.get("post_id", ""),
        "title": row.get("title", ""),
        "body_text": body,
        "creator_name": row.get("author", ""),
        "community_name": row.get("subreddit", ""),
        "published_at": published_at,
        "published_date": published_dt.date().isoformat() if published_dt != datetime.min else "",
        "published_year_month": published_dt.strftime("%Y-%m") if published_dt != datetime.min else "",
        "url": row.get("permalink", ""),
        "search_context": row.get("matched_query", ""),
        "matched_topics": matched_topics,
        "content_format": (
            "comment"
            if row_type == "comment"
            else "self_post"
            if row.get("is_self", "") == "True"
            else "link_post"
        ),
        "tags": "",
        "views": "",
        "likes": "",
        "comments": str(comments) if row_type == "post" else "",
        "score": str(score),
        "engagement_total": str(score + comments if row_type == "post" else score),
        "engagement_rate": "",
        "quality_tier": (
            "high"
            if (row_type == "post" and (score >= 25 or comments >= 20)) or
               (row_type == "comment" and score >= 10)
            else "standard"
        ),
        "quality_notes": ",".join(quality_notes),
    }


def write_csv(path: Path, rows: Iterable[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_report(
    path: Path,
    stats: MergeStats,
    output_path: Path,
    youtube_inputs: list[Path],
    reddit_inputs: list[Path],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Growth Manager Merge Report",
        "",
        f"- Output CSV: `{output_path}`",
        f"- Output sort order: `chronological_ascending`",
        f"- Oldest event: `{stats.oldest_published_at or 'n/a'}`",
        f"- Newest event: `{stats.newest_published_at or 'n/a'}`",
        f"- YouTube input files: `{stats.youtube_input_files}`",
        f"- Reddit input files: `{stats.reddit_input_files}`",
        f"- YouTube input rows: `{stats.youtube_input_rows}`",
        f"- Reddit input rows: `{stats.reddit_input_rows}`",
        f"- YouTube kept rows: `{stats.youtube_kept_rows}`",
        f"- Reddit kept rows: `{stats.reddit_kept_rows}`",
        f"- Final merged rows: `{stats.merged_output_rows}`",
        "",
        "## Drops",
        "",
        f"- YouTube irrelevant rows dropped: `{stats.youtube_dropped_irrelevant}`",
        f"- YouTube promotional rows dropped: `{stats.youtube_dropped_promotional}`",
        f"- YouTube duplicate-id rows dropped: `{stats.youtube_dropped_duplicate_id}`",
        f"- YouTube duplicate-content rows dropped: `{stats.youtube_dropped_duplicate_content}`",
        f"- Reddit bot/deleted rows dropped: `{stats.reddit_dropped_bot_or_deleted}`",
        f"- Reddit irrelevant rows dropped: `{stats.reddit_dropped_irrelevant}`",
        f"- Reddit promotional rows dropped: `{stats.reddit_dropped_promotional}`",
        f"- Reddit duplicate-id rows dropped: `{stats.reddit_dropped_duplicate_id}`",
        f"- Reddit duplicate-content rows dropped: `{stats.reddit_dropped_duplicate_content}`",
        "",
        "## Notes",
        "",
        "- The merge uses a common schema across YouTube and Reddit and preserves full timestamps for historical analysis.",
        "- The output is sorted oldest-to-newest so downstream analysis can follow the timeline directly.",
        "- Reddit posts and comments are both preserved as separate timeline events.",
        "- YouTube rows are kept unless they look irrelevant, promotional, or duplicate, since the source file is already query-focused.",
        "",
        "## Input Files",
        "",
    ]

    lines.extend([f"- YouTube: `{input_path}`" for input_path in youtube_inputs])
    lines.extend([f"- Reddit: `{input_path}`" for input_path in reddit_inputs])
    lines.extend(
        [
            "",
            "## Sample Removed Rows",
            "",
        ]
    )

    if stats.examples:
        for reason in sorted(stats.examples):
            lines.append(f"### {reason}")
            lines.append("")
            for example in stats.examples[reason]:
                lines.append(f"- {example}")
            lines.append("")
    else:
        lines.append("- None")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    youtube_inputs = expand_input_paths(args.youtube_inputs, DEFAULT_YOUTUBE_GLOBS)
    reddit_inputs = expand_input_paths(args.reddit_inputs, DEFAULT_REDDIT_GLOBS)
    if not youtube_inputs:
        parser.error("No YouTube CSV inputs found.")
    if not reddit_inputs:
        parser.error("No Reddit CSV inputs found.")

    youtube_rows = read_csv_rows(youtube_inputs)
    reddit_rows = read_csv_rows(reddit_inputs)
    stats = MergeStats()
    stats.youtube_input_files = len(youtube_inputs)
    stats.reddit_input_files = len(reddit_inputs)
    clean_youtube = clean_youtube_rows(youtube_rows, stats)
    clean_reddit = clean_reddit_rows(reddit_rows, stats)

    merged_rows = [transform_youtube_row(row) for row in clean_youtube] + [
        transform_reddit_row(row) for row in clean_reddit
    ]
    merged_rows.sort(
        key=lambda row: (
            parse_published_at(row["published_at"]),
            row["platform"],
            row["record_id"],
        )
    )
    for index, row in enumerate(merged_rows, start=1):
        row["timeline_index"] = str(index)
    if merged_rows:
        stats.oldest_published_at = merged_rows[0]["published_at"]
        stats.newest_published_at = merged_rows[-1]["published_at"]
    stats.merged_output_rows = len(merged_rows)

    output_path = Path(args.output)
    report_path = Path(args.report_output)
    write_csv(output_path, merged_rows, common_fieldnames())
    write_report(report_path, stats, output_path, youtube_inputs, reddit_inputs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
