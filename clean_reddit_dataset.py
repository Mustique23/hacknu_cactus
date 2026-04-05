#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_INPUT = "data/reddit_anthropic_discussions_since_2023.csv"
DEFAULT_OUTPUT = "data/reddit_anthropic_discussions_since_2023_clean.csv"
DEFAULT_REPORT = "data/reddit_anthropic_discussions_since_2023_clean_report.md"

LOW_SIGNAL_COMMENT_PHRASES = {
    "+1",
    "agreed",
    "based",
    "details please",
    "dm",
    "dm me",
    "dm please",
    "following",
    "lol",
    "lmao",
    "me too",
    "no",
    "same",
    "same here",
    "this",
    "true",
    "yes",
}

PROMO_PATTERNS = [
    r"40%\s+below\s+anthropic\s+pricing",
    r"\bbelow\s+anthropic\s+pricing\b",
    r"\bcheck\s+bio\b",
    r"\bclaim\s+your\s+free\b",
    r"\bdm\s+me\b",
    r"\bdm\s+please\b",
    r"\bfor\s+sale\b",
    r"\bfree\s+(credits|usage|trial)\b",
    r"\bpomo\s+code\b",
    r"\bpromo\s+code\b",
    r"\btry\s+free\s+before\s+you\s+pay\b",
]

PROMO_REGEX = re.compile("|".join(PROMO_PATTERNS), re.IGNORECASE)


@dataclass
class CleanStats:
    input_rows: int = 0
    output_rows: int = 0
    dropped_deleted_authors: int = 0
    dropped_bot_authors: int = 0
    dropped_low_signal_comments: int = 0
    dropped_promotional_rows: int = 0
    dropped_empty_comments: int = 0
    dropped_duplicate_posts: int = 0
    dropped_duplicate_comments: int = 0
    duplicate_post_groups: int = 0
    duplicate_comment_groups: int = 0
    kept_posts: int = 0
    kept_comments: int = 0
    bot_author_counts: Counter = field(default_factory=Counter)
    dropped_examples: dict[str, list[str]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def remember(self, reason: str, example: str) -> None:
        if len(self.dropped_examples[reason]) < 8:
            self.dropped_examples[reason].append(example)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Clean a scraped Reddit Anthropic dataset into a higher-quality CSV."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Input CSV path.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output CSV path.")
    parser.add_argument(
        "--report-output",
        default=DEFAULT_REPORT,
        help="Markdown report output path.",
    )
    return parser


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def row_text(row: dict[str, str]) -> str:
    return " ".join(
        part for part in [row.get("title", ""), row.get("body", "")] if part
    ).strip()


def is_deleted_author(author: str) -> bool:
    return author.strip().lower() in {"[deleted]", "[removed]", "[unknown]"}


def is_bot_author(author: str) -> bool:
    lowered = author.strip().lower()
    if not lowered:
        return True
    if lowered in {"automoderator", "claudeai-mod-bot"}:
        return True
    if "not_a_bot" in lowered:
        return False
    if lowered.endswith("bot"):
        return True
    if any(token in lowered for token in ["_bot", "-bot", "bot_", "bot-"]):
        return True
    return False


def is_promotional(row: dict[str, str]) -> bool:
    return bool(PROMO_REGEX.search(row_text(row)))


def is_low_signal_comment(row: dict[str, str]) -> bool:
    body = row.get("body", "").strip()
    lowered = body.lower()
    words = body.split()

    if not body:
        return True
    if lowered in LOW_SIGNAL_COMMENT_PHRASES:
        return True
    if len(body) <= 3:
        return True
    if len(words) == 1 and len(body) <= 12:
        return True
    if len(words) <= 2 and len(body) <= 10:
        return True
    if not row.get("matched_products", "").strip() and len(body) < 20:
        return True
    return False


def choose_best_post(rows: list[dict[str, str]]) -> dict[str, str]:
    return max(
        rows,
        key=lambda row: (
            int(row.get("score", "0") or 0),
            int(row.get("num_comments", "0") or 0),
            row.get("created_iso", ""),
        ),
    )


def choose_best_comment(rows: list[dict[str, str]]) -> dict[str, str]:
    return max(
        rows,
        key=lambda row: (
            int(row.get("score", "0") or 0),
            len(row.get("body", "")),
            row.get("created_iso", ""),
        ),
    )


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_report(path: Path, stats: CleanStats, output_path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Clean Dataset Report",
        "",
        f"- Output CSV: `{output_path}`",
        f"- Input rows: `{stats.input_rows}`",
        f"- Output rows: `{stats.output_rows}`",
        f"- Rows removed: `{stats.input_rows - stats.output_rows}`",
        f"- Kept posts: `{stats.kept_posts}`",
        f"- Kept comments: `{stats.kept_comments}`",
        "",
        "## Removal Counts",
        "",
        f"- Deleted or missing authors removed: `{stats.dropped_deleted_authors}`",
        f"- Bot or mod authors removed: `{stats.dropped_bot_authors}`",
        f"- Empty comments removed: `{stats.dropped_empty_comments}`",
        f"- Low-signal comments removed: `{stats.dropped_low_signal_comments}`",
        f"- Promotional rows removed: `{stats.dropped_promotional_rows}`",
        f"- Duplicate post rows removed: `{stats.dropped_duplicate_posts}`",
        f"- Duplicate comment rows removed: `{stats.dropped_duplicate_comments}`",
        f"- Duplicate post groups collapsed: `{stats.duplicate_post_groups}`",
        f"- Duplicate comment groups collapsed: `{stats.duplicate_comment_groups}`",
        "",
        "## Top Bot Authors Removed",
        "",
    ]

    if stats.bot_author_counts:
        for author, count in stats.bot_author_counts.most_common(10):
            lines.append(f"- `{author}`: `{count}`")
    else:
        lines.append("- None")

    lines.extend(["", "## Sample Removed Rows", ""])
    if not stats.dropped_examples:
        lines.append("- None")
    else:
        for reason in sorted(stats.dropped_examples):
            lines.append(f"### {reason}")
            lines.append("")
            for example in stats.dropped_examples[reason]:
                lines.append(f"- {example}")
            lines.append("")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    report_path = Path(args.report_output)

    with input_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    stats = CleanStats(input_rows=len(rows))
    filtered_rows: list[dict[str, str]] = []

    for row in rows:
        author = row.get("author", "")
        body = row.get("body", "")
        row_type = row.get("row_type", "")
        example = (
            f"{row.get('row_type')} | {row.get('subreddit')} | {author} | "
            f"{(row.get('title') or body)[:140]}"
        )

        if is_deleted_author(author):
            stats.dropped_deleted_authors += 1
            stats.remember("deleted_authors", example)
            continue

        if is_bot_author(author):
            stats.dropped_bot_authors += 1
            stats.bot_author_counts[author] += 1
            stats.remember("bot_authors", example)
            continue

        if row_type == "comment" and not body.strip():
            stats.dropped_empty_comments += 1
            stats.remember("empty_comments", example)
            continue

        if row_type == "comment" and is_low_signal_comment(row):
            stats.dropped_low_signal_comments += 1
            stats.remember("low_signal_comments", example)
            continue

        if is_promotional(row):
            stats.dropped_promotional_rows += 1
            stats.remember("promotional", example)
            continue

        filtered_rows.append(row)

    post_groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    comment_groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    other_rows: list[dict[str, str]] = []

    for row in filtered_rows:
        row_type = row.get("row_type", "")
        if row_type == "post":
            key = (
                normalize_text(row.get("title", "")),
                normalize_text(row.get("body", "")),
            )
            post_groups[key].append(row)
        elif row_type == "comment":
            key = (
                row.get("thread_root_id", ""),
                normalize_text(row.get("body", "")),
            )
            comment_groups[key].append(row)
        else:
            other_rows.append(row)

    cleaned_rows: list[dict[str, str]] = list(other_rows)

    for group in post_groups.values():
        if len(group) > 1:
            stats.duplicate_post_groups += 1
            stats.dropped_duplicate_posts += len(group) - 1
        cleaned_rows.append(choose_best_post(group))

    for group in comment_groups.values():
        if len(group) > 1:
            stats.duplicate_comment_groups += 1
            stats.dropped_duplicate_comments += len(group) - 1
        cleaned_rows.append(choose_best_comment(group))

    cleaned_rows.sort(key=lambda row: int(row.get("created_utc", "0") or 0), reverse=True)
    stats.output_rows = len(cleaned_rows)
    stats.kept_posts = sum(1 for row in cleaned_rows if row.get("row_type") == "post")
    stats.kept_comments = sum(
        1 for row in cleaned_rows if row.get("row_type") == "comment"
    )

    write_csv(output_path, cleaned_rows, fieldnames)
    write_report(report_path, stats, output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
