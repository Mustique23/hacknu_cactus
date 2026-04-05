"""
01_bot_filter.py
================
Bot & Slop Filter — Data hygiene pipeline.
Removes bot spam, zero-engagement noise, AI-generated slop, and YouTube stubs.

Run:
  python3 01_bot_filter.py              # Full run → growth_manager_cleaned.csv
  python3 01_bot_filter.py --dry-run    # Report only, no file written

Outputs:
  - growth_manager_cleaned.csv          — Cleaned corpus for Agent 1
  - bot_filter_report.txt               — Audit trail of every filter applied
"""

import sys
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

CSV_PATH = "growth_manager_historical_timeline_merged copy.csv"
OUTPUT_PATH = "growth_manager_cleaned.csv"
DRY_RUN = "--dry-run" in sys.argv

# ── Filter definitions ─────────────────────────────────────────────────────
# Each filter is (label, description, mask_fn) where mask_fn returns True for rows to REMOVE

SLOP_KEYWORDS = [
    "in today's ai news",
    "in today's artificial intelligence news",
    "as an ai language model",
    "as an ai assistant",
    "i am an ai",
    "i'm an ai",
    "i cannot assist",
    "breaking news:",
]

GENERIC_NEWS_KEYWORDS = [
    "ai news roundup",
    "ai news digest",
    "ai news summary",
    "ai weekly digest",
    "today in ai",
]


def contains_any(text: str, keywords: list[str]) -> bool:
    text_lower = str(text).lower()
    return any(kw in text_lower for kw in keywords)


def main():
    mode_label = "[DRY RUN]" if DRY_RUN else "[LIVE RUN]"
    print("=" * 60)
    print(f"  BOT & SLOP FILTER  {mode_label}")
    print("=" * 60)

    df = pd.read_csv(CSV_PATH, low_memory=False)
    df["body_text"] = df["body_text"].fillna("")
    df["title"] = df["title"].fillna("")
    df["full_text"] = df["title"] + " " + df["body_text"]
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")

    initial_count = len(df)
    report_lines = [
        "=" * 60,
        "  BOT & SLOP FILTER — AUDIT TRAIL",
        "=" * 60,
        f"Initial dataset: {initial_count:,} rows",
        "",
    ]

    def apply_filter(df_in, label, description, mask):
        removed = mask.sum()
        df_out = df_in[~mask].copy()
        line = f"  [{label}] {description}: removed {removed:,} rows  (remaining: {len(df_out):,})"
        print(line)
        report_lines.append(line)
        return df_out

    # ── Filter 1: YouTube stubs ────────────────────────────────────────────
    # YouTube rows have no meaningful body_text and contribute < 5% of data
    mask = df["platform"] == "youtube"
    df = apply_filter(df, "F1", "Remove YouTube stubs (no body text)", mask)

    # ── Filter 2: Zero engagement ──────────────────────────────────────────
    mask = df["engagement_total"] == 0
    df = apply_filter(df, "F2", "Remove zero-engagement posts (no social proof)", mask)

    # ── Filter 3: Null / empty body_text ──────────────────────────────────
    mask = (df["body_text"].str.strip() == "") & (df["title"].str.strip() == "")
    df = apply_filter(df, "F3", "Remove completely empty content rows", mask)

    # ── Filter 4: AI-generated slop keywords ──────────────────────────────
    mask = df["full_text"].apply(lambda t: contains_any(t, SLOP_KEYWORDS))
    df = apply_filter(df, "F4", "Remove AI-generated slop ('as an AI', 'I am an AI', etc.)", mask)

    # ── Filter 5: Generic AI news aggregator content ───────────────────────
    mask = df["full_text"].apply(lambda t: contains_any(t, GENERIC_NEWS_KEYWORDS))
    df = apply_filter(df, "F5", "Remove generic AI news digest/aggregator posts", mask)

    # ── Filter 6: High-frequency bot posters (>50 posts/day) ──────────────
    daily_counts = df.groupby(["creator_name", "published_date"]).size().reset_index(name="daily_count")
    spam_creators = daily_counts[daily_counts["daily_count"] > 50]["creator_name"].unique()
    mask = df["creator_name"].isin(spam_creators)
    df = apply_filter(df, "F6", f"Remove bot accounts posting >50x/day ({len(spam_creators)} found)", mask)

    # ── Filter 7: Low-signal general subreddit comments ───────────────────
    # Preserve high-quality tier; only cut low-engagement general_subreddit comments
    low_signal_mask = (
        df["quality_notes"].str.contains("general_subreddit", na=False) &
        df["quality_notes"].str.contains("comment_row", na=False) &
        (df["engagement_total"] < 3)
    )
    df = apply_filter(df, "F7", "Remove low-engagement general_subreddit comments (<3 engagement)", low_signal_mask)

    # ── Summary ───────────────────────────────────────────────────────────
    final_count = len(df)
    removed_total = initial_count - final_count
    summary = [
        "",
        "=" * 60,
        "  FILTER SUMMARY",
        "=" * 60,
        f"  Initial rows  : {initial_count:,}",
        f"  Removed rows  : {removed_total:,}  ({removed_total/initial_count*100:.1f}%)",
        f"  Clean corpus  : {final_count:,}  ({final_count/initial_count*100:.1f}% retained)",
        "",
        "  Platform breakdown (clean corpus):",
    ]
    for platform, count in df["platform"].value_counts().items():
        summary.append(f"    {platform:<15} {count:>6,}")
    summary += [
        "",
        "  Quality tier (clean corpus):",
    ]
    for tier, count in df["quality_tier"].value_counts().items():
        summary.append(f"    {tier:<20} {count:>6,}")
    summary += [
        "",
        "  Content format (clean corpus):",
    ]
    for fmt, count in df["content_format"].value_counts().items():
        summary.append(f"    {fmt:<20} {count:>6,}")

    for line in summary:
        print(line)
        report_lines.append(line)

    # ── Output ─────────────────────────────────────────────────────────────
    report_text = "\n".join(report_lines)
    with open("bot_filter_report.txt", "w") as f:
        f.write(report_text)
    print("\n✅ Saved: bot_filter_report.txt")

    if not DRY_RUN:
        # Drop helper column before saving
        df = df.drop(columns=["full_text"])
        df.to_csv(OUTPUT_PATH, index=False)
        print(f"✅ Saved: {OUTPUT_PATH}  ({final_count:,} rows)")
    else:
        print(f"\n[DRY RUN] No file written. Would have saved {final_count:,} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
