"""
00_data_audit.py
================
Phase 1: Data Audit — Schema profiling, engagement stats, and Manual Sieve top-50.
Run: python3 00_data_audit.py

Outputs:
  - top50_engagement_report.txt   — Top 50 posts with Growth DNA highlights
  - data_audit_summary.txt        — Schema + aggregate stats
"""

import pandas as pd
import warnings
warnings.filterwarnings("ignore")

CSV_PATH = "growth_manager_historical_timeline_merged copy.csv"
TOP_N = 50

# ── TTV Delta calibration keywords (from Manual Sieve reading) ─────────────
TTV_PHRASES = [
    "3x faster", "10x faster", "5x faster", "2x faster",
    "saved me", "used to take", "instead of", "one-shot",
    "didn't have to write", "didn't write a line",
    "blasted through", "minutes instead", "days instead",
    "revolutionized", "radically accelerated",
    "seconds", "10x", "5x", "3x",
]

# ── Artifacts / Remix calibration keywords ─────────────────────────────────
ARTIFACT_PHRASES = [
    "github.com", "try this prompt", "here's my setup",
    "my setup", "copy this", "fork", "remix", "one-shot",
    "3200 lines", "built this", "try it", "open-source",
]

# ── ChatGPT Refugee calibration keywords ──────────────────────────────────
REFUGEE_PHRASES = [
    "done with chatgpt", "switched from chatgpt", "left chatgpt",
    "moved from chatgpt", "moved to claude", "switching to claude",
    "moved to claude", "quit chatgpt", "abandoned chatgpt",
    "after the 4o", "after the gpt", "chatgpt refugee",
    "chatgpt is done", "i'm done with chatgpt", "im done with chatgpt",
]


def flag_phrases(text: str, phrases: list[str]) -> list[str]:
    text_lower = str(text).lower()
    return [p for p in phrases if p in text_lower]


def main():
    print("=" * 60)
    print("  CLAUDE GROWTH PLAYBOOK — DATA AUDIT")
    print("=" * 60)

    # ── Load ───────────────────────────────────────────────────────────────
    df = pd.read_csv(CSV_PATH, low_memory=False)
    df["body_text"] = df["body_text"].fillna("")
    df["title"] = df["title"].fillna("")
    df["full_text"] = df["title"] + " " + df["body_text"]
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")

    # ── Schema Overview ───────────────────────────────────────────────────
    lines = []
    lines.append("=" * 60)
    lines.append("SCHEMA OVERVIEW")
    lines.append("=" * 60)
    lines.append(f"Total rows        : {len(df):,}")
    lines.append(f"Total columns     : {df.shape[1]}")
    lines.append(f"Date range        : {df['published_date'].min().date()} → {df['published_date'].max().date()}")
    lines.append("")
    lines.append("Column Types:")
    for col, dtype in df.dtypes.items():
        null_count = df[col].isnull().sum()
        lines.append(f"  {col:<30} {str(dtype):<10} nulls={null_count:,}")

    lines.append("")
    lines.append("Platform Distribution:")
    for platform, count in df["platform"].value_counts().items():
        lines.append(f"  {platform:<15} {count:>6,}  ({count/len(df)*100:.1f}%)")

    lines.append("")
    lines.append("Quality Tier Distribution:")
    for tier, count in df["quality_tier"].value_counts().items():
        lines.append(f"  {tier:<20} {count:>6,}  ({count/len(df)*100:.1f}%)")

    lines.append("")
    lines.append("Content Format Distribution:")
    for fmt, count in df["content_format"].value_counts().items():
        lines.append(f"  {fmt:<20} {count:>6,}  ({count/len(df)*100:.1f}%)")

    lines.append("")
    lines.append("Engagement Stats (engagement_total):")
    stats = df["engagement_total"].describe()
    for k, v in stats.items():
        lines.append(f"  {k:<10} {v:>10.1f}")

    lines.append("")
    lines.append("Data Quality Flags:")
    lines.append(f"  Null body_text            : {(df['body_text'] == '').sum():,}")
    lines.append(f"  Zero engagement posts     : {(df['engagement_total'] == 0).sum():,}")
    lines.append(f"  High-quality tier posts   : {(df['quality_tier'] == 'high').sum():,}")

    audit_summary = "\n".join(lines)
    print(audit_summary)
    with open("data_audit_summary.txt", "w") as f:
        f.write(audit_summary)
    print("\n✅ Saved: data_audit_summary.txt")

    # ── Top 50 by Engagement ──────────────────────────────────────────────
    top50 = df.nlargest(TOP_N, "engagement_total").reset_index(drop=True)

    report_lines = []
    report_lines.append("=" * 70)
    report_lines.append(f"  MANUAL SIEVE: TOP {TOP_N} POSTS BY ENGAGEMENT")
    report_lines.append("=" * 70)
    report_lines.append("  North Star phrases detected per post:")
    report_lines.append("  [TTV] = Time-to-Value Delta language")
    report_lines.append("  [ART] = Artifacts/Remix signal")
    report_lines.append("  [REF] = ChatGPT Refugee migration language")
    report_lines.append("")

    for rank, (_, row) in enumerate(top50.iterrows(), 1):
        text = row["full_text"]
        ttv_found = flag_phrases(text, TTV_PHRASES)
        art_found = flag_phrases(text, ARTIFACT_PHRASES)
        ref_found = flag_phrases(text, REFUGEE_PHRASES)

        report_lines.append(f"{'─'*70}")
        report_lines.append(
            f"RANK {rank:02d} | Engagement: {int(row['engagement_total']):,} | "
            f"Platform: {row['platform']} | Date: {str(row['published_date'])[:10]}"
        )
        report_lines.append(
            f"Format: {row['content_format']} | Context: {row['search_context']}"
        )
        if ttv_found:
            report_lines.append(f"  [TTV] {ttv_found}")
        if art_found:
            report_lines.append(f"  [ART] {art_found}")
        if ref_found:
            report_lines.append(f"  [REF] {ref_found}")

        snippet = str(row["body_text"])[:400].replace("\n", " ")
        report_lines.append(f"  TEXT: {snippet}...")
        report_lines.append("")

    # ── North Star Summary ─────────────────────────────────────────────────
    report_lines.append("=" * 70)
    report_lines.append("  NORTH STAR PHRASE FREQUENCY (Top 50 posts)")
    report_lines.append("=" * 70)

    ttv_hits = top50["full_text"].apply(lambda t: bool(flag_phrases(t, TTV_PHRASES)))
    art_hits = top50["full_text"].apply(lambda t: bool(flag_phrases(t, ARTIFACT_PHRASES)))
    ref_hits = top50["full_text"].apply(lambda t: bool(flag_phrases(t, REFUGEE_PHRASES)))

    report_lines.append(f"  Posts with TTV Delta language     : {ttv_hits.sum()}/{TOP_N}")
    report_lines.append(f"  Posts with Artifacts/Remix signal : {art_hits.sum()}/{TOP_N}")
    report_lines.append(f"  Posts with ChatGPT Refugee signal : {ref_hits.sum()}/{TOP_N}")
    report_lines.append("")
    report_lines.append("  Most common TTV phrases:")
    from collections import Counter
    all_ttv = []
    for t in top50["full_text"]:
        all_ttv.extend(flag_phrases(t, TTV_PHRASES))
    for phrase, count in Counter(all_ttv).most_common(10):
        report_lines.append(f"    '{phrase}' — {count}x")

    top50_report = "\n".join(report_lines)
    print("\n" + top50_report)
    with open("top50_engagement_report.txt", "w") as f:
        f.write(top50_report)
    print("\n✅ Saved: top50_engagement_report.txt")


if __name__ == "__main__":
    main()
