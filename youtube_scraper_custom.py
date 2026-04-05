"""
YouTube Custom Date Range Scraper for Claude and Anthropic topics.

Usage:
    python youtube_scraper_custom.py
    python youtube_scraper_custom.py --start 2024-06-01 --end 2024-12-31
    python youtube_scraper_custom.py --start 2024-06-01 --end 2024-12-31 --output data/youtube_20240601_20241231.csv
"""

import os
import re
import time
import argparse
import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── Setup ─────────────────────────────────────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    raise ValueError("YOUTUBE_API_KEY not found in .env file")

youtube = build("youtube", "v3", developerKey=API_KEY)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SEARCH_QUERIES = [
    "Claude AI",
    "Anthropic Claude",
    "Claude vs ChatGPT",
    "Claude AI tutorial",
    "Claude 3",
    "Claude Opus",
    "Claude Sonnet",
]

MAX_PAGES        = 4
RESULTS_PER_PAGE = 50

errors_log       = []
quota_units_used = 0
quota_exceeded   = False

OUTPUT_COLS = [
    "video_id", "title", "channel_name", "publish_date", "description",
    "view_count", "like_count", "comment_count", "duration_seconds",
    "tags", "engagement_rate", "is_comparison", "is_tutorial",
    "is_review", "content_type", "search_query", "platform",
]


def default_output_path(start_date: datetime.date, end_date: datetime.date) -> Path:
    filename = f"youtube_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
    return Path(BASE_DIR) / filename


# ── Date input ────────────────────────────────────────────────────────────────

def ask_date(prompt: str, default: datetime.date | None = None) -> datetime.date:
    hint = f" [{default}]" if default else ""
    while True:
        raw = input(f"{prompt}{hint}: ").strip()
        if not raw and default:
            return default
        try:
            return datetime.date.fromisoformat(raw)
        except ValueError:
            print("  Invalid format. Please enter YYYY-MM-DD (e.g. 2024-01-15).")


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_duration(s: str) -> int:
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", s or "")
    if not m:
        return 0
    return int(m.group(1) or 0) * 3600 + int(m.group(2) or 0) * 60 + int(m.group(3) or 0)


def to_api_str(d: datetime.date) -> str:
    return datetime.datetime(d.year, d.month, d.day,
                             tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def month_windows(start: datetime.date, end: datetime.date):
    """Yield (after_str, before_str, label) per calendar month in [start, end]."""
    cur = start.replace(day=1)
    while cur <= end:
        nxt = (cur.replace(month=cur.month + 1, day=1)
               if cur.month < 12
               else cur.replace(year=cur.year + 1, month=1, day=1))
        upper = min(nxt, end + datetime.timedelta(days=1))
        yield to_api_str(cur), to_api_str(upper), cur.strftime("%Y-%m")
        cur = nxt


# ── API calls ─────────────────────────────────────────────────────────────────

def search_videos(query: str, after: str, before: str) -> tuple[list[dict], bool]:
    global quota_units_used, quota_exceeded
    results    = []
    page_token = None

    for page_num in range(MAX_PAGES):
        params = {
            "q":                 query,
            "type":              "video",
            "maxResults":        RESULTS_PER_PAGE,
            "order":             "relevance",
            "publishedAfter":    after,
            "publishedBefore":   before,
            "relevanceLanguage": "en",
            "part":              "id,snippet",
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            resp = youtube.search().list(**params).execute()
            quota_units_used += 100

            for item in resp.get("items", []):
                vid_id = item["id"].get("videoId")
                if not vid_id:
                    continue
                snip = item.get("snippet", {})
                results.append({
                    "video_id":     vid_id,
                    "title":        snip.get("title", ""),
                    "channel_name": snip.get("channelTitle", ""),
                    "publish_date": snip.get("publishedAt", ""),
                    "description":  snip.get("description", ""),
                    "search_query": query,
                })

            page_token = resp.get("nextPageToken")
            if not page_token:
                break
            time.sleep(0.5)

        except HttpError as e:
            if e.resp.status == 403:
                print(f"  [QUOTA] Exceeded on '{query}' page {page_num + 1}.")
                errors_log.append(f"Quota exceeded: '{query}' after={after}")
                quota_exceeded = True
                return results, True
            msg = f"HttpError {e.resp.status} on '{query}' page {page_num + 1}: {e}"
            print(f"  [ERR] {msg}")
            errors_log.append(msg)
            break
        except Exception as e:
            msg = f"Error on '{query}' page {page_num + 1}: {e}"
            print(f"  [ERR] {msg} — retrying in 5s")
            errors_log.append(msg)
            time.sleep(5)

    return results, False


def fetch_video_details(video_ids: list[str]) -> dict:
    global quota_units_used, quota_exceeded
    details = {}
    batches = [video_ids[i:i + 50] for i in range(0, len(video_ids), 50)]

    for batch_num, batch in enumerate(batches):
        try:
            resp = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(batch),
            ).execute()
            quota_units_used += len(batch)

            for item in resp.get("items", []):
                vid_id  = item["id"]
                stats   = item.get("statistics", {})
                content = item.get("contentDetails", {})
                snippet = item.get("snippet", {})
                details[vid_id] = {
                    "view_count":       int(stats.get("viewCount",    0) or 0),
                    "like_count":       int(stats.get("likeCount",    0) or 0),
                    "comment_count":    int(stats.get("commentCount", 0) or 0),
                    "duration_seconds": parse_duration(content.get("duration", "")),
                    "tags":             snippet.get("tags", []),
                }
            time.sleep(0.5)

        except HttpError as e:
            if e.resp.status == 403:
                print(f"  [QUOTA] Exceeded on details batch {batch_num + 1}.")
                errors_log.append(f"Quota exceeded: details batch {batch_num + 1}")
                quota_exceeded = True
                return details
            msg = f"HttpError {e.resp.status} on details batch {batch_num + 1}: {e}"
            print(f"  [ERR] {msg}")
            errors_log.append(msg)
        except Exception as e:
            msg = f"Error on details batch {batch_num + 1}: {e}"
            print(f"  [ERR] {msg} — retrying in 5s")
            errors_log.append(msg)
            time.sleep(5)

    return details


# ── Computed columns ──────────────────────────────────────────────────────────

def add_computed_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["engagement_rate"] = df.apply(
        lambda r: (r["like_count"] + r["comment_count"]) / r["view_count"]
        if r["view_count"] > 0 else 0,
        axis=1,
    )
    title_lower = df["title"].str.lower()
    df["is_comparison"] = title_lower.str.contains(r"\bvs\b|\bversus\b", regex=True)
    df["is_tutorial"]   = title_lower.str.contains(r"tutorial|how to|guide", regex=True)
    df["is_review"]     = title_lower.str.contains(r"review|honest", regex=True)
    df["content_type"]  = pd.cut(
        df["duration_seconds"],
        bins=[-1, 179, 899, float("inf")],
        labels=["short", "medium", "long"],
    )
    df["platform"] = "youtube"
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape YouTube Claude AI videos for a custom date range."
    )
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   help="End date   YYYY-MM-DD")
    parser.add_argument(
        "--output",
        help=(
            "Output CSV path. Defaults to youtube_YYYYMMDD_YYYYMMDD.csv in the "
            "project directory."
        ),
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  YouTube Custom Date Range Scraper — Claude AI")
    print("=" * 60)

    # ── Get dates ──────────────────────────────────────────────────────────────
    if args.start:
        try:
            start_date = datetime.date.fromisoformat(args.start)
        except ValueError:
            print(f"Invalid --start date: {args.start}")
            return 1
    else:
        start_date = ask_date("Enter start date (YYYY-MM-DD)")

    if args.end:
        try:
            end_date = datetime.date.fromisoformat(args.end)
        except ValueError:
            print(f"Invalid --end date: {args.end}")
            return 1
    else:
        end_date = ask_date("Enter end date   (YYYY-MM-DD)",
                            default=datetime.date.today())

    if start_date > end_date:
        print("Error: start date must be before end date.")
        return 1

    # ── Output file ────────────────────────────────────────────────────────────
    output_path = Path(args.output) if args.output else default_output_path(start_date, end_date)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_name = output_path.name

    span_days = (end_date - start_date).days + 1
    windows   = list(month_windows(start_date, end_date))

    print(f"\nDate range : {start_date} → {end_date}  ({span_days} days, {len(windows)} month window(s))")
    print(f"Output     : {output_name}\n")

    # ── Scrape month by month ──────────────────────────────────────────────────
    all_rows = {}   # video_id → row  (auto-dedup)

    for idx, (after, before, label) in enumerate(windows):
        if quota_exceeded:
            print("\n[QUOTA] Daily limit reached — partial results saved.")
            break

        print(f"[{idx + 1}/{len(windows)}]  {label}  ({after[:10]} → {before[:10]})")

        for query in SEARCH_QUERIES:
            if quota_exceeded:
                break
            print(f"  Searching: '{query}'")
            rows, hit = search_videos(query, after, before)
            for r in rows:
                all_rows.setdefault(r["video_id"], r)
            if hit:
                break

        print(f"  Cumulative unique videos: {len(all_rows)}")

    # ── Enrich with details ────────────────────────────────────────────────────
    unique = list(all_rows.values())
    if not unique:
        print("\nNo videos found for the given date range.")
        return 0

    print(f"\nFetching video details for {len(unique)} videos...")
    details = fetch_video_details([r["video_id"] for r in unique])

    for r in unique:
        d = details.get(r["video_id"], {})
        r["view_count"]       = d.get("view_count", 0)
        r["like_count"]       = d.get("like_count", 0)
        r["comment_count"]    = d.get("comment_count", 0)
        r["duration_seconds"] = d.get("duration_seconds", 0)
        r["tags"]             = d.get("tags", [])

    # ── Build & save DataFrame ─────────────────────────────────────────────────
    df = pd.DataFrame(unique)
    df = add_computed_columns(df)
    df = df[[c for c in OUTPUT_COLS if c in df.columns]]
    df.sort_values("view_count", ascending=False, inplace=True)
    df.to_csv(output_path, index=False)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total videos saved  : {len(df)}")
    print(f"Output file         : {output_path}")

    if "publish_date" in df.columns:
        dates = pd.to_datetime(df["publish_date"], errors="coerce").dropna()
        if not dates.empty:
            print(f"Actual date range   : {dates.min().date()} → {dates.max().date()}")

    if not df.empty and "channel_name" in df.columns:
        top = df.groupby("channel_name")["view_count"].sum().sort_values(ascending=False).head(5)
        print("\nTop 5 channels by views:")
        for ch, v in top.items():
            print(f"  {ch}: {v:,}")

    print(f"\nAvg engagement rate : {df['engagement_rate'].mean():.4f}")
    print(f"Quota units used    : {quota_units_used:,}")

    if errors_log:
        print(f"\nErrors ({len(errors_log)}):")
        for e in errors_log:
            print(f"  - {e}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
