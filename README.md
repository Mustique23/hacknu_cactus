# Weekly Reddit and YouTube Pipeline

This project scrapes Reddit posts/comments and YouTube videos mentioning Anthropic and Claude-related products, cleans the Reddit dataset, and merges both sources into a timeline CSV.

## What it collects

The scraper combines:

- global Reddit search across Anthropic and Claude query variants
- subreddit-scoped search across AI and developer communities
- direct `/new` crawling for Anthropic-centric subreddits
- comment harvesting from matched threads

Default queries include:

- `Anthropic`
- `Anthropic Claude`
- `Anthropic API`
- `Claude`
- `Claude AI`
- `Claude Code`
- `Claude API`
- `Claude Sonnet`
- `Claude Haiku`
- `Claude Opus`
- `Claude Max`
- `Claude 3`
- `Claude 3.5`
- `Claude 3.7`
- `Claude 4`
- `Model Context Protocol`

The generated CSV includes:

- row ID and row type (`post` or `comment`)
- subreddit
- source strategy
- post ID and parent ID
- thread root title
- title
- body text
- author
- creation timestamp
- score
- number of comments
- permalink
- external URL
- matched query
- detected Anthropic product keywords
- post selftext

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
python3 reddit_scraper.py
```

The default output file is:

```text
data/reddit_anthropic_discussions_since_2023.csv
```

The default run report is:

```text
data/reddit_anthropic_discussions_since_2023_report.md
```

The cleaner outputs:

```text
data/reddit_anthropic_discussions_since_2023_large_clean.csv
data/reddit_anthropic_discussions_since_2023_large_clean_report.md
```

Useful options:

```bash
python3 reddit_scraper.py --output data/custom.csv
python3 reddit_scraper.py --start-date 2024-01-01 --limit-per-query 1500
python3 reddit_scraper.py --start-date 2024-01-01 --target-date 2024-06-30
python3 reddit_scraper.py --subreddit-query '"Claude Code"' --subreddit-query '"Claude 4"'
python3 reddit_scraper.py --max-comment-posts 600 --max-comments-per-post 250
python3 reddit_scraper.py --max-pages-per-query 20 --max-pages-per-subreddit 60
python3 reddit_scraper.py --skip-comments
python3 reddit_scraper.py --query '"Claude Max"' --query '"Anthropic API"'
python3 clean_reddit_dataset.py
```

## Weekly Pipeline

The weekly pipeline is anchored to `2026-03-01` by default and reads the current logical date from `PIPELINE_CURRENT_DATE`.

When `PIPELINE_CURRENT_DATE` advances by at least one full week past the next unprocessed start date, the pipeline will:

- scrape Reddit for that week
- clean the weekly Reddit CSV
- scrape YouTube for that week
- merge both weekly datasets into one CSV in `data/`

### One-shot mode

Use this when you want to run the scheduler once against the current date value:

```bash
export PIPELINE_CURRENT_DATE=2026-03-08
python3 weekly_pipeline.py
```

### Watch mode

Use this when you want the pipeline app to stay alive and react automatically after you manually change the date.

Important: a running process cannot see `export ...` changes made later in another shell. For live-like behavior, put `PIPELINE_CURRENT_DATE` in `.env`, start the watcher once, then edit `.env`.

Example `.env` line:

```text
PIPELINE_CURRENT_DATE=2026-03-01
```

Start the watcher:

```bash
python3 weekly_pipeline.py --watch
```

Then manually update `.env` to:

```text
PIPELINE_CURRENT_DATE=2026-03-08
```

The watcher will detect the change and generate the first weekly dataset for `2026-03-01` through `2026-03-07`.

That run creates week-scoped files such as:

```text
data/reddit_20260301_20260307_raw.csv
data/reddit_20260301_20260307_clean.csv
data/youtube_20260301_20260307.csv
data/growth_manager_merged_20260301_20260307.csv
```

The pipeline tracks progress in:

```text
data/weekly_pipeline_state.json
```

Useful options:

```bash
python3 weekly_pipeline.py --dry-run
python3 weekly_pipeline.py --current-date 2026-03-15
python3 weekly_pipeline.py --max-weeks 1
python3 weekly_pipeline.py --watch --poll-seconds 1
python3 weekly_pipeline.py --watch --env-file /tmp/pipeline.env
```

## Notes

- The scraper uses Reddit's public JSON endpoints, so request rate matters. If Reddit rate-limits you, increase `--sleep-seconds`.
- Results are deduplicated across repeated searches and comment harvests.
- Only rows created between the configured `--start-date` and `--target-date` inclusive are written to the CSV.
- Subreddit-scoped search uses a smaller default query set than global search to reduce runtime; override it with `--subreddit-query` if you want a broader subreddit pass.
- A relevance filter keeps Anthropic- and Claude-product context while reducing obvious false positives from the generic name `Claude`.
- The generated report documents request failures, retries, rate limits, truncated comment trees, and other issues encountered during the run.
- The cleaner removes deleted authors, bot/mod rows, low-signal comments, promotional rows, and normalized content duplicates from the larger CSV.
- `youtube_scraper_custom.py` accepts `--output`, which the weekly pipeline uses to save weekly files directly into `data/`.
