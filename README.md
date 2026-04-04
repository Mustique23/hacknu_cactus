# Reddit Anthropic Scraper

This project scrapes Reddit search results for mentions of Anthropic and Claude-related products from the last 7 days and exports the dataset to CSV.

## What it collects

The scraper searches Reddit for recent posts matching queries such as:

- `Anthropic`
- `Anthropic Claude`
- `Claude`
- `Claude AI`
- `Claude Code`
- `Claude API`
- `Claude Sonnet`
- `Claude Haiku`
- `Claude Opus`
- `Claude Max`
- `Model Context Protocol`

The generated CSV includes:

- Reddit post ID
- subreddit
- title
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
data/reddit_anthropic_last_week.csv
```

Useful options:

```bash
python3 reddit_scraper.py --output data/custom.csv
python3 reddit_scraper.py --days 7 --limit-per-query 150
python3 reddit_scraper.py --query '"Claude Max"' --query '"Anthropic API"'
```

## Notes

- The scraper uses Reddit's public JSON search endpoint, so request rate matters. If Reddit rate-limits you, increase `--sleep-seconds`.
- Results are deduplicated across queries by Reddit post ID.
- Only posts created within the requested lookback window are written to the CSV.
- A relevance filter keeps Anthropic- and Claude-product context while reducing obvious false positives from the generic name `Claude`.
