# Python Projects Documentation

This document explains the real Python scripts in this repository in clear, non-technical language.

The order below follows the practical workflow of the project:

1. collect Reddit data
2. clean Reddit data
3. collect YouTube data
4. merge both sources
5. automate the weekly run
6. analyze the merged dataset in `part22/`

Note:

- `.venv/` files are not part of your project. They belong to installed packages.
- `part22/__MACOSX/...` files are archive leftovers and are not real working scripts.

## Project Summary

This repository is built to gather public online discussions about Claude and Anthropic, clean them, combine them, and then study the final dataset.

In simple terms, the project does four big jobs:

- it collects Reddit conversations
- it collects YouTube videos
- it prepares one cleaner merged dataset
- it can repeat that work every week when the date moves forward

After that, the `part22/` scripts help inspect, filter, classify, and visualize the final dataset.

## Scraping Part Explained Simply

In this project, “scraping” means:

- looking through public Reddit and YouTube results
- finding posts or videos related to Claude and Anthropic
- saving the useful details into spreadsheet-style CSV files

It does not mean hacking, breaking into systems, or accessing private user data.

This project only works with public information that can already be viewed on Reddit or YouTube.

The scraping part has three practical stages:

1. find Reddit discussions for a chosen time period
2. find YouTube videos for the same time period
3. save both sources so they can later be cleaned and merged

The weekly pipeline simply repeats that same process whenever the tracked date moves far enough forward.

## 1. [reddit_scraper.py](/home/jilnash/projects/dev/python/hacknu_reddit_scrapper/reddit_scraper.py)

### What this script does

This script collects Reddit posts and comments that mention Claude, Anthropic, or related product names.

It does not only search one place. It combines several collection methods:

- broad Reddit search
- search inside selected subreddits
- direct browsing of recent posts from important subreddits
- comment collection from matched posts

### Precise description for non-technical readers

Think of this script as a researcher reading many Reddit pages for you and writing the findings into a table.

For a chosen date range, it does the following:

1. it searches Reddit for built-in phrases like `Claude`, `Anthropic API`, or `Claude Code`
2. it checks a list of relevant communities such as AI, developer, and Anthropic-related subreddits
3. it looks at recent posts in the most important Claude-related communities
4. when it finds a matching post, it can also collect the comments under that post
5. it saves the results into one CSV file

In simple terms, it is trying to answer:

“What were people on Reddit saying about Claude or Anthropic during this time period?”

### What it actually saves

For each matching Reddit item, it can save:

- whether the row is a post or a comment
- the subreddit name
- the title
- the written text
- the author name, if available
- the date and time
- the popularity score
- the number of comments on the post
- the direct Reddit link
- the search phrase that helped find it

### What it does not try to do

This script does not:

- collect private messages
- collect hidden or private subreddit content
- understand the meaning of every post perfectly
- guarantee that every saved row is useful

That is why the next cleaning step exists.

### Why it matters

This is the main Reddit data source for the whole project. If this step does not run, there is no Reddit dataset to clean or merge later.

### What it reads

- Reddit public JSON endpoints
- the date range you choose
- built-in search terms such as `Claude`, `Anthropic API`, and `Claude Code`

### What it creates

By default it creates:

- `data/reddit_anthropic_discussions_since_2023.csv`
- `data/reddit_anthropic_discussions_since_2023_report.md`

In the weekly pipeline, it creates week-specific files such as:

- `data/reddit_20260301_20260307_raw.csv`
- `data/reddit_20260301_20260307_raw_report.md`

### What kind of information appears in the output

The Reddit CSV can include:

- post title
- post body
- comment text
- author name
- subreddit name
- date and time
- score
- comment count
- matching search phrase

### When to use it

Use this script when you want fresh Reddit data for a date range.

### Simple example

```bash
python3 reddit_scraper.py --start-date 2026-03-01 --target-date 2026-03-07
```

### Important notes

- It depends on Reddit’s public endpoints, so heavy use can be rate-limited.
- It tries to remove obvious irrelevant matches before writing the file.
- It can collect both posts and comments, so the output can be large.

## 2. [clean_reddit_dataset.py](/home/jilnash/projects/dev/python/hacknu_reddit_scrapper/clean_reddit_dataset.py)

### What this script does

This script takes the raw Reddit CSV and removes low-quality rows.

It is a cleanup step. It keeps useful Reddit discussion and removes obvious noise.

### Why it matters

Raw Reddit data often contains spam, deleted users, short empty replies, bot accounts, and repeated rows. This script makes the Reddit dataset much more usable.

### Precise description for non-technical readers

This is the “remove the junk” step for Reddit.

The first Reddit scraper is intentionally broad, because it is safer to collect too much and clean it later than to miss useful discussion.

So this cleaner reads the raw Reddit table and removes rows that are clearly weak, repetitive, or misleading.

In simple terms, it is trying to answer:

“From everything we collected on Reddit, which rows still look useful enough to keep?”

### What it reads

By default it reads:

- `data/reddit_anthropic_discussions_since_2023.csv`

It can also read any custom Reddit CSV passed with `--input`.

### What it creates

By default it creates:

- `data/reddit_anthropic_discussions_since_2023_clean.csv`
- `data/reddit_anthropic_discussions_since_2023_clean_report.md`

In the weekly pipeline, it creates files such as:

- `data/reddit_20260301_20260307_clean.csv`
- `data/reddit_20260301_20260307_clean_report.md`

### What it removes

This script removes rows such as:

- deleted or removed authors
- bot or moderator accounts
- empty comments
- very short low-signal comments like `+1` or `same`
- promotional language
- duplicated posts
- duplicated comments

### When to use it

Use this right after `reddit_scraper.py`.

### Simple example

```bash
python3 clean_reddit_dataset.py --input data/reddit_20260301_20260307_raw.csv --output data/reddit_20260301_20260307_clean.csv
```

### Important notes

- This script only cleans Reddit data, not YouTube data.
- It also creates a report so you can see what was removed and why.

## 3. [youtube_scraper_custom.py](/home/jilnash/projects/dev/python/hacknu_reddit_scrapper/youtube_scraper_custom.py)

### What this script does

This script collects YouTube videos about Claude and Anthropic within a chosen date range.

It searches YouTube using several built-in search phrases, gathers video details, and saves the result as a CSV file.

### Precise description for non-technical readers

Think of this as the YouTube version of the Reddit scraper.

For a chosen date range, it does the following:

1. it searches YouTube for built-in phrases such as `Claude AI`, `Anthropic Claude`, and `Claude vs ChatGPT`
2. it gathers matching videos published during that period
3. it asks YouTube for extra details about each video, such as views, likes, comments, and duration
4. it saves the results into one CSV file

In simple terms, it is trying to answer:

“What videos about Claude or Anthropic were published during this time period, and how much attention did they get?”

### What it actually saves

For each matching video, it can save:

- video title
- channel name
- publish date
- video description
- views
- likes
- comment count
- duration
- tags
- the search phrase that found the video

### What it does not try to do

This script does not:

- download the video itself
- download the full comment section
- judge whether the video is positive or negative
- guarantee that every result is deeply relevant

Its job is collection, not final judgment.

### Why it matters

This is the YouTube side of the project. Without it, the merged dataset would only contain Reddit discussions.

### What it reads

- the YouTube Data API
- the date range you choose
- the `YOUTUBE_API_KEY` value from `.env`

### What it creates

It creates a YouTube CSV such as:

- `data/youtube_20260301_20260307.csv`

If no output path is given, it creates a date-based CSV in the project folder.

### What kind of information appears in the output

The YouTube CSV can include:

- video title
- channel name
- publish date
- description
- views
- likes
- comment count
- duration
- tags
- basic engagement calculations

### When to use it

Use this when you want YouTube coverage for the same time period as your Reddit data.

### Simple example

```bash
python3 youtube_scraper_custom.py --start 2026-03-01 --end 2026-03-07 --output data/youtube_20260301_20260307.csv
```

### Important notes

- This script requires a valid YouTube API key in `.env`.
- If the API quota is exhausted, it may stop early and save partial results.
- It uses built-in query phrases and English-language search settings.

## 4. [merge_growth_manager_dataset.py](/home/jilnash/projects/dev/python/hacknu_reddit_scrapper/merge_growth_manager_dataset.py)

### What this script does

This script combines cleaned Reddit data and YouTube data into one timeline-style dataset.

It also removes more low-value rows during the merge, keeps a shared column layout, and sorts everything by time.

### Why it matters

This is the step that turns two separate sources into one dataset that can be studied more easily.

### What it reads

By default it looks for:

- `data/youtube*.csv`
- `data/reddit*_clean.csv`
- `data/reddit*.csv`

It can also accept direct file paths from the weekly pipeline.

### What it creates

By default it creates:

- `data/growth_manager_historical_timeline_merged.csv`
- `data/growth_manager_historical_timeline_merged_report.md`

In the weekly pipeline, it creates files such as:

- `data/growth_manager_merged_20260301_20260307.csv`
- `data/growth_manager_merged_20260301_20260307_report.md`

### What this merged file is good for

The merged file is useful when you want one table that shows:

- what was posted
- where it came from
- when it happened
- how much engagement it received

### When to use it

Use this after you already have:

- a cleaned Reddit CSV
- a YouTube CSV for the same period

### Simple example

```bash
python3 merge_growth_manager_dataset.py --youtube-input data/youtube_20260301_20260307.csv --reddit-input data/reddit_20260301_20260307_clean.csv --output data/growth_manager_merged_20260301_20260307.csv
```

### Important notes

- It now supports both relative paths and full absolute paths.
- It keeps both Reddit posts and Reddit comments as separate timeline events.
- It writes a merge report so you can inspect what was kept or removed.

## 5. [weekly_pipeline.py](/home/jilnash/projects/dev/python/hacknu_reddit_scrapper/weekly_pipeline.py)

### What this script does

This script automates the full weekly workflow.

When the tracked current date moves forward by at least one full week, it runs:

1. Reddit scraping
2. Reddit cleaning
3. YouTube scraping
4. dataset merging

### Precise description for non-technical readers

This script is the project’s automatic scheduler.

Instead of asking you to remember four separate commands every week, it watches the project date and decides when a new weekly data package should be created.

Its logic is:

1. start from an anchor date, which is currently `2026-03-01`
2. wait until a full week has passed
3. when that week becomes complete, run the Reddit scrape, Reddit clean, YouTube scrape, and merge steps for that exact week
4. save the finished weekly files
5. remember that this week is already done
6. wait for the next date increase

In simple terms, it is trying to answer:

“Has enough time passed to create the next weekly dataset yet?”

### Why it matters

This script is the automation layer. Instead of manually running every step, you can let this script do the entire chain for each eligible week.

### What it reads

- `PIPELINE_CURRENT_DATE`
- `.env` in watch mode
- the saved pipeline state file

### What it creates

For each eligible week, it creates:

- weekly raw Reddit CSV
- weekly cleaned Reddit CSV
- weekly YouTube CSV
- weekly merged CSV
- weekly report files

It also keeps:

- `data/weekly_pipeline_state.json`

### Two ways to use it

#### One-shot mode

This checks the date once and runs any missing weekly jobs once.

Example:

```bash
export PIPELINE_CURRENT_DATE=2026-03-08
python3 weekly_pipeline.py
```

#### Watch mode

This keeps the process alive and watches `.env` for date changes.

Example:

```bash
python3 weekly_pipeline.py --watch
```

Then update `.env`:

```text
PIPELINE_CURRENT_DATE=2026-03-08
```

### How it decides when to run

The default starting point is:

- `2026-03-01`

That means:

- the week `2026-03-01` to `2026-03-07` becomes eligible on `2026-03-08`
- the week `2026-03-08` to `2026-03-14` becomes eligible on `2026-03-15`

### What “automatic” means here

Automatic does not mean the computer reads your mind.

It means:

- you keep `weekly_pipeline.py --watch` running
- you manually change `PIPELINE_CURRENT_DATE` in `.env`
- the watcher notices that change
- if a full new week is now complete, it generates the new weekly files

So the human still controls the date, but the script controls the repeated work.

### Important notes

- A running process cannot see later `export ...` commands from another shell.
- For automatic behavior, watch mode plus `.env` editing is the correct setup.
- This script does not scrape by itself. It calls the other project scripts.

## 6. [part22/part22/00_data_audit.py](/home/jilnash/projects/dev/python/hacknu_reddit_scrapper/part22/part22/00_data_audit.py)

### What this script does

This script performs a first-pass audit of the merged dataset.

It answers simple questions such as:

- how many rows exist
- what columns are present
- what the date range is
- how engagement is distributed
- which high-engagement posts appear most important

### Why it matters

This is the first analysis step in `part22/`. It helps a person understand what the dataset looks like before more advanced filtering or charting begins.

### What it reads

- `growth_manager_historical_timeline_merged copy.csv`

### What it creates

- `data_audit_summary.txt`
- `top50_engagement_report.txt`

### What is special about it

It looks for a few specific phrase groups that the author treats as “growth signals,” such as:

- time-saving claims
- remix or sharing signals
- migration away from ChatGPT

### When to use it

Use this when you want a quick written summary of the merged dataset and a ranked list of the most engaging posts.

### Simple example

```bash
python3 part22/part22/00_data_audit.py
```

## 7. [part22/part22/01_bot_filter.py](/home/jilnash/projects/dev/python/hacknu_reddit_scrapper/part22/part22/01_bot_filter.py)

### What this script does

This script removes noisy or weak rows from the merged dataset before the classifier is used.

Its purpose is to leave behind a more useful “clean corpus” for deeper analysis.

### Why it matters

This is a stricter cleanup stage than the earlier Reddit cleaner. It is focused on the final merged dataset used in `part22/`.

### What it reads

- `growth_manager_historical_timeline_merged copy.csv`

### What it creates

- `growth_manager_cleaned.csv`
- `bot_filter_report.txt`

### What it removes

Examples include:

- YouTube rows treated as low-information stubs
- zero-engagement rows
- fully empty content
- generic AI news digest wording
- likely bot-like posting behavior
- low-engagement comments from broad general communities

### When to use it

Use this before running the LLM classifier in the next step.

### Simple example

```bash
python3 part22/part22/01_bot_filter.py
```

Dry-run example:

```bash
python3 part22/part22/01_bot_filter.py --dry-run
```

## 8. [part22/part22/02_agent1_classifier.py](/home/jilnash/projects/dev/python/hacknu_reddit_scrapper/part22/part22/02_agent1_classifier.py)

### What this script does

This script sends each cleaned row to an AI model and asks the model to label it with a small set of “growth DNA” tags.

It adds four fields:

- whether the post mentions time savings
- whether the post contains remix or reusable sharing signals
- what the strongest “aha moment” appears to be
- what audience signal appears strongest

### Why it matters

This script changes the dataset from plain collected content into labeled content that can be analyzed more deeply.

### What it reads

- `growth_manager_cleaned.csv`
- either `GEMINI_API_KEY` or `OPENAI_API_KEY`

### What it creates

- `growth_manager_classified.csv`
- `classification_checkpoint.csv`
- `classification_errors.jsonl`

### What makes it practical

This script includes:

- resume support after interruption
- checkpoint saving every 500 rows
- retry handling for rate limits
- sample mode for testing on a small number of rows

### When to use it

Use this after `01_bot_filter.py` if you want AI-generated labels for the cleaned corpus.

### Simple examples

Gemini:

```bash
export GEMINI_API_KEY="your_key_here"
python3 part22/part22/02_agent1_classifier.py
```

OpenAI:

```bash
export OPENAI_API_KEY="your_key_here"
python3 part22/part22/02_agent1_classifier.py --provider openai
```

Sample run:

```bash
python3 part22/part22/02_agent1_classifier.py --sample 50
```

### Important notes

- This step can cost money because it uses an external AI API.
- The labels are only as good as the prompt and the model response.
- Failed rows are written to a separate error file instead of stopping the full run.

## 9. [part22/part22/03_visualizations.py](/home/jilnash/projects/dev/python/hacknu_reddit_scrapper/part22/part22/03_visualizations.py)

### What this script does

This script turns the cleaned or classified dataset into three charts.

The charts are designed to support a growth story about Claude’s adoption patterns.

### Why it matters

This is the presentation layer of `part22/`. It helps turn the dataset into something easier to explain to other people.

### What it reads

It prefers:

- `growth_manager_classified.csv`

If that file does not exist, it falls back to:

- `growth_manager_cleaned.csv`

In fallback mode, it guesses some labels using simple keyword rules.

### What it creates

- `chart1_velocity_moat.png`
- `chart2_ttv_engagement_matrix.png`
- `chart3_remix_rate_funnel.png`

### What the three charts mean in simple language

#### Chart 1: release timing and migration spikes

This chart tries to show whether discussion about moving from ChatGPT to Claude rises around major Anthropic releases.

#### Chart 2: speed messages versus quality messages

This chart compares whether posts about speed and time savings tend to get stronger engagement than posts that do not mention speed.

#### Chart 3: remix and sharing signals

This chart looks at whether posts that share reusable outputs seem to earn more engagement.

### When to use it

Use this after the cleaned dataset exists. It works best after classification, but it can still run without it.

### Simple example

```bash
python3 part22/part22/03_visualizations.py
```

## Recommended Practical Order

If your goal is the main data pipeline, use this order:

1. `reddit_scraper.py`
2. `clean_reddit_dataset.py`
3. `youtube_scraper_custom.py`
4. `merge_growth_manager_dataset.py`
5. `weekly_pipeline.py` if you want automation

If your goal is the `part22/` analysis flow, use this order:

1. `part22/part22/00_data_audit.py`
2. `part22/part22/01_bot_filter.py`
3. `part22/part22/02_agent1_classifier.py`
4. `part22/part22/03_visualizations.py`

## Environment Variables Used in This Repository

### `YOUTUBE_API_KEY`

Used by:

- `youtube_scraper_custom.py`

### `PIPELINE_CURRENT_DATE`

Used by:

- `weekly_pipeline.py`

### `GEMINI_API_KEY`

Used by:

- `part22/part22/02_agent1_classifier.py`

### `OPENAI_API_KEY`

Used by:

- `part22/part22/02_agent1_classifier.py`

## Final Plain-Language Summary

If you explain this repository to a non-technical person, the shortest accurate description is:

“This project collects weekly Reddit and YouTube conversations about Claude, cleans them, combines them into one dataset, and then uses extra scripts to inspect, classify, and visualize what people are saying.”
