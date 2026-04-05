#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
CURRENT_DATE_ENV_VAR = "PIPELINE_CURRENT_DATE"
DEFAULT_INITIAL_DATE = date(2026, 3, 1)
DEFAULT_STATE_PATH = DATA_DIR / "weekly_pipeline_state.json"
DEFAULT_ENV_FILE = ROOT_DIR / ".env"


@dataclass
class PipelineState:
    initial_start_date: str
    next_start_date: str
    last_completed_end_date: str
    last_seen_current_date: str


def parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid {field_name}: {value!r}. Expected YYYY-MM-DD.") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the weekly Reddit + YouTube scraping pipeline whenever the "
            f"{CURRENT_DATE_ENV_VAR} date has advanced by at least one full week."
        )
    )
    parser.add_argument(
        "--current-date",
        help=(
            "Current date in YYYY-MM-DD format. Defaults to the "
            f"{CURRENT_DATE_ENV_VAR} environment variable."
        ),
    )
    parser.add_argument(
        "--initial-date",
        default=DEFAULT_INITIAL_DATE.isoformat(),
        help="Initial weekly anchor date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--state-file",
        default=str(DEFAULT_STATE_PATH),
        help="Path to the pipeline state JSON file.",
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_FILE),
        help=(
            "Env file to read while watching. The file may contain "
            f"{CURRENT_DATE_ENV_VAR}=YYYY-MM-DD."
        ),
    )
    parser.add_argument(
        "--max-weeks",
        type=int,
        help="Optional cap on how many pending weeks to process in this run.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help=(
            "Keep the pipeline running and poll the env file for date changes. "
            "Use this when you want the pipeline to react automatically after you "
            f"edit {CURRENT_DATE_ENV_VAR}."
        ),
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=2.0,
        help="Polling interval in seconds for --watch mode.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the work that would run without scraping or writing state.",
    )
    return parser


def load_dynamic_env(env_file: Path) -> dict[str, str]:
    if not env_file.exists():
        return {}
    values: dict[str, str] = {}
    with env_file.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def resolve_current_date(
    args: argparse.Namespace,
    env_file: Path,
    *,
    allow_missing: bool = False,
) -> date | None:
    raw_value = args.current_date
    if not raw_value:
        dynamic_env = load_dynamic_env(env_file)
        raw_value = dynamic_env.get(CURRENT_DATE_ENV_VAR) or os.getenv(CURRENT_DATE_ENV_VAR)
    if not raw_value:
        if allow_missing:
            return None
        raise SystemExit(
            "Missing current date. Pass --current-date, set the process env var, "
            f"or add {CURRENT_DATE_ENV_VAR}=YYYY-MM-DD to {env_file}."
        )
    return parse_iso_date(raw_value, "current date")


def load_state(state_path: Path, initial_date: date) -> PipelineState:
    if not state_path.exists():
        return PipelineState(
            initial_start_date=initial_date.isoformat(),
            next_start_date=initial_date.isoformat(),
            last_completed_end_date="",
            last_seen_current_date="",
        )

    with state_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    next_start_date = payload.get("next_start_date") or initial_date.isoformat()
    return PipelineState(
        initial_start_date=payload.get("initial_start_date") or initial_date.isoformat(),
        next_start_date=next_start_date,
        last_completed_end_date=payload.get("last_completed_end_date", ""),
        last_seen_current_date=payload.get("last_seen_current_date", ""),
    )


def save_state(state_path: Path, state: PipelineState) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(asdict(state), indent=2) + "\n", encoding="utf-8")


def week_end_for(start_date: date) -> date:
    return start_date + timedelta(days=6)


def discover_pending_weeks(
    next_start_date: date,
    current_date: date,
    max_weeks: int | None,
) -> list[tuple[date, date]]:
    pending: list[tuple[date, date]] = []
    cursor = next_start_date

    while current_date >= cursor + timedelta(days=7):
        pending.append((cursor, week_end_for(cursor)))
        cursor += timedelta(days=7)
        if max_weeks is not None and len(pending) >= max_weeks:
            break

    return pending


def weekly_paths(start_date: date, end_date: date) -> dict[str, Path]:
    token = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    return {
        "reddit_raw": DATA_DIR / f"reddit_{token}_raw.csv",
        "reddit_clean": DATA_DIR / f"reddit_{token}_clean.csv",
        "youtube": DATA_DIR / f"youtube_{token}.csv",
        "merged": DATA_DIR / f"growth_manager_merged_{token}.csv",
        "reddit_report": DATA_DIR / f"reddit_{token}_raw_report.md",
        "reddit_clean_report": DATA_DIR / f"reddit_{token}_clean_report.md",
        "merged_report": DATA_DIR / f"growth_manager_merged_{token}_report.md",
    }


def run_command(label: str, command: list[str], dry_run: bool) -> None:
    rendered = " ".join(command)
    print(f"[run] {label}: {rendered}")
    if dry_run:
        return
    subprocess.run(command, cwd=ROOT_DIR, check=True)


def process_week(start_date: date, end_date: date, dry_run: bool) -> None:
    paths = weekly_paths(start_date, end_date)
    common_range = [start_date.isoformat(), end_date.isoformat()]

    reddit_command = [
        sys.executable,
        str(ROOT_DIR / "reddit_scraper.py"),
        "--start-date",
        common_range[0],
        "--target-date",
        common_range[1],
        "--output",
        str(paths["reddit_raw"]),
        "--report-output",
        str(paths["reddit_report"]),
    ]
    clean_command = [
        sys.executable,
        str(ROOT_DIR / "clean_reddit_dataset.py"),
        "--input",
        str(paths["reddit_raw"]),
        "--output",
        str(paths["reddit_clean"]),
        "--report-output",
        str(paths["reddit_clean_report"]),
    ]
    youtube_command = [
        sys.executable,
        str(ROOT_DIR / "youtube_scraper_custom.py"),
        "--start",
        common_range[0],
        "--end",
        common_range[1],
        "--output",
        str(paths["youtube"]),
    ]
    merge_command = [
        sys.executable,
        str(ROOT_DIR / "merge_growth_manager_dataset.py"),
        "--youtube-input",
        str(paths["youtube"]),
        "--reddit-input",
        str(paths["reddit_clean"]),
        "--output",
        str(paths["merged"]),
        "--report-output",
        str(paths["merged_report"]),
    ]

    print(
        f"Processing weekly window {start_date.isoformat()} -> {end_date.isoformat()}"
    )
    run_command("reddit scrape", reddit_command, dry_run=dry_run)
    run_command("reddit clean", clean_command, dry_run=dry_run)
    run_command("youtube scrape", youtube_command, dry_run=dry_run)
    run_command("merge datasets", merge_command, dry_run=dry_run)


def run_pending_weeks(
    *,
    args: argparse.Namespace,
    initial_date: date,
    current_date: date,
    state_path: Path,
) -> int:
    state = load_state(state_path, initial_date)
    next_start_date = parse_iso_date(state.next_start_date, "state next_start_date")
    pending_weeks = discover_pending_weeks(
        next_start_date=next_start_date,
        current_date=current_date,
        max_weeks=args.max_weeks,
    )

    print(f"Current pipeline date: {current_date.isoformat()}")
    print(f"Initial anchor date: {initial_date.isoformat()}")
    print(f"Next unprocessed week starts: {next_start_date.isoformat()}")

    if not pending_weeks:
        next_ready_date = next_start_date + timedelta(days=7)
        print(
            "No full weekly window is ready yet. "
            f"The next dataset becomes eligible on {next_ready_date.isoformat()}."
        )
        return 0

    for start_date, end_date in pending_weeks:
        process_week(start_date, end_date, dry_run=args.dry_run)
        if not args.dry_run:
            state.next_start_date = (end_date + timedelta(days=1)).isoformat()
            state.last_completed_end_date = end_date.isoformat()
            state.last_seen_current_date = current_date.isoformat()
            save_state(state_path, state)

    if args.dry_run:
        print("Dry run completed. State file was not changed.")
        return 0

    print(
        f"Completed {len(pending_weeks)} weekly window(s). "
        f"State saved to {state_path}."
    )
    return 0


def watch_loop(
    *,
    args: argparse.Namespace,
    initial_date: date,
    state_path: Path,
    env_file: Path,
) -> int:
    print(
        f"Watching {env_file} for {CURRENT_DATE_ENV_VAR} changes every "
        f"{args.poll_seconds:.1f}s."
    )
    last_observed_date: date | None = None

    while True:
        current_date = resolve_current_date(args, env_file, allow_missing=True)
        if current_date is None:
            time.sleep(args.poll_seconds)
            continue

        if current_date != last_observed_date:
            print(f"Observed pipeline date change: {current_date.isoformat()}")
            last_observed_date = current_date
            run_pending_weeks(
                args=args,
                initial_date=initial_date,
                current_date=current_date,
                state_path=state_path,
            )

        time.sleep(args.poll_seconds)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    initial_date = parse_iso_date(args.initial_date, "initial date")
    state_path = Path(args.state_file)
    env_file = Path(args.env_file)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.watch:
        return watch_loop(
            args=args,
            initial_date=initial_date,
            state_path=state_path,
            env_file=env_file,
        )

    current_date = resolve_current_date(args, env_file)
    return run_pending_weeks(
        args=args,
        initial_date=initial_date,
        current_date=current_date,
        state_path=state_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
