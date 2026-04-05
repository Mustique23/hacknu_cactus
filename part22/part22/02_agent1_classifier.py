"""
02_agent1_classifier.py
=======================
Agent 1 — LLM Growth DNA Classifier Pipeline.

Iterates through growth_manager_cleaned.csv, sends each post to the LLM API,
and appends 4 classification columns:
  - TTV_Delta_Mentioned       (bool)
  - Artifacts_Remix_Signal    (bool)
  - Aha_Moment_Trigger        (str | null)
  - Demographic_Signal        (str | null)

Supports: Gemini Flash (default), OpenAI GPT-4o-mini (fallback)

Usage:
  export GEMINI_API_KEY="your_key_here"         # Recommended (~$0.50 total)
  python3 02_agent1_classifier.py

  # OR with OpenAI:
  export OPENAI_API_KEY="your_key_here"
  python3 02_agent1_classifier.py --provider openai

  # Sample mode (first N rows only, for validation):
  python3 02_agent1_classifier.py --sample 50

  # Resume a crashed run (reads checkpoint):
  python3 02_agent1_classifier.py --resume

Outputs:
  - growth_manager_classified.csv   — Full dataset with 4 new columns
  - classification_checkpoint.csv   — Auto-checkpoint every 500 rows
  - classification_errors.jsonl     — Failed rows (for debugging)
"""

import sys
import os
import json
import time
import argparse
import re
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────
INPUT_PATH = "growth_manager_cleaned.csv"
OUTPUT_PATH = "growth_manager_classified.csv"
CHECKPOINT_PATH = "classification_checkpoint.csv"
ERRORS_PATH = "classification_errors.jsonl"
MAX_TEXT_LEN = 800      # Truncate post text to keep tokens low
BATCH_DELAY = 0.3       # Seconds between API calls (rate limit safety)
CHECKPOINT_EVERY = 500  # Save checkpoint every N rows
MAX_RETRIES = 3
RETRY_DELAY = 5         # Seconds to wait on rate limit error

# ── System Prompt ─────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a Growth Engineering Classifier for an AI company analysis project.
Analyze this social media post about Claude AI (by Anthropic).
Output ONLY a strict JSON object — no explanation, no markdown, no prose.

Evaluate these 'Growth DNA' markers:
{
  "TTV_Delta_Mentioned": <true|false>,
  "Artifacts_Remix_Signal": <true|false>,
  "Aha_Moment_Trigger": <"CSS/UI Rendering"|"ChatGPT Refugee"|"Long-Context"|"Mobile Speed"|null>,
  "Demographic_Signal": <"University/Student"|"Junior Dev"|"TikTok-style formatting"|null>
}

CLASSIFICATION RULES:

TTV_Delta_Mentioned = true if the post EXPLICITLY compares Claude's speed or time savings
  vs. a competitor or manual process. Positive signals:
  - "3x faster", "10x faster", "5x faster"
  - "saved me [N] hours/days/weeks"
  - "used to take [X], now takes [Y]"
  - "one-shot [N] lines of code"
  - "didn't write a line of code in months"
  - "blasted through it", "instantly solved"
  - "revolutionized how fast", "radically accelerated"
  - Any explicit before/after time comparison with Claude

Artifacts_Remix_Signal = true if the post shares a code/UI output AND invites others
  to use, copy, fork, or remix it. Positive signals:
  - Sharing a GitHub link with working code
  - "try this prompt", "here's my exact setup", "copy this"
  - Posting a code snippet others can directly use
  - "built [tool] with Claude" + encouraging others to clone/use it
  - "one-shot [game/app/component]" that others can try
  - Sharing a template, workflow, or reusable configuration

Aha_Moment_Trigger: Pick the SINGLE strongest trigger category or null:
  - "CSS/UI Rendering" — Post marvels at Claude's visual/UI output quality
      e.g., "Claude built this entire UI one-shot", "the CSS output was perfect"
  - "ChatGPT Refugee" — Post explicitly migrates FROM ChatGPT/GPT-4 TO Claude
      e.g., "done with ChatGPT", "switched to Claude", "moved from GPT-4"
  - "Long-Context" — Post highlights handling of massive files, long PDFs, huge codebases
      e.g., "fed it my entire codebase", "processed a 200-page PDF"
  - "Mobile Speed" — Post highlights response speed on mobile or API latency
      e.g., "fastest on mobile", "instant responses", API speed comparison

Demographic_Signal: Pick the SINGLE strongest signal or null:
  - "University/Student" — Mentions school, coursework, student projects, learning coding
  - "Junior Dev" — "just started coding", "new to this", "vibe coding", entry-level framing
  - "TikTok-style formatting" — Excessive line breaks, emojis as bullets, very short staccato sentences

Return ONLY valid JSON. Do not include any text outside the JSON object."""

NULL_RESULT = {
    "TTV_Delta_Mentioned": False,
    "Artifacts_Remix_Signal": False,
    "Aha_Moment_Trigger": None,
    "Demographic_Signal": None,
}

# ── Provider setup ─────────────────────────────────────────────────────────

def get_provider(provider_name: str):
    """Return a classification function for the specified provider."""
    if provider_name == "gemini":
        return _setup_gemini()
    elif provider_name == "openai":
        return _setup_openai()
    else:
        raise ValueError(f"Unknown provider: {provider_name}")


def _setup_gemini():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("❌ ERROR: GEMINI_API_KEY environment variable not set.")
        print("   Run: export GEMINI_API_KEY='your_key_here'")
        print("   Or use OpenAI: export OPENAI_API_KEY='your_key' && python3 02_agent1_classifier.py --provider openai")
        sys.exit(1)

    try:
        import google.generativeai as genai
    except ImportError:
        print("❌ ERROR: google-generativeai not installed.")
        print("   Run: pip install google-generativeai")
        sys.exit(1)

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash",
        system_instruction=SYSTEM_PROMPT,
        generation_config=genai.GenerationConfig(
            temperature=0.0,
            max_output_tokens=200,
            response_mime_type="application/json",
        ),
    )
    print("✅ Provider: Gemini 1.5 Flash")

    def classify(text: str) -> dict:
        response = model.generate_content(text[:MAX_TEXT_LEN])
        return json.loads(response.text)

    return classify


def _setup_openai():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("❌ ERROR: OPENAI_API_KEY environment variable not set.")
        print("   Run: export OPENAI_API_KEY='your_key_here'")
        sys.exit(1)

    try:
        from openai import OpenAI
    except ImportError:
        print("❌ ERROR: openai not installed.")
        print("   Run: pip install openai")
        sys.exit(1)

    client = OpenAI(api_key=api_key)
    print("✅ Provider: OpenAI GPT-4o-mini")

    def classify(text: str) -> dict:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text[:MAX_TEXT_LEN]},
            ],
            temperature=0.0,
            max_tokens=200,
            response_format={"type": "json_object"},
        )
        return json.loads(response.choices[0].message.content)

    return classify


# ── Validation ─────────────────────────────────────────────────────────────

def validate_and_normalize(result: dict) -> dict:
    """Ensure all required keys exist with valid values."""
    VALID_AHA = {"CSS/UI Rendering", "ChatGPT Refugee", "Long-Context", "Mobile Speed", None}
    VALID_DEMO = {"University/Student", "Junior Dev", "TikTok-style formatting", None}

    out = {}
    out["TTV_Delta_Mentioned"] = bool(result.get("TTV_Delta_Mentioned", False))
    out["Artifacts_Remix_Signal"] = bool(result.get("Artifacts_Remix_Signal", False))

    aha = result.get("Aha_Moment_Trigger")
    out["Aha_Moment_Trigger"] = aha if aha in VALID_AHA else None

    demo = result.get("Demographic_Signal")
    out["Demographic_Signal"] = demo if demo in VALID_DEMO else None

    return out


# ── Core classification loop ───────────────────────────────────────────────

def classify_row(classify_fn, text: str, record_id: str, errors_file) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = classify_fn(text)
            return validate_and_normalize(result)
        except Exception as e:
            err_msg = str(e)
            if "rate" in err_msg.lower() or "429" in err_msg or "quota" in err_msg.lower():
                wait = RETRY_DELAY * attempt
                print(f"  ⚠️  Rate limit hit — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
            elif attempt < MAX_RETRIES:
                time.sleep(1)
            else:
                # Log failure
                errors_file.write(json.dumps({
                    "record_id": record_id,
                    "error": err_msg,
                    "text_preview": text[:100],
                }) + "\n")
                errors_file.flush()
                return NULL_RESULT.copy()
    return NULL_RESULT.copy()


def main():
    parser = argparse.ArgumentParser(description="Agent 1 — LLM Growth DNA Classifier")
    parser.add_argument("--provider", choices=["gemini", "openai"], default="gemini")
    parser.add_argument("--sample", type=int, default=None, help="Only classify first N rows")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    args = parser.parse_args()

    print("=" * 60)
    print("  AGENT 1 — LLM GROWTH DNA CLASSIFIER")
    print("=" * 60)

    # ── Load cleaned data ──────────────────────────────────────────────────
    if not os.path.exists(INPUT_PATH):
        print(f"❌ ERROR: {INPUT_PATH} not found. Run 01_bot_filter.py first.")
        sys.exit(1)

    df = pd.read_csv(INPUT_PATH, low_memory=False)
    df["body_text"] = df["body_text"].fillna("")
    df["title"] = df["title"].fillna("")
    df["full_text"] = (df["title"] + " " + df["body_text"]).str.strip()

    if args.sample:
        print(f"🔬 SAMPLE MODE: classifying first {args.sample} rows")
        df = df.head(args.sample).copy()

    # ── Resume logic ───────────────────────────────────────────────────────
    start_idx = 0
    if args.resume and os.path.exists(CHECKPOINT_PATH):
        checkpoint = pd.read_csv(CHECKPOINT_PATH, low_memory=False)
        done_ids = set(checkpoint["record_id"])
        df = df[~df["record_id"].isin(done_ids)].copy()
        start_idx = len(checkpoint)
        print(f"♻️  Resuming from checkpoint — {start_idx:,} already done, {len(df):,} remaining")

    total = len(df)
    print(f"📊 Total rows to classify: {total:,}")

    # ── Setup provider ─────────────────────────────────────────────────────
    classify_fn = get_provider(args.provider)

    # ── Prepare output columns ─────────────────────────────────────────────
    results = []
    errors_file = open(ERRORS_PATH, "a")

    print(f"\nClassifying... (checkpoint every {CHECKPOINT_EVERY} rows)")
    print("-" * 60)

    for i, (idx, row) in enumerate(df.iterrows()):
        text = str(row["full_text"])[:MAX_TEXT_LEN]
        result = classify_row(classify_fn, text, row["record_id"], errors_file)
        result["record_id"] = row["record_id"]
        results.append(result)

        # Progress indicator
        if (i + 1) % 100 == 0 or (i + 1) == total:
            ttv_count = sum(1 for r in results if r["TTV_Delta_Mentioned"])
            art_count = sum(1 for r in results if r["Artifacts_Remix_Signal"])
            print(
                f"  [{i+1:>5}/{total}] TTV={ttv_count} ({ttv_count/(i+1)*100:.1f}%)  "
                f"Artifacts={art_count} ({art_count/(i+1)*100:.1f}%)"
            )

        # Checkpoint
        if (i + 1) % CHECKPOINT_EVERY == 0:
            _save_checkpoint(df, results, start_idx)

        time.sleep(BATCH_DELAY)

    errors_file.close()

    # ── Merge results ──────────────────────────────────────────────────────
    results_df = pd.DataFrame(results)
    df_out = pd.merge(df, results_df, on="record_id", how="left")

    # If resuming, combine checkpoint + new results
    if args.resume and os.path.exists(CHECKPOINT_PATH):
        checkpoint_df = pd.read_csv(CHECKPOINT_PATH, low_memory=False)
        df_out = pd.concat([checkpoint_df, df_out], ignore_index=True)

    df_out = df_out.drop(columns=["full_text"], errors="ignore")

    output_path = OUTPUT_PATH if not args.sample else f"sample_{args.sample}_classified.csv"
    df_out.to_csv(output_path, index=False)
    print(f"\n✅ Saved: {output_path}  ({len(df_out):,} rows)")

    # ── Final stats ────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  CLASSIFICATION SUMMARY")
    print("=" * 60)
    print(f"  Rows classified   : {len(df_out):,}")
    print(f"  TTV_Delta=True    : {df_out['TTV_Delta_Mentioned'].sum():,}  ({df_out['TTV_Delta_Mentioned'].mean()*100:.1f}%)")
    print(f"  Artifacts=True    : {df_out['Artifacts_Remix_Signal'].sum():,}  ({df_out['Artifacts_Remix_Signal'].mean()*100:.1f}%)")
    print(f"\n  Aha Moment Trigger distribution:")
    for val, count in df_out["Aha_Moment_Trigger"].value_counts(dropna=False).items():
        print(f"    {str(val):<25} {count:>5,}  ({count/len(df_out)*100:.1f}%)")
    print(f"\n  Demographic Signal distribution:")
    for val, count in df_out["Demographic_Signal"].value_counts(dropna=False).items():
        print(f"    {str(val):<25} {count:>5,}  ({count/len(df_out)*100:.1f}%)")


def _save_checkpoint(df, results, start_offset):
    results_df = pd.DataFrame(results)
    checkpoint = pd.merge(
        df.iloc[:len(results)],
        results_df,
        on="record_id",
        how="left"
    ).drop(columns=["full_text"], errors="ignore")
    checkpoint.to_csv(CHECKPOINT_PATH, index=False)
    print(f"  💾 Checkpoint saved: {len(checkpoint):,} rows")


if __name__ == "__main__":
    main()
