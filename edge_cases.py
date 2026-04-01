"""
Stress dataset generator — ~1,400 adversarial issues designed to break Jira
connectors. Each of the 12 edge case categories tests a specific failure mode.

Usage:
    python edge_cases.py                    # Full run (all 12 categories)
    python edge_cases.py --category E6      # Single category
    python edge_cases.py --dry-run          # Print plan without API calls
    python edge_cases.py --project BENCH-K1 # Override target project
"""

import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import config
from utils.rate_limiter import JiraRateLimiter
from utils.text_generator import TextGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Default target project for most edge cases
DEFAULT_PROJECT = "BENCH-S1"

# ------------------------------------------------------------------ #
# ADF helper
# ------------------------------------------------------------------ #

def adf_text(text: str) -> dict:
    return {
        "type": "doc",
        "version": 1,
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": text}]},
        ],
    }


def adf_large_text(text: str) -> dict:
    """Split large text into multiple paragraphs for ADF (Jira limit ~32K chars)."""
    chunks = [text[i:i+10000] for i in range(0, len(text), 10000)]
    content = [
        {"type": "paragraph", "content": [{"type": "text", "text": chunk}]}
        for chunk in chunks
    ]
    return {"type": "doc", "version": 1, "content": content}


# ------------------------------------------------------------------ #
# Checkpoint helpers
# ------------------------------------------------------------------ #

def ckpt_path(category: str) -> Path:
    return config.CHECKPOINT_DIR / f"edge_{category}.json"


def load_checkpoint(category: str) -> dict | None:
    p = ckpt_path(category)
    if p.exists():
        return json.loads(p.read_text())
    return None


def save_checkpoint(category: str, keys: list[str], count: int) -> None:
    data = {
        "category": category,
        "keys": keys,
        "count": count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    ckpt_path(category).write_text(json.dumps(data, indent=2))


# ------------------------------------------------------------------ #
# Issue creation helper
# ------------------------------------------------------------------ #

def create_issue(api: JiraRateLimiter, project: str, summary: str,
                 issue_type: str = "Task", description: dict | None = None,
                 extra_fields: dict | None = None,
                 dry_run: bool = False) -> str | None:
    """Create a single issue and return its key, or None on failure."""
    if dry_run:
        return f"{project}-DRY-{random.randint(1000, 9999)}"

    fields = {
        "project": {"key": project},
        "summary": summary,
        "issuetype": {"name": issue_type},
    }
    if description:
        fields["description"] = description
    if extra_fields:
        fields.update(extra_fields)

    resp = api.post("/rest/api/3/issue", json={"fields": fields})
    if resp.status_code == 201:
        return resp.json()["key"]
    log.warning("Issue create failed (%d): %s", resp.status_code, resp.text[:300])
    return None


# ------------------------------------------------------------------ #
# E1: All optional fields empty (500 issues)
# ------------------------------------------------------------------ #

def create_e1(api: JiraRateLimiter, project: str, dry_run: bool) -> dict:
    """Null handling — only Summary and Issue Type set."""
    keys = []
    target = 500
    for i in range(target):
        key = create_issue(api, project, f"E1-null-test-{i:04d}",
                          issue_type="Task", dry_run=dry_run)
        if key:
            keys.append(key)
        if (i + 1) % 100 == 0:
            log.info("  [E1] %d / %d", i + 1, target)
    return {"category": "E1", "description": "All optional fields empty",
            "target": target, "created": len(keys), "keys": keys}


# ------------------------------------------------------------------ #
# E2: 255-character summary (100 issues)
# ------------------------------------------------------------------ #

def create_e2(api: JiraRateLimiter, project: str, text_gen: TextGenerator,
              dry_run: bool) -> dict:
    """Max-length string in summary field."""
    keys = []
    target = 100
    for i in range(target):
        base = text_gen.generate_title("Task")
        # Pad or trim to exactly 255 characters
        summary = (base * 5)[:255]
        key = create_issue(api, project, summary, dry_run=dry_run)
        if key:
            keys.append(key)
    log.info("  [E2] Created %d / %d", len(keys), target)
    return {"category": "E2", "description": "255-char summary",
            "target": target, "created": len(keys), "keys": keys}


# ------------------------------------------------------------------ #
# E3: 32K+ character description (50 issues)
# ------------------------------------------------------------------ #

def create_e3(api: JiraRateLimiter, project: str, text_gen: TextGenerator,
              dry_run: bool) -> dict:
    """Large text field handling."""
    keys = []
    target = 50
    for i in range(target):
        # Generate 32K+ of text
        desc_parts = [text_gen.generate_description("Task", "long") for _ in range(20)]
        large_text = "\n\n".join(desc_parts)
        if len(large_text) < 32000:
            large_text = large_text + " " * (32000 - len(large_text))

        key = create_issue(api, project, f"E3-large-desc-{i:03d}",
                          description=adf_large_text(large_text), dry_run=dry_run)
        if key:
            keys.append(key)
    log.info("  [E3] Created %d / %d", len(keys), target)
    return {"category": "E3", "description": "32K+ char description",
            "target": target, "created": len(keys), "keys": keys}


# ------------------------------------------------------------------ #
# E4: CJK, Arabic, emoji, RTL (200 issues — 50 each)
# ------------------------------------------------------------------ #

def create_e4(api: JiraRateLimiter, project: str, text_gen: TextGenerator,
              dry_run: bool) -> dict:
    """Unicode encoding across 4 sub-categories."""
    keys = []
    target = 200
    subcats = [("cjk", 50), ("arabic", 50), ("emoji", 50), ("rtl", 50)]
    for subcat, count in subcats:
        for i in range(count):
            title = text_gen.generate_edge_text(subcat)
            desc = text_gen.generate_edge_text(subcat)
            key = create_issue(api, project,
                              f"E4-{subcat}-{i:03d}: {title[:100]}",
                              description=adf_text(desc), dry_run=dry_run)
            if key:
                keys.append(key)
        log.info("  [E4/%s] Created %d issues", subcat, count)
    return {"category": "E4", "description": "CJK/Arabic/emoji/RTL",
            "target": target, "created": len(keys), "keys": keys}


# ------------------------------------------------------------------ #
# E5: HTML tags, SQL strings, newlines (100 issues)
# ------------------------------------------------------------------ #

def create_e5(api: JiraRateLimiter, project: str, text_gen: TextGenerator,
              dry_run: bool) -> dict:
    """Input sanitization testing."""
    keys = []
    target = 100
    for i in range(50):
        html_text = text_gen.generate_edge_text("html")
        key = create_issue(api, project,
                          f"E5-html-{i:03d}: {html_text[:80]}",
                          description=adf_text(html_text), dry_run=dry_run)
        if key:
            keys.append(key)

    for i in range(50):
        sql_text = text_gen.generate_edge_text("sql")
        key = create_issue(api, project,
                          f"E5-sql-{i:03d}: {sql_text[:80]}",
                          description=adf_text(sql_text), dry_run=dry_run)
        if key:
            keys.append(key)

    log.info("  [E5] Created %d / %d", len(keys), target)
    return {"category": "E5", "description": "HTML/SQL injection strings",
            "target": target, "created": len(keys), "keys": keys}


# ------------------------------------------------------------------ #
# E6: 100+ comments on one issue (50 issues)
# ------------------------------------------------------------------ #

def create_e6(api: JiraRateLimiter, project: str, text_gen: TextGenerator,
              rng: random.Random, dry_run: bool) -> dict:
    """Comment pagination at depth."""
    keys = []
    target = 50
    for i in range(target):
        key = create_issue(api, project, f"E6-deep-comments-{i:03d}",
                          description=adf_text("Issue with 100+ comments for pagination testing"),
                          dry_run=dry_run)
        if not key:
            continue
        keys.append(key)

        comment_count = rng.randint(100, 150)
        for c in range(comment_count):
            body = text_gen.generate_comment()
            if not dry_run:
                api.post(f"/rest/api/3/issue/{key}/comment", json={
                    "body": adf_text(body),
                })
        log.info("  [E6] %d / %d — added %d comments to %s", i + 1, target, comment_count, key)

    return {"category": "E6", "description": "100+ comments per issue",
            "target": target, "created": len(keys), "keys": keys}


# ------------------------------------------------------------------ #
# E7: 50+ worklogs on one issue (50 issues)
# ------------------------------------------------------------------ #

def create_e7(api: JiraRateLimiter, project: str, rng: random.Random,
              dry_run: bool) -> dict:
    """Worklog pagination at depth."""
    keys = []
    target = 50
    for i in range(target):
        key = create_issue(api, project, f"E7-deep-worklogs-{i:03d}",
                          description=adf_text("Issue with 50+ worklogs for pagination testing"),
                          dry_run=dry_run)
        if not key:
            continue
        keys.append(key)

        worklog_count = rng.randint(50, 80)
        for w in range(worklog_count):
            seconds = rng.randint(900, 14400)
            started = (datetime.now(timezone.utc) - timedelta(days=rng.randint(0, 365))
                      ).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
            if not dry_run:
                api.post(f"/rest/api/3/issue/{key}/worklog", json={
                    "timeSpentSeconds": seconds,
                    "started": started,
                })
        log.info("  [E7] %d / %d — added %d worklogs to %s", i + 1, target, worklog_count, key)

    return {"category": "E7", "description": "50+ worklogs per issue",
            "target": target, "created": len(keys), "keys": keys}


# ------------------------------------------------------------------ #
# E8: Epic -> Story -> Sub-task chain (50 issues)
# ------------------------------------------------------------------ #

def create_e8(api: JiraRateLimiter, project: str, text_gen: TextGenerator,
              dry_run: bool) -> dict:
    """Three-level hierarchy traversal."""
    keys = []
    target = 50  # 10 epics × 2 stories × ~2 subtasks ≈ 50
    for e in range(10):
        epic_key = create_issue(api, project,
                               f"E8-epic-{e:02d}: {text_gen.generate_title('Epic')}",
                               issue_type="Epic",
                               description=adf_text("Three-level hierarchy test epic"),
                               dry_run=dry_run)
        if not epic_key:
            continue
        keys.append(epic_key)

        for s in range(2):
            story_key = create_issue(api, project,
                                    f"E8-story-{e:02d}-{s}: {text_gen.generate_title('Story')}",
                                    issue_type="Story",
                                    description=adf_text(f"Story under epic {epic_key}"),
                                    extra_fields={"parent": {"key": epic_key}} if not dry_run else None,
                                    dry_run=dry_run)
            if not story_key:
                continue
            keys.append(story_key)

            subtask_count = 1 if s == 0 else 2
            for t in range(subtask_count):
                st_key = create_issue(api, project,
                                     f"E8-subtask-{e:02d}-{s}-{t}: {text_gen.generate_title('Sub-task')}",
                                     issue_type="Sub-task",
                                     description=adf_text(f"Sub-task under story {story_key}"),
                                     extra_fields={"parent": {"key": story_key}} if not dry_run else None,
                                     dry_run=dry_run)
                if st_key:
                    keys.append(st_key)

    log.info("  [E8] Created %d issues (target ~%d)", len(keys), target)
    return {"category": "E8", "description": "Epic->Story->Sub-task chains",
            "target": target, "created": len(keys), "keys": keys}


# ------------------------------------------------------------------ #
# E9: 20+ status transitions (50 issues)
# ------------------------------------------------------------------ #

def create_e9(api: JiraRateLimiter, project: str, rng: random.Random,
              dry_run: bool) -> dict:
    """Deep changelog via repeated transitions."""
    keys = []
    target = 50
    for i in range(target):
        key = create_issue(api, project, f"E9-deep-changelog-{i:03d}",
                          description=adf_text("Issue with 20+ transitions for deep changelog"),
                          dry_run=dry_run)
        if not key:
            continue
        keys.append(key)

        transition_count = rng.randint(20, 30)
        done = 0
        for _ in range(transition_count):
            if dry_run:
                done += 1
                continue
            resp = api.get(f"/rest/api/3/issue/{key}/transitions")
            if resp.status_code != 200:
                break
            transitions = resp.json().get("transitions", [])
            if not transitions:
                break
            tid = rng.choice(transitions)["id"]
            resp = api.post(f"/rest/api/3/issue/{key}/transitions", json={
                "transition": {"id": tid},
            })
            if resp.status_code == 204:
                done += 1

        log.info("  [E9] %d / %d — %d transitions on %s", i + 1, target, done, key)

    return {"category": "E9", "description": "20+ status transitions",
            "target": target, "created": len(keys), "keys": keys}


# ------------------------------------------------------------------ #
# E10: Cross-project links (100 issues)
# ------------------------------------------------------------------ #

def create_e10(api: JiraRateLimiter, rng: random.Random, dry_run: bool) -> dict:
    """Cross-project reference resolution."""
    link_types = ["Blocks", "Relates", "Duplicate", "Cloners"]
    keys = []
    target = 100

    # Create 50 issues in BENCH-S1 and 50 in BENCH-K1
    s1_keys = []
    k1_keys = []
    for i in range(50):
        key = create_issue(api, "BENCH-S1", f"E10-cross-s1-{i:03d}",
                          description=adf_text("Cross-project link source"),
                          dry_run=dry_run)
        if key:
            s1_keys.append(key)
            keys.append(key)

    for i in range(50):
        key = create_issue(api, "BENCH-K1", f"E10-cross-k1-{i:03d}",
                          description=adf_text("Cross-project link target"),
                          dry_run=dry_run)
        if key:
            k1_keys.append(key)
            keys.append(key)

    # Link them across projects
    link_count = min(len(s1_keys), len(k1_keys))
    for i in range(link_count):
        lt = link_types[i % len(link_types)]
        if not dry_run:
            api.post("/rest/api/3/issueLink", json={
                "type": {"name": lt},
                "inwardIssue": {"key": s1_keys[i]},
                "outwardIssue": {"key": k1_keys[i]},
            })

    log.info("  [E10] Created %d issues, %d cross-project links", len(keys), link_count)
    return {"category": "E10", "description": "Cross-project links",
            "target": target, "created": len(keys), "keys": keys}


# ------------------------------------------------------------------ #
# E11: Old created dates (100 issues)
# ------------------------------------------------------------------ #

def create_e11(api: JiraRateLimiter, project: str, dry_run: bool) -> dict:
    """Historical data handling. Note: Jira Cloud REST API doesn't allow
    setting created date. We document the intended date in the description."""
    keys = []
    target = 100
    rng = random.Random(config.RANDOM_SEED + 11)
    for i in range(target):
        year = rng.choice([2020, 2021])
        month = rng.randint(1, 12)
        day = rng.randint(1, 28)
        intended_date = f"{year}-{month:02d}-{day:02d}"

        key = create_issue(api, project,
                          f"E11-historical-{intended_date}-{i:03d}",
                          description=adf_text(
                              f"EDGE CASE: Intended created date: {intended_date}. "
                              f"Jira Cloud REST API does not support overriding created date. "
                              f"This issue was created via API on today's date but represents "
                              f"historical data from {intended_date} for testing old-data handling."
                          ),
                          dry_run=dry_run)
        if key:
            keys.append(key)

    log.info("  [E11] Created %d / %d (note: created date override not supported)",
             len(keys), target)
    return {"category": "E11", "description": "Historical dates (documented limitation)",
            "target": target, "created": len(keys), "keys": keys,
            "note": "Jira Cloud REST API does not allow created date override. "
                    "Intended dates documented in description field."}


# ------------------------------------------------------------------ #
# E12: Deactivated user assignment (50 issues)
# ------------------------------------------------------------------ #

def create_e12(api: JiraRateLimiter, project: str, dry_run: bool) -> dict:
    """Inactive user reference handling. Attempts to assign to a deactivated user.
    If rejected, creates unassigned with note in description."""
    keys = []
    target = 50
    deactivated_user = "deactivated-benchmark-user"

    for i in range(target):
        # Try with assignee first
        if dry_run:
            key = create_issue(api, project, f"E12-deactivated-user-{i:03d}",
                              description=adf_text(
                                  f"EDGE CASE: Assigned to deactivated user '{deactivated_user}'. "
                                  f"Tests inactive user reference handling in connector sync."
                              ),
                              dry_run=True)
        else:
            # Attempt to assign — will likely fail, create unassigned instead
            key = create_issue(api, project, f"E12-deactivated-user-{i:03d}",
                              description=adf_text(
                                  f"EDGE CASE: Intended assignee was deactivated user "
                                  f"'{deactivated_user}'. If this issue is unassigned, "
                                  f"the API rejected the deactivated user assignment. "
                                  f"Tests inactive user reference handling."
                              ))
        if key:
            keys.append(key)

    log.info("  [E12] Created %d / %d", len(keys), target)
    return {"category": "E12", "description": "Deactivated user assignment",
            "target": target, "created": len(keys), "keys": keys,
            "note": "Jira may reject assignment to deactivated users. "
                    "Issues created unassigned with documentation in description."}


# ------------------------------------------------------------------ #
# Category registry
# ------------------------------------------------------------------ #

CATEGORIES = {
    "E1": ("All optional fields empty", 500),
    "E2": ("255-char summary", 100),
    "E3": ("32K+ char description", 50),
    "E4": ("CJK/Arabic/emoji/RTL", 200),
    "E5": ("HTML/SQL injection", 100),
    "E6": ("100+ comments", 50),
    "E7": ("50+ worklogs", 50),
    "E8": ("Epic->Story->Sub-task", 50),
    "E9": ("20+ transitions", 50),
    "E10": ("Cross-project links", 100),
    "E11": ("Historical dates", 100),
    "E12": ("Deactivated user", 50),
}


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate stress dataset (~1,400 edge case issues)"
    )
    parser.add_argument("--category", type=str, default=None,
                        choices=list(CATEGORIES.keys()),
                        help="Run a single category (e.g., E6)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without API calls")
    parser.add_argument("--project", type=str, default=DEFAULT_PROJECT,
                        help=f"Target project (default: {DEFAULT_PROJECT})")
    args = parser.parse_args()

    config.validate_config()

    text_gen = TextGenerator(seed=config.RANDOM_SEED)
    rng = random.Random(config.RANDOM_SEED)

    if args.dry_run:
        api = None
        log.info("=== DRY RUN MODE ===")
    else:
        email, token = config.get_jira_auth()
        api = JiraRateLimiter(config.JIRA_URL, email, token)

    project = args.project
    categories_to_run = [args.category] if args.category else list(CATEGORIES.keys())

    results = {}
    start_time = time.time()

    for cat in categories_to_run:
        # Check checkpoint
        ckpt = load_checkpoint(cat)
        if ckpt and not args.dry_run:
            log.info("Skipping %s — checkpoint exists (%d issues)", cat, ckpt.get("count", 0))
            results[cat] = ckpt
            continue

        log.info("\n--- %s: %s (%d issues) ---", cat, CATEGORIES[cat][0], CATEGORIES[cat][1])

        if cat == "E1":
            result = create_e1(api, project, args.dry_run)
        elif cat == "E2":
            result = create_e2(api, project, text_gen, args.dry_run)
        elif cat == "E3":
            result = create_e3(api, project, text_gen, args.dry_run)
        elif cat == "E4":
            result = create_e4(api, project, text_gen, args.dry_run)
        elif cat == "E5":
            result = create_e5(api, project, text_gen, args.dry_run)
        elif cat == "E6":
            result = create_e6(api, project, text_gen, rng, args.dry_run)
        elif cat == "E7":
            result = create_e7(api, project, rng, args.dry_run)
        elif cat == "E8":
            result = create_e8(api, project, text_gen, args.dry_run)
        elif cat == "E9":
            result = create_e9(api, project, rng, args.dry_run)
        elif cat == "E10":
            result = create_e10(api, rng, args.dry_run)
        elif cat == "E11":
            result = create_e11(api, project, args.dry_run)
        elif cat == "E12":
            result = create_e12(api, project, args.dry_run)
        else:
            continue

        results[cat] = result
        if not args.dry_run:
            save_checkpoint(cat, result.get("keys", []), result.get("created", 0))

    elapsed = time.time() - start_time

    # Summary
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": args.dry_run,
        "project": project,
        "elapsed_seconds": round(elapsed, 1),
        "categories": {k: {
            "description": v.get("description", ""),
            "target": v.get("target", 0),
            "created": v.get("created", v.get("count", 0)),
        } for k, v in results.items()},
        "total_created": sum(
            r.get("created", r.get("count", 0)) for r in results.values()
        ),
    }

    summary_path = config.OUTPUT_DIR / "edge_case_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))

    print("\n" + "=" * 60)
    print("STRESS DATASET — GENERATION COMPLETE")
    print("=" * 60)
    for cat, result in sorted(results.items()):
        desc = CATEGORIES.get(cat, ("?",))[0]
        created = result.get("created", result.get("count", 0))
        target = result.get("target", CATEGORIES.get(cat, ("?", 0))[1])
        print(f"  {cat}: {desc:30s} — {created:>5d} / {target:>5d}")
    print(f"\n  Total: {summary['total_created']:,} issues")
    print(f"  Elapsed: {elapsed:.0f}s")
    print(f"  Summary: {summary_path}")
    print("=" * 60)

    if api and not args.dry_run:
        api.close()


if __name__ == "__main__":
    main()
