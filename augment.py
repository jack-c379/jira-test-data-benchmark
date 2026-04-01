"""
Augment existing Jira issues (created via CSV import) with comments, worklogs,
status transitions, issue links, and sprint assignments via the Jira API.

This is the second step after generate_csv.py + Jira bulk import. It enriches
flat issues into richly connected data that exercises all 20 Fivetran objects.

Usage:
    python augment.py                      # Full run (all 5 passes)
    python augment.py --pass comments      # Single pass
    python augment.py --dry-run            # Count what would be done
    python augment.py --workers 5          # Override worker count
"""

import argparse
import json
import logging
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import config
from utils.rate_limiter import JiraRateLimiter
from utils.text_generator import TextGenerator
from utils.distributions import ArchetypeSampler, ARCHETYPES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

LINK_TYPES = ["Blocks", "Cloners", "Duplicate", "Relates"]
PASS_NAMES = ["comments", "worklogs", "transitions", "links", "sprints"]


# ------------------------------------------------------------------ #
# Checkpoint helpers
# ------------------------------------------------------------------ #

def ckpt_path(pass_name: str) -> Path:
    return config.CHECKPOINT_DIR / f"augment_{pass_name}.json"


def load_checkpoint(pass_name: str) -> set[str]:
    """Return set of already-completed issue keys for this pass."""
    p = ckpt_path(pass_name)
    if p.exists():
        data = json.loads(p.read_text())
        return set(data.get("completed_keys", []))
    return set()


def save_checkpoint(pass_name: str, completed: set[str], total_done: int) -> None:
    data = {
        "pass": pass_name,
        "completed_keys": sorted(completed),
        "total_done": total_done,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    ckpt_path(pass_name).write_text(json.dumps(data))


# ------------------------------------------------------------------ #
# Issue discovery
# ------------------------------------------------------------------ #

def discover_issues(api: JiraRateLimiter, project_key: str) -> list[str]:
    """Fetch all issue keys from a project using JQL pagination."""
    keys = []
    start_at = 0
    max_results = 100

    while True:
        resp = api.get("/rest/api/3/search", params={
            "jql": f"project = {project_key} ORDER BY created ASC",
            "startAt": start_at,
            "maxResults": max_results,
            "fields": "key",
        })
        if resp.status_code != 200:
            log.error("JQL search failed (%d): %s", resp.status_code, resp.text[:200])
            break

        data = resp.json()
        issues = data.get("issues", [])
        if not issues:
            break

        keys.extend(iss["key"] for iss in issues)
        start_at += len(issues)

        if start_at >= data.get("total", 0):
            break

    log.info("  [%s] Discovered %d issues", project_key, len(keys))
    return keys


def discover_all_issues(api: JiraRateLimiter) -> dict[str, list[str]]:
    """Discover issues across all benchmark projects."""
    all_keys = {}
    for pkey in config.PROJECTS:
        all_keys[pkey] = discover_issues(api, pkey)
    return all_keys


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


# ------------------------------------------------------------------ #
# Pass implementations
# ------------------------------------------------------------------ #

class AugmentPass:
    """Base for an augmentation pass with progress, checkpointing, error collection."""

    def __init__(self, name: str, api: JiraRateLimiter, workers: int, dry_run: bool):
        self.name = name
        self.api = api
        self.workers = workers
        self.dry_run = dry_run
        self.completed = load_checkpoint(name)
        self.total_done = len(self.completed)
        self.errors: list[dict] = []
        self._start_time = time.time()
        self._ops_since_ckpt = 0

    def _checkpoint_if_needed(self) -> None:
        self._ops_since_ckpt += 1
        if self._ops_since_ckpt >= 1000:
            save_checkpoint(self.name, self.completed, self.total_done)
            self._ops_since_ckpt = 0

    def _log_progress(self, target: int) -> None:
        elapsed = time.time() - self._start_time
        rate = self.total_done / elapsed * 60 if elapsed > 0 else 0
        pct = self.total_done / target * 100 if target > 0 else 100
        log.info("[%s] %d / %d (%.1f%%) — %.0f ops/min",
                 self.name, self.total_done, target, pct, rate)

    def _record_error(self, key: str, status: int, body: str) -> None:
        self.errors.append({
            "pass": self.name,
            "key": key,
            "status": status,
            "body": body[:500],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def finalize(self) -> dict:
        save_checkpoint(self.name, self.completed, self.total_done)
        return {
            "pass": self.name,
            "total_done": self.total_done,
            "errors": len(self.errors),
            "elapsed_seconds": round(time.time() - self._start_time, 1),
        }


def run_comments_pass(
    api: JiraRateLimiter,
    all_keys: dict[str, list[str]],
    target: int,
    workers: int,
    dry_run: bool,
) -> dict:
    """Pass 1: Add comments to issues."""
    ctx = AugmentPass("comments", api, workers, dry_run)
    sampler = ArchetypeSampler(seed=config.RANDOM_SEED + 1)
    text_gen = TextGenerator(seed=config.RANDOM_SEED + 1)

    tasks = []
    for pkey, pdef in config.PROJECTS.items():
        keys = all_keys.get(pkey, [])
        archetype = pdef.archetype
        for key in keys:
            if key in ctx.completed:
                continue
            n = sampler.sample_comment_count(archetype)
            if n > 0:
                tasks.append((key, n, archetype))
            if ctx.total_done + len(tasks) >= target:
                break

    log.info("[comments] %d issues to process, target: %d", len(tasks), target)

    def add_comments_for_issue(args: tuple) -> tuple[str, int]:
        key, count, _ = args
        created = 0
        for _ in range(count):
            body = text_gen.generate_comment()
            if dry_run:
                created += 1
                continue
            resp = api.post(f"/rest/api/3/issue/{key}/comment", json={
                "body": adf_text(body),
            })
            if resp.status_code == 201:
                created += 1
            else:
                ctx._record_error(key, resp.status_code, resp.text)
        return key, created

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(add_comments_for_issue, t): t for t in tasks}
        for future in as_completed(futures):
            key, count = future.result()
            ctx.completed.add(key)
            ctx.total_done += count
            ctx._checkpoint_if_needed()
            if ctx.total_done % 1000 < count + 1:
                ctx._log_progress(target)

    return ctx.finalize()


def run_worklogs_pass(
    api: JiraRateLimiter,
    all_keys: dict[str, list[str]],
    target: int,
    workers: int,
    dry_run: bool,
) -> dict:
    """Pass 2: Add worklogs to issues."""
    ctx = AugmentPass("worklogs", api, workers, dry_run)
    sampler = ArchetypeSampler(seed=config.RANDOM_SEED + 2)
    rng = random.Random(config.RANDOM_SEED + 2)

    tasks = []
    for pkey, pdef in config.PROJECTS.items():
        keys = all_keys.get(pkey, [])
        archetype = pdef.archetype
        for key in keys:
            if key in ctx.completed:
                continue
            n = sampler.sample_worklog_count(archetype)
            if n > 0:
                tasks.append((key, n))
            if ctx.total_done + len(tasks) >= target:
                break

    log.info("[worklogs] %d issues to process, target: %d", len(tasks), target)

    def add_worklogs_for_issue(args: tuple) -> tuple[str, int]:
        key, count = args
        created = 0
        for _ in range(count):
            seconds = rng.randint(900, 28800)
            started_dt = datetime.now(timezone.utc) - timedelta(days=rng.randint(0, 180))
            started = started_dt.strftime("%Y-%m-%dT%H:%M:%S.000+0000")
            if dry_run:
                created += 1
                continue
            resp = api.post(f"/rest/api/3/issue/{key}/worklog", json={
                "timeSpentSeconds": seconds,
                "started": started,
            })
            if resp.status_code == 201:
                created += 1
            else:
                ctx._record_error(key, resp.status_code, resp.text)
        return key, created

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(add_worklogs_for_issue, t): t for t in tasks}
        for future in as_completed(futures):
            key, count = future.result()
            ctx.completed.add(key)
            ctx.total_done += count
            ctx._checkpoint_if_needed()
            if ctx.total_done % 1000 < count + 1:
                ctx._log_progress(target)

    return ctx.finalize()


def run_transitions_pass(
    api: JiraRateLimiter,
    all_keys: dict[str, list[str]],
    target: int,
    workers: int,
    dry_run: bool,
) -> dict:
    """Pass 3: Transition issues through statuses to generate field history."""
    ctx = AugmentPass("transitions", api, workers, dry_run)
    rng = random.Random(config.RANDOM_SEED + 3)

    # Flatten all keys with 2 transitions per issue on average
    tasks = []
    for pkey in config.PROJECTS:
        keys = all_keys.get(pkey, [])
        for key in keys:
            if key in ctx.completed:
                continue
            n = rng.randint(1, 5)  # 1-5 transitions per issue
            tasks.append((key, n))
            if ctx.total_done + len(tasks) * 2 >= target:
                break

    log.info("[transitions] %d issues to process, target: %d", len(tasks), target)

    def transition_issue(args: tuple) -> tuple[str, int]:
        key, count = args
        done = 0
        for _ in range(count):
            if dry_run:
                done += 1
                continue
            # Get available transitions
            resp = api.get(f"/rest/api/3/issue/{key}/transitions")
            if resp.status_code != 200:
                continue
            transitions = resp.json().get("transitions", [])
            if not transitions:
                break
            tid = rng.choice(transitions)["id"]
            resp = api.post(f"/rest/api/3/issue/{key}/transitions", json={
                "transition": {"id": tid},
            })
            if resp.status_code == 204:
                done += 1
            else:
                ctx._record_error(key, resp.status_code, resp.text)
        return key, done

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(transition_issue, t): t for t in tasks}
        for future in as_completed(futures):
            key, count = future.result()
            ctx.completed.add(key)
            ctx.total_done += count
            ctx._checkpoint_if_needed()
            if ctx.total_done % 1000 < count + 1:
                ctx._log_progress(target)

    return ctx.finalize()


def run_links_pass(
    api: JiraRateLimiter,
    all_keys: dict[str, list[str]],
    target: int,
    workers: int,
    dry_run: bool,
) -> dict:
    """Pass 4: Create issue links within and across projects."""
    ctx = AugmentPass("links", api, workers, dry_run)
    rng = random.Random(config.RANDOM_SEED + 4)

    # Build link pairs: 80% intra-project, 20% cross-project
    flat_keys = []
    for pkey in config.PROJECTS:
        flat_keys.extend(all_keys.get(pkey, []))

    per_project_keys = {pkey: keys for pkey, keys in all_keys.items() if keys}

    tasks = []
    remaining = target - ctx.total_done
    for i in range(remaining):
        if rng.random() < 0.80 and per_project_keys:
            # Intra-project link
            pkey = rng.choice(list(per_project_keys.keys()))
            keys = per_project_keys[pkey]
            if len(keys) < 2:
                continue
            a, b = rng.sample(keys, 2)
        else:
            # Cross-project link
            if len(flat_keys) < 2:
                break
            a, b = rng.sample(flat_keys, 2)

        link_type = rng.choice(LINK_TYPES)
        pair_id = f"{a}-{b}"
        if pair_id in ctx.completed:
            continue
        tasks.append((a, b, link_type, pair_id))

    log.info("[links] %d pairs to create, target: %d", len(tasks), target)

    def create_link(args: tuple) -> tuple[str, bool]:
        inward, outward, ltype, pair_id = args
        if dry_run:
            return pair_id, True
        resp = api.post("/rest/api/3/issueLink", json={
            "type": {"name": ltype},
            "inwardIssue": {"key": inward},
            "outwardIssue": {"key": outward},
        })
        if resp.status_code == 201:
            return pair_id, True
        ctx._record_error(pair_id, resp.status_code, resp.text)
        return pair_id, False

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(create_link, t): t for t in tasks}
        for future in as_completed(futures):
            pair_id, success = future.result()
            if success:
                ctx.completed.add(pair_id)
                ctx.total_done += 1
            ctx._checkpoint_if_needed()
            if ctx.total_done % 1000 == 0:
                ctx._log_progress(target)

    return ctx.finalize()


def run_sprints_pass(
    api: JiraRateLimiter,
    all_keys: dict[str, list[str]],
    target: int,
    workers: int,
    dry_run: bool,
) -> dict:
    """Pass 5: Assign issues to sprints (Scrum projects only)."""
    ctx = AugmentPass("sprints", api, workers, dry_run)

    scrum_projects = {
        pkey: pdef for pkey, pdef in config.PROJECTS.items()
        if ARCHETYPES[pdef.archetype].get("board_type") == "scrum"
    }

    if not scrum_projects:
        log.info("[sprints] No Scrum projects found — skipping")
        return ctx.finalize()

    # Discover boards and sprints
    board_sprints: dict[str, list[dict]] = {}
    for pkey in scrum_projects:
        if dry_run:
            board_sprints[pkey] = [{"id": f"dry-{i}", "name": f"Sprint {i}"}
                                   for i in range(1, 11)]
            continue
        resp = api.get(f"/rest/agile/1.0/board?projectKeyOrId={pkey}")
        if resp.status_code != 200:
            continue
        boards = resp.json().get("values", [])
        if not boards:
            continue
        board_id = boards[0]["id"]
        resp = api.get(f"/rest/agile/1.0/board/{board_id}/sprint?maxResults=50")
        if resp.status_code == 200:
            board_sprints[pkey] = resp.json().get("values", [])

    # Build batches: assign up to 50 issues per sprint per API call
    tasks = []
    for pkey, sprints in board_sprints.items():
        if not sprints:
            continue
        keys = [k for k in all_keys.get(pkey, []) if k not in ctx.completed]
        if not keys:
            continue

        issues_per_sprint = max(1, len(keys) // len(sprints))
        for si, sprint in enumerate(sprints):
            start = si * issues_per_sprint
            end = min(start + issues_per_sprint, len(keys))
            batch = keys[start:end]
            if not batch:
                continue
            # Split into chunks of 50
            for chunk_start in range(0, len(batch), 50):
                chunk = batch[chunk_start:chunk_start + 50]
                tasks.append((sprint["id"], chunk, pkey))

    log.info("[sprints] %d batches to process, target: %d", len(tasks), target)

    def assign_batch(args: tuple) -> tuple[list[str], bool]:
        sprint_id, keys_batch, pkey = args
        if dry_run:
            return keys_batch, True
        resp = api.post(f"/rest/agile/1.0/sprint/{sprint_id}/issue", json={
            "issues": keys_batch,
        })
        if resp.status_code in (200, 204):
            return keys_batch, True
        ctx._record_error(f"sprint-{sprint_id}", resp.status_code, resp.text)
        return keys_batch, False

    # Sequential for sprints (batching already gives parallelism)
    for task in tasks:
        keys_batch, success = assign_batch(task)
        if success:
            for k in keys_batch:
                ctx.completed.add(k)
            ctx.total_done += len(keys_batch)
        ctx._checkpoint_if_needed()
        if ctx.total_done % 1000 < len(keys_batch) + 1:
            ctx._log_progress(target)

    return ctx.finalize()


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Augment Jira issues with comments, worklogs, transitions, links, sprints"
    )
    parser.add_argument("--pass", dest="pass_name", type=str, default=None,
                        choices=PASS_NAMES,
                        help="Run a single pass only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count what would be done without API calls")
    parser.add_argument("--workers", type=int, default=None,
                        help=f"Override worker count (default: {config.AUGMENT_WORKERS})")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Auto-resume from checkpoints (default: True)")
    args = parser.parse_args()

    config.validate_config()

    workers = args.workers or config.AUGMENT_WORKERS
    dry_run = args.dry_run

    if dry_run:
        log.info("=== DRY RUN MODE ===")
        api = None
        # Create a mock API for dry-run discovery
        all_keys = {pkey: [f"{pkey}-{i}" for i in range(1, 101)]
                    for pkey in config.PROJECTS}
    else:
        email, token = config.get_jira_auth()
        api = JiraRateLimiter(config.JIRA_URL, email, token)
        log.info("Discovering issues across all projects...")
        all_keys = discover_all_issues(api)

    total_issues = sum(len(v) for v in all_keys.values())
    log.info("Total issues discovered: %d", total_issues)

    if total_issues == 0 and not dry_run:
        log.error("No issues found. Run generate_csv.py + Jira bulk import first.")
        sys.exit(1)

    # Define passes and targets
    targets = {
        "comments": config.scaled(config.AUGMENT.comments),
        "worklogs": config.scaled(config.AUGMENT.worklogs),
        "transitions": config.scaled(config.AUGMENT.transitions),
        "links": config.scaled(config.AUGMENT.issue_links),
        "sprints": config.scaled(config.AUGMENT.sprint_assignments),
    }

    pass_runners = {
        "comments": lambda: run_comments_pass(api, all_keys, targets["comments"], workers, dry_run),
        "worklogs": lambda: run_worklogs_pass(api, all_keys, targets["worklogs"], workers, dry_run),
        "transitions": lambda: run_transitions_pass(api, all_keys, targets["transitions"], workers, dry_run),
        "links": lambda: run_links_pass(api, all_keys, targets["links"], workers, dry_run),
        "sprints": lambda: run_sprints_pass(api, all_keys, targets["sprints"], workers, dry_run),
    }

    passes_to_run = [args.pass_name] if args.pass_name else PASS_NAMES
    results = {}

    for pass_name in passes_to_run:
        log.info("\n" + "=" * 50)
        log.info("PASS: %s (target: %d)", pass_name, targets[pass_name])
        log.info("=" * 50)
        results[pass_name] = pass_runners[pass_name]()

    # Collect all errors
    all_errors = []
    for pass_name in passes_to_run:
        ckpt = ckpt_path(pass_name)
        # Errors are in the AugmentPass objects, but we have results
        pass  # Errors were logged inline

    # Save summary
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "workers": workers,
        "scale_factor": config.SCALE_FACTOR,
        "targets": targets,
        "results": results,
    }

    summary_path = config.OUTPUT_DIR / "augment_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))

    # Print final summary
    print("\n" + "=" * 60)
    print("AUGMENTATION — COMPLETE")
    print("=" * 60)
    for name, result in results.items():
        target = targets[name]
        done = result["total_done"]
        errors = result["errors"]
        elapsed = result["elapsed_seconds"]
        print(f"  {name:15s}: {done:>10,} / {target:>10,}  "
              f"({errors} errors, {elapsed:.0f}s)")
    print(f"\n  Summary: {summary_path}")
    print("=" * 60)

    if api and not dry_run:
        stats = api.stats()
        print(f"  API requests: {stats['requests_made']:,}")
        print(f"  API retries:  {stats['retries_total']:,}")
        api.close()


if __name__ == "__main__":
    main()
