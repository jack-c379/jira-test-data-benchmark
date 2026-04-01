"""
Scale dataset CSV generator — produces CSV files for Jira's bulk import feature
to create 100K issues (at scale factor 1.0).

CSV import is server-side and far faster than API calls for mass issue creation.
After importing, run augment.py to enrich with comments, worklogs, transitions, etc.

Usage:
    python generate_csv.py                         # Full run (100K issues)
    python generate_csv.py --scale-factor 0.01     # 1% scale (1K issues)
    python generate_csv.py --project BENCH-S1      # Single project only
"""

import argparse
import csv
import json
import logging
import math
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import config
from utils.text_generator import TextGenerator
from utils.distributions import ArchetypeSampler, ARCHETYPES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
# Constants
# ------------------------------------------------------------------ #

LABELS_POOL = [
    "backend", "frontend", "urgent", "tech-debt", "customer-reported",
    "regression", "performance", "security", "documentation", "api",
    "mobile", "infra", "onboarding", "analytics", "migration",
    "testing", "ux", "devops", "compliance", "feature-request",
]

COMPONENTS_PER_PROJECT = {
    "BENCH-S1": ["API", "Frontend", "Backend", "Database", "Auth"],
    "BENCH-S2": ["API", "Backend", "Notifications", "Reports", "Admin"],
    "BENCH-K1": ["Backend", "Database", "Integration", "Search", "Auth"],
    "BENCH-K2": ["Frontend", "API", "Reports", "Notifications", "Admin"],
    "BENCH-CL": ["Backend", "Integration", "Database", "Admin", "Search"],
}

VERSIONS_PER_PROJECT = {
    "BENCH-S1": ["v1.0", "v1.1", "v2.0", "v2.1"],
    "BENCH-S2": ["v1.0", "v1.1", "v2.0"],
    "BENCH-K1": ["v1.0", "v1.1", "v2.0", "v2.1"],
    "BENCH-K2": ["v1.0", "v1.1", "v2.0"],
    "BENCH-CL": ["v1.0", "v1.1", "v2.0", "v2.1"],
}

USERS = [f"user{i:02d}" for i in range(1, 11)]

# Date range for created dates: 2024-01-01 to 2026-01-01
DATE_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DATE_END = datetime(2026, 1, 1, tzinfo=timezone.utc)
DATE_RANGE_DAYS = (DATE_END - DATE_START).days


# ------------------------------------------------------------------ #
# Date helpers
# ------------------------------------------------------------------ #

def jira_date(dt: datetime) -> str:
    """Format datetime as Jira CSV import format: dd/MMM/yy HH:mm."""
    return dt.strftime("%d/%b/%y %H:%M")


def jira_date_only(dt: datetime) -> str:
    """Format datetime as date-only for due dates: dd/MMM/yy."""
    return dt.strftime("%d/%b/%y")


def sample_created_date(rng: random.Random) -> datetime:
    """Sample a created date with exponential bias toward recent dates."""
    # Exponential distribution: more recent dates are more likely
    u = rng.random()
    # Transform uniform [0,1) to exponentially biased toward 1
    biased = 1.0 - math.exp(-3 * u) / math.exp(-3)  # Bias factor of 3
    day_offset = int(biased * DATE_RANGE_DAYS)
    dt = DATE_START + timedelta(days=day_offset, hours=rng.randint(0, 23),
                                minutes=rng.randint(0, 59))
    return dt


def sample_updated_date(rng: random.Random, created: datetime) -> datetime:
    """Updated = created + random 0-90 days, capped at now."""
    offset = rng.randint(0, 90)
    updated = created + timedelta(days=offset, hours=rng.randint(0, 12))
    now = datetime.now(timezone.utc)
    return min(updated, now)


# ------------------------------------------------------------------ #
# Description length sampling
# ------------------------------------------------------------------ #

def sample_desc_length(rng: random.Random) -> str:
    """70% medium, 20% short, 10% long."""
    r = rng.random()
    if r < 0.70:
        return "medium"
    elif r < 0.90:
        return "short"
    else:
        return "long"


# ------------------------------------------------------------------ #
# CSV generation for one project
# ------------------------------------------------------------------ #

def generate_project_csv(
    project_key: str,
    project_def: config.ProjectDef,
    scale_factor: float,
    sampler: ArchetypeSampler,
    text_gen: TextGenerator,
    rng: random.Random,
) -> dict:
    """Generate a CSV file for one project. Returns stats dict."""
    archetype = project_def.archetype
    issue_count = max(1, int(project_def.base_issues * scale_factor))
    components = COMPONENTS_PER_PROJECT.get(project_key, ["API", "Backend"])
    versions = VERSIONS_PER_PROJECT.get(project_key, ["v1.0"])
    arch_cfg = ARCHETYPES[archetype]
    is_scrum = arch_cfg.get("board_type") == "scrum"
    sprint_count = arch_cfg.get("sprints", 0)

    csv_path = config.OUTPUT_DIR / f"scale_{project_key}.csv"

    headers = [
        "Summary", "Issue Type", "Priority", "Description", "Assignee",
        "Reporter", "Due Date", "Labels", "Component", "Fix Version",
        "Status", "Created", "Updated",
    ]
    if is_scrum:
        headers.extend(["Story Points", "Sprint", "Epic Link"])
    else:
        headers.append("Epic Link")

    # First pass: generate epics, then stories/tasks/bugs, then sub-tasks
    # We need epic summaries for Epic Link references
    issues: list[dict] = []
    epic_summaries: list[str] = []
    story_summaries: list[str] = []

    # Pre-sample all issue types so we know counts
    type_sequence = [sampler.sample_issue_type(archetype) for _ in range(issue_count)]

    # Ensure we have at least some epics for linking
    epic_count = sum(1 for t in type_sequence if t == "Epic")
    if epic_count == 0 and issue_count > 10:
        type_sequence[0] = "Epic"

    for i in range(issue_count):
        issue_type = type_sequence[i]
        summary = text_gen.generate_title(issue_type)
        desc_len = sample_desc_length(rng)
        description = text_gen.generate_description(issue_type, desc_len)
        priority = sampler.sample_priority(archetype)
        status = sampler.sample_status(archetype)
        assignee = rng.choice(USERS)
        reporter = rng.choice(USERS)
        created = sample_created_date(rng)
        updated = sample_updated_date(rng, created)

        # Labels: 0-3 random labels
        label_count = rng.randint(0, 3)
        labels = ";".join(rng.sample(LABELS_POOL, min(label_count, len(LABELS_POOL))))

        # Component: one random component
        component = rng.choice(components)

        # Fix version: one random version (70% chance)
        fix_version = rng.choice(versions) if rng.random() < 0.70 else ""

        # Due date
        due_dt = sampler.sample_due_date(archetype, created)
        due_date = jira_date_only(due_dt) if due_dt else ""

        row = {
            "Summary": summary,
            "Issue Type": issue_type,
            "Priority": priority,
            "Description": description,
            "Assignee": assignee,
            "Reporter": reporter,
            "Due Date": due_date,
            "Labels": labels,
            "Component": component,
            "Fix Version": fix_version,
            "Status": status,
            "Created": jira_date(created),
            "Updated": jira_date(updated),
        }

        # Epic Link: stories/tasks/bugs link to an epic (if available)
        epic_link = ""
        if issue_type == "Epic":
            epic_summaries.append(summary)
        elif issue_type in ("Story", "Task", "Bug") and epic_summaries:
            if rng.random() < 0.60:  # 60% chance of having an epic link
                epic_link = rng.choice(epic_summaries)
        elif issue_type == "Sub-task" and story_summaries:
            pass  # Sub-tasks use Parent, not Epic Link

        row["Epic Link"] = epic_link

        # Story/task summaries for sub-task parenting
        if issue_type in ("Story", "Task"):
            story_summaries.append(summary)

        # Scrum-specific fields
        if is_scrum:
            points = sampler.sample_story_points(archetype)
            row["Story Points"] = str(points) if points else ""
            if sprint_count > 0 and issue_type != "Epic":
                sprint_num = rng.randint(1, sprint_count)
                row["Sprint"] = f"Sprint {sprint_num}"
            else:
                row["Sprint"] = ""

        issues.append(row)

        if (i + 1) % 10000 == 0:
            log.info("  [%s] Generated %d / %d rows", project_key, i + 1, issue_count)

    # Write CSV
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(issues)

    log.info("  [%s] Wrote %d rows to %s", project_key, len(issues), csv_path)

    # Stats
    type_counts = {}
    priority_counts = {}
    for iss in issues:
        t = iss["Issue Type"]
        type_counts[t] = type_counts.get(t, 0) + 1
        p = iss["Priority"]
        priority_counts[p] = priority_counts.get(p, 0) + 1

    return {
        "project": project_key,
        "archetype": archetype,
        "total_issues": len(issues),
        "csv_path": str(csv_path),
        "issue_types": type_counts,
        "priorities": priority_counts,
    }


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate scale dataset CSVs for Jira bulk import"
    )
    parser.add_argument("--scale-factor", type=float, default=None,
                        help="Override scale factor (default: from config)")
    parser.add_argument("--project", type=str, default=None,
                        help="Generate for a single project only")
    args = parser.parse_args()

    config.validate_config()

    scale = args.scale_factor if args.scale_factor is not None else config.SCALE_FACTOR
    log.info("Scale factor: %.2f", scale)

    sampler = ArchetypeSampler(seed=config.RANDOM_SEED)
    text_gen = TextGenerator(seed=config.RANDOM_SEED)
    rng = random.Random(config.RANDOM_SEED)

    projects = config.PROJECTS
    if args.project:
        if args.project not in projects:
            log.error("Unknown project: %s", args.project)
            return
        projects = {args.project: projects[args.project]}

    start_time = time.time()
    results = {}

    for pkey, pdef in projects.items():
        log.info("=== Generating CSV for %s (%s) ===", pkey, pdef.archetype)
        stats = generate_project_csv(pkey, pdef, scale, sampler, text_gen, rng)
        results[pkey] = stats

    elapsed = time.time() - start_time

    # Generate manifest
    total_issues = sum(r["total_issues"] for r in results.values())
    manifest = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "random_seed": config.RANDOM_SEED,
        "scale_factor": scale,
        "total_issues": total_issues,
        "elapsed_seconds": round(elapsed, 1),
        "projects": results,
    }

    manifest_path = config.OUTPUT_DIR / "scale_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    # Print summary
    print("\n" + "=" * 60)
    print("SCALE DATASET — CSV GENERATION COMPLETE")
    print("=" * 60)
    for pkey, stats in results.items():
        print(f"  {pkey:12s}: {stats['total_issues']:>10,} issues → {stats['csv_path']}")
    print(f"  {'TOTAL':12s}: {total_issues:>10,} issues")
    print(f"  Elapsed: {elapsed:.1f}s")
    print(f"  Manifest: {manifest_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
