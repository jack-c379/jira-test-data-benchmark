"""
Coverage dataset generator — 5,000 crafted issues touching every Jira field,
object, and relationship at least once.

This is NOT random bulk data; it systematically ensures all 20 Fivetran-synced
Jira objects are populated so the connector has something to sync for each one.

Usage:
    python generate_contract.py              # Full run
    python generate_contract.py --dry-run    # Print plan without API calls
    python generate_contract.py --project BENCH-S1  # Single project only
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import config
from utils.rate_limiter import JiraRateLimiter
from utils.text_generator import TextGenerator
from utils.distributions import ArchetypeSampler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
# Constants
# ------------------------------------------------------------------ #

ISSUES_PER_PROJECT = config.COVERAGE_ISSUES // len(config.PROJECTS)  # ~1,000
COMPONENTS = ["API", "Frontend", "Backend", "Database", "Auth",
              "Notifications", "Reports", "Admin", "Integration", "Search"]
VERSIONS = ["v1.0", "v1.1", "v2.0", "v2.1"]
LINK_TYPES = ["Blocks", "Cloners", "Duplicate", "Relates"]
USERS = [f"user{i:02d}" for i in range(1, 11)]

# Per-project enrichment targets (minimum)
MIN_COMMENTS = 50
MIN_WORKLOGS = 30
MIN_LINKS = 20
MIN_WATCHERS = 10
MIN_REMOTE_LINKS = 10
MIN_PROPERTIES = 10
MIN_TRANSITIONS = 40
MIN_MULTISELECT = 5


# ------------------------------------------------------------------ #
# Checkpoint helpers
# ------------------------------------------------------------------ #

def checkpoint_path(project_key: str) -> Path:
    return config.CHECKPOINT_DIR / f"contract_{project_key}.json"


def load_checkpoint(project_key: str) -> dict | None:
    p = checkpoint_path(project_key)
    if p.exists():
        return json.loads(p.read_text())
    return None


def save_checkpoint(project_key: str, data: dict) -> None:
    checkpoint_path(project_key).write_text(json.dumps(data, indent=2))


# ------------------------------------------------------------------ #
# ADF (Atlassian Document Format) helpers
# ------------------------------------------------------------------ #

def adf_text(text: str) -> dict:
    """Wrap plain text in ADF format for Jira Cloud v3 API."""
    return {
        "type": "doc",
        "version": 1,
        "content": [
            {
                "type": "paragraph",
                "content": [{"type": "text", "text": text}],
            }
        ],
    }


# ------------------------------------------------------------------ #
# Project setup: components, versions
# ------------------------------------------------------------------ #

def setup_project(api: JiraRateLimiter, project_key: str, dry_run: bool) -> dict:
    """Create components and versions for a project. Returns IDs."""
    info = {"components": [], "versions": []}

    for name in COMPONENTS[:5]:  # 5 components per project
        if dry_run:
            log.info("[dry-run] Would create component '%s' in %s", name, project_key)
            info["components"].append({"id": "dry", "name": name})
            continue
        resp = api.post("/rest/api/3/component", json={
            "name": name,
            "project": project_key,
            "description": f"Component {name} for benchmark testing",
        })
        if resp.status_code == 201:
            data = resp.json()
            info["components"].append({"id": data["id"], "name": name})
            log.info("Created component %s in %s", name, project_key)
        elif resp.status_code == 409:
            # Already exists — fetch it
            existing = api.get(f"/rest/api/3/project/{project_key}/components").json()
            for c in existing:
                if c["name"] == name:
                    info["components"].append({"id": c["id"], "name": name})
                    break
        else:
            log.warning("Component create failed (%d): %s", resp.status_code, resp.text[:200])

    for vname in VERSIONS:
        if dry_run:
            log.info("[dry-run] Would create version '%s' in %s", vname, project_key)
            info["versions"].append({"id": "dry", "name": vname})
            continue
        resp = api.post("/rest/api/3/version", json={
            "name": vname,
            "project": project_key,
            "description": f"Version {vname} for benchmark",
        })
        if resp.status_code == 201:
            data = resp.json()
            info["versions"].append({"id": data["id"], "name": vname})
        elif resp.status_code == 409:
            existing = api.get(f"/rest/api/3/project/{project_key}/versions").json()
            for v in existing:
                if v["name"] == vname:
                    info["versions"].append({"id": v["id"], "name": vname})
                    break
        else:
            log.warning("Version create failed (%d): %s", resp.status_code, resp.text[:200])

    return info


# ------------------------------------------------------------------ #
# Sprints (Scrum projects only)
# ------------------------------------------------------------------ #

def get_board_id(api: JiraRateLimiter, project_key: str) -> int | None:
    """Find the board ID for a project."""
    resp = api.get(f"/rest/agile/1.0/board?projectKeyOrId={project_key}")
    if resp.status_code == 200:
        boards = resp.json().get("values", [])
        if boards:
            return boards[0]["id"]
    return None


def setup_sprints(api: JiraRateLimiter, board_id: int, count: int, dry_run: bool) -> list[dict]:
    """Create sprints on a board. Returns list of sprint info dicts."""
    sprints = []
    for i in range(1, count + 1):
        name = f"Sprint {i}"
        if dry_run:
            sprints.append({"id": "dry", "name": name})
            continue
        resp = api.post(f"/rest/agile/1.0/sprint", json={
            "name": name,
            "originBoardId": board_id,
        })
        if resp.status_code == 201:
            data = resp.json()
            sprints.append({"id": data["id"], "name": name})
        elif resp.status_code == 409:
            # Already exists — list existing sprints
            existing = api.get(f"/rest/agile/1.0/board/{board_id}/sprint?maxResults=50").json()
            for s in existing.get("values", []):
                if s["name"] == name:
                    sprints.append({"id": s["id"], "name": name})
                    break
        else:
            log.warning("Sprint create failed (%d): %s", resp.status_code, resp.text[:200])
    return sprints


# ------------------------------------------------------------------ #
# Issue creation
# ------------------------------------------------------------------ #

def create_issue(
    api: JiraRateLimiter,
    project_key: str,
    sampler: ArchetypeSampler,
    archetype: str,
    text_gen: TextGenerator,
    project_info: dict,
    issue_index: int,
    parent_key: str | None = None,
    dry_run: bool = False,
) -> dict | None:
    """Create a single issue with fields sampled from the archetype."""
    issue_type = sampler.sample_issue_type(archetype)

    # Sub-tasks need a parent
    if issue_type == "Sub-task" and not parent_key:
        issue_type = "Task"  # Demote if no parent available

    title = text_gen.generate_title(issue_type)
    desc = text_gen.generate_description(issue_type, "medium")
    priority = sampler.sample_priority(archetype)

    payload = {
        "fields": {
            "project": {"key": project_key},
            "summary": title,
            "issuetype": {"name": issue_type},
            "priority": {"name": priority},
            "description": adf_text(desc),
        }
    }

    # Assignee (rotate through users)
    payload["fields"]["reporter"] = {"id": None}  # Default reporter
    # Labels
    labels = ["benchmark", f"batch-{issue_index // 100}"]
    payload["fields"]["labels"] = labels

    # Component (if available)
    if project_info.get("components"):
        comp = project_info["components"][issue_index % len(project_info["components"])]
        payload["fields"]["components"] = [{"id": comp["id"]}]

    # Fix version (if available)
    if project_info.get("versions"):
        ver = project_info["versions"][issue_index % len(project_info["versions"])]
        payload["fields"]["fixVersions"] = [{"id": ver["id"]}]

    # Story points for Scrum
    points = sampler.sample_story_points(archetype)
    if points is not None:
        payload["fields"]["story_points"] = points

    # Due date
    created = datetime.now(timezone.utc)
    due = sampler.sample_due_date(archetype, created)
    if due:
        payload["fields"]["duedate"] = due.strftime("%Y-%m-%d")

    # Parent for sub-tasks
    if issue_type == "Sub-task" and parent_key:
        payload["fields"]["parent"] = {"key": parent_key}

    if dry_run:
        return {"key": f"{project_key}-DRY-{issue_index}", "type": issue_type, "dry": True}

    resp = api.post("/rest/api/3/issue", json=payload)
    if resp.status_code == 201:
        data = resp.json()
        return {"key": data["key"], "id": data["id"], "type": issue_type}
    else:
        log.warning("Issue create failed (%d): %s", resp.status_code, resp.text[:300])
        return None


# ------------------------------------------------------------------ #
# Enrichment: comments, worklogs, links, watchers, etc.
# ------------------------------------------------------------------ #

def add_comments(api: JiraRateLimiter, issue_key: str, count: int,
                 text_gen: TextGenerator, dry_run: bool) -> int:
    """Add comments to an issue."""
    created = 0
    for _ in range(count):
        body = text_gen.generate_comment()
        if dry_run:
            created += 1
            continue
        resp = api.post(f"/rest/api/3/issue/{issue_key}/comment", json={
            "body": adf_text(body),
        })
        if resp.status_code == 201:
            created += 1
    return created


def add_worklogs(api: JiraRateLimiter, issue_key: str, count: int, dry_run: bool) -> int:
    """Add worklogs to an issue."""
    created = 0
    import random as _rand
    rng = _rand.Random(hash(issue_key))
    for _ in range(count):
        seconds = rng.randint(900, 28800)  # 15 min to 8 hours
        started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
        if dry_run:
            created += 1
            continue
        resp = api.post(f"/rest/api/3/issue/{issue_key}/worklog", json={
            "timeSpentSeconds": seconds,
            "started": started,
        })
        if resp.status_code == 201:
            created += 1
    return created


def add_issue_link(api: JiraRateLimiter, inward_key: str, outward_key: str,
                   link_type: str, dry_run: bool) -> bool:
    """Create a link between two issues."""
    if dry_run:
        return True
    resp = api.post("/rest/api/3/issueLink", json={
        "type": {"name": link_type},
        "inwardIssue": {"key": inward_key},
        "outwardIssue": {"key": outward_key},
    })
    return resp.status_code == 201


def add_watcher(api: JiraRateLimiter, issue_key: str, account_id: str,
                dry_run: bool) -> bool:
    """Add a watcher to an issue."""
    if dry_run:
        return True
    resp = api.post(f"/rest/api/3/issue/{issue_key}/watchers",
                    json=f'"{account_id}"')
    return resp.status_code == 204


def add_remote_link(api: JiraRateLimiter, issue_key: str, index: int,
                    dry_run: bool) -> bool:
    """Add a remote link to an issue."""
    if dry_run:
        return True
    resp = api.post(f"/rest/api/3/issue/{issue_key}/remotelink", json={
        "object": {
            "url": f"https://example.com/docs/ref-{index}",
            "title": f"External Reference #{index}",
            "summary": f"Benchmark remote link for testing object sync #{index}",
        },
    })
    return resp.status_code in (200, 201)


def set_issue_property(api: JiraRateLimiter, issue_key: str, prop_key: str,
                       value: dict, dry_run: bool) -> bool:
    """Set a property on an issue."""
    if dry_run:
        return True
    resp = api.put(f"/rest/api/3/issue/{issue_key}/properties/{prop_key}",
                   json=value)
    return resp.status_code in (200, 201, 204)


def transition_issue(api: JiraRateLimiter, issue_key: str, dry_run: bool) -> int:
    """Transition an issue through available transitions. Returns count."""
    if dry_run:
        return 1
    resp = api.get(f"/rest/api/3/issue/{issue_key}/transitions")
    if resp.status_code != 200:
        return 0
    transitions = resp.json().get("transitions", [])
    if not transitions:
        return 0
    # Pick the first available transition
    tid = transitions[0]["id"]
    resp = api.post(f"/rest/api/3/issue/{issue_key}/transitions", json={
        "transition": {"id": tid},
    })
    return 1 if resp.status_code == 204 else 0


def assign_to_sprint(api: JiraRateLimiter, sprint_id: int | str,
                     issue_keys: list[str], dry_run: bool) -> bool:
    """Assign issues to a sprint."""
    if dry_run:
        return True
    resp = api.post(f"/rest/agile/1.0/sprint/{sprint_id}/issue", json={
        "issues": issue_keys,
    })
    return resp.status_code in (200, 204)


# ------------------------------------------------------------------ #
# Main generation loop for one project
# ------------------------------------------------------------------ #

def generate_for_project(
    api: JiraRateLimiter,
    project_def: config.ProjectDef,
    sampler: ArchetypeSampler,
    text_gen: TextGenerator,
    dry_run: bool,
) -> dict:
    """Generate coverage data for a single project. Returns summary dict."""
    pkey = project_def.key
    archetype = project_def.archetype
    arch_cfg = sampler.get_archetype_config(archetype)
    is_scrum = arch_cfg.get("board_type") == "scrum"

    log.info("=== %s (%s) — %d issues ===", pkey, archetype, ISSUES_PER_PROJECT)

    # Setup components + versions
    project_info = setup_project(api, pkey, dry_run)
    log.info("  Components: %d, Versions: %d",
             len(project_info["components"]), len(project_info["versions"]))

    # Setup sprints for Scrum projects
    sprints = []
    if is_scrum:
        board_id = get_board_id(api, pkey) if not dry_run else None
        if board_id or dry_run:
            sprints = setup_sprints(
                api, board_id or 0, arch_cfg.get("sprints", 0), dry_run
            )
            log.info("  Sprints: %d", len(sprints))

    # Create issues
    created_issues: list[dict] = []
    epic_keys: list[str] = []
    story_keys: list[str] = []

    for i in range(ISSUES_PER_PROJECT):
        parent = None
        # For sub-tasks, use a story or epic as parent
        issue_type_hint = sampler.sample_issue_type(archetype)
        if issue_type_hint == "Sub-task" and story_keys:
            parent = story_keys[i % len(story_keys)]

        result = create_issue(
            api, pkey, sampler, archetype, text_gen,
            project_info, i, parent_key=parent, dry_run=dry_run,
        )
        if result:
            created_issues.append(result)
            if result["type"] == "Epic":
                epic_keys.append(result["key"])
            elif result["type"] == "Story":
                story_keys.append(result["key"])

        if (i + 1) % 100 == 0:
            log.info("  [%s] Created %d / %d issues", pkey, i + 1, ISSUES_PER_PROJECT)

    # Enrichment counters
    counts = {
        "issues": len(created_issues),
        "comments": 0,
        "worklogs": 0,
        "links": 0,
        "watchers": 0,
        "remote_links": 0,
        "properties": 0,
        "transitions": 0,
        "sprint_assignments": 0,
    }

    if not created_issues:
        return counts

    keys = [iss["key"] for iss in created_issues]

    # --- Comments ---
    for i in range(min(MIN_COMMENTS, len(keys))):
        n = sampler.sample_comment_count(archetype)
        n = max(n, 1)  # At least 1 for coverage
        counts["comments"] += add_comments(api, keys[i], n, text_gen, dry_run)
    log.info("  [%s] Comments: %d", pkey, counts["comments"])

    # --- Worklogs ---
    for i in range(min(MIN_WORKLOGS, len(keys))):
        n = sampler.sample_worklog_count(archetype)
        n = max(n, 1)
        counts["worklogs"] += add_worklogs(api, keys[i], n, dry_run)
    log.info("  [%s] Worklogs: %d", pkey, counts["worklogs"])

    # --- Issue links ---
    link_count = min(MIN_LINKS, len(keys) // 2)
    for i in range(link_count):
        lt = LINK_TYPES[i % len(LINK_TYPES)]
        if add_issue_link(api, keys[i * 2], keys[i * 2 + 1], lt, dry_run):
            counts["links"] += 1
    log.info("  [%s] Issue links: %d", pkey, counts["links"])

    # --- Watchers ---
    for i in range(min(MIN_WATCHERS, len(keys))):
        # Use a placeholder account ID — in real runs, discover real account IDs
        if add_watcher(api, keys[i], f"placeholder-{USERS[i % len(USERS)]}", dry_run):
            counts["watchers"] += 1

    # --- Remote links ---
    for i in range(min(MIN_REMOTE_LINKS, len(keys))):
        if add_remote_link(api, keys[i], i, dry_run):
            counts["remote_links"] += 1
    log.info("  [%s] Remote links: %d", pkey, counts["remote_links"])

    # --- Issue properties ---
    for i in range(min(MIN_PROPERTIES, len(keys))):
        if set_issue_property(api, keys[i], "benchmark.metadata",
                              {"batch": i, "generated": True, "suite": "coverage"}, dry_run):
            counts["properties"] += 1

    # --- Transitions (generates field history) ---
    for i in range(min(MIN_TRANSITIONS, len(keys))):
        counts["transitions"] += transition_issue(api, keys[i], dry_run)
    log.info("  [%s] Transitions: %d", pkey, counts["transitions"])

    # --- Sprint assignments (Scrum only) ---
    if sprints and is_scrum:
        issues_per_sprint = max(1, len(keys) // len(sprints))
        for si, sprint in enumerate(sprints):
            start = si * issues_per_sprint
            end = start + issues_per_sprint
            batch = keys[start:end]
            if batch:
                if assign_to_sprint(api, sprint["id"], batch, dry_run):
                    counts["sprint_assignments"] += len(batch)
        log.info("  [%s] Sprint assignments: %d", pkey, counts["sprint_assignments"])

    return counts


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate coverage dataset (5K issues) for Jira benchmark"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without making API calls")
    parser.add_argument("--project", type=str, default=None,
                        help="Run for a single project key (e.g., BENCH-S1)")
    args = parser.parse_args()

    config.validate_config()

    sampler = ArchetypeSampler(seed=config.RANDOM_SEED)
    text_gen = TextGenerator(seed=config.RANDOM_SEED)

    if args.dry_run:
        api = None
        log.info("=== DRY RUN MODE ===")
    else:
        email, token = config.get_jira_auth()
        api = JiraRateLimiter(config.JIRA_URL, email, token)

    projects = config.PROJECTS
    if args.project:
        if args.project not in projects:
            log.error("Unknown project: %s. Available: %s",
                      args.project, ", ".join(projects))
            sys.exit(1)
        projects = {args.project: projects[args.project]}

    overall = {}
    start_time = time.time()

    for pkey, pdef in projects.items():
        # Check for existing checkpoint
        ckpt = load_checkpoint(pkey)
        if ckpt and not args.dry_run:
            log.info("Skipping %s — checkpoint exists (%d issues already created)",
                     pkey, ckpt.get("issues", 0))
            overall[pkey] = ckpt
            continue

        counts = generate_for_project(api, pdef, sampler, text_gen, args.dry_run)
        overall[pkey] = counts

        if not args.dry_run:
            save_checkpoint(pkey, counts)
            log.info("Checkpoint saved for %s", pkey)

    elapsed = time.time() - start_time

    # Summary
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "seed": config.RANDOM_SEED,
        "dry_run": args.dry_run,
        "elapsed_seconds": round(elapsed, 1),
        "per_project": overall,
        "totals": {},
    }

    # Aggregate totals
    all_keys = set()
    for proj_counts in overall.values():
        for k, v in proj_counts.items():
            if isinstance(v, (int, float)):
                all_keys.add(k)
    for k in sorted(all_keys):
        summary["totals"][k] = sum(
            proj.get(k, 0) for proj in overall.values()
        )

    summary_path = config.OUTPUT_DIR / "contract_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    log.info("Summary saved to %s", summary_path)

    # Print final summary
    print("\n" + "=" * 60)
    print("COVERAGE DATASET — GENERATION COMPLETE")
    print("=" * 60)
    for k, v in summary["totals"].items():
        print(f"  {k:25s}: {v:>10,}")
    print(f"  {'elapsed':25s}: {elapsed:.0f}s")
    print("=" * 60)

    if api and not args.dry_run:
        stats = api.stats()
        print(f"  API requests: {stats['requests_made']:,}")
        print(f"  API retries:  {stats['retries_total']:,}")
        api.close()


if __name__ == "__main__":
    main()
