"""
Quick smoke test — verify all 20 Jira objects are populated after data generation.

Run this after generate_contract.py / generate_csv.py + bulk import / augment.py
to confirm the test data is in place before running the full eval suite.

Usage:
    python verify.py                  # Full check
    python verify.py --verbose        # Show API response details
    python verify.py --project BENCH-S1  # Single project
"""

import argparse
import json
import logging
import random
import sys
from datetime import datetime, timezone

import config
from utils.rate_limiter import JiraRateLimiter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Result types
# ------------------------------------------------------------------ #

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"


class CheckResult:
    def __init__(self, num: int, name: str, status: str, detail: str):
        self.num = num
        self.name = name
        self.status = status
        self.detail = detail

    def __str__(self):
        icon = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}[self.status]
        return f" [{self.status}] {icon} {self.num:2d}. {self.name:28s} — {self.detail}"


# ------------------------------------------------------------------ #
# Helper: get sample issue keys
# ------------------------------------------------------------------ #

def get_sample_keys(api: JiraRateLimiter, projects: list[str],
                    count: int = 10) -> list[str]:
    """Fetch a random sample of issue keys across projects."""
    all_keys = []
    for pkey in projects:
        resp = api.get("/rest/api/3/search", params={
            "jql": f"project = {pkey}",
            "maxResults": 50,
            "fields": "key",
        })
        if resp.status_code == 200:
            all_keys.extend(iss["key"] for iss in resp.json().get("issues", []))

    if len(all_keys) > count:
        rng = random.Random(42)
        all_keys = rng.sample(all_keys, count)
    return all_keys


# ------------------------------------------------------------------ #
# Individual checks
# ------------------------------------------------------------------ #

def check_01_issues(api: JiraRateLimiter, projects: list[str],
                    verbose: bool) -> CheckResult:
    """All 20 objects: Issues exist."""
    total = 0
    per_project = {}
    for pkey in projects:
        resp = api.get("/rest/api/3/search", params={
            "jql": f"project = {pkey}",
            "maxResults": 0,
        })
        if resp.status_code == 200:
            count = resp.json().get("total", 0)
            per_project[pkey] = count
            total += count
    if verbose:
        log.info("  Issues per project: %s", per_project)
    if total > 0:
        return CheckResult(1, "Issues", PASS, f"{total:,} total across {len(projects)} projects")
    return CheckResult(1, "Issues", FAIL, "No issues found")


def check_02_field_history(api: JiraRateLimiter, sample_keys: list[str],
                           verbose: bool) -> CheckResult:
    """Issue Field History — changelog entries exist."""
    for key in sample_keys[:10]:
        resp = api.get(f"/rest/api/3/issue/{key}", params={
            "expand": "changelog",
            "fields": "key",
        })
        if resp.status_code == 200:
            changelog = resp.json().get("changelog", {})
            histories = changelog.get("histories", [])
            if histories:
                return CheckResult(2, "Field History", PASS,
                                   f"changelog found on {key} ({len(histories)} entries)")
    return CheckResult(2, "Field History", FAIL, "No changelog entries found on any sampled issue")


def check_03_multiselect_history(api: JiraRateLimiter, sample_keys: list[str],
                                  verbose: bool) -> CheckResult:
    """Issue Multiselect History — multi-select field changes."""
    for key in sample_keys[:10]:
        resp = api.get(f"/rest/api/3/issue/{key}", params={
            "expand": "changelog",
            "fields": "key",
        })
        if resp.status_code == 200:
            changelog = resp.json().get("changelog", {})
            for history in changelog.get("histories", []):
                for item in history.get("items", []):
                    if item.get("fieldtype") == "custom" or "multi" in item.get("field", "").lower():
                        return CheckResult(3, "Multiselect History", PASS,
                                           f"multiselect change found on {key}")
    return CheckResult(3, "Multiselect History", WARN,
                       "No multiselect changes detected (may need custom fields)")


def check_04_comments(api: JiraRateLimiter, sample_keys: list[str],
                      verbose: bool) -> CheckResult:
    """Comments exist on at least one issue."""
    total = 0
    for key in sample_keys[:10]:
        resp = api.get(f"/rest/api/3/issue/{key}/comment", params={"maxResults": 1})
        if resp.status_code == 200:
            count = resp.json().get("total", 0)
            total += count
            if count > 0:
                if verbose:
                    log.info("  %s has %d comments", key, count)
    if total > 0:
        return CheckResult(4, "Comments", PASS, f"found on sampled issues")
    return CheckResult(4, "Comments", FAIL, "No comments found on any sampled issue")


def check_05_worklogs(api: JiraRateLimiter, sample_keys: list[str],
                      verbose: bool) -> CheckResult:
    """Worklogs exist."""
    for key in sample_keys[:10]:
        resp = api.get(f"/rest/api/3/issue/{key}/worklog", params={"maxResults": 1})
        if resp.status_code == 200:
            total = resp.json().get("total", 0)
            if total > 0:
                return CheckResult(5, "Worklogs", PASS, f"found on {key} ({total} worklogs)")
    return CheckResult(5, "Worklogs", FAIL, "No worklogs found on any sampled issue")


def check_06_issue_links(api: JiraRateLimiter, sample_keys: list[str],
                         verbose: bool) -> CheckResult:
    """Issue links exist."""
    for key in sample_keys[:10]:
        resp = api.get(f"/rest/api/3/issue/{key}", params={"fields": "issuelinks"})
        if resp.status_code == 200:
            links = resp.json().get("fields", {}).get("issuelinks", [])
            if links:
                return CheckResult(6, "Issue Links", PASS,
                                   f"found on {key} ({len(links)} links)")
    return CheckResult(6, "Issue Links", FAIL, "No issue links found")


def check_07_watchers(api: JiraRateLimiter, sample_keys: list[str],
                      verbose: bool) -> CheckResult:
    """Watchers or votes exist."""
    for key in sample_keys[:10]:
        resp = api.get(f"/rest/api/3/issue/{key}/watchers")
        if resp.status_code == 200:
            watch_count = resp.json().get("watchCount", 0)
            if watch_count > 0:
                return CheckResult(7, "Watchers / Votes", PASS,
                                   f"watchCount={watch_count} on {key}")
    return CheckResult(7, "Watchers / Votes", WARN,
                       "No watchers found (may need manual watcher setup)")


def check_08_properties(api: JiraRateLimiter, sample_keys: list[str],
                        verbose: bool) -> CheckResult:
    """Issue properties exist."""
    for key in sample_keys[:10]:
        resp = api.get(f"/rest/api/3/issue/{key}/properties")
        if resp.status_code == 200:
            props = resp.json().get("keys", [])
            if props:
                return CheckResult(8, "Issue Properties", PASS,
                                   f"found on {key} ({len(props)} properties)")
    return CheckResult(8, "Issue Properties", WARN,
                       "No issue properties found (generate_contract.py sets these)")


def check_09_remote_links(api: JiraRateLimiter, sample_keys: list[str],
                          verbose: bool) -> CheckResult:
    """Remote links exist."""
    for key in sample_keys[:10]:
        resp = api.get(f"/rest/api/3/issue/{key}/remotelink")
        if resp.status_code == 200:
            links = resp.json()
            if isinstance(links, list) and links:
                return CheckResult(9, "Remote Links", PASS,
                                   f"found on {key} ({len(links)} links)")
    return CheckResult(9, "Remote Links", WARN,
                       "No remote links found (generate_contract.py sets these)")


def check_10_projects(api: JiraRateLimiter, projects: list[str],
                      verbose: bool) -> CheckResult:
    """Benchmark projects exist."""
    found = []
    for pkey in projects:
        resp = api.get(f"/rest/api/3/project/{pkey}")
        if resp.status_code == 200:
            found.append(pkey)
    if len(found) == len(projects):
        return CheckResult(10, "Projects", PASS, f"all {len(found)} projects exist")
    return CheckResult(10, "Projects", FAIL,
                       f"only {len(found)}/{len(projects)} projects found: {found}")


def check_11_components_versions(api: JiraRateLimiter, projects: list[str],
                                  verbose: bool) -> CheckResult:
    """Components and versions exist."""
    comp_count = 0
    ver_count = 0
    for pkey in projects:
        resp = api.get(f"/rest/api/3/project/{pkey}/components")
        if resp.status_code == 200:
            comp_count += len(resp.json())
        resp = api.get(f"/rest/api/3/project/{pkey}/versions")
        if resp.status_code == 200:
            ver_count += len(resp.json())
    if comp_count > 0 and ver_count > 0:
        return CheckResult(11, "Components / Versions", PASS,
                           f"{comp_count} components, {ver_count} versions")
    return CheckResult(11, "Components / Versions", FAIL,
                       f"components: {comp_count}, versions: {ver_count}")


def check_12_boards(api: JiraRateLimiter, projects: list[str],
                    verbose: bool) -> CheckResult:
    """Boards exist for Scrum/Kanban projects."""
    found = 0
    for pkey in projects:
        resp = api.get(f"/rest/agile/1.0/board", params={"projectKeyOrId": pkey})
        if resp.status_code == 200:
            boards = resp.json().get("values", [])
            found += len(boards)
    if found > 0:
        return CheckResult(12, "Boards", PASS, f"{found} boards found")
    return CheckResult(12, "Boards", WARN, "No boards found (create Scrum/Kanban boards manually)")


def check_13_sprints(api: JiraRateLimiter, projects: list[str],
                     verbose: bool) -> CheckResult:
    """Sprints exist in Scrum projects."""
    for pkey in projects:
        resp = api.get(f"/rest/agile/1.0/board", params={"projectKeyOrId": pkey})
        if resp.status_code != 200:
            continue
        boards = resp.json().get("values", [])
        for board in boards:
            resp2 = api.get(f"/rest/agile/1.0/board/{board['id']}/sprint",
                           params={"maxResults": 1})
            if resp2.status_code == 200:
                sprints = resp2.json().get("values", [])
                if sprints:
                    return CheckResult(13, "Sprints", PASS,
                                       f"found in board {board['id']} ({board.get('name', '')})")
    return CheckResult(13, "Sprints", WARN, "No sprints found (Scrum projects need sprints)")


def check_14_users(api: JiraRateLimiter, verbose: bool) -> CheckResult:
    """Users exist."""
    resp = api.get("/rest/api/3/users/search", params={"maxResults": 1})
    if resp.status_code == 200:
        users = resp.json()
        if users:
            return CheckResult(14, "Users", PASS, f"at least {len(users)} user(s)")
    return CheckResult(14, "Users", FAIL, "No users found")


def check_15_groups(api: JiraRateLimiter, verbose: bool) -> CheckResult:
    """Groups exist."""
    resp = api.get("/rest/api/3/group/bulk", params={"maxResults": 1})
    if resp.status_code == 200:
        groups = resp.json().get("values", [])
        if groups:
            return CheckResult(15, "Groups", PASS, f"at least {len(groups)} group(s)")
    return CheckResult(15, "Groups", WARN, "Groups endpoint returned empty or failed")


def check_16_teams(api: JiraRateLimiter, verbose: bool) -> CheckResult:
    """Atlassian Teams — may not be available via standard REST API."""
    # Teams are often managed via Atlassian Admin, not Jira REST
    resp = api.get("/rest/api/3/group/bulk", params={"maxResults": 5})
    if resp.status_code == 200:
        return CheckResult(16, "Atlassian Teams", WARN,
                           "Teams not directly exposed via Jira REST API — using groups as proxy")
    return CheckResult(16, "Atlassian Teams", WARN,
                       "Teams endpoint not available (Atlassian Admin manages these)")


def check_17_fields(api: JiraRateLimiter, verbose: bool) -> CheckResult:
    """Fields and field options exist."""
    resp = api.get("/rest/api/3/field")
    if resp.status_code == 200:
        fields = resp.json()
        custom = [f for f in fields if f.get("custom", False)]
        return CheckResult(17, "Fields & Field Options", PASS,
                           f"{len(fields)} fields ({len(custom)} custom)")
    return CheckResult(17, "Fields & Field Options", FAIL, "Fields endpoint failed")


def check_18_types_statuses(api: JiraRateLimiter, verbose: bool) -> CheckResult:
    """Issue types, statuses, priorities exist."""
    types_ok = False
    statuses_ok = False
    priorities_ok = False

    resp = api.get("/rest/api/3/issuetype")
    if resp.status_code == 200 and resp.json():
        types_ok = True
    resp = api.get("/rest/api/3/status")
    if resp.status_code == 200 and resp.json():
        statuses_ok = True
    resp = api.get("/rest/api/3/priority")
    if resp.status_code == 200 and resp.json():
        priorities_ok = True

    if types_ok and statuses_ok and priorities_ok:
        return CheckResult(18, "Types / Statuses / Priorities", PASS, "all present")
    parts = []
    if not types_ok: parts.append("types")
    if not statuses_ok: parts.append("statuses")
    if not priorities_ok: parts.append("priorities")
    return CheckResult(18, "Types / Statuses / Priorities", FAIL,
                       f"missing: {', '.join(parts)}")


def check_19_roles(api: JiraRateLimiter, projects: list[str],
                   verbose: bool) -> CheckResult:
    """Project roles exist."""
    for pkey in projects:
        resp = api.get(f"/rest/api/3/project/{pkey}/role")
        if resp.status_code == 200 and resp.json():
            return CheckResult(19, "Project Roles", PASS, f"roles found in {pkey}")
    return CheckResult(19, "Project Roles", FAIL, "No project roles found")


def check_20_security(api: JiraRateLimiter, projects: list[str],
                      verbose: bool) -> CheckResult:
    """Security schemes — may not be configured."""
    resp = api.get("/rest/api/3/issuesecurityschemes")
    if resp.status_code == 200:
        schemes = resp.json().get("issueSecuritySchemes", [])
        if schemes:
            return CheckResult(20, "Security Schemes", PASS,
                               f"{len(schemes)} scheme(s) found")
    return CheckResult(20, "Security Schemes", WARN,
                       "No security scheme configured (optional for benchmark)")


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Smoke test — verify all 20 Jira objects are populated"
    )
    parser.add_argument("--verbose", action="store_true",
                        help="Show detailed API responses")
    parser.add_argument("--project", type=str, default=None,
                        help="Check a single project only")
    args = parser.parse_args()

    config.validate_config()

    email, token = config.get_jira_auth()
    api = JiraRateLimiter(config.JIRA_URL, email, token)

    projects = list(config.PROJECTS.keys())
    if args.project:
        if args.project not in config.PROJECTS:
            log.error("Unknown project: %s", args.project)
            sys.exit(1)
        projects = [args.project]

    print("\nJira Test Data Benchmark — Smoke Test")
    print("=" * 50)

    # Get sample issue keys for checks that need them
    sample_keys = get_sample_keys(api, projects, count=20)
    if not sample_keys:
        print(" [FAIL] Cannot proceed — no issues found in any project")
        sys.exit(1)

    # Run all 20 checks
    results = [
        check_01_issues(api, projects, args.verbose),
        check_02_field_history(api, sample_keys, args.verbose),
        check_03_multiselect_history(api, sample_keys, args.verbose),
        check_04_comments(api, sample_keys, args.verbose),
        check_05_worklogs(api, sample_keys, args.verbose),
        check_06_issue_links(api, sample_keys, args.verbose),
        check_07_watchers(api, sample_keys, args.verbose),
        check_08_properties(api, sample_keys, args.verbose),
        check_09_remote_links(api, sample_keys, args.verbose),
        check_10_projects(api, projects, args.verbose),
        check_11_components_versions(api, projects, args.verbose),
        check_12_boards(api, projects, args.verbose),
        check_13_sprints(api, projects, args.verbose),
        check_14_users(api, args.verbose),
        check_15_groups(api, args.verbose),
        check_16_teams(api, args.verbose),
        check_17_fields(api, args.verbose),
        check_18_types_statuses(api, args.verbose),
        check_19_roles(api, projects, args.verbose),
        check_20_security(api, projects, args.verbose),
    ]

    print()
    for r in results:
        print(r)

    # Summary
    pass_count = sum(1 for r in results if r.status == PASS)
    warn_count = sum(1 for r in results if r.status == WARN)
    fail_count = sum(1 for r in results if r.status == FAIL)

    print(f"\nResults: {pass_count} PASS, {warn_count} WARN, {fail_count} FAIL")

    # Save results
    results_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "projects": projects,
        "checks": [
            {"num": r.num, "name": r.name, "status": r.status, "detail": r.detail}
            for r in results
        ],
        "summary": {"pass": pass_count, "warn": warn_count, "fail": fail_count},
    }

    results_path = config.OUTPUT_DIR / "verify_results.json"
    results_path.write_text(json.dumps(results_data, indent=2))
    print(f"Results saved to {results_path}")

    api.close()

    sys.exit(1 if fail_count > 0 else 0)


if __name__ == "__main__":
    main()
