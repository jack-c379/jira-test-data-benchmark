"""
Central configuration for the Jira Test Data Benchmark suite.

Loads credentials from .env, defines project archetypes, volume targets,
augmentation parameters, and directory paths. Every other script imports
from here — this is the single source of truth.
"""

import base64
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Tuple

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parent
load_dotenv(ROOT_DIR / ".env")

JIRA_URL = os.getenv("JIRA_URL", "")
JIRA_EMAIL = os.getenv("JIRA_EMAIL", "")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN", "")

# ---------------------------------------------------------------------------
# Project archetypes
# ---------------------------------------------------------------------------
# Each project maps a Jira project key to an archetype that controls
# issue-type distributions, workflow shapes, and field usage patterns.
# The base_issues count is the target at SCALE_FACTOR = 1.0 (100K total).

ARCHETYPES = ("SCRUM-1", "SCRUM-2", "KANBAN-1", "KANBAN-2", "CLASSIC")


@dataclass(frozen=True)
class ProjectDef:
    """One benchmark project inside the Jira instance."""

    key: str
    archetype: str
    base_issues: int  # issue count at scale factor 1.0


PROJECTS: Dict[str, ProjectDef] = {
    "BENCH-S1": ProjectDef(key="BENCH-S1", archetype="SCRUM-1", base_issues=30_000),
    "BENCH-S2": ProjectDef(key="BENCH-S2", archetype="SCRUM-2", base_issues=20_000),
    "BENCH-K1": ProjectDef(key="BENCH-K1", archetype="KANBAN-1", base_issues=20_000),
    "BENCH-K2": ProjectDef(key="BENCH-K2", archetype="KANBAN-2", base_issues=15_000),
    "BENCH-CL": ProjectDef(key="BENCH-CL", archetype="CLASSIC", base_issues=15_000),
}

# Sanity check — the base counts must add up to 100K at factor 1.0.
_TOTAL_BASE = sum(p.base_issues for p in PROJECTS.values())
assert _TOTAL_BASE == 100_000, f"Base issues sum to {_TOTAL_BASE}, expected 100000"

# ---------------------------------------------------------------------------
# Volume controls
# ---------------------------------------------------------------------------

SCALE_FACTOR: float = 1.0  # multiply all counts; 1.0 = 100K issues total


def scaled(n: int) -> int:
    """Return *n* adjusted by the global scale factor, rounded to int."""
    return max(1, int(n * SCALE_FACTOR))


def total_issues() -> int:
    """Total issue count across all projects at the current scale."""
    return sum(scaled(p.base_issues) for p in PROJECTS.values())


# ---------------------------------------------------------------------------
# Dataset sizes
# ---------------------------------------------------------------------------

COVERAGE_ISSUES: int = 5_000   # generate_contract.py — crafted field coverage
STRESS_ISSUES: int = 1_400     # edge_cases.py — extreme patterns

# ---------------------------------------------------------------------------
# Augmentation targets (at scale factor 1.0)
# ---------------------------------------------------------------------------
# augment.py uses these to decide how many child objects to create.
# Actual counts are multiplied by SCALE_FACTOR at runtime via scaled().


@dataclass(frozen=True)
class AugmentTargets:
    """Volume targets for augmentation objects."""

    comments: int = 300_000       # ~3 per issue average
    worklogs: int = 80_000        # ~0.8 per issue average
    transitions: int = 200_000    # ~2 per issue average
    issue_links: int = 5_000
    sprint_assignments: int = 50_000


AUGMENT = AugmentTargets()

# ---------------------------------------------------------------------------
# Parallelism & reproducibility
# ---------------------------------------------------------------------------

AUGMENT_WORKERS: int = 10   # concurrent threads/processes for augment.py
RANDOM_SEED: int = 42       # every RNG in the suite seeds from this

# ---------------------------------------------------------------------------
# Directories (created lazily by validate_config)
# ---------------------------------------------------------------------------

CHECKPOINT_DIR: Path = ROOT_DIR / "checkpoints"
OUTPUT_DIR: Path = ROOT_DIR / "output"
MANIFEST_DIR: Path = ROOT_DIR / "manifests"

# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def get_jira_auth() -> Tuple[str, str]:
    """Return ``(email, api_token)`` for Jira basic auth."""
    if not JIRA_EMAIL or not JIRA_API_TOKEN:
        raise RuntimeError(
            "JIRA_EMAIL and JIRA_API_TOKEN must be set in .env "
            "(see .env.example)"
        )
    return JIRA_EMAIL, JIRA_API_TOKEN


def get_jira_headers() -> Dict[str, str]:
    """Return HTTP headers dict with Basic auth + JSON content type."""
    email, token = get_jira_auth()
    creds = base64.b64encode(f"{email}:{token}".encode()).decode()
    return {
        "Authorization": f"Basic {creds}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_config() -> None:
    """Check that env vars are set and working directories exist.

    Creates checkpoint, output, and manifest directories if missing.
    Raises ``SystemExit`` on fatal problems (missing credentials or URL).
    """
    errors: list[str] = []

    if not JIRA_URL:
        errors.append("JIRA_URL is not set in .env")
    if not JIRA_EMAIL:
        errors.append("JIRA_EMAIL is not set in .env")
    if not JIRA_API_TOKEN:
        errors.append("JIRA_API_TOKEN is not set in .env")

    if errors:
        for e in errors:
            print(f"[config] ERROR: {e}", file=sys.stderr)
        print(
            "[config] Copy .env.example to .env and fill in your values.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Ensure directories exist (idempotent).
    for d in (CHECKPOINT_DIR, OUTPUT_DIR, MANIFEST_DIR):
        d.mkdir(parents=True, exist_ok=True)

    print(f"[config] Jira URL   : {JIRA_URL}")
    print(f"[config] Scale      : {SCALE_FACTOR}x  ({total_issues():,} issues)")
    print(f"[config] Projects   : {', '.join(PROJECTS)}")
    print(f"[config] Checkpoints: {CHECKPOINT_DIR}")
    print(f"[config] Output     : {OUTPUT_DIR}")
    print(f"[config] Manifests  : {MANIFEST_DIR}")


# ---------------------------------------------------------------------------
# CLI quick-check
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    validate_config()
    print("\nConfig OK.")
