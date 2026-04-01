"""
Archetype-based sampling for Jira test data generation.

Defines 5 project archetypes (SCRUM-1, SCRUM-2, KANBAN-1, KANBAN-2, CLASSIC) with
realistic distributions for issue types, priorities, statuses, story points, due dates,
comments, and worklogs. All sampling is deterministic via a seed parameter.

Usage:
    sampler = ArchetypeSampler(seed=42)
    issue_type = sampler.sample_issue_type("SCRUM-1")
    priority   = sampler.sample_priority("SCRUM-1")
    status     = sampler.sample_status("KANBAN-1")
    points     = sampler.sample_story_points("SCRUM-1")
    due        = sampler.sample_due_date("SCRUM-1", created=datetime.now())
    n_comments = sampler.sample_comment_count("SCRUM-1")
    n_worklogs = sampler.sample_worklog_count("SCRUM-1")

    # Scale down for testing
    sampler.scale_volumes(0.01)  # 1% of full volume
"""

import math
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional


# --------------------------------------------------------------------------- #
# Archetype definitions
# --------------------------------------------------------------------------- #

ARCHETYPES: Dict[str, dict] = {
    "SCRUM-1": {
        "description": "Large mature Scrum project -- high volume, broad type mix, active sprints",
        "issues": 30000,
        "board_type": "scrum",
        "issue_types": {
            "Epic": 0.05,
            "Story": 0.40,
            "Task": 0.20,
            "Bug": 0.25,
            "Sub-task": 0.10,
        },
        "priorities": {
            "Highest": 0.05,
            "High": 0.20,
            "Medium": 0.45,
            "Low": 0.25,
            "Lowest": 0.05,
        },
        "statuses": {
            "To Do": 0.30,
            "In Progress": 0.25,
            "Done": 0.35,
            "Closed": 0.10,
        },
        "due_date_pct": 0.60,
        "story_points": True,
        "sprints": 10,
        "avg_comments": 3,
        "avg_worklogs": 1,
    },
    "SCRUM-2": {
        "description": "Mid-size Scrum project -- fewer issues, more bugs (maintenance-heavy)",
        "issues": 20000,
        "board_type": "scrum",
        "issue_types": {
            "Epic": 0.03,
            "Story": 0.30,
            "Task": 0.15,
            "Bug": 0.40,
            "Sub-task": 0.12,
        },
        "priorities": {
            "Highest": 0.10,
            "High": 0.25,
            "Medium": 0.40,
            "Low": 0.20,
            "Lowest": 0.05,
        },
        "statuses": {
            "To Do": 0.25,
            "In Progress": 0.30,
            "Done": 0.30,
            "Closed": 0.15,
        },
        "due_date_pct": 0.70,
        "story_points": True,
        "sprints": 8,
        "avg_comments": 4,
        "avg_worklogs": 2,
    },
    "KANBAN-1": {
        "description": "Large Kanban project -- continuous flow, ops/support-oriented",
        "issues": 20000,
        "board_type": "kanban",
        "issue_types": {
            "Epic": 0.02,
            "Story": 0.20,
            "Task": 0.35,
            "Bug": 0.35,
            "Sub-task": 0.08,
        },
        "priorities": {
            "Highest": 0.08,
            "High": 0.22,
            "Medium": 0.40,
            "Low": 0.22,
            "Lowest": 0.08,
        },
        "statuses": {
            "Backlog": 0.20,
            "Selected for Development": 0.15,
            "In Progress": 0.30,
            "Done": 0.35,
        },
        "due_date_pct": 0.45,
        "story_points": False,
        "sprints": 0,
        "avg_comments": 2,
        "avg_worklogs": 1,
    },
    "KANBAN-2": {
        "description": "Smaller Kanban project -- product-focused, lighter volume",
        "issues": 15000,
        "board_type": "kanban",
        "issue_types": {
            "Epic": 0.04,
            "Story": 0.35,
            "Task": 0.30,
            "Bug": 0.20,
            "Sub-task": 0.11,
        },
        "priorities": {
            "Highest": 0.03,
            "High": 0.15,
            "Medium": 0.50,
            "Low": 0.27,
            "Lowest": 0.05,
        },
        "statuses": {
            "Backlog": 0.25,
            "Selected for Development": 0.10,
            "In Progress": 0.25,
            "Done": 0.40,
        },
        "due_date_pct": 0.50,
        "story_points": False,
        "sprints": 0,
        "avg_comments": 2,
        "avg_worklogs": 0.5,
    },
    "CLASSIC": {
        "description": "Classic project -- traditional workflow, waterfall-ish, task-heavy",
        "issues": 15000,
        "board_type": "classic",
        "issue_types": {
            "Epic": 0.03,
            "Story": 0.15,
            "Task": 0.45,
            "Bug": 0.30,
            "Sub-task": 0.07,
        },
        "priorities": {
            "Highest": 0.05,
            "High": 0.20,
            "Medium": 0.45,
            "Low": 0.20,
            "Lowest": 0.10,
        },
        "statuses": {
            "Open": 0.25,
            "In Progress": 0.25,
            "Resolved": 0.30,
            "Closed": 0.20,
        },
        "due_date_pct": 0.80,
        "story_points": False,
        "sprints": 0,
        "avg_comments": 3,
        "avg_worklogs": 2,
    },
}


# --------------------------------------------------------------------------- #
# Story points: Fibonacci with descending probability
# --------------------------------------------------------------------------- #

# Values and their relative weights (higher weight = more likely)
_FIBONACCI_POINTS = [1, 2, 3, 5, 8, 13]
_FIBONACCI_WEIGHTS = [0.30, 0.25, 0.20, 0.15, 0.07, 0.03]


# --------------------------------------------------------------------------- #
# ArchetypeSampler class
# --------------------------------------------------------------------------- #


class ArchetypeSampler:
    """Deterministic sampler for Jira issue attributes based on project archetypes.

    Args:
        seed: Integer seed for reproducible sampling. Same seed = same sequence.
    """

    def __init__(self, seed: int = 42):
        self._rng = random.Random(seed)
        # Work on a copy so scale_volumes doesn't mutate the module-level dict
        self._archetypes: Dict[str, dict] = {
            k: dict(v) for k, v in ARCHETYPES.items()
        }

    def _get_archetype(self, archetype: str) -> dict:
        """Retrieve archetype config or raise ValueError."""
        cfg = self._archetypes.get(archetype)
        if cfg is None:
            valid = ", ".join(sorted(self._archetypes.keys()))
            raise ValueError(
                f"Unknown archetype '{archetype}'. Valid: {valid}"
            )
        return cfg

    def _weighted_choice(self, distribution: Dict[str, float]) -> str:
        """Pick a key from a {value: probability} dict using the internal RNG."""
        keys = list(distribution.keys())
        weights = [distribution[k] for k in keys]
        # random.choices returns a list; we want one item
        return self._rng.choices(keys, weights=weights, k=1)[0]

    # ------------------------------------------------------------------ #
    # Sampling methods
    # ------------------------------------------------------------------ #

    def sample_issue_type(self, archetype: str) -> str:
        """Sample an issue type according to the archetype's distribution.

        Returns:
            One of: Epic, Story, Task, Bug, Sub-task.
        """
        cfg = self._get_archetype(archetype)
        return self._weighted_choice(cfg["issue_types"])

    def sample_priority(self, archetype: str) -> str:
        """Sample a priority level.

        Returns:
            One of: Highest, High, Medium, Low, Lowest.
        """
        cfg = self._get_archetype(archetype)
        return self._weighted_choice(cfg["priorities"])

    def sample_status(self, archetype: str) -> str:
        """Sample a workflow status appropriate to the board type.

        Returns:
            Scrum:   To Do, In Progress, Done, Closed
            Kanban:  Backlog, Selected for Development, In Progress, Done
            Classic: Open, In Progress, Resolved, Closed
        """
        cfg = self._get_archetype(archetype)
        return self._weighted_choice(cfg["statuses"])

    def sample_story_points(self, archetype: str) -> Optional[int]:
        """Sample story points (Fibonacci scale) for Scrum projects.

        Returns:
            An integer from [1, 2, 3, 5, 8, 13] for Scrum archetypes.
            None for non-Scrum archetypes.
        """
        cfg = self._get_archetype(archetype)
        if not cfg.get("story_points", False):
            return None
        return self._rng.choices(
            _FIBONACCI_POINTS, weights=_FIBONACCI_WEIGHTS, k=1
        )[0]

    def sample_due_date(
        self, archetype: str, created: datetime
    ) -> Optional[datetime]:
        """Sample a due date relative to the created date.

        The archetype's `due_date_pct` controls how often a due date is assigned.
        When assigned, the due date is 1-90 days after creation.

        Args:
            archetype: Archetype key.
            created: The issue's created datetime.

        Returns:
            A datetime 1-90 days after `created`, or None if the random draw
            falls outside the due_date_pct.
        """
        cfg = self._get_archetype(archetype)
        if self._rng.random() > cfg["due_date_pct"]:
            return None
        days_ahead = self._rng.randint(1, 90)
        return created + timedelta(days=days_ahead)

    def sample_comment_count(self, archetype: str) -> int:
        """Sample the number of comments using Poisson distribution.

        Returns:
            A non-negative integer drawn from Poisson(avg_comments).
        """
        cfg = self._get_archetype(archetype)
        lam = cfg["avg_comments"]
        if lam <= 0:
            return 0
        return self._poisson(lam)

    def sample_worklog_count(self, archetype: str) -> int:
        """Sample the number of worklogs using Poisson distribution.

        Returns:
            A non-negative integer drawn from Poisson(avg_worklogs).
        """
        cfg = self._get_archetype(archetype)
        lam = cfg["avg_worklogs"]
        if lam <= 0:
            return 0
        return self._poisson(lam)

    # ------------------------------------------------------------------ #
    # Volume scaling
    # ------------------------------------------------------------------ #

    def scale_volumes(self, factor: float) -> None:
        """Multiply all archetype issue counts by a factor.

        Useful for running tests at reduced scale (e.g., factor=0.01 for 1%).
        Counts are rounded to the nearest integer, minimum 1.

        Args:
            factor: Multiplier (e.g., 0.1 for 10%, 2.0 for double).
        """
        if factor <= 0:
            raise ValueError(f"Scale factor must be positive, got {factor}")
        for key in self._archetypes:
            original = ARCHETYPES[key]["issues"]
            self._archetypes[key]["issues"] = max(1, round(original * factor))

    def get_issue_count(self, archetype: str) -> int:
        """Return the (possibly scaled) issue count for an archetype."""
        cfg = self._get_archetype(archetype)
        return cfg["issues"]

    def get_archetype_config(self, archetype: str) -> dict:
        """Return a copy of the full archetype configuration."""
        cfg = self._get_archetype(archetype)
        return dict(cfg)

    def list_archetypes(self) -> List[str]:
        """Return all archetype keys."""
        return sorted(self._archetypes.keys())

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _poisson(self, lam: float) -> int:
        """Sample from Poisson distribution using Knuth's algorithm.

        Uses the internal RNG for reproducibility instead of numpy.

        Args:
            lam: Lambda (mean) of the Poisson distribution.

        Returns:
            A non-negative integer.
        """
        # Knuth's algorithm -- fine for small lambda values (< 30)
        if lam > 30:
            # For large lambda, use normal approximation
            value = round(self._rng.gauss(lam, math.sqrt(lam)))
            return max(0, value)

        L = math.exp(-lam)
        k = 0
        p = 1.0
        while True:
            k += 1
            p *= self._rng.random()
            if p <= L:
                return k - 1
