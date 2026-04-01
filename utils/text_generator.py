"""
Generates realistic Jira ticket text from seed templates.

Deterministic via a seed parameter -- same seed always produces the same sequence
of titles, descriptions, and comments. Used by generate_contract.py, generate_csv.py,
and edge_cases.py.

Usage:
    gen = TextGenerator(seed=42)
    title = gen.generate_title("Bug")
    desc  = gen.generate_description("Story", length="medium")
    comment = gen.generate_comment(context="code review")
    edge  = gen.generate_edge_text("cjk")
"""

import random
import textwrap
from typing import List


# --------------------------------------------------------------------------- #
# Vocabulary pools
# --------------------------------------------------------------------------- #

COMPONENTS = [
    "API Gateway",
    "Auth Service",
    "Data Pipeline",
    "Search Engine",
    "Notification Service",
    "Payment Processor",
    "User Profile Service",
    "Audit Logger",
    "Cache Layer",
    "Message Queue",
    "File Storage Service",
    "Admin Dashboard",
    "Reporting Module",
    "CI/CD Pipeline",
    "Rate Limiter",
    "Email Service",
    "Webhook Handler",
    "Config Service",
    "Feature Flag Service",
    "Analytics Engine",
    "Scheduler",
    "Migration Tool",
    "CLI Tooling",
    "GraphQL Resolver",
    "Load Balancer",
    "Session Manager",
    "Permissions Engine",
    "Content Delivery",
    "Monitoring Agent",
    "Backup Service",
]

SYMPTOMS = [
    "timeout",
    "crash",
    "memory leak",
    "null pointer exception",
    "race condition",
    "data corruption",
    "incorrect response",
    "slow query",
    "connection pool exhaustion",
    "deadlock",
    "infinite loop",
    "stack overflow",
    "encoding error",
    "permission denied",
    "stale cache",
]

CONDITIONS = [
    "handling concurrent requests",
    "processing large payloads",
    "running under high load",
    "connecting over VPN",
    "using pagination",
    "filtering by date range",
    "exporting to CSV",
    "running scheduled jobs",
    "refreshing auth tokens",
    "processing webhook callbacks",
    "during database failover",
    "with special characters in input",
    "after timezone change",
    "with empty optional fields",
    "during peak traffic hours",
]

ERRORS = [
    "HTTP 500 Internal Server Error",
    "ConnectionRefusedError",
    "OutOfMemoryError",
    "TimeoutException",
    "SerializationError",
    "ValidationError",
    "AuthenticationFailure",
    "ResourceNotFoundException",
    "ConcurrencyException",
    "SSLHandshakeException",
    "QuotaExceededException",
    "InvalidStateTransition",
    "DuplicateKeyException",
    "SchemaValidationError",
    "CircuitBreakerOpenException",
]

MODULES = [
    "user authentication flow",
    "batch processing pipeline",
    "event sourcing module",
    "REST API v2 endpoints",
    "GraphQL schema resolver",
    "real-time sync engine",
    "data export pipeline",
    "permission evaluation layer",
    "webhook delivery system",
    "metrics aggregation service",
    "configuration management",
    "multi-tenant routing",
    "audit trail subsystem",
    "search indexer",
    "notification dispatcher",
]

ROLES = [
    "developer",
    "product manager",
    "QA engineer",
    "DevOps engineer",
    "data analyst",
    "team lead",
    "support agent",
    "security auditor",
    "system administrator",
    "UX designer",
    "solutions architect",
    "release manager",
]

ACTIONS = [
    "view real-time metrics on the dashboard",
    "export filtered data as CSV",
    "configure custom alert thresholds",
    "integrate with Slack for notifications",
    "manage team permissions in bulk",
    "schedule recurring reports",
    "search across all projects with filters",
    "track deployment history per environment",
    "set up automated regression tests",
    "create custom workflow transitions",
    "monitor API usage quotas",
    "configure SSO with SAML provider",
]

BENEFITS = [
    "I can respond to incidents faster",
    "the team has better visibility into progress",
    "we reduce manual toil by 50%",
    "compliance requirements are met automatically",
    "onboarding new team members is self-service",
    "we can audit changes without manual queries",
    "stakeholders get updates without asking",
    "the release process is less error-prone",
    "cross-team dependencies are visible",
    "we catch regressions before production",
]

VERBS = [
    "Implement",
    "Configure",
    "Migrate",
    "Refactor",
    "Optimize",
    "Document",
    "Upgrade",
    "Validate",
    "Automate",
    "Benchmark",
    "Set up",
    "Integrate",
]

OBJECTS = [
    "database connection pooling",
    "CI pipeline caching",
    "Terraform infrastructure modules",
    "API rate limiting rules",
    "logging format and rotation",
    "monitoring dashboards",
    "unit test coverage for auth module",
    "data retention policies",
    "service mesh configuration",
    "dependency version pins",
    "error tracking integration",
    "health check endpoints",
]

PURPOSES = [
    "Q3 reliability targets",
    "compliance with SOC 2 requirements",
    "production readiness review",
    "cost optimization initiative",
    "improved developer experience",
    "the upcoming security audit",
    "customer-facing SLA commitments",
    "multi-region deployment",
    "the platform migration project",
    "reducing incident response time",
]

INITIATIVES = [
    "Platform Reliability Improvement",
    "Developer Experience Overhaul",
    "Customer Data Privacy Compliance",
    "Multi-Region Expansion",
    "Observability Stack Migration",
    "API Versioning Strategy",
    "Self-Service Analytics",
    "Zero-Trust Security Model",
    "Cloud Cost Optimization",
    "Performance Benchmarking Suite",
]

QUARTERS = ["Q1 2026", "Q2 2026", "Q3 2026", "Q4 2026", "H1 2026", "H2 2026"]

FEATURE_AREAS = [
    "granular RBAC for project-level access",
    "event-driven architecture for data sync",
    "incremental data export pipeline",
    "real-time collaboration on dashboards",
    "automated compliance reporting",
    "AI-assisted ticket triage",
    "cross-project dependency graph",
    "custom field type extensibility",
    "webhook retry and dead-letter queue",
    "federated search across integrations",
]

SPECIFIC_ACTIONS = [
    "unit tests for edge cases",
    "input validation on all endpoints",
    "error handling for timeout scenarios",
    "logging for debug-level traces",
    "database index for the new query pattern",
    "API response schema validation",
    "retry logic with exponential backoff",
    "pagination support for list endpoints",
    "cache invalidation on write path",
    "metric emission for latency percentiles",
    "field mapping for the new data model",
    "rollback script for the migration",
]

PARENT_CONTEXTS = [
    "the auth service refactor",
    "the data pipeline migration",
    "the API v2 release",
    "the search reindex project",
    "the monitoring overhaul",
    "the permissions system redesign",
    "the multi-tenant isolation work",
    "the CI/CD modernization",
    "the compliance remediation sprint",
    "the performance optimization epic",
]


# --------------------------------------------------------------------------- #
# Title templates per issue type
# --------------------------------------------------------------------------- #

TITLE_TEMPLATES = {
    "Bug": [
        "Fix {component} {symptom} when {condition}",
        "Resolve {error} in {module}",
        "{component}: {symptom} under {condition}",
        "Bug: {error} triggered by {condition}",
        "{component} produces {symptom} after recent deploy",
    ],
    "Story": [
        "As a {role}, I want to {action} so that {benefit}",
        "[User Story] {role}: {action}",
        "Enable {role} to {action}",
    ],
    "Task": [
        "{verb} {object} for {purpose}",
        "Update {component} {object}",
        "{verb} {component} configuration",
        "[Task] {verb} {object}",
    ],
    "Epic": [
        "{initiative} for {quarter}",
        "Implement {feature_area}",
        "[Epic] {initiative}",
        "{initiative}: {feature_area}",
    ],
    "Sub-task": [
        "{verb} {specific_action} in {parent_context}",
        "Add {specific_action} for {parent_context}",
        "[Sub-task] {specific_action}",
    ],
}


# --------------------------------------------------------------------------- #
# Description building blocks
# --------------------------------------------------------------------------- #

_BUG_SECTIONS = {
    "steps": [
        "1. Navigate to the {component} settings page.\n"
        "2. Trigger the operation that causes {condition}.\n"
        "3. Observe the {symptom} in the response / logs.",
        "1. Send a request to the {component} endpoint with a large payload.\n"
        "2. Wait for the async processing to complete.\n"
        "3. Check the logs for {error}.",
    ],
    "expected": [
        "The operation should complete without errors and return a 200 response.",
        "The system should handle the edge case gracefully and log a warning.",
    ],
    "actual": [
        "A {symptom} occurs, causing the request to fail with {error}.",
        "The {component} enters an inconsistent state and subsequent requests also fail.",
    ],
    "environment": [
        "Production (us-east-1), observed on 3 separate occasions this week.",
        "Staging environment, reproducible with the attached test script.",
    ],
}

_STORY_SECTIONS = {
    "context": [
        "Currently, {role}s have to perform this task manually, which takes ~15 minutes "
        "per occurrence and is error-prone.",
        "This capability has been requested by 4 different customers in the last quarter. "
        "It directly impacts retention for mid-market accounts.",
    ],
    "acceptance": [
        "- [ ] The feature is accessible from the main navigation.\n"
        "- [ ] The feature works for users with read-only and admin roles.\n"
        "- [ ] Performance: page loads in under 2 seconds for 10K records.\n"
        "- [ ] Audit log entries are created for every action.",
        "- [ ] The UI matches the approved mockups (attached).\n"
        "- [ ] Edge case: empty state shows a helpful message.\n"
        "- [ ] Integration test covers the happy path and one failure mode.\n"
        "- [ ] Documentation updated in the user guide.",
    ],
}

_TASK_SECTIONS = {
    "scope": [
        "This task covers the configuration changes, testing, and documentation updates. "
        "Code changes are tracked in a separate sub-task.",
        "Scope is limited to the {component} service. Cross-service impacts will be handled "
        "in follow-up tasks.",
    ],
    "steps": [
        "1. Review the current {component} configuration.\n"
        "2. Apply the changes per the design doc (linked).\n"
        "3. Run the integration test suite.\n"
        "4. Update the runbook with the new procedure.",
        "1. Create a feature branch from main.\n"
        "2. Implement the changes described below.\n"
        "3. Add/update unit tests (target: 80% coverage for changed files).\n"
        "4. Open a PR and request review from the platform team.",
    ],
}

_EPIC_SECTIONS = {
    "vision": [
        "This epic aims to {feature_area}. The expected business impact is a measurable "
        "improvement in {purpose}, with a target completion date aligned to {quarter}.",
        "The initiative addresses a gap identified during the last quarterly review. "
        "Without this work, we risk falling behind on {purpose}.",
    ],
    "milestones": [
        "**Phase 1 (Weeks 1-2):** Discovery and design.\n"
        "**Phase 2 (Weeks 3-6):** Implementation of core functionality.\n"
        "**Phase 3 (Weeks 7-8):** Testing, documentation, and rollout.",
        "**M1:** Technical design approved.\n"
        "**M2:** MVP feature complete (behind feature flag).\n"
        "**M3:** Beta testing with 3 internal teams.\n"
        "**M4:** GA release.",
    ],
}


# --------------------------------------------------------------------------- #
# Comment templates
# --------------------------------------------------------------------------- #

_COMMENT_TEMPLATES = [
    "I looked into this and the root cause is in the {component} module. "
    "The {symptom} happens because we're not handling the edge case where {condition}. "
    "I'll push a fix in the next PR.",

    "Tested this on staging and it works as expected. "
    "One concern: when {condition}, the response time increases by ~200ms. "
    "Not a blocker for this ticket but worth tracking.",

    "Can we get clarification on the acceptance criteria? "
    "Specifically, should this work for all user roles or just admins? "
    "The current implementation restricts it to admin-level permissions.",

    "Reviewed the PR. A few suggestions:\n"
    "1. The error handling in `handle_request()` should catch `TimeoutError` explicitly.\n"
    "2. Consider adding a retry with backoff for transient failures.\n"
    "3. The test coverage for the new code path is at 65% -- can we push to 80%?",

    "Blocked on this -- waiting for the {component} team to provide the API schema. "
    "I've pinged them on Slack and in their standup. "
    "ETA from their side is end of this week.",

    "Moving this to Done. Changes are deployed to production and monitoring looks clean. "
    "No alerts in the last 24 hours. Will keep an eye on it through the next release cycle.",

    "I've added the database migration script. "
    "Please review the rollback procedure in the PR description before approving. "
    "The migration is backward-compatible and can run while the service is live.",

    "This is related to the issue we saw last sprint with {component}. "
    "I think we should address both together to avoid duplicate work. "
    "Linking the other ticket for context.",

    "Updated the priority to High based on the customer escalation. "
    "Three enterprise accounts are affected. "
    "Support has a workaround documented but it's manual and error-prone.",

    "Pairing with {role} on this tomorrow. "
    "The scope is bigger than originally estimated -- "
    "I think we need to break it into 2-3 sub-tasks for parallel work.",

    "Performance test results are in:\n"
    "- P50 latency: 45ms (target: <100ms) -- PASS\n"
    "- P99 latency: 380ms (target: <500ms) -- PASS\n"
    "- Throughput: 1,200 req/s (target: >1,000) -- PASS\n"
    "All green. Ready for production.",

    "Flagging a potential security concern: "
    "the current implementation doesn't validate the input length on the {component} endpoint. "
    "An attacker could send a 10MB payload and exhaust memory. "
    "Adding input size validation as part of this ticket.",
]


# --------------------------------------------------------------------------- #
# Edge case text pools
# --------------------------------------------------------------------------- #

_EDGE_TEXT = {
    "cjk": [
        "\u30c6\u30b9\u30c8\u30c1\u30b1\u30c3\u30c8: \u65e5\u672c\u8a9e\u306e\u8aac\u660e\u6587\u3067\u3059\u3002\u3053\u308c\u306f\u30c6\u30b9\u30c8\u7528\u306e\u30c6\u30ad\u30b9\u30c8\u3067\u3059\u3002",
        "\u6d4b\u8bd5\u5de5\u5355: \u8fd9\u662f\u4e2d\u6587\u63cf\u8ff0\u3002\u7528\u4e8e\u6d4b\u8bd5\u591a\u8bed\u8a00\u652f\u6301\u548c\u7f16\u7801\u5904\u7406\u3002",
        "\ud14c\uc2a4\ud2b8 \ud2f0\ucf13: \ud55c\uad6d\uc5b4 \uc124\uba85\uc785\ub2c8\ub2e4. \ub2e4\uad6d\uc5b4 \uc9c0\uc6d0\uc744 \ud14c\uc2a4\ud2b8\ud569\ub2c8\ub2e4.",
        "\u6df7\u5408\u30c6\u30b9\u30c8 Mixed: English\u3068\u65e5\u672c\u8a9e\u3068\u4e2d\u6587\u6df7\u5408\u6587\u7ae0\u3002",
    ],
    "arabic": [
        "\u062a\u0630\u0643\u0631\u0629 \u0627\u062e\u062a\u0628\u0627\u0631: \u0647\u0630\u0627 \u0646\u0635 \u0639\u0631\u0628\u064a \u0644\u0644\u0627\u062e\u062a\u0628\u0627\u0631. \u064a\u062a\u0645 \u0627\u062e\u062a\u0628\u0627\u0631 \u0627\u062a\u062c\u0627\u0647 \u0627\u0644\u0643\u062a\u0627\u0628\u0629 \u0645\u0646 \u0627\u0644\u064a\u0645\u064a\u0646 \u0625\u0644\u0649 \u0627\u0644\u064a\u0633\u0627\u0631.",
        "\u05db\u05e8\u05d8\u05d9\u05e1 \u05d1\u05d3\u05d9\u05e7\u05d4: \u05d8\u05e7\u05e1\u05d8 \u05d1\u05e2\u05d1\u05e8\u05d9\u05ea \u05dc\u05d1\u05d3\u05d9\u05e7\u05ea RTL.",
    ],
    "rtl": [
        "\u0645\u062a\u0646 \u0622\u0632\u0645\u0627\u06cc\u0634\u06cc \u0641\u0627\u0631\u0633\u06cc: \u0627\u06cc\u0646 \u06cc\u06a9 \u0645\u062a\u0646 \u0622\u0632\u0645\u0627\u06cc\u0634\u06cc \u0627\u0633\u062a \u06a9\u0647 \u0628\u0631\u0627\u06cc \u0628\u0631\u0631\u0633\u06cc \u067e\u0634\u062a\u06cc\u0628\u0627\u0646\u06cc RTL \u0627\u0633\u062a\u0641\u0627\u062f\u0647 \u0645\u06cc\u200c\u0634\u0648\u062f.",
        "\u200f\u200fRTL with LTR embedded: English text \u062f\u0627\u062e\u0644 \u0645\u062a\u0646 \u0639\u0631\u0628\u06cc and back again.",
    ],
    "emoji": [
        "Bug report \U0001f41b: The login button \U0001f510 doesn't work \u274c when clicked \U0001f5b1\ufe0f twice rapidly \u26a1",
        "Feature request \U0001f680: Add dark mode \U0001f319 with \U0001f3a8 theme customization \u2728\u2728\u2728",
        "\U0001f525\U0001f525\U0001f525 CRITICAL \U0001f525\U0001f525\U0001f525 Production is down \U0001f6a8 ALL HANDS \U0001f64b\u200d\u2642\ufe0f\U0001f64b\u200d\u2640\ufe0f",
        "Emoji-only title: \U0001f4dd\u27a1\ufe0f\U0001f4ca\u27a1\ufe0f\u2705",
        "Flags: \U0001f1fa\U0001f1f8 \U0001f1ec\U0001f1e7 \U0001f1ef\U0001f1f5 \U0001f1e9\U0001f1ea \U0001f1ee\U0001f1f3 | Skin tones: \U0001f44d\U0001f3fb\U0001f44d\U0001f3fc\U0001f44d\U0001f3fd\U0001f44d\U0001f3fe\U0001f44d\U0001f3ff",
    ],
    "html": [
        '<script>alert("XSS test")</script>',
        '<img src=x onerror="alert(1)">',
        "Title with <b>bold</b> and <i>italic</i> and <a href='http://evil.com'>link</a>",
        '"><img src=x onerror=alert(document.cookie)>',
        "<div style='background:red;width:9999px;height:9999px;position:fixed;top:0;left:0;z-index:9999'>OVERLAY</div>",
    ],
    "sql_injection": [
        "'; DROP TABLE issues; --",
        "' OR '1'='1' --",
        "1; UPDATE users SET role='admin' WHERE '1'='1",
        "Robert'); DROP TABLE Students;--",
        "' UNION SELECT username, password FROM users --",
    ],
    "long_255": [
        "A" * 255,
        "This is a title that is exactly 255 characters long and is used to test field length limits in Jira " + "x" * 155,
        "\u00e4\u00f6\u00fc\u00df\u00e9\u00e8\u00ea\u00eb\u00e0\u00e1\u00e2\u00e3\u00e4\u00e5\u00e6\u00e7" * 15 + "\u00e4\u00f6\u00fc" * 5,
    ],
    "long_32k": [
        "A" * 32768,
        ("This is a very long description intended to test the maximum field length. " * 500)[:32768],
    ],
    "special_chars": [
        "Ticket with \ttabs\t and \nnewlines\n embedded",
        "NULL bytes: \x00\x00\x00 (should be stripped)",
        "Backslashes: C:\\Users\\test\\path and \\n literal",
        "Quotes: 'single' \"double\" `backtick` \u201csmart\u201d \u2018curly\u2019",
        "Math: 2\u00b2 + 3\u00b3 = 4 + 27 = 31 \u2260 30 \u00b1 1 \u00d7 10\u207b\u00b2",
    ],
    "empty": [
        "",
        "   ",
        "\n\n\n",
        "\t\t",
    ],
}


# --------------------------------------------------------------------------- #
# TextGenerator class
# --------------------------------------------------------------------------- #


class TextGenerator:
    """Generates deterministic, realistic Jira ticket text from seed templates.

    Args:
        seed: Integer seed for reproducible output. Same seed = same sequence.
    """

    def __init__(self, seed: int = 42):
        self._rng = random.Random(seed)

    def _pick(self, items: list) -> str:
        """Pick a random item from a list."""
        return self._rng.choice(items)

    def _pick_n(self, items: list, n: int) -> List[str]:
        """Pick n random items (with replacement) from a list."""
        return [self._rng.choice(items) for _ in range(n)]

    # ------------------------------------------------------------------ #
    # Title generation
    # ------------------------------------------------------------------ #

    def generate_title(self, issue_type: str) -> str:
        """Generate a realistic ticket title for the given issue type.

        Args:
            issue_type: One of Bug, Story, Task, Epic, Sub-task.
                        Falls back to Task templates for unknown types.

        Returns:
            A formatted title string.
        """
        templates = TITLE_TEMPLATES.get(issue_type, TITLE_TEMPLATES["Task"])
        template = self._pick(templates)

        replacements = {
            "component": self._pick(COMPONENTS),
            "symptom": self._pick(SYMPTOMS),
            "condition": self._pick(CONDITIONS),
            "error": self._pick(ERRORS),
            "module": self._pick(MODULES),
            "role": self._pick(ROLES),
            "action": self._pick(ACTIONS),
            "benefit": self._pick(BENEFITS),
            "verb": self._pick(VERBS),
            "object": self._pick(OBJECTS),
            "purpose": self._pick(PURPOSES),
            "initiative": self._pick(INITIATIVES),
            "quarter": self._pick(QUARTERS),
            "feature_area": self._pick(FEATURE_AREAS),
            "specific_action": self._pick(SPECIFIC_ACTIONS),
            "parent_context": self._pick(PARENT_CONTEXTS),
        }

        return template.format(**replacements)

    # ------------------------------------------------------------------ #
    # Description generation
    # ------------------------------------------------------------------ #

    def generate_description(
        self, issue_type: str, length: str = "medium"
    ) -> str:
        """Generate a realistic ticket description.

        Args:
            issue_type: One of Bug, Story, Task, Epic, Sub-task.
            length: 'short' (~50 words), 'medium' (~150 words), 'long' (~500 words).

        Returns:
            A multi-section description string.
        """
        ctx = {
            "component": self._pick(COMPONENTS),
            "symptom": self._pick(SYMPTOMS),
            "condition": self._pick(CONDITIONS),
            "error": self._pick(ERRORS),
            "module": self._pick(MODULES),
            "role": self._pick(ROLES),
            "action": self._pick(ACTIONS),
            "benefit": self._pick(BENEFITS),
            "verb": self._pick(VERBS),
            "object": self._pick(OBJECTS),
            "purpose": self._pick(PURPOSES),
            "initiative": self._pick(INITIATIVES),
            "quarter": self._pick(QUARTERS),
            "feature_area": self._pick(FEATURE_AREAS),
            "specific_action": self._pick(SPECIFIC_ACTIONS),
            "parent_context": self._pick(PARENT_CONTEXTS),
        }

        if issue_type == "Bug":
            return self._bug_description(ctx, length)
        elif issue_type == "Story":
            return self._story_description(ctx, length)
        elif issue_type == "Epic":
            return self._epic_description(ctx, length)
        elif issue_type == "Sub-task":
            return self._subtask_description(ctx, length)
        else:
            return self._task_description(ctx, length)

    def _bug_description(self, ctx: dict, length: str) -> str:
        parts = []
        # -- short: summary + brief context + frequency (~50 words) --
        parts.append(
            f"## Summary\n"
            f"A {ctx['symptom']} occurs in the {ctx['component']} when {ctx['condition']}. "
            f"This issue was first reported by the QA team during regression testing and has "
            f"since been observed in production. The error manifests as a {ctx['error']} in "
            f"the {ctx['module']}. Frequency appears to be intermittent, correlating with "
            f"periods of elevated traffic."
        )

        if length in ("medium", "long"):
            # -- medium adds: steps, expected, actual, severity (~150 words total) --
            parts.append(
                f"\n\n## Steps to Reproduce\n"
                f"{self._pick(_BUG_SECTIONS['steps']).format(**ctx)}"
            )
            parts.append(
                f"\n\n## Expected Behavior\n"
                f"{self._pick(_BUG_SECTIONS['expected'])}"
            )
            parts.append(
                f"\n\n## Actual Behavior\n"
                f"{self._pick(_BUG_SECTIONS['actual']).format(**ctx)}"
            )
            parts.append(
                f"\n\n## Severity Assessment\n"
                f"This bug affects the core workflow for users interacting with the "
                f"{ctx['component']}. When triggered, the {ctx['symptom']} prevents the "
                f"user from completing their task and requires a manual retry or page "
                f"refresh. No data loss has been confirmed, but the user experience is "
                f"significantly degraded during affected sessions."
            )

        if length == "long":
            # -- long adds: environment, RCA, impact, fix, workaround, testing (~500 words total) --
            parts.append(
                f"\n\n## Environment\n"
                f"{self._pick(_BUG_SECTIONS['environment'])}\n"
                f"- Browser: Chrome 124, Firefox 125 (both affected)\n"
                f"- OS: macOS 14.4, Ubuntu 22.04\n"
                f"- API version: v3 (current stable)"
            )
            parts.append(
                f"\n\n## Root Cause Analysis\n"
                f"Preliminary investigation suggests the issue is in the "
                f"{ctx['module']}. The {ctx['component']} does not properly "
                f"handle the case where {ctx['condition']}. When this happens, "
                f"the system throws a {ctx['error']} which is not caught by the "
                f"current error handling middleware.\n\n"
                f"The problematic code path was introduced in the last release "
                f"as part of the performance optimization work. The optimization "
                f"removed a defensive check that previously prevented this scenario. "
                f"Specifically, the previous implementation validated the input size "
                f"before forwarding the request to the downstream service, but the "
                f"new code assumes the input has already been validated at the API "
                f"gateway layer. This assumption is incorrect for requests that bypass "
                f"the gateway (internal service-to-service calls).\n\n"
                f"The issue is exacerbated under concurrent load because the shared "
                f"connection pool reaches its limit before the timeout triggers, "
                f"leading to cascading failures across the service mesh."
            )
            parts.append(
                f"\n\n## Impact\n"
                f"This affects approximately 5% of requests during peak hours. "
                f"The {ctx['symptom']} cascades to downstream services, causing "
                f"degraded performance across the entire {ctx['component']} cluster. "
                f"Customer-facing impact includes increased error rates on the dashboard "
                f"and intermittent failures on the export functionality. Three enterprise "
                f"customers have filed support tickets referencing this behavior. The "
                f"estimated revenue risk if left unresolved is moderate -- primarily "
                f"through increased churn probability for affected accounts."
            )
            parts.append(
                f"\n\n## Proposed Fix\n"
                f"1. Restore the defensive check in the hot path with a size limit "
                f"of 10MB per request payload.\n"
                f"2. Add a circuit breaker for the {ctx['module']} with a threshold "
                f"of 50% failure rate over a 30-second window.\n"
                f"3. Add monitoring for this specific failure mode including a PagerDuty "
                f"alert when the error rate exceeds 1% of total requests.\n"
                f"4. Add a regression test covering this edge case with both small and "
                f"large payloads under concurrent load.\n"
                f"5. Update the API documentation to reflect the payload size limit.\n\n"
                f"## Workaround\n"
                f"Until the fix is deployed, users can avoid triggering the issue by "
                f"reducing the payload size or retrying the operation after a brief "
                f"pause. Support has been notified and will share this workaround with "
                f"affected customers.\n\n"
                f"## Testing Plan\n"
                f"- Unit test: validate input size check triggers correctly.\n"
                f"- Integration test: send oversized payload through full request path.\n"
                f"- Load test: simulate 500 concurrent requests with mixed payload sizes.\n"
                f"- Regression: re-run the full test suite for the {ctx['component']}."
            )

        return "".join(parts)

    def _story_description(self, ctx: dict, length: str) -> str:
        parts = []
        # -- short: story + brief context (~50 words) --
        parts.append(
            f"## User Story\n"
            f"As a {ctx['role']}, I want to {ctx['action']} so that {ctx['benefit']}. "
            f"This story addresses a gap in the current {ctx['component']} workflow "
            f"where users must perform the operation manually, leading to delays and "
            f"inconsistent results across the team."
        )

        if length in ("medium", "long"):
            # -- medium adds: context, acceptance, priority rationale (~150 words total) --
            parts.append(
                f"\n\n## Context\n"
                f"{self._pick(_STORY_SECTIONS['context']).format(**ctx)}"
            )
            parts.append(
                f"\n\n## Acceptance Criteria\n"
                f"{self._pick(_STORY_SECTIONS['acceptance'])}"
            )
            parts.append(
                f"\n\n## Priority Rationale\n"
                f"This story is prioritized based on customer feedback volume (4 requests "
                f"this quarter) and alignment with the team's OKR to improve self-service "
                f"capabilities. The {ctx['component']} team has capacity in the current "
                f"sprint to pick this up, and the backend dependencies are already resolved."
            )

        if length == "long":
            # -- long adds: technical notes, design, out of scope, testing, analytics (~500 words total) --
            parts.append(
                f"\n\n## Technical Notes\n"
                f"This feature requires changes to the {ctx['component']} service. "
                f"The backend needs a new API endpoint and the frontend needs a new "
                f"page component.\n\n"
                f"Key considerations:\n"
                f"- The data model for this feature already exists; we need to expose "
                f"it through the API.\n"
                f"- Permissions: only users with the 'manage_settings' scope should "
                f"see this feature.\n"
                f"- Performance: the underlying query should use the existing index. "
                f"If it doesn't, we'll need a migration (separate task).\n"
                f"- Feature flag: roll out behind `enable_{ctx['component'].lower().replace(' ', '_')}_v2` flag.\n"
                f"- Rate limiting: the new endpoint should respect the global rate "
                f"limit configuration.\n"
                f"- Caching: responses can be cached for 5 minutes at the CDN layer."
            )
            parts.append(
                f"\n\n## Design\n"
                f"Mockups are attached. The design follows the existing pattern used "
                f"in the {ctx['component']} settings page. Key interactions:\n"
                f"- Primary action button in the top-right corner.\n"
                f"- Confirmation modal before destructive operations.\n"
                f"- Inline validation on all form fields.\n"
                f"- Loading skeleton during async operations.\n"
                f"- Empty state with helpful onboarding message.\n"
                f"- Responsive layout: collapses to single column on mobile."
            )
            parts.append(
                f"\n\n## Analytics\n"
                f"Track the following events:\n"
                f"- `feature_viewed` -- when the page loads\n"
                f"- `action_initiated` -- when the user clicks the primary CTA\n"
                f"- `action_completed` -- when the operation succeeds\n"
                f"- `action_failed` -- when the operation fails (with error category)\n"
                f"- `time_to_complete` -- duration from initiation to completion"
            )
            parts.append(
                f"\n\n## Out of Scope\n"
                f"- Mobile responsive design (tracked in a follow-up story)\n"
                f"- Bulk operations (separate epic)\n"
                f"- Email notifications for this action (separate story)\n"
                f"- Webhook integration for external consumers\n"
                f"- Custom keyboard shortcuts\n\n"
                f"## Migration Notes\n"
                f"Existing users who previously used the workaround (direct API calls) "
                f"will not be affected. The new UI is additive. No data migration is "
                f"required. The feature flag allows gradual rollout: start with internal "
                f"users, then beta customers, then GA."
            )

        return "".join(parts)

    def _task_description(self, ctx: dict, length: str) -> str:
        parts = []
        # -- short: objective + context (~50 words) --
        parts.append(
            f"## Objective\n"
            f"{ctx['verb']} {ctx['object']} for {ctx['purpose']}. This task is "
            f"part of the ongoing effort to improve reliability and maintainability "
            f"of the {ctx['component']}. The current configuration has not been "
            f"updated since the last major release and needs to align with the new "
            f"operational requirements."
        )

        if length in ("medium", "long"):
            # -- medium adds: scope, steps, background (~150 words total) --
            parts.append(
                f"\n\n## Scope\n"
                f"{self._pick(_TASK_SECTIONS['scope']).format(**ctx)}"
            )
            parts.append(
                f"\n\n## Steps\n"
                f"{self._pick(_TASK_SECTIONS['steps']).format(**ctx)}"
            )
            parts.append(
                f"\n\n## Background\n"
                f"The {ctx['component']} was last configured during the Q1 setup phase. "
                f"Since then, the team has adopted new practices around {ctx['purpose']} "
                f"which require corresponding infrastructure changes. This task captures "
                f"the operational work needed to bring the system into compliance."
            )

        if length == "long":
            # -- long adds: dependencies, risks, DoD, rollback, verification (~500 words total) --
            parts.append(
                f"\n\n## Dependencies\n"
                f"- Requires access to the {ctx['component']} configuration repository.\n"
                f"- The {ctx['module']} changes must be merged before this task can begin.\n"
                f"- Coordinate with the platform team for deployment window.\n"
                f"- The monitoring dashboard must be updated to reflect the new "
                f"configuration parameters before the change goes live."
            )
            parts.append(
                f"\n\n## Risks\n"
                f"- If the {ctx['component']} service is under active development, "
                f"merge conflicts are likely. Mitigation: coordinate with the feature "
                f"team and schedule the change during a low-activity window.\n"
                f"- The configuration change could affect other services in the shared "
                f"namespace. Mitigation: validate with integration tests before "
                f"promoting to production.\n"
                f"- If the new configuration values are incorrect, the service may "
                f"experience degraded performance or increased error rates. Mitigation: "
                f"deploy behind a feature flag with gradual rollout.\n"
                f"- Rollback complexity is low -- the previous configuration is "
                f"version-controlled and can be restored in under 5 minutes."
            )
            parts.append(
                f"\n\n## Rollback Procedure\n"
                f"If issues are detected after deployment:\n"
                f"1. Revert the configuration change via `git revert` on the config repo.\n"
                f"2. Trigger a deployment pipeline run for the {ctx['component']}.\n"
                f"3. Verify service health via the monitoring dashboard.\n"
                f"4. Notify the team in the #platform channel."
            )
            parts.append(
                f"\n\n## Definition of Done\n"
                f"- [ ] Changes applied and verified in staging.\n"
                f"- [ ] Integration tests pass with the new configuration.\n"
                f"- [ ] Runbook updated with the new procedure and rollback steps.\n"
                f"- [ ] Peer review completed by at least one platform team member.\n"
                f"- [ ] Change deployed to production with monitoring confirmed.\n"
                f"- [ ] No increase in error rates or latency for 24 hours post-deploy.\n"
                f"- [ ] Documentation in Confluence updated to reflect the change."
            )

        return "".join(parts)

    def _epic_description(self, ctx: dict, length: str) -> str:
        parts = []
        # -- short: vision + problem statement (~50 words) --
        parts.append(
            f"## Vision\n"
            f"{self._pick(_EPIC_SECTIONS['vision']).format(**ctx)} "
            f"This initiative was identified as a strategic priority after the last "
            f"quarterly planning session. Without this investment, the team risks "
            f"accumulating technical debt that will slow delivery velocity in "
            f"subsequent quarters."
        )

        if length in ("medium", "long"):
            # -- medium adds: milestones, metrics, team (~150 words total) --
            parts.append(
                f"\n\n## Milestones\n"
                f"{self._pick(_EPIC_SECTIONS['milestones'])}"
            )
            parts.append(
                f"\n\n## Success Metrics\n"
                f"- Primary: Achieve measurable improvement in {ctx['purpose']}.\n"
                f"- Secondary: Reduce manual effort by 40%+.\n"
                f"- Guardrail: No increase in P99 latency above 500ms.\n"
                f"- Leading indicator: 80% of child stories completed by Phase 2 end."
            )
            parts.append(
                f"\n\n## Team\n"
                f"- 2 backend engineers (full-time during Phase 2)\n"
                f"- 1 frontend engineer (part-time during Phase 2-3)\n"
                f"- 1 QA engineer (Phase 3)\n"
                f"- Product and design support throughout"
            )

        if length == "long":
            # -- long adds: stakeholders, risks, related, non-goals, budget, comms (~500 words total) --
            parts.append(
                f"\n\n## Stakeholders\n"
                f"- **Sponsor:** VP Engineering\n"
                f"- **Technical Lead:** Staff Engineer, {ctx['component']} team\n"
                f"- **Product Owner:** PM for {ctx['component']}\n"
                f"- **Consumers:** {ctx['role']}s across all product lines\n"
                f"- **Dependencies:** Platform team (infrastructure), Security team (review)"
            )
            parts.append(
                f"\n\n## Risks and Mitigations\n"
                f"| Risk | Likelihood | Impact | Mitigation |\n"
                f"|------|-----------|--------|------------|\n"
                f"| Scope creep from stakeholder requests | High | Medium | "
                f"Strict phase gates with sign-off |\n"
                f"| Key engineer OOO during Phase 2 | Medium | High | "
                f"Cross-train second engineer before Phase 2 |\n"
                f"| Integration complexity with {ctx['component']} | Medium | Medium | "
                f"Spike in Phase 1 to validate approach |\n"
                f"| External dependency delay (Platform team) | Medium | High | "
                f"Pre-book platform team capacity in Sprint 3 |\n"
                f"| Security review findings require rework | Low | High | "
                f"Engage security team early in Phase 1 design review |"
            )
            parts.append(
                f"\n\n## Budget and Resources\n"
                f"- Engineering effort: ~12 person-weeks across all phases.\n"
                f"- Infrastructure cost: estimated $500/month increase for additional "
                f"compute during the migration phase, returning to baseline after "
                f"optimization in Phase 3.\n"
                f"- No additional headcount required. Existing team capacity is sufficient "
                f"assuming no competing P0 incidents."
            )
            parts.append(
                f"\n\n## Communication Plan\n"
                f"- Weekly status update in the #epic-{ctx['component'].lower().replace(' ', '-')} "
                f"Slack channel.\n"
                f"- Bi-weekly demo to stakeholders (30 min).\n"
                f"- Phase gate reviews with VP Engineering at each milestone.\n"
                f"- Post-mortem and retrospective after GA release."
            )
            parts.append(
                f"\n\n## Related Epics\n"
                f"- [Link to prerequisite epic]\n"
                f"- [Link to dependent epic]\n"
                f"- [Link to the observability epic that tracks monitoring for this work]\n\n"
                f"## Non-Goals\n"
                f"- Complete rewrite of the {ctx['component']} (only targeted improvements)\n"
                f"- Support for legacy API consumers (they migrate separately)\n"
                f"- Multi-region support (tracked in a separate initiative)\n"
                f"- Changes to the public API contract (backward compatibility required)"
            )

        return "".join(parts)

    def _subtask_description(self, ctx: dict, length: str) -> str:
        parts = []
        # -- short: task + rationale (~50 words) --
        parts.append(
            f"## Task\n"
            f"{ctx['verb']} {ctx['specific_action']} in {ctx['parent_context']}. "
            f"This sub-task was broken out from the parent to allow parallel work and "
            f"focused code review. The {ctx['specific_action']} is a prerequisite for "
            f"the integration testing phase of the parent task."
        )

        if length in ("medium", "long"):
            # -- medium adds: details, files, approach (~150 words total) --
            parts.append(
                f"\n\n## Details\n"
                f"This sub-task is part of the larger effort to improve the "
                f"{ctx['component']}. Specifically, we need to add "
                f"{ctx['specific_action']} to ensure reliability when "
                f"{ctx['condition']}.\n\n"
                f"Files likely affected:\n"
                f"- `src/{ctx['component'].lower().replace(' ', '_')}/handler.py`\n"
                f"- `tests/{ctx['component'].lower().replace(' ', '_')}/test_handler.py`"
            )
            parts.append(
                f"\n\n## Approach\n"
                f"Follow the existing pattern in the {ctx['component']} codebase. "
                f"The implementation should be minimal and focused -- no refactoring "
                f"of surrounding code unless strictly necessary to complete this task. "
                f"If unexpected complexity is discovered, flag it and discuss with the "
                f"team before expanding scope."
            )

        if length == "long":
            # -- long adds: implementation, testing, acceptance, context, checklist (~500 words total) --
            parts.append(
                f"\n\n## Implementation Notes\n"
                f"The parent context ({ctx['parent_context']}) established the overall "
                f"architecture. This sub-task fills in the {ctx['specific_action']} gap.\n\n"
                f"Key constraints:\n"
                f"- Must be backward compatible with the existing API contract.\n"
                f"- Performance budget: no more than 5ms added latency per request.\n"
                f"- The implementation should follow the existing patterns in the codebase "
                f"(see `src/common/patterns.py` for reference).\n"
                f"- All new code must include inline documentation explaining the "
                f"rationale for non-obvious decisions.\n"
                f"- Error messages should be specific and actionable -- no generic "
                f"'something went wrong' strings."
            )
            parts.append(
                f"\n\n## Testing Strategy\n"
                f"- Unit tests for the new code path (minimum 3 test cases):\n"
                f"  - Happy path with valid input.\n"
                f"  - Edge case with boundary values.\n"
                f"  - Error case with invalid input.\n"
                f"- One integration test covering the interaction with {ctx['component']}.\n"
                f"- Manual verification in staging before marking as Done.\n"
                f"- Performance spot-check: run the benchmark suite and compare "
                f"P50/P99 latency against the baseline."
            )
            parts.append(
                f"\n\n## Context from Parent\n"
                f"The parent task ({ctx['parent_context']}) aims to {ctx['verb'].lower()} "
                f"the overall {ctx['component']} system. This sub-task handles one specific "
                f"piece: {ctx['specific_action']}. Other sub-tasks in this parent cover "
                f"related but independent pieces of work. Coordinate with sibling sub-task "
                f"owners if you encounter shared code that needs modification.\n\n"
                f"Relevant design decisions from the parent:\n"
                f"- The {ctx['module']} will be used as the primary integration point.\n"
                f"- All changes must be behind the existing feature flag.\n"
                f"- Rollback should be possible without a database migration."
            )
            parts.append(
                f"\n\n## Acceptance Criteria\n"
                f"- [ ] Code implemented and reviewed by at least one team member.\n"
                f"- [ ] Tests pass in CI (unit + integration).\n"
                f"- [ ] No regressions in existing test suite.\n"
                f"- [ ] Code coverage for changed files is at least 80%.\n"
                f"- [ ] Documentation updated if any public interfaces changed.\n"
                f"- [ ] Parent task updated with status and any findings."
            )

        return "".join(parts)

    # ------------------------------------------------------------------ #
    # Comment generation
    # ------------------------------------------------------------------ #

    def generate_comment(self, context: str = "") -> str:
        """Generate a realistic Jira comment.

        Args:
            context: Optional context string (e.g., 'code review', 'status update').
                     Currently used to seed variety but the templates are general enough
                     to work without it.

        Returns:
            A comment string.
        """
        template = self._pick(_COMMENT_TEMPLATES)
        return template.format(
            component=self._pick(COMPONENTS),
            symptom=self._pick(SYMPTOMS),
            condition=self._pick(CONDITIONS),
            error=self._pick(ERRORS),
            module=self._pick(MODULES),
            role=self._pick(ROLES),
        )

    # ------------------------------------------------------------------ #
    # Edge case text
    # ------------------------------------------------------------------ #

    def generate_edge_text(self, category: str) -> str:
        """Generate edge-case text for stress testing.

        Args:
            category: One of 'cjk', 'arabic', 'rtl', 'emoji', 'html',
                      'sql_injection', 'long_255', 'long_32k', 'special_chars',
                      'empty'.

        Returns:
            An edge-case text string.

        Raises:
            ValueError: If the category is not recognized.
        """
        pool = _EDGE_TEXT.get(category)
        if pool is None:
            valid = ", ".join(sorted(_EDGE_TEXT.keys()))
            raise ValueError(
                f"Unknown edge text category '{category}'. Valid: {valid}"
            )
        return self._pick(pool)

    def list_edge_categories(self) -> List[str]:
        """Return all available edge text categories."""
        return sorted(_EDGE_TEXT.keys())
