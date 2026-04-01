# Jira Test Data Benchmark

Generate synthetic Jira data and benchmark Fivetran's Jira connector for BQ-DTS.

## Scripts

| Script | What It Does |
|--------|-------------|
| `config.py` | Settings: Jira connection, project keys, volume targets |
| `generate_contract.py` | Coverage dataset (5-10K crafted issues touching every field/object) |
| `generate_csv.py` | Scale dataset CSVs for Jira bulk import (100K issues) |
| `augment.py` | API calls to add comments, worklogs, transitions, links, sprints |
| `edge_cases.py` | Stress dataset (extreme patterns designed to break things) |
| `verify.py` | Quick smoke test: all 20 objects populated? |
| `eval_suite.py` | Full validation (B1-B6, Q1-Q9) + benchmark manifest generation |
| `utils/rate_limiter.py` | Jira API rate limiting + auto-resume |
| `utils/text_generator.py` | Realistic ticket titles/descriptions from seed templates |
| `utils/distributions.py` | Archetype-based sampling (issue types, priorities, etc.) |

## Setup

```bash
cp .env.example .env
# Fill in JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN
pip install -r requirements.txt
```

## Usage

```bash
# Phase 0: Generate
python generate_contract.py   # Coverage data
python generate_csv.py        # Scale CSVs (import via Jira UI)
python augment.py             # Enrich with comments, worklogs, etc.
python edge_cases.py          # Stress data

# Phase 1: Validate
python verify.py              # Smoke test
python eval_suite.py          # Full eval + manifest
```

## Related

- [Fivetran Jira Scorecard](https://docs.google.com/document/d/1cKuxWgDnwmcBYc25YCfBXkASHpoq6bKypCBZVBYMVj8/edit)
