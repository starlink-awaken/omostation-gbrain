---
type: ssot
name: no-external
description: Skill that operates purely on local state
triggers:
  - "rotate the log file"
mutating: true
owner: governance-team
last-reviewed: 2026-09-04
---

# no-external

This skill rotates log files locally. No external APIs, no brain queries.
Trivially exempt from brain-first compliance because there's nothing
to consult.

## How

Read the log path from config, rename to .log.1, truncate the active file.
