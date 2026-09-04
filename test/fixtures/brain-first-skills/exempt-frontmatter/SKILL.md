---
type: ssot
last_updated: 2026-09-04
name: exempt-frontmatter
description: Pure-infra skill that opts out via frontmatter
triggers:
  - "schedule a cron job"
mutating: true
brain_first: exempt
owner: governance-team
last-reviewed: 2026-09-04
---

# exempt-frontmatter

This skill manages cron schedules. It does call web_search for time-zone
data and perplexity for cron syntax help — but the maintainer declared
`brain_first: exempt` because the skill is pure infrastructure that
doesn't consult brain knowledge.

## How

Use web_search for tz data, perplexity for cron syntax. Update the
crontab via the host system call.
