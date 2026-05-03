# Changelog

All notable changes to this project are documented in this file.

---
## [scheduler-v2.0.0] - 2026-05-03

## scheduler v2.0.0

> **Bump type:** Major - Breaking change
> **Previous version:** scheduler-v1.2.2

### Breaking changes
- breaking: Release v2 framework (identify state and step of the pipeline run ([`93a136f`](../../commit/93a136f)) - Louis Barillon

### Features
- feat: find the data freshness automatically for each job to be sure we can run the new pipeline efficiently ([`8988778`](../../commit/8988778)) - Louis Barillon
- feat: analyze cron to adapt airflow and the orchestrator to need and difference frequencies ([`080dfbb`](../../commit/080dfbb)) - Louis Barillon
- feat: update segment resolver using new configuration, avoid issue using cron ([`72c1ba9`](../../commit/72c1ba9)) - Louis Barillon
- feat: update all config management to correspond of v2 framework ([`e9c112d`](../../commit/e9c112d)) - Louis Barillon
- feat: add configuration to use it as a remote package ([`e3502e8`](../../commit/e3502e8)) - Louis Barillon
- feat: add unit tests for the framework ([`dd9b2ef`](../../commit/dd9b2ef)) - Louis Barillon

### Other
- add : .gitignore file ([`b2a86a0`](../../commit/b2a86a0)) - Louis Barillon
- docs: updating docs with new implementation of v2 ([`dfea9f6`](../../commit/dfea9f6)) - Louis Barillon


---
---
## [scheduler-v1.2.2] - 2026-05-02

## scheduler v1.2.2

> **Bump type:** Patch - bug fix
> **Previous version:** scheduler-v1.2.1

### Fixes
- fix: airflow env configuration ([`d18ea49`](../../commit/d18ea49)) - Louis Barillon


---
---
## [scheduler-v1.2.1] - 2026-05-02

## scheduler v1.2.1

> **Bump type:** Patch - bug fix
> **Previous version:** scheduler-v1.2.0

### Fixes
- fix: primary bugs on the framework ([`1b0b6e3`](../../commit/1b0b6e3)) - Louis Barillon
- fix: issues during deployment ([`e740a38`](../../commit/e740a38)) - Louis Barillon

### Other
- Remove unwanted text at the end ([`8e4e9ed`](../../commit/8e4e9ed)) - Louis Barillon


---
---
## [scheduler-v1.2.0] - 2026-05-01

## scheduler v1.2.0

> **Bump type:** Minor - new feature
> **Previous version:** scheduler-v1.1.0

### Features
- feat: add github secret configuraiton for airflow ([`a1fa646`](../../commit/a1fa646)) - Louis Barillon

### Other
- Add: license & badges ([`1a82357`](../../commit/1a82357)) - Louis Barillon


---
