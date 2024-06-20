---
name: Bug report
about: Create a report to help us improve
title: ''
labels: 'kind/bug'
assignees: ''
---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:

1. Go to '...'
2. Click on '....'
3. Scroll down to '....'
4. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

**Spicepod**
Add the relevant `spicepod.yml` section.

**Output of the `describe table`**
Add any relevant `describe table` output here.

**Output of `explain query`**
Add any relevant `explain query` output here.

E.g. `explain select 1`
```
+---------------+--------------------------------------+
| plan_type     | plan                                 |
+---------------+--------------------------------------+
| logical_plan  | Projection: Int64(1)                 |
|               |   EmptyRelation                      |
| physical_plan | ProjectionExec: expr=[1 as Int64(1)] |
|               |   PlaceholderRowExec                 |
|               |                                      |
+---------------+--------------------------------------+
```

**spice, spiced, OS info**
- spice version: `spice version`
- spiced version: `spiced --version`
- OS info: `uname -a`

**Have you tried this on the latest `trunk` branch?**

**If possible, run `spiced` with DEBUG log level**

By setting the environment variable `SPICED_LOG="spiced=DEBUG,runtime=DEBUG,sql_provider_datafusion=DEBUG,data_components=DEBUG,cache=DEBUG"`

**Screenshots**
If applicable, add screenshots to help explain your problem.

**Additional context**
Add any other context about the problem here.
