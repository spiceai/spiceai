---
name: Bug report
about: Create a report to help us improve
title: ''
type: bug
assignees: ''
---

## Describe the bug

_A clear and concise description of what the bug is._

## To Reproduce

_Steps to reproduce the behavior:_

1. Go to '...'
2. Click on '...'
3. Scroll down to '...'
4. See error

## Expected behavior

_A clear and concise description of what you expected to happen._

## Runtime Details

### Spicepod

_Add the relevant `spicepod.yml` section._

### Output of `describe table`

_Add any relevant `describe table` output here._

### Output of `explain query`

_Add any relevant `explain query` output here._

E.g. `explain select 1`

```bash
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

### spice, spiced, OS info

- spice version: `spice version`
- spiced version: `spiced --version`
- OS info: `uname -a`

**Have you tried this on the latest `trunk` branch?**

- [ ] Yes
- [ ] No

**If possible, run `spiced` with DEBUG log level**
By setting the environment variable `SPICED_LOG="task_history=DEBUG,spiced=DEBUG,runtime=DEBUG,secrets=DEBUG,data_components=DEBUG,cache=DEBUG,extensions=DEBUG,spice_cloud=DEBUG"`

## Screenshots

If applicable, add screenshots to help explain your problem.

## Additional context

Add any other context about the problem here.
