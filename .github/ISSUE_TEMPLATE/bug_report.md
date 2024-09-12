---
name: Bug report
about: Create a report to help us improve
title: ''
labels: 'kind/bug'
assignees: ''
---

## Describe the bug

A clear and concise description of what the bug is.

## To Reproduce

Steps to reproduce the behavior:

1. Go to '...'
2. Click on '....'
3. Scroll down to '....'
4. See error

## Expected behavior

A clear and concise description of what you expected to happen.

## Runtime Details

### Spicepod

Add the relevant `spicepod.yml` section.

### Output of the `describe table`

Add any relevant `describe table` output here.

### Output of `explain query`

Add any relevant `explain query` output here.

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
By setting the environment variable `SPICED_LOG="task_history=INFO,spiced=INFO,runtime=INFO,secrets=INFO,data_components=INFO,cache=INFO,extensions=INFO,spice_cloud=INFO"`

## Screenshots

If applicable, add screenshots to help explain your problem.

## Additional context

Add any other context about the problem here.
