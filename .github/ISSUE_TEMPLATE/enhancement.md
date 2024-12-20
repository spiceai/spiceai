---
name: Enhancement
about: Suggest an Enhancement
title: 'Enhancement: <>'
type: enhancement
assignees: ''
---

<!--
_REMEMBER, BE **SMART**!_

_S: Specific_
_M: Measurable_
_A: Achievable_
_R: Relevant_
_T: Time-Bound_
-->

## Goal-State/What/Result

<!-- _A clear and concise description of what the enhancement is and the target goal-state._ -->

## Why/Purpose

<!-- _Clear and concise answer to why this should be done now._ -->

## By When

<!-- _A target date for done-done completion of the entire enhancement._ -->

**Issue/Spec written and reviewed:** _Target Date_
**Done-Done:** _Target Date_

## Done-Done

- [ ] [Principles Driven](https://github.com/spiceai/spiceai/blob/trunk/docs/PRINCIPLES.md)
- [ ] The Algorithm
- [ ] PM/Design Review
- [ ] DX/UX Review
- [ ] Threat Model / Security Review
- [ ] Tests
- [ ] Performance / Benchmarks
- [ ] Documentation
- [ ] Cookbook Recipes/Tutorials
- [ ] Release Notes prepared

## The Algorithm

- [ ] Every requirement questioned?
- [ ] Delete (Scope) any part you can.
- [ ] Simplify.
- [ ] Break down into smaller iterations/milestones.
- [ ] Opportunities for automation.

## Specification

<!-- _Provide a basic specification of the enhancement._ -->

### Security Review

<!-- _Provide threat model and security review._ -->

## How/Plan

<!-- _A clear and concise plan of how this should be implemented._ -->

### Implementation (Sub-Issues)

<!--
- [ ] _Sub-Issue 1_
- [ ] _Sub-Issue 2_
-->

### QA

<!--
- [ ] _Sub-Issue 1_
- [ ] _Sub-Issue 2_
-->

## Release Notes

<!--

Release notes for this enhancement, to be filled once completed. These release notes will be included as highlights on the next release.

Example:

## Release Notes

API Key Authentication: Spice now supports optional authentication for API endpoints via configurable API keys, for additional security and control over runtime access.

Example Spicepod.yml configuration:
```yaml
runtime:
  auth:
    api-key:
      enabled: true
      keys:
        - ${ secrets:api_key } # Load from a secret store
        - my-api-key # Or specify directly
```

Would be included on the next release like:

# Highlights in v1.0-rc.1

{other release note}

API Key Authentication: Spice now supports optional authentication for API endpoints via configurable API keys, for additional security and control over runtime access.

Example Spicepod.yml configuration:
```yaml
runtime:
  auth:
    api-key:
      enabled: true
      keys:
        - ${ secrets:api_key } # Load from a secret store
        - my-api-key # Or specify directly
```

{other release note}

-->