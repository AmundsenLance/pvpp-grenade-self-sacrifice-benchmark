# Exploratory Scalar-Favoring Withdrawal Variant

## Status

This document is exploratory only.

It is not part of the main benchmark claim in this repository. It records a possible side experiment intended to pressure-test the benchmark by asking whether a plausible non-grenade withdrawal variant can be constructed in which scalar reasoning has a better chance to outperform PV–PP.

## Purpose

This specification defines a narrow optional benchmark mode for the existing grenade mission-flow sim.

Its purpose is to create a non-grenade withdrawal environment in which scalar-style tradeoff handling has a plausible opportunity to outperform PV–PP.

This mode is not intended to replace the current benchmark.

It is intended to answer a focused exploratory question:

**Does a plausible parameterized version of the current mission-flow scenario exist in which scalar outperforms PV–PP?**

The design goal is to test that question without:

- building a new sim,
- rewriting the core movement system,
- rewriting grenade logic,
- or hard-coding a scalar advantage.

## Core Principle

The current no-grenade environment produces path variation but no decision divergence between PV–PP and scalar.

This suggests that the ordinary withdrawal branch currently lacks a live tradeoff that separates the models.

The proposed mode introduces such a tradeoff by making withdrawal success depend on a balance between:

- escort screen strength, and
- movement and exposure costs of clustering.

This is intended to favor a smoother tradeoff structure rather than a hard governing-threshold structure. That is the kind of environment where scalar should have its best chance.

## Scope

This mode applies only when grenade response is not the active source of model divergence, specifically for ordinary withdrawal conditions.

It should be implemented as an optional mode that can be toggled by command-line argument.

It should not alter:

- entry phase logic,
- advance phase logic,
- recovery phase logic,
- map generation,
- replay format except for recording the mode,
- or core grenade-response logic.

It may affect:

- ordinary withdrawal risk calculation,
- escort protection effects,
- escort spacing effects,
- and ordinary withdrawal path incentives if those are already expressed through risk.

## Proposed Command-Line Argument

Add:

`--scalar-favor-mode`

Allowed values:

- `off`
- `screen_tradeoff`

Default:

- `off`

## Proposed Supporting Parameters

### Escort screen bonus strength

`--screen-bonus-strength`

- type: float
- default: 0.35

Controls how strongly nearby escorts reduce background-fire risk against the carrier and recoveree during ordinary withdrawal.

### Over-cluster penalty strength

`--cluster-penalty-strength`

- type: float
- default: 0.15

Controls the penalty applied when too many soldiers remain tightly clustered near the carrier and recoveree during withdrawal.

### Screen radius

`--screen-radius`

- type: int
- default: 2

Radius within which an escort counts as screening the carrier.

### Cluster radius

`--cluster-radius`

- type: int
- default: 1

Radius used to count over-clustering near the carrier and recoveree.

### Optimal escort count

`--optimal-screen-count`

- type: int
- default: 2

The target number of nearby escorts that gives the best protection benefit.

### Over-cluster threshold

`--cluster-threshold`

- type: int
- default: 3

If more than this many friendly soldiers are tightly clustered near the carrier and recoveree, the over-cluster penalty begins applying.

## Mode Semantics

When `--scalar-favor-mode screen_tradeoff` is enabled, ordinary withdrawal risk becomes sensitive to two competing effects.

### Screen benefit

Nearby escorts reduce fire risk to:

- the carrier
- the recoveree

### Cluster penalty

Too much clustering near the carrier and recoveree increases fire risk or exposure cost for the group.

This creates the intended tradeoff:

- too few escorts means weak protection,
- too many or too tight means congestion and exposure cost.

## Where It Should Apply

This mode should apply only during ordinary withdrawal.

It should not apply:

- during advance,
- during recovery before withdrawal,
- during grenade-triggered locked-blast response,
- or during grenade resolution itself.

The first implementation should keep scope narrow: ordinary non-grenade withdrawal only.

## Protection Calculation Intent

For the carrier, compute the number of alive escorts within `screen_radius`, excluding the carrier itself.

If no carrier exists, use the recoveree as the anchor only if the recoveree is moving independently.

The protection curve should be strongest up to the optimal escort count, then flatten:

- 0 escorts: no benefit
- 1 escort: partial benefit
- 2 escorts: full intended benefit
- 3 or more escorts: capped benefit, not linearly increasing

This avoids a silly “more clustering is always better” effect.

## Cluster Penalty Intent

Count alive soldiers within `cluster_radius` of the anchor position.

If this count exceeds `cluster_threshold`, apply a modest but real penalty.

It must be strong enough to matter, but not so strong that escorting becomes irrational.

## Risk Application

Under `screen_tradeoff`, ordinary withdrawal fire risk should be modified as follows:

- begin with current ordinary withdrawal fire probability,
- reduce it according to screen bonus,
- increase it according to cluster penalty.

The same structure should apply to the recoveree.

Escort risk can optionally receive a small exposure increase if the formation is too tight, but that is not necessary for a first pass.

## Decision-Theoretic Intent

This mode is intended to create a world where success depends on balancing:

- enough escort protection,
- not too much clustering,
- continued movement toward extraction,
- and cumulative expected harm.

That is a smoother and more scalar-friendly problem than the compressed grenade tradeoff.

The mode is not intended to guarantee scalar victory. It is intended to make scalar victory plausible.

## Constraints on Implementation

Implementation should be narrow.

Do not:

- rewrite movement ranking,
- rewrite phase transitions,
- rewrite pickup logic,
- rewrite extraction logic,
- rewrite map logic,
- or rewrite grenade-response policy construction.

Preferred implementation location:

- ordinary withdrawal background-fire calculation,
- or a narrowly scoped withdrawal-risk helper.

## Expected Behavioral Effect

When enabled, this mode should tend to produce cases where:

- aggressively preserving a tight escort shell is not always best,
- aggressively minimizing local exposure is not always best,
- and the best choice may be a weighted compromise.

Possible outcomes:

- scalar may outperform PV–PP in some runs,
- PV–PP may remain equivalent,
- PV–PP may still outperform.

Any of those is informative. What matters is whether scalar can ever win in a plausible variant of the same mission environment.

## Suggested Exploratory Usage

Recommended first exploratory test:

- `--grenade-trigger-prob 0.0`
- `--scalar-favor-mode screen_tradeoff`

Run:

- 100
- 1000
- then 10000 if promising

Compare:

- extraction rate
- casualty totals
- path statistics
- PV–PP-only wins
- scalar-only wins

This is exploratory falsification pressure, not the main publication result.

## Success Criterion

This mode is successful if it produces a plausible environment in which:

- scalar and PV–PP are no longer identical in no-grenade withdrawal,
- and scalar can at least sometimes outperform PV–PP.

It is not necessary that scalar dominate.

## Failure Criterion

This mode fails if:

- it simply hard-codes scalar advantage,
- it rewrites too much of the sim,
- it produces bizarre behavior that does not resemble a plausible withdrawal environment,
- or the tradeoff is too weak and nothing changes.

## Recommendation

Keep this document only as an exploratory appendix document.

Do not present it as part of the main benchmark claim unless the exploratory mode is actually implemented, run, and shown to produce meaningful results.
