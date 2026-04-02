# Replay Viewer

## Purpose

This document explains how to use the replay viewer included in this repository.

The viewer exists to support manual inspection of paired benchmark runs. It is meant for visual review, not for recomputing the simulation. Its main value is that it lets a reviewer inspect what happened in a particular run without rerunning benchmark logic or inferring behavior from aggregate statistics alone.

## What the Viewer Is For

The replay viewer is useful for:

- inspecting paired `pvpp` and `baseline_self_preserving` runs side by side
- reviewing representative wins and losses
- checking whether a flagged movement pattern is actually ugly or only looks suspicious to the audit tool
- confirming that extraction success or failure happened for the reason claimed
- validating that the benchmark behavior still matches the intended architectural interpretation

The viewer is especially useful for:

- moderate and difficult PV–PP win cases
- improbable tail cases
- representative extraction failures
- replay files flagged by the audit tool

## What the Viewer Is Not For

The viewer is not:

- the simulation engine
- an optimizer
- a proof tool
- a replacement for the replay trace schema
- a substitute for aggregate benchmark results

It is a visual inspection tool.

## Expected Input

The viewer expects replay JSON files written by the benchmark simulation.

A replay file should represent:

- one benchmark episode
- one scenario or case
- one seed
- one paired comparison

with two model branches:

- `pvpp`
- `baseline_self_preserving`

The viewer should not need to recompute decisions. It should read states and display them.

## Typical File Location

In this repository, replay files are typically written to a directory such as:

- `data/grenade_missionflow_replays_v3/`

For public repository use, it is usually better to keep only a small curated sample set in version control rather than a full replay dump.

## Launching the Viewer

Typical launch command:

```bash
python3 src/replay_viewer.py --dir data/grenade_missionflow_replays_v3
```

To open a specific replay immediately, use `--file` with either the exact filename or a unique substring:

```bash
python3 src/replay_viewer.py --dir data/grenade_missionflow_replays_v3 --file 0905
```

To slow down or speed up playback, use `--delay-ms`:

```bash
python3 src/replay_viewer.py --dir data/grenade_missionflow_replays_v3 --delay-ms 250
```

## Recommended Workflow

The best workflow is:

1. run the benchmark or generate replay traces
2. run the replay trace audit tool
3. identify representative or suspicious replay files
4. open those files in the viewer
5. inspect the paired branches manually
6. decide whether the behavior is actually defective, merely imperfect, or fully acceptable

That order matters. The viewer is most useful when it is being used to answer a focused question.

## Map / Terrain Legend

The viewer uses simple visual symbols to represent terrain and map features.

At minimum:

- a **tree** indicates **cover**
- a **small brick wall** indicates **cover**

Treat both as protected terrain rather than open movement space.

## What to Inspect Manually

When reviewing a replay, inspect the following:

### Recoveree extraction path

Check:

- whether the recoveree path is smooth
- whether extraction succeeds cleanly
- whether extraction failure is understandable
- whether the recoveree is being preserved for the reason claimed

### Sacrificial protection cases

Check:

- whether the grenade event is truly compressed
- whether non-sacrificial escape paths were really unavailable or inadequate
- whether the sacrificial act appears only in the narrow intended class of cases
- whether the outcome matches the benchmark interpretation

### Movement quality

Check for:

- obvious left-right-left oscillation
- repeated lateral churn near extraction
- extraction-mouth blocking
- supporter clustering that looks visibly stupid rather than merely imperfect

### Paired-comparison difference

Check:

- whether `pvpp` and `baseline_self_preserving` start from the same geometry
- where their behavior first diverges
- whether that divergence matches the intended architectural difference
- whether a PV–PP win looks real rather than accidental

## How to Use the Viewer Well

The viewer is not for browsing random files with no question in mind. It works best when you use it to inspect one of three things:

- a representative success case
- a representative failure case
- a replay flagged by the audit tool

The most informative cases are usually:

- a moderate PV–PP-only extraction win
- a difficult PV–PP-only extraction win
- an improbable tail case
- a flagged crowding or oscillation case

## Interpreting What You See

Do not overinterpret tiny movement weirdness.

A benchmark replay can be publication-acceptable even if the pathing is not aesthetically perfect. The real question is:

- does the motion look structurally coherent?
- does the outcome happen for the reason the benchmark claims?
- does any remaining awkwardness materially damage interpretability?

That is the standard.

## Relationship to the Audit Tool

The viewer and the audit tool do different jobs.

The audit tool:

- scans many files quickly
- flags suspicious patterns
- is useful for triage

The viewer:

- lets you inspect the actual run
- determines whether a flagged pattern is really bad
- provides final human judgment

When they disagree, trust the viewer more than the audit metrics.

## Repository Practice

For repository use:

- keep the viewer in `src/`
- keep sample replay files small in number
- do not commit huge replay dumps unless there is a specific reason
- use viewer screenshots or notes only when they actually support an argument

## Bottom Line

Use the replay viewer to inspect representative or flagged paired runs and decide whether the benchmark behavior is actually coherent, not merely whether an automated audit was suspicious.
