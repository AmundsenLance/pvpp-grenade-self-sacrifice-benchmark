# Replay Data

## Purpose

This document explains how replay data is used in this repository.

Replay traces exist to make benchmark runs inspectable. They allow a reviewer to examine a specific paired run without rerunning the simulation or trying to reconstruct behavior from aggregate metrics alone.

## What a Replay Represents

A replay file should represent:

- one benchmark episode
- one scenario or case
- one seed
- one shared initial setup
- two model executions:
  - `pvpp`
  - `baseline_self_preserving`

That is the correct replay unit for this benchmark.

## Why Replay Data Matters

Replay traces support:

- deterministic replay
- side-by-side comparison of PV–PP and baseline
- visual animation without rerunning benchmark logic
- debugging of candidate choice, movement, and extraction
- manual validation of benchmark claims
- later demo or GitHub presentation

## Replay Design Rule

A replay trace should be:

- self-contained
- deterministic
- human-readable
- animation-ready
- independent of the live simulation engine

The viewer should not have to recompute the benchmark logic.

## Typical Location

Replay files are typically written to a directory such as:

- `data/grenade_missionflow_replays_v3/`

For public repository use, it is usually better to keep only a small curated sample set under version control rather than an enormous raw replay archive.

## Suggested Repository Practice

The cleanest practice is:

- keep large replay dumps local
- commit only a small curated sample set
- keep audit summaries separately
- reference the replay schema document if a reader needs the full data contract

A good committed sample set would include only a handful of files, such as:

- one easy convergence case
- one moderate PV–PP-only win
- one difficult PV–PP-only win
- one improbable tail case
- one replay that is useful for testing the viewer or audit tool

## Recommended Sample Layout

Example structure:

```text
data/
├── replay_trace_audit_findings.csv
├── replay_trace_audit_report.json
├── replay_trace_audit_report.txt
└── sample_replays/
    ├── moderate_pvpp_win_episode.json
    ├── difficult_pvpp_win_episode.json
    └── improbable_tail_case.json
```

That is better than committing a full raw replay dump unless you have a specific publication reason to do so.

## Replay Contents

A replay should normally include:

- schema version
- run metadata
- map definition
- shared initial state
- per-model branches
- chosen policy information
- full-frame snapshots
- final summary

This repository’s replay tooling assumes full-state snapshots rather than only diffs.

## Relationship to the Replay Schema

If the repository includes the replay schema note, that schema is the data contract.

This markdown file is not a substitute for the schema. It is the repository-facing explanation of how replay data is being used and stored.

## Relationship to the Viewer

The replay viewer consumes replay JSON files.

Its job is to display the stored states, not to rerun decision logic.

That means replay files should be written clearly enough that the viewer can show:

- positions
- state changes
- grenade events
- extraction progress
- final outcomes

without needing the simulation engine to infer missing pieces.

## Relationship to the Audit Tool

The audit tool also consumes replay JSON files.

It scans them for suspicious patterns and generates triage outputs. Those audit outputs should be treated as supporting diagnostics, not as benchmark claims in themselves.

## What Not to Do

Do not:

- treat replay files as aggregate benchmark summaries
- commit giant replay dumps by default
- assume that a replay sample proves the entire benchmark
- assume that a flagged replay is automatically defective

Replay data is evidentiary support and validation material, not the main statistical result.

## Bottom Line

Replay data exists so that benchmark runs can be inspected, validated, and demonstrated. Keep the committed replay set small and curated, and use the full replay archive locally when you need deeper validation.
