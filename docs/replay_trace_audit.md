# Replay Trace Audit

## Purpose

This document explains how to use the public replay trace audit tool included in this repository.

The audit tool scans replay JSON files and flags replay-integrity problems and severe benchmark-facing replay issues that may deserve manual review. It is a triage tool, not a proof engine.

## What the Audit Tool Is For

The public audit tool is useful for:

- scanning many replay files quickly
- identifying replay-integrity failures
- surfacing severe benchmark-facing replay-quality issues
- finding representative files for manual viewer inspection
- checking whether a new code change introduced clearly bad public-facing replay behavior

## What the Audit Tool Is Not For

The audit tool is not:

- a proof that something is a bug
- a substitute for viewer inspection
- a standalone measure of benchmark validity
- a replacement for aggregate benchmark results
- a headline benchmark metric

It exists to narrow the search space for manual review.

## Expected Input

The audit tool expects a directory of replay JSON files written by the benchmark simulation.

Typical input directory:

- `data/grenade_missionflow_replays_v3/`

For public repository use, it is usually better to keep only a small curated replay sample in version control and treat large replay dumps as local outputs.

## Typical Launch Command

Typical command:

```bash
python3 src/replay_trace_audit.py --dir data/grenade_missionflow_replays_v3 --print --limit 100
```

Adjust the directory and limit to match your local layout.

## What the Audit Produces

The audit typically produces:

- console summary output
- `replay_trace_audit_report.txt`
- `replay_trace_audit_report.json`
- `replay_trace_audit_findings.csv`

These files are useful for triage and documentation, but they should still be interpreted carefully.

## Public Audit Scope

The public audit is intentionally narrow.

It is designed to surface issues that are serious enough to matter for replay integrity or for public interpretation of benchmark behavior. It is not designed to expose every odd motion pattern or every trace-level texture issue.

That is deliberate. A public-facing audit should surface issues that actually matter, not manufacture noise.

## Severity Guide

The public audit groups surfaced findings by severity.

### Hard

A hard finding indicates a replay-integrity failure or a severe state-consistency problem. These findings should be treated as real defects until proven otherwise.

### Major

A major finding indicates a strong replay-quality problem that is serious enough to deserve immediate inspection. It may not always invalidate benchmark results, but it is not cosmetic.

### Minor

A minor finding indicates a weaker but still benchmark-facing replay concern. Minor findings still deserve review, but they should not be treated as automatic benchmark failures.

## How to Read the Audit Output

Start with:

1. hard findings
2. major findings
3. files with repeated findings
4. top flagged files for manual replay inspection

That order usually gives the most useful review targets first.

## Interpretation Rules

### Rule 1: a finding is not a proof

A flagged pattern is a reason to inspect the replay manually. The viewer remains the final judge.

### Rule 2: hard findings should be treated seriously

If the public audit surfaces a hard finding, assume it matters until you have verified otherwise.

### Rule 3: not every surfaced issue invalidates the benchmark

Some findings indicate replay-quality problems worth fixing without overturning the benchmark itself. The point is to identify real review targets, not to collapse all issues into one bucket.

### Rule 4: the audit is secondary to the benchmark itself

The benchmark’s main empirical claim comes from scenario design, extraction and casualty outcomes, paired model comparison, and manual inspection of representative runs. The audit is a support tool.

## Recommended Workflow

Best workflow:

1. generate replay traces from a benchmark run
2. run the public audit tool
3. inspect the highest-severity findings
4. open the flagged files in the replay viewer
5. decide whether the issue is:
   - real
   - cosmetic
   - acceptable
   - a false positive

## When to Trust the Audit More

Take the audit seriously when it points to:

- replay/state integrity failures
- grenade/state consistency failures
- severe terminal replay-quality issues
- the same file being bad across multiple surfaced findings

Those are the patterns most likely to matter publicly.

## When to Trust the Audit Less

Be more skeptical when:

- a finding is weak and the viewer looks fine
- the flagged behavior is visually acceptable and does not obstruct interpretation
- the issue looks cosmetic rather than structural

If the audit flags behavior that the viewer shows to be acceptable, the audit may be over-flagging and should be tightened.

## Recommended Repository Practice

For repository use:

- keep the public audit tool in `src/`
- keep only selected sample audit outputs in `data/`
- do not treat audit counts as headline benchmark results
- use the audit primarily as a support tool for validation and polish

## Relationship to Benchmark Claims

The audit tool is not part of the benchmark’s main empirical claim.

The main benchmark claim comes from:

- matched scenario design
- extraction and casualty outcomes
- paired model comparison
- manual interpretation of representative runs

The audit is secondary. It helps ensure that replay behavior is not obviously broken or publicly misleading.

## Bottom Line

Use the public audit tool to identify replay files that deserve manual inspection. Treat hard findings seriously. Do not confuse a surfaced finding with a complete proof.
