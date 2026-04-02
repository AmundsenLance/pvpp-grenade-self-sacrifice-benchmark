from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_REPLAY_DIR = "grenade_missionflow_replays_v3"
DEFAULT_OUTPUT_JSON = "replay_trace_audit_report.json"
DEFAULT_OUTPUT_TEXT = "replay_trace_audit_report.txt"
DEFAULT_OUTPUT_CSV = "replay_trace_audit_findings.csv"

PUBLIC_MODE = "public"
INTERNAL_MODE = "internal"

Position = Tuple[int, int]

PUBLIC_INCLUDED_CATEGORIES = {
    "movement_jump",
    "recoveree_jump",
    "revived_dead",
    "dead_carrying",
    "dead_integrity_mismatch",
    "recoveree_dead_integrity_mismatch",
    "dead_extracted_recoveree",
    "grenade_reactivated",
    "grenade_active_and_detonated",
    "absorb_missing_actor",
    "absorb_survived",
    "recoveree_absorb_survived",
    "chosen_absorb_not_applied",
    "no_policy_selected",
    "sacrifice_policy_mismatch",
    "grenade_lock_mismatch",
    "terminal_lateral_stall",
    "terminal_neutral_churn",
}


@dataclass
class Finding:
    file: str
    case_id: str
    sim_version: str
    map_mode: str
    model: str
    category: str
    severity: int
    message: str
    frame_index: Optional[int] = None
    tick: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file": self.file,
            "case_id": self.case_id,
            "sim_version": self.sim_version,
            "map_mode": self.map_mode,
            "model": self.model,
            "category": self.category,
            "severity": self.severity,
            "message": self.message,
            "frame_index": self.frame_index,
            "tick": self.tick,
        }


@dataclass
class AuditContext:
    file_name: str
    case_id: str
    sim_version: str
    map_mode: str
    model_name: str
    findings: List[Finding] = field(default_factory=list)
    candidate_evaluations: List[Dict[str, Any]] = field(default_factory=list)
    chosen_policy: Optional[Dict[str, Any]] = None

    def add(
        self,
        category: str,
        severity: int,
        message: str,
        frame_index: Optional[int] = None,
        tick: Optional[int] = None,
    ) -> None:
        self.findings.append(
            Finding(
                file=self.file_name,
                case_id=self.case_id,
                sim_version=self.sim_version,
                map_mode=self.map_mode,
                model=self.model_name,
                category=category,
                severity=severity,
                message=message,
                frame_index=frame_index,
                tick=tick,
            )
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit grenade replay traces for integrity failures and replay-quality issues."
    )
    parser.add_argument("--dir", type=str, default=DEFAULT_REPLAY_DIR)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--top", type=int, default=80)
    parser.add_argument("--mode", type=str, choices=[INTERNAL_MODE], default=PUBLIC_MODE, help=argparse.SUPPRESS)
    parser.add_argument("--json-out", type=str, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--text-out", type=str, default=DEFAULT_OUTPUT_TEXT)
    parser.add_argument("--csv-out", type=str, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--print-top", "--print", dest="print_top", action="store_true")
    return parser.parse_args()


def read_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def pos_key(entity: Dict[str, Any]) -> Position:
    return int(entity["row"]), int(entity["col"])


def manhattan(a: Position, b: Position) -> int:
    return abs(a[0] - b[0]) + abs(a[1] - b[1])


def get_run_metadata(trace_data: Dict[str, Any]) -> Tuple[str, str, str]:
    run_metadata = trace_data.get("run_metadata", {})
    case_id = str(run_metadata.get("case_id", "UNKNOWN"))
    map_mode = str(run_metadata.get("map_mode", "UNKNOWN"))
    sim_version = "UNKNOWN"
    models = trace_data.get("models", {})
    if models:
        first_model = next(iter(models.values()))
        sim_version = str(first_model.get("sim_version", "UNKNOWN"))
    return case_id, sim_version, map_mode


def get_entity_snapshots(frames: List[Dict[str, Any]], entity_id: str) -> List[Tuple[int, int, int, str, int]]:
    out: List[Tuple[int, int, int, str, int]] = []
    for frame in frames:
        tick = int(frame.get("tick", -1))
        phase = str(frame.get("phase"))
        frame_index = int(frame.get("frame_index", -1))
        if entity_id == "RECOVEREE":
            rec = frame.get("recoveree", {})
            out.append((tick, int(rec.get("row", -1)), int(rec.get("col", -1)), phase, frame_index))
        else:
            soldiers = frame.get("soldiers", [])
            match = next((s for s in soldiers if s.get("soldier_id") == entity_id), None)
            if match is not None:
                out.append((tick, int(match.get("row", -1)), int(match.get("col", -1)), phase, frame_index))
    return out


def get_frame_carrier(frame: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rec = frame.get("recoveree", {})
    carrier_id = rec.get("carried_by_id")
    if not carrier_id:
        return None
    return next((s for s in frame.get("soldiers", []) if s.get("soldier_id") == carrier_id), None)


def extraction_distance(pos: Position, extraction_zone: set[Position]) -> int:
    return min(manhattan(pos, ex) for ex in extraction_zone)


def _count_lateral_reversals(cols: List[int]) -> int:
    signed_steps: List[int] = []
    for prev, curr in zip(cols, cols[1:]):
        delta = curr - prev
        if delta > 0:
            signed_steps.append(1)
        elif delta < 0:
            signed_steps.append(-1)
    reversals = 0
    for prev, curr in zip(signed_steps, signed_steps[1:]):
        if prev != curr:
            reversals += 1
    return reversals


def _same_row_dither_metrics(positions: List[Position]) -> Tuple[int, int, int]:
    cols = [p[1] for p in positions]
    transitions = sum(1 for a, b in zip(cols, cols[1:]) if a != b)
    reversals = _count_lateral_reversals(cols)
    return len(set(positions)), transitions, reversals


def _is_meaningful_same_row_dither(positions: List[Position], extraction_zone: set[Position]) -> bool:
    unique_positions, transitions, reversals = _same_row_dither_metrics(positions)
    if unique_positions < 2:
        return False
    if reversals == 0 and transitions <= 1:
        return False
    if unique_positions == 2 and reversals >= 1 and transitions >= 2:
        return True
    if unique_positions >= 3 and transitions >= 3:
        dists = [extraction_distance(p, extraction_zone) for p in positions]
        return max(dists) - min(dists) <= 1
    return False


def analyze_oscillation(frames: List[Dict[str, Any]], extraction_zone: set[Position], ctx: AuditContext) -> None:
    entity_ids = ["RECOVEREE", "S1", "S2", "S3", "S4", "S5"]
    for entity_id in entity_ids:
        seq = get_entity_snapshots(frames, entity_id)
        if len(seq) < 4:
            continue
        for i in range(2, len(seq)):
            a = (seq[i - 2][1], seq[i - 2][2])
            b = (seq[i - 1][1], seq[i - 1][2])
            c = (seq[i][1], seq[i][2])
            if a == c and a != b:
                da = extraction_distance(a, extraction_zone)
                db = extraction_distance(b, extraction_zone)
                dc = extraction_distance(c, extraction_zone)
                if db < da and db < dc and max(da, dc) - db == 1:
                    continue
                tick = seq[i][0]
                frame_index = seq[i][4]
                near_extraction = min(da, db, dc) <= 2
                sev = 3 + (1 if entity_id == "RECOVEREE" else 0) + (1 if near_extraction else 0)
                msg = f"{entity_id} shows A-B-A oscillation at ticks {seq[i-2][0]}->{seq[i-1][0]}->{seq[i][0]} positions {a}->{b}->{c}"
                ctx.add("oscillation", min(sev, 9), msg, frame_index=frame_index, tick=tick)
        for i in range(3, len(seq)):
            tail = seq[i - 3:i + 1]
            rows = {p[1] for p in tail}
            phases = {p[3] for p in tail}
            positions = [(p[1], p[2]) for p in tail]
            if len(rows) == 1 and "withdrawal" in phases and _is_meaningful_same_row_dither(positions, extraction_zone):
                near_extraction = min(extraction_distance(pos, extraction_zone) for pos in set(positions)) <= 2
                tick = tail[-1][0]
                frame_index = tail[-1][4]
                sev = 3 + (1 if entity_id == "RECOVEREE" else 0) + (1 if near_extraction else 0)
                msg = f"{entity_id} lateral dithering in withdrawal on row {next(iter(rows))}: positions {positions}"
                ctx.add("lateral_dither", min(sev, 9), msg, frame_index=frame_index, tick=tick)


def analyze_movement_legality(frames: List[Dict[str, Any]], ctx: AuditContext) -> None:
    previous_positions: Dict[str, Tuple[Position, bool]] = {}
    for frame in frames:
        tick = int(frame.get("tick", -1))
        frame_index = int(frame.get("frame_index", -1))
        phase = str(frame.get("phase"))
        for soldier in frame.get("soldiers", []):
            sid = str(soldier.get("soldier_id"))
            pos = pos_key(soldier)
            alive = bool(soldier.get("alive", True))
            prev = previous_positions.get(sid)
            if prev is not None:
                prev_pos, prev_alive = prev
                dist = manhattan(prev_pos, pos)
                if prev_alive and alive and phase not in {"post_grenade", "grenade_trigger"} and dist > 3:
                    ctx.add("movement_jump", 8, f"{sid} moved illegal distance {dist} from {prev_pos} to {pos} during phase {phase}", frame_index=frame_index, tick=tick)
            previous_positions[sid] = (pos, alive)
        rec = frame.get("recoveree", {})
        if rec:
            pos = pos_key(rec)
            alive = bool(rec.get("alive", True))
            carried = rec.get("carried_by_id") is not None
            prev = previous_positions.get("RECOVEREE")
            if prev is not None:
                prev_pos, prev_alive = prev
                dist = manhattan(prev_pos, pos)
                if prev_alive and alive and not carried and phase not in {"post_grenade", "grenade_trigger"} and dist > 2:
                    ctx.add("recoveree_jump", 8, f"Recoveree moved illegal distance {dist} from {prev_pos} to {pos} during phase {phase}", frame_index=frame_index, tick=tick)
            previous_positions["RECOVEREE"] = (pos, alive)


def analyze_state_consistency(frames: List[Dict[str, Any]], ctx: AuditContext) -> None:
    dead_seen: set[str] = set()
    grenade_detonated = False
    for frame in frames:
        tick = int(frame.get("tick", -1))
        frame_index = int(frame.get("frame_index", -1))
        soldiers = frame.get("soldiers", [])
        rec = frame.get("recoveree", {})
        grenade = frame.get("grenade")
        for soldier in soldiers:
            sid = str(soldier.get("soldier_id"))
            alive = bool(soldier.get("alive", True))
            carrying = bool(soldier.get("is_carrying_recoveree", False))
            integrity = soldier.get("integrity")
            if sid in dead_seen and alive:
                ctx.add("revived_dead", 9, f"{sid} appears alive after previously being dead", frame_index=frame_index, tick=tick)
            if not alive:
                dead_seen.add(sid)
                if carrying:
                    ctx.add("dead_carrying", 8, f"{sid} is dead but still marked as carrying recoveree", frame_index=frame_index, tick=tick)
                if integrity != "I0":
                    ctx.add("dead_integrity_mismatch", 7, f"{sid} is dead but integrity={integrity}", frame_index=frame_index, tick=tick)
        if rec:
            rec_alive = bool(rec.get("alive", True))
            rec_integrity = rec.get("integrity")
            if not rec_alive and rec_integrity != "I0":
                ctx.add("recoveree_dead_integrity_mismatch", 7, f"Recoveree is dead but integrity={rec_integrity}", frame_index=frame_index, tick=tick)
            if bool(rec.get("extracted", False)) and not rec_alive:
                ctx.add("dead_extracted_recoveree", 9, "Recoveree marked both dead and extracted", frame_index=frame_index, tick=tick)
        if grenade:
            active = bool(grenade.get("active", False))
            has_detonated = bool(grenade.get("has_detonated", False))
            if grenade_detonated and active:
                ctx.add("grenade_reactivated", 8, "Grenade became active after prior detonation", frame_index=frame_index, tick=tick)
            if has_detonated:
                grenade_detonated = True
                if active:
                    ctx.add("grenade_active_and_detonated", 7, "Grenade marked both active and detonated", frame_index=frame_index, tick=tick)


def analyze_grenade_logic(frames: List[Dict[str, Any]], final_summary: Dict[str, Any], ctx: AuditContext) -> None:
    for frame in frames:
        phase = str(frame.get("phase"))
        tick = int(frame.get("tick", -1))
        frame_index = int(frame.get("frame_index", -1))
        grenade = frame.get("grenade")
        if not grenade:
            continue
        if phase == "grenade_trigger":
            locked_center = list(grenade.get("locked_center_ids", []))
            locked_adjacent = list(grenade.get("locked_adjacent_ids", []))
            rec_on_center = bool(grenade.get("locked_recoveree_on_center", False))
            rec_adjacent = bool(grenade.get("locked_recoveree_adjacent", False))
            if rec_on_center and "RECOVEREE" not in locked_center:
                ctx.add("grenade_lock_mismatch", 8, "Recoveree marked on-center but RECOVEREE missing from locked_center_ids", frame_index=frame_index, tick=tick)
            if rec_adjacent and "RECOVEREE" not in locked_adjacent:
                ctx.add("grenade_lock_mismatch", 8, "Recoveree marked adjacent but RECOVEREE missing from locked_adjacent_ids", frame_index=frame_index, tick=tick)
        if phase == "post_grenade":
            absorb_actor_id = grenade.get("absorb_actor_id")
            absorb_actor_kind = grenade.get("absorb_actor_kind")
            soldiers = frame.get("soldiers", [])
            rec = frame.get("recoveree", {})
            if absorb_actor_id:
                if absorb_actor_kind == "soldier":
                    absorber = next((s for s in soldiers if s.get("soldier_id") == absorb_actor_id), None)
                    if absorber is None:
                        ctx.add("absorb_missing_actor", 9, f"Absorb actor {absorb_actor_id} missing from soldier list", frame_index=frame_index, tick=tick)
                    elif bool(absorber.get("alive", True)):
                        ctx.add("absorb_survived", 9, f"Absorb actor {absorb_actor_id} survived post_grenade", frame_index=frame_index, tick=tick)
                elif absorb_actor_kind == "recoveree" and bool(rec.get("alive", True)):
                    ctx.add("recoveree_absorb_survived", 9, "Recoveree marked as absorber but survives post_grenade", frame_index=frame_index, tick=tick)
            if ctx.chosen_policy is not None and ctx.chosen_policy.get("policy") == "ABSORB_GRENADE" and not absorb_actor_id:
                ctx.add("chosen_absorb_not_applied", 9, "Chosen policy was ABSORB_GRENADE but post_grenade has no absorb_actor_id", frame_index=frame_index, tick=tick)
    if final_summary.get("grenade_triggered") and ctx.candidate_evaluations:
        feasible_absorb = [e for e in ctx.candidate_evaluations if e.get("policy") == "ABSORB_GRENADE" and e.get("feasible") is True]
        if feasible_absorb and ctx.chosen_policy is None:
            ctx.add("no_policy_selected", 7, "Grenade triggered and absorb was feasible but no policy was selected")
        if final_summary.get("sacrifice_occurred") and ctx.chosen_policy is not None and ctx.chosen_policy.get("policy") != "ABSORB_GRENADE":
            ctx.add("sacrifice_policy_mismatch", 8, f"Final summary says sacrifice occurred but chosen policy is {ctx.chosen_policy.get('policy')}")


def analyze_extraction_lane(frames: List[Dict[str, Any]], extraction_zone: set[Position], ctx: AuditContext) -> None:
    for frame in frames:
        tick = int(frame.get("tick", -1))
        frame_index = int(frame.get("frame_index", -1))
        if str(frame.get("phase")) != "withdrawal":
            continue
        rec = frame.get("recoveree", {})
        carrier_id = rec.get("carried_by_id")
        if not carrier_id:
            continue
        soldiers = frame.get("soldiers", [])
        carrier = next((s for s in soldiers if s.get("soldier_id") == carrier_id), None)
        if carrier is None or not carrier.get("alive", True):
            continue
        carrier_pos = pos_key(carrier)
        carrier_dist = extraction_distance(carrier_pos, extraction_zone)
        if carrier_dist > 2:
            continue
        blockers: List[Tuple[str, Position]] = []
        for soldier in soldiers:
            sid = str(soldier.get("soldier_id"))
            if sid == carrier_id or not soldier.get("alive", True):
                continue
            pos = pos_key(soldier)
            if pos in extraction_zone:
                blockers.append((sid, pos))
        if len(blockers) >= 2:
            ctx.add("extraction_lane_crowding", 5, f"Carrier {carrier_id} is near extraction at distance {carrier_dist}, but extraction zone still occupied by {blockers}", frame_index=frame_index, tick=tick)


def analyze_progress(frames: List[Dict[str, Any]], extraction_zone: set[Position], ctx: AuditContext) -> None:
    last_carrier_distance: Optional[int] = None
    flat_count = 0
    for frame in frames:
        tick = int(frame.get("tick", -1))
        frame_index = int(frame.get("frame_index", -1))
        if str(frame.get("phase")) != "withdrawal":
            continue
        carrier = get_frame_carrier(frame)
        if carrier is None or not carrier.get("alive", True):
            continue
        pos = pos_key(carrier)
        dist = extraction_distance(pos, extraction_zone)
        if last_carrier_distance is not None:
            if dist > last_carrier_distance:
                ctx.add("carrier_progress_reversal", 6, f"Carrier {carrier.get('soldier_id')} increased extraction distance from {last_carrier_distance} to {dist}", frame_index=frame_index, tick=tick)
                flat_count = 0
            elif dist == last_carrier_distance:
                flat_count += 1
                if flat_count >= 3 and dist <= 3:
                    ctx.add("carrier_stall_near_extraction", 6, f"Carrier {carrier.get('soldier_id')} stalled near extraction for {flat_count + 1} withdrawal frames at distance {dist}", frame_index=frame_index, tick=tick)
            else:
                flat_count = 0
        last_carrier_distance = dist


def analyze_terminal_endgame(frames: List[Dict[str, Any]], extraction_zone: set[Position], ctx: AuditContext) -> None:
    carrier_seq: List[Tuple[int, int, int, int]] = []
    recoveree_seq: List[Tuple[int, int, int, int]] = []
    for frame in frames:
        phase = str(frame.get("phase"))
        if phase not in {"withdrawal", "post_grenade"}:
            continue
        tick = int(frame.get("tick", -1))
        frame_index = int(frame.get("frame_index", -1))
        carrier = get_frame_carrier(frame)
        if carrier is not None and carrier.get("alive", True):
            cpos = pos_key(carrier)
            cdist = extraction_distance(cpos, extraction_zone)
            if cdist <= 3:
                carrier_seq.append((tick, cpos[0], cpos[1], frame_index))
        rec = frame.get("recoveree", {})
        if rec:
            rpos = pos_key(rec)
            rdist = extraction_distance(rpos, extraction_zone)
            if rdist <= 3 and rec.get("carried_by_id") is None and bool(rec.get("alive", True)):
                recoveree_seq.append((tick, rpos[0], rpos[1], frame_index))

    def inspect_sequence(seq: List[Tuple[int, int, int, int]], label: str) -> None:
        if len(seq) < 4:
            return
        for i in range(3, len(seq)):
            tail = seq[i - 3:i + 1]
            positions = [(t[1], t[2]) for t in tail]
            dists = [extraction_distance(p, extraction_zone) for p in positions]
            rows = {p[0] for p in positions}
            cols = [p[1] for p in positions]
            if len(set(dists)) == 1 and dists[0] <= 3 and len(set(positions)) >= 2:
                if len(rows) == 1 and len(set(positions)) == 2:
                    ctx.add("terminal_lateral_stall", 7, f"{label} is laterally churning near extraction with constant distance {dists[0]} over positions {positions}", frame_index=tail[-1][3], tick=tail[-1][0])
                elif len(set(positions)) >= 3:
                    ctx.add("terminal_neutral_churn", 6, f"{label} is churning among same-distance tiles near extraction. positions={positions} cols={cols} distance={dists[0]}", frame_index=tail[-1][3], tick=tail[-1][0])

    inspect_sequence(carrier_seq, "Carrier")
    inspect_sequence(recoveree_seq, "Recoveree")


def analyze_pairwise_symmetry(trace_data: Dict[str, Any], file_name: str, case_id: str, sim_version: str, map_mode: str) -> List[Finding]:
    findings: List[Finding] = []
    models = trace_data.get("models", {})
    pvpp = models.get("pvpp")
    base = models.get("baseline_self_preserving")
    if not pvpp or not base:
        return findings
    p_final = pvpp.get("final_summary", {})
    b_final = base.get("final_summary", {})
    p_policy = pvpp.get("chosen_policy")
    b_policy = base.get("chosen_policy")
    if bool(p_final.get("grenade_triggered")) != bool(b_final.get("grenade_triggered")):
        findings.append(Finding(file=file_name, case_id=case_id, sim_version=sim_version, map_mode=map_mode, model="pair", category="paired_mismatch", severity=8, message="PVPP and baseline disagree on whether grenade triggered"))
    if pvpp.get("governing_reason") != base.get("governing_reason"):
        findings.append(Finding(file=file_name, case_id=case_id, sim_version=sim_version, map_mode=map_mode, model="pair", category="governing_reason_mismatch", severity=4, message=f"Governing reason differs: pvpp={pvpp.get('governing_reason')} baseline={base.get('governing_reason')}"))
    p_candidates = {(e.get("actor_id"), e.get("actor_kind"), e.get("policy")) for e in pvpp.get("candidate_evaluations", [])}
    b_candidates = {(e.get("actor_id"), e.get("actor_kind"), e.get("policy")) for e in base.get("candidate_evaluations", [])}
    if p_candidates != b_candidates:
        only_p = sorted(p_candidates - b_candidates)[:5]
        only_b = sorted(b_candidates - p_candidates)[:5]
        findings.append(Finding(file=file_name, case_id=case_id, sim_version=sim_version, map_mode=map_mode, model="pair", category="candidate_space_mismatch", severity=7, message=f"Candidate policy sets differ. Only PVPP: {only_p}; Only baseline: {only_b}"))
    if p_policy and b_policy and p_policy.get("policy") == b_policy.get("policy") == "ABSORB_GRENADE":
        p_frames = pvpp.get("frames", [])
        trigger_frame = next((fr for fr in p_frames if fr.get("phase") == "grenade_trigger"), None)
        if trigger_frame:
            g = trigger_frame.get("grenade", {})
            if not g.get("locked_recoveree_on_center", False) and not g.get("locked_recoveree_adjacent", False):
                findings.append(Finding(file=file_name, case_id=case_id, sim_version=sim_version, map_mode=map_mode, model="pair", category="shared_absorb_without_recoveree_exposure", severity=5, message="Both models chose absorb even though recoveree was neither on-center nor adjacent at trigger"))
    return findings


def audit_single_model(file_name: str, case_id: str, sim_version: str, map_mode: str, model_name: str, model_payload: Dict[str, Any], extraction_zone: set[Position]) -> List[Finding]:
    ctx = AuditContext(file_name=file_name, case_id=case_id, sim_version=sim_version, map_mode=map_mode, model_name=model_name, candidate_evaluations=model_payload.get("candidate_evaluations", []), chosen_policy=model_payload.get("chosen_policy"))
    frames = model_payload.get("frames", [])
    final_summary = model_payload.get("final_summary", {})
    analyze_oscillation(frames, extraction_zone, ctx)
    analyze_movement_legality(frames, ctx)
    analyze_state_consistency(frames, ctx)
    analyze_grenade_logic(frames, final_summary, ctx)
    analyze_extraction_lane(frames, extraction_zone, ctx)
    analyze_progress(frames, extraction_zone, ctx)
    analyze_terminal_endgame(frames, extraction_zone, ctx)
    return ctx.findings


def load_trace_files(replay_dir: Path, limit: Optional[int]) -> List[Path]:
    files = sorted(replay_dir.glob("episode_*.json"))
    if limit is not None:
        files = files[:limit]
    return files


def filter_findings_by_mode(findings: List[Finding], mode: str) -> List[Finding]:
    if mode == INTERNAL_MODE:
        return findings
    return [finding for finding in findings if finding.category in PUBLIC_INCLUDED_CATEGORIES]



def worst_file_summaries(findings: List[Finding], top_n: int = 15) -> Dict[str, Any]:
    by_file: Dict[str, List[Finding]] = defaultdict(list)
    for finding in findings:
        by_file[finding.file].append(finding)
    totals: List[Tuple[str, int, int]] = []
    for file_name, items in by_file.items():
        totals.append((file_name, len(items), max(f.severity for f in items)))
    totals.sort(key=lambda x: (-x[1], -x[2], x[0]))
    worst_by_total = [{"file": file_name, "total_findings": total_count, "max_severity": max_severity} for file_name, total_count, max_severity in totals[:top_n]]
    category_by_file: Dict[str, Counter[str]] = defaultdict(Counter)
    for finding in findings:
        category_by_file[finding.file][finding.category] += 1
    worst_by_category: Dict[str, List[Dict[str, Any]]] = {}
    all_categories = sorted({f.category for f in findings})
    for category in all_categories:
        rows: List[Tuple[str, int, int]] = []
        for file_name, counter in category_by_file.items():
            if counter[category] > 0:
                max_sev = max(f.severity for f in by_file[file_name] if f.category == category)
                rows.append((file_name, counter[category], max_sev))
        rows.sort(key=lambda x: (-x[1], -x[2], x[0]))
        worst_by_category[category] = [{"file": file_name, "count": count, "max_severity": max_sev} for file_name, count, max_sev in rows[:5]]
    return {"worst_files_by_total_findings": worst_by_total, "worst_files_by_category": worst_by_category}


def build_breakdowns(findings: List[Finding], mode: str) -> Dict[str, Any]:
    by_scenario = Counter(f.case_id for f in findings)
    by_category = Counter(f.category for f in findings)
    by_severity = Counter(str(f.severity) for f in findings)
    result: Dict[str, Any] = {
        "findings_by_scenario": dict(by_scenario),
        "findings_by_category": dict(by_category),
        "findings_by_severity": dict(by_severity),
    }
    if mode == INTERNAL_MODE:
        by_model = Counter(f.model for f in findings)
        by_scenario_model_category: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        for f in findings:
            by_scenario_model_category[f.case_id][f.model][f.category] += 1
        result["findings_by_model"] = dict(by_model)
        result["findings_by_scenario_model_category"] = {
            scenario: {model: dict(category_counts) for model, category_counts in model_map.items()}
            for scenario, model_map in by_scenario_model_category.items()
        }
    return result


def build_text_report(summary: Dict[str, Any], top_findings: List[Finding]) -> str:
    mode = summary["mode"]
    lines: List[str] = []
    lines.append("=" * 96)
    lines.append("GRENADE REPLAY TRACE AUDIT REPORT")
    lines.append("=" * 96)
    if mode == INTERNAL_MODE:
        lines.append("Mode: internal")
    lines.append(f"Replay files scanned: {summary['files_scanned']}")
    lines.append(f"Model audits run: {summary['model_audits_run']}")
    lines.append(f"Total surfaced findings: {summary['total_findings']}")
    lines.append("")
    lines.append("*" * 96)
    if mode == PUBLIC_MODE:
        lines.append("PUBLIC DISCLAIMER")
        lines.append("This report surfaces benchmark-facing replay-integrity failures and severe replay-quality issues.")
        lines.append("Any surfaced finding should generally be treated as worth reviewing and fixing because it may")
        lines.append("affect benchmark credibility, replay integrity, or measured results.")
    else:
        lines.append("INTERNAL DISCLAIMER")
        lines.append("This report includes additional debugging diagnostics used for internal review.")
        lines.append("Many internal findings, especially motion-pattern heuristics, do not materially affect aggregate")
        lines.append("benchmark results and may appear on both PVPP and scalar traces. Some internal findings can reveal")
        lines.append("real architecture or trace-level differences and should be treated as diagnostics, not public evidence.")
    lines.append("*" * 96)
    lines.append("")
    lines.append("Severity guide:")
    lines.append("  Severity 1-3: minor review item")
    lines.append("  Severity 4-6: material issue worth inspection")
    lines.append("  Severity 7-8: serious integrity or replay-quality issue")
    lines.append("  Severity 9: hard failure / benchmark-threatening issue")
    lines.append("")
    lines.append("Findings by scenario:")
    for scenario, count in sorted(summary["findings_by_scenario"].items(), key=lambda x: (-x[1], x[0])):
        lines.append(f"  {scenario}: {count}")
    if not summary["findings_by_scenario"]:
        lines.append("  None.")
    lines.append("")
    if mode == INTERNAL_MODE:
        lines.append("Findings by model:")
        for model, count in sorted(summary.get("findings_by_model", {}).items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"  {model}: {count}")
        if not summary.get("findings_by_model"):
            lines.append("  None.")
        lines.append("")
    lines.append("Findings by category:")
    for category, count in sorted(summary["findings_by_category"].items(), key=lambda x: (-x[1], x[0])):
        lines.append(f"  {category}: {count}")
    if not summary["findings_by_category"]:
        lines.append("  None.")
    lines.append("")
    lines.append("Findings by severity:")
    for severity, count in sorted(summary["findings_by_severity"].items(), key=lambda x: int(x[0])):
        lines.append(f"  severity {severity}: {count}")
    if not summary["findings_by_severity"]:
        lines.append("  None.")
    lines.append("")
    if mode == INTERNAL_MODE:
        lines.append("Scenario / model / category breakdown:")
        for scenario in sorted(summary.get("findings_by_scenario_model_category", {})):
            lines.append(f"  {scenario}:")
            model_map = summary["findings_by_scenario_model_category"][scenario]
            for model in sorted(model_map):
                lines.append(f"    {model}:")
                for category, count in sorted(model_map[model].items(), key=lambda x: (-x[1], x[0])):
                    lines.append(f"      {category}: {count}")
        if not summary.get("findings_by_scenario_model_category"):
            lines.append("  None.")
        lines.append("")
    lines.append("Worst files by total findings:")
    worst_files = summary["worst_file_summary"]["worst_files_by_total_findings"]
    if worst_files:
        for row in worst_files:
            lines.append(f"  {row['file']}: total_findings={row['total_findings']} max_severity={row['max_severity']}")
    else:
        lines.append("  None.")
    lines.append("")
    lines.append("Worst files by category:")
    worst_by_category = summary["worst_file_summary"]["worst_files_by_category"]
    if worst_by_category:
        for category in sorted(worst_by_category):
            lines.append(f"  {category}:")
            rows = worst_by_category[category]
            if rows:
                for row in rows:
                    lines.append(f"    {row['file']}: count={row['count']} max_severity={row['max_severity']}")
            else:
                lines.append("    None.")
    else:
        lines.append("  None.")
    lines.append("")
    lines.append("Top findings:")
    for finding in top_findings:
        if mode == PUBLIC_MODE:
            prefix = f"[sev {finding.severity}] {finding.file} | case={finding.case_id} | {finding.category}"
        else:
            prefix = f"[sev {finding.severity}] {finding.file} | case={finding.case_id} | {finding.model} | {finding.category}"
        suffix = f" | frame={finding.frame_index}" if finding.frame_index is not None else ""
        suffix += f" | tick={finding.tick}" if finding.tick is not None else ""
        lines.append(prefix + suffix)
        lines.append(f"    {finding.message}")
    if not top_findings:
        lines.append("  No findings.")
    lines.append("")
    if mode == PUBLIC_MODE:
        lines.append("Interpretation note:")
        lines.append("  This audit is a repository-facing integrity and severe-quality check, not a benchmark metric.")
        lines.append("  Use the replay viewer to inspect surfaced files manually.")
    else:
        lines.append("Interpretation note:")
        lines.append("  This audit flags suspicious patterns. It does not prove every finding is a bug.")
        lines.append("  Start manual review with:")
        lines.append("    1. highest severity findings")
        lines.append("    2. worst files by total findings")
        lines.append("    3. repeated recoveree oscillation, terminal stall, and extraction-mouth issues")
    return "\n".join(lines) + "\n"


def write_csv_findings(path: Path, findings: Iterable[Finding], mode: str) -> None:
    if mode == INTERNAL_MODE:
        fieldnames = ["file", "case_id", "sim_version", "map_mode", "model", "category", "severity", "frame_index", "tick", "message"]
    else:
        fieldnames = ["file", "case_id", "sim_version", "map_mode", "category", "severity", "frame_index", "tick", "message"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for finding in findings:
            row = finding.to_dict()
            if mode != INTERNAL_MODE:
                row.pop("model", None)
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    replay_dir = Path(args.dir)
    if not replay_dir.exists():
        raise FileNotFoundError(f"Replay directory not found: {replay_dir}")
    files = load_trace_files(replay_dir, args.limit)
    if not files:
        raise FileNotFoundError(f"No replay files found in: {replay_dir}")
    raw_findings: List[Finding] = []
    model_audits_run = 0
    for path in files:
        trace_data = read_json(path)
        case_id, sim_version, map_mode = get_run_metadata(trace_data)
        extraction_zone = {(int(p["row"]), int(p["col"])) for p in trace_data.get("map", {}).get("extraction_zone", [])}
        models = trace_data.get("models", {})
        for model_name, payload in models.items():
            raw_findings.extend(audit_single_model(file_name=path.name, case_id=case_id, sim_version=sim_version, map_mode=map_mode, model_name=model_name, model_payload=payload, extraction_zone=extraction_zone))
            model_audits_run += 1
        raw_findings.extend(analyze_pairwise_symmetry(trace_data=trace_data, file_name=path.name, case_id=case_id, sim_version=sim_version, map_mode=map_mode))
    surfaced_findings = filter_findings_by_mode(raw_findings, args.mode)
    breakdowns = build_breakdowns(surfaced_findings, args.mode)
    worst_summary = worst_file_summaries(surfaced_findings, top_n=15)
    top_findings = sorted(surfaced_findings, key=lambda f: (-f.severity, f.case_id, f.file, f.model, f.category, f.tick if f.tick is not None else -1))[: args.top]
    summary = {
        "mode": args.mode,
        "files_scanned": len(files),
        "model_audits_run": model_audits_run,
        "total_findings": len(surfaced_findings),
        **breakdowns,
        "worst_file_summary": worst_summary,
        "top_findings": [f.to_dict() for f in top_findings],
        "all_findings": [f.to_dict() for f in surfaced_findings],
    }
    json_out = Path(args.json_out)
    text_out = Path(args.text_out)
    csv_out = Path(args.csv_out)
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with open(text_out, "w", encoding="utf-8") as f:
        f.write(build_text_report(summary, top_findings))
    write_csv_findings(csv_out, surfaced_findings, args.mode)
    if args.print_top:
        print(build_text_report(summary, top_findings))
    if args.mode == INTERNAL_MODE:
        print("Audit complete. Mode: internal")
    else:
        print("Audit complete.")
    print(f"Files scanned: {len(files)}")
    print(f"Model audits run: {model_audits_run}")
    print(f"Surfaced findings: {len(surfaced_findings)}")
    print(f"JSON report: {json_out}")
    print(f"Text report: {text_out}")
    print(f"CSV findings: {csv_out}")


if __name__ == "__main__":
    main()
