from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Set, Tuple
import argparse
import csv
import json
import os
import random
import statistics
from datetime import datetime, timezone


SIM_VERSION = "V31.6d"
SIM_VERSION_LABEL = "MissionFlow V31.6d"
SIM_SCHEMA_VERSION = "replay_trace_sim3_v31_6d_terminal_lane_commitment"
SIM_FILENAME = "benchmark.py"

MODEL_PVPP = "pvpp"
MODEL_BASELINE_SELF = "baseline_self_preserving"

RECOVEREE_WOUNDED_NONAMBULATORY = "wounded_nonambulatory"
RECOVEREE_WOUNDED_AMBULATORY = "wounded_ambulatory"
RECOVEREE_INTACT_AMBULATORY = "intact_ambulatory"

MAP_MODE_LOCKED = "locked"
MAP_MODE_RANDOMIZED_COVER = "randomized_cover"

DEFAULT_OUTPUT_DIR = "data/grenade_missionflow_output_v3"
DEFAULT_REPLAY_DIR = "data/grenade_missionflow_replays_v3"
DEFAULT_RUNS = 100
DEFAULT_SEED = 42
DEFAULT_MAX_TICKS = 56
DEFAULT_ENEMY_FIRE_SCALE = 1.0
DEFAULT_GRENADE_TRIGGER_PROB = 0.30
DEFAULT_RECOVEREE_CONDITION = RECOVEREE_WOUNDED_NONAMBULATORY
DEFAULT_MAP_MODE = MAP_MODE_LOCKED
DEFAULT_COVER_PERTURB_PROB = 0.15
DEFAULT_SCALAR_FAVOR_MODE = "off"
DEFAULT_SCREEN_BONUS_STRENGTH = 0.35
DEFAULT_CLUSTER_PENALTY_STRENGTH = 0.15

ENEMY_FIRE_PRESETS = {
    "none": 0.0,
    "light": 0.5,
    "moderate": 1.0,
    "heavy": 1.5,
    "very_heavy": 3.0,
}

GRENADE_FREQUENCY_PRESETS = {
    "none": 0.0,
    "rare": 0.10,
    "sometimes": 0.30,
    "often": 0.60,
    "always": 1.0,
}

RECOVEREE_CONDITION_PRESETS = {
    "healthy": RECOVEREE_INTACT_AMBULATORY,
    "slightly_wounded": RECOVEREE_WOUNDED_AMBULATORY,
    "severely_wounded": RECOVEREE_WOUNDED_NONAMBULATORY,
}

MAP_MODE_PRESETS = {
    "fixed": MAP_MODE_LOCKED,
    "varied": MAP_MODE_RANDOMIZED_COVER,
}

SCENARIO_PRESETS = {
    "extraction_easy": {
        "enemy_fire": "none",
        "grenade_frequency": "none",
        "recoveree_condition": "healthy",
    },
    "extraction_moderate": {
        "enemy_fire": "light",
        "grenade_frequency": "sometimes",
        "recoveree_condition": "slightly_wounded",
    },
    "extraction_difficult": {
        "enemy_fire": "moderate",
        "grenade_frequency": "often",
        "recoveree_condition": "severely_wounded",
    },
    "extraction_improbable": {
        "enemy_fire": "very_heavy",
        "grenade_frequency": "always",
        "recoveree_condition": "severely_wounded",
    },
}

ABSORB_ADVANTAGE_THRESHOLD = 2


COVER_CONFIG = {
    "movement_rank": {
        "NONE": 3,
        "PARTIAL": 1,
        "STRONG": 0,
    },
    "exposure_from_cover": {
        "NONE": "HIGH",
        "PARTIAL": "MODERATE",
        "STRONG": "LOW",
    },
    "fire_modifier": {
        "NONE": 1.00,
        "PARTIAL": 0.50,
        "STRONG": 0.10,
    },
    "withdrawal_cover_override_threshold": 2,
    "withdrawal_open_penalty": 3,
    "advance_open_penalty": 2,
}


BASE_RAW_ROWS = {
    1:  [(0, "N"), (0, "N"), (0, "N"), (0, "N"), (0, "N"), (1, "N"), (2, "N"), (2, "P"), (3, "P"), (3, "N"), (3, "P"), (2, "P")],
    2:  [(0, "N"), (0, "N"), (0, "N"), (0, "N"), (0, "P"), (1, "N"), (2, "N"), (2, "N"), (3, "N"), (3, "P"), (3, "N"), (2, "N")],
    3:  [(0, "N"), (0, "N"), (0, "N"), (0, "P"), (1, "P"), (1, "N"), (2, "N"), (2, "P"), (3, "P"), (3, "N"), (3, "P"), (2, "N")],
    4:  [(0, "N"), (0, "N"), (0, "N"), (0, "N"), (1, "N"), (1, "N"), (2, "N"), (2, "S"), (2, "P"), (2, "N"), (2, "P"), (2, "N")],
    5:  [(0, "N"), (0, "N"), (0, "P"), (0, "N"), (1, "P"), (1, "N"), (1, "S"), (2, "P"), (2, "N"), (2, "N"), (2, "P"), (1, "N")],
    6:  [(0, "N"), (0, "P"), (0, "N"), (0, "N"), (1, "N"), (1, "P"), (1, "N"), (2, "N"), (2, "S"), (2, "P"), (1, "N"), (1, "N")],
    7:  [(0, "N"), (0, "N"), (0, "N"), (0, "P"), (1, "N"), (1, "N"), (1, "P"), (1, "N"), (2, "P"), (1, "S"), (1, "N"), (1, "N")],
    8:  [(0, "P"), (0, "N"), (0, "N"), (0, "N"), (0, "P"), (1, "N"), (1, "N"), (1, "S"), (1, "N"), (1, "N"), (1, "P"), (0, "N")],
    9:  [(0, "N"), (0, "N"), (0, "P"), (0, "N"), (0, "N"), (0, "P"), (1, "N"), (1, "S"), (1, "P"), (1, "N"), (0, "N"), (0, "N")],
    10: [(0, "N"), (0, "N"), (0, "N"), (0, "P"), (0, "N"), (0, "N"), (0, "P"), (0, "N"), (0, "S"), (0, "P"), (0, "N"), (0, "N")],
    11: [(0, "N"), (0, "P"), (0, "N"), (0, "N"), (0, "N"), (0, "P"), (0, "N"), (0, "S"), (0, "P"), (0, "N"), (0, "N"), (0, "N")],
    12: [(0, "N"), (0, "N"), (0, "N"), (0, "P"), (0, "N"), (0, "N"), (0, "N"), (0, "P"), (0, "N"), (0, "N"), (0, "N"), (0, "N")],
}


class CoverType(Enum):
    NONE = "N"
    PARTIAL = "P"
    STRONG = "S"


class IntegrityState(Enum):
    INTACT = "I2"
    WOUNDED = "I1"
    DEAD = "I0"


class MobilityState(Enum):
    FULL = "M3"
    REDUCED = "M2"
    MINIMAL = "M1"
    NONE = "M0"


class ExposureState(Enum):
    HIGH = "E2"
    MODERATE = "E1"
    LOW = "E0"


class SquadViabilityState(Enum):
    SV3 = "SV3"
    SV2 = "SV2"
    SV1 = "SV1"
    SV0 = "SV0"


class MissionContinuityState(Enum):
    MC3 = "MC3"
    MC2 = "MC2"
    MC1 = "MC1"
    MC0 = "MC0"


class CohesionState(Enum):
    C3 = "C3"
    C2 = "C2"
    C1 = "C1"
    C0 = "C0"


class MissionPhase(Enum):
    ENTRY = auto()
    ADVANCE = auto()
    RECOVERY = auto()
    WITHDRAWAL = auto()
    COMPLETE = auto()
    FAILED = auto()


class GrenadeClass(Enum):
    A_IRRELEVANT = auto()
    B_ESCAPABLE = auto()
    C_DAMAGING_NON_GOVERNING = auto()
    D_COMPRESSED_GOVERNING = auto()


class PolicyType(Enum):
    HOLD = auto()
    ADVANCE = auto()
    SCATTER = auto()
    OBJECTIVE_CARRIER_ESCAPE = auto()
    ABSORB_GRENADE = auto()
    SUPPORT_WITHDRAWAL = auto()
    REACQUIRE_RECOVEREE = auto()


class PaceMode(Enum):
    REST = "REST"
    WALK = "WALK"
    DOUBLE_TIME = "DOUBLE_TIME"
    SPRINT = "SPRINT"


@dataclass(frozen=True)
class Position:
    row: int
    col: int

    def manhattan(self, other: "Position") -> int:
        return abs(self.row - other.row) + abs(self.col - other.col)


@dataclass
class Tile:
    pos: Position
    height: int
    cover: CoverType


@dataclass
class Soldier:
    soldier_id: str
    pos: Position
    integrity: IntegrityState = IntegrityState.INTACT
    mobility: MobilityState = MobilityState.FULL
    exposure: ExposureState = ExposureState.HIGH
    is_carrying_recoveree: bool = False
    last_action: Optional[PolicyType] = None
    last_pace: Optional[PaceMode] = None
    recovery_ticks_remaining: int = 0
    previous_pos: Optional[Position] = None
    pre_previous_pos: Optional[Position] = None
    consecutive_double_time_bursts: int = 0
    sprint_cooldown: int = 0

    @property
    def alive(self) -> bool:
        return self.integrity != IntegrityState.DEAD

    @property
    def wounded(self) -> bool:
        return self.integrity == IntegrityState.WOUNDED


@dataclass
class RecovereeState:
    pos: Position
    integrity: IntegrityState = IntegrityState.WOUNDED
    mobility: MobilityState = MobilityState.REDUCED
    exposure: ExposureState = ExposureState.HIGH
    recovered: bool = False
    extracted: bool = False
    carried_by_id: Optional[str] = None
    dragged_by_id: Optional[str] = None
    assigned_helper_id: Optional[str] = None
    can_self_move: bool = True
    requires_assist: bool = False
    last_action: Optional[str] = None
    last_pace: Optional[PaceMode] = None
    previous_pos: Optional[Position] = None
    pre_previous_pos: Optional[Position] = None

    @property
    def alive(self) -> bool:
        return self.integrity != IntegrityState.DEAD

    @property
    def wounded(self) -> bool:
        return self.integrity == IntegrityState.WOUNDED


@dataclass
class GrenadeEvent:
    pos: Position
    active: bool = True
    grenade_class: Optional[GrenadeClass] = None
    has_detonated: bool = False
    absorb_actor_id: Optional[str] = None
    absorb_actor_kind: Optional[str] = None
    absorb_intercept_pos: Optional[Position] = None
    triggered_tick: Optional[int] = None
    intended_target_id: Optional[str] = None
    intended_target_kind: Optional[str] = None
    intended_target_pos: Optional[Position] = None
    scatter_kind: Optional[str] = None
    locked_center_ids: List[str] = field(default_factory=list)
    locked_adjacent_ids: List[str] = field(default_factory=list)
    locked_positions_by_id: Dict[str, Position] = field(default_factory=dict)
    locked_recoveree_on_center: bool = False
    locked_recoveree_adjacent: bool = False
    locked_recoveree_pos: Optional[Position] = None


@dataclass
class SquadState:
    soldiers: Dict[str, Soldier]
    recoveree: RecovereeState
    mission_phase: MissionPhase = MissionPhase.ENTRY
    squad_viability: SquadViabilityState = SquadViabilityState.SV3
    mission_continuity: MissionContinuityState = MissionContinuityState.MC3
    cohesion: CohesionState = CohesionState.C3
    grenade: Optional[GrenadeEvent] = None
    case_id: str = "UNKNOWN"
    tick: int = 0
    post_absorb_protection_ticks: int = 0
    terminal_reason: Optional[str] = None
    terminal_lane_col: Optional[int] = None

    def alive_soldiers(self) -> List[Soldier]:
        return [s for s in self.soldiers.values() if s.alive]

    def wounded_soldiers(self) -> List[Soldier]:
        return [s for s in self.alive_soldiers() if s.wounded]

    def carrier(self) -> Optional[Soldier]:
        if not self.recoveree.carried_by_id:
            return None
        return self.soldiers.get(self.recoveree.carried_by_id)

    def recoveree_mobile(self) -> bool:
        rec = self.recoveree
        if not rec.alive:
            return False
        if rec.carried_by_id is not None or rec.dragged_by_id is not None:
            return False
        if not rec.can_self_move:
            return False
        return rec.mobility in (MobilityState.FULL, MobilityState.REDUCED, MobilityState.MINIMAL)


@dataclass
class CandidatePolicy:
    actor_id: str
    actor_kind: str
    policy_type: PolicyType


@dataclass
class PolicyEvaluation:
    candidate: CandidatePolicy
    feasible: bool
    feasibility_reason: str
    adequate: bool
    adequacy_reason: str


@dataclass(frozen=True)
class RuntimeConfig:
    runs: int
    seed: int
    case_mode: str
    enemy_fire_scale: float
    max_ticks: int
    grenade_trigger_prob: float
    output_dir: str
    replay_dir: str
    write_replay_trace: bool
    recoveree_condition: str
    map_mode: str
    map_seed: Optional[int]
    cover_perturb_prob: float
    scalar_favor_mode: str
    screen_bonus_strength: float
    cluster_penalty_strength: float
    detailed_output: bool


class MapModel:
    def __init__(self, tiles: Dict[Tuple[int, int], Tile], objective_zone: Set[Position], extraction_zone: Set[Position]) -> None:
        self.tiles = tiles
        self.objective_zone = objective_zone
        self.extraction_zone = extraction_zone
        self.rows = 12
        self.cols = 12
        self.extraction_boundary = self._build_extraction_boundary()

    def get_tile(self, pos: Position) -> Tile:
        return self.tiles[(pos.row, pos.col)]

    def in_bounds(self, pos: Position) -> bool:
        return 1 <= pos.row <= self.rows and 1 <= pos.col <= self.cols

    def orthogonal_neighbors(self, pos: Position) -> List[Position]:
        candidates = [
            Position(pos.row - 1, pos.col),
            Position(pos.row + 1, pos.col),
            Position(pos.row, pos.col - 1),
            Position(pos.row, pos.col + 1),
        ]
        return [p for p in candidates if self.in_bounds(p)]

    def all_neighbors_8(self, pos: Position) -> List[Position]:
        out: List[Position] = []
        for dr in (-1, 0, 1):
            for dc in (-1, 0, 1):
                if dr == 0 and dc == 0:
                    continue
                p = Position(pos.row + dr, pos.col + dc)
                if self.in_bounds(p):
                    out.append(p)
        return out

    def distance2_ring(self, pos: Position) -> List[Position]:
        out: List[Position] = []
        for r in range(1, self.rows + 1):
            for c in range(1, self.cols + 1):
                p = Position(r, c)
                if pos.manhattan(p) == 2:
                    out.append(p)
        return out

    def movement_cost(self, origin: Position, dest: Position) -> int:
        if dest not in self.orthogonal_neighbors(origin):
            raise ValueError("Non-adjacent move")
        origin_height = self.get_tile(origin).height
        dest_height = self.get_tile(dest).height
        delta = dest_height - origin_height
        if abs(delta) > 2:
            raise ValueError("Height jump > 2")
        return 1 + max(0, delta)

    def in_extraction(self, pos: Position) -> bool:
        return pos in self.extraction_zone

    def _build_extraction_boundary(self) -> Set[Position]:
        boundary: Set[Position] = set()
        for ex in self.extraction_zone:
            for nbr in self.orthogonal_neighbors(ex):
                if nbr not in self.extraction_zone:
                    boundary.add(nbr)
        return boundary

    def touches_extraction_boundary(self, pos: Position) -> bool:
        return pos in self.extraction_boundary

    def extraction_success(self, pos: Position) -> bool:
        return self.in_extraction(pos) or self.touches_extraction_boundary(pos)

    def in_objective_zone(self, pos: Position) -> bool:
        return pos in self.objective_zone

    def nearest_extraction_distance(self, pos: Position) -> int:
        return min(pos.manhattan(ex) for ex in self.extraction_zone)

    def nearest_objective_distance(self, pos: Position) -> int:
        return min(pos.manhattan(obj) for obj in self.objective_zone)

    def nearest_extraction_success_distance(self, pos: Position) -> int:
        candidates = set(self.extraction_zone) | set(self.extraction_boundary)
        return min(pos.manhattan(ex) for ex in candidates)

    def nearest_extraction_zone_distance(self, pos: Position) -> int:
        return min(pos.manhattan(ex) for ex in self.extraction_zone)


def base_extraction_zone() -> Set[Position]:
    return {
        Position(10, 1), Position(10, 2), Position(10, 3),
        Position(11, 1), Position(11, 2), Position(11, 3),
        Position(12, 1), Position(12, 2), Position(12, 3),
    }


def base_objective_zone() -> Set[Position]:
    return {
        Position(1, 9), Position(1, 10), Position(1, 11),
        Position(2, 9), Position(2, 10), Position(2, 11),
        Position(3, 9), Position(3, 10), Position(3, 11),
    }


def build_base_raw_rows_copy() -> Dict[int, List[Tuple[int, str]]]:
    return {row: list(values) for row, values in BASE_RAW_ROWS.items()}


def perturb_cover_layout(
    raw_rows: Dict[int, List[Tuple[int, str]]],
    rng: random.Random,
    perturb_prob: float,
) -> None:
    extraction_zone = base_extraction_zone()
    objective_zone = base_objective_zone()

    for row in range(1, 13):
        for col in range(1, 13):
            pos = Position(row, col)
            if pos in extraction_zone or pos in objective_zone:
                continue

            height, cover_code = raw_rows[row][col - 1]
            if cover_code == "S":
                continue
            if rng.random() >= perturb_prob:
                continue

            if cover_code == "N":
                raw_rows[row][col - 1] = (height, "P")
            elif cover_code == "P":
                raw_rows[row][col - 1] = (height, "N")


def build_map_from_raw_rows(raw_rows: Dict[int, List[Tuple[int, str]]]) -> MapModel:
    cover_map = {"N": CoverType.NONE, "P": CoverType.PARTIAL, "S": CoverType.STRONG}
    tiles: Dict[Tuple[int, int], Tile] = {}
    for row in range(1, 13):
        for col in range(1, 13):
            height, cover_code = raw_rows[row][col - 1]
            tiles[(row, col)] = Tile(Position(row, col), height, cover_map[cover_code])

    return MapModel(tiles, base_objective_zone(), base_extraction_zone())


def build_map_for_episode(config: RuntimeConfig, case_id: str, episode_index: int, episode_seed: int) -> Tuple[MapModel, str, Optional[int]]:
    raw_rows = build_base_raw_rows_copy()
    if config.map_mode == MAP_MODE_LOCKED:
        return build_map_from_raw_rows(raw_rows), MAP_MODE_LOCKED, None

    base_map_seed = config.map_seed if config.map_seed is not None else config.seed
    derived_map_seed = (
        base_map_seed
        + 1009 * episode_index
        + 9173 * episode_seed
        + 37 * ord(case_id[0])
    ) % (2**31 - 1)

    rng = random.Random(derived_map_seed)
    perturb_cover_layout(raw_rows, rng, config.cover_perturb_prob)
    return build_map_from_raw_rows(raw_rows), MAP_MODE_RANDOMIZED_COVER, derived_map_seed


def base_start_soldiers() -> Dict[str, Soldier]:
    return {
        "S1": Soldier("S1", Position(12, 1)),
        "S2": Soldier("S2", Position(12, 2)),
        "S3": Soldier("S3", Position(11, 1)),
        "S4": Soldier("S4", Position(11, 2)),
        "S5": Soldier("S5", Position(10, 2)),
    }


def build_recoveree_from_condition(condition: str) -> RecovereeState:
    if condition == RECOVEREE_WOUNDED_NONAMBULATORY:
        return RecovereeState(
            pos=Position(2, 10),
            integrity=IntegrityState.WOUNDED,
            mobility=MobilityState.NONE,
            can_self_move=False,
            requires_assist=True,
        )
    if condition == RECOVEREE_WOUNDED_AMBULATORY:
        return RecovereeState(
            pos=Position(2, 10),
            integrity=IntegrityState.WOUNDED,
            mobility=MobilityState.REDUCED,
            can_self_move=True,
            requires_assist=False,
        )
    if condition == RECOVEREE_INTACT_AMBULATORY:
        return RecovereeState(
            pos=Position(2, 10),
            integrity=IntegrityState.INTACT,
            mobility=MobilityState.FULL,
            can_self_move=True,
            requires_assist=False,
        )
    raise ValueError(f"Unknown recoveree condition: {condition}")


def build_case(case_id: str, recoveree_condition: str) -> SquadState:
    soldiers = base_start_soldiers()
    recoveree = build_recoveree_from_condition(recoveree_condition)
    return SquadState(soldiers=soldiers, recoveree=recoveree, case_id=case_id)


def cover_name(cover: CoverType) -> str:
    return "NONE" if cover == CoverType.NONE else "PARTIAL" if cover == CoverType.PARTIAL else "STRONG"


def movement_cover_rank(cover: CoverType) -> int:
    return COVER_CONFIG["movement_rank"][cover_name(cover)]


def cover_exposure_weight(exposure: ExposureState) -> float:
    if exposure == ExposureState.HIGH:
        return 1.0
    if exposure == ExposureState.MODERATE:
        return 0.7
    return 0.35


def kill_soldier(soldier: Soldier) -> None:
    soldier.integrity = IntegrityState.DEAD
    soldier.mobility = MobilityState.NONE
    soldier.recovery_ticks_remaining = 0
    soldier.consecutive_double_time_bursts = 0
    soldier.sprint_cooldown = 0
    soldier.is_carrying_recoveree = False


def kill_recoveree(recoveree: RecovereeState) -> None:
    recoveree.integrity = IntegrityState.DEAD
    recoveree.mobility = MobilityState.NONE
    recoveree.carried_by_id = None
    recoveree.dragged_by_id = None
    recoveree.assigned_helper_id = None
    recoveree.extracted = False


def apply_nonfatal_damage_soldier(soldier: Soldier) -> str:
    if soldier.integrity == IntegrityState.INTACT:
        soldier.integrity = IntegrityState.WOUNDED
        soldier.mobility = MobilityState.REDUCED
        return "wounded"
    if soldier.integrity == IntegrityState.WOUNDED:
        kill_soldier(soldier)
        return "killed"
    return "dead_already"


def apply_nonfatal_damage_recoveree(recoveree: RecovereeState) -> str:
    if recoveree.integrity == IntegrityState.INTACT:
        recoveree.integrity = IntegrityState.WOUNDED
        if recoveree.can_self_move:
            recoveree.mobility = MobilityState.REDUCED
        return "wounded"
    if recoveree.integrity == IntegrityState.WOUNDED:
        kill_recoveree(recoveree)
        return "killed"
    return "dead_already"


def refresh_soldier(soldier: Soldier, game_map: MapModel) -> None:
    if soldier.integrity == IntegrityState.DEAD:
        soldier.mobility = MobilityState.NONE
        soldier.recovery_ticks_remaining = 0
        soldier.consecutive_double_time_bursts = 0
        soldier.sprint_cooldown = 0
    elif soldier.integrity == IntegrityState.WOUNDED:
        soldier.mobility = MobilityState.REDUCED
    else:
        soldier.mobility = MobilityState.FULL

    if soldier.is_carrying_recoveree and soldier.mobility == MobilityState.FULL:
        soldier.mobility = MobilityState.REDUCED

    tile = game_map.get_tile(soldier.pos)
    exposure_name = COVER_CONFIG["exposure_from_cover"][cover_name(tile.cover)]
    soldier.exposure = ExposureState.HIGH if exposure_name == "HIGH" else ExposureState.MODERATE if exposure_name == "MODERATE" else ExposureState.LOW


def refresh_recoveree(recoveree: RecovereeState, game_map: MapModel) -> None:
    if recoveree.integrity == IntegrityState.DEAD:
        recoveree.mobility = MobilityState.NONE
    elif recoveree.carried_by_id is not None:
        recoveree.mobility = MobilityState.MINIMAL
    elif recoveree.requires_assist or not recoveree.can_self_move:
        recoveree.mobility = MobilityState.NONE
    elif recoveree.integrity == IntegrityState.INTACT:
        recoveree.mobility = MobilityState.FULL
    else:
        recoveree.mobility = MobilityState.REDUCED

    tile = game_map.get_tile(recoveree.pos)
    exposure_name = COVER_CONFIG["exposure_from_cover"][cover_name(tile.cover)]
    recoveree.exposure = ExposureState.HIGH if exposure_name == "HIGH" else ExposureState.MODERATE if exposure_name == "MODERATE" else ExposureState.LOW


def refresh_state(state: SquadState, game_map: MapModel) -> None:
    for soldier in state.soldiers.values():
        refresh_soldier(soldier, game_map)
    refresh_recoveree(state.recoveree, game_map)


def decrement_recovery(state: SquadState) -> None:
    for soldier in state.soldiers.values():
        if soldier.recovery_ticks_remaining > 0:
            soldier.recovery_ticks_remaining -= 1
        if soldier.sprint_cooldown > 0:
            soldier.sprint_cooldown -= 1


def support_graph(state: SquadState) -> Dict[str, Set[str]]:
    graph: Dict[str, Set[str]] = {sid: set() for sid in state.soldiers}
    alive = state.alive_soldiers()
    for i, s1 in enumerate(alive):
        for s2 in alive[i + 1:]:
            if s1.pos.manhattan(s2.pos) <= 2:
                graph[s1.soldier_id].add(s2.soldier_id)
                graph[s2.soldier_id].add(s1.soldier_id)
    return graph


def update_squad_domains(state: SquadState) -> None:
    alive = state.alive_soldiers()
    wounded = state.wounded_soldiers()
    mobile = [s for s in alive if s.mobility not in (MobilityState.NONE, MobilityState.MINIMAL)]
    dead_count = 5 - len(alive)
    wounded_count = len(wounded)

    graph = support_graph(state)

    def dfs(start: str) -> Set[str]:
        stack = [start]
        comp: Set[str] = set()
        while stack:
            node = stack.pop()
            if node in comp:
                continue
            comp.add(node)
            stack.extend(graph[node] - comp)
        return comp

    largest_component = 0
    visited: Set[str] = set()
    for sid, soldier in state.soldiers.items():
        if not soldier.alive or sid in visited:
            continue
        comp = dfs(sid)
        visited |= comp
        largest_component = max(largest_component, len(comp))

    cohesion = CohesionState.C3 if largest_component >= 4 else CohesionState.C2 if largest_component >= 3 else CohesionState.C1 if largest_component >= 2 else CohesionState.C0
    if dead_count >= 1 and wounded_count >= 2 and cohesion == CohesionState.C3:
        cohesion = CohesionState.C2
    if dead_count >= 2 and cohesion in (CohesionState.C3, CohesionState.C2):
        cohesion = CohesionState.C1
    state.cohesion = cohesion

    if len(alive) >= 4 and len(mobile) >= 4 and cohesion == CohesionState.C3 and wounded_count <= 1:
        viability = SquadViabilityState.SV3
    elif len(alive) >= 3 and len(mobile) >= 2 and cohesion in (CohesionState.C3, CohesionState.C2):
        viability = SquadViabilityState.SV2
    elif len(alive) >= 2 and len(mobile) >= 1:
        viability = SquadViabilityState.SV1
    else:
        viability = SquadViabilityState.SV0
    state.squad_viability = viability

    rec = state.recoveree
    if not rec.alive:
        continuity = MissionContinuityState.MC0
    elif rec.extracted:
        continuity = MissionContinuityState.MC3
    elif state.squad_viability == SquadViabilityState.SV0 and not state.recoveree_mobile():
        continuity = MissionContinuityState.MC0
    elif rec.carried_by_id is not None:
        carrier = state.carrier()
        if carrier is not None and carrier.wounded and state.squad_viability == SquadViabilityState.SV1:
            continuity = MissionContinuityState.MC1
        else:
            continuity = MissionContinuityState.MC2
    elif rec.recovered and state.recoveree_mobile():
        continuity = MissionContinuityState.MC2
    elif rec.recovered:
        continuity = MissionContinuityState.MC1
    else:
        continuity = MissionContinuityState.MC3 if state.squad_viability in (SquadViabilityState.SV3, SquadViabilityState.SV2) else MissionContinuityState.MC1
    state.mission_continuity = continuity


def apply_post_event_caps(state: SquadState) -> None:
    grenade = state.grenade
    if grenade and grenade.has_detonated and grenade.absorb_actor_id is not None:
        if state.squad_viability == SquadViabilityState.SV3:
            state.squad_viability = SquadViabilityState.SV2
        if state.cohesion == CohesionState.C3:
            state.cohesion = CohesionState.C2


def count_dead_soldiers(state: SquadState) -> int:
    return sum(1 for s in state.soldiers.values() if s.integrity == IntegrityState.DEAD)


def count_wounded_soldiers(state: SquadState) -> int:
    return sum(1 for s in state.soldiers.values() if s.integrity == IntegrityState.WOUNDED)


def count_dead_total(state: SquadState) -> int:
    return count_dead_soldiers(state) + (1 if state.recoveree.integrity == IntegrityState.DEAD else 0)


def count_wounded_total(state: SquadState) -> int:
    return count_wounded_soldiers(state) + (1 if state.recoveree.integrity == IntegrityState.WOUNDED else 0)


def count_total_casualties(state: SquadState) -> int:
    return count_dead_total(state) + count_wounded_total(state)


def sacrifice_occurred(state: SquadState) -> bool:
    return bool(state.grenade and state.grenade.absorb_actor_id is not None)


def count_nearby_escorts(state: SquadState, carrier: Soldier, radius: int = 2) -> int:
    return sum(
        1
        for soldier in state.alive_soldiers()
        if soldier.soldier_id != carrier.soldier_id and soldier.pos.manhattan(carrier.pos) <= radius
    )


def reset_fatigue_for_pickup(soldier: Soldier) -> None:
    soldier.recovery_ticks_remaining = 0
    soldier.consecutive_double_time_bursts = 0
    soldier.sprint_cooldown = 0
    soldier.last_pace = PaceMode.REST


def ordinary_non_grenade_withdrawal(state: SquadState) -> bool:
    return state.mission_phase == MissionPhase.WITHDRAWAL and (state.grenade is None or not state.grenade.active)


def count_screen_escorts(state: SquadState, carrier: Soldier, radius: int = 2) -> int:
    return count_nearby_escorts(state, carrier, radius=radius)


def local_cluster_count(state: SquadState, anchor: Position, radius: int = 1) -> int:
    count = sum(1 for soldier in state.alive_soldiers() if soldier.pos.manhattan(anchor) <= radius)
    if state.recoveree.alive and state.recoveree.pos.manhattan(anchor) <= radius:
        count += 1
    return count


def screen_tradeoff_multiplier(state: SquadState, carrier: Optional[Soldier], config: RuntimeConfig) -> float:
    if config.scalar_favor_mode != "screen_tradeoff":
        return 1.0
    if not ordinary_non_grenade_withdrawal(state):
        return 1.0
    if carrier is None or not carrier.alive:
        return 1.0

    escorts = count_screen_escorts(state, carrier, radius=2)
    effective_escorts = min(escorts, 2)
    benefit = config.screen_bonus_strength * (effective_escorts / 2.0)

    cluster_count = local_cluster_count(state, carrier.pos, radius=1)
    cluster_excess = max(0, cluster_count - 3)
    penalty = config.cluster_penalty_strength * cluster_excess

    multiplier = (1.0 - benefit) + penalty
    return max(0.40, min(multiplier, 1.75))


def choose_best_screen_position_for_soldier(
    soldier: Soldier,
    desired_slot: Position,
    state: SquadState,
    game_map: MapModel,
) -> Tuple[Optional[Position], PaceMode, int]:
    occupied = {s.pos for s in state.soldiers.values() if s.alive and s.soldier_id != soldier.soldier_id}
    if state.recoveree.alive and state.recoveree.carried_by_id is None:
        occupied.add(state.recoveree.pos)

    best_pos: Optional[Position] = None
    best_pace = PaceMode.REST
    best_steps = 0
    best_rank: Optional[Tuple[int, ...]] = None

    for pace in (PaceMode.WALK, PaceMode.DOUBLE_TIME, PaceMode.SPRINT):
        budget = pace_budget_for_soldier(soldier, pace)
        if budget <= 0:
            continue
        positions = reachable_positions_with_cost(soldier.pos, budget, game_map, occupied)
        for pos, steps, spent in positions:
            if pos == soldier.pos:
                continue
            tile = game_map.get_tile(pos)
            c_rank = movement_cover_rank(tile.cover)
            slot_dist = pos.manhattan(desired_slot)
            extract_dist = game_map.nearest_extraction_success_distance(pos)
            density = local_density_penalty(pos, state, soldier.soldier_id)
            osc = oscillation_penalty(soldier.previous_pos, soldier.pre_previous_pos, pos, "withdraw")
            pace_penalty = 3 if pace == PaceMode.SPRINT else 1 if pace == PaceMode.DOUBLE_TIME else 0
            rank = (slot_dist, c_rank, extract_dist, density, osc, pace_penalty, -steps, spent, pos.row, pos.col)
            if best_rank is None or rank < best_rank:
                best_rank = rank
                best_pos = pos
                best_pace = pace
                best_steps = steps

    return best_pos, best_pace, best_steps


def pace_budget_for_soldier(soldier: Soldier, pace: PaceMode) -> int:
    if not soldier.alive or soldier.recovery_ticks_remaining > 0 or soldier.sprint_cooldown > 0:
        return 0

    carrier_penalty = 1 if soldier.is_carrying_recoveree else 0

    if soldier.integrity == IntegrityState.WOUNDED:
        if pace == PaceMode.WALK:
            return 1
        if pace == PaceMode.DOUBLE_TIME:
            return max(1, 2 - carrier_penalty)
        return 0

    if soldier.integrity == IntegrityState.INTACT:
        if pace == PaceMode.WALK:
            return 1
        if pace == PaceMode.DOUBLE_TIME:
            return max(1, 2 - carrier_penalty)
        if pace == PaceMode.SPRINT:
            return max(1, 3 - carrier_penalty)
    return 0


def pace_budget_for_recoveree(recoveree: RecovereeState, pace: PaceMode) -> int:
    if not recoveree.alive or not recoveree.can_self_move or recoveree.requires_assist:
        return 0
    if recoveree.carried_by_id is not None or recoveree.dragged_by_id is not None:
        return 0
    if recoveree.mobility == MobilityState.FULL:
        if pace == PaceMode.WALK:
            return 1
        if pace == PaceMode.DOUBLE_TIME:
            return 2
        return 0
    if recoveree.mobility == MobilityState.REDUCED:
        return 1 if pace == PaceMode.WALK else 0
    if recoveree.mobility == MobilityState.MINIMAL:
        return 1 if pace == PaceMode.WALK else 0
    return 0


def reachable_positions_with_cost(origin: Position, budget: int, game_map: MapModel, occupied: Set[Position]) -> List[Tuple[Position, int, int]]:
    frontier = [(origin, budget, 0)]
    seen = {(origin.row, origin.col, budget)}
    best_cost_by_pos: Dict[Tuple[int, int], int] = {(origin.row, origin.col): 0}

    while frontier:
        pos, remaining, spent = frontier.pop()
        for nxt in game_map.orthogonal_neighbors(pos):
            if nxt in occupied and nxt != origin:
                continue
            try:
                cost = game_map.movement_cost(pos, nxt)
            except ValueError:
                continue
            new_remaining = remaining - cost
            if new_remaining < 0:
                continue
            new_spent = spent + cost
            key = (nxt.row, nxt.col, new_remaining)
            if key not in seen:
                seen.add(key)
                pos_key = (nxt.row, nxt.col)
                if pos_key not in best_cost_by_pos or new_spent < best_cost_by_pos[pos_key]:
                    best_cost_by_pos[pos_key] = new_spent
                frontier.append((nxt, new_remaining, new_spent))

    out: List[Tuple[Position, int, int]] = []
    for (row, col), spent in best_cost_by_pos.items():
        pos = Position(row, col)
        steps = origin.manhattan(pos)
        out.append((pos, steps, spent))
    return out


def local_density_penalty(pos: Position, state: SquadState, exclude_id: str) -> int:
    penalty = sum(1 for s in state.alive_soldiers() if s.soldier_id != exclude_id and s.pos.manhattan(pos) <= 1)
    if state.recoveree.alive and state.recoveree.pos.manhattan(pos) <= 1 and exclude_id != "RECOVEREE":
        penalty += 1
    return penalty


def escort_slots(anchor_pos: Position, game_map: MapModel) -> List[Position]:
    candidates = [
        Position(anchor_pos.row, anchor_pos.col - 1),
        Position(anchor_pos.row, anchor_pos.col + 1),
        Position(anchor_pos.row + 1, anchor_pos.col),
        Position(anchor_pos.row - 1, anchor_pos.col),
    ]
    return [p for p in candidates if game_map.in_bounds(p)]


def oscillation_penalty(previous_pos: Optional[Position], pre_previous_pos: Optional[Position], pos: Position, phase: str) -> int:
    penalty = 0
    if previous_pos is not None and pos == previous_pos:
        penalty += 12 if phase == "withdraw" else 4
    if pre_previous_pos is not None and pos == pre_previous_pos:
        penalty += 4
    return penalty


def best_adjacent_cover_rank_for_advance(
    pos: Position,
    game_map: MapModel,
    occupied: Set[Position],
    origin: Position,
) -> int:
    ranks = [movement_cover_rank(game_map.get_tile(pos).cover)]
    for nbr in game_map.orthogonal_neighbors(pos):
        if nbr in occupied and nbr != origin:
            continue
        ranks.append(movement_cover_rank(game_map.get_tile(nbr).cover))
    return min(ranks)


def advance_departure_penalty(current_cover: CoverType, next_cover: CoverType) -> int:
    if current_cover == CoverType.STRONG and next_cover == CoverType.NONE:
        return 12
    if current_cover == CoverType.STRONG and next_cover == CoverType.PARTIAL:
        return 5
    if current_cover == CoverType.PARTIAL and next_cover == CoverType.NONE:
        return 4
    return 0


def advance_pickup_priority(
    pos: Position,
    soldier: Soldier,
    state: SquadState,
    game_map: MapModel,
) -> tuple[int, int, int]:
    rec = state.recoveree

    if not rec.alive or rec.recovered:
        zone_dist = game_map.nearest_objective_distance(pos)
        return (2, zone_dist, pos.manhattan(rec.pos))

    in_zone = 0 if game_map.in_objective_zone(pos) else 1
    if in_zone == 0:
        return (0, 0, pos.manhattan(rec.pos))

    zone_dist = game_map.nearest_objective_distance(pos)
    rec_dist = pos.manhattan(rec.pos)

    # Once a soldier is near the pickup area, objective acquisition should dominate cover preservation.
    near_pickup = (
        soldier.pos.manhattan(rec.pos) <= 4
        or game_map.nearest_objective_distance(soldier.pos) <= 2
        or zone_dist <= 1
    )
    if near_pickup:
        return (1, zone_dist, rec_dist)

    return (2, zone_dist, rec_dist)




def extraction_yield_tiles(game_map: MapModel) -> List[Position]:
    """
    Preferred supporter yield locations near home zone. These favor deeper blue
    squares and off-lane side positions so the carrier can claim the mouth/boundary.
    """
    boundary_tiles = list(game_map.extraction_boundary)
    extraction_tiles = [tile.pos for tile in game_map.tiles.values() if game_map.in_extraction(tile.pos)]
    side_buffer = [Position(r, c) for r in (8, 9, 10, 11, 12) for c in (4,) if Position(r, c) in game_map.tiles]

    def rank(pos: Position) -> tuple[int, ...]:
        deep_blue = 0 if (game_map.in_extraction(pos) and pos.row >= 11) else 1
        off_center = 0 if pos.col in (1, 3, 4) else 1
        in_extract = 0 if game_map.in_extraction(pos) else 1
        boundary_pref = 0 if pos in game_map.extraction_boundary else 1
        return (deep_blue, off_center, in_extract, boundary_pref, -pos.row, abs(pos.col - 2), pos.col)

    tiles = sorted(set(boundary_tiles + extraction_tiles + side_buffer), key=rank)
    return tiles



def choose_terminal_commit_lane(anchor_pos: Position, game_map: MapModel) -> int:
    candidates = [1, 2, 3]
    candidates.sort(key=lambda c: (abs(anchor_pos.col - c), 0 if c != 2 else 1, c))
    return candidates[0]


def get_terminal_commit_lane(state: SquadState, game_map: MapModel) -> int:
    if state.terminal_lane_col in (1, 2, 3):
        return int(state.terminal_lane_col)

    carrier = state.carrier()
    anchor = carrier.pos if carrier is not None and carrier.alive else state.recoveree.pos
    lane = choose_terminal_commit_lane(anchor, game_map)
    state.terminal_lane_col = lane
    return lane


def clear_terminal_commit_lane_if_needed(state: SquadState, game_map: MapModel) -> None:
    if state.mission_phase != MissionPhase.WITHDRAWAL:
        state.terminal_lane_col = None
        return

    carrier = state.carrier()
    if carrier is not None and carrier.alive:
        if game_map.nearest_extraction_success_distance(carrier.pos) > 3:
            state.terminal_lane_col = None
        return

    if state.recoveree_mobile():
        if game_map.nearest_extraction_success_distance(state.recoveree.pos) > 3:
            state.terminal_lane_col = None
    else:
        state.terminal_lane_col = None


def terminal_commit_reserved_tiles(game_map: MapModel, commit_lane: int) -> Set[Position]:
    reserved: Set[Position] = set()
    for row in (9, 10, 11, 12):
        pos = Position(row, commit_lane)
        if pos in game_map.tiles and game_map.extraction_success(pos):
            reserved.add(pos)
    return reserved


def terminal_mouth_tiles(game_map: MapModel) -> Set[Position]:
    mouth: Set[Position] = set()
    for row in (8, 9, 10):
        for col in (1, 2, 3):
            pos = Position(row, col)
            if pos in game_map.tiles:
                mouth.add(pos)
    return mouth


def choose_forced_extraction_entry(pos: Position, game_map: MapModel, occupied: Set[Position], preferred_lane: Optional[int] = None) -> Optional[Position]:
    candidates = []
    for nxt in game_map.orthogonal_neighbors(pos):
        if nxt in occupied:
            continue
        if not game_map.extraction_success(nxt):
            continue
        try:
            spent = game_map.movement_cost(pos, nxt)
        except ValueError:
            continue
        lane_pen = abs(nxt.col - preferred_lane) if preferred_lane is not None else 0
        boundary_pref = 0 if game_map.touches_extraction_boundary(nxt) else 1
        depth_pref = 0 if nxt.row >= pos.row else 1
        candidates.append((boundary_pref, lane_pen, spent, -nxt.row, abs(nxt.col - 2), nxt.col, nxt))
    if not candidates:
        return None
    candidates.sort()
    return candidates[0][-1]

def should_force_escort_yield(soldier: Soldier, state: SquadState, game_map: MapModel) -> bool:
    if soldier.is_carrying_recoveree or not soldier.alive:
        return False
    carrier = state.carrier()
    if carrier is None or not carrier.alive:
        return False
    carrier_dist = game_map.nearest_extraction_success_distance(carrier.pos)
    if carrier_dist > 4:
        return False
    commit_lane = get_terminal_commit_lane(state, game_map)
    reserved = terminal_commit_reserved_tiles(game_map, commit_lane)
    mouth = terminal_mouth_tiles(game_map)
    if soldier.pos in reserved or soldier.pos in mouth:
        return True
    if game_map.in_extraction(soldier.pos):
        return True
    if soldier.pos.manhattan(carrier.pos) <= 3 and game_map.nearest_extraction_success_distance(soldier.pos) <= 4:
        return True
    return False


def best_yield_tile_for_soldier(
    soldier: Soldier,
    state: SquadState,
    game_map: MapModel,
    occupied: set[Position],
) -> Optional[Position]:
    carrier = state.carrier()
    if carrier is None:
        return None

    current_dist = game_map.nearest_extraction_success_distance(soldier.pos)
    candidates: List[Tuple[Tuple[int, ...], Position]] = []
    carrier_col = carrier.pos.col
    for pos in extraction_yield_tiles(game_map):
        if pos == soldier.pos:
            continue
        if pos in occupied:
            continue
        dist = game_map.nearest_extraction_success_distance(pos)
        deeper = 0 if pos.row >= soldier.pos.row else 1
        side = 0 if pos.col in (1, 3) else 1
        away_from_carrier_lane = 0 if abs(pos.col - carrier_col) >= 1 else 1
        progress = 0 if dist <= current_dist else 1
        rank = (progress, deeper, side, away_from_carrier_lane, -pos.row, abs(pos.col - 2), pos.row, pos.col)
        candidates.append((rank, pos))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]





def anti_loop_penalty(
    previous_pos: Optional[Position],
    pre_previous_pos: Optional[Position],
    candidate_pos: Position,
    current_dist: int,
    candidate_dist: int,
    in_terminal_mode: bool = False,
) -> int:
    """
    Harder penalty for exact A-B-A reversals, especially when the move does not improve
    extraction distance. This targets the repeated recoveree/carrier loop class directly.
    """
    if previous_pos is None or pre_previous_pos is None:
        return 0
    if candidate_pos != pre_previous_pos:
        return 0

    # Exact snap-back to two-step-old square.
    if candidate_dist < current_dist:
        return 4 if in_terminal_mode else 2
    if candidate_dist == current_dist:
        return 500 if in_terminal_mode else 120
    return 2000 if in_terminal_mode else 600


def terminal_extraction_mode_active(state: SquadState, game_map: MapModel) -> bool:
    carrier = state.carrier()
    if carrier is not None and carrier.alive and state.recoveree.alive:
        return game_map.nearest_extraction_success_distance(carrier.pos) <= 4
    if carrier is None and state.recoveree_mobile():
        return game_map.nearest_extraction_success_distance(state.recoveree.pos) <= 4
    return False


def choose_terminal_carrier_move(
    soldier: Soldier,
    state: SquadState,
    game_map: MapModel,
) -> Tuple[Optional[Position], PaceMode, int]:
    occupied = {s.pos for s in state.soldiers.values() if s.alive and s.soldier_id != soldier.soldier_id}
    cur_zone_dist = game_map.nearest_extraction_zone_distance(soldier.pos)
    cur_success_dist = game_map.nearest_extraction_success_distance(soldier.pos)
    commit_lane = get_terminal_commit_lane(state, game_map)
    budget = pace_budget_for_soldier(soldier, PaceMode.WALK)
    positions = reachable_positions_with_cost(soldier.pos, budget, game_map, occupied)

    forced_entry = choose_forced_extraction_entry(soldier.pos, game_map, occupied, commit_lane)
    if forced_entry is not None:
        return forced_entry, PaceMode.WALK, 1

    forward: List[Tuple[Tuple[int, ...], Position, int]] = []
    neutral: List[Tuple[Tuple[int, ...], Position, int]] = []

    for pos, steps, spent in positions:
        if pos == soldier.pos:
            continue
        zone_dist = game_map.nearest_extraction_zone_distance(pos)
        success_dist = game_map.nearest_extraction_success_distance(pos)
        delta = cur_zone_dist - zone_dist
        if delta < 0:
            continue
        enters_extract = 0 if game_map.in_extraction(pos) else 1
        depth_bonus = 0 if pos.row >= soldier.pos.row else 1
        lane_pen = abs(pos.col - commit_lane)
        boundary_penalty = 1 if (not game_map.in_extraction(pos) and game_map.touches_extraction_boundary(pos)) else 0
        loop_pen = anti_loop_penalty(soldier.previous_pos, soldier.pre_previous_pos, pos, cur_zone_dist, zone_dist, True)
        no_reverse = 0 if success_dist <= cur_success_dist else 1
        exact_hold = 0 if pos == soldier.pos else 1
        rank = (enters_extract, zone_dist, no_reverse, lane_pen, boundary_penalty, loop_pen, success_dist, depth_bonus, spent, -pos.row, pos.col)
        if delta > 0 or game_map.in_extraction(pos):
            forward.append((rank, pos, steps))
        elif cur_success_dist > 1:
            neutral.append((rank, pos, steps))

    if forward:
        forward.sort(key=lambda x: x[0])
        chosen = forward[0]
        return chosen[1], PaceMode.WALK, chosen[2]
    if neutral:
        neutral.sort(key=lambda x: x[0])
        chosen = neutral[0]
        # Refuse neutral snap-back motion in terminal mode; holding is cleaner than churn.
        if soldier.pre_previous_pos is not None and chosen[1] == soldier.pre_previous_pos:
            return None, PaceMode.REST, 0
        return chosen[1], PaceMode.WALK, chosen[2]
    return None, PaceMode.REST, 0


def choose_terminal_recoveree_move(
    recoveree: RecovereeState,
    state: SquadState,
    game_map: MapModel,
) -> Tuple[Optional[Position], PaceMode, int]:
    occupied = {s.pos for s in state.soldiers.values() if s.alive}
    cur_zone_dist = game_map.nearest_extraction_zone_distance(recoveree.pos)
    cur_success_dist = game_map.nearest_extraction_success_distance(recoveree.pos)
    commit_lane = get_terminal_commit_lane(state, game_map)
    budgets = [PaceMode.WALK, PaceMode.DOUBLE_TIME] if recoveree.can_self_move else [PaceMode.WALK]

    forced_entry = choose_forced_extraction_entry(recoveree.pos, game_map, occupied, commit_lane)
    if forced_entry is not None:
        return forced_entry, PaceMode.WALK, 1

    forward: List[Tuple[Tuple[int, ...], Position, PaceMode, int]] = []
    neutral: List[Tuple[Tuple[int, ...], Position, PaceMode, int]] = []

    for pace in budgets:
        budget = pace_budget_for_recoveree(recoveree, pace)
        positions = reachable_positions_with_cost(recoveree.pos, budget, game_map, occupied)
        for pos, steps, spent in positions:
            if pos == recoveree.pos:
                continue
            zone_dist = game_map.nearest_extraction_zone_distance(pos)
            success_dist = game_map.nearest_extraction_success_distance(pos)
            delta = cur_zone_dist - zone_dist
            if delta < 0:
                continue
            enters_extract = 0 if game_map.in_extraction(pos) else 1
            depth_bonus = 0 if pos.row >= recoveree.pos.row else 1
            lane_pen = abs(pos.col - commit_lane)
            boundary_penalty = 1 if (not game_map.in_extraction(pos) and game_map.touches_extraction_boundary(pos)) else 0
            loop_pen = anti_loop_penalty(recoveree.previous_pos, recoveree.pre_previous_pos, pos, cur_zone_dist, zone_dist, True)
            no_reverse = 0 if success_dist <= cur_success_dist else 1
            rank = (enters_extract, zone_dist, no_reverse, lane_pen, boundary_penalty, loop_pen, success_dist, depth_bonus, spent, -pos.row, pos.col)
            if delta > 0 or game_map.in_extraction(pos):
                forward.append((rank, pos, pace, steps))
            elif cur_success_dist > 1:
                neutral.append((rank, pos, pace, steps))

    if forward:
        forward.sort(key=lambda x: x[0])
        chosen = forward[0]
        return chosen[1], chosen[2], chosen[3]
    if neutral:
        neutral.sort(key=lambda x: x[0])
        chosen = neutral[0]
        if recoveree.pre_previous_pos is not None and chosen[1] == recoveree.pre_previous_pos:
            return None, PaceMode.REST, 0
        return chosen[1], chosen[2], chosen[3]
    return None, PaceMode.REST, 0


def assign_terminal_yield_tiles(
    supporters: List[Soldier],
    state: SquadState,
    game_map: MapModel,
) -> Dict[str, Position]:
    carrier = state.carrier()
    if carrier is None:
        return {}

    commit_lane = get_terminal_commit_lane(state, game_map)
    occupied = {s.pos for s in state.soldiers.values() if s.alive and s.soldier_id not in {sup.soldier_id for sup in supporters}}
    occupied.add(carrier.pos)

    preferred_tiles = extraction_yield_tiles(game_map)
    assignments: Dict[str, Position] = {}

    ordered_supporters = sorted(
        supporters,
        key=lambda s: (
            0 if game_map.in_extraction(s.pos) else 1,
            game_map.nearest_extraction_success_distance(s.pos),
            s.soldier_id,
        ),
    )

    reserved = terminal_commit_reserved_tiles(game_map, commit_lane)
    mouth = terminal_mouth_tiles(game_map)
    available = [p for p in preferred_tiles if p not in occupied and p not in reserved and p not in mouth]
    for soldier in ordered_supporters:
        if not available:
            break
        available.sort(
            key=lambda p, sp=soldier.pos, cc=commit_lane: (
                0 if p.row >= 11 else 1,
                0 if abs(p.col - cc) >= 1 else 1,
                0 if p.col in (1, 3, 4) else 1,
                sp.manhattan(p),
                -p.row,
                p.col,
            )
        )
        assignments[soldier.soldier_id] = available.pop(0)

    return assignments


def choose_terminal_yield_move(
    soldier: Soldier,
    target: Position,
    state: SquadState,
    game_map: MapModel,
) -> Tuple[Optional[Position], PaceMode, int]:
    occupied = {s.pos for s in state.soldiers.values() if s.alive and s.soldier_id != soldier.soldier_id}
    carrier = state.carrier()
    commit_lane = get_terminal_commit_lane(state, game_map)
    reserved = terminal_commit_reserved_tiles(game_map, commit_lane)
    mouth = terminal_mouth_tiles(game_map)
    if carrier is not None:
        occupied.add(carrier.pos)

    cur_target_dist = soldier.pos.manhattan(target)
    cur_extract_dist = game_map.nearest_extraction_success_distance(soldier.pos)
    currently_blocking = soldier.pos in reserved or soldier.pos in mouth or game_map.in_extraction(soldier.pos)

    # If the supporter is already off the commit lane and outside extraction, hold.
    if (not currently_blocking) and (not game_map.in_extraction(soldier.pos)) and abs(soldier.pos.col - commit_lane) >= 1 and cur_target_dist <= 1:
        return None, PaceMode.REST, 0

    best: List[Tuple[Tuple[int, ...], Position, int, PaceMode]] = []

    for pace in (PaceMode.WALK, PaceMode.DOUBLE_TIME):
        budget = pace_budget_for_soldier(soldier, pace)
        if budget <= 0:
            continue
        positions = reachable_positions_with_cost(soldier.pos, budget, game_map, occupied)
        for pos, steps, spent in positions:
            if pos == soldier.pos:
                continue
            target_dist = pos.manhattan(target)
            extract_dist = game_map.nearest_extraction_success_distance(pos)
            progress_to_target = 0 if target_dist < cur_target_dist else 1
            leaves_reserved = 0 if (currently_blocking and pos not in reserved and pos not in mouth) else 1
            leaves_extract = 0 if (game_map.in_extraction(soldier.pos) and not game_map.in_extraction(pos)) else 1
            outside_extract = 0 if not game_map.in_extraction(pos) else 1
            deep_blue = 0 if (game_map.in_extraction(pos) and pos.row >= 11 and abs(pos.col - commit_lane) >= 1) else 1
            side_clear = 0 if abs(pos.col - commit_lane) >= 1 else 1
            mouth_clear = 0 if pos not in mouth else 1
            no_reverse = 0 if extract_dist <= cur_extract_dist else 1
            loop_pen = anti_loop_penalty(soldier.previous_pos, soldier.pre_previous_pos, pos, cur_extract_dist, extract_dist, True)
            pace_pen = 0 if pace == PaceMode.WALK else 1
            rank = (
                leaves_reserved,
                mouth_clear,
                side_clear,
                deep_blue,
                leaves_extract,
                outside_extract,
                progress_to_target,
                no_reverse,
                loop_pen,
                target_dist,
                pace_pen,
                spent,
                -pos.row,
                pos.col,
            )
            best.append((rank, pos, steps, pace))

    if best:
        best.sort(key=lambda x: x[0])
        chosen = best[0]
        chosen_extract_dist = game_map.nearest_extraction_success_distance(chosen[1])
        if (not currently_blocking) and chosen[1].manhattan(target) >= cur_target_dist and chosen_extract_dist > cur_extract_dist:
            return None, PaceMode.REST, 0
        return chosen[1], chosen[3], chosen[2]
    return None, PaceMode.REST, 0


def choose_best_position_for_soldier(
    soldier: Soldier,
    target_kind: str,
    state: SquadState,
    game_map: MapModel,
    support_target: Optional[Position] = None,
) -> Tuple[Optional[Position], PaceMode, int]:
    occupied = {s.pos for s in state.soldiers.values() if s.alive and s.soldier_id != soldier.soldier_id}
    if state.recoveree.alive and state.recoveree.carried_by_id is None:
        occupied.add(state.recoveree.pos)

    forced_yield = should_force_escort_yield(soldier, state, game_map)
    forced_yield_target = best_yield_tile_for_soldier(soldier, state, game_map, occupied) if forced_yield else None

    current_tile = game_map.get_tile(soldier.pos)
    current_cover = movement_cover_rank(current_tile.cover)
    allowed_paces = (PaceMode.WALK,) if soldier.is_carrying_recoveree else (PaceMode.WALK, PaceMode.DOUBLE_TIME, PaceMode.SPRINT)

    best_pos: Optional[Position] = None
    best_pace = PaceMode.REST
    best_steps = 0
    best_rank: Optional[Tuple[int, ...]] = None

    carrier_forward_candidates: List[Tuple[Tuple[int, ...], Position, PaceMode, int]] = []
    carrier_neutral_candidates: List[Tuple[Tuple[int, ...], Position, PaceMode, int]] = []
    carrier_all_candidates: List[Tuple[Tuple[int, ...], Position, PaceMode, int]] = []

    cur_success_dist_base = game_map.nearest_extraction_success_distance(soldier.pos)

    for pace in allowed_paces:
        budget = pace_budget_for_soldier(soldier, pace)
        if budget <= 0:
            continue
        positions = reachable_positions_with_cost(soldier.pos, budget, game_map, occupied)
        for pos, steps, spent in positions:
            if pos == soldier.pos:
                continue
            tile = game_map.get_tile(pos)
            c_rank = movement_cover_rank(tile.cover)
            density = local_density_penalty(pos, state, soldier.soldier_id)

            if target_kind == "advance":
                pickup_band, zone_dist, rec_dist = advance_pickup_priority(pos, soldier, state, game_map)
                corridor_rank = best_adjacent_cover_rank_for_advance(pos, game_map, occupied, soldier.pos)
                departure_penalty = advance_departure_penalty(current_tile.cover, tile.cover)
                open_penalty = 6 if tile.cover == CoverType.NONE else 1 if tile.cover == CoverType.PARTIAL else 0
                osc = oscillation_penalty(soldier.previous_pos, soldier.pre_previous_pos, pos, "advance")
                loop_pen = 0
                pace_penalty = 4 if pace == PaceMode.SPRINT and tile.cover == CoverType.NONE else 1 if pace == PaceMode.SPRINT else 0

                near_pickup_commit = (
                    soldier.pos.manhattan(state.recoveree.pos) <= 3
                    or pos.manhattan(state.recoveree.pos) <= 1
                    or zone_dist <= 1
                    or game_map.nearest_objective_distance(soldier.pos) <= 2
                )
                if near_pickup_commit:
                    departure_penalty = min(departure_penalty, 1)
                    open_penalty = 0 if rec_dist <= 1 or zone_dist == 0 else min(open_penalty, 1)
                    corridor_rank = min(corridor_rank, 1)
                    density_penalty = max(0, density - 1)
                    commit_bonus = 0 if rec_dist < soldier.pos.manhattan(state.recoveree.pos) else 1
                    rank = (
                        pickup_band,
                        zone_dist,
                        rec_dist,
                        commit_bonus,
                        spent,
                        c_rank,
                        density_penalty,
                        osc,
                        pace_penalty,
                        pos.row + pos.col,
                    )
                else:
                    rank = (
                        pickup_band,
                        zone_dist,
                        rec_dist,
                        c_rank,
                        corridor_rank,
                        departure_penalty,
                        open_penalty,
                        density,
                        osc,
                        pace_penalty,
                        spent,
                        pos.row + pos.col,
                    )

            elif target_kind == "reacquire":
                assert support_target is not None
                dist = pos.manhattan(support_target)
                cur_dist = soldier.pos.manhattan(support_target)
                progress = 0 if dist < cur_dist else 1
                osc = oscillation_penalty(soldier.previous_pos, soldier.pre_previous_pos, pos, "advance")
                pace_penalty = 3 if pace == PaceMode.SPRINT else 0
                rank = (progress, dist, c_rank, density, osc, pace_penalty, -steps, spent, pos.row, pos.col)

            elif target_kind == "withdraw":
                success_dist = game_map.nearest_extraction_success_distance(pos)
                cur_success_dist = game_map.nearest_extraction_success_distance(soldier.pos)
                delta = cur_success_dist - success_dist

                reverse_penalty = 100 if delta < 0 else 12 if delta == 0 else 0
                open_penalty = 8 if tile.cover == CoverType.NONE else 0
                cover_bonus_rank = c_rank
                density_penalty = density
                if soldier.is_carrying_recoveree:
                    reverse_penalty = 100 if delta < 0 else 8 if delta == 0 else 0
                    open_penalty = 14 if tile.cover == CoverType.NONE else 0
                    density_penalty = max(0, density - 1)

                osc = oscillation_penalty(soldier.previous_pos, soldier.pre_previous_pos, pos, "withdraw")
                loop_pen = anti_loop_penalty(soldier.previous_pos, soldier.pre_previous_pos, pos, cur_success_dist, success_dist, forced_yield or (soldier.is_carrying_recoveree and (cur_success_dist <= 3 or success_dist <= 2)))
                pace_penalty = 0 if pace == PaceMode.WALK else 10 if soldier.is_carrying_recoveree else 3 if pace == PaceMode.SPRINT else 1

                in_extract = 0 if game_map.in_extraction(pos) else 1
                if not soldier.is_carrying_recoveree and game_map.in_extraction(pos):
                    extraction_lane_penalty = 0 if pos.row >= 11 else 2
                    side_clearance_penalty = 0 if pos.col in (1, 3) else 1
                else:
                    extraction_lane_penalty = 0
                    side_clearance_penalty = 0

                if forced_yield and forced_yield_target is not None:
                    target_dist = pos.manhattan(forced_yield_target)
                    current_target_dist = soldier.pos.manhattan(forced_yield_target)
                    progress_to_yield = 0 if target_dist < current_target_dist else 1
                    must_be_extract = 0 if game_map.in_extraction(pos) else 1
                    deep_extract = 0 if game_map.in_extraction(pos) and pos.row >= 11 else 1
                    side_lane = 0 if pos.col in (1, 3) else 1
                    carrier = state.carrier()
                    carrier_col = carrier.pos.col if carrier is not None else 2
                    off_carrier_lane = 0 if abs(pos.col - carrier_col) >= 1 else 1
                    rank = (
                        progress_to_yield,
                        must_be_extract,
                        deep_extract,
                        side_lane,
                        off_carrier_lane,
                        loop_pen,
                        target_dist,
                        osc,
                        spent,
                        pos.row,
                        pos.col,
                    )
                else:
                    endgame_commit = cur_success_dist <= 3 or success_dist <= 2
                    if soldier.is_carrying_recoveree and endgame_commit:
                        reverse_penalty = 10000 if delta < 0 else 250 if delta == 0 else 0
                        open_penalty = 0 if success_dist <= 1 else min(open_penalty, 2)
                        density_penalty = 0
                        extraction_depth_bonus = 0 if pos.row >= soldier.pos.row else 1
                        centerline_penalty = 0 if pos.col <= 3 else 1
                        rank = (
                            success_dist,
                            in_extract,
                            reverse_penalty,
                            extraction_depth_bonus,
                            centerline_penalty,
                            loop_pen,
                            osc,
                            spent,
                            pos.row,
                            pos.col,
                        )
                        candidate = (rank, pos, pace, steps)
                        carrier_all_candidates.append(candidate)
                        if delta > 0 or game_map.in_extraction(pos):
                            carrier_forward_candidates.append(candidate)
                        elif delta == 0:
                            carrier_neutral_candidates.append(candidate)
                    elif (not soldier.is_carrying_recoveree) and endgame_commit:
                        extraction_depth_bonus = 0 if game_map.in_extraction(pos) and pos.row >= 11 else 1
                        side_clearance_penalty = 0 if pos.col in (1, 3) else 1
                        lane_block_penalty = 2 if pos.col == 2 and pos.row <= 11 else 0
                        must_enter_extract = 0 if game_map.in_extraction(pos) else 1
                        rank = (
                            must_enter_extract,
                            extraction_depth_bonus,
                            side_clearance_penalty,
                            lane_block_penalty,
                            reverse_penalty,
                            success_dist,
                            loop_pen,
                            osc,
                            cover_bonus_rank,
                            open_penalty,
                            density_penalty,
                            -steps,
                            spent,
                            pos.row,
                            pos.col,
                        )
                    else:
                        rank = (
                            in_extract,
                            reverse_penalty,
                            success_dist,
                            loop_pen,
                            extraction_lane_penalty,
                            side_clearance_penalty,
                            cover_bonus_rank,
                            open_penalty,
                            density_penalty,
                            osc,
                            pace_penalty,
                            -steps,
                            spent,
                            pos.row,
                            pos.col,
                        )

            elif target_kind == "support":
                assert support_target is not None
                slots = escort_slots(support_target, game_map)
                slot_dist = min((pos.manhattan(slot) for slot in slots), default=99)
                extract_dist = game_map.nearest_extraction_success_distance(pos)
                cur_extract = game_map.nearest_extraction_success_distance(soldier.pos)
                delta = cur_extract - extract_dist
                reverse_penalty = 100 if delta < 0 else 12 if delta == 0 else 0
                overstack_penalty = 1 if density >= 2 else 0
                osc = oscillation_penalty(soldier.previous_pos, soldier.pre_previous_pos, pos, "withdraw")
                pace_penalty = 3 if pace == PaceMode.SPRINT else 0
                support_open_penalty = 5 if tile.cover == CoverType.NONE else 0
                if game_map.nearest_extraction_success_distance(support_target) <= 3:
                    in_extract = 0 if game_map.in_extraction(pos) else 1
                    deep_extract = 0 if game_map.in_extraction(pos) and pos.row >= 11 else 1
                    side_lane = 0 if pos.col in (1, 3) else 1
                    rank = (
                        in_extract,
                        deep_extract,
                        side_lane,
                        reverse_penalty,
                        extract_dist,
                        slot_dist,
                        overstack_penalty,
                        density,
                        osc,
                        spent,
                        pos.row,
                        pos.col,
                    )
                else:
                    rank = (reverse_penalty, c_rank, support_open_penalty, extract_dist, slot_dist, overstack_penalty, density, osc, pace_penalty, -steps, spent, pos.row, pos.col)

            elif target_kind == "scatter":
                grenade = state.grenade
                assert grenade is not None
                blast_dist = pos.manhattan(grenade.pos)
                safe = 0 if blast_dist > 1 else 1
                osc = oscillation_penalty(soldier.previous_pos, soldier.pre_previous_pos, pos, "withdraw")
                rank = (safe, c_rank, density, osc, -blast_dist, -steps, spent, pos.row, pos.col)

            else:
                raise ValueError(f"Unknown target_kind: {target_kind}")

            if not (target_kind == "withdraw" and soldier.is_carrying_recoveree and cur_success_dist_base <= 3):
                if best_rank is None or rank < best_rank:
                    best_rank = rank
                    best_pos = pos
                    best_pace = pace
                    best_steps = steps

    if target_kind == "withdraw" and soldier.is_carrying_recoveree and cur_success_dist_base <= 3:
        if cur_success_dist_base <= 2:
            extraction_forward = [c for c in carrier_forward_candidates if game_map.in_extraction(c[1])]
            if extraction_forward:
                extraction_forward.sort(key=lambda x: x[0])
                chosen = extraction_forward[0]
                return chosen[1], chosen[2], chosen[3]
        if carrier_forward_candidates:
            carrier_forward_candidates.sort(key=lambda x: x[0])
            chosen = carrier_forward_candidates[0]
            return chosen[1], chosen[2], chosen[3]
        if carrier_neutral_candidates:
            carrier_neutral_candidates.sort(key=lambda x: x[0])
            chosen = carrier_neutral_candidates[0]
            return chosen[1], chosen[2], chosen[3]
        if carrier_all_candidates:
            carrier_all_candidates.sort(key=lambda x: x[0])
            chosen = carrier_all_candidates[0]
            return chosen[1], chosen[2], chosen[3]

    return best_pos, best_pace, best_steps


def choose_best_position_for_recoveree(recoveree: RecovereeState, state: SquadState, game_map: MapModel) -> Tuple[Optional[Position], PaceMode, int]:
    occupied = {s.pos for s in state.soldiers.values() if s.alive}
    current_tile = game_map.get_tile(recoveree.pos)
    current_cover = movement_cover_rank(current_tile.cover)

    best_pos: Optional[Position] = None
    best_pace = PaceMode.REST
    best_steps = 0
    best_rank: Optional[Tuple[int, ...]] = None

    forward_candidates: List[Tuple[Tuple[int, ...], Position, PaceMode, int]] = []
    neutral_candidates: List[Tuple[Tuple[int, ...], Position, PaceMode, int]] = []
    all_candidates: List[Tuple[Tuple[int, ...], Position, PaceMode, int]] = []

    cur_base_dist = game_map.nearest_extraction_success_distance(recoveree.pos)
    endgame_commit = cur_base_dist <= 3

    for pace in (PaceMode.WALK, PaceMode.DOUBLE_TIME):
        budget = pace_budget_for_recoveree(recoveree, pace)
        if budget <= 0:
            continue
        positions = reachable_positions_with_cost(recoveree.pos, budget, game_map, occupied)
        for pos, steps, spent in positions:
            if pos == recoveree.pos:
                continue
            tile = game_map.get_tile(pos)
            c_rank = movement_cover_rank(tile.cover)
            dist = game_map.nearest_extraction_success_distance(pos)
            cur_dist = game_map.nearest_extraction_success_distance(recoveree.pos)
            delta = cur_dist - dist
            reverse_penalty = 100 if delta < 0 else 12 if delta == 0 else 0
            open_penalty = 8 if tile.cover == CoverType.NONE else 0
            osc = oscillation_penalty(recoveree.previous_pos, recoveree.pre_previous_pos, pos, "withdraw")
            loop_pen = anti_loop_penalty(recoveree.previous_pos, recoveree.pre_previous_pos, pos, cur_dist, dist, endgame_commit)
            no_cover_improvement = 1 if c_rank > current_cover else 0
            pace_penalty = 2 if pace == PaceMode.DOUBLE_TIME else 0
            in_extract = 0 if game_map.in_extraction(pos) else 1

            if endgame_commit:
                reverse_penalty = 10000 if delta < 0 else 250 if delta == 0 else 0
                open_penalty = 0 if dist <= 1 else min(open_penalty, 2)
                rank = (dist, in_extract, reverse_penalty, loop_pen, osc, spent, pos.row, pos.col)
                candidate = (rank, pos, pace, steps)
                all_candidates.append(candidate)
                if delta > 0 or game_map.in_extraction(pos):
                    forward_candidates.append(candidate)
                elif delta == 0:
                    neutral_candidates.append(candidate)
            else:
                rank = (in_extract, reverse_penalty, loop_pen, c_rank, open_penalty, dist, no_cover_improvement, osc, pace_penalty, -steps, spent, pos.row, pos.col)
                if best_rank is None or rank < best_rank:
                    best_rank = rank
                    best_pos = pos
                    best_pace = pace
                    best_steps = steps

    if endgame_commit:
        if cur_base_dist <= 2:
            extraction_forward = [c for c in forward_candidates if game_map.in_extraction(c[1])]
            if extraction_forward:
                extraction_forward.sort(key=lambda x: x[0])
                chosen = extraction_forward[0]
                return chosen[1], chosen[2], chosen[3]
        if forward_candidates:
            forward_candidates.sort(key=lambda x: x[0])
            chosen = forward_candidates[0]
            return chosen[1], chosen[2], chosen[3]
        if neutral_candidates:
            neutral_candidates.sort(key=lambda x: x[0])
            chosen = neutral_candidates[0]
            return chosen[1], chosen[2], chosen[3]
        if all_candidates:
            all_candidates.sort(key=lambda x: x[0])
            chosen = all_candidates[0]
            return chosen[1], chosen[2], chosen[3]

    return best_pos, best_pace, best_steps


def apply_fatigue_from_realized_steps(soldier: Soldier, steps: int) -> None:
    if steps <= 0:
        soldier.last_pace = PaceMode.REST
        return

    if steps == 1:
        soldier.last_pace = PaceMode.WALK
        soldier.consecutive_double_time_bursts = 0
        return

    if steps == 2:
        soldier.last_pace = PaceMode.DOUBLE_TIME
        soldier.consecutive_double_time_bursts += 1
        if soldier.consecutive_double_time_bursts >= 2:
            soldier.recovery_ticks_remaining = max(soldier.recovery_ticks_remaining, 1)
            soldier.consecutive_double_time_bursts = 0
        return

    soldier.last_pace = PaceMode.SPRINT
    soldier.consecutive_double_time_bursts = 0
    soldier.recovery_ticks_remaining = max(soldier.recovery_ticks_remaining, 1)
    soldier.sprint_cooldown = max(soldier.sprint_cooldown, 2)


def apply_move_soldier(soldier: Soldier, dest: Optional[Position], realized_steps: int) -> None:
    if dest is None or realized_steps <= 0:
        soldier.last_pace = PaceMode.REST
        return
    soldier.pre_previous_pos = soldier.previous_pos
    soldier.previous_pos = soldier.pos
    soldier.pos = dest
    apply_fatigue_from_realized_steps(soldier, realized_steps)


def apply_move_recoveree(recoveree: RecovereeState, dest: Optional[Position], pace: PaceMode, realized_steps: int) -> None:
    if dest is None or realized_steps <= 0:
        recoveree.last_pace = PaceMode.REST
        return
    recoveree.pre_previous_pos = recoveree.previous_pos
    recoveree.previous_pos = recoveree.pos
    recoveree.pos = dest
    recoveree.last_pace = pace


def bullet_impact_event_tag(actor_kind: str, actor_id: str, impact_pos: Position, effect: str) -> str:
    actor_prefix = "RECOVEREE" if actor_kind == "recoveree" else actor_id
    return f"bullet_ground_impact_{actor_prefix}_{effect}_r{impact_pos.row}_c{impact_pos.col}"


def choose_ground_impact_position(anchor: Position, game_map: MapModel, rng: random.Random) -> Position:
    candidates = [anchor] + game_map.all_neighbors_8(anchor)
    return rng.choice(candidates)


def choose_grenade_target(state: SquadState, rng: random.Random) -> Tuple[str, str, Position]:
    carrier = state.carrier()
    candidates: List[Tuple[str, str, Position, float]] = []

    for soldier in state.alive_soldiers():
        weight = 1.0
        weight += 1.2 * cover_exposure_weight(soldier.exposure)
        local_cluster = sum(
            1 for other in state.alive_soldiers()
            if other.soldier_id != soldier.soldier_id and other.pos.manhattan(soldier.pos) <= 2
        )
        weight += 0.45 * local_cluster
        weight += 0.08 * (12 - min(12, soldier.pos.row))
        if carrier is not None and soldier.soldier_id == carrier.soldier_id:
            weight += 0.8
        candidates.append((soldier.soldier_id, "soldier", soldier.pos, max(weight, 0.01)))

    if state.recoveree.alive and state.recoveree.recovered:
        weight = 1.2 + 1.0 * cover_exposure_weight(state.recoveree.exposure)
        if state.recoveree.carried_by_id is not None:
            weight += 0.8
        candidates.append(("RECOVEREE", "recoveree", state.recoveree.pos, max(weight, 0.01)))

    chosen = rng.choices(candidates, weights=[c[3] for c in candidates], k=1)[0]
    return chosen[0], chosen[1], chosen[2]


def choose_grenade_landing(target_pos: Position, game_map: MapModel, rng: random.Random) -> Tuple[Position, str]:
    roll = rng.random()
    if roll < 0.50:
        return target_pos, "exact"

    orth = game_map.orthogonal_neighbors(target_pos)
    diag = [p for p in game_map.all_neighbors_8(target_pos) if p not in orth]
    ring2 = game_map.distance2_ring(target_pos)

    if roll < 0.85 and orth:
        return rng.choice(orth), "adjacent_orth"
    if roll < 0.95 and diag:
        return rng.choice(diag), "adjacent_diag"
    if ring2:
        return rng.choice(ring2), "miss_distance2"
    if diag:
        return rng.choice(diag), "adjacent_diag_fallback"
    if orth:
        return rng.choice(orth), "adjacent_orth_fallback"
    return target_pos, "exact_fallback"


def lock_grenade_occupancy(state: SquadState, grenade: GrenadeEvent) -> None:
    center_ids: List[str] = []
    adjacent_ids: List[str] = []
    locked_positions_by_id: Dict[str, Position] = {}

    for sid, soldier in state.soldiers.items():
        if soldier.alive:
            locked_positions_by_id[sid] = soldier.pos
            d = soldier.pos.manhattan(grenade.pos)
            if d == 0:
                center_ids.append(sid)
            elif d == 1:
                adjacent_ids.append(sid)

    if state.recoveree.alive:
        locked_positions_by_id["RECOVEREE"] = state.recoveree.pos
        d = state.recoveree.pos.manhattan(grenade.pos)
        if d == 0:
            center_ids.append("RECOVEREE")
        elif d == 1:
            adjacent_ids.append("RECOVEREE")

    grenade.locked_center_ids = center_ids
    grenade.locked_adjacent_ids = adjacent_ids
    grenade.locked_positions_by_id = locked_positions_by_id
    grenade.locked_recoveree_on_center = state.recoveree.alive and state.recoveree.recovered and state.recoveree.pos.manhattan(grenade.pos) == 0
    grenade.locked_recoveree_adjacent = state.recoveree.alive and state.recoveree.recovered and state.recoveree.pos.manhattan(grenade.pos) == 1
    grenade.locked_recoveree_pos = state.recoveree.pos if state.recoveree.alive else None


def grenade_has_locked_targets(grenade: GrenadeEvent) -> bool:
    return bool(grenade.locked_center_ids or grenade.locked_adjacent_ids)


def can_clear_blast_soldier(soldier: Soldier, state: SquadState, game_map: MapModel) -> bool:
    grenade = state.grenade
    if grenade is None:
        return True
    occupied = {s.pos for s in state.soldiers.values() if s.alive and s.soldier_id != soldier.soldier_id}
    if state.recoveree.alive and state.recoveree.carried_by_id is None:
        occupied.add(state.recoveree.pos)

    for pace in (PaceMode.WALK, PaceMode.DOUBLE_TIME, PaceMode.SPRINT):
        budget = pace_budget_for_soldier(soldier, pace)
        if budget <= 0:
            continue
        reachable = reachable_positions_with_cost(soldier.pos, budget, game_map, occupied)
        if any(pos.manhattan(grenade.pos) > 1 for pos, _, _ in reachable):
            return True
    return False


def can_clear_blast_recoveree(recoveree: RecovereeState, state: SquadState, game_map: MapModel) -> bool:
    grenade = state.grenade
    if grenade is None:
        return True
    occupied = {s.pos for s in state.soldiers.values() if s.alive}
    budget = pace_budget_for_recoveree(recoveree, PaceMode.WALK)
    if budget <= 0:
        return False
    reachable = reachable_positions_with_cost(recoveree.pos, budget, game_map, occupied)
    return any(pos.manhattan(grenade.pos) > 1 for pos, _, _ in reachable)


def absorb_intercept_positions(grenade_pos: Position, game_map: MapModel) -> List[Position]:
    return [grenade_pos]


def absorb_feasibility(candidate: CandidatePolicy, state: SquadState, game_map: MapModel) -> Tuple[bool, str]:
    grenade = state.grenade
    if grenade is None or not grenade.active:
        return False, "grenade_not_active"

    if candidate.actor_kind == "soldier":
        soldier = state.soldiers[candidate.actor_id]
        if not soldier.alive:
            return False, "actor_dead"
        if soldier.pos == grenade.pos:
            return True, "already_on_grenade_square"

        carrier = state.carrier()
        if (
            carrier is not None
            and carrier.alive
            and carrier.soldier_id == candidate.actor_id
            and soldier.is_carrying_recoveree
            and soldier.pos.manhattan(grenade.pos) == 1
        ):
            return True, "carrier_adjacent_shield"
        return False, "not_on_grenade_square"

    rec = state.recoveree
    if not rec.alive:
        return False, "actor_dead"
    if rec.carried_by_id is not None or not rec.can_self_move:
        return False, "recoveree_cannot_intercept"
    if rec.pos == grenade.pos:
        return True, "already_on_grenade_square"
    return False, "not_on_grenade_square"


def grenade_recoveree_protection_required(state: SquadState) -> bool:
    grenade = state.grenade
    if grenade is None or not grenade.active:
        return False
    if not state.recoveree.alive or not state.recoveree.recovered:
        return False
    lethal_to_recoveree_center = grenade.locked_recoveree_on_center
    lethal_to_recoveree_adjacent = grenade.locked_recoveree_adjacent and state.recoveree.integrity == IntegrityState.WOUNDED
    return lethal_to_recoveree_center or lethal_to_recoveree_adjacent


def second_pass_absorb_candidates(state: SquadState, game_map: MapModel) -> List[CandidatePolicy]:
    grenade = state.grenade
    if grenade is None or not grenade.active:
        return []

    out: List[CandidatePolicy] = []
    for soldier in state.alive_soldiers():
        candidate = CandidatePolicy(soldier.soldier_id, "soldier", PolicyType.ABSORB_GRENADE)
        feasible, _ = absorb_feasibility(candidate, state, game_map)
        if feasible:
            out.append(candidate)

    out.sort(key=lambda c: c.actor_id)
    return out


def classify_governance(state: SquadState) -> Tuple[List[str], str]:
    grenade = state.grenade
    default = ["Mission Continuity", "Squad Viability"]
    if grenade is None or not grenade.active:
        return default, "baseline_governance"
    if grenade.grenade_class == GrenadeClass.A_IRRELEVANT:
        return default, "local_threat_non_governing"
    if grenade.grenade_class == GrenadeClass.B_ESCAPABLE:
        return default, "dangerous_but_escapable"
    if grenade.grenade_class == GrenadeClass.C_DAMAGING_NON_GOVERNING:
        return default, "damaging_but_non_collapse"
    if grenade.grenade_class == GrenadeClass.D_COMPRESSED_GOVERNING:
        return ["Mission Continuity", "Squad Viability"], "joint_squad_mission_governing"
    return default, "baseline_governance"


def construct_candidate_policies(state: SquadState, game_map: MapModel) -> List[CandidatePolicy]:
    policies: List[CandidatePolicy] = []
    grenade = state.grenade

    for sid, soldier in state.soldiers.items():
        if not soldier.alive:
            continue
        policies.append(CandidatePolicy(sid, "soldier", PolicyType.HOLD))
        policies.append(CandidatePolicy(sid, "soldier", PolicyType.ADVANCE))
        if grenade and grenade.active:
            policies.append(CandidatePolicy(sid, "soldier", PolicyType.SCATTER))
            if soldier.is_carrying_recoveree:
                policies.append(CandidatePolicy(sid, "soldier", PolicyType.OBJECTIVE_CARRIER_ESCAPE))
            feasible_absorb, _ = absorb_feasibility(CandidatePolicy(sid, "soldier", PolicyType.ABSORB_GRENADE), state, game_map)
            if feasible_absorb:
                policies.append(CandidatePolicy(sid, "soldier", PolicyType.ABSORB_GRENADE))

    if state.recoveree.alive and state.recoveree.recovered:
        policies.append(CandidatePolicy("RECOVEREE", "recoveree", PolicyType.HOLD))
        policies.append(CandidatePolicy("RECOVEREE", "recoveree", PolicyType.ADVANCE))
        if grenade and grenade.active:
            policies.append(CandidatePolicy("RECOVEREE", "recoveree", PolicyType.SCATTER))
            feasible_absorb, _ = absorb_feasibility(CandidatePolicy("RECOVEREE", "recoveree", PolicyType.ABSORB_GRENADE), state, game_map)
            if feasible_absorb:
                policies.append(CandidatePolicy("RECOVEREE", "recoveree", PolicyType.ABSORB_GRENADE))

    existing = {(c.actor_id, c.actor_kind, c.policy_type) for c in policies}
    for candidate in second_pass_absorb_candidates(state, game_map):
        key = (candidate.actor_id, candidate.actor_kind, candidate.policy_type)
        if key not in existing:
            policies.append(candidate)
            existing.add(key)

    return policies


def evaluate_feasibility(candidate: CandidatePolicy, state: SquadState, game_map: MapModel) -> Tuple[bool, str]:
    if candidate.actor_kind == "soldier":
        soldier = state.soldiers[candidate.actor_id]
        if not soldier.alive:
            return False, "actor_dead"
        if candidate.policy_type == PolicyType.HOLD:
            return True, "always_feasible"
        if candidate.policy_type == PolicyType.ADVANCE:
            return True, "movement_nominally_available"
        if candidate.policy_type == PolicyType.SCATTER:
            return (True, "can_clear_blast") if can_clear_blast_soldier(soldier, state, game_map) else (False, "cannot_clear_blast")
        if candidate.policy_type == PolicyType.ABSORB_GRENADE:
            return absorb_feasibility(candidate, state, game_map)
        if candidate.policy_type == PolicyType.OBJECTIVE_CARRIER_ESCAPE:
            return (True, "carrier_can_attempt_escape") if soldier.is_carrying_recoveree else (False, "not_recoveree_carrier")
        return False, "unimplemented_policy"

    rec = state.recoveree
    if not rec.alive:
        return False, "actor_dead"
    if candidate.policy_type == PolicyType.HOLD:
        return True, "always_feasible"
    if candidate.policy_type == PolicyType.ADVANCE:
        return (True, "recoveree_can_move") if state.recoveree_mobile() else (False, "recoveree_cannot_self_move")
    if candidate.policy_type == PolicyType.SCATTER:
        return (True, "can_clear_blast") if can_clear_blast_recoveree(rec, state, game_map) else (False, "cannot_clear_blast")
    if candidate.policy_type == PolicyType.ABSORB_GRENADE:
        return absorb_feasibility(candidate, state, game_map)
    return False, "unimplemented_policy"


GLOBAL_MAP_FOR_ADEQUACY: MapModel


def projected_preservation_score(
    candidate: CandidatePolicy,
    state: SquadState,
    game_map: MapModel,
    governing_reason: str,
) -> Tuple[int, str]:
    grenade = state.grenade
    rec = state.recoveree
    carrier = state.carrier()
    clear_terminal_commit_lane_if_needed(state, game_map)

    if grenade is None or not grenade.active:
        return 0, "no_active_grenade"

    score = 0

    if grenade.locked_recoveree_on_center:
        if candidate.policy_type == PolicyType.ABSORB_GRENADE:
            score += 8
        elif candidate.policy_type in (PolicyType.SCATTER, PolicyType.OBJECTIVE_CARRIER_ESCAPE):
            score -= 8
    elif grenade.locked_recoveree_adjacent and rec.integrity == IntegrityState.WOUNDED:
        if candidate.policy_type == PolicyType.ABSORB_GRENADE:
            score += 6
        elif candidate.policy_type in (PolicyType.SCATTER, PolicyType.OBJECTIVE_CARRIER_ESCAPE):
            score -= 6

    for sid in grenade.locked_center_ids:
        if sid not in ("RECOVEREE",):
            score += 4 if candidate.policy_type == PolicyType.ABSORB_GRENADE else -2

    for sid in grenade.locked_adjacent_ids:
        if sid == "RECOVEREE":
            continue
        soldier = state.soldiers.get(sid)
        if soldier is not None and soldier.wounded:
            score += 3 if candidate.policy_type == PolicyType.ABSORB_GRENADE else -2

    if carrier is not None and carrier.alive:
        carrier_d = carrier.pos.manhattan(grenade.pos)
        if carrier_d <= 1:
            if candidate.policy_type == PolicyType.ABSORB_GRENADE:
                score += 4
            elif candidate.policy_type == PolicyType.OBJECTIVE_CARRIER_ESCAPE:
                score += 2
            elif candidate.policy_type == PolicyType.SCATTER:
                score += 1

        escorts = count_nearby_escorts(state, carrier, radius=2)
        score += min(2, escorts)

    if governing_reason == "joint_squad_mission_governing":
        if candidate.policy_type == PolicyType.ABSORB_GRENADE:
            score += 2
        elif candidate.policy_type == PolicyType.HOLD:
            score -= 4

    if candidate.policy_type == PolicyType.ABSORB_GRENADE:
        if candidate.actor_kind == "soldier":
            actor = state.soldiers[candidate.actor_id]
            score -= 4 if actor.integrity == IntegrityState.INTACT else 5
            if carrier is not None and actor.soldier_id == carrier.soldier_id:
                score -= 2
        else:
            score -= 8

    if candidate.policy_type == PolicyType.OBJECTIVE_CARRIER_ESCAPE:
        score += 1
        return score, "carrier_escape_projection"

    if candidate.policy_type == PolicyType.SCATTER:
        return score, "scatter_projection"

    if candidate.policy_type == PolicyType.ADVANCE:
        if candidate.actor_kind == "recoveree" and len(state.alive_soldiers()) == 0 and state.recoveree_mobile():
            score += 3
            return score, "solo_recoveree_self_withdraw_projection"
        return -6, "advance_ignores_blast"

    if candidate.policy_type == PolicyType.HOLD:
        return -8, "hold_under_grenade"

    return score, "generic_projection"


def best_feasible_non_sacrificial_score(state: SquadState, game_map: MapModel, governing_reason: str) -> Tuple[int, Optional[CandidatePolicy], str]:
    best_score = -999
    best_candidate: Optional[CandidatePolicy] = None
    best_reason = "none"

    for other in construct_candidate_policies(state, game_map):
        if other.policy_type == PolicyType.ABSORB_GRENADE:
            continue
        feasible, _ = evaluate_feasibility(other, state, game_map)
        if not feasible:
            continue
        score, reason = projected_preservation_score(other, state, game_map, governing_reason)
        if score > best_score:
            best_score = score
            best_candidate = other
            best_reason = reason

    return best_score, best_candidate, best_reason


def carrier_escape_directly_relevant(state: SquadState) -> bool:
    grenade = state.grenade
    carrier = state.carrier()
    if grenade is None or not grenade.active or carrier is None or not carrier.alive:
        return False

    if grenade.locked_recoveree_on_center:
        return True
    if grenade.locked_recoveree_adjacent and state.recoveree.integrity == IntegrityState.WOUNDED:
        return True
    if carrier.soldier_id in grenade.locked_center_ids:
        return True
    if carrier.soldier_id in grenade.locked_adjacent_ids:
        return True
    return False


def forced_absorb_structurally_required(state: SquadState, game_map: MapModel) -> Tuple[bool, str]:
    grenade = state.grenade
    if grenade is None or not grenade.active:
        return False, "no_active_grenade"

    rec = state.recoveree
    carrier = state.carrier()
    clear_terminal_commit_lane_if_needed(state, game_map)

    if grenade.locked_recoveree_on_center:
        return True, "recoveree_locked_center"

    if grenade.locked_recoveree_adjacent and rec.alive and rec.integrity == IntegrityState.WOUNDED:
        return True, "recoveree_locked_adjacent_wounded"

    if carrier is not None and carrier.alive:
        if carrier.soldier_id in grenade.locked_center_ids:
            return True, "carrier_locked_center"
        if carrier.soldier_id in grenade.locked_adjacent_ids and carrier.wounded:
            return True, "wounded_carrier_locked_adjacent"
        if (
            grenade.grenade_class == GrenadeClass.D_COMPRESSED_GOVERNING
            and carrier.is_carrying_recoveree
            and carrier.wounded
            and any(
                sid != "RECOVEREE"
                and (soldier := state.soldiers.get(sid)) is not None
                and soldier.alive
                for sid in grenade.locked_adjacent_ids
            )
        ):
            return True, "wounded_carrier_with_locked_adjacent_escort"

    for sid in grenade.locked_center_ids:
        if sid == "RECOVEREE":
            continue
        soldier = state.soldiers.get(sid)
        if soldier is not None and soldier.alive:
            return True, f"soldier_{sid}_locked_center"

    for sid in grenade.locked_adjacent_ids:
        if sid == "RECOVEREE":
            continue
        soldier = state.soldiers.get(sid)
        if soldier is None or not soldier.alive:
            continue
        if soldier.wounded:
            return True, f"wounded_soldier_{sid}_locked_adjacent"
        if state.squad_viability == SquadViabilityState.SV1 or state.cohesion == CohesionState.C0:
            return True, f"critical_structure_adjacent_{sid}"

    return False, "no_forced_absorb_condition"


def absorb_advantage_ok(candidate: CandidatePolicy, state: SquadState, game_map: MapModel, governing_reason: str) -> Tuple[bool, str]:
    feasible, _ = evaluate_feasibility(candidate, state, game_map)
    if not feasible:
        return False, "absorb_not_feasible"

    absorb_score, _ = projected_preservation_score(candidate, state, game_map, governing_reason)
    best_non_sac_score, best_non_sac_candidate, _ = best_feasible_non_sacrificial_score(state, game_map, governing_reason)

    if absorb_score >= best_non_sac_score + ABSORB_ADVANTAGE_THRESHOLD:
        if best_non_sac_candidate is None:
            return True, "absorb_required_no_feasible_non_sacrificial_option"
        return True, f"absorb_advantage_{absorb_score}_over_{best_non_sac_candidate.policy_type.name}_{best_non_sac_score}"

    if best_non_sac_candidate is None:
        return False, f"absorb_advantage_insufficient_{absorb_score}_vs_none_{best_non_sac_score}"

    return False, f"absorb_advantage_insufficient_{absorb_score}_vs_{best_non_sac_candidate.policy_type.name}_{best_non_sac_score}"


def evaluate_adequacy_pvpp(candidate: CandidatePolicy, state: SquadState, governing_reason: str) -> Tuple[bool, str]:
    grenade = state.grenade
    if grenade is None or not grenade.active:
        return True, "no_active_grenade"

    if candidate.policy_type == PolicyType.ABSORB_GRENADE:
        if governing_reason != "joint_squad_mission_governing":
            return False, "sacrifice_not_required"

        feasible, _ = evaluate_feasibility(candidate, state, GLOBAL_MAP_FOR_ADEQUACY)
        if not feasible:
            return False, "absorb_not_feasible"

        if grenade_recoveree_protection_required(state):
            return True, "absorb_required_to_preserve_recoveree"

        forced, forced_reason = forced_absorb_structurally_required(state, GLOBAL_MAP_FOR_ADEQUACY)
        if forced:
            return True, f"absorb_required_{forced_reason}"

        ok, reason = absorb_advantage_ok(candidate, state, GLOBAL_MAP_FOR_ADEQUACY, governing_reason)
        return ok, reason

    if candidate.policy_type == PolicyType.SCATTER:
        if state.recoveree.alive and grenade.locked_recoveree_adjacent and state.recoveree.integrity == IntegrityState.WOUNDED:
            return False, "scatter_fails_to_preserve_wounded_recoveree"
        if governing_reason in ("dangerous_but_escapable", "damaging_but_non_collapse", "local_threat_non_governing"):
            return True, "escape_adequate"
        return False, "scatter_fails_governing_structure"

    if candidate.policy_type == PolicyType.OBJECTIVE_CARRIER_ESCAPE:
        if not carrier_escape_directly_relevant(state):
            return False, "carrier_escape_not_directly_relevant_to_locked_blast"
        if state.recoveree.alive and grenade.locked_recoveree_adjacent and state.recoveree.integrity == IntegrityState.WOUNDED:
            return False, "carrier_escape_does_not_prevent_locked_adjacent_lethal_recoveree"
        feasible, _ = evaluate_feasibility(candidate, state, GLOBAL_MAP_FOR_ADEQUACY)
        if feasible:
            return True, "carrier_escape_structurally_admissible"
        return False, "carrier_escape_not_feasible"

    if candidate.policy_type == PolicyType.HOLD:
        return False, "temporary_delay_only"

    if candidate.policy_type == PolicyType.ADVANCE:
        if candidate.actor_kind == "recoveree":
            if len(state.alive_soldiers()) == 0 and state.recoveree_mobile():
                return True, "solo_self_withdrawal_after_total_squad_loss"
            return False, "solo_self_withdrawal_not_allowed_while_soldiers_alive"
        return False, "ignores_immediate_blast"

    return False, "no_adequate_basis"


def evaluate_adequacy_baseline(candidate: CandidatePolicy, state: SquadState, governing_reason: str) -> Tuple[bool, str]:
    grenade = state.grenade
    if grenade is None or not grenade.active:
        return True, "no_active_grenade"

    if candidate.policy_type == PolicyType.ABSORB_GRENADE:
        if candidate.actor_kind != "soldier":
            return False, "scalar_absorb_not_allowed_for_recoveree"

        soldier = state.soldiers[candidate.actor_id]
        carrier = state.carrier()
        if carrier is None or not carrier.alive:
            return False, "scalar_absorb_requires_live_carrier"
        if soldier.soldier_id != carrier.soldier_id or not soldier.is_carrying_recoveree:
            return False, "scalar_absorb_requires_carrier"
        if soldier.pos.manhattan(grenade.pos) != 1:
            return False, "scalar_absorb_adjacent_shield_only"

        feasible, _ = evaluate_feasibility(candidate, state, GLOBAL_MAP_FOR_ADEQUACY)
        if not feasible:
            return False, "absorb_not_feasible"

        if grenade_recoveree_protection_required(state):
            return True, "scalar_adjacent_shield_required_to_preserve_recoveree"

        return True, "scalar_adjacent_shield"

    if candidate.policy_type == PolicyType.SCATTER:
        return True, "self_preserving_escape"
    if candidate.policy_type == PolicyType.OBJECTIVE_CARRIER_ESCAPE:
        return True, "carrier_self_preserving_escape"
    if candidate.policy_type == PolicyType.ADVANCE and candidate.actor_kind == "recoveree":
        if len(state.alive_soldiers()) == 0 and state.recoveree_mobile():
            return True, "solo_self_preserving_move_after_total_squad_loss"
        return False, "solo_self_move_not_allowed_while_soldiers_alive"
    if candidate.policy_type == PolicyType.HOLD:
        return False, "temporary_delay_only"
    return False, "no_baseline_adequacy_basis"


def evaluate_adequacy_for_model(model_name: str, candidate: CandidatePolicy, state: SquadState, governing_reason: str) -> Tuple[bool, str]:
    return evaluate_adequacy_pvpp(candidate, state, governing_reason) if model_name == MODEL_PVPP else evaluate_adequacy_baseline(candidate, state, governing_reason)


def emergency_select_policy_pvpp(evals: List[PolicyEvaluation], state: SquadState, governing_reason: str) -> Optional[PolicyEvaluation]:
    feasible = [e for e in evals if e.feasible]
    if not feasible:
        return None

    forced, _ = forced_absorb_structurally_required(state, GLOBAL_MAP_FOR_ADEQUACY)
    if forced:
        absorb_candidates = [e for e in feasible if e.candidate.policy_type == PolicyType.ABSORB_GRENADE]
        if absorb_candidates:
            absorb_candidates.sort(key=lambda e: (0 if e.candidate.actor_kind == "soldier" else 1, e.candidate.actor_id))
            return absorb_candidates[0]

    carrier_escape_candidates = [
        e for e in feasible
        if e.candidate.policy_type == PolicyType.OBJECTIVE_CARRIER_ESCAPE and carrier_escape_directly_relevant(state)
    ]
    if carrier_escape_candidates:
        carrier_escape_candidates.sort(key=lambda e: e.candidate.actor_id)
        return carrier_escape_candidates[0]

    scatter_candidates = [e for e in feasible if e.candidate.policy_type == PolicyType.SCATTER]
    if scatter_candidates:
        scatter_candidates.sort(key=lambda e: (0 if e.candidate.actor_kind == "soldier" else 1, e.candidate.actor_id))
        return scatter_candidates[0]

    return feasible[0]


def select_policy_pvpp(evals: List[PolicyEvaluation], state: SquadState, governing_reason: str) -> Optional[PolicyEvaluation]:
    feasible_and_adequate = [e for e in evals if e.feasible and e.adequate]
    if not feasible_and_adequate:
        return emergency_select_policy_pvpp(evals, state, governing_reason)

    strongest_absorb = [
        e for e in feasible_and_adequate
        if e.candidate.policy_type == PolicyType.ABSORB_GRENADE
        and (
            e.adequacy_reason == "absorb_required_to_preserve_recoveree"
            or e.adequacy_reason.startswith("absorb_required_")
            or e.adequacy_reason == "absorb_required_no_feasible_non_sacrificial_option"
            or e.adequacy_reason.startswith("absorb_advantage_")
        )
    ]
    if strongest_absorb:
        strongest_absorb.sort(key=lambda e: (0 if e.candidate.actor_kind == "soldier" else 1, e.candidate.actor_id))
        return strongest_absorb[0]

    recoveree_advance = [
        e for e in feasible_and_adequate
        if e.candidate.actor_kind == "recoveree" and e.candidate.policy_type == PolicyType.ADVANCE
    ]
    if recoveree_advance:
        return recoveree_advance[0]

    carrier_escape = [e for e in feasible_and_adequate if e.candidate.policy_type == PolicyType.OBJECTIVE_CARRIER_ESCAPE]
    if carrier_escape:
        return carrier_escape[0]

    scatter = [e for e in feasible_and_adequate if e.candidate.policy_type == PolicyType.SCATTER]
    if scatter:
        return scatter[0]

    absorb = [e for e in feasible_and_adequate if e.candidate.policy_type == PolicyType.ABSORB_GRENADE]
    if absorb:
        absorb.sort(key=lambda e: (0 if e.candidate.actor_kind == "soldier" else 1, e.candidate.actor_id))
        return absorb[0]

    return feasible_and_adequate[0]


def emergency_select_policy_baseline(evals: List[PolicyEvaluation]) -> Optional[PolicyEvaluation]:
    feasible = [e for e in evals if e.feasible]
    if not feasible:
        return None

    absorb = [e for e in feasible if e.candidate.policy_type == PolicyType.ABSORB_GRENADE]
    if absorb:
        absorb.sort(key=lambda e: (0 if e.candidate.actor_kind == "soldier" else 1, e.candidate.actor_id))
        return absorb[0]

    carrier_escape = [e for e in feasible if e.candidate.policy_type == PolicyType.OBJECTIVE_CARRIER_ESCAPE]
    if carrier_escape:
        return carrier_escape[0]

    scatter = [e for e in feasible if e.candidate.policy_type == PolicyType.SCATTER]
    if scatter:
        return scatter[0]

    recoveree_advance = [e for e in feasible if e.candidate.actor_kind == "recoveree" and e.candidate.policy_type == PolicyType.ADVANCE]
    if recoveree_advance:
        return recoveree_advance[0]

    return feasible[0]


def select_policy_baseline(evals: List[PolicyEvaluation]) -> Optional[PolicyEvaluation]:
    feasible_and_adequate = [e for e in evals if e.feasible and e.adequate]
    if not feasible_and_adequate:
        return emergency_select_policy_baseline(evals)

    strongest_absorb = [
        e for e in feasible_and_adequate
        if e.candidate.policy_type == PolicyType.ABSORB_GRENADE
        and (
            e.adequacy_reason == "scalar_adjacent_shield_required_to_preserve_recoveree"
            or e.adequacy_reason == "scalar_adjacent_shield"
        )
    ]
    if strongest_absorb:
        strongest_absorb.sort(key=lambda e: (0 if e.candidate.actor_kind == "soldier" else 1, e.candidate.actor_id))
        return strongest_absorb[0]

    carrier_escape = [e for e in feasible_and_adequate if e.candidate.policy_type == PolicyType.OBJECTIVE_CARRIER_ESCAPE]
    if carrier_escape:
        return carrier_escape[0]
    scatter = [e for e in feasible_and_adequate if e.candidate.policy_type == PolicyType.SCATTER]
    if scatter:
        return scatter[0]
    recoveree_advance = [e for e in feasible_and_adequate if e.candidate.actor_kind == "recoveree" and e.candidate.policy_type == PolicyType.ADVANCE]
    if recoveree_advance:
        return recoveree_advance[0]
    absorb = [e for e in feasible_and_adequate if e.candidate.policy_type == PolicyType.ABSORB_GRENADE]
    if absorb:
        absorb.sort(key=lambda e: (0 if e.candidate.actor_kind == "soldier" else 1, e.candidate.actor_id))
        return absorb[0]
    return feasible_and_adequate[0]


def select_policy_for_model(model_name: str, evals: List[PolicyEvaluation], state: SquadState, governing_reason: str) -> Optional[PolicyEvaluation]:
    return select_policy_pvpp(evals, state, governing_reason) if model_name == MODEL_PVPP else select_policy_baseline(evals)


def choose_absorb_intercept_pos(actor_id: str, actor_kind: str, state: SquadState, game_map: MapModel) -> Optional[Position]:
    grenade = state.grenade
    if grenade is None:
        return None
    return grenade.pos


def execute_selected_grenade_policy(chosen: Optional[PolicyEvaluation], state: SquadState, game_map: MapModel) -> List[str]:
    events: List[str] = []
    if chosen is None:
        events.append("grenade_response_none")
        return events

    candidate = chosen.candidate
    if candidate.policy_type == PolicyType.ABSORB_GRENADE and state.grenade is not None:
        state.grenade.absorb_actor_id = candidate.actor_id
        state.grenade.absorb_actor_kind = candidate.actor_kind
        state.grenade.absorb_intercept_pos = choose_absorb_intercept_pos(candidate.actor_id, candidate.actor_kind, state, game_map)
        if candidate.actor_kind == "soldier":
            state.soldiers[candidate.actor_id].last_action = PolicyType.ABSORB_GRENADE
            state.soldiers[candidate.actor_id].last_pace = PaceMode.REST
        else:
            state.recoveree.last_action = "ABSORB_GRENADE"
            state.recoveree.last_pace = PaceMode.REST
        events.append(f"{candidate.actor_id}_response_absorb_marked")
        return events

    if candidate.actor_kind == "soldier":
        actor = state.soldiers[candidate.actor_id]
        actor.last_action = candidate.policy_type

        if candidate.policy_type == PolicyType.SCATTER:
            dest, _, steps = choose_best_position_for_soldier(actor, "scatter", state, game_map)
            apply_move_soldier(actor, dest, steps)
            if actor.is_carrying_recoveree and dest is not None:
                state.recoveree.pos = dest
            events.append(f"{actor.soldier_id}_response_{candidate.policy_type.name}_{actor.last_pace.value if actor.last_pace else 'REST'}")
            return events

        if candidate.policy_type == PolicyType.OBJECTIVE_CARRIER_ESCAPE:
            dest, _, steps = choose_best_position_for_soldier(actor, "withdraw", state, game_map)
            apply_move_soldier(actor, dest, steps)
            if actor.is_carrying_recoveree and dest is not None:
                state.recoveree.pos = dest
            events.append(f"{actor.soldier_id}_response_{candidate.policy_type.name}_{actor.last_pace.value if actor.last_pace else 'REST'}")
            return events

        events.append(f"{actor.soldier_id}_response_hold")
        return events

    if candidate.policy_type == PolicyType.SCATTER:
        dest, pace, steps = choose_best_position_for_recoveree(state.recoveree, state, game_map)
        apply_move_recoveree(state.recoveree, dest, pace, steps)
        state.recoveree.last_action = "SCATTER"
        events.append(f"RECOVEREE_response_SCATTER_{state.recoveree.last_pace.value if state.recoveree.last_pace else 'REST'}")
        return events

    if candidate.policy_type == PolicyType.ADVANCE:
        dest, pace, steps = choose_best_position_for_recoveree(state.recoveree, state, game_map)
        apply_move_recoveree(state.recoveree, dest, pace, steps)
        state.recoveree.last_action = "SELF_WITHDRAW"
        events.append(f"RECOVEREE_response_SELF_WITHDRAW_{state.recoveree.last_pace.value if state.recoveree.last_pace else 'REST'}")
        return events

    events.append("RECOVEREE_response_hold")
    return events


def resolve_grenade(state: SquadState) -> List[str]:
    events: List[str] = []
    grenade = state.grenade
    if grenade is None or not grenade.active or grenade.has_detonated:
        return events

    events.append("grenade_resolution_begin")

    if grenade.absorb_actor_id is not None:
        if grenade.absorb_actor_kind == "soldier":
            actor = state.soldiers[grenade.absorb_actor_id]
            intercept_pos = grenade.absorb_intercept_pos or grenade.locked_positions_by_id.get(actor.soldier_id, actor.pos)
            actor.pos = intercept_pos
            carrying = actor.is_carrying_recoveree
            kill_soldier(actor)
            if carrying:
                state.recoveree.carried_by_id = None
                state.recoveree.pos = grenade.locked_positions_by_id.get(actor.soldier_id, actor.pos)
                events.append("recoveree_dropped_alive_after_absorb")
            if grenade.locked_recoveree_adjacent:
                events.append("recoveree_survived_locked_adjacent_due_to_absorb")
            if grenade.locked_recoveree_on_center:
                events.append("recoveree_survived_locked_center_due_to_absorb")
            events.append(f"{actor.soldier_id}_absorbed_grenade")
        else:
            rec = state.recoveree
            if grenade.locked_recoveree_pos is not None:
                rec.pos = grenade.locked_recoveree_pos
            kill_recoveree(rec)
            events.append("recoveree_absorbed_grenade_and_died")

        state.post_absorb_protection_ticks = 3
        events.append("absorb_fully_protected_others")
    else:
        for entity_id in list(grenade.locked_center_ids):
            if entity_id == "RECOVEREE":
                if state.recoveree.alive:
                    if grenade.locked_recoveree_pos is not None:
                        state.recoveree.pos = grenade.locked_recoveree_pos
                    kill_recoveree(state.recoveree)
                    events.append("recoveree_killed_by_grenade_locked_center")
                continue

            soldier = state.soldiers[entity_id]
            if not soldier.alive:
                continue
            locked_pos = grenade.locked_positions_by_id.get(entity_id, soldier.pos)
            carrying = soldier.is_carrying_recoveree
            soldier.pos = locked_pos
            kill_soldier(soldier)
            if carrying:
                state.recoveree.carried_by_id = None
                state.recoveree.pos = locked_pos
                if grenade.locked_recoveree_on_center:
                    kill_recoveree(state.recoveree)
                    events.append("recoveree_killed_with_carrier_locked_center_blast")
                else:
                    events.append("recoveree_dropped_alive_after_center_blast_carrier_death")
            events.append(f"{entity_id}_killed_by_grenade_locked_center")

        for entity_id in list(grenade.locked_adjacent_ids):
            if entity_id == "RECOVEREE":
                if state.recoveree.alive:
                    if grenade.locked_recoveree_pos is not None:
                        state.recoveree.pos = grenade.locked_recoveree_pos
                    result = apply_nonfatal_damage_recoveree(state.recoveree)
                    if result == "wounded":
                        events.append("recoveree_wounded_by_grenade_locked_adjacent")
                    elif result == "killed":
                        events.append("recoveree_killed_by_grenade_locked_adjacent")
                continue

            soldier = state.soldiers[entity_id]
            if not soldier.alive:
                continue
            locked_pos = grenade.locked_positions_by_id.get(entity_id, soldier.pos)
            carrying = soldier.is_carrying_recoveree
            soldier.pos = locked_pos
            result = apply_nonfatal_damage_soldier(soldier)
            if result == "wounded":
                events.append(f"{entity_id}_wounded_by_grenade_locked_adjacent")
            elif result == "killed":
                if carrying:
                    soldier.is_carrying_recoveree = False
                    state.recoveree.carried_by_id = None
                    state.recoveree.pos = locked_pos
                    events.append("recoveree_dropped_alive_after_adjacent_grenade_carrier_death")
                events.append(f"{entity_id}_killed_by_second_wound_grenade_locked_adjacent")

    grenade.has_detonated = True
    grenade.active = False
    events.append("grenade_detonated")
    return events


def assign_carrier_if_needed(state: SquadState) -> Optional[str]:
    rec = state.recoveree
    if not rec.recovered or not rec.alive or rec.extracted:
        return None
    if rec.carried_by_id is not None:
        carrier = state.carrier()
        if carrier is not None and carrier.alive:
            return carrier.soldier_id

    candidates = [s for s in state.alive_soldiers() if s.pos.manhattan(rec.pos) <= 1]
    if not candidates:
        return None

    candidates.sort(key=lambda s: (0 if s.integrity == IntegrityState.INTACT else 1, s.pos.manhattan(rec.pos), s.soldier_id))
    carrier = candidates[0]
    for soldier in state.soldiers.values():
        soldier.is_carrying_recoveree = False
    carrier.is_carrying_recoveree = True
    rec.carried_by_id = carrier.soldier_id
    rec.dragged_by_id = None
    rec.assigned_helper_id = carrier.soldier_id
    rec.pos = carrier.pos
    reset_fatigue_for_pickup(carrier)
    return carrier.soldier_id


def reconcile_recoveree_after_grenade(state: SquadState) -> List[str]:
    events: List[str] = []
    rec = state.recoveree
    if not rec.recovered or rec.extracted or not rec.alive:
        return events

    carrier = state.carrier()
    if carrier is not None and carrier.alive:
        rec.pos = carrier.pos
        events.append("post_grenade_carrier_still_valid")
        return events

    rec.carried_by_id = None
    reassigned = assign_carrier_if_needed(state)
    if reassigned is not None:
        state.mission_phase = MissionPhase.WITHDRAWAL
        events.append(f"{reassigned}_reacquired_recoveree_immediate")
    elif len(state.alive_soldiers()) == 0 and state.recoveree_mobile():
        state.mission_phase = MissionPhase.WITHDRAWAL
        events.append("recoveree_self_mobile_post_grenade_after_total_squad_loss")
    else:
        state.mission_phase = MissionPhase.RECOVERY
        events.append("recoveree_waiting_reacquire")
    return events


def maybe_recover_recoveree(state: SquadState, game_map: MapModel) -> List[str]:
    events: List[str] = []
    rec = state.recoveree
    if not rec.alive:
        return events

    if not rec.recovered:
        in_zone_soldiers = [s for s in state.alive_soldiers() if game_map.in_objective_zone(s.pos)]
        if not in_zone_soldiers:
            return events

        in_zone_soldiers.sort(key=lambda s: (0 if s.integrity == IntegrityState.INTACT else 1, s.soldier_id))
        pickup_soldier = in_zone_soldiers[0]

        rec.recovered = True
        state.mission_phase = MissionPhase.RECOVERY
        rec.pos = pickup_soldier.pos

        assigned = assign_carrier_if_needed(state)
        events.append("recoveree_recovered")
        if assigned is not None:
            events.append(f"{assigned}_assigned_carrier")
        elif len(state.alive_soldiers()) == 0 and state.recoveree_mobile():
            events.append("recoveree_can_self_withdraw_after_total_squad_loss")
        return events

    if rec.carried_by_id is None and not rec.extracted:
        assigned = assign_carrier_if_needed(state)
        if assigned is not None:
            state.mission_phase = MissionPhase.WITHDRAWAL
            events.append(f"{assigned}_reacquired_recoveree")
        elif len(state.alive_soldiers()) == 0 and state.recoveree_mobile():
            state.mission_phase = MissionPhase.WITHDRAWAL
            events.append("recoveree_self_withdrawal_enabled_after_total_squad_loss")
    return events


def run_recovery_step(state: SquadState, game_map: MapModel) -> List[str]:
    events: List[str] = []
    if not state.recoveree.recovered:
        return run_advance_step(state, game_map)

    for soldier in sorted(state.alive_soldiers(), key=lambda s: s.soldier_id):
        if soldier.recovery_ticks_remaining > 0 or soldier.sprint_cooldown > 0:
            soldier.last_pace = PaceMode.REST
            continue
        if soldier.pos.manhattan(state.recoveree.pos) <= 1:
            soldier.last_pace = PaceMode.REST
            continue
        dest, _, steps = choose_best_position_for_soldier(soldier, "reacquire", state, game_map, support_target=state.recoveree.pos)
        apply_move_soldier(soldier, dest, steps)
        if dest is not None:
            soldier.last_action = PolicyType.REACQUIRE_RECOVEREE
            events.append(f"{soldier.soldier_id}_{soldier.last_pace.value}_toward_recoveree")

    events.extend(maybe_recover_recoveree(state, game_map))
    return events


def run_advance_step(state: SquadState, game_map: MapModel) -> List[str]:
    events: List[str] = []
    for soldier in sorted(state.alive_soldiers(), key=lambda s: s.soldier_id):
        if soldier.recovery_ticks_remaining > 0 or soldier.sprint_cooldown > 0:
            soldier.last_pace = PaceMode.REST
            continue
        dest, _, steps = choose_best_position_for_soldier(soldier, "advance", state, game_map)
        apply_move_soldier(soldier, dest, steps)
        if dest is not None:
            soldier.last_action = PolicyType.ADVANCE
            events.append(f"{soldier.soldier_id}_{soldier.last_pace.value}")

    events.extend(maybe_recover_recoveree(state, game_map))
    if state.recoveree.recovered:
        if state.recoveree.carried_by_id is not None:
            state.mission_phase = MissionPhase.WITHDRAWAL
        elif len(state.alive_soldiers()) == 0 and state.recoveree_mobile():
            state.mission_phase = MissionPhase.WITHDRAWAL
        else:
            state.mission_phase = MissionPhase.RECOVERY
    return events


def choose_forced_extraction_entry(
    pos: Position,
    game_map: MapModel,
    occupied: Set[Position],
    preferred_lane: Optional[int] = None,
) -> Optional[Position]:
    candidates = []
    for nxt in game_map.orthogonal_neighbors(pos):
        if nxt in occupied:
            continue
        if not game_map.in_extraction(nxt):
            continue
        try:
            spent = game_map.movement_cost(pos, nxt)
        except ValueError:
            continue
        lane_penalty = 0 if preferred_lane is None or nxt.col == preferred_lane else 1
        candidates.append((lane_penalty, spent, -nxt.row, abs(nxt.col - 2), nxt.col, nxt))
    if not candidates:
        return None
    candidates.sort()
    return candidates[0][-1]


def run_withdrawal_step(state: SquadState, game_map: MapModel, model_name: str, config: RuntimeConfig) -> List[str]:
    events: List[str] = []
    rec = state.recoveree
    carrier = state.carrier()
    clear_terminal_commit_lane_if_needed(state, game_map)

    if not rec.alive:
        state.mission_phase = MissionPhase.FAILED
        events.append("recoveree_dead")
        return events

    # Hard terminal controller near extraction: deterministic escort yielding + no-reverse carrier logic.
    if terminal_extraction_mode_active(state, game_map):
        if carrier is not None:
            supporters = [s for s in state.alive_soldiers() if s.soldier_id != carrier.soldier_id and should_force_escort_yield(s, state, game_map)]
            assignments = assign_terminal_yield_tiles(supporters, state, game_map)
            for soldier in supporters:
                if soldier.recovery_ticks_remaining > 0 or soldier.sprint_cooldown > 0:
                    soldier.last_pace = PaceMode.REST
                    continue
                if soldier.soldier_id in assignments:
                    dest, pace, steps = choose_terminal_yield_move(soldier, assignments[soldier.soldier_id], state, game_map)
                    apply_move_soldier(soldier, dest, steps)
                    if dest is not None:
                        soldier.last_action = PolicyType.SUPPORT_WITHDRAWAL
                        soldier.last_pace = pace
                        events.append(f"{soldier.soldier_id}_{soldier.last_pace.value}_yield")
            rec.pos = carrier.pos
            if carrier.recovery_ticks_remaining > 0 or carrier.sprint_cooldown > 0:
                carrier.last_pace = PaceMode.REST
            else:
                occupied = {s.pos for s in state.soldiers.values() if s.alive and s.soldier_id != carrier.soldier_id}
                forced_entry = choose_forced_extraction_entry(carrier.pos, game_map, occupied, get_terminal_commit_lane(state, game_map))
                if forced_entry is not None:
                    apply_move_soldier(carrier, forced_entry, 1)
                    carrier.last_action = PolicyType.OBJECTIVE_CARRIER_ESCAPE
                    rec.pos = carrier.pos
                    events.append(f"{carrier.soldier_id}_{carrier.last_pace.value}_terminal_forced_entry")
                else:
                    dest, _, steps = choose_terminal_carrier_move(carrier, state, game_map)
                    apply_move_soldier(carrier, dest, steps)
                    if dest is not None:
                        carrier.last_action = PolicyType.OBJECTIVE_CARRIER_ESCAPE
                        rec.pos = carrier.pos
                        events.append(f"{carrier.soldier_id}_{carrier.last_pace.value}_terminal")
            rec.pos = carrier.pos
            if game_map.extraction_success(carrier.pos):
                rec.extracted = True
                state.mission_phase = MissionPhase.COMPLETE
                events.append("recoveree_extracted_by_terminal_carrier")
                return events
            return events

        if state.recoveree_mobile():
            occupied = {s.pos for s in state.soldiers.values() if s.alive}
            forced_entry = choose_forced_extraction_entry(rec.pos, game_map, occupied, get_terminal_commit_lane(state, game_map))
            if forced_entry is not None:
                apply_move_recoveree(rec, forced_entry, PaceMode.WALK, 1)
                rec.last_action = "SELF_WITHDRAW_TERMINAL"
                events.append("RECOVEREE_WALK_terminal_forced_entry")
            else:
                dest, pace, steps = choose_terminal_recoveree_move(rec, state, game_map)
                apply_move_recoveree(rec, dest, pace, steps)
                rec.last_action = "SELF_WITHDRAW_TERMINAL"
                if dest is not None:
                    events.append(f"RECOVEREE_{rec.last_pace.value}_terminal_self_withdraw")
            if game_map.extraction_success(rec.pos):
                rec.extracted = True
                state.mission_phase = MissionPhase.COMPLETE
                events.append("recoveree_terminal_self_extracted")
                return events
            return events

    if carrier is not None:
        rec.pos = carrier.pos
        if carrier.recovery_ticks_remaining > 0 or carrier.sprint_cooldown > 0:
            carrier.last_pace = PaceMode.REST
        else:
            dest, _, steps = choose_best_position_for_soldier(carrier, "withdraw", state, game_map)
            apply_move_soldier(carrier, dest, steps)
            if dest is not None:
                carrier.last_action = PolicyType.OBJECTIVE_CARRIER_ESCAPE
                rec.pos = carrier.pos
                events.append(f"{carrier.soldier_id}_{carrier.last_pace.value}")
        if game_map.extraction_success(carrier.pos):
            rec.pos = carrier.pos
            rec.extracted = True
            state.mission_phase = MissionPhase.COMPLETE
            events.append("recoveree_extracted_by_carrier")
            return events
    else:
        if len(state.alive_soldiers()) == 0 and state.recoveree_mobile():
            dest, pace, steps = choose_best_position_for_recoveree(rec, state, game_map)
            apply_move_recoveree(rec, dest, pace, steps)
            rec.last_action = "SELF_WITHDRAW"
            if dest is not None:
                events.append(f"RECOVEREE_{rec.last_pace.value}_self_withdraw")
            if game_map.extraction_success(rec.pos):
                rec.extracted = True
                state.mission_phase = MissionPhase.COMPLETE
                events.append("recoveree_self_extracted")
                return events
        else:
            state.mission_phase = MissionPhase.RECOVERY
            events.append("recoveree_needs_help_or_squad_still_present_switch_to_recovery")
            return events

    support_target = rec.pos
    supporters = [s for s in state.alive_soldiers() if carrier is None or s.soldier_id != carrier.soldier_id]
    supporters.sort(key=lambda s: (game_map.nearest_extraction_success_distance(s.pos), s.soldier_id))

    screen_mode_active = (
        config.scalar_favor_mode == "screen_tradeoff"
        and ordinary_non_grenade_withdrawal(state)
        and carrier is not None
    )
    available_slots = escort_slots(support_target, game_map) if screen_mode_active else []
    assigned_slots: Dict[str, Position] = {}
    if screen_mode_active:
        remaining_slots = list(available_slots)
        for soldier in supporters:
            if not remaining_slots:
                break
            remaining_slots.sort(key=lambda p, sp=soldier.pos: (sp.manhattan(p), p.row, p.col))
            assigned_slots[soldier.soldier_id] = remaining_slots.pop(0)

    for soldier in supporters:
        if soldier.recovery_ticks_remaining > 0 or soldier.sprint_cooldown > 0:
            soldier.last_pace = PaceMode.REST
            continue

        if soldier.soldier_id in assigned_slots:
            dest, _, steps = choose_best_screen_position_for_soldier(soldier, assigned_slots[soldier.soldier_id], state, game_map)
        else:
            dest, _, steps = choose_best_position_for_soldier(soldier, "support", state, game_map, support_target=support_target)

        apply_move_soldier(soldier, dest, steps)
        if dest is not None:
            soldier.last_action = PolicyType.SUPPORT_WITHDRAWAL
            events.append(f"{soldier.soldier_id}_{soldier.last_pace.value}")

    if carrier is not None and game_map.extraction_success(carrier.pos):
        rec.extracted = True
        state.mission_phase = MissionPhase.COMPLETE
        events.append("recoveree_extracted_post_support_check")
    elif carrier is None and len(state.alive_soldiers()) == 0 and game_map.extraction_success(rec.pos):
        rec.extracted = True
        state.mission_phase = MissionPhase.COMPLETE
        events.append("recoveree_self_extracted_post_support_check")
    return events

def background_fire_probabilities_for_soldier(soldier: Soldier, state: SquadState, enemy_fire_scale: float, config: RuntimeConfig) -> Tuple[float, float]:
    cover_mult = COVER_CONFIG["fire_modifier"][cover_name(CoverType.NONE if soldier.exposure == ExposureState.HIGH else CoverType.PARTIAL if soldier.exposure == ExposureState.MODERATE else CoverType.STRONG)]
    if soldier.exposure == ExposureState.HIGH:
        wound, kill = 0.08, 0.02
    elif soldier.exposure == ExposureState.MODERATE:
        wound, kill = 0.05, 0.01
    else:
        wound, kill = 0.02, 0.005

    wound *= cover_mult
    kill *= cover_mult

    carrier = state.carrier()
    if carrier is not None and soldier.is_carrying_recoveree:
        escorts = count_nearby_escorts(state, carrier, radius=2)
        reduction_factor = max(0.40, 1.0 - 0.20 * escorts)
        wound *= reduction_factor
        kill *= reduction_factor
        if escorts <= 1:
            wound *= 1.35
            kill *= 1.35
        if state.post_absorb_protection_ticks > 0:
            wound *= 0.60
            kill *= 0.60

        tradeoff_mult = screen_tradeoff_multiplier(state, carrier, config)
        wound *= tradeoff_mult
        kill *= tradeoff_mult

    wound *= enemy_fire_scale
    kill *= enemy_fire_scale
    return min(wound, 0.95), min(kill, 0.95)


def background_fire_probabilities_for_recoveree(recoveree: RecovereeState, state: SquadState, enemy_fire_scale: float, config: RuntimeConfig) -> Tuple[float, float]:
    cover_mult = COVER_CONFIG["fire_modifier"][cover_name(CoverType.NONE if recoveree.exposure == ExposureState.HIGH else CoverType.PARTIAL if recoveree.exposure == ExposureState.MODERATE else CoverType.STRONG)]
    if recoveree.exposure == ExposureState.HIGH:
        wound, kill = 0.07, 0.03
    elif recoveree.exposure == ExposureState.MODERATE:
        wound, kill = 0.04, 0.015
    else:
        wound, kill = 0.015, 0.007

    wound *= cover_mult
    kill *= cover_mult

    carrier = state.carrier()
    if recoveree.carried_by_id is not None and state.post_absorb_protection_ticks > 0:
        wound *= 0.60
        kill *= 0.60

    tradeoff_mult = screen_tradeoff_multiplier(state, carrier, config)
    wound *= tradeoff_mult
    kill *= tradeoff_mult

    wound *= enemy_fire_scale
    kill *= enemy_fire_scale
    return min(wound, 0.95), min(kill, 0.95)


def apply_background_fire(state: SquadState, game_map: MapModel, rng: random.Random, config: RuntimeConfig) -> List[str]:
    events: List[str] = []

    for soldier in sorted(state.alive_soldiers(), key=lambda s: s.soldier_id):
        if state.recoveree.extracted and game_map.extraction_success(soldier.pos):
            continue
        p_wound, p_kill = background_fire_probabilities_for_soldier(soldier, state, config.enemy_fire_scale, config)
        roll = rng.random()
        if roll < p_kill:
            dropped_pos = soldier.pos
            carrying = soldier.is_carrying_recoveree
            kill_soldier(soldier)
            if carrying:
                state.recoveree.carried_by_id = None
                state.recoveree.pos = dropped_pos
                events.append("recoveree_dropped_alive_after_background_fire_carrier_death")
            events.append(f"{soldier.soldier_id}_killed")
        elif roll < p_kill + p_wound:
            dropped_pos = soldier.pos
            carrying = soldier.is_carrying_recoveree
            result = apply_nonfatal_damage_soldier(soldier)
            if result == "wounded":
                events.append(f"{soldier.soldier_id}_wounded")
            elif result == "killed":
                if carrying:
                    state.recoveree.carried_by_id = None
                    state.recoveree.pos = dropped_pos
                    events.append("recoveree_dropped_alive_after_background_fire_second_wound")
                events.append(f"{soldier.soldier_id}_killed_by_second_wound")
        else:
            impact_pos = choose_ground_impact_position(soldier.pos, game_map, rng)
            impact_effect = "dust" if game_map.get_tile(impact_pos).cover == CoverType.NONE else "smoke"
            events.append(bullet_impact_event_tag("soldier", soldier.soldier_id, impact_pos, impact_effect))

    if state.recoveree.alive and not state.recoveree.extracted:
        p_wound, p_kill = background_fire_probabilities_for_recoveree(state.recoveree, state, config.enemy_fire_scale, config)
        roll = rng.random()
        if roll < p_kill:
            kill_recoveree(state.recoveree)
            events.append("recoveree_killed_by_background_fire")
        elif roll < p_kill + p_wound:
            result = apply_nonfatal_damage_recoveree(state.recoveree)
            if result == "wounded":
                events.append("recoveree_wounded_by_background_fire")
            elif result == "killed":
                events.append("recoveree_killed_by_second_wound_background_fire")
        else:
            impact_pos = choose_ground_impact_position(state.recoveree.pos, game_map, rng)
            impact_effect = "dust" if game_map.get_tile(impact_pos).cover == CoverType.NONE else "smoke"
            events.append(bullet_impact_event_tag("recoveree", "RECOVEREE", impact_pos, impact_effect))

    if state.post_absorb_protection_ticks > 0:
        state.post_absorb_protection_ticks -= 1
    return events

def maybe_trigger_grenade(state: SquadState, game_map: MapModel, rng: random.Random, config: RuntimeConfig) -> Tuple[List[str], bool]:
    events: List[str] = []
    rec = state.recoveree
    if state.grenade is not None or state.mission_phase != MissionPhase.WITHDRAWAL or not rec.recovered or rec.extracted or not rec.alive:
        return events, False

    trigger_anchor = state.carrier().pos if state.carrier() is not None else rec.pos
    in_band = 5 <= trigger_anchor.row <= 8
    if not in_band:
        return events, False

    roll = rng.random()
    if roll < config.grenade_trigger_prob:
        target_id, target_kind, target_pos = choose_grenade_target(state, rng)
        landing_pos, scatter_kind = choose_grenade_landing(target_pos, game_map, rng)
        grenade = GrenadeEvent(
            pos=landing_pos,
            grenade_class=GrenadeClass.D_COMPRESSED_GOVERNING,
            triggered_tick=state.tick,
            intended_target_id=target_id,
            intended_target_kind=target_kind,
            intended_target_pos=target_pos,
            scatter_kind=scatter_kind,
        )
        state.grenade = grenade
        lock_grenade_occupancy(state, grenade)

        if not grenade_has_locked_targets(grenade):
            events.append(f"grenade_triggered_probabilistic_roll_{roll:.3f}_target_{target_id}_{scatter_kind}_harmless_miss")
            state.grenade = None
            return events, False

        events.append(f"grenade_triggered_probabilistic_roll_{roll:.3f}_target_{target_id}_{scatter_kind}")
        return events, True
    return events, False


def snapshot_soldier(soldier: Soldier) -> dict:
    return {
        "soldier_id": soldier.soldier_id,
        "row": soldier.pos.row,
        "col": soldier.pos.col,
        "integrity": soldier.integrity.value,
        "mobility": soldier.mobility.value,
        "exposure": soldier.exposure.value,
        "is_carrying_recoveree": soldier.is_carrying_recoveree,
        "last_action": None if soldier.last_action is None else soldier.last_action.name,
        "last_pace": None if soldier.last_pace is None else soldier.last_pace.value,
        "recovery_ticks_remaining": soldier.recovery_ticks_remaining,
        "sprint_cooldown": soldier.sprint_cooldown,
        "alive": soldier.alive,
    }


def snapshot_recoveree(recoveree: RecovereeState) -> dict:
    return {
        "row": recoveree.pos.row,
        "col": recoveree.pos.col,
        "integrity": recoveree.integrity.value,
        "mobility": recoveree.mobility.value,
        "exposure": recoveree.exposure.value,
        "recovered": recoveree.recovered,
        "extracted": recoveree.extracted,
        "carried_by_id": recoveree.carried_by_id,
        "dragged_by_id": recoveree.dragged_by_id,
        "assigned_helper_id": recoveree.assigned_helper_id,
        "can_self_move": recoveree.can_self_move,
        "requires_assist": recoveree.requires_assist,
        "alive": recoveree.alive,
        "last_action": recoveree.last_action,
        "last_pace": None if recoveree.last_pace is None else recoveree.last_pace.value,
    }


def snapshot_grenade(grenade: Optional[GrenadeEvent]) -> Optional[dict]:
    if grenade is None:
        return None
    return {
        "row": grenade.pos.row,
        "col": grenade.pos.col,
        "active": grenade.active,
        "has_detonated": grenade.has_detonated,
        "absorb_actor_id": grenade.absorb_actor_id,
        "absorb_actor_kind": grenade.absorb_actor_kind,
        "absorb_intercept_pos": None if grenade.absorb_intercept_pos is None else {
            "row": grenade.absorb_intercept_pos.row,
            "col": grenade.absorb_intercept_pos.col,
        },
        "grenade_class": None if grenade.grenade_class is None else grenade.grenade_class.name,
        "intended_target_id": grenade.intended_target_id,
        "intended_target_kind": grenade.intended_target_kind,
        "intended_target_pos": None if grenade.intended_target_pos is None else {
            "row": grenade.intended_target_pos.row,
            "col": grenade.intended_target_pos.col,
        },
        "scatter_kind": grenade.scatter_kind,
        "locked_center_ids": list(grenade.locked_center_ids),
        "locked_adjacent_ids": list(grenade.locked_adjacent_ids),
        "locked_positions_by_id": {
            sid: {"row": pos.row, "col": pos.col}
            for sid, pos in grenade.locked_positions_by_id.items()
        },
        "locked_recoveree_on_center": grenade.locked_recoveree_on_center,
        "locked_recoveree_adjacent": grenade.locked_recoveree_adjacent,
        "locked_recoveree_pos": None if grenade.locked_recoveree_pos is None else {
            "row": grenade.locked_recoveree_pos.row,
            "col": grenade.locked_recoveree_pos.col,
        },
    }


def add_frame(state: SquadState, frame_list: List[dict], phase: str, label: str, events: Optional[List[str]] = None) -> None:
    frame_list.append({
        "frame_index": len(frame_list),
        "phase": phase,
        "tick": state.tick,
        "label": label,
        "grenade": snapshot_grenade(state.grenade),
        "recoveree": snapshot_recoveree(state.recoveree),
        "soldiers": [snapshot_soldier(s) for s in state.soldiers.values()],
        "domains": {
            "sv": state.squad_viability.value,
            "mc": state.mission_continuity.value,
            "cohesion": state.cohesion.value,
        },
        "events": events or [],
    })


def evaluate_terminal_failure(state: SquadState) -> Optional[str]:
    rec = state.recoveree
    if not rec.alive:
        return "recoveree_dead"
    if rec.extracted:
        return None
    if not state.alive_soldiers():
        if state.recoveree_mobile():
            return None
        return "recoveree_stranded_alive_no_rescuers"
    return None


def outcome_bucket(state: SquadState) -> str:
    if state.recoveree.extracted:
        total_dead = count_dead_total(state)
        total_wounded = count_wounded_total(state)
        if total_dead == 0 and total_wounded == 0:
            return "success_no_casualties"
        if total_dead == 0:
            return "success_minor_casualties"
        return "success_significant_casualties"

    if state.terminal_reason == "recoveree_stranded_alive_no_rescuers":
        return "recoveree_stranded_alive"

    return "mission_failed"


def ensure_solo_recoveree_continuation(state: SquadState) -> List[str]:
    events: List[str] = []
    if state.recoveree.extracted or not state.recoveree.alive:
        return events

    if not state.alive_soldiers() and state.recoveree_mobile():
        state.mission_phase = MissionPhase.WITHDRAWAL
        if state.recoveree.carried_by_id is not None:
            state.recoveree.carried_by_id = None
        if state.recoveree.dragged_by_id is not None:
            state.recoveree.dragged_by_id = None
        state.recoveree.assigned_helper_id = None
        events.append("no_soldiers_remaining_recoveree_continues_solo")
    return events


def run_single_episode(
    case_id: str,
    model_name: str,
    episode_index: int,
    seed: int,
    config: RuntimeConfig,
    game_map: MapModel,
) -> dict:
    global GLOBAL_MAP_FOR_ADEQUACY
    GLOBAL_MAP_FOR_ADEQUACY = game_map

    rng = random.Random(seed)
    state = build_case(case_id, config.recoveree_condition)

    refresh_state(state, game_map)
    update_squad_domains(state)

    frames: List[dict] = []
    add_frame(state, frames, "initial", "Initial entry posture")

    candidate_eval_snapshot: List[dict] = []
    chosen_policy_snapshot: Optional[dict] = None
    governing_domains_snapshot: List[str] = []
    governing_reason_snapshot: Optional[str] = None

    for tick in range(1, config.max_ticks + 1):
        state.tick = tick
        decrement_recovery(state)

        continuation_events = ensure_solo_recoveree_continuation(state)
        terminal_reason = evaluate_terminal_failure(state)
        if terminal_reason is not None:
            state.terminal_reason = terminal_reason
            state.mission_phase = MissionPhase.FAILED
            add_frame(state, frames, "terminal", f"Terminal failure at tick {tick}", continuation_events + [terminal_reason])
            break

        if state.mission_phase in (MissionPhase.ENTRY, MissionPhase.ADVANCE):
            if state.mission_phase == MissionPhase.ENTRY:
                state.mission_phase = MissionPhase.ADVANCE
            events = continuation_events + run_advance_step(state, game_map)
            refresh_state(state, game_map)
            update_squad_domains(state)
            add_frame(state, frames, "advance", f"Advance tick {tick}", events)
            if state.recoveree.extracted or state.mission_phase == MissionPhase.FAILED:
                break
            continue

        if state.mission_phase == MissionPhase.RECOVERY:
            events = continuation_events + run_recovery_step(state, game_map)
            refresh_state(state, game_map)
            update_squad_domains(state)
            add_frame(state, frames, "recovery", f"Recovery tick {tick}", events)
            if state.recoveree.extracted or state.mission_phase == MissionPhase.FAILED:
                break
            continue

        if state.mission_phase == MissionPhase.WITHDRAWAL:
            grenade_check_events, grenade_triggered = maybe_trigger_grenade(state, game_map, rng, config)

            if grenade_triggered and state.grenade is not None and state.grenade.active:
                add_frame(state, frames, "grenade_trigger", f"Grenade trigger at withdrawal tick {tick}", continuation_events + grenade_check_events + ["grenade_trigger_state_locked"])

                governing_domains, governing_reason = classify_governance(state)
                governing_domains_snapshot = governing_domains
                governing_reason_snapshot = governing_reason

                candidates = construct_candidate_policies(state, game_map)
                evals: List[PolicyEvaluation] = []
                for candidate in candidates:
                    feasible, feas_reason = evaluate_feasibility(candidate, state, game_map)
                    adequate, adeq_reason = evaluate_adequacy_for_model(model_name, candidate, state, governing_reason)
                    evals.append(PolicyEvaluation(candidate, feasible, feas_reason, adequate, adeq_reason))

                candidate_eval_snapshot = [{
                    "actor_id": e.candidate.actor_id,
                    "actor_kind": e.candidate.actor_kind,
                    "policy": e.candidate.policy_type.name,
                    "feasible": e.feasible,
                    "feasibility_reason": e.feasibility_reason,
                    "adequate": e.adequate,
                    "adequacy_reason": e.adequacy_reason,
                } for e in evals]

                chosen = select_policy_for_model(model_name, evals, state, governing_reason)
                chosen_policy_snapshot = None if chosen is None else {
                    "actor_id": chosen.candidate.actor_id,
                    "actor_kind": chosen.candidate.actor_kind,
                    "policy": chosen.candidate.policy_type.name,
                }

                response_events = execute_selected_grenade_policy(chosen, state, game_map)

                grenade_events = resolve_grenade(state)
                refresh_state(state, game_map)
                update_squad_domains(state)
                apply_post_event_caps(state)
                grenade_events = response_events + ["ordinary_withdrawal_skipped_this_tick"] + grenade_events
                grenade_events.extend(reconcile_recoveree_after_grenade(state))
                solo_events = ensure_solo_recoveree_continuation(state)
                add_frame(state, frames, "post_grenade", f"Grenade resolved at withdrawal tick {tick}", grenade_events + solo_events)

                terminal_reason = evaluate_terminal_failure(state)
                if terminal_reason is not None:
                    state.terminal_reason = terminal_reason
                    state.mission_phase = MissionPhase.FAILED
                    add_frame(state, frames, "terminal", f"Terminal failure at tick {tick}", [terminal_reason])
                    break
                continue

            withdraw_events = continuation_events + grenade_check_events + run_withdrawal_step(state, game_map, model_name, config)
            fire_events = [] if state.mission_phase == MissionPhase.COMPLETE else apply_background_fire(state, game_map, rng, config)

            refresh_state(state, game_map)
            update_squad_domains(state)
            withdraw_events.extend(maybe_recover_recoveree(state, game_map))
            withdraw_events.extend(ensure_solo_recoveree_continuation(state))
            add_frame(state, frames, "withdrawal", f"Withdrawal tick {tick}", withdraw_events + fire_events)

            terminal_reason = evaluate_terminal_failure(state)
            if terminal_reason is not None:
                state.terminal_reason = terminal_reason
                state.mission_phase = MissionPhase.FAILED
                add_frame(state, frames, "terminal", f"Terminal failure at tick {tick}", [terminal_reason])
                break

            if state.recoveree.extracted or state.mission_phase == MissionPhase.FAILED:
                break
            continue

        if state.mission_phase in (MissionPhase.COMPLETE, MissionPhase.FAILED):
            break

    if state.recoveree.extracted:
        state.mission_phase = MissionPhase.COMPLETE
    elif not state.recoveree.alive:
        state.mission_phase = MissionPhase.FAILED
        state.terminal_reason = state.terminal_reason or "recoveree_dead"
    elif state.tick >= config.max_ticks and not state.recoveree.extracted:
        state.mission_phase = MissionPhase.FAILED
        state.terminal_reason = state.terminal_reason or "time_expired"

    refresh_state(state, game_map)
    update_squad_domains(state)

    row = {
        "episode_index": episode_index,
        "seed": seed,
        "case_id": case_id,
        "model_name": model_name,
        "recoveree_condition": config.recoveree_condition,
        "map_mode": config.map_mode,
        "scalar_favor_mode": config.scalar_favor_mode,
        "screen_bonus_strength": config.screen_bonus_strength,
        "cluster_penalty_strength": config.cluster_penalty_strength,
        "recoveree_extracted": state.recoveree.extracted,
        "recoveree_alive": state.recoveree.alive,
        "recoveree_integrity": state.recoveree.integrity.value,
        "soldier_dead_count": count_dead_soldiers(state),
        "soldier_wounded_count": count_wounded_soldiers(state),
        "recoveree_dead_count": 1 if state.recoveree.integrity == IntegrityState.DEAD else 0,
        "recoveree_wounded_count": 1 if state.recoveree.integrity == IntegrityState.WOUNDED else 0,
        "dead_count_total": count_dead_total(state),
        "wounded_count_total": count_wounded_total(state),
        "total_casualties": count_total_casualties(state),
        "sacrifice_occurred": sacrifice_occurred(state),
        "outcome_bucket": outcome_bucket(state),
        "terminal_reason": state.terminal_reason,
        "sv_final": state.squad_viability.value,
        "mc_final": state.mission_continuity.value,
        "cohesion_final": state.cohesion.value,
        "final_phase": state.mission_phase.name,
        "grenade_triggered": bool(state.grenade is not None),
        "grenade_response_policy": None if chosen_policy_snapshot is None else chosen_policy_snapshot["policy"],
        "grenade_response_actor_id": None if chosen_policy_snapshot is None else chosen_policy_snapshot["actor_id"],
        "grenade_response_actor_kind": None if chosen_policy_snapshot is None else chosen_policy_snapshot["actor_kind"],
        "governing_reason": governing_reason_snapshot,
    }

    trace = {
        "episode_index": episode_index,
        "seed": seed,
        "case_id": case_id,
        "model_name": model_name,
        "sim_version": SIM_VERSION,
        "governing_domains": governing_domains_snapshot,
        "governing_reason": governing_reason_snapshot,
        "candidate_evaluations": candidate_eval_snapshot,
        "chosen_policy": chosen_policy_snapshot,
        "frames": frames,
        "final_summary": row,
    }

    return {"row": row, "trace": trace}


def choose_case_id(rng: random.Random, case_mode: str) -> str:
    scenarios = [
        "extraction_easy",
        "extraction_moderate",
        "extraction_difficult",
        "extraction_improbable",
    ]
    return rng.choice(scenarios) if case_mode == "mixed" else case_mode


def build_replay_trace_payload(
    case_id: str,
    episode_index: int,
    seed: int,
    config: RuntimeConfig,
    game_map: MapModel,
    per_model_results: Dict[str, dict],
    realized_map_mode: str,
    realized_map_seed: Optional[int],
) -> dict:
    return {
        "schema_version": SIM_SCHEMA_VERSION,
        "run_metadata": {
            "episode_index": episode_index,
            "case_id": case_id,
            "seed": seed,
            "enemy_fire_scale": config.enemy_fire_scale,
            "max_ticks": config.max_ticks,
            "grenade_trigger_prob": config.grenade_trigger_prob,
            "recoveree_condition": config.recoveree_condition,
            "map_mode": realized_map_mode,
            "map_seed": realized_map_seed,
            "cover_perturb_prob": config.cover_perturb_prob,
            "scalar_favor_mode": config.scalar_favor_mode,
            "screen_bonus_strength": config.screen_bonus_strength,
            "cluster_penalty_strength": config.cluster_penalty_strength,
            "generated_by": SIM_FILENAME,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        },
        "cover_config": COVER_CONFIG,
        "map": {
            "rows": game_map.rows,
            "cols": game_map.cols,
            "tiles": [{"row": tile.pos.row, "col": tile.pos.col, "height": tile.height, "cover": tile.cover.value} for _, tile in sorted(game_map.tiles.items())],
            "extraction_zone": [{"row": pos.row, "col": pos.col} for pos in sorted(game_map.extraction_zone, key=lambda p: (p.row, p.col))],
            "extraction_boundary": [{"row": pos.row, "col": pos.col} for pos in sorted(game_map.extraction_boundary, key=lambda p: (p.row, p.col))],
            "objective_zone": [{"row": pos.row, "col": pos.col} for pos in sorted(game_map.objective_zone, key=lambda p: (p.row, p.col))],
        },
        "models": {
            model_name: {
                "governing_domains": result["trace"]["governing_domains"],
                "governing_reason": result["trace"]["governing_reason"],
                "candidate_evaluations": result["trace"]["candidate_evaluations"],
                "chosen_policy": result["trace"]["chosen_policy"],
                "frames": result["trace"]["frames"],
                "final_summary": result["trace"]["final_summary"],
            }
            for model_name, result in per_model_results.items()
        },
    }


def run_batch(config: RuntimeConfig) -> Tuple[List[dict], List[dict]]:
    rows: List[dict] = []
    traces: List[dict] = []

    os.makedirs(config.output_dir, exist_ok=True)
    if config.write_replay_trace:
        os.makedirs(config.replay_dir, exist_ok=True)

    models = [MODEL_PVPP, MODEL_BASELINE_SELF]
    master_rng = random.Random(config.seed)

    for episode_index in range(config.runs):
        case_id = choose_case_id(master_rng, config.case_mode)
        episode_seed = master_rng.randint(0, 10_000_000)

        game_map, realized_map_mode, realized_map_seed = build_map_for_episode(config, case_id, episode_index, episode_seed)

        per_model_results: Dict[str, dict] = {}
        for model_name in models:
            result = run_single_episode(case_id, model_name, episode_index, episode_seed, config, game_map)
            result["row"]["map_seed"] = realized_map_seed
            rows.append(result["row"])
            traces.append(result["trace"])
            per_model_results[model_name] = result

        if config.write_replay_trace:
            payload = build_replay_trace_payload(
                case_id,
                episode_index,
                episode_seed,
                config,
                game_map,
                per_model_results,
                realized_map_mode,
                realized_map_seed,
            )
            replay_path = os.path.join(config.replay_dir, f"episode_{episode_index:04d}.json")
            with open(replay_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)

    return rows, traces


def pct(x: int, n: int) -> float:
    return 0.0 if n == 0 else round(100.0 * x / n, 2)


def summarize_rows(rows: List[dict]) -> List[dict]:
    models = sorted(set(r["model_name"] for r in rows))
    summaries: List[dict] = []
    for model_name in models:
        subset = [r for r in rows if r["model_name"] == model_name]
        n = len(subset)
        grenade_subset = [r for r in subset if r["grenade_triggered"]]

        no_cas = sum(r["outcome_bucket"] == "success_no_casualties" for r in subset)
        minor = sum(r["outcome_bucket"] == "success_minor_casualties" for r in subset)
        significant = sum(r["outcome_bucket"] == "success_significant_casualties" for r in subset)
        failed = sum(r["outcome_bucket"] == "mission_failed" for r in subset)
        stranded = sum(r["outcome_bucket"] == "recoveree_stranded_alive" for r in subset)

        summaries.append({
            "model_name": model_name,
            "runs": n,
            "success_no_casualties_pct": pct(no_cas, n),
            "success_minor_casualties_pct": pct(minor, n),
            "success_significant_casualties_pct": pct(significant, n),
            "mission_failed_pct": pct(failed, n),
            "recoveree_stranded_alive_pct": pct(stranded, n),
            "avg_soldier_dead": round(statistics.mean(r["soldier_dead_count"] for r in subset), 3),
            "avg_soldier_wounded": round(statistics.mean(r["soldier_wounded_count"] for r in subset), 3),
            "avg_recoveree_dead": round(statistics.mean(r["recoveree_dead_count"] for r in subset), 3),
            "avg_recoveree_wounded": round(statistics.mean(r["recoveree_wounded_count"] for r in subset), 3),
            "avg_dead_total": round(statistics.mean(r["dead_count_total"] for r in subset), 3),
            "avg_wounded_total": round(statistics.mean(r["wounded_count_total"] for r in subset), 3),
            "avg_total_casualties": round(statistics.mean(r["total_casualties"] for r in subset), 3),
            "sacrifice_pct": pct(sum(bool(r["sacrifice_occurred"]) for r in subset), n),
            "grenade_triggered_pct": pct(sum(bool(r["grenade_triggered"]) for r in subset), n),
            "recoveree_extracted_pct": pct(sum(bool(r["recoveree_extracted"]) for r in subset), n),
            "absorb_selected_pct_of_grenade_runs": pct(
                sum(r["grenade_response_policy"] == "ABSORB_GRENADE" for r in grenade_subset),
                len(grenade_subset),
            ),
            "carrier_escape_selected_pct_of_grenade_runs": pct(
                sum(r["grenade_response_policy"] == "OBJECTIVE_CARRIER_ESCAPE" for r in grenade_subset),
                len(grenade_subset),
            ),
            "scatter_selected_pct_of_grenade_runs": pct(
                sum(r["grenade_response_policy"] == "SCATTER" for r in grenade_subset),
                len(grenade_subset),
            ),
            "null_grenade_response_pct_of_grenade_runs": pct(
                sum(r["grenade_response_policy"] is None for r in grenade_subset),
                len(grenade_subset),
            ),
        })
    return summaries


def build_comparative_summary(rows: List[dict]) -> dict:
    by_episode: Dict[Tuple[int, str], Dict[str, dict]] = {}
    for row in rows:
        key = (row["episode_index"], row["case_id"])
        by_episode.setdefault(key, {})
        by_episode[key][row["model_name"]] = row

    pvpp_only_wins = 0
    baseline_only_wins = 0
    both_extract = 0
    neither_extract = 0

    pvpp_only_wins_with_grenade = 0
    pvpp_only_wins_with_absorb = 0
    pvpp_only_wins_baseline_carrier_escape = 0

    for pair in by_episode.values():
        if MODEL_PVPP not in pair or MODEL_BASELINE_SELF not in pair:
            continue

        pvpp = pair[MODEL_PVPP]
        base = pair[MODEL_BASELINE_SELF]

        if pvpp["recoveree_extracted"] and not base["recoveree_extracted"]:
            pvpp_only_wins += 1
            if pvpp["grenade_triggered"] or base["grenade_triggered"]:
                pvpp_only_wins_with_grenade += 1
            if pvpp["grenade_response_policy"] == "ABSORB_GRENADE":
                pvpp_only_wins_with_absorb += 1
            if base["grenade_response_policy"] == "OBJECTIVE_CARRIER_ESCAPE":
                pvpp_only_wins_baseline_carrier_escape += 1
        elif base["recoveree_extracted"] and not pvpp["recoveree_extracted"]:
            baseline_only_wins += 1
        elif base["recoveree_extracted"] and pvpp["recoveree_extracted"]:
            both_extract += 1
        else:
            neither_extract += 1

    total = pvpp_only_wins + baseline_only_wins + both_extract + neither_extract

    return {
        "paired_runs": total,
        "pvpp_only_extraction_wins": pvpp_only_wins,
        "baseline_only_extraction_wins": baseline_only_wins,
        "both_extract": both_extract,
        "neither_extract": neither_extract,
        "pvpp_only_wins_with_grenade": pvpp_only_wins_with_grenade,
        "pvpp_only_wins_with_absorb": pvpp_only_wins_with_absorb,
        "pvpp_only_wins_baseline_carrier_escape": pvpp_only_wins_baseline_carrier_escape,
    }


def write_csv(path: str, rows: List[dict]) -> None:
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: str, payload: dict | list) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def write_jsonl(path: str, items: List[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item) + "\n")


def print_summary(config: RuntimeConfig, summaries: List[dict]) -> None:
    if config.detailed_output:
        print("=" * 80)
        print(f"MISSION-FLOW BENCHMARK SUMMARY — {SIM_VERSION_LABEL}")
        print("=" * 80)
        print(f"Scenario: {config.case_mode}")
        print(f"Runs: {config.runs}")
        print(f"Seed: {config.seed}")
        print(f"Enemy Fire Scale: {config.enemy_fire_scale}")
        print(f"Max Ticks: {config.max_ticks}")
        print(f"Grenade Trigger Prob (eligible withdrawal tick): {config.grenade_trigger_prob}")
        print(f"Recoveree Condition: {config.recoveree_condition}")
        print(f"Map Mode: {config.map_mode}")
        print(f"Map Seed: {config.map_seed if config.map_seed is not None else 'derived_from_seed'}")
        print(f"Cover Perturb Prob: {config.cover_perturb_prob}")
        print(f"Scalar Favor Mode: {config.scalar_favor_mode}")
        print(f"Screen Bonus Strength: {config.screen_bonus_strength}")
        print(f"Cluster Penalty Strength: {config.cluster_penalty_strength}")
        print(f"Detailed Output: {config.detailed_output}")
        print("-" * 80)

        for s in summaries:
            print(f"MODEL: {s['model_name']}")
            print(f"  Runs: {s['runs']}")
            print(f"  Mission Success — No Casualties: {s['success_no_casualties_pct']}%")
            print(f"  Mission Success — Minor Casualties: {s['success_minor_casualties_pct']}%")
            print(f"  Mission Success — Significant Casualties: {s['success_significant_casualties_pct']}%")
            print(f"  Mission Failed: {s['mission_failed_pct']}%")
            print(f"  Recoveree Stranded Alive: {s['recoveree_stranded_alive_pct']}%")
            print(f"  Avg Soldier Dead: {s['avg_soldier_dead']}")
            print(f"  Avg Soldier Wounded: {s['avg_soldier_wounded']}")
            print(f"  Avg Recoveree Dead: {s['avg_recoveree_dead']}")
            print(f"  Avg Recoveree Wounded: {s['avg_recoveree_wounded']}")
            print(f"  Avg Dead Total: {s['avg_dead_total']}")
            print(f"  Avg Wounded Total: {s['avg_wounded_total']}")
            print(f"  Avg Total Casualties: {s['avg_total_casualties']}")
            print(f"  Sacrifice %: {s['sacrifice_pct']}%")
            print(f"  Grenade Triggered %: {s['grenade_triggered_pct']}%")
            print(f"  Absorb Selected % of Grenade Runs: {s['absorb_selected_pct_of_grenade_runs']}%")
            print(f"  Carrier Escape Selected % of Grenade Runs: {s['carrier_escape_selected_pct_of_grenade_runs']}%")
            print(f"  Scatter Selected % of Grenade Runs: {s['scatter_selected_pct_of_grenade_runs']}%")
            print(f"  Null Grenade Response % of Grenade Runs: {s['null_grenade_response_pct_of_grenade_runs']}%")
            print(f"  Recoveree Extracted %: {s['recoveree_extracted_pct']}%")
            print("-" * 80)
        return

    print("=" * 80)
    print(f"MISSION-FLOW BENCHMARK SUMMARY — {SIM_VERSION_LABEL}")
    print("=" * 80)
    print(f"Scenario: {config.case_mode}")
    print(f"Runs: {config.runs}")
    print("-" * 80)

    for s in summaries:
        print(f"MODEL: {s['model_name']}")
        print(f"  Recoveree Extracted %: {s['recoveree_extracted_pct']}%")
        print(f"  Avg Dead Total: {s['avg_dead_total']}")
        print(f"  Avg Wounded Total: {s['avg_wounded_total']}")
        print(f"  Avg Total Casualties: {s['avg_total_casualties']}")
        print("-" * 80)


def print_comparative_summary(comparative: dict, detailed_output: bool = False) -> None:
    print("=" * 80)
    print("EXTRACTION ADVANTAGE DECOMPOSITION")
    print("=" * 80)
    print(f"PVPP-only extraction wins: {comparative['pvpp_only_extraction_wins']}")
    print(f"Baseline-only extraction wins: {comparative['baseline_only_extraction_wins']}")
    if detailed_output:
        print(f"Paired runs: {comparative['paired_runs']}")
        print(f"Both extract: {comparative['both_extract']}")
        print(f"Neither extract: {comparative['neither_extract']}")
        print(f"PVPP-only wins with grenade: {comparative['pvpp_only_wins_with_grenade']}")
        print(f"PVPP-only wins with absorb: {comparative['pvpp_only_wins_with_absorb']}")
        print(f"PVPP-only wins where baseline used carrier escape: {comparative['pvpp_only_wins_baseline_carrier_escape']}")
    print("-" * 80)


def parse_args() -> RuntimeConfig:
    parser = argparse.ArgumentParser(description=f"{SIM_VERSION_LABEL} with restored dust/smoke ground-impact trace events.")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--scenario", type=str, default="mixed", choices=["mixed", "extraction_easy", "extraction_moderate", "extraction_difficult", "extraction_improbable"], help="Named extraction scenario preset or mixed.")
    parser.add_argument(
        "--enemy-fire",
        type=str,
        default=None,
        choices=list(ENEMY_FIRE_PRESETS.keys()),
        help="Named enemy-fire intensity preset.",
    )
    parser.add_argument("--max-ticks", type=int, default=DEFAULT_MAX_TICKS)
    parser.add_argument(
        "--grenade-frequency",
        type=str,
        default=None,
        choices=list(GRENADE_FREQUENCY_PRESETS.keys()),
        help="Named grenade trigger frequency preset.",
    )
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--replay-dir", type=str, default=DEFAULT_REPLAY_DIR)
    parser.add_argument("--write-replay-trace", action="store_true")
    parser.add_argument(
        "--recoveree-condition",
        type=str,
        default=None,
        choices=list(RECOVEREE_CONDITION_PRESETS.keys()),
        help="Named recoveree condition preset.",
    )
    parser.add_argument(
        "--map-mode",
        type=str,
        default="varied",
        choices=list(MAP_MODE_PRESETS.keys()),
        help="Named map-mode preset.",
    )
    parser.add_argument(
        "--map-seed",
        type=int,
        default=None,
        help="Base seed for map randomization. If omitted, uses --seed.",
    )
    parser.add_argument("--enemy-fire-scale", type=float, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--grenade-trigger-prob", type=float, default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--cover-perturb-prob",
        type=float,
        default=DEFAULT_COVER_PERTURB_PROB,
        help="Probability of toggling eligible cover cells in randomized_cover mode.",
    )
    parser.add_argument(
        "--scalar-favor-mode",
        type=str,
        default=DEFAULT_SCALAR_FAVOR_MODE,
        choices=["off", "screen_tradeoff"],
        help="Optional non-grenade withdrawal mode that favors smoother escort-screen tradeoffs.",
    )
    parser.add_argument(
        "--screen-bonus-strength",
        type=float,
        default=DEFAULT_SCREEN_BONUS_STRENGTH,
        help="Strength of escort-screen protection during ordinary withdrawal when scalar-favor mode is enabled.",
    )
    parser.add_argument(
        "--cluster-penalty-strength",
        type=float,
        default=DEFAULT_CLUSTER_PENALTY_STRENGTH,
        help="Penalty strength for over-clustering near the carrier during ordinary withdrawal when scalar-favor mode is enabled.",
    )
    parser.add_argument(
        "--detailed-output",
        action="store_true",
        help="Print the full detailed benchmark statistics block instead of the compact summary output.",
    )

    args = parser.parse_args()
    if args.runs <= 0:
        parser.error("--runs must be > 0")
    if args.max_ticks <= 0:
        parser.error("--max-ticks must be > 0")

    scenario_enemy_fire = None
    scenario_grenade_frequency = None
    scenario_recoveree_condition = None
    if args.scenario != "mixed":
        scenario_enemy_fire = SCENARIO_PRESETS[args.scenario]["enemy_fire"]
        scenario_grenade_frequency = SCENARIO_PRESETS[args.scenario]["grenade_frequency"]
        scenario_recoveree_condition = SCENARIO_PRESETS[args.scenario]["recoveree_condition"]

    enemy_fire_label = (
        args.enemy_fire
        if args.enemy_fire is not None
        else scenario_enemy_fire if scenario_enemy_fire is not None
        else "moderate"
    )
    grenade_frequency_label = (
        args.grenade_frequency
        if args.grenade_frequency is not None
        else scenario_grenade_frequency if scenario_grenade_frequency is not None
        else "sometimes"
    )
    recoveree_condition_label = (
        args.recoveree_condition
        if args.recoveree_condition is not None
        else scenario_recoveree_condition if scenario_recoveree_condition is not None
        else "severely_wounded"
    )

    resolved_enemy_fire_scale = ENEMY_FIRE_PRESETS[enemy_fire_label]
    if args.enemy_fire_scale is not None:
        resolved_enemy_fire_scale = args.enemy_fire_scale
    if resolved_enemy_fire_scale < 0:
        parser.error("--enemy-fire-scale must be >= 0")

    resolved_grenade_trigger_prob = GRENADE_FREQUENCY_PRESETS[grenade_frequency_label]
    if args.grenade_trigger_prob is not None:
        resolved_grenade_trigger_prob = args.grenade_trigger_prob
    if not (0.0 <= resolved_grenade_trigger_prob <= 1.0):
        parser.error("--grenade-trigger-prob must be between 0.0 and 1.0")
    if not (0.0 <= args.cover_perturb_prob <= 1.0):
        parser.error("--cover-perturb-prob must be between 0.0 and 1.0")
    if args.screen_bonus_strength < 0.0:
        parser.error("--screen-bonus-strength must be >= 0.0")
    if args.cluster_penalty_strength < 0.0:
        parser.error("--cluster-penalty-strength must be >= 0.0")

    return RuntimeConfig(
        runs=args.runs,
        seed=args.seed,
        case_mode=args.scenario,
        enemy_fire_scale=resolved_enemy_fire_scale,
        max_ticks=args.max_ticks,
        grenade_trigger_prob=resolved_grenade_trigger_prob,
        output_dir=args.output_dir,
        replay_dir=args.replay_dir,
        write_replay_trace=args.write_replay_trace,
        recoveree_condition=RECOVEREE_CONDITION_PRESETS[recoveree_condition_label],
        map_mode=MAP_MODE_PRESETS[args.map_mode],
        map_seed=args.map_seed,
        cover_perturb_prob=args.cover_perturb_prob,
        scalar_favor_mode=args.scalar_favor_mode,
        screen_bonus_strength=args.screen_bonus_strength,
        cluster_penalty_strength=args.cluster_penalty_strength,
        detailed_output=args.detailed_output,
    )


def main() -> None:
    config = parse_args()
    os.makedirs(config.output_dir, exist_ok=True)

    rows, traces = run_batch(config)
    summaries = summarize_rows(rows)
    comparative = build_comparative_summary(rows)

    write_csv(os.path.join(config.output_dir, "per_run_rows.csv"), rows)
    write_jsonl(os.path.join(config.output_dir, "per_run_traces.jsonl"), traces)
    write_csv(os.path.join(config.output_dir, "model_summary.csv"), summaries)
    write_json(os.path.join(config.output_dir, "model_summary.json"), summaries)
    write_json(os.path.join(config.output_dir, "comparative_summary.json"), comparative)

    print_summary(config, summaries)
    print_comparative_summary(comparative, config.detailed_output)
    print(f"Output directory: {config.output_dir}")
    if config.write_replay_trace:
        print(f"Replay trace directory: {config.replay_dir}")


if __name__ == "__main__":
    main()
