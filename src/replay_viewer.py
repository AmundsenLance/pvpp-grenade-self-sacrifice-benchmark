from __future__ import annotations

import argparse
import json
import math
import re
import shutil
import subprocess
from pathlib import Path
import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, List, Optional, Tuple


TITLE = "Grenade Replay Viewer V18"
DEFAULT_CELL_SIZE = 42
DEFAULT_MARGIN = 24
DEFAULT_GAP = 48
DEFAULT_FRAME_DELAY_MS = 700

MODEL_LEFT = "pvpp"
MODEL_RIGHT = "baseline_self_preserving"

GROUND_IMPACT_RE = re.compile(
    r"^bullet_ground_impact_(?P<actor>.+)_(?P<effect>dust|smoke)_r(?P<row>\d+)_c(?P<col>\d+)$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay viewer for grenade replay traces with gunshot and grenade sound support.")
    parser.add_argument(
        "--dir",
        type=str,
        default="grenade_missionflow_replays_v3",
        help="Directory containing replay trace JSON files",
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Specific replay file to open first. Accepts exact filename or unique substring, e.g. episode_0905_case_D.json or 0905",
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=DEFAULT_FRAME_DELAY_MS,
        help=f"Initial autoplay frame delay in milliseconds (default: {DEFAULT_FRAME_DELAY_MS})",
    )
    parser.add_argument(
        "--impact-sound",
        type=str,
        default="gunshot.mp3",
        help="Path to a short impact sound file played when ground-impact events appear. Defaults to gunshot.mp3.",
    )
    parser.add_argument(
        "--grenade-sound",
        type=str,
        default="grenade.mp3",
        help="Path to a short grenade explosion sound file played when grenade detonation appears. Defaults to grenade.mp3.",
    )
    parser.add_argument(
        "--mute",
        action="store_true",
        help="Disable sound playback.",
    )
    return parser.parse_args()


class ReplayViewer:
    def __init__(
        self,
        root: tk.Tk,
        replay_dir: str,
        delay_ms: int,
        initial_file: Optional[str] = None,
        impact_sound: Optional[str] = None,
        grenade_sound: Optional[str] = None,
        mute: bool = False,
    ) -> None:
        self.root = root
        self.root.title(TITLE)

        self.replay_dir = Path(replay_dir)
        self.cell_size = DEFAULT_CELL_SIZE
        self.margin = DEFAULT_MARGIN
        self.board_gap = DEFAULT_GAP

        self.trace_files: List[Path] = self.discover_trace_files(self.replay_dir)
        if not self.trace_files:
            raise FileNotFoundError(f"No replay trace JSON files found in: {self.replay_dir}")

        self.trace_index = self.resolve_initial_trace_index(initial_file)
        self.trace_data: Dict[str, Any] = {}
        self.map_data: Dict[str, Any] = {}
        self.model_names: List[str] = []
        self.frames_by_model: Dict[str, List[Dict[str, Any]]] = {}
        self.max_frames = 0
        self.frame_index = 0

        self.autoplay = False
        self.after_id: Optional[str] = None

        self.status_var = tk.StringVar()
        self.frame_var = tk.StringVar()
        self.file_var = tk.StringVar()
        self.jump_var = tk.StringVar()
        self.delay_var = tk.IntVar(value=max(100, delay_ms))

        self.mute = mute
        self.afplay_path = shutil.which("afplay")
        self.impact_sound_path = self.resolve_sound_path(impact_sound)
        self.grenade_sound_path = self.resolve_sound_path(grenade_sound)
        self.impact_sound_enabled = (not self.mute) and self.afplay_path is not None and self.impact_sound_path is not None
        self.grenade_sound_enabled = (not self.mute) and self.afplay_path is not None and self.grenade_sound_path is not None
        self.last_impact_sound_frame_key: Optional[Tuple[int, int]] = None
        self.last_grenade_sound_frame_key: Optional[Tuple[int, int]] = None

        self.build_ui()
        self.load_trace(self.trace_index)
        self.render()
        self.position_window()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.bind("<Left>", lambda _: self.prev_frame())
        self.root.bind("<Right>", lambda _: self.next_frame())
        self.root.bind("<Up>", lambda _: self.prev_trace())
        self.root.bind("<Down>", lambda _: self.next_trace())
        self.root.bind("<space>", lambda _: self.toggle_autoplay())

    @staticmethod
    def discover_trace_files(replay_dir: Path) -> List[Path]:
        if not replay_dir.exists():
            return []
        return sorted([p for p in replay_dir.glob("*.json") if p.is_file()], key=lambda p: p.name)

    def resolve_initial_trace_index(self, initial_file: Optional[str]) -> int:
        if not initial_file:
            return 0

        target = initial_file.strip()
        if not target:
            return 0

        exact_matches = [i for i, p in enumerate(self.trace_files) if p.name == target]
        if len(exact_matches) == 1:
            return exact_matches[0]

        substring_matches = [i for i, p in enumerate(self.trace_files) if target in p.name]
        if len(substring_matches) == 1:
            return substring_matches[0]

        if len(exact_matches) > 1 or len(substring_matches) > 1:
            raise ValueError(
                f"Initial file selector '{target}' matched multiple replay files. Use a more specific filename."
            )

        raise FileNotFoundError(f"Could not find replay file '{target}' in {self.replay_dir}")

    def resolve_sound_path(self, raw_value: Optional[str]) -> Optional[Path]:
        if raw_value is None:
            return None
        raw = raw_value.strip()
        if not raw:
            return None

        candidate = Path(raw).expanduser()
        if candidate.is_file():
            return candidate.resolve()

        cwd_candidate = Path.cwd() / raw
        if cwd_candidate.is_file():
            return cwd_candidate.resolve()

        viewer_dir_candidate = Path(__file__).resolve().parent / raw
        if viewer_dir_candidate.is_file():
            return viewer_dir_candidate.resolve()

        replay_dir_candidate = self.replay_dir / raw
        if replay_dir_candidate.is_file():
            return replay_dir_candidate.resolve()

        return None

    def build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=8)
        outer.pack(fill="both", expand=True)

        control_bar = ttk.Frame(outer)
        control_bar.pack(fill="x", pady=(0, 8))

        ttk.Button(control_bar, text="Prev File", command=self.prev_trace).pack(side="left")
        ttk.Button(control_bar, text="Next File", command=self.next_trace).pack(side="left", padx=(6, 6))
        ttk.Button(control_bar, text="Reload Files", command=self.reload_files).pack(side="left", padx=(0, 12))

        ttk.Button(control_bar, text="<<", command=self.prev_frame).pack(side="left")
        ttk.Button(control_bar, text=">>", command=self.next_frame).pack(side="left", padx=(6, 12))

        self.play_button = ttk.Button(control_bar, text="Play", command=self.toggle_autoplay)
        self.play_button.pack(side="left")

        ttk.Button(control_bar, text="Restart", command=self.restart_frames).pack(side="left", padx=(6, 12))

        ttk.Label(control_bar, text="Speed").pack(side="left", padx=(6, 4))
        self.speed_scale = tk.Scale(
            control_bar,
            from_=100,
            to=2000,
            orient="horizontal",
            variable=self.delay_var,
            showvalue=True,
            resolution=50,
            length=180,
            command=self.on_speed_change,
        )
        self.speed_scale.pack(side="left")

        ttk.Button(control_bar, text="Faster", command=self.make_faster).pack(side="left", padx=(8, 4))
        ttk.Button(control_bar, text="Slower", command=self.make_slower).pack(side="left", padx=(0, 12))

        jump_bar = ttk.Frame(outer)
        jump_bar.pack(fill="x", pady=(0, 8))

        ttk.Label(jump_bar, text="Open file").pack(side="left")
        jump_entry = ttk.Entry(jump_bar, textvariable=self.jump_var, width=28)
        jump_entry.pack(side="left", padx=(6, 6))
        jump_entry.bind("<Return>", lambda _: self.jump_to_file())

        ttk.Button(jump_bar, text="Go", command=self.jump_to_file).pack(side="left", padx=(0, 12))

        ttk.Label(jump_bar, textvariable=self.file_var).pack(side="left", padx=(12, 0))
        ttk.Label(jump_bar, textvariable=self.frame_var).pack(side="right")

        self.canvas = tk.Canvas(outer, bg="white", highlightthickness=0)
        self.canvas.pack(fill="both", expand=True)

        status_bar = ttk.Frame(outer)
        status_bar.pack(fill="x", pady=(8, 0))
        ttk.Label(status_bar, textvariable=self.status_var, justify="left").pack(side="left")

    def position_window(self) -> None:
        self.root.update_idletasks()

        width = max(1300, self.canvas.winfo_reqwidth() + 20)
        height = max(960, self.canvas.winfo_reqheight() + 20)

        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()

        x = 20
        y = 20

        width = min(width, screen_w - 40)
        height = min(height, screen_h - 80)

        self.root.geometry(f"{width}x{height}+{x}+{y}")

    def on_speed_change(self, _value: str) -> None:
        if self.autoplay:
            if self.after_id is not None:
                self.root.after_cancel(self.after_id)
                self.after_id = None
            self.schedule_next_frame()

    def make_faster(self) -> None:
        self.delay_var.set(max(100, self.delay_var.get() - 100))
        self.on_speed_change(str(self.delay_var.get()))

    def make_slower(self) -> None:
        self.delay_var.set(min(3000, self.delay_var.get() + 100))
        self.on_speed_change(str(self.delay_var.get()))

    def current_delay_ms(self) -> int:
        return max(100, int(self.delay_var.get()))

    def load_trace(self, index: int, preserve_autoplay: bool = False) -> None:
        if not preserve_autoplay:
            self.stop_autoplay()

        trace_path = self.trace_files[index]
        with open(trace_path, "r", encoding="utf-8") as f:
            self.trace_data = json.load(f)

        self.map_data = self.trace_data["map"]
        models = self.trace_data["models"]

        preferred = [MODEL_LEFT, MODEL_RIGHT]
        actual = list(models.keys())

        ordered: List[str] = []
        for model_name in preferred:
            if model_name in models:
                ordered.append(model_name)
        for model_name in actual:
            if model_name not in ordered:
                ordered.append(model_name)

        self.model_names = ordered[:2]
        self.frames_by_model = {
            model_name: models[model_name].get("frames", [])
            for model_name in self.model_names
        }
        self.max_frames = max((len(frames) for frames in self.frames_by_model.values()), default=0)
        self.frame_index = 0

        self.file_var.set(f"File: {trace_path.name}")
        self.update_frame_var()
        self.last_impact_sound_frame_key = None
        self.last_grenade_sound_frame_key = None

    def reload_files(self) -> None:
        current_name = self.trace_files[self.trace_index].name if self.trace_files else None
        refreshed = self.discover_trace_files(self.replay_dir)
        if not refreshed:
            self.status_var.set(f"No replay trace JSON files found in: {self.replay_dir}")
            return

        self.trace_files = refreshed
        if current_name is not None:
            matches = [i for i, p in enumerate(self.trace_files) if p.name == current_name]
            self.trace_index = matches[0] if matches else 0
        else:
            self.trace_index = 0

        self.load_trace(self.trace_index)
        self.render()
        self.status_var.set(f"Reloaded {len(self.trace_files)} replay file(s) from {self.replay_dir}")

    def update_frame_var(self) -> None:
        current = min(self.frame_index + 1, max(self.max_frames, 1))
        self.frame_var.set(f"Frame {current}/{max(self.max_frames, 1)}")

    def restart_frames(self) -> None:
        self.stop_autoplay()
        self.frame_index = 0
        self.update_frame_var()
        self.last_impact_sound_frame_key = None
        self.last_grenade_sound_frame_key = None
        self.render()

    def prev_trace(self) -> None:
        self.trace_index = (self.trace_index - 1) % len(self.trace_files)
        self.load_trace(self.trace_index)
        self.render()

    def next_trace(self) -> None:
        self.trace_index = (self.trace_index + 1) % len(self.trace_files)
        self.load_trace(self.trace_index)
        self.render()

    def jump_to_file(self) -> None:
        selector = self.jump_var.get().strip()
        if not selector:
            return

        exact_matches = [i for i, p in enumerate(self.trace_files) if p.name == selector]
        if len(exact_matches) == 1:
            self.trace_index = exact_matches[0]
            self.load_trace(self.trace_index)
            self.render()
            return

        substring_matches = [i for i, p in enumerate(self.trace_files) if selector in p.name]
        if len(substring_matches) == 1:
            self.trace_index = substring_matches[0]
            self.load_trace(self.trace_index)
            self.render()
            return

        if len(substring_matches) == 0:
            self.status_var.set(
                f"No replay file matched '{selector}'.\n"
                f"Use exact filename or a unique substring.\n"
                f"Example: 0905 or episode_0905_case_D.json"
            )
            return

        self.status_var.set(
            f"'{selector}' matched multiple files.\n"
            f"Use a more specific selector.\n"
            f"Matches: {', '.join(self.trace_files[i].name for i in substring_matches[:5])}"
        )

    def prev_frame(self) -> None:
        self.stop_autoplay()
        if self.max_frames == 0:
            return
        self.frame_index = max(0, self.frame_index - 1)
        self.update_frame_var()
        self.render()

    def next_frame(self) -> None:
        self.stop_autoplay()
        if self.max_frames == 0:
            return
        self.frame_index = min(self.max_frames - 1, self.frame_index + 1)
        self.update_frame_var()
        self.render()

    def toggle_autoplay(self) -> None:
        if self.autoplay:
            self.stop_autoplay()
        else:
            self.start_autoplay()

    def start_autoplay(self) -> None:
        if self.max_frames == 0:
            return
        self.autoplay = True
        self.play_button.config(text="Pause")
        self.schedule_next_frame()

    def stop_autoplay(self) -> None:
        self.autoplay = False
        self.play_button.config(text="Play")
        if self.after_id is not None:
            self.root.after_cancel(self.after_id)
            self.after_id = None

    def schedule_next_frame(self) -> None:
        if not self.autoplay:
            return
        self.after_id = self.root.after(self.current_delay_ms(), self.autoplay_step)

    def autoplay_step(self) -> None:
        if not self.autoplay:
            return

        if self.frame_index < self.max_frames - 1:
            self.frame_index += 1
            self.update_frame_var()
            self.render()
            self.schedule_next_frame()
            return

        self.trace_index = (self.trace_index + 1) % len(self.trace_files)
        self.load_trace(self.trace_index, preserve_autoplay=True)
        self.render()
        self.schedule_next_frame()

    def on_close(self) -> None:
        self.stop_autoplay()
        self.root.destroy()

    def render(self) -> None:
        self.canvas.delete("all")

        rows = self.map_data["rows"]
        cols = self.map_data["cols"]

        board_width = cols * self.cell_size
        board_height = rows * self.cell_size
        legend_height = 140
        total_width = (
            self.margin * 2
            + board_width * len(self.model_names)
            + self.board_gap * max(0, len(self.model_names) - 1)
        )
        total_height = self.margin * 2 + board_height + 230 + legend_height

        self.canvas.config(width=total_width, height=total_height, scrollregion=(0, 0, total_width, total_height))

        meta = self.trace_data.get("run_metadata", {})
        case_id = meta.get("case_id", "?")
        seed = meta.get("seed", "?")
        gen_by = meta.get("generated_by", "?")
        active_file = self.trace_files[self.trace_index].name if self.trace_files else "?"

        self.canvas.create_text(
            self.margin,
            8,
            anchor="nw",
            text=f"Replay: {active_file}",
            font=("Helvetica", 14, "bold"),
            fill="#111111",
        )
        self.canvas.create_text(
            self.margin,
            28,
            anchor="nw",
            text=f"Case {case_id}   Seed {seed}   Generated by {gen_by}",
            font=("Helvetica", 11),
            fill="#333333",
        )

        for idx, model_name in enumerate(self.model_names):
            origin_x = self.margin + idx * (board_width + self.board_gap)
            origin_y = 56

            frames = self.frames_by_model[model_name]
            frame = frames[min(self.frame_index, len(frames) - 1)] if frames else None

            self.draw_board(origin_x, origin_y, model_name, frame)

        self.draw_legend(
            x=self.margin,
            y=56 + board_height + 112,
            width=total_width - (2 * self.margin),
        )

        self.maybe_play_frame_sounds()
        self.update_status_text()

    def draw_board(self, origin_x: int, origin_y: int, model_name: str, frame: Optional[Dict[str, Any]]) -> None:
        rows = self.map_data["rows"]
        cols = self.map_data["cols"]
        board_width = cols * self.cell_size

        display_name = {
            "pvpp": "PVPP",
            "baseline_self_preserving": "Scalar / self-preserving",
        }.get(model_name, model_name)

        self.canvas.create_text(
            origin_x,
            origin_y - 16,
            anchor="nw",
            text=display_name,
            font=("Helvetica", 12, "bold"),
            fill="#222222",
        )

        self.draw_grid(origin_x, origin_y)
        self.draw_extraction_zone(origin_x, origin_y)
        self.draw_objective_zone(origin_x, origin_y)

        if frame is None:
            self.canvas.create_text(
                origin_x + board_width / 2,
                origin_y + (rows * self.cell_size) / 2,
                text="No frames",
                font=("Helvetica", 16, "bold"),
                fill="#aa0000",
            )
            return

        self.draw_ground_impacts(origin_x, origin_y, frame.get("events", []))
        self.draw_grenade(origin_x, origin_y, frame.get("grenade"), frame.get("events", []))
        self.draw_soldiers(origin_x, origin_y, frame.get("soldiers", []), frame.get("grenade"))
        self.draw_recoveree(origin_x, origin_y, frame.get("recoveree"))
        self.draw_frame_footer(origin_x, origin_y, board_width, frame)

    def draw_grid(self, origin_x: int, origin_y: int) -> None:
        for tile in self.map_data["tiles"]:
            row = tile["row"]
            col = tile["col"]
            x1, y1, x2, y2 = self.cell_bbox(origin_x, origin_y, row, col)

            fill = self.tile_fill(tile["height"])
            self.canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline="#8b8b8b", width=1)

            self.canvas.create_text(
                x1 + 4,
                y1 + 3,
                anchor="nw",
                text=str(tile["height"]),
                font=("Helvetica", 7),
                fill="#223322",
            )

            self.draw_cover_icon(origin_x, origin_y, row, col, tile["cover"])

    def draw_extraction_zone(self, origin_x: int, origin_y: int) -> None:
        for pos in self.map_data.get("extraction_zone", []):
            row = pos["row"]
            col = pos["col"]
            x1, y1, x2, y2 = self.cell_bbox(origin_x, origin_y, row, col)
            self.canvas.create_rectangle(
                x1 + 3,
                y1 + 3,
                x2 - 3,
                y2 - 3,
                outline="#1e88e5",
                width=2,
            )

        for pos in self.map_data.get("extraction_boundary", []):
            row = pos["row"]
            col = pos["col"]
            x1, y1, x2, y2 = self.cell_bbox(origin_x, origin_y, row, col)
            self.canvas.create_rectangle(
                x1 + 8,
                y1 + 8,
                x2 - 8,
                y2 - 8,
                outline="#90caf9",
                width=1,
            )

    def draw_objective_zone(self, origin_x: int, origin_y: int) -> None:
        for pos in self.map_data.get("objective_zone", []):
            row = pos["row"]
            col = pos["col"]
            x1, y1, x2, y2 = self.cell_bbox(origin_x, origin_y, row, col)
            self.canvas.create_rectangle(
                x1 + 5,
                y1 + 5,
                x2 - 5,
                y2 - 5,
                outline="#f39c12",
                width=2,
            )

    def draw_cover_icon(self, origin_x: int, origin_y: int, row: int, col: int, cover: str) -> None:
        if cover == "N":
            return

        x1, y1, x2, y2 = self.cell_bbox(origin_x, origin_y, row, col)
        cx = (x1 + x2) / 2
        cy = (y1 + y2) / 2 + 6

        if cover == "P":
            self.canvas.create_polygon(
                cx, cy - 10,
                cx - 8, cy + 4,
                cx + 8, cy + 4,
                fill="#2e7d32",
                outline="#1b5e20",
                width=1,
            )
            self.canvas.create_line(
                cx, cy + 4,
                cx, cy + 11,
                fill="#6d4c41",
                width=2,
            )
        elif cover == "S":
            wall_x1 = cx - 12
            wall_y1 = cy - 7
            wall_x2 = cx + 12
            wall_y2 = cy + 5
            self.canvas.create_rectangle(
                wall_x1, wall_y1, wall_x2, wall_y2,
                fill="#8d6e63",
                outline="#4e342e",
                width=1,
            )
            # Brick rows
            self.canvas.create_line(wall_x1, cy - 1, wall_x2, cy - 1, fill="#5d4037", width=1)
            # Vertical brick joints, staggered a bit to read as masonry.
            for x in (wall_x1 + 6, wall_x1 + 18):
                self.canvas.create_line(x, wall_y1, x, cy - 1, fill="#5d4037", width=1)
            for x in (wall_x1 + 3, wall_x1 + 15, wall_x1 + 21):
                self.canvas.create_line(x, cy - 1, x, wall_y2, fill="#5d4037", width=1)

    def draw_ground_impacts(self, origin_x: int, origin_y: int, events: List[str]) -> None:
        impacts = self.parse_ground_impact_events(events)
        for impact in impacts:
            row = impact["row"]
            col = impact["col"]
            effect = impact["effect"]

            x1, y1, x2, y2 = self.cell_bbox(origin_x, origin_y, row, col)
            cx = (x1 + x2) / 2
            cy = (y1 + y2) / 2

            if effect == "dust":
                fill = "#c7b299"
                outline = "#9c7f62"
            else:
                fill = "#cfd8dc"
                outline = "#90a4ae"

            offsets = [(-4, -2), (0, -3), (4, -1), (-2, 2), (3, 3)]
            radii = [2.5, 3, 2.5, 2, 2]
            for (dx, dy), r in zip(offsets, radii):
                self.canvas.create_oval(
                    cx + dx - r,
                    cy + dy - r,
                    cx + dx + r,
                    cy + dy + r,
                    fill=fill,
                    outline=outline,
                    width=1,
                )

    def parse_ground_impact_events(self, events: List[str]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for event in events:
            match = GROUND_IMPACT_RE.match(event)
            if not match:
                continue
            out.append(
                {
                    "actor": match.group("actor"),
                    "effect": match.group("effect"),
                    "row": int(match.group("row")),
                    "col": int(match.group("col")),
                }
            )
        return out

    def frame_has_ground_impacts(self) -> bool:
        for model_name in self.model_names:
            frames = self.frames_by_model.get(model_name, [])
            if not frames:
                continue
            frame = frames[min(self.frame_index, len(frames) - 1)]
            if self.parse_ground_impact_events(frame.get("events", [])):
                return True
        return False

    def frame_has_grenade_detonation(self) -> bool:
        for model_name in self.model_names:
            frames = self.frames_by_model.get(model_name, [])
            if not frames:
                continue
            frame = frames[min(self.frame_index, len(frames) - 1)]
            events = frame.get("events", [])
            if "grenade_detonated" in events:
                return True
        return False

    def play_sound(self, sound_path: Optional[Path]) -> None:
        if self.afplay_path is None or sound_path is None or self.mute:
            return
        try:
            subprocess.Popen(
                [str(self.afplay_path), str(sound_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass

    def maybe_play_frame_sounds(self) -> None:
        frame_key = (self.trace_index, self.frame_index)

        if self.frame_has_ground_impacts() and self.impact_sound_enabled:
            if self.last_impact_sound_frame_key != frame_key:
                self.last_impact_sound_frame_key = frame_key
                self.play_sound(self.impact_sound_path)

        if self.frame_has_grenade_detonation() and self.grenade_sound_enabled:
            if self.last_grenade_sound_frame_key != frame_key:
                self.last_grenade_sound_frame_key = frame_key
                self.play_sound(self.grenade_sound_path)

    def draw_grenade(self, origin_x: int, origin_y: int, grenade: Optional[Dict[str, Any]], events: List[str]) -> None:
        if not grenade:
            return

        row = grenade["row"]
        col = grenade["col"]
        x1, y1, x2, y2 = self.cell_bbox(origin_x, origin_y, row, col)
        cx = (x1 + x2) / 2
        cy = (y1 + y2) / 2

        if grenade.get("active", False):
            self.canvas.create_oval(
                cx - 7, cy - 7, cx + 7, cy + 7,
                fill="#d32f2f", outline="#7f0000", width=2
            )
            self.canvas.create_oval(
                cx - 13, cy - 13, cx + 13, cy + 13,
                outline="#ff5252", width=1
            )
            return

        if grenade.get("has_detonated", False) or "grenade_detonated" in events:
            self.draw_grenade_explosion(cx, cy, x1, y1, x2, y2)

    def draw_grenade_explosion(self, cx: float, cy: float, x1: float, y1: float, x2: float, y2: float) -> None:
        cloud_specs = [
            (-10, -6, 10, "#ff8a65", "#c62828"),
            (0, -12, 11, "#ff7043", "#bf360c"),
            (10, -5, 9, "#ff8a65", "#c62828"),
            (-12, 6, 8, "#ffab91", "#d84315"),
            (0, 9, 12, "#ff7043", "#bf360c"),
            (12, 7, 8, "#ffab91", "#d84315"),
        ]
        for dx, dy, r, fill, outline in cloud_specs:
            self.canvas.create_oval(
                cx + dx - r,
                cy + dy - r,
                cx + dx + r,
                cy + dy + r,
                fill=fill,
                outline=outline,
                width=1,
            )

        self.canvas.create_oval(
            cx - 6, cy - 6, cx + 6, cy + 6,
            fill="#ffd54f",
            outline="#ff6f00",
            width=1,
        )

        shard_angles = [0, 35, 70, 110, 145, 180, 215, 250, 290, 325]
        for angle in shard_angles:
            rad = math.radians(angle)
            inner = 10
            outer = 18
            x_start = cx + inner * math.cos(rad)
            y_start = cy + inner * math.sin(rad)
            x_end = cx + outer * math.cos(rad)
            y_end = cy + outer * math.sin(rad)
            self.canvas.create_line(
                x_start, y_start, x_end, y_end,
                fill="#5d4037",
                width=2,
            )

        self.canvas.create_line(x1 + 6, y1 + 6, x2 - 6, y2 - 6, fill="#ffb300", width=2)
        self.canvas.create_line(x1 + 6, y2 - 6, x2 - 6, y1 + 6, fill="#ffb300", width=2)

    def draw_soldiers(
        self,
        origin_x: int,
        origin_y: int,
        soldiers: List[Dict[str, Any]],
        grenade: Optional[Dict[str, Any]],
    ) -> None:
        absorb_actor_id = None
        absorb_actor_kind = None
        if grenade:
            absorb_actor_id = grenade.get("absorb_actor_id")
            absorb_actor_kind = grenade.get("absorb_actor_kind")

        for soldier in soldiers:
            row = soldier["row"]
            col = soldier["col"]
            x1, y1, x2, y2 = self.cell_bbox(origin_x, origin_y, row, col)
            cx = (x1 + x2) / 2
            cy = (y1 + y2) / 2 - 6

            alive = soldier.get("alive", True)
            carrying = soldier.get("is_carrying_recoveree", False)
            integrity = soldier.get("integrity", "I2")
            soldier_id = soldier.get("soldier_id", "?")
            is_absorber = soldier_id == absorb_actor_id and absorb_actor_kind == "soldier"

            if not alive:
                fill = "#d32f2f"
                outline = "#7f0000"
                self.canvas.create_oval(
                    cx - 10,
                    cy - 10,
                    cx + 10,
                    cy + 10,
                    fill=fill,
                    outline=outline,
                    width=3,
                )
                self.canvas.create_line(cx - 7, cy - 7, cx + 7, cy + 7, fill="#ffffff", width=2)
                self.canvas.create_line(cx - 7, cy + 7, cx + 7, cy - 7, fill="#ffffff", width=2)
            else:
                if integrity == "I1":
                    fill = "#fdd835"
                    outline = "#f57f17"
                    text_fill = "#222222"
                else:
                    fill = "#1976d2"
                    outline = "#0d47a1"
                    text_fill = "white"

                if carrying:
                    self.canvas.create_oval(
                        cx - 13,
                        cy - 13,
                        cx + 13,
                        cy + 13,
                        fill="",
                        outline="#7b1fa2",
                        width=2,
                    )

                self.canvas.create_oval(
                    cx - 10,
                    cy - 10,
                    cx + 10,
                    cy + 10,
                    fill=fill,
                    outline=outline,
                    width=3 if integrity == "I1" else 2,
                )

            if is_absorber:
                self.draw_absorber_marker(cx, cy - 14)

            self.canvas.create_text(
                cx,
                cy,
                text=soldier_id,
                font=("Helvetica", 8, "bold"),
                fill="#111111" if not alive else text_fill,
            )

    def draw_recoveree(self, origin_x: int, origin_y: int, recoveree: Optional[Dict[str, Any]]) -> None:
        if not recoveree:
            return

        row = recoveree["row"]
        col = recoveree["col"]
        x1, y1, x2, y2 = self.cell_bbox(origin_x, origin_y, row, col)
        cx = (x1 + x2) / 2
        cy = (y1 + y2) / 2 + 11

        alive = recoveree.get("alive", True)
        integrity = recoveree.get("integrity", "I1")
        carried = recoveree.get("carried_by_id") is not None
        extracted = bool(recoveree.get("extracted", False))

        if not alive:
            self.canvas.create_oval(
                cx - 8, cy - 15, cx + 8, cy + 15,
                fill="",
                outline="#d32f2f",
                width=2,
            )
            self.canvas.create_oval(
                cx - 4, cy - 11, cx + 4, cy - 3,
                fill="#d32f2f",
                outline="#7f0000",
                width=1,
            )
            self.canvas.create_line(cx, cy - 3, cx, cy + 6, fill="#d32f2f", width=3)
            self.canvas.create_line(cx - 6, cy + 1, cx + 6, cy + 1, fill="#d32f2f", width=2)
            self.canvas.create_line(cx, cy + 6, cx - 5, cy + 12, fill="#d32f2f", width=2)
            self.canvas.create_line(cx, cy + 6, cx + 5, cy + 12, fill="#d32f2f", width=2)
            self.canvas.create_line(cx - 8, cy - 14, cx + 8, cy + 14, fill="#ffffff", width=2)
            self.canvas.create_line(cx - 8, cy + 14, cx + 8, cy - 14, fill="#ffffff", width=2)
            self.canvas.create_text(
                cx,
                cy + 17,
                text="REC",
                font=("Helvetica", 6, "bold"),
                fill="#7f0000",
            )
            return

        if integrity == "I1":
            head_fill = "#fdd835"
            head_outline = "#f57f17"
            body_fill = "#fdd835"
            label_fill = "#7a5a00"
            border_color = "#f57f17"
            border_width = 2
        else:
            head_fill = "#66bb6a"
            head_outline = "#2e7d32"
            body_fill = "#66bb6a"
            label_fill = "#1b5e20"
            border_color = "#2e7d32"
            border_width = 2

        self.canvas.create_oval(
            cx - 8, cy - 15, cx + 8, cy + 15,
            fill="",
            outline=border_color,
            width=border_width,
        )

        self.canvas.create_oval(
            cx - 4, cy - 11, cx + 4, cy - 3,
            fill=head_fill,
            outline=head_outline,
            width=1,
        )
        self.canvas.create_line(cx, cy - 3, cx, cy + 6, fill=body_fill, width=3)
        self.canvas.create_line(cx - 6, cy + 1, cx + 6, cy + 1, fill=body_fill, width=2)
        self.canvas.create_line(cx, cy + 6, cx - 5, cy + 12, fill=body_fill, width=2)
        self.canvas.create_line(cx, cy + 6, cx + 5, cy + 12, fill=body_fill, width=2)

        if carried:
            self.canvas.create_rectangle(
                cx - 11, cy - 18, cx + 11, cy + 18,
                outline="#7b1fa2",
                width=1,
            )

        label = "EXT" if extracted else "REC"
        self.canvas.create_text(
            cx,
            cy + 17,
            text=label,
            font=("Helvetica", 6, "bold"),
            fill=label_fill,
        )

    def draw_absorber_marker(self, cx: float, cy: float) -> None:
        r1 = 7
        r2 = 3
        points: List[float] = []
        for i in range(10):
            angle_deg = -90 + i * 36
            angle_rad = math.radians(angle_deg)
            r = r1 if i % 2 == 0 else r2
            x = cx + r * math.cos(angle_rad)
            y = cy + r * math.sin(angle_rad)
            points.extend([x, y])

        self.canvas.create_polygon(
            points,
            fill="#d32f2f",
            outline="#7f0000",
            width=1,
        )

    def draw_frame_footer(self, origin_x: int, origin_y: int, board_width: int, frame: Dict[str, Any]) -> None:
        rows = self.map_data["rows"]
        footer_y = origin_y + rows * self.cell_size + 12

        phase = frame.get("phase", "?")
        tick = frame.get("tick", "?")
        label = frame.get("label", "")
        domains = frame.get("domains", {})
        events = frame.get("events", [])
        recoveree = frame.get("recoveree", {})

        rec_integrity = recoveree.get("integrity", "?")
        rec_carried = recoveree.get("carried_by_id", None)
        rec_extracted = recoveree.get("extracted", False)
        rec_line = (
            f"Recoveree: integrity={rec_integrity}   carried_by={rec_carried if rec_carried else 'none'}   "
            f"alive={recoveree.get('alive', '?')}   extracted={rec_extracted}"
        )

        line1 = f"Phase: {phase}   Tick: {tick}"
        line2 = f"SV={domains.get('sv', '?')}   MC={domains.get('mc', '?')}   C={domains.get('cohesion', '?')}"
        line3 = label
        line4 = rec_line
        line5 = f"Events: {', '.join(events)}" if events else "Events: none"

        self.canvas.create_text(
            origin_x,
            footer_y,
            anchor="nw",
            text=line1,
            font=("Helvetica", 10, "bold"),
            fill="#222222",
        )
        self.canvas.create_text(
            origin_x,
            footer_y + 18,
            anchor="nw",
            text=line2,
            font=("Helvetica", 9),
            fill="#333333",
        )
        self.canvas.create_text(
            origin_x,
            footer_y + 36,
            anchor="nw",
            text=line3,
            font=("Helvetica", 9),
            fill="#333333",
            width=board_width,
        )
        self.canvas.create_text(
            origin_x,
            footer_y + 54,
            anchor="nw",
            text=line4,
            font=("Helvetica", 9),
            fill="#7a0000" if rec_integrity == "I1" else "#333333",
            width=board_width,
        )
        self.canvas.create_text(
            origin_x,
            footer_y + 72,
            anchor="nw",
            text=line5,
            font=("Helvetica", 9),
            fill="#333333",
            width=board_width,
        )

    def draw_legend(self, x: int, y: int, width: int) -> None:
        self.canvas.create_rectangle(
            x, y, x + width, y + 126,
            outline="#cccccc", fill="#fafafa", width=1
        )
        self.canvas.create_text(
            x + 10, y + 8,
            anchor="nw",
            text="Legend",
            font=("Helvetica", 11, "bold"),
            fill="#222222",
        )

        lx = x + 12
        ly = y + 34

        self.canvas.create_oval(lx, ly, lx + 20, ly + 20, fill="#1976d2", outline="#0d47a1", width=2)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Healthy soldier", font=("Helvetica", 9))

        lx += 140
        self.canvas.create_oval(lx - 3, ly - 3, lx + 23, ly + 23, fill="", outline="#7b1fa2", width=2)
        self.canvas.create_oval(lx, ly, lx + 20, ly + 20, fill="#1976d2", outline="#0d47a1", width=2)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Carrier", font=("Helvetica", 9))

        lx += 120
        self.canvas.create_oval(lx, ly, lx + 20, ly + 20, fill="#fdd835", outline="#f57f17", width=3)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Wounded soldier", font=("Helvetica", 9))

        lx += 150
        self.canvas.create_oval(lx, ly, lx + 20, ly + 20, fill="#d32f2f", outline="#7f0000", width=3)
        self.canvas.create_line(lx + 4, ly + 4, lx + 16, ly + 16, fill="#ffffff", width=2)
        self.canvas.create_line(lx + 4, ly + 16, lx + 16, ly + 4, fill="#ffffff", width=2)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Dead soldier", font=("Helvetica", 9))

        lx += 120
        self.draw_absorber_marker(lx + 10, ly + 10)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Grenade absorber", font=("Helvetica", 9))

        lx += 150
        self.draw_recoveree_legend_icon(lx + 10, ly + 10)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Recoveree", font=("Helvetica", 9))

        lx = x + 12
        ly = y + 68

        self.canvas.create_rectangle(lx, ly, lx + 20, ly + 20, outline="#1e88e5", width=2)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Extraction zone", font=("Helvetica", 9))

        lx += 150
        self.canvas.create_rectangle(lx + 4, ly + 4, lx + 16, ly + 16, outline="#90caf9", width=1)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Extraction boundary", font=("Helvetica", 9))

        lx += 170
        self.canvas.create_rectangle(lx, ly, lx + 20, ly + 20, outline="#f39c12", width=2)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Objective zone", font=("Helvetica", 9))

        lx += 150
        self.canvas.create_polygon(
            lx + 10, ly + 2,
            lx + 2, ly + 16,
            lx + 18, ly + 16,
            fill="#2e7d32",
            outline="#1b5e20",
            width=1,
        )
        self.canvas.create_line(lx + 10, ly + 16, lx + 10, ly + 20, fill="#6d4c41", width=2)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Partial cover", font=("Helvetica", 9))

        lx += 150
        self.canvas.create_rectangle(lx, ly + 2, lx + 20, ly + 14, fill="#8d6e63", outline="#4e342e", width=1)
        self.canvas.create_line(lx, ly + 8, lx + 20, ly + 8, fill="#5d4037", width=1)
        self.canvas.create_line(lx + 6, ly + 2, lx + 6, ly + 8, fill="#5d4037", width=1)
        self.canvas.create_line(lx + 14, ly + 2, lx + 14, ly + 8, fill="#5d4037", width=1)
        self.canvas.create_line(lx + 3, ly + 8, lx + 3, ly + 14, fill="#5d4037", width=1)
        self.canvas.create_line(lx + 11, ly + 8, lx + 11, ly + 14, fill="#5d4037", width=1)
        self.canvas.create_line(lx + 17, ly + 8, lx + 17, ly + 14, fill="#5d4037", width=1)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Strong cover", font=("Helvetica", 9))

        lx += 150
        self.canvas.create_oval(lx, ly + 2, lx + 10, ly + 12, fill="#c7b299", outline="#9c7f62", width=1)
        self.canvas.create_oval(lx + 8, ly, lx + 20, ly + 12, fill="#cfd8dc", outline="#90a4ae", width=1)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Dust / smoke impact", font=("Helvetica", 9))

        lx += 180
        self.canvas.create_oval(lx + 2, ly + 2, lx + 18, ly + 18, fill="#ff7043", outline="#bf360c", width=1)
        self.canvas.create_line(lx - 2, ly + 10, lx + 22, ly + 10, fill="#5d4037", width=2)
        self.canvas.create_text(lx + 28, ly + 10, anchor="w", text="Grenade explosion", font=("Helvetica", 9))

    def draw_recoveree_legend_icon(self, cx: float, cy: float) -> None:
        self.canvas.create_oval(
            cx - 8, cy - 15, cx + 8, cy + 15,
            fill="",
            outline="#2e7d32",
            width=2,
        )
        self.canvas.create_oval(
            cx - 4, cy - 11, cx + 4, cy - 3,
            fill="#66bb6a",
            outline="#2e7d32",
            width=1,
        )
        self.canvas.create_line(cx, cy - 3, cx, cy + 6, fill="#66bb6a", width=3)
        self.canvas.create_line(cx - 6, cy + 1, cx + 6, cy + 1, fill="#66bb6a", width=2)
        self.canvas.create_line(cx, cy + 6, cx - 5, cy + 12, fill="#66bb6a", width=2)
        self.canvas.create_line(cx, cy + 6, cx + 5, cy + 12, fill="#66bb6a", width=2)

    def update_status_text(self) -> None:
        meta = self.trace_data.get("run_metadata", {})
        case_id = meta.get("case_id", "?")
        episode_index = meta.get("episode_index", "?")
        seed = meta.get("seed", "?")
        enemy_fire_scale = meta.get("enemy_fire_scale", "?")
        max_ticks = meta.get("max_ticks", meta.get("extraction_ticks", "?"))
        active_file = self.trace_files[self.trace_index].name if self.trace_files else "?"
        recoveree_condition = meta.get("recoveree_condition", "?")

        if self.mute:
            sound_status = "muted"
        else:
            impact_status = self.impact_sound_path.name if self.impact_sound_enabled else "missing"
            grenade_status = self.grenade_sound_path.name if self.grenade_sound_enabled else "missing"
            sound_status = f"impact={impact_status}, grenade={grenade_status}"

        lines = [
            f"Replay file: {active_file}",
            f"Case: {case_id}    Episode: {episode_index}    Seed: {seed}",
            f"Enemy fire scale: {enemy_fire_scale}    Max ticks: {max_ticks}    Recoveree condition: {recoveree_condition}",
            f"Delay: {self.current_delay_ms()} ms    Sound: {sound_status}",
            "Keys: Left/Right = frame, Up/Down = file, Space = play/pause",
            "Use --file 0905 at launch, or type 0905 in the Open file box and press Go.",
        ]
        self.status_var.set("\n".join(lines))

    def tile_fill(self, height: int) -> str:
        height_palette = {
            0: "#eef7ea",
            1: "#cfe8c6",
            2: "#8fbc8f",
            3: "#4f7f4f",
        }
        return height_palette.get(height, "#eef7ea")

    def cell_bbox(self, origin_x: int, origin_y: int, row: int, col: int) -> Tuple[int, int, int, int]:
        x1 = origin_x + (col - 1) * self.cell_size
        y1 = origin_y + (row - 1) * self.cell_size
        x2 = x1 + self.cell_size
        y2 = y1 + self.cell_size
        return x1, y1, x2, y2


def main() -> None:
    args = parse_args()

    root = tk.Tk()
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass

    ReplayViewer(
        root,
        args.dir,
        args.delay_ms,
        args.file,
        impact_sound=args.impact_sound,
        grenade_sound=args.grenade_sound,
        mute=args.mute,
    )
    root.mainloop()


if __name__ == "__main__":
    main()
