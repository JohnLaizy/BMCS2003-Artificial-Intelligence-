# -*- coding: utf-8 -*-
"""
Library Bot — Google Sheets slot scheduling (08:00–20:00, 30-min slots)

Highlights:
  • Sheets schema: Rooms, Schedule, Bookings (auto-created with headers)
  • Slot model: 24 slots per room/day; 2h = 4 slots
  • Room pools: 22 small, 13 medium, 8 large, 18 solo
  • Auto category from size: 1 → solo, >1 → discussion (S/M/L buckets)
  • One booking per student per day (enforced at booking-time)
  • HOLD → booking_id finalize flow with idempotent replacement
  • Healthcheck endpoint for Render
  • Supports GOOGLE_SA_JSON or GOOGLE_SA_JSON_B64 env var
  • Uses SPREADSHEET_ID (recommended) or falls back to SHEET_TITLE

Assumptions:
  • Single user at a time (no race handling). For concurrency, use a DB or add recheck+retry.
  • Date format stored in Sheets is dd/mm/YYYY.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time as pytime
import uuid
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, List, Tuple

import gspread
from dateutil import parser
from flask import Flask, jsonify, request
from flask_cors import CORS
from oauth2client.service_account import ServiceAccountCredentials

# ===============================
# Logging
# ===============================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ===============================
# Flask
# ===============================
app = Flask(__name__)
CORS(app)  # For Dialogflow Messenger in browsers

# Healthcheck for Render
@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True})

# ===============================
# Session Store (local, per-process)
# ===============================
session_store: Dict[str, Dict] = {}


def get_session_id(req) -> str:
    """Extract Dialogflow session id (projects/.../sessions/<ID>)."""
    return req.get("session", "unknown_session")


def update_session_store(session_id: str, new_params: dict | None):
    existing = session_store.get(session_id, {})
    for k, v in (new_params or {}).items():
        if v not in ["", None, []]:
            existing[k] = v
    session_store[session_id] = existing
    logging.debug("🧠 Updated session_store[%s]: %s", session_id,
                  json.dumps(session_store[session_id], indent=2, default=str))


def get_stored_params(session_id: str) -> dict:
    return session_store.get(session_id, {})


def _dbg_kv(label: str, obj: dict):
    try:
        logging.debug("🔎 %s:", label)
        if not isinstance(obj, dict):
            logging.debug("  (not a dict) -> %r  type=%s", obj, type(obj).__name__)
            return
        for k in sorted(obj.keys()):
            v = obj[k]
            t = type(v).__name__
            logging.debug("  • %s = %r  (type=%s)", k, v, t)
    except Exception:
        logging.exception("debug print failed for %s", label)


# ===============================
# Business rules
# ===============================
OPEN_HOUR = 8
CLOSE_HOUR = 20            # exclusive upper bound (→ 24 slots at 30 mins)
SLOT_MINUTES = 30
MAX_BOOKING_HOURS = 2      # 2h => 4 slots
ALLOW_UNTIL_MIDNIGHT = os.getenv("ALLOW_UNTIL_MIDNIGHT", "false").lower() == "true"

LIB_OPEN = dtime(OPEN_HOUR, 0)
LIB_CLOSE = dtime(CLOSE_HOUR, 0)

MIN_GROUP = 1
MAX_GROUP = 9

ROOM_COUNTS = {
    "solo": 18,
    "small": 22,
    "medium": 13,
    "large": 8,
}

ROOM_TYPE_DISPLAY = {
    "SOLO-1": "Solo room",
    "DISCUSSION-S": "Small discussion room",
    "DISCUSSION-M": "Medium discussion room",
    "DISCUSSION-L": "Large discussion room",
}


def _display_room_type(code: str) -> str:
    return ROOM_TYPE_DISPLAY.get(code, code or "room")


_def_plur = lambda n: "" if str(n) == "1" else "s"  # noqa: E731

# ===============================
# Google Sheets setup
# ===============================
SHEET_TITLE = os.getenv("SHEET_TITLE", "library-bot-sheet")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")  # Recommended

WS_ROOMS = "Rooms"
WS_SCHEDULE = "Schedule"
WS_BOOKINGS = "Bookings"

HEADERS_ROOMS = ["room_id", "room_type", "capacity_min", "capacity_max"]
HEADERS_SCHEDULE = ["date", "room_id", "room_type"] + [f"S{i}" for i in range(1, 25)]
HEADERS_BOOKINGS = [
    "booking_id", "student_id", "date", "start_time", "end_time",
    "room_type", "room_id", "slots_json", "created_at", "status"
]

_scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _load_service_account():
    sa_json = os.getenv("GOOGLE_SA_JSON")
    sa_b64 = os.getenv("GOOGLE_SA_JSON_B64")
    if sa_b64 and not sa_json:
        try:
            sa_json = base64.b64decode(sa_b64).decode("utf-8")
        except Exception:
            logging.exception("Failed to base64-decode GOOGLE_SA_JSON_B64")
            raise RuntimeError("Invalid GOOGLE_SA_JSON_B64")
    if not sa_json:
        raise RuntimeError("Missing GOOGLE_SA_JSON (or GOOGLE_SA_JSON_B64) env var")
    try:
        return json.loads(sa_json)
    except Exception:
        logging.exception("Failed to parse GOOGLE_SA_JSON")
        raise RuntimeError("GOOGLE_SA_JSON is not valid JSON")


_creds = ServiceAccountCredentials.from_json_keyfile_dict(_load_service_account(), _scope)
_client = gspread.authorize(_creds)

if SPREADSHEET_ID:
    sh = _client.open_by_key(SPREADSHEET_ID)
else:
    # Fallback by title (requires the service account to be shared on that file)
    sh = _client.open(SHEET_TITLE)


def _ensure_worksheet(title: str, headers: List[str]):
    """Open or create worksheet, enforce EXACT header row, and shrink columns to len(headers)."""
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        default_rows = 2000 if title == WS_SCHEDULE else 200
        ws = sh.add_worksheet(title=title, rows=default_rows, cols=max(26, len(headers)))
    ws.update("A1", [headers])
    ws.resize(rows=max(ws.row_count, 1), cols=len(headers))
    return ws


ws_rooms = _ensure_worksheet(WS_ROOMS, HEADERS_ROOMS)
ws_schedule = _ensure_worksheet(WS_SCHEDULE, HEADERS_SCHEDULE)
ws_bookings = _ensure_worksheet(WS_BOOKINGS, HEADERS_BOOKINGS)


def _seed_rooms_if_empty():
    values = ws_rooms.get_all_values()
    if len(values) > 1:
        return
    rows = []
    for i in range(1, ROOM_COUNTS["solo"] + 1):
        rows.append([f"SOLO-{i:02d}", "solo", 1, 1])
    for i in range(1, ROOM_COUNTS["small"] + 1):
        rows.append([f"S-{i:02d}", "small", 2, 3])
    for i in range(1, ROOM_COUNTS["medium"] + 1):
        rows.append([f"M-{i:02d}", "medium", 4, 6])
    for i in range(1, ROOM_COUNTS["large"] + 1):
        rows.append([f"L-{i:02d}", "large", 7, 9])
    ws_rooms.append_rows(rows)


_seed_rooms_if_empty()

# ===============================
# Slot math
# ===============================
def dt_to_slot_index(dt: datetime) -> int:
    minutes_since_open = (dt.hour - OPEN_HOUR) * 60 + dt.minute
    if minutes_since_open < 0 or dt.hour >= CLOSE_HOUR:
        raise ValueError("time outside opening hours")
    idx = minutes_since_open // SLOT_MINUTES + 1
    if not (1 <= idx <= 24):
        raise ValueError("invalid slot index")
    return int(idx)


def slots_from_period(start_dt: datetime, end_dt: datetime) -> List[int]:
    total_slots = int((end_dt - start_dt).total_seconds() // (SLOT_MINUTES * 60))
    start_slot = dt_to_slot_index(start_dt)
    return [start_slot + i for i in range(total_slots)]

# ===============================
# Helpers: size → category/type
# ===============================
def _size_to_int(room_size) -> int | None:
    if isinstance(room_size, int):
        return room_size
    if isinstance(room_size, float) and float(room_size).is_integer():
        return int(room_size)
    if isinstance(room_size, str) and room_size.strip().isdigit():
        return int(room_size.strip())
    return None


def auto_category_from_size(room_size) -> str | None:
    """
    Returns 'solo' if size == 1, 'discussion' if 2..9, else None.
    """
    n = _size_to_int(room_size)
    if n is None:
        return None
    if not (MIN_GROUP <= n <= MAX_GROUP):
        return None
    return "solo" if n == 1 else "discussion"


def normalize_room_size(room_size) -> int | None:
    n = _size_to_int(room_size)
    if n is None:
        return None
    return n if (MIN_GROUP <= n <= MAX_GROUP) else None


def room_type_from_size_and_category(room_size, room_category) -> str | None:
    """Return an internal room_type code mapped to size(category)."""
    n = _size_to_int(room_size)
    cat = (room_category or "").strip().lower()
    if cat == "solo":
        return "SOLO-1"
    if cat == "discussion":
        if 2 <= n <= 3:
            return "DISCUSSION-S"
        if 4 <= n <= 6:
            return "DISCUSSION-M"
        if 7 <= n <= 9:
            return "DISCUSSION-L"
    return None

# ===============================
# Schedule sheet access & utils
# ===============================
def _date_str(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def _coalesce_slots(slots: List[int]) -> List[Tuple[int, int]]:
    """[(start_slot, end_slot_inclusive), ...] with merged consecutive runs."""
    if not slots:
        return []
    s = sorted(int(round(x)) for x in slots)
    runs: List[Tuple[int, int]] = []
    rs = re = s[0]
    for v in s[1:]:
        if v == re + 1:
            re = v
        else:
            runs.append((rs, re))
            rs = re = v
    runs.append((rs, re))
    return runs


def _slot_run_to_a1_range(row_idx: int, s: int, e: int) -> str:
    # S1 is column D (4) → S_k is column 3 + k
    col_start = 3 + s
    col_end = 3 + e
    a1_start = gspread.utils.rowcol_to_a1(row_idx, col_start)
    a1_end = gspread.utils.rowcol_to_a1(row_idx, col_end)
    return f"{a1_start}:{a1_end}"


def slots_free(row_idx: int, slots: List[int]) -> bool:
    """Batch-read minimal contiguous blocks and check any non-empty."""
    runs = _coalesce_slots(slots)
    for s, e in runs:
        a1 = _slot_run_to_a1_range(row_idx, s, e)
        block_wrapped = ws_schedule.batch_get([a1])
        block = block_wrapped[0] if block_wrapped else []
        for row in block:
            for cell in row:
                if cell:
                    return False
    return True


def occupy_slots(row_idx: int, slots: List[int], booking_id: str):
    """Batch-write contiguous blocks in as few ranges as possible."""
    updates = []
    for s, e in _coalesce_slots(slots):
        a1 = _slot_run_to_a1_range(row_idx, s, e)
        width = e - s + 1
        updates.append({"range": a1, "values": [[booking_id] * width]})
    logging.debug("occupy_slots() updating ranges: %s", [u["range"] for u in updates])
    if updates:
        ws_schedule.batch_update(updates)


def free_slots(row_idx: int, slots: List[int]):
    """Batch-clear contiguous blocks."""
    updates = []
    for s, e in _coalesce_slots(slots):
        a1 = _slot_run_to_a1_range(row_idx, s, e)
        width = e - s + 1
        updates.append({"range": a1, "values": [[""] * width]})
    logging.debug("free_slots() clearing ranges: %s", [u["range"] for u in updates])
    if updates:
        ws_schedule.batch_update(updates)


def _bookings_list_with_row_indexes():
    """
    Returns a list of tuples (row_idx, rec_dict) for Bookings.
    row_idx is 1-based; header is row 1.
    """
    values = ws_bookings.get_all_values()  # 1 call
    if not values:
        return []
    header = values[0]
    out = []
    for r_idx in range(2, len(values) + 1):
        row = values[r_idx - 1]
        rec = dict(zip(header, row + [None] * (len(header) - len(row))))
        out.append((r_idx, rec))
    return out


class ScheduleIndex:
    def __init__(self, ws, ws_rooms):
        self.ws = ws
        self.ws_rooms = ws_rooms
        self.index_by_date: Dict[str, Dict[str, int]] = {}  # date_str -> {room_id: row_idx}
        self.row_count_snapshot = None

    def _load_all_for_date(self, date_str: str):
        """Build {room_id -> row_idx} for a given date with ONE API call."""
        values = self.ws.get_all_values()
        idx_map: Dict[str, int] = {}
        for r_idx in range(2, len(values) + 1):
            row = values[r_idx - 1]
            if len(row) >= 2 and row[0] == date_str and row[1]:
                idx_map[row[1]] = r_idx
        self.index_by_date[date_str] = idx_map
        try:
            self.row_count_snapshot = self.ws.row_count
        except Exception:
            self.row_count_snapshot = len(values)

    def get_map(self, date_str: str) -> Dict[str, int]:
        if date_str not in self.index_by_date:
            self._load_all_for_date(date_str)
        return self.index_by_date[date_str]

    def ensure_rows_for_bucket(self, date_str: str, bucket: str):
        """
        Make sure every room in the bucket has a row for date_str.
        Do ONE append_rows for all missing rooms, auto-grow if needed.
        """
        idx_map = self.get_map(date_str)
        room_records = self.ws_rooms.get_all_records(expected_headers=HEADERS_ROOMS)
        bucket_rooms: List[Tuple[str, str]] = [
            (r["room_id"], r["room_type"]) for r in room_records if r.get("room_type") == bucket
        ]
        missing: List[Tuple[str, str]] = [(rid, rtype) for (rid, rtype) in bucket_rooms if rid not in idx_map]
        if not missing:
            return

        # Grow rows if needed
        current_rows = self.ws.row_count
        needed_rows = len(missing)
        if current_rows - 1 < needed_rows:
            self.ws.add_rows(max(100, needed_rows))

        empty_slots = ["" for _ in range(24)]
        to_append = [[date_str, rid, rtype] + empty_slots for rid, rtype in missing]
        self.ws.append_rows(to_append)
        self._load_all_for_date(date_str)

    @staticmethod
    def slots_to_a1(row_idx: int, slots: List[int]) -> List[str]:
        a1s = []
        for s in slots:
            col = 3 + int(s)  # S1 is col D(4) => 3 + 1
            a1s.append(gspread.utils.rowcol_to_a1(row_idx, col))
        return a1s


def _slot_block_columns(slots: List[int]) -> Tuple[int, int]:
    """Return inclusive slot bounds, e.g., slots [17,18,19,20] -> (17,20)."""
    mn, mx = min(slots), max(slots)
    return int(mn), int(mx)


def _slot_range_a1(row_idx: int, slot_l: int, slot_r: int) -> str:
    """Return a single A1 range covering the slot window for a row, e.g., 'T14:W14'."""
    col_l = 3 + slot_l
    col_r = 3 + slot_r
    a1_l = gspread.utils.rowcol_to_a1(row_idx, col_l)
    a1_r = gspread.utils.rowcol_to_a1(row_idx, col_r)
    return f"{a1_l}:{a1_r}"

# ===============================
# Room picking
# ===============================
def list_rooms_by_type(room_bucket: str) -> List[Tuple[str, str, int, int]]:
    data = ws_rooms.get_all_records(expected_headers=HEADERS_ROOMS)
    out = []
    for r in data:
        if r.get("room_type") == room_bucket:
            out.append((r["room_id"], r["room_type"], int(r["capacity_min"]), int(r["capacity_max"])))
    return out


def bucket_from_internal_type(internal_code: str) -> str:
    if internal_code == "SOLO-1":
        return "solo"
    return {
        "DISCUSSION-S": "small",
        "DISCUSSION-M": "medium",
        "DISCUSSION-L": "large",
    }.get(internal_code, "")

# ===============================
# Booking + cancellation (Sheets)
# ===============================
def has_active_booking(student_id: str, date_str: str) -> bool:
    rows = ws_bookings.get_all_records(expected_headers=HEADERS_BOOKINGS)
    for r in rows:
        if str(r.get("student_id")) == str(student_id) and r.get("date") == date_str and r.get("status") == "active":
            return True
    return False


def append_booking_row(bkg: dict):
    ws_bookings.append_row([
        bkg["booking_id"], bkg["student_id"], bkg["date"], bkg["start_time"], bkg["end_time"],
        bkg["room_type"], bkg["room_id"], json.dumps(bkg["slots"]), bkg["created_at"], bkg["status"]
    ])


def find_and_hold_room_for_period(date_obj: date, start_dt: datetime, end_dt: datetime,
                                  internal_room_type: str, student_id: str):
    """
    Optimized: 3–5 API calls total.
    1) Build per-date index (1 call)
    2) Ensure rows for all rooms in bucket (0–2 calls)
    3) Batch read candidate rows for slot window (1 call)
    4) Batch write HOLD for chosen row/slots (1 call)

    NOTE: We only enforce "already_booked" for normalized 7-digit IDs.
    """
    try:
        slots = slots_from_period(start_dt, end_dt)
    except Exception:
        return None, None, "invalid_time"

    bucket = bucket_from_internal_type(internal_room_type)
    if not bucket:
        return None, None, "invalid_type"

    dstr = _date_str(date_obj)

    norm_sid = normalize_student_id(student_id)
    if norm_sid and has_active_booking(norm_sid, dstr):
        return None, None, "already_booked"

    sched_ix = ScheduleIndex(ws_schedule, ws_rooms)
    sched_ix.ensure_rows_for_bucket(dstr, bucket)

    idx_map = sched_ix.get_map(dstr)
    room_records = ws_rooms.get_all_records(expected_headers=HEADERS_ROOMS)
    candidate_room_ids = [r["room_id"] for r in room_records if r.get("room_type") == bucket and r["room_id"] in idx_map]
    if not candidate_room_ids:
        return None, None, "no_availability"

    sL, sR = _slot_block_columns(slots)
    ranges = []
    rows_for_room: Dict[str, int] = {}
    for rid in candidate_room_ids:
        row_idx = idx_map[rid]
        rows_for_room[rid] = row_idx
        ranges.append(_slot_range_a1(row_idx, sL, sR))

    blocks = ws_schedule.batch_get(ranges)

    chosen_room = None
    for (rid, row_idx), block in zip(rows_for_room.items(), blocks):
        row_vals = block[0] if (block and len(block) > 0) else []
        all_free = True
        for slot in slots:
            offset = slot - sL
            cell_val = row_vals[offset] if (0 <= offset < len(row_vals)) else ""
            if cell_val:
                all_free = False
                break
        if all_free:
            chosen_room = (rid, row_idx)
            break

    if not chosen_room:
        return None, None, "no_availability"

    room_id, row_idx = chosen_room
    hold_tag = f"HOLD:{norm_sid or student_id}"

    updates = [{"range": a1, "values": [[hold_tag]]} for a1 in ScheduleIndex.slots_to_a1(row_idx, slots)]
    ws_schedule.batch_update(updates)

    return room_id, slots, None


def replace_hold_with_booking(row_idx: int, slots: List[int], booking_id: str):
    """
    Replace any HOLD:* value in the targeted cells with booking_id.
    Safe under the 'single user at a time' assumption.
    """
    updates = []
    for s, e in _coalesce_slots(slots):
        a1 = _slot_run_to_a1_range(row_idx, s, e)
        block_wrapped = ws_schedule.batch_get([a1])
        block = block_wrapped[0] if block_wrapped else []
        new_values = []
        for row in block:
            row_out = []
            for cell in row:
                if isinstance(cell, str) and cell.startswith("HOLD:"):
                    row_out.append(booking_id)
                else:
                    row_out.append(cell or booking_id)
            new_values.append(row_out)
        updates.append({"range": a1, "values": new_values})
    if updates:
        ws_schedule.batch_update(updates)


def finalize_booking(student_id: str, date_obj: date, start_dt: datetime, end_dt: datetime,
                     internal_room_type: str, room_id: str, slots: List[int]) -> str:
    dstr = _date_str(date_obj)
    start_str = start_dt.strftime("%I:%M %p")
    end_str = end_dt.strftime("%I:%M %p")
    # Booking ID pattern: BKG-XXXXXXXXXX (first 10 hex chars, uppercase)
    booking_id = f"BKG-{uuid.uuid4().hex[:10].upper()}"

    row_idx = ensure_schedule_row(dstr, room_id, bucket_from_internal_type(internal_room_type))
    replace_hold_with_booking(row_idx, slots, booking_id)

    append_booking_row({
        "booking_id": booking_id,
        "student_id": student_id,
        "date": dstr,
        "start_time": start_str,
        "end_time": end_str,
        "room_type": internal_room_type,
        "room_id": room_id,
        "slots": slots,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "active",
    })
    logging.info("✅ Booking appended: %s for student %s on %s", booking_id, student_id, dstr)
    return booking_id


def cancel_by_student_and_date(student_id: str, date_obj: date) -> int:
    """
    Batched cancellation:
      - Finds ALL active bookings for (student_id, date) in one read
      - Clears all their slots in Schedule via ONE batch_update
      - Marks all as 'cancelled' via ONE batch_update
    Returns: number of bookings cancelled (0 if none).
    """
    dstr = _date_str(date_obj)
    sid = str(student_id)

    rows = _bookings_list_with_row_indexes()
    matches = []
    for r_idx, rec in rows:
        if (rec.get("student_id") == sid and
                rec.get("date") == dstr and
                (rec.get("status") or "").lower() == "active"):
            try:
                slots = json.loads(rec.get("slots_json") or "[]")
                slots = [int(round(x)) for x in slots]
            except Exception:
                slots = []
            matches.append({
                "bookings_row_idx": r_idx,
                "room_id": rec.get("room_id") or "",
                "room_type": rec.get("room_type") or "",
                "slots": slots
            })

    if not matches:
        return 0

    sched_ix = ScheduleIndex(ws_schedule, ws_rooms)
    idx_map = sched_ix.get_map(dstr)

    clear_updates = []
    for m in matches:
        rid = m["room_id"]
        slots = m["slots"]
        if not rid or not slots:
            continue
        row_idx = idx_map.get(rid)
        if not row_idx:
            continue
        for a1 in ScheduleIndex.slots_to_a1(row_idx, slots):
            clear_updates.append({"range": a1, "values": [[""]]})
    if clear_updates:
        ws_schedule.batch_update(clear_updates)

    status_col = HEADERS_BOOKINGS.index("status") + 1
    status_updates = []
    for m in matches:
        r_idx = m["bookings_row_idx"]
        a1 = gspread.utils.rowcol_to_a1(r_idx, status_col)
        status_updates.append({"range": a1, "values": [["cancelled"]]})
    if status_updates:
        ws_bookings.batch_update(status_updates)

    return len(matches)

# ===============================
# Schedule helpers
# ===============================
def ensure_schedule_row(date_str: str, room_id: str, room_type_bucket: str) -> int:
    """Ensure a Schedule row exists for (date_str, room_id)."""
    ix = ScheduleIndex(ws_schedule, ws_rooms)
    m = ix.get_map(date_str)
    if room_id in m:
        return m[room_id]
    empty_slots = ["" for _ in range(24)]
    ws_schedule.append_row([date_str, room_id, room_type_bucket] + empty_slots)
    ix._load_all_for_date(date_str)
    return ix.get_map(date_str)[room_id]

# ===============================
# Dialogflow helpers: contexts & state
# ===============================
CTX_MENU = "awaiting_menu"
CTX_BOOKING = "booking_info"
CTX_CHECK_FLOW = "check_flow"
CTX_READY_TO_BOOK = "ready_to_book"
CTX_AWAIT_CONFIRM = "awaiting_confirmation"


def _get_ctx_params(req, ctx_name=CTX_BOOKING):
    for c in req["queryResult"].get("outputContexts", []):
        if ctx_name in c.get("name", ""):
            return c.get("parameters", {}) or {}
    return {}


def _has_ctx(req, ctx_name):
    for c in req["queryResult"].get("outputContexts", []):
        if ctx_name in c.get("name", "") and c.get("lifespanCount", 0) > 0:
            return True
    return False


def get_param(req, name, ctx_name="booking_info"):
    val = req.get("queryResult", {}).get("parameters", {}).get(name)
    if val not in ("", None, []):
        return val
    for c in req.get("queryResult", {}).get("outputContexts", []):
        if ctx_name in c.get("name", ""):
            v = c.get("parameters", {}).get(name)
            if v not in ("", None, []):
                return v
    return None


def get_from_ctx(req, ctx_suffix, key):
    for c in req.get("queryResult", {}).get("outputContexts", []):
        name = c.get("name", "").lower()
        if name.endswith(f"/{ctx_suffix.lower()}"):
            v = (c.get("parameters") or {}).get(key)
            if v not in ("", None, []):
                return v
    return None


def get_param_from_steps(req, key, step_ctx_suffix, booking_ctx="booking_info"):
    v = req.get("queryResult", {}).get("parameters", {}).get(key)
    if v not in ("", None, []):
        return v
    v = get_from_ctx(req, step_ctx_suffix, key)
    if v not in ("", None, []):
        return v
    return get_param(req, key, ctx_name=booking_ctx)


def collect_by_steps(req):
    return {
        "date": get_param_from_steps(req, "date", "prompt_time"),
        "explicit_date": get_param_from_steps(req, "explicit_date", "prompt_time"),
        "booking_time": get_param_from_steps(req, "booking_time", "prompt_size"),
        "room_size": get_param_from_steps(req, "room_size", "prompt_category"),
        "room_category": get_param_from_steps(req, "room_category", "awaiting_confirmation"),
        "student_id": get_param_from_steps(req, "student_id", "awaiting_confirmation"),
        "room_type": get_param_from_steps(req, "room_type", "awaiting_confirmation"),
        "room_id": get_param_from_steps(req, "room_id", "awaiting_confirmation"),
        "slots": get_param_from_steps(req, "slots", "awaiting_confirmation"),
        "time": get_param_from_steps(req, "time", "awaiting_confirmation"),
    }


STICKY_LIFESPAN = 50


def _ctx_obj(req, params: dict, ctx_name=CTX_BOOKING, lifespan=5):
    return {
        "name": f"{req['session']}/contexts/{ctx_name}",
        "lifespanCount": lifespan,
        "parameters": params,
    }


def _merge_ctx_params(existing: dict, new_params: dict) -> dict:
    return {**(existing or {}), **(new_params or {})}


def _sticky_outcontexts(req, booking_params=None, extra_ctx=None, keep_menu=False):
    session_id = get_session_id(req)
    base = _get_ctx_params(req, CTX_BOOKING)
    merged = _merge_ctx_params(base, booking_params or {})
    _dbg_kv("STICKY MERGED (about to write)", merged)

    out = []
    out.append(_ctx_obj(req, merged, CTX_BOOKING, lifespan=STICKY_LIFESPAN))
    update_session_store(session_id, merged)

    for item in (extra_ctx or []):
        if isinstance(item, tuple) and len(item) == 2:
            nm, life = item
            out.append({"name": f"{req['session']}/contexts/{nm}", "lifespanCount": life})
        elif isinstance(item, str):
            out.append({"name": f"{req['session']}/contexts/{item}", "lifespanCount": STICKY_LIFESPAN})

    if not keep_menu:
        out.append({"name": f"{req['session']}/contexts/{CTX_MENU}", "lifespanCount": 0})

    return out


def normalize_student_id(val) -> str | None:
    """
    Accepts int/float/str from Dialogflow and returns a clean 7-digit string.
    - Floats like 1234567.0 → '1234567'
    - Strings with spaces → stripped
    - Anything else → None
    """
    if val in ("", None, []):
        return None
    try:
        if isinstance(val, float) and float(val).is_integer():
            s = str(int(val))
        elif isinstance(val, int):
            s = str(val)
        else:
            s = str(val).strip()
            if s.endswith(".0"):
                s = s[:-2]
        s = "".join(ch for ch in s if ch.isdigit())
        return s if len(s) == 7 else None
    except Exception:
        logging.exception("normalize_student_id failed")
        return None

# ===============================
# Responses
# ===============================
RESPONSE = {
    "welcome": (
        "Hi! Welcome to the Library Booking Bot.\n"
        "1️⃣ Check availability\n"
        "2️⃣ Make a booking\n"
        "3️⃣ Cancel a booking\n"
        "4️⃣ Library information\n"
    ),
    "library_info": (
        "📚 Library Information:\n"
        "🕘 Opening Hours: 8:00 AM – 8:00 PM daily (extended until midnight during exam periods).\n"
        "📚 Borrowing Rules: Students can borrow up to 5 books for 14 days. Renewal is allowed online if no reservations exist. Overdue items incur daily fines.\n"
        "🛎 Help Desk: Assistance is available at the Service Counter (Level G) for borrowing, membership, or locating resources.\n"
        "👥 Discussion Rooms: 15 rooms available (1–3 pax, 3–6 pax, 6–9 pax). Each booking is limited to 3 hours per session.\n"
        "🎫 Lost Student ID: Report immediately to the service counter to deactivate your account and apply for a replacement card."
    ),
    "already_booked": "⚠ You already booked for that day (one per day).",
    "invalid_date": "⚠ Invalid date format: {}",
    "invalid_time": "⚠ Invalid time format. Please provide both start and end clearly.",
    "outside_hours": "⚠ Booking time must be between 8 AM and 8 PM (or until midnight during exam period).",
    "too_long": "⚠ You can only book up to 2 hours per session. Re-enter your booking time.",
    "missing_date_checkAvailability": "⚠ Which date do you want to check? Today or tomorrow?",
    "missing_date": "⚠ Please provide a date: today or tomorrow?",
    "missing_time": "⚠ Please provide a time range, e.g. 2 PM to 5 PM.",
    "missing_time_checkAvailability": "⚠ What time would you like to check? For example: 2 PM to 5 PM.",
    "missing_people": "How many people will be using the room?",
    "confirm": "Let me confirm: You want to book a {} room for {} people on {} from {}, correct? Say 'Yes' to confirm.",
    "confirm_success": "✅ Your booking has been saved successfully.",
    "confirm_failed": "⚠ Booking failed. Missing information.",
    "cancel": "🖑 Your booking has been cancelled.",
    "unknown": "Sorry, I didn't understand that.",
    "cancel_confirm": "Got it. The booking has been cancelled.",
}

# ===============================
# Date & time parsing/validation
# ===============================
def parse_date(date_param):
    if not date_param:
        return None
    try:
        if isinstance(date_param, dict):
            for k in ("date_time", "startDate", "start_date", "start"):
                if k in date_param and date_param[k]:
                    return parser.isoparse(date_param[k]).date()
            if "date" in date_param and date_param["date"]:
                try:
                    return parser.isoparse(date_param["date"]).date()
                except Exception:
                    pass
                for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%Y-%m-%d"):
                    try:
                        return datetime.strptime(date_param["date"], fmt).date()
                    except Exception:
                        continue
            return None
        if isinstance(date_param, str):
            s = date_param.strip()
            sl = s.lower()
            if sl == "today":
                return date.today()
            if sl == "tomorrow":
                return date.today() + timedelta(days=1)
            try:
                return parser.isoparse(s).date()
            except Exception:
                pass
            for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%Y-%m-%d"):
                try:
                    return datetime.strptime(s, fmt).date()
                except Exception:
                    continue
        return None
    except Exception:
        logging.exception("Date parsing error")
        return None


def parse_and_validate_timeperiod(time_period):
    """
    Parse Dialogflow @sys.time-period and validate:
      - same day (unless ALLOW_UNTIL_MIDNIGHT exact 24:00 crossing)
      - within opening hours
      - <= MAX_BOOKING_HOURS
      - 30-min boundaries

    AM fallback: if user typed “10 to 12” and parser landed at night, try coercing to daytime.
    """

    def _within_hours(s, e, opening_dt, closing_dt):
        return opening_dt <= s < e <= closing_dt

    if not time_period or not isinstance(time_period, dict):
        return False, RESPONSE["missing_time"], None, None, None

    start_time = time_period.get("startTime")
    end_time = time_period.get("endTime")
    if not start_time or not end_time:
        return False, RESPONSE["missing_time"], None, None, None

    try:
        start_obj = parser.isoparse(start_time)
        end_obj = parser.isoparse(end_time)

        same_day = (start_obj.date() == end_obj.date())
        allows_2400 = (
            ALLOW_UNTIL_MIDNIGHT and
            end_obj == start_obj.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        )
        if not same_day and not allows_2400:
            return False, RESPONSE["invalid_time"], None, None, None

        opening_dt = start_obj.replace(hour=OPEN_HOUR, minute=0, second=0, microsecond=0)
        closing_dt = (
            (start_obj.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
            if ALLOW_UNTIL_MIDNIGHT else
            start_obj.replace(hour=CLOSE_HOUR, minute=0, second=0, microsecond=0)
        )

        duration = end_obj - start_obj

        def _validate_and_return(s, e):
            duration_hours = (e - s).total_seconds() / 3600.0
            if duration_hours - MAX_BOOKING_HOURS > 1e-6:
                return False, RESPONSE["too_long"], None, None, None
            if s.minute not in (0, 30) or e.minute not in (0, 30):
                return False, "⚠ Please book on 30-minute boundaries (e.g., 2:00–4:00 or 2:30–4:30).", None, None, None
            if not _within_hours(s, e, opening_dt, closing_dt):
                return False, RESPONSE["outside_hours"], None, None, None
            time_str = f"{s.strftime('%I:%M %p')} to {e.strftime('%I:%M %p')}"
            return True, None, time_str, s, e

        ok, msg, time_str, s_ok, e_ok = _validate_and_return(start_obj, end_obj)
        if ok:
            return True, None, time_str, s_ok, e_ok

        if msg == RESPONSE["outside_hours"]:
            def _to_day_hour(h):
                m = h % 12
                return 12 if m == 0 else m

            s_alt = start_obj.replace(hour=_to_day_hour(start_obj.hour))
            e_alt = end_obj.replace(hour=_to_day_hour(end_obj.hour))
            if e_alt <= s_alt:
                e_alt = s_alt + duration
            ok2, msg2, time_str2, s2, e2 = _validate_and_return(s_alt, e_alt)
            if ok2:
                return True, None, time_str2, s2, e2
            return False, msg2, None, None, None

        return False, msg, None, None, None

    except Exception:
        logging.exception("Time parsing failed")
        return False, RESPONSE["invalid_time"], None, None, None


def _commit_valid_time(state: dict, start_dt: datetime, end_dt: datetime, time_str: str):
    if not start_dt or not end_dt:
        logging.warning("Attempted _commit_valid_time with None start/end.")
        return False
    state["startTime"] = start_dt.isoformat()
    state["endTime"] = end_dt.isoformat()
    state["time_str"] = time_str
    return True

# ===============================
# Flow handlers
# ===============================
def _is_ready_to_book(state: dict) -> bool:
    return bool(state.get("date") and state.get("booking_time") and state.get("room_size"))


def handle_flow(req):
    """
    CheckAvailability (single driver)
    - Never re-asks for everything.
    - If date missing -> ask for date only.
    - If time missing/invalid -> ask for time only (preserve date/size).
    - If size missing -> ask for size only.
    - When all valid -> set category, stage, then EVT_BOOK.
    """
    state = collect_by_steps(req)

    # 1) Date
    date_param = state.get("explicit_date") or state.get("date")
    date_obj = parse_date(date_param)
    if not date_obj:
        return jsonify({
            "fulfillmentText": "📅 Which date would you like to book — today or tomorrow?",
            "outputContexts": _sticky_outcontexts(req, state, keep_menu=True),
        })
    state["date"] = date_obj.strftime("%d/%m/%Y")

    state = _invalidate_staged_room_if_inputs_changed(req, state)

    # 2) Time
    if not state.get("booking_time"):
        return jsonify({
            "fulfillmentText": "🕒 What time would you like? (e.g., 2 PM to 4 PM)",
            "outputContexts": _sticky_outcontexts(req, state, keep_menu=True, extra_ctx=[("prompt_time", 3)]),
        })

    ok, msg, time_str, start_dt, end_dt = parse_and_validate_timeperiod(state["booking_time"])
    if not ok:
        return jsonify({
            "fulfillmentText": f"⏱ {msg or 'Please provide a valid time range (e.g., 2 PM to 4 PM).'}",
            "outputContexts": _sticky_outcontexts(req, state, keep_menu=True, extra_ctx=[("prompt_time", 3)]),
        })

    _commit_valid_time(state, start_dt, end_dt, time_str)
    state["time"] = time_str
    state = _invalidate_staged_room_if_inputs_changed(req, state)

    # 3) Size
    if not state.get("room_size"):
        return jsonify({
            "fulfillmentText": "👥 How many people will use the room? (e.g., 1 or 3)",
            "outputContexts": _sticky_outcontexts(req, state, keep_menu=True),
        })

    auto_cat = auto_category_from_size(state.get("room_size"))
    if not auto_cat:
        return jsonify({
            "fulfillmentText": "I couldn't understand the group size. Please enter a number between 1 and 9.",
            "outputContexts": _sticky_outcontexts(req, state, keep_menu=True),
        })
    state["room_category"] = auto_cat
    state = _invalidate_staged_room_if_inputs_changed(req, state)

    return jsonify({
        "fulfillmentText": f"Great — assigning a {auto_cat.upper()} room and checking availability…",
        "outputContexts": _sticky_outcontexts(req, state, keep_menu=True),
        "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"},
    })


def handle_welcome(req):
    session_id = get_session_id(req)
    session_store[session_id] = {"booking_info": {}}
    lines = [ln for ln in RESPONSE["welcome"].split("\n") if ln.strip()]
    return jsonify({
        "fulfillmentMessages": [{"text": {"text": [ln]}} for ln in lines],
        "outputContexts": [
            {"name": f"{req['session']}/contexts/{CTX_BOOKING}", "lifespanCount": 0},
            {"name": f"{req['session']}/contexts/{CTX_CHECK_FLOW}", "lifespanCount": 0},
            {"name": f"{req['session']}/contexts/{CTX_READY_TO_BOOK}", "lifespanCount": 0},
            {"name": f"{req['session']}/contexts/{CTX_AWAIT_CONFIRM}", "lifespanCount": 0},
            {"name": f"{req['session']}/contexts/{CTX_MENU}", "lifespanCount": 5},
        ],
    })


def handle_menu_check(req):
    state = collect_by_steps(req)
    date_obj = parse_date(state.get("explicit_date") or state.get("date"))
    ok_time, _msg_time, time_str, start_dt, end_dt = parse_and_validate_timeperiod(state.get("booking_time"))
    size_norm = normalize_room_size(state.get("room_size"))

    if date_obj and ok_time and size_norm:
        state["date"] = date_obj.strftime("%d/%m/%Y")
        _commit_valid_time(state, start_dt, end_dt, time_str)
        state["time"] = time_str
        state["room_size"] = size_norm
        auto_cat = auto_category_from_size(size_norm)
        state["room_category"] = auto_cat if auto_cat else None
        for k in ("room_id", "room_type", "slots", "slots_json"):
            state.pop(k, None)
        return jsonify({
            "fulfillmentText": "",
            "outputContexts": _sticky_outcontexts(req, state, keep_menu=True, extra_ctx=[(CTX_READY_TO_BOOK, 5)]),
            "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"},
        })

    return jsonify({
        "fulfillmentText": "",
        "outputContexts": _sticky_outcontexts(req, state, keep_menu=True),
        "followupEventInput": {"name": "EVT_CHECK", "languageCode": "en"},
    })


def handle_menu_book(req):
    state = collect_by_steps(req)
    date_obj = parse_date(state.get("explicit_date") or state.get("date"))
    ok_time, _msg_time, time_str, start_dt, end_dt = parse_and_validate_timeperiod(state.get("booking_time"))
    size_norm = normalize_room_size(state.get("room_size"))

    if date_obj and ok_time and size_norm:
        state["date"] = date_obj.strftime("%d/%m/%Y")
        _commit_valid_time(state, start_dt, end_dt, time_str)
        state["time"] = time_str
        state["room_size"] = size_norm
        auto_cat = auto_category_from_size(size_norm)
        state["room_category"] = auto_cat if auto_cat else None
        for k in ("room_id", "room_type", "slots", "slots_json"):
            state.pop(k, None)
        return jsonify({
            "fulfillmentText": "",
            "outputContexts": _sticky_outcontexts(req, state, keep_menu=True, extra_ctx=[(CTX_READY_TO_BOOK, 5)]),
            "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"},
        })

    return jsonify({
        "fulfillmentText": "",
        "outputContexts": _sticky_outcontexts(req, state, keep_menu=True),
        "followupEventInput": {"name": "EVT_CHECK", "languageCode": "en"},
    })


def handle_menu_cancel(req):
    return jsonify({
        "fulfillmentText": "Okay, let's cancel a booking. Please provide your 7-digit student ID and the date.",
        "followupEventInput": {"name": "EVT_CANCEL", "languageCode": "en"},
    })


def handle_menu_info(req):
    lines = [ln for ln in RESPONSE["library_info"].split("\n") if ln.strip()]
    return jsonify({
        "fulfillmentMessages": [{"text": {"text": [ln]}} for ln in lines],
        "outputContexts": _sticky_outcontexts(req),
    })


def handle_book_room(req):
    state = collect_by_steps(req)
    date_obj = parse_date(state.get("date") or state.get("explicit_date"))
    ok, msg, time_str, start_dt, end_dt = parse_and_validate_timeperiod(state.get("booking_time"))
    if not date_obj:
        return jsonify({
            "fulfillmentText": "⚠ Please provide a valid date (today/tomorrow or dd/mm/YYYY).",
            "outputContexts": _sticky_outcontexts(req, state),
        })
    if not ok:
        return jsonify({"fulfillmentText": msg, "outputContexts": _sticky_outcontexts(req, state)})

    state["date"] = date_obj.strftime("%d/%m/%Y")
    _commit_valid_time(state, start_dt, end_dt, time_str)

    for k in ("room_id", "room_type", "slots", "slots_json"):
        state.pop(k, None)

    cat = auto_category_from_size(state.get("room_size"))
    if not cat:
        return jsonify({
            "fulfillmentText": "I couldn't understand the group size. Please enter a number (e.g., 1 or 3).",
            "outputContexts": _sticky_outcontexts(req, state),
        })
    state["room_category"] = cat
    internal_type = room_type_from_size_and_category(state.get("room_size"), cat)
    if not internal_type:
        return jsonify({
            "fulfillmentText": "Unsupported group size for available rooms.",
            "outputContexts": _sticky_outcontexts(req, state),
        })

    raw_sid_turn = req.get("queryResult", {}).get("parameters", {}).get("student_id")
    raw_sid_state = state.get("student_id")
    sid_now = normalize_student_id(raw_sid_turn or raw_sid_state)
    if sid_now:
        state["student_id"] = sid_now
    hold_sid = state.get("student_id") or "PENDING"

    room_id, slots, reason = find_and_hold_room_for_period(
        date_obj, start_dt, end_dt, internal_type, str(hold_sid)
    )

    if not room_id:
        if reason == "already_booked":
            msg2 = "⚠ This student ID has already been used in another booking for that day. Try a different ID."
        elif reason == "invalid_type":
            msg2 = "Unsupported group size for available rooms. Please enter a number (e.g., 1 or 3)."
        else:
            msg2 = "No available rooms for this time. Please try a different time or room size."
        return jsonify({"fulfillmentText": msg2, "outputContexts": _sticky_outcontexts(req, state)})

    state["room_type"] = internal_type
    state["room_id"] = room_id
    slots = [int(round(x)) for x in (slots or [])]
    state["slots"] = slots
    state["slots_json"] = json.dumps(slots)

    _dbg_kv("BOOK_ROOM — STAGED STATE", {
        "date": state.get("date"),
        "time": state.get("time"),
        "room_type": state.get("room_type"),
        "room_id": state.get("room_id"),
        "slots": state.get("slots"),
        "slots_json": state.get("slots_json"),
        "student_id": state.get("student_id"),
    })

    size_val = state.get("room_size")
    size_display = str(int(size_val)) if isinstance(size_val, float) and size_val.is_integer() else str(size_val)
    sid_line = f" Student ID: {state['student_id']}." if state.get("student_id") else ""
    confirm_text = (
        f"Let me confirm your booking: a {_display_room_type(internal_type)} in room {room_id} "
        f"for {size_display} person{_def_plur(size_display)} on {state['date']} from {state['time']}.{sid_line} "
        "Say 'Yes' to confirm or 'No' to cancel."
    )

    return jsonify({
        "fulfillmentText": confirm_text,
        "outputContexts": _sticky_outcontexts(req, state, extra_ctx=[("awaiting_confirmation", 5)]),
    })


def handle_confirm_booking(req):
    store = get_stored_params(get_session_id(req))
    ctx = _get_ctx_params(req, CTX_BOOKING)

    _dbg_kv("CONFIRM — STORE BEFORE MERGE", store or {})
    _dbg_kv("CONFIRM — CTX BEFORE MERGE", ctx or {})

    params = {**(store or {}), **(ctx or {})}
    for k in ("room_type", "room_id", "slots", "slots_json", "booking_time", "date"):
        if k not in params or params.get(k) in ("", None, []):
            if store and store.get(k) not in ("", None, []):
                params[k] = store[k]

    if (not params.get("slots")) and params.get("slots_json"):
        try:
            params["slots"] = json.loads(params["slots_json"])
        except Exception:
            logging.exception("Failed to json-load slots_json in confirm")
            params["slots"] = []

    _dbg_kv("CONFIRM — PARAMS AFTER MERGE/REBUILD", params)
    try:
        params["slots"] = [int(round(x)) for x in (params.get("slots") or [])]
    except Exception:
        logging.exception("CONFIRM — bad slots content")
        return jsonify({
            "fulfillmentText": "⚠ Booking data corrupted. Please try booking again.",
            "outputContexts": _sticky_outcontexts(req, params),
        })

    student_id = normalize_student_id(params.get("student_id"))
    if not student_id:
        return jsonify({
            "fulfillmentText": "Please enter your 7-digit student ID.",
            "outputContexts": _sticky_outcontexts(req, booking_params=params, extra_ctx=[(CTX_AWAIT_CONFIRM, 5)]),
        })

    date_obj = datetime.strptime(params["date"], "%d/%m/%Y").date()
    ok, _, _, start_dt, end_dt = parse_and_validate_timeperiod(params.get("booking_time"))
    if not ok:
        return jsonify({"fulfillmentText": "Time invalid.", "outputContexts": _sticky_outcontexts(req, params)})

    required = (params.get("room_type"), params.get("room_id"))
    slots_ok = isinstance(params.get("slots"), list) and len(params["slots"]) > 0
    if not (all(required) and slots_ok):
        have_min = bool(params.get("date") and params.get("booking_time") and params.get("room_size"))
        if have_min:
            return jsonify({
                "fulfillmentText": "I couldn’t find a staged room. Re-checking and holding a room now…",
                "outputContexts": _sticky_outcontexts(req, params),
                "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"},
            })
        return jsonify({
            "fulfillmentText": "I couldn't find a staged room. Please try booking again.",
            "outputContexts": _sticky_outcontexts(req, params),
        })

    finalize_booking(
        student_id=str(student_id),
        date_obj=date_obj,
        start_dt=start_dt,
        end_dt=end_dt,
        internal_room_type=params["room_type"],
        room_id=params["room_id"],
        slots=params["slots"],
    )
    return jsonify({
        "fulfillmentText": "✅ Your booking has been saved successfully. Enter hi to go back to main menu.",
        "outputContexts": _sticky_outcontexts(req, booking_params={"student_id": None}, extra_ctx=[(CTX_AWAIT_CONFIRM, 0)]),
    })


def handle_cancel_booking(req):
    params = _get_ctx_params(req, CTX_BOOKING) or get_stored_params(get_session_id(req))
    raw_sid = req.get("queryResult", {}).get("parameters", {}).get("student_id") or params.get("student_id")
    student_id = normalize_student_id(raw_sid)
    date_param = req.get("queryResult", {}).get("parameters", {}).get("date") or params.get("date")
    date_obj = parse_date(date_param)

    if not (student_id and date_obj):
        return jsonify({
            "fulfillmentText": "Please provide your 7-digit student ID and the date to cancel (today/tomorrow or dd/mm/YYYY).",
            "outputContexts": _sticky_outcontexts(req, params),
        })

    n = cancel_by_student_and_date(student_id, date_obj)
    if n > 0:
        return jsonify({
            "fulfillmentText": f"Got it. Booking for {student_id} on {date_obj.strftime('%d/%m/%Y')} {'has' if n == 1 else 'have'} been cancelled.",
            "outputContexts": _sticky_outcontexts(req, booking_params={"student_id": student_id}),
        })
    else:
        return jsonify({
            "fulfillmentText": "No active booking found for that student and date.",
            "outputContexts": _sticky_outcontexts(req, booking_params={"student_id": student_id}),
        })


def _invalidate_staged_room_if_inputs_changed(req, state: dict) -> dict:
    """
    If date / booking_time / room_size changed vs. what we had in booking_info,
    drop any staged room fields so we don't reuse previous holds.
    """
    prev = _get_ctx_params(req, CTX_BOOKING) or {}
    prev_date = prev.get("date") or prev.get("explicit_date")
    new_date = state.get("date") or state.get("explicit_date")

    prev_time = prev.get("booking_time")
    new_time = state.get("booking_time")

    prev_size = prev.get("room_size")
    new_size = state.get("room_size")

    changed = ((prev_date != new_date) or (prev_time != new_time) or (prev_size != new_size))
    if changed:
        for k in ("room_id", "room_type", "slots", "slots_json"):
            state.pop(k, None)
        logging.debug("🧹 Inputs changed — invalidated staged room_id/room_type/slots.")
    return state


def handle_cancel_after_confirmation(req):
    return jsonify({"fulfillmentText": RESPONSE["cancel_confirm"], "outputContexts": _sticky_outcontexts(req)})


def handle_library_info(req):
    # BUGFIX: Previously referenced a non-existent key "Library_Info_Response"
    return jsonify({"fulfillmentText": RESPONSE["library_info"], "outputContexts": _sticky_outcontexts(req)})


def handle_default(req):
    return jsonify({"fulfillmentText": RESPONSE["unknown"]})

# ===============================
# Intent Map
# ===============================
INTENT_HANDLERS = {
    "Welcome": handle_welcome,
    "Menu_CheckAvailability": handle_menu_check,
    "Menu_BookRoom": handle_menu_book,
    "Menu_CancelBooking": handle_menu_cancel,
    "Menu_LibraryInfo": handle_menu_info,
    "CheckAvailability": handle_flow,
    "book_room": handle_book_room,
    "ConfirmBooking": handle_confirm_booking,
    "CancelBooking": handle_cancel_booking,
    "CancelAfterConfirmation": handle_cancel_after_confirmation,
    "LibraryInfo": handle_library_info,
}

# ===============================
# Webhook entry
# ===============================
@app.route("/webhook", methods=["POST"])
def webhook():
    t0 = pytime.monotonic()
    try:
        req = request.get_json(force=True, silent=True) or {}
        intent = req.get("queryResult", {}).get("intent", {}).get("displayName", "UnknownIntent")
        raw_turn_params = req.get("queryResult", {}).get("parameters", {}) or {}
        _dbg_kv("RAW TURN PARAMS", raw_turn_params)
        logging.info("==============================📥 Incoming Intent: %s ==============================", intent)

        handler = INTENT_HANDLERS.get(intent, handle_default)

        if pytime.monotonic() - t0 > 3.5:
            logging.warning("⏱ Budget exceeded before handler; returning fast fallback.")
            return jsonify({"fulfillmentText": "One moment… Please try again."})

        response = handler(req)
        logging.info("📤 Fulfillment response: %s",
                     response.get_json() if hasattr(response, "get_json") else str(response))
        logging.info("⏱ Webhook handler time = %.3fs", pytime.monotonic() - t0)
        return response
    except Exception:
        logging.exception("Webhook crashed")
        return jsonify({"fulfillmentText": "Something went wrong. Please try again."})

# ===============================
# Debug endpoints (optional)
# ===============================
@app.route("/debug/test-sheets", methods=["GET"])
def debug_test_sheets():
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws_bookings.append_row([
            f"TEST-{uuid.uuid4().hex[:6].upper()}",
            "9999999",
            date.today().strftime("%d/%m/%Y"),
            "08:00 AM",
            "08:30 AM",
            "DISCUSSION-S",
            "S-01",
            json.dumps([1]),
            ts,
            "active",
        ])
        return jsonify({"ok": True})
    except Exception as e:
        logging.exception("❌ /debug/test-sheets failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/debug/session", methods=["GET"])
def debug_session_dump():
    try:
        return jsonify({"ok": True, "session_store": session_store})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ===============================
# Local run (Render uses gunicorn)
# ===============================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "false").lower() == "true")
