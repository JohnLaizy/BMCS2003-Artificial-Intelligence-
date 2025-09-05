# -*- coding: utf-8 -*-
"""
Library Bot — Google Sheets slot scheduling (08:00–20:00, 30-min slots)

This single-file app keeps your existing menu text/format, and implements:
  • Sheets schema: Rooms, Schedule, Bookings (auto-created with headers)
  • Slot model: 24 slots per room/day; 2h = 4 slots
  • Room pools: 22 small, 13 medium, 8 large, 18 solo
  • Auto category from size: 1 → solo, >1 → discussion (S/M/L buckets)
  • One booking per student per day (enforced at booking-time)
  • Pick a specific room_id; write occupancy into Schedule (HOLD → booking_id)
  • Cancellation frees slots and marks row as cancelled

Assumptions:
  • Single user at a time (no race handling). For concurrency, use a DB or add recheck+retry.
  • Date format in sheets is dd/mm/YYYY.
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime, date, timedelta
from dateutil import parser
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import json
import uuid
from datetime import datetime, date, time as dtime, timedelta
import time
from typing import Dict, List, Tuple

# ===============================
# Logging
# ===============================
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# ===============================
# Session Store (local)
# ===============================
session_store = {}

def get_session_id(req) -> str:
    """Extract Dialogflow session id (projects/.../sessions/<ID>)."""
    return req.get('session', 'unknown_session')


def update_session_store(session_id, new_params):
    existing = session_store.get(session_id, {})
    for k, v in (new_params or {}).items():
        if v not in ["", None, []]:
            existing[k] = v
    session_store[session_id] = existing
    logging.debug(f"🧠 Updated session_store[{session_id}]: {json.dumps(session_store[session_id], indent=2, default=str)}")


def get_stored_params(session_id):
    return session_store.get(session_id, {})

#Debugger
def _dbg_kv(label: str, obj: dict):
    try:
        logging.debug(f"🔎 {label}:")
        if not isinstance(obj, dict):
            logging.debug(f"  (not a dict) -> {repr(obj)}  type={type(obj).__name__}")
            return
        for k in sorted(obj.keys()):
            v = obj[k]
            t = type(v).__name__
            logging.debug(f"  • {k} = {repr(v)}  (type={t})")
    except Exception:
        logging.exception(f"debug print failed for {label}")


# ===============================
# Flask
# ===============================
app = Flask(__name__)
CORS(app)

# ===============================
# Business rules
# ===============================
OPEN_HOUR = 8
CLOSE_HOUR = 20            # exclusive upper bound (→ 24 slots at 30 mins)
SLOT_MINUTES = 30          # 24 slots/day
MAX_BOOKING_HOURS = 2      # 2h => 4 slots
ALLOW_UNTIL_MIDNIGHT = False

LIB_OPEN = dtime(OPEN_HOUR, 0)
LIB_CLOSE = dtime(CLOSE_HOUR, 0)

# Group size range
MIN_GROUP = 1
MAX_GROUP = 9

# Room inventory
ROOM_COUNTS = {
    "solo": 18,
    "small": 22,
    "medium": 13,
    "large": 8,
}

# Display names for room_type codes used in confirmation
ROOM_TYPE_DISPLAY = {
    "SOLO-1": "Solo room",
    "DISCUSSION-S": "Small discussion room",
    "DISCUSSION-M": "Medium discussion room",
    "DISCUSSION-L": "Large discussion room",
}

def _display_room_type(code: str) -> str:
    return ROOM_TYPE_DISPLAY.get(code, code or "room")

# Friendly plural helper
_def_plur = lambda n: '' if str(n) == '1' else 's'

# ===============================
# Google Sheets setup
# ===============================
SHEET_TITLE = 'library-bot-sheet'
WS_ROOMS = 'Rooms'
WS_SCHEDULE = 'Schedule'
WS_BOOKINGS = 'Bookings'

HEADERS_ROOMS = ["room_id", "room_type", "capacity_min", "capacity_max"]
HEADERS_SCHEDULE = ["date", "room_id", "room_type"] + [f"S{i}" for i in range(1, 25)]
HEADERS_BOOKINGS = [
    "booking_id", "student_id", "date", "start_time", "end_time",
    "room_type", "room_id", "slots_json", "created_at", "status"
]

scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)
sh = client.open(SHEET_TITLE)

# Worksheet initialisation

def _ensure_worksheet(title: str, headers: list[str]):
    """Open or create worksheet, enforce EXACT header row, and shrink columns to len(headers)."""
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        default_rows = 2000 if title == WS_SCHEDULE else 200
        ws = sh.add_worksheet(title=title, rows=default_rows, cols=max(26, len(headers)))
    ws.update('A1', [headers])
    ws.resize(rows=max(ws.row_count, 1), cols=len(headers))
    return ws


ws_rooms = _ensure_worksheet(WS_ROOMS, HEADERS_ROOMS)
ws_schedule = _ensure_worksheet(WS_SCHEDULE, HEADERS_SCHEDULE)
ws_bookings = _ensure_worksheet(WS_BOOKINGS, HEADERS_BOOKINGS)

# Seed Rooms once

def _seed_rooms_if_empty():
    values = ws_rooms.get_all_values()
    if len(values) > 1:
        return
    rows = []
    for i in range(1, ROOM_COUNTS['solo'] + 1):
        rows.append([f"SOLO-{i:02d}", "solo", 1, 1])
    for i in range(1, ROOM_COUNTS['small'] + 1):
        rows.append([f"S-{i:02d}", "small", 2, 3])
    for i in range(1, ROOM_COUNTS['medium'] + 1):
        rows.append([f"M-{i:02d}", "medium", 4, 6])
    for i in range(1, ROOM_COUNTS['large'] + 1):
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


def slots_from_period(start_dt: datetime, end_dt: datetime) -> list[int]:
    total_slots = int((end_dt - start_dt).total_seconds() // (SLOT_MINUTES * 60))
    start_slot = dt_to_slot_index(start_dt)
    return [start_slot + i for i in range(total_slots)]

# ===============================
# Helpers: size → category/type
# ===============================

def _size_to_int(room_size):
    if isinstance(room_size, int):
        return room_size
    if isinstance(room_size, float) and float(room_size).is_integer():
        return int(room_size)
    if isinstance(room_size, str) and room_size.strip().isdigit():
        return int(room_size.strip())
    return None


def auto_category_from_size(room_size):
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
    """
    Convert Dialogflow value to int in [MIN_GROUP, MAX_GROUP]; else None.
    """
    n = _size_to_int(room_size)
    if n is None:
        return None
    return n if (MIN_GROUP <= n <= MAX_GROUP) else None



def room_type_from_size_and_category(room_size, room_category):
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
# Schedule sheet access (with batching/coalescing & row cache)
# ===============================

def _date_str(d: date) -> str:
    return d.strftime('%d/%m/%Y')

# Per-process lightweight cache for (date_str, room_id) → row index
_schedule_row_cache: dict[tuple[str, str], int] = {}

# Optional: fast ensure_schedule_row if still used elsewhere
def ensure_schedule_row(date_str: str, room_id: str, room_type: str) -> int:
    ix = ScheduleIndex(ws_schedule, ws_rooms)
    m = ix.get_map(date_str)
    if room_id in m:
        return m[room_id]
    # create just this one row (rare path)
    empty_slots = ["" for _ in range(24)]
    ws_schedule.append_row([date_str, room_id, room_type] + empty_slots)
    ix._load_all_for_date(date_str)
    return ix.get_map(date_str)[room_id]

def _coalesce_slots(slots: list[int]) -> list[tuple[int, int]]:
    """[(start_slot, end_slot_inclusive), ...] with merged consecutive runs."""
    if not slots:
        return []
    s = sorted(int(round(x)) for x in slots)
    runs = []
    rs, re = s[0], s[0]
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
    col_end   = 3 + e
    a1_start = gspread.utils.rowcol_to_a1(row_idx, col_start)
    a1_end   = gspread.utils.rowcol_to_a1(row_idx, col_end)
    return f"{a1_start}:{a1_end}"

def slots_free(row_idx: int, slots: list[int]) -> bool:
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

def occupy_slots(row_idx: int, slots: list[int], booking_id: str):
    """Batch-write contiguous blocks in as few ranges as possible."""
    updates = []
    for s, e in _coalesce_slots(slots):
        a1 = _slot_run_to_a1_range(row_idx, s, e)
        width = e - s + 1
        updates.append({'range': a1, 'values': [[booking_id] * width]})
    logging.debug(f"occupy_slots() updating ranges: {[u['range'] for u in updates]}")
    if updates:
        ws_schedule.batch_update(updates)

def free_slots(row_idx: int, slots: list[int]):
    """Batch-clear contiguous blocks."""
    updates = []
    for s, e in _coalesce_slots(slots):
        a1 = _slot_run_to_a1_range(row_idx, s, e)
        width = e - s + 1
        updates.append({'range': a1, 'values': [[""] * width]})
    logging.debug(f"free_slots() clearing ranges: {[u['range'] for u in updates]}")
    if updates:
        ws_schedule.batch_update(updates)
        
class ScheduleIndex:
    def __init__(self, ws, ws_rooms):
        self.ws = ws
        self.ws_rooms = ws_rooms
        self.index_by_date: Dict[str, Dict[str, int]] = {}  # date_str -> {room_id: row_idx}
        self.row_count_snapshot = None

    def _load_all_for_date(self, date_str: str):
        """Build {room_id -> row_idx} for a given date with ONE API call."""
        values = self.ws.get_all_values()  # A..all, all rows (1 call)
        idx_map: Dict[str, int] = {}
        # Header is row 1
        for r_idx in range(2, len(values) + 1):
            row = values[r_idx - 1]
            # row[0]=date, row[1]=room_id
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

        # Gather all rooms in this bucket
        room_records = self.ws_rooms.get_all_records(expected_headers=HEADERS_ROOMS)
        bucket_rooms: List[Tuple[str, str]] = [
            (r["room_id"], r["room_type"]) for r in room_records if r.get("room_type") == bucket
        ]

        missing: List[Tuple[str, str]] = [(rid, rtype) for (rid, rtype) in bucket_rooms if rid not in idx_map]
        if not missing:
            return

        # Grow rows if needed (do it once)
        needed_rows = len(missing)
        # +1 header row already present; new rows will start after current last data row
        # We don't know data rows exactly; grow defensively by a chunk
        current_rows = self.ws.row_count
        # We’ll append; Sheets automatically grows if needed, but manual add_rows avoids 400s on some accounts
        if current_rows - 1 < needed_rows:
            self.ws.add_rows(max(100, needed_rows))

        # Prepare rows in one shot
        empty_slots = ["" for _ in range(24)]
        to_append = []
        for rid, rtype in missing:
            to_append.append([date_str, rid, rtype] + empty_slots)

        # Append all missing rows at once (1 call)
        self.ws.append_rows(to_append)

        # Refresh the index (no need to re-fetch whole sheet: we can compute row indices)
        # We don't know exact insertion row without reading, so safest is to rebuild quickly (still cheap vs the old loops)
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
    # slot 1 -> col D(4) => col = 3 + slot
    col_l = 3 + slot_l
    col_r = 3 + slot_r
    a1_l = gspread.utils.rowcol_to_a1(row_idx, col_l)
    a1_r = gspread.utils.rowcol_to_a1(row_idx, col_r)
    return f"{a1_l}:{a1_r}"

# ===============================
# Room picking
# ===============================

def list_rooms_by_type(room_bucket: str) -> list[tuple[str, str, int, int]]:
    data = ws_rooms.get_all_records(expected_headers=HEADERS_ROOMS)
    out = []
    for r in data:
        if r.get('room_type') == room_bucket:
            out.append(
                (r['room_id'], r['room_type'], int(r['capacity_min']), int(r['capacity_max']))
            )
    return out

def bucket_from_internal_type(internal_code: str) -> str:
    if internal_code == 'SOLO-1':
        return 'solo'
    return {
        'DISCUSSION-S': 'small',
        'DISCUSSION-M': 'medium',
        'DISCUSSION-L': 'large',
    }.get(internal_code, '')

# ===============================
# Booking + cancellation (Sheets)
# ===============================

def has_active_booking(student_id: str, date_str: str) -> bool:
    rows = ws_bookings.get_all_records(expected_headers=HEADERS_BOOKINGS)
    for r in rows:
        if str(r.get('student_id')) == str(student_id) and r.get('date') == date_str and r.get('status') == 'active':
            return True
    return False

def append_booking_row(bkg):
    ws_bookings.append_row([
        bkg['booking_id'], bkg['student_id'], bkg['date'], bkg['start_time'], bkg['end_time'],
        bkg['room_type'], bkg['room_id'], json.dumps(bkg['slots']), bkg['created_at'], bkg['status']
    ])

def find_and_hold_room_for_period(date_obj: date, start_dt: datetime, end_dt: datetime,
                                  internal_room_type: str, student_id: str):
    """
    Optimized: 3–5 API calls total.
    1) Build per-date index (1 call)
    2) Ensure rows for all rooms in bucket (0–2 calls)
    3) Batch read candidate rows for slot window (1 call)
    4) Batch write HOLD for chosen row/slots (1 call)
    """
    try:
        slots = slots_from_period(start_dt, end_dt)
    except Exception:
        return None, None, "invalid_time"

    bucket = bucket_from_internal_type(internal_room_type)
    if not bucket:
        return None, None, "invalid_type"

    dstr = _date_str(date_obj)

    # Enforce "one per day" only for real 7-digit IDs
    norm_sid = normalize_student_id(student_id)
    if norm_sid and has_active_booking(norm_sid, dstr):
        return None, None, "already_booked"

    # Build/ensure index and rows for this date & bucket (each step ≤ 1 call)
    sched_ix = ScheduleIndex(ws_schedule, ws_rooms)
    sched_ix.ensure_rows_for_bucket(dstr, bucket)

    # Fetch candidate rows for the bucket
    idx_map = sched_ix.get_map(dstr)
    room_records = ws_rooms.get_all_records(expected_headers=HEADERS_ROOMS)
    candidate_room_ids = [r["room_id"] for r in room_records if r.get("room_type") == bucket and r["room_id"] in idx_map]
    if not candidate_room_ids:
        return None, None, "no_availability"

    # Batch-read the slot window for all candidates in ONE call
    sL, sR = _slot_block_columns(slots)
    ranges = []
    rows_for_room: Dict[str, int] = {}
    for rid in candidate_room_ids:
        row_idx = idx_map[rid]
        rows_for_room[rid] = row_idx
        ranges.append(_slot_range_a1(row_idx, sL, sR))  # e.g., 'T14:W14'

    blocks = ws_schedule.batch_get(ranges)  # 1 call, list parallel to ranges

    # Pick the first room whose required slots are all empty
    chosen_room = None
    for (rid, row_idx), block in zip(rows_for_room.items(), blocks):
        # block is a 2D array with shape 1 x (sR-sL+1) (or empty if not set)
        row_vals = block[0] if (block and len(block) > 0) else []
        # Verify the subset indices for our exact slots inside [sL..sR] are empty
        all_free = True
        for slot in slots:
            offset = slot - sL  # position in row_vals
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

    # Batch write the exact cells for the selected room
    updates = []
    for a1 in ScheduleIndex.slots_to_a1(row_idx, slots):
        updates.append({'range': a1, 'values': [[hold_tag]]})
    ws_schedule.batch_update(updates)  # 1 call

    return room_id, slots, None

def replace_hold_with_booking(row_idx: int, slots: list[int], booking_id: str):
    """
    Replace any HOLD:* value in the targeted cells with booking_id.
    Safe under the 'single user at a time' assumption in your header.
    """
    updates = []
    for s, e in _coalesce_slots(slots):
        a1 = _slot_run_to_a1_range(row_idx, s, e)
        block_wrapped = ws_schedule.batch_get([a1])
        block = block_wrapped[0] if block_wrapped else []
        # build a new row values list by replacing any HOLD:* with booking_id
        new_values = []
        for row in block:
            row_out = []
            for cell in row:
                if isinstance(cell, str) and cell.startswith("HOLD:"):
                    row_out.append(booking_id)
                else:
                    row_out.append(cell or booking_id)  # finalize to booking_id regardless
            new_values.append(row_out)
        updates.append({'range': a1, 'values': new_values})
    if updates:
        ws_schedule.batch_update(updates)

def finalize_booking(student_id: str, date_obj: date, start_dt: datetime, end_dt: datetime, internal_room_type: str, room_id: str, slots: list[int]):
    dstr = _date_str(date_obj)
    start_str = start_dt.strftime('%I:%M %p')
    end_str = end_dt.strftime('%I:%M %p')
    booking_id = f"BKG-{uuid.uuid4().hex[:10].upper()}"

    row_idx = ensure_schedule_row(dstr, room_id, bucket_from_internal_type(internal_room_type))
    replace_hold_with_booking(row_idx, slots, booking_id)

    append_booking_row({
        'booking_id': booking_id,
        'student_id': student_id,
        'date': dstr,
        'start_time': start_str,
        'end_time': end_str,
        'room_type': internal_room_type,
        'room_id': room_id,
        'slots': slots,
        'created_at': datetime.now().isoformat(timespec='seconds'),
        'status': 'active'
    })
    logging.info(f"✅ Booking appended: {booking_id} for student {student_id} on {dstr}")
    return booking_id

def cancel_by_student_and_date(student_id: str, date_obj: date) -> bool:
    dstr = _date_str(date_obj)
    data = ws_bookings.get_all_values()
    for r in range(2, len(data) + 1):
        row = ws_bookings.row_values(r)
        if not row:
            continue
        rec = dict(zip(HEADERS_BOOKINGS, row + [None]*(len(HEADERS_BOOKINGS)-len(row))))
        if rec.get('student_id') == str(student_id) and rec.get('date') == dstr and rec.get('status') == 'active':
            room_id = rec.get('room_id')
            slots = json.loads(rec.get('slots_json') or '[]')
            row_idx = ensure_schedule_row(dstr, room_id, '')
            free_slots(row_idx, slots)
            ws_bookings.update_cell(r, HEADERS_BOOKINGS.index('status') + 1, 'cancelled')
            return True
    return False

# ===============================
# Dialogflow helpers: contexts & state
# ===============================

CTX_MENU = "awaiting_menu"
CTX_BOOKING = "booking_info"
CTX_CHECK_FLOW = "check_flow"
CTX_READY_TO_BOOK = "ready_to_book"
CTX_AWAIT_CONFIRM = "awaiting_confirmation"

def _get_ctx_params(req, ctx_name=CTX_BOOKING):
    for c in req['queryResult'].get('outputContexts', []):
        if ctx_name in c.get('name', ''):
            return c.get('parameters', {}) or {}
    return {}

def _has_ctx(req, ctx_name):
    for c in req['queryResult'].get('outputContexts', []):
        if ctx_name in c.get('name', '') and c.get('lifespanCount', 0) > 0:
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
        "date":          get_param_from_steps(req, "date",          "prompt_time"),
        "explicit_date": get_param_from_steps(req, "explicit_date", "prompt_time"),
        "booking_time":  get_param_from_steps(req, "booking_time",  "prompt_size"),
        "room_size":     get_param_from_steps(req, "room_size",     "prompt_category"),
        "room_category": get_param_from_steps(req, "room_category", "awaiting_confirmation"),
        "student_id":    get_param_from_steps(req, "student_id",    "awaiting_confirmation"),
        "room_type":     get_param_from_steps(req, "room_type",     "awaiting_confirmation"),
        "room_id":       get_param_from_steps(req, "room_id",       "awaiting_confirmation"),
        "slots":         get_param_from_steps(req, "slots",         "awaiting_confirmation"),
        "time":          get_param_from_steps(req, "time",          "awaiting_confirmation"),
    }

STICKY_LIFESPAN = 50

def _ctx_obj(req, params: dict, ctx_name=CTX_BOOKING, lifespan=5):
    return {
        "name": f"{req['session']}/contexts/{ctx_name}",
        "lifespanCount": lifespan,
        "parameters": params
    }

def _merge_ctx_params(existing: dict, new_params: dict) -> dict:
    merged = {**(existing or {}), **(new_params or {})}
    return merged

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

# To clean up student_id input to match google sheet format
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
        elif isinstance(val, (int,)):
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
# Responses (menu text/format unchanged except hours)
# ===============================

RESPONSE = {
    "welcome": (
        "Hi! Welcome to the Library Booking Bot.\n"
        "1️⃣ Check availability\n"
        "2️⃣ Make a booking\n"
        "3️⃣ Cancel a booking\n"
        "4️⃣ Library information\n"
    ),
    "Library_Info": (
        "📚 Library Information:\n"
        "🕘 Opening Hours: 8:00 AM – 10:00 PM daily (extended until midnight during exam periods).\n"
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
    "library_info": "Library hours: 8:00 AM - 8:00 PM daily. Solo rooms fit 1 person; discussion rooms fit 2-9 people."
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

    NEW: If initial parse lands outside opening hours (common with ambiguous phrases like "10 to 12"),
    we try a "daytime fallback" by coercing hours to AM (10:00–12:00) while preserving minutes and duration.
    """
    def _within_hours(s, e, opening_dt, closing_dt):
        return opening_dt <= s < e <= closing_dt

    if not time_period or not isinstance(time_period, dict):
        return False, RESPONSE['missing_time'], None, None, None

    start_time = time_period.get('startTime')
    end_time   = time_period.get('endTime')
    if not start_time or not end_time:
        return False, RESPONSE['missing_time'], None, None, None

    try:
        start_obj = parser.isoparse(start_time)
        end_obj   = parser.isoparse(end_time)

        same_day = (start_obj.date() == end_obj.date())
        allows_2400 = (
            ALLOW_UNTIL_MIDNIGHT and
            end_obj == start_obj.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        )
        if not same_day and not allows_2400:
            return False, RESPONSE['invalid_time'], None, None, None

        # Opening/closing bounds for that day
        opening_dt = start_obj.replace(hour=OPEN_HOUR, minute=0, second=0, microsecond=0)
        closing_dt = (
            (start_obj.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
            if ALLOW_UNTIL_MIDNIGHT else
            start_obj.replace(hour=CLOSE_HOUR, minute=0, second=0, microsecond=0)
        )

        duration = end_obj - start_obj

        def _validate_and_return(s, e):
            # Duration check
            duration_hours = (e - s).total_seconds() / 3600.0
            if duration_hours - MAX_BOOKING_HOURS > 1e-6:
                return False, RESPONSE['too_long'], None, None, None
            # 30-min boundaries
            if s.minute not in (0, 30) or e.minute not in (0, 30):
                return False, "⚠ Please book on 30-minute boundaries (e.g., 2:00–4:00 or 2:30–4:30).", None, None, None
            # Opening hours
            if not _within_hours(s, e, opening_dt, closing_dt):
                return False, RESPONSE['outside_hours'], None, None, None
            time_str = f"{s.strftime('%I:%M %p')} to {e.strftime('%I:%M %p')}"
            return True, None, time_str, s, e

        # 1) Try the raw parse first
        ok, msg, time_str, s_ok, e_ok = _validate_and_return(start_obj, end_obj)
        if ok:
            return True, None, time_str, s_ok, e_ok

        # 2) If raw parse failed ONLY because of opening hours, try a "daytime AM" fallback.
        if msg == RESPONSE['outside_hours']:
            # Coerce hours to AM by stripping 12h offset:
            #   22 → 10,  0 → 12,  12 → 12, others → h%12
            def _to_day_hour(h):
                m = h % 12
                return 12 if m == 0 else m

            s_alt = start_obj.replace(hour=_to_day_hour(start_obj.hour))
            e_alt = end_obj.replace(hour=_to_day_hour(end_obj.hour))

            # Preserve intended duration if needed
            if e_alt <= s_alt:
                e_alt = s_alt + duration

            ok2, msg2, time_str2, s2, e2 = _validate_and_return(s_alt, e_alt)
            if ok2:
                return True, None, time_str2, s2, e2
            return False, msg2, None, None, None

        return False, msg, None, None, None

    except Exception:
        logging.exception("Time parsing failed")
        return False, RESPONSE['invalid_time'], None, None, None

def _commit_valid_time(state: dict, start_dt: datetime, end_dt: datetime, time_str: str):
    """Persist validated time to state; never raise on None."""
    if not start_dt or not end_dt:
        logging.warning("Attempted _commit_valid_time with None start/end.")
        return False
    state["startTime"] = start_dt.isoformat()
    state["endTime"]   = end_dt.isoformat()
    state["time_str"]  = time_str
    return True

# ===============================
# Flow handlers (wired to Sheets flow)
# ===============================

def _is_ready_to_book(state: dict) -> bool:
    return bool(state.get("date") and state.get("booking_time") and state.get("room_size"))

def handle_flow(req):
    state = collect_by_steps(req)

    # 1) Date
    date_param = state.get("explicit_date") or state.get("date")
    date_obj = parse_date(date_param)
    if not date_obj:
        return jsonify({
            "fulfillmentText": "📅 Which date would you like to book — today or tomorrow?",
            "outputContexts": _sticky_outcontexts(req, state)
        })
    state["date"] = date_obj.strftime("%d/%m/%Y")
    state = _invalidate_staged_room_if_inputs_changed(req, state)

    # 2) Time
    if not state.get("booking_time"):
        return jsonify({
            "fulfillmentText": "🕒 What time would you like? (e.g., 2 PM to 4 PM)",
            "outputContexts": _sticky_outcontexts(req, state)
        })

    ok, msg, time_str, start_dt, end_dt = parse_and_validate_timeperiod(state["booking_time"])
    if not ok:
        return jsonify({"fulfillmentText": msg, "outputContexts": _sticky_outcontexts(req, state)})

    _commit_valid_time(state, start_dt, end_dt, time_str)

    if not state.get("time"):
        state["time"] = time_str
    state = _invalidate_staged_room_if_inputs_changed(req, state)

    # 3) Size
    if not state.get("room_size"):
        return jsonify({
            "fulfillmentText": "👥 How many people will use the room? (e.g., 1 or 3)",
            "outputContexts": _sticky_outcontexts(req, state)
        })

    auto_cat = auto_category_from_size(state.get("room_size"))
    if not auto_cat:
        return jsonify({
            "fulfillmentText": "I couldn't understand the group size. Please enter a number (e.g., 1 or 3).",
            "outputContexts": _sticky_outcontexts(req, state)
        })
    state["room_category"] = auto_cat
    state = _invalidate_staged_room_if_inputs_changed(req, state)

    return jsonify({
        "fulfillmentText": f"Great — assigning a {auto_cat.upper()} room and checking availability...",
        "outputContexts": _sticky_outcontexts(req, state),
        "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"}
    })

def handle_welcome(req):
    session_id = get_session_id(req)
    # Reset booking_info on Welcome
    session_store[session_id] = {"booking_info": {}}

    lines = [ln for ln in RESPONSE['welcome'].split("\n") if ln.strip()]
    return jsonify({
        "fulfillmentMessages": [{"text": {"text": [ln]}} for ln in lines],
        "outputContexts": [
            {"name": f"{req['session']}/contexts/{CTX_BOOKING}", "lifespanCount": 0},
            {"name": f"{req['session']}/contexts/{CTX_CHECK_FLOW}", "lifespanCount": 0},
            {"name": f"{req['session']}/contexts/{CTX_READY_TO_BOOK}", "lifespanCount": 0},
            {"name": f"{req['session']}/contexts/{CTX_AWAIT_CONFIRM}", "lifespanCount": 0},
            {"name": f"{req['session']}/contexts/{CTX_MENU}", "lifespanCount": 5},
        ]
    })

def handle_menu_check(req):
    """
    Menu_CheckAvailability:
    - No prompting here.
    - If date + booking_time + room_size are complete & valid:
        • normalize/commit
        • set READY_TO_BOOK
        • followupEventInput → EVT_BOOK
    - Else:
        • just pass current state forward
        • followupEventInput → EVT_CHECK  (handled by CheckAvailability intent)
    """
    state = collect_by_steps(req)

    # Try to parse what's already provided
    date_obj = parse_date(state.get("explicit_date") or state.get("date"))
    ok_time, _msg_time, time_str, start_dt, end_dt = parse_and_validate_timeperiod(state.get("booking_time"))
    size_norm = normalize_room_size(state.get("room_size"))

    if date_obj and ok_time and size_norm:
        # ---- Complete path → proceed to booking
        state["date"] = date_obj.strftime('%d/%m/%Y')
        _commit_valid_time(state, start_dt, end_dt, time_str)
        state["time"] = time_str
        state["room_size"] = size_norm

        # Auto-derive category (solo vs discussion)
        auto_cat = auto_category_from_size(size_norm)
        state["room_category"] = auto_cat if auto_cat else None

        # Clear any stale staged data
        for k in ("room_id", "room_type", "slots", "slots_json"):
            state.pop(k, None)

        return jsonify({
            "fulfillmentText": "",  # stay silent in menu handler
            "outputContexts": _sticky_outcontexts(req, state, keep_menu=True, extra_ctx=[(CTX_READY_TO_BOOK, 5)]),
            "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"}
        })

    # ---- Incomplete path → delegate to CheckAvailability via event
    return jsonify({
        "fulfillmentText": "",  # no prompt here
        "outputContexts": _sticky_outcontexts(req, state, keep_menu=True),
        "followupEventInput": {"name": "EVT_CHECK", "languageCode": "en"}
    })

def handle_menu_book(req):
    """
    Menu_BookRoom:
    - No prompting here.
    - If date + booking_time + room_size are complete & valid:
        • normalize/commit
        • set READY_TO_BOOK
        • followupEventInput → EVT_BOOK
    - Else:
        • pass current (partial) state forward
        • followupEventInput → EVT_CHECK  (handled by CheckAvailability intent)
    """
    state = collect_by_steps(req)

    # Try to parse what's already provided in this turn/context
    date_obj = parse_date(state.get("explicit_date") or state.get("date"))
    ok_time, _msg_time, time_str, start_dt, end_dt = parse_and_validate_timeperiod(state.get("booking_time"))
    size_norm = normalize_room_size(state.get("room_size"))

    if date_obj and ok_time and size_norm:
        # ---- Complete path → proceed to booking
        state["date"] = date_obj.strftime('%d/%m/%Y')
        _commit_valid_time(state, start_dt, end_dt, time_str)  # sets startTime/endTime/time_str
        state["time"] = time_str
        state["room_size"] = size_norm

        # Auto-derive category (solo vs discussion) from size
        auto_cat = auto_category_from_size(size_norm)
        state["room_category"] = auto_cat if auto_cat else None

        # Clear any stale staged data from previous attempts
        for k in ("room_id", "room_type", "slots", "slots_json"):
            state.pop(k, None)

        return jsonify({
            "fulfillmentText": "",  # stay silent in menu handler
            "outputContexts": _sticky_outcontexts(req, state, keep_menu=True, extra_ctx=[(CTX_READY_TO_BOOK, 5)]),
            "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"}
        })

    # ---- Incomplete path → delegate to CheckAvailability via event
    return jsonify({
        "fulfillmentText": "",  # no prompt here
        "outputContexts": _sticky_outcontexts(req, state, keep_menu=True),
        "followupEventInput": {"name": "EVT_CHECK", "languageCode": "en"}
    })

def handle_menu_cancel(req):
    return jsonify({
        "fulfillmentText": "Okay, let's cancel a booking. Please provide your 7-digit student ID and the date.",
        "followupEventInput": {"name": "EVT_CANCEL", "languageCode": "en"}
    })

def handle_menu_info(req):
    return jsonify({"fulfillmentText": RESPONSE["library_info"]})

def handle_book_room(req):
    state = collect_by_steps(req)

    # Validate date & time
    date_obj = parse_date(state.get("date") or state.get("explicit_date"))
    ok, msg, time_str, start_dt, end_dt = parse_and_validate_timeperiod(state.get("booking_time"))
    if not date_obj:
        return jsonify({
            "fulfillmentText": "⚠ Please provide a valid date (today/tomorrow or dd/mm/YYYY).",
            "outputContexts": _sticky_outcontexts(req, state)
        })
    if not ok:
        return jsonify({"fulfillmentText": msg, "outputContexts": _sticky_outcontexts(req, state)})

    # Overwrite with the *current* validated values (and commit the corrected time window)
    state["date"] = date_obj.strftime('%d/%m/%Y')
    _commit_valid_time(state, start_dt, end_dt, time_str)

    # Force-drop any lingering staged data before we hold a room
    for k in ("room_id", "room_type", "slots", "slots_json"):
        state.pop(k, None)

    # Auto-derive room type (with your 1–9 validation if present)
    cat = auto_category_from_size(state.get("room_size"))
    if not cat:
        return jsonify({
            "fulfillmentText": "I couldn't understand the group size. Please enter a number (e.g., 1 or 3).",
            "outputContexts": _sticky_outcontexts(req, state)
        })
    state["room_category"] = cat
    internal_type = room_type_from_size_and_category(state.get("room_size"), cat)
    if not internal_type:
        return jsonify({
            "fulfillmentText": "Unsupported group size for available rooms.",
            "outputContexts": _sticky_outcontexts(req, state)
        })

    logging.info(
        f"🧮 Holding with start={start_dt}, end={end_dt} "
        f"(expected slots {(end_dt - start_dt).total_seconds() / 1800:.0f})"
    )

    # If user already supplied student_id in the same turn, normalize & store it now
    raw_sid_turn = req.get("queryResult", {}).get("parameters", {}).get("student_id")
    raw_sid_state = state.get("student_id")
    sid_now = normalize_student_id(raw_sid_turn or raw_sid_state)
    if sid_now:
        state["student_id"] = sid_now  # keep in sticky context/session

    # Use a neutral placeholder only for HOLD; ConfirmBooking still enforces a real ID before finalize
    hold_sid = state.get("student_id") or "PENDING"

    room_id, slots, reason = find_and_hold_room_for_period(
        date_obj, start_dt, end_dt, internal_type, str(hold_sid)
    )

    if not room_id:
        if reason == "already_booked":
            msg = "⚠ This student ID has already been used in another booking for that day. Try a different ID."
        elif reason == "invalid_type":
            msg = "Unsupported group size for available rooms. Please enter a number (e.g., 1 or 3)."
        else:  # "no_availability" or unknown
            msg = "No available rooms for this time. Please try a different time or room size."
        return jsonify({"fulfillmentText": msg, "outputContexts": _sticky_outcontexts(req, state)})

    state["room_type"] = internal_type
    state["room_id"] = room_id
    # Ensure slots are ints before storing
    slots = [int(round(x)) for x in (slots or [])]
    state["slots"] = slots
    state["slots_json"] = json.dumps(slots)  # survives context round-trip

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

    # Build confirmation text; if we already have a valid student_id, mention it and don't ask for it later
    sid_line = f" Student ID: {state['student_id']}." if state.get("student_id") else ""
    confirm_text = (
        f"Let me confirm your booking: a {_display_room_type(internal_type)} in room {room_id} "
        f"for {size_display} person{_def_plur(size_display)} on {state['date']} from {state['time']}.{sid_line} "
        "Say 'Yes' to confirm or 'No' to cancel."
    )

    return jsonify({
        "fulfillmentText": confirm_text,
        "outputContexts": _sticky_outcontexts(req, state, extra_ctx=[("awaiting_confirmation", 5)])
    })

def handle_confirm_booking(req):
    store = get_stored_params(get_session_id(req))
    ctx   = _get_ctx_params(req, CTX_BOOKING)

    _dbg_kv("CONFIRM — STORE BEFORE MERGE", store or {})
    _dbg_kv("CONFIRM — CTX BEFORE MERGE", ctx or {})

    # Session first, then ctx overrides any newer values
    params = {**(store or {}), **(ctx or {})}

    # Backfill explicitly from session if missing (contexts sometimes drop lists)
    for k in ("room_type", "room_id", "slots", "slots_json", "booking_time", "date"):
        if k not in params or params.get(k) in ("", None, []):
            if store and store.get(k) not in ("", None, []):
                params[k] = store[k]

    # Rebuild slots from JSON if needed
    if (not params.get("slots")) and params.get("slots_json"):
        try:
            params["slots"] = json.loads(params["slots_json"])
        except Exception:
            logging.exception("Failed to json-load slots_json in confirm")
            params["slots"] = []

    _dbg_kv("CONFIRM — PARAMS AFTER MERGE/REBUILD", params)
    # Force slots to ints to avoid gspread TypeError
    try:
        params["slots"] = [int(round(x)) for x in (params.get("slots") or [])]
    except Exception:
        logging.exception("CONFIRM — bad slots content")
        return jsonify({
            "fulfillmentText": "⚠ Booking data corrupted. Please try booking again.",
            "outputContexts": _sticky_outcontexts(req, params)
        })

    student_id = normalize_student_id(params.get('student_id'))
    if not student_id:
        return jsonify({
            "fulfillmentText": "Please enter your 7-digit student ID.",
            "outputContexts": _sticky_outcontexts(req, booking_params=params, extra_ctx=[(CTX_AWAIT_CONFIRM, 5)])
        })

    date_obj = datetime.strptime(params['date'], '%d/%m/%Y').date()
    ok, _, _, start_dt, end_dt = parse_and_validate_timeperiod(params.get('booking_time'))
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
                "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"}
            })
        return jsonify({
            "fulfillmentText": "I couldn't find a staged room. Please try booking again.",
            "outputContexts": _sticky_outcontexts(req, params)
        })

    finalize_booking(
        student_id=str(student_id),
        date_obj=date_obj,
        start_dt=start_dt,
        end_dt=end_dt,
        internal_room_type=params['room_type'],
        room_id=params['room_id'],
        slots=params['slots']
    )
    return jsonify({"fulfillmentText": "✅ Your booking has been saved successfully. Enter hi to go back to main menu.", "outputContexts": _sticky_outcontexts(req, booking_params={"student_id": None}, extra_ctx=[(CTX_AWAIT_CONFIRM, 0)])})

def handle_cancel_booking(req):
    params = _get_ctx_params(req, CTX_BOOKING) or get_stored_params(get_session_id(req))
    raw_sid = req.get("queryResult", {}).get("parameters", {}).get("student_id") or params.get("student_id")
    student_id = normalize_student_id(raw_sid)
    date_param = req.get("queryResult", {}).get("parameters", {}).get("date") or params.get("date")
    date_obj = parse_date(date_param)

    if not (student_id and date_obj):
        return jsonify({
            "fulfillmentText": "Please provide your 7-digit student ID and the date to cancel (today/tomorrow or dd/mm/YYYY).",
            "outputContexts": _sticky_outcontexts(req, params)
        })

    ok = cancel_by_student_and_date(student_id, date_obj)
    if ok:
        return jsonify({"fulfillmentText": f"Got it. The booking for {student_id} on {date_obj.strftime('%d/%m/%Y')} has been cancelled.", "outputContexts": _sticky_outcontexts(req)})
    return jsonify({"fulfillmentText": "No booking found for that student and date.", "outputContexts": _sticky_outcontexts(req)})

def _invalidate_staged_room_if_inputs_changed(req, state: dict) -> dict:
    """
    If date / booking_time / room_size changed vs. what we had in booking_info,
    drop any staged room fields so we don't reuse previous holds.
    """
    prev = _get_ctx_params(req, CTX_BOOKING) or {}
    prev_date = prev.get("date") or prev.get("explicit_date")
    new_date  = state.get("date") or state.get("explicit_date")

    prev_time = prev.get("booking_time")
    new_time  = state.get("booking_time")

    prev_size = prev.get("room_size")
    new_size  = state.get("room_size")

    changed = (
        (prev_date != new_date) or
        (prev_time != new_time) or
        (prev_size != new_size)
    )
    if changed:
        for k in ("room_id", "room_type", "slots", "slots_json"):
            state.pop(k, None)
        logging.debug("🧹 Inputs changed — invalidated staged room_id/room_type/slots.")
    return state

def handle_cancel_after_confirmation(req):
    return jsonify({"fulfillmentText": RESPONSE['cancel_confirm'], "outputContexts": _sticky_outcontexts(req)})

def handle_library_info(req):
    return jsonify({"fulfillmentText": RESPONSE["Library_Info"], "outputContexts": _sticky_outcontexts(req)})

def handle_default(req):
    return jsonify({"fulfillmentText": RESPONSE['unknown']})

def _parse_coarse_label(label: str):
    """Map coarse words to time ranges, e.g. 'morning', 'afternoon', 'evening'."""
    s = label.strip().lower()
    if s in ("morning", "早上"):
        return dtime(9, 0), dtime(12, 0)
    if s in ("afternoon", "中午", "下午"):
        return dtime(13, 0), dtime(17, 0)
    if s in ("evening", "晚上", "傍晚", "night"):
        return dtime(18, 0), dtime(21, 0)
    return None, None

def _round_to_slot(dt: datetime, minutes=30):
    """Round down to nearest slot size (30 min default)."""
    q = (dt.minute // minutes) * minutes
    return dt.replace(minute=q, second=0, microsecond=0)

def _compute_time_window(date_obj: date, time_str: str):
    """
    Returns (start_dt, end_dt, err_msg).
    err_msg is None if success; otherwise a short reason string.
    """
    if not time_str:
        return None, None, "missing_time"

    s = time_str.strip().lower()

    # 1) coarse labels first
    st, et = _parse_coarse_label(s)
    if st and et:
        start_dt = datetime.combine(date_obj, st)
        end_dt   = datetime.combine(date_obj, et)
    else:
        # 2) try ranges like "10 to 12", "10-12", "10:30–12:00"
        import re
        m = re.search(r'(\d{1,2}(:\d{2})?\s*(am|pm)?)\s*(to|-|–|—)\s*(\d{1,2}(:\d{2})?\s*(am|pm)?)', s)
        if m:
            lhs = m.group(1)
            rhs = m.group(5)
            try:
                def _parse_clock(tok: str):
                    tok = tok.strip()
                    try:
                        return datetime.strptime(tok, "%I:%M%p").time()
                    except:
                        try:
                            return datetime.strptime(tok, "%I%p").time()
                        except:
                            if tok.isdigit():
                                h = int(tok)
                                return dtime(h, 0)
                            raise

                t1 = _parse_clock(lhs)
                t2 = _parse_clock(rhs)
                start_dt = datetime.combine(date_obj, t1)
                end_dt   = datetime.combine(date_obj, t2)
            except Exception:
                return None, None, "unparsable_range"
        else:
            # 3) single time like "2pm" → default to 2 hours duration (fits your typical flow)
            try:
                try:
                    t = datetime.strptime(s, "%I:%M%p").time()
                except:
                    try:
                        t = datetime.strptime(s, "%I%p").time()
                    except:
                        if s.isdigit():
                            t = dtime(int(s), 0)
                        else:
                            return None, None, "unparsable_time"
                start_dt = datetime.combine(date_obj, t)
                end_dt   = start_dt + timedelta(hours=2)
            except Exception:
                return None, None, "unparsable_time"

    # Business-hour clamp & rounding
    start_dt = _round_to_slot(start_dt, 30)
    end_dt   = _round_to_slot(end_dt, 30)

    # Enforce 08:00–20:00
    open_dt  = datetime.combine(date_obj, LIB_OPEN)
    close_dt = datetime.combine(date_obj, LIB_CLOSE)

    if start_dt < open_dt: start_dt = open_dt
    if end_dt   > close_dt: end_dt = close_dt
    if not (open_dt <= start_dt < end_dt <= close_dt):
        return None, None, "outside_hours"

    # Minimum 30 mins
    if (end_dt - start_dt) < timedelta(minutes=30):
        return None, None, "too_short"

    return start_dt, end_dt, None

# ===============================
# Intent Map
# ===============================

INTENT_HANDLERS = {
    'Welcome': handle_welcome,
    'Menu_CheckAvailability': handle_menu_check,
    'Menu_BookRoom': handle_menu_book,
    'Menu_CancelBooking': handle_menu_cancel,
    'Menu_LibraryInfo': handle_menu_info,

    'CheckAvailability': handle_flow,

    # Book
    'book_room': handle_book_room,
    'ConfirmBooking': handle_confirm_booking,
    'CancelBooking': handle_cancel_booking,
    'CancelAfterConfirmation': handle_cancel_after_confirmation,
    'LibraryInfo': handle_library_info
}

# ===============================
# Webhook entry
# ===============================

@app.route('/webhook', methods=['POST'])
def webhook():
    t0 = time.monotonic()
    _schedule_row_cache.clear()  # clear per request for safety

    try:
        req = request.get_json()
        intent = req['queryResult']['intent']['displayName']
        raw_turn_params = req.get("queryResult", {}).get("parameters", {}) or {}
        _dbg_kv("RAW TURN PARAMS", raw_turn_params)
        logging.info(f"==============================📥 Incoming Intent: {intent}==============================")

        handler = INTENT_HANDLERS.get(intent, handle_default)
        # Simple budget guard (helps avoid Dialogflow DEADLINE_EXCEEDED)
        if time.monotonic() - t0 > 3.5:
            logging.warning("⏱ Budget exceeded before handler; returning fast fallback.")
            return jsonify({"fulfillmentText": "One moment… Please try again."})

        response = handler(req)
        logging.info(f"📤 Fulfillment response: {response.get_json() if hasattr(response, 'get_json') else response}")
        logging.info(f"⏱ Webhook handler time = {time.monotonic() - t0:.3f}s")
        return response
    except Exception:
        logging.exception("Webhook crashed")
        return jsonify({"fulfillmentText": "Something went wrong. Please try again."})

# ===============================
# Debug endpoints (updated to new sheets)
# ===============================

@app.route('/debug/test-sheets', methods=['GET'])
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
            "active"
        ])
        return jsonify({"ok": True})
    except Exception as e:
        logging.exception("❌ /debug/test-sheets failed")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/debug/session', methods=['GET'])
def debug_session_dump():
    try:
        return jsonify({"ok": True, "session_store": session_store})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ===============================
# Run
# ===============================

if __name__ == '__main__':
    app.run(port=5000, debug=True)
