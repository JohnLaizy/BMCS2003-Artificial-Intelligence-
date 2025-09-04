# -*- coding: utf-8 -*-
"""
Library Bot – Google Sheets slot scheduling (8:00–20:00, 30‑min slots)
This version keeps your existing menu/flows and adds:
  • Sheets schema: Rooms, Schedule, Bookings (auto‑created)
  • Slot math: 24 slots per room/day; 2h = 4 slots
  • Room pools: 22 small, 13 medium, 8 large, 18 solo
  • One booking per student per day (enforced in Bookings sheet)
  • Pick a specific room_id during confirmation; write occupancy to Schedule
  • Cancellation removes occupancy and frees the room

Notes:
  • Menu text/format unchanged.
  • Works for single user at a time (no heavy race handling).
  • If you later need concurrency safety, move to a DB or add retry+recheck.
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

# ===============================
# Logging
# ===============================
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# ===============================
# Session Store (local)
# ===============================
session_store = {}

# ===============================
# Flask
# ===============================
app = Flask(__name__)
CORS(app)

# ===============================
# Business rules
# ===============================
OPEN_HOUR = 8
CLOSE_HOUR = 20            # exclusive upper bound
SLOT_MINUTES = 30          # 24 slots/day
MAX_BOOKING_HOURS = 2      # 2h => 4 slots

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

# ===============================
# Google Sheets
# ===============================
SHEET_TITLE = 'library-bot-sheet'

# Worksheets and headers
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

# Ensure worksheets exist and are initialized

def _ensure_worksheet(title: str, headers: list[str]):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=200, cols=max(26, len(headers)))
    first_row = ws.row_values(1)
    if first_row != headers:
        ws.resize(rows=max(ws.row_count, 1), cols=max(len(headers), ws.col_count))
        ws.update('A1', [headers])
    return ws

ws_rooms = _ensure_worksheet(WS_ROOMS, HEADERS_ROOMS)
ws_schedule = _ensure_worksheet(WS_SCHEDULE, HEADERS_SCHEDULE)
ws_bookings = _ensure_worksheet(WS_BOOKINGS, HEADERS_BOOKINGS)

# Seed Rooms once (idempotent)

def _seed_rooms_if_empty():
    values = ws_rooms.get_all_values()
    if len(values) > 1:
        return
    rows = []
    # Solo rooms: SOLO-01 .. SOLO-18
    for i in range(1, ROOM_COUNTS['solo'] + 1):
        rows.append([f"SOLO-{i:02d}", "solo", 1, 1])
    # Small: S-01 .. S-22 (2–3)
    for i in range(1, ROOM_COUNTS['small'] + 1):
        rows.append([f"S-{i:02d}", "small", 2, 3])
    # Medium: M-01 .. M-13 (4–6)
    for i in range(1, ROOM_COUNTS['medium'] + 1):
        rows.append([f"M-{i:02d}", "medium", 4, 6])
    # Large: L-01 .. L-08 (7–9)
    for i in range(1, ROOM_COUNTS['large'] + 1):
        rows.append([f"L-{i:02d}", "large", 7, 9])
    if rows:
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
# Helpers: state + category + room_type
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
    n = _size_to_int(room_size)
    if n is None:
        return None
    return "solo" if n == 1 else "discussion"


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
# Schedule sheet access
# ===============================

def _date_str(d: date) -> str:
    return d.strftime('%d/%m/%Y')


def ensure_schedule_row(date_str: str, room_id: str, room_type: str) -> int:
    """Find or create the Schedule row for (date, room_id). Return row index (1-based)."""
    data = ws_schedule.get_all_values()
    # headers at row 1
    for r_idx in range(2, len(data) + 1):
        row = ws_schedule.row_values(r_idx)
        if len(row) >= 3 and row[0] == date_str and row[1] == room_id:
            return r_idx
    # not found: append a new row with empty S1..S24
    empty_slots = ["" for _ in range(24)]
    ws_schedule.append_row([date_str, room_id, room_type] + empty_slots)
    return ws_schedule.row_count  # appended row index


def schedule_cells_for_slots(row_idx: int, slots: list[int]):
    """Return A1 notations for the S{n} cells in the given row."""
    # Columns: A=date, B=room_id, C=room_type, D..AE = S1..S24
    ranges = []
    for s in slots:
        col = 3 + s  # S1 at column D(4) => 3 + 1
        ranges.append(gspread.utils.rowcol_to_a1(row_idx, col))
    return ranges


def slots_free(row_idx: int, slots: list[int]) -> bool:
    cells = ws_schedule.batch_get([f"{ws_schedule.title}!{a1}" for a1 in schedule_cells_for_slots(row_idx, slots)])
    # batch_get returns list of lists; each inner list is [[value]] or [] if empty
    for val in cells:
        if val and val[0] and val[0][0]:
            return False
    return True


def occupy_slots(row_idx: int, slots: list[int], booking_id: str):
    updates = []
    for a1 in schedule_cells_for_slots(row_idx, slots):
        updates.append({
            'range': f"{ws_schedule.title}!{a1}",
            'values': [[booking_id]]
        })
    ws_schedule.batch_update(updates)


def free_slots(row_idx: int, slots: list[int]):
    updates = []
    for a1 in schedule_cells_for_slots(row_idx, slots):
        updates.append({
            'range': f"{ws_schedule.title}!{a1}",
            'values': [[""]]
        })
    ws_schedule.batch_update(updates)

# ===============================
# Room picking
# ===============================

def list_rooms_by_type(room_bucket: str) -> list[tuple[str, str, int, int]]:
    """Return [(room_id, room_type, cap_min, cap_max), ...] filtered by bucket.
       bucket is one of: 'solo','small','medium','large'.
    """
    data = ws_rooms.get_all_records()
    out = []
    for r in data:
        if r.get('room_type') == room_bucket:
            out.append((r['room_id'], r['room_type'], int(r['capacity_min']), int(r['capacity_max'])))
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
    rows = ws_bookings.get_all_records()
    for r in rows:
        if str(r.get('student_id')) == str(student_id) and r.get('date') == date_str and r.get('status') == 'active':
            return True
    return False


def append_booking_row(bkg):
    ws_bookings.append_row([
        bkg['booking_id'], bkg['student_id'], bkg['date'], bkg['start_time'], bkg['end_time'],
        bkg['room_type'], bkg['room_id'], json.dumps(bkg['slots']), bkg['created_at'], bkg['status']
    ])


def find_and_hold_room_for_period(date_obj: date, start_dt: datetime, end_dt: datetime, internal_room_type: str, student_id: str):
    """Pick the first free room of the requested type and occupy its slots in Schedule.
       Returns (room_id, slots) or (None, None) if none found.
    """
    slots = slots_from_period(start_dt, end_dt)
    bucket = bucket_from_internal_type(internal_room_type)
    if not bucket:
        return None, None

    # one booking per student per day guard
    dstr = _date_str(date_obj)
    if has_active_booking(student_id, dstr):
        return None, None

    rooms = list_rooms_by_type(bucket)
    for room_id, room_type, _, _ in rooms:
        row_idx = ensure_schedule_row(dstr, room_id, room_type)
        if slots_free(row_idx, slots):
            occupy_slots(row_idx, slots, booking_id="HOLD")  # temporary hold marker
            return room_id, slots
    return None, None


def finalize_booking(student_id: str, date_obj: date, start_dt: datetime, end_dt: datetime, internal_room_type: str, room_id: str, slots: list[int]):
    dstr = _date_str(date_obj)
    start_str = start_dt.strftime('%I:%M %p')
    end_str = end_dt.strftime('%I:%M %p')
    booking_id = f"BKG-{uuid.uuid4().hex[:10].upper()}"

    # replace HOLD with booking_id
    row_idx = ensure_schedule_row(dstr, room_id, bucket_from_internal_type(internal_room_type))
    updates = []
    for a1 in schedule_cells_for_slots(row_idx, slots):
        updates.append({'range': f"{ws_schedule.title}!{a1}", 'values': [[booking_id]]})
    ws_schedule.batch_update(updates)

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
    return booking_id


def cancel_by_student_and_date(student_id: str, date_obj: date) -> bool:
    dstr = _date_str(date_obj)
    # find active booking row
    data = ws_bookings.get_all_values()
    # headers at row 1
    for r in range(2, len(data) + 1):
        row = ws_bookings.row_values(r)
        if not row:
            continue
        rec = dict(zip(HEADERS_BOOKINGS, row + [None]*(len(HEADERS_BOOKINGS)-len(row))))
        if rec.get('student_id') == str(student_id) and rec.get('date') == dstr and rec.get('status') == 'active':
            # free schedule slots
            room_id = rec.get('room_id')
            slots = json.loads(rec.get('slots_json') or '[]')
            row_idx = ensure_schedule_row(dstr, room_id, '')
            free_slots(row_idx, slots)
            # mark cancelled
            ws_bookings.update_cell(r, HEADERS_BOOKINGS.index('status') + 1, 'cancelled')
            return True
    return False

# ===============================
# Minimal NLU helpers (same as your current ones where needed)
# ===============================
# ... You can copy your existing NLU/context helpers here unchanged ...

# ===============================
# Example integration in handlers (snippets)
# ===============================
# 1) During confirmation step, BEFORE sending the confirmation message, pick and hold a room
#    using the computed time period from your existing booking_time.

# def handle_book_room(req):
#     state = collect_by_steps(req)
#     ok, msg, time_str, start_dt, end_dt = parse_and_validate_timeperiod(state.get("booking_time"))
#     if not ok:
#         return jsonify({"fulfillmentText": msg, "outputContexts": _sticky_outcontexts(req, state)})
#     state["time"] = time_str
#
#     # auto category and internal room_type (you already have this logic)
#     cat = auto_category_from_size(state.get("room_size"))
#     state["room_category"] = cat
#     internal_type = room_type_from_size_and_category(state.get("room_size"), cat)
#     if not internal_type:
#         return jsonify({"fulfillmentText": "Unsupported group size.", "outputContexts": _sticky_outcontexts(req, state)})
#
#     # pick and hold a concrete room
#     date_obj = parser.isoparse(state["date"]).date() if isinstance(state["date"], str) else state["date"]
#     room_id, slots = find_and_hold_room_for_period(date_obj, start_dt, end_dt, internal_type, str(state.get("student_id") or "PENDING"))
#     if not room_id:
#         return jsonify({"fulfillmentText": "No rooms available for that time.", "outputContexts": _sticky_outcontexts(req, state)})
#
#     state["room_type"] = internal_type
#     state["room_id"] = room_id
#     state["slots"] = slots
#
#     # confirmation message (friendly)
#     size_val = state.get("room_size")
#     size_display = str(int(size_val)) if isinstance(size_val, float) and size_val.is_integer() else str(size_val)
#     return jsonify({
#         "fulfillmentText": (
#             f"Let me confirm your booking: a {_display_room_type(internal_type)} in room {room_id} "
#             f"for {size_display} person{'s' if size_display != '1' else ''} on {state['date']} from {time_str}. "
#             "Say 'Yes' to confirm or 'No' to cancel."),
#         "outputContexts": _sticky_outcontexts(req, state, extra_ctx=[("awaiting_confirmation", 5)])
#     })
#
# 2) On ConfirmBooking, finalize the booking: write booking row and replace HOLD with booking_id
#
# def handle_confirm_booking(req):
#     params = _get_ctx_params(req, CTX_BOOKING) or get_stored_params(get_session_id(req))
#     student_id = params.get('student_id')
#     if not student_id or not str(student_id).isdigit() or len(str(student_id)) != 7:
#         return jsonify({"fulfillmentText": "Please enter your 7-digit student ID.",
#                         "outputContexts": _sticky_outcontexts(req, booking_params=params, extra_ctx=[(CTX_AWAIT_CONFIRM, 5)])})
#     date_obj = datetime.strptime(params['date'], '%d/%m/%Y').date()
#     ok, _, _, start_dt, end_dt = parse_and_validate_timeperiod(params.get('booking_time'))
#     if not ok:
#         return jsonify({"fulfillmentText": "Time invalid.", "outputContexts": _sticky_outcontexts(req, params)})
#     booking_id = finalize_booking(student_id=str(student_id), date_obj=date_obj, start_dt=start_dt, end_dt=end_dt,
#                                   internal_room_type=params['room_type'], room_id=params['room_id'], slots=params['slots'])
#     return jsonify({"fulfillmentText": "✅ Your booking has been saved successfully."})
#
# 3) On CancelBooking, free slots and mark booking cancelled
#
# def handle_cancel_booking(req):
#     params = _get_ctx_params(req, CTX_BOOKING) or get_stored_params(get_session_id(req))
#     student_id = req.get("queryResult", {}).get("parameters", {}).get("student_id") or params.get("student_id")
#     date_param = req.get("queryResult", {}).get("parameters", {}).get("date") or params.get("date")
#     date_obj = parse_date(date_param)
#     if not (student_id and date_obj):
#         return jsonify({"fulfillmentText": "Please provide your 7-digit student ID and the date to cancel (today/tomorrow or dd/mm/YYYY).",
#                         "outputContexts": _sticky_outcontexts(req, params)})
#     ok = cancel_by_student_and_date(str(student_id), date_obj)
#     if ok:
#         return jsonify({"fulfillmentText": "Got it. The booking has been cancelled.", "outputContexts": _sticky_outcontexts(req)})
#     return jsonify({"fulfillmentText": "No booking found for that student and date.", "outputContexts": _sticky_outcontexts(req)})

# The rest of your existing webhook, menu, and flow functions can remain as-is.


# ===============================
# SETUP CHECKLIST (Google Sheets)
# ===============================
"""
What you need to do in Google Sheets:

1) Spreadsheet
   • Create (or reuse) a spreadsheet titled exactly: library-bot-sheet
   • Share it with your service account email from credentials.json (Editor access).

2) Tabs (worksheets)
   • No manual tabs required — the code auto-creates three sheets if missing:
       - Rooms      (columns: room_id, room_type, capacity_min, capacity_max)
       - Schedule   (columns: date, room_id, room_type, S1..S24)
       - Bookings   (columns: booking_id, student_id, date, start_time, end_time, room_type, room_id, slots_json, created_at, status)
   • If they already exist, headers will be ensured/updated but your data will be preserved.

3) Room inventory seeding
   • On first run, Rooms is auto-seeded to:
       - 18 solo rooms (SOLO-01..SOLO-18)
       - 22 small rooms (S-01..S-22)
       - 13 medium rooms (M-01..M-13)
       - 8 large rooms (L-01..L-08)
   • If you already have room rows, seeding is skipped.

4) Time/slot model
   • Library open 08:00–20:00 (exclusive), 30‑minute slots ⇒ 24 slots/day.
   • 2 hours = 4 consecutive slots. Times must align to 30‑min boundaries.
   • Date format in sheets is dd/mm/YYYY (adjust _date_str if you prefer another).

5) Migration from your old sheet1
   • This code does not use the old sheet1 headers (Student ID, Category, Size, Date, Time).
   • You can keep sheet1 for legacy, or delete it. New bookings write to Bookings and slot occupancy to Schedule.

6) Cancellation
   • Cancelling removes slot occupancy in Schedule and marks the booking row as status=cancelled in Bookings.

7) Quotas & timezone
   • Ensure the Google project has Sheets/Drive APIs enabled.
   • Server timezone should match your business rules; all displayed dates use dd/mm/YYYY.

No other manual formatting is required. Start the app — it will provision tabs and headers on first use.
"""
