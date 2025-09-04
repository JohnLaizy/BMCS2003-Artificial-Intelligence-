# -*- coding: utf-8 -*-
"""
Library Bot — Google Sheets slot scheduling (08:00–20:00, 30‑min slots)

This single-file app keeps your existing menu text/format, and implements:
  • Sheets schema: Rooms, Schedule, Bookings (auto‑created with headers)
  • Slot model: 24 slots per room/day; 2h = 4 slots
  • Room pools: 22 small, 13 medium, 8 large, 18 solo
  • Auto category from size: 1 → solo, >1 → discussion (S/M/L buckets)
  • One booking per student per day (enforced at booking‑time)
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
CLOSE_HOUR = 20            # exclusive upper bound
SLOT_MINUTES = 30          # 24 slots/day
MAX_BOOKING_HOURS = 2      # 2h => 4 slots
ALLOW_UNTIL_MIDNIGHT = False

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
    for r_idx in range(2, len(data) + 1):
        row = ws_schedule.row_values(r_idx)
        if len(row) >= 3 and row[0] == date_str and row[1] == room_id:
            return r_idx
    empty_slots = ["" for _ in range(24)]
    ws_schedule.append_row([date_str, room_id, room_type] + empty_slots)
    return ws_schedule.row_count


def schedule_cells_for_slots(row_idx: int, slots: list[int]):
    ranges = []
    for s in slots:
        col = 3 + s  # S1 at column D(4) => 3 + 1
        ranges.append(gspread.utils.rowcol_to_a1(row_idx, col))
    logging.debug(f"schedule_cells_for_slots(row={row_idx}, slots={slots}) -> {ranges}")

    return ranges


def slots_free(row_idx: int, slots: list[int]) -> bool:
    a1s = schedule_cells_for_slots(row_idx, slots)  # e.g. ['P3','Q3','R3','S3']
    logging.debug(f"slots_free() checking ranges (worksheet-scoped): {a1s}")
    cells = ws_schedule.batch_get(a1s)  # IMPORTANT: do NOT prefix sheet title here
    for val in cells:
        if val and val[0] and val[0][0]:
            return False
    return True


def occupy_slots(row_idx: int, slots: list[int], booking_id: str):
    updates = []
    for a1 in schedule_cells_for_slots(row_idx, slots):
        updates.append({'range': a1, 'values': [[booking_id]]})
    logging.debug(f"occupy_slots() updating ranges (worksheet-scoped): {[u['range'] for u in updates]}")
    ws_schedule.batch_update(updates)



def free_slots(row_idx: int, slots: list[int]):
    updates = []
    for a1 in schedule_cells_for_slots(row_idx, slots):
        updates.append({'range': a1, 'values': [[""]]})
    logging.debug(f"free_slots() clearing ranges (worksheet-scoped): {[u['range'] for u in updates]}")
    ws_schedule.batch_update(updates)


# ===============================
# Room picking
# ===============================

def list_rooms_by_type(room_bucket: str) -> list[tuple[str, str, int, int]]:
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

    dstr = _date_str(date_obj)
    if has_active_booking(student_id, dstr):
        return None, None

    rooms = list_rooms_by_type(bucket)
    for room_id, room_type, _, _ in rooms:
        row_idx = ensure_schedule_row(dstr, room_id, room_type)
        if slots_free(row_idx, slots):
            occupy_slots(row_idx, slots, booking_id=f"HOLD:{student_id}")  # hold tied to student
            return room_id, slots
    return None, None


def finalize_booking(student_id: str, date_obj: date, start_dt: datetime, end_dt: datetime, internal_room_type: str, room_id: str, slots: list[int]):
    dstr = _date_str(date_obj)
    start_str = start_dt.strftime('%I:%M %p')
    end_str = end_dt.strftime('%I:%M %p')
    booking_id = f"BKG-{uuid.uuid4().hex[:10].upper()}"

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
    "already_booked": "⚠ You already booked for that day (one per day).",
    "invalid_date": "⚠ Invalid date format: {}",
    "invalid_time": "⚠ Invalid time format. Please provide both start and end clearly.",
    "outside_hours": "⚠ Booking time must be between 8 AM and 8 PM (or until midnight during exam period).",
    "too_long": "⚠ You can only book up to 2 hours per session.",
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
    if not time_period or not isinstance(time_period, dict):
        return False, RESPONSE['missing_time'], None, None, None
    start_time = time_period.get('startTime')
    end_time = time_period.get('endTime')
    if not start_time or not end_time:
        return False, RESPONSE['missing_time'], None, None, None
    try:
        start_obj = parser.isoparse(start_time)
        end_obj = parser.isoparse(end_time)
        same_day = (start_obj.date() == end_obj.date())
        allows_2400 = (
            ALLOW_UNTIL_MIDNIGHT and
            end_obj == start_obj.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        )
        if not same_day and not allows_2400:
            return False, RESPONSE['invalid_time'], None, None, None

        opening_dt = start_obj.replace(hour=OPEN_HOUR, minute=0, second=0, microsecond=0)
        if ALLOW_UNTIL_MIDNIGHT:
            closing_dt = start_obj.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        else:
            closing_dt = start_obj.replace(hour=CLOSE_HOUR, minute=0, second=0, microsecond=0)

        if not (opening_dt <= start_obj < end_obj <= closing_dt):
            return False, RESPONSE['outside_hours'], None, None, None

        # Enforce max duration
        duration_hours = (end_obj - start_obj).total_seconds() / 3600.0
        if duration_hours - MAX_BOOKING_HOURS > 1e-6:
            return False, RESPONSE['too_long'], None, None, None

        # Enforce 30‑minute boundaries
        if start_obj.minute not in (0, 30) or end_obj.minute not in (0, 30):
            return False, "⚠ Please book on 30-minute boundaries (e.g., 2:00–4:00 or 2:30–4:30).", None, None, None

        time_str = f"{start_obj.strftime('%I:%M %p')} to {end_obj.strftime('%I:%M %p')}"
        return True, None, time_str, start_obj, end_obj
    except Exception:
        logging.exception("Time parsing failed")
        return False, RESPONSE['invalid_time'], None, None, None

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

    # 2) Time
    if not state.get("booking_time"):
        return jsonify({
            "fulfillmentText": "🕒 What time would you like? (e.g., 2 PM to 4 PM)",
            "outputContexts": _sticky_outcontexts(req, state)
        })

    ok, msg, time_str, _, _ = parse_and_validate_timeperiod(state["booking_time"])
    if not ok:
        return jsonify({"fulfillmentText": msg, "outputContexts": _sticky_outcontexts(req, state)})
    if not state.get("time"):
        state["time"] = time_str

    # 3) Size
    if not state.get("room_size"):
        return jsonify({
            "fulfillmentText": "👥 How many people will use the room? (e.g., 1 or 3)",
            "outputContexts": _sticky_outcontexts(req, state)
        })

    # 4) Auto-assign category
    auto_cat = auto_category_from_size(state.get("room_size"))
    if not auto_cat:
        return jsonify({
            "fulfillmentText": "I couldn't understand the group size. Please enter a number (e.g., 1 or 3).",
            "outputContexts": _sticky_outcontexts(req, state)
        })
    state["room_category"] = auto_cat

    return jsonify({
        "fulfillmentText": f"Great — assigning a {auto_cat.upper()} room and checking availability...",
        "outputContexts": _sticky_outcontexts(req, state),
        "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"}
    })


def handle_welcome(req):
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
    session_id = get_session_id(req)
    update_session_store(session_id, {"booking_time": ""})
    return jsonify({
        "fulfillmentText": "Entering availability check. Which date would you like to check — today or tomorrow?",
        "outputContexts": _sticky_outcontexts(req, booking_params={"booking_time": None}),
        "followupEventInput": {"name": "EVT_CHECK", "languageCode": "en"}
    })


def handle_menu_book(req):
    if not _has_ctx(req, CTX_READY_TO_BOOK):
        return jsonify({
            "fulfillmentText": "Let's check availability first. Which date would you like — today or tomorrow?",
            "outputContexts": _sticky_outcontexts(req)
        })
    return jsonify({
        "fulfillmentText": "Proceeding to booking. Please enter your 7-digit student ID.",
        "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"}
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

    state["date"] = date_obj.strftime('%d/%m/%Y')
    state["time"] = time_str

    # Auto-derive room type
    cat = auto_category_from_size(state.get("room_size"))
    if not cat:
        return jsonify({
            "fulfillmentText": "I couldn't understand the group size. Please enter a number (e.g., 1 or 3).",
            "outputContexts": _sticky_outcontexts(req, state)
        })
    state["room_category"] = cat
    internal_type = room_type_from_size_and_category(state.get("room_size"), cat)
    if not internal_type:
        return jsonify({"fulfillmentText": "Unsupported group size for available rooms.", "outputContexts": _sticky_outcontexts(req, state)})

    # Pick and HOLD a specific room in Schedule
    room_id, slots = find_and_hold_room_for_period(date_obj, start_dt, end_dt, internal_type, str(state.get("student_id") or "PENDING"))
    if not room_id:
        return jsonify({"fulfillmentText": "No rooms available for that time.", "outputContexts": _sticky_outcontexts(req, state)})

    state["room_type"] = internal_type
    state["room_id"] = room_id
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
    return jsonify({
        "fulfillmentText": (
            f"Let me confirm your booking: a {_display_room_type(internal_type)} in room {room_id} "
            f"for {size_display} person{_def_plur(size_display)} on {state['date']} from {time_str}. "
            "Say 'Yes' to confirm or 'No' to cancel."),
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


    student_id = params.get('student_id')
    if not student_id or not str(student_id).isdigit() or len(str(student_id)) != 7:
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
    logging.debug(f"CONFIRM — required_ok={all(required)}, slots_ok={slots_ok}")
    if not (all(required) and slots_ok):
        _dbg_kv("CONFIRM — MISSING FIELDS", {
            "room_type": params.get("room_type"),
            "room_id": params.get("room_id"),
            "slots": params.get("slots"),
            "slots_json": params.get("slots_json"),
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
    return jsonify({"fulfillmentText": "✅ Your booking has been saved successfully."})


def handle_cancel_booking(req):
    params = _get_ctx_params(req, CTX_BOOKING) or get_stored_params(get_session_id(req))
    student_id = req.get("queryResult", {}).get("parameters", {}).get("student_id") or params.get("student_id")
    date_param = req.get("queryResult", {}).get("parameters", {}).get("date") or params.get("date")
    date_obj = parse_date(date_param)

    if not (student_id and date_obj):
        return jsonify({
            "fulfillmentText": "Please provide your 7-digit student ID and the date to cancel (today/tomorrow or dd/mm/YYYY).",
            "outputContexts": _sticky_outcontexts(req, params)
        })

    ok = cancel_by_student_and_date(str(student_id), date_obj)
    if ok:
        return jsonify({"fulfillmentText": "Got it. The booking has been cancelled.", "outputContexts": _sticky_outcontexts(req)})
    return jsonify({"fulfillmentText": "No booking found for that student and date.", "outputContexts": _sticky_outcontexts(req)})


def handle_cancel_after_confirmation(req):
    return jsonify({"fulfillmentText": RESPONSE['cancel_confirm'], "outputContexts": _sticky_outcontexts(req)})


def handle_library_info(req):
    return jsonify({"fulfillmentText": RESPONSE["library_info"], "outputContexts": _sticky_outcontexts(req)})


def handle_default(req):
    return jsonify({"fulfillmentText": RESPONSE['unknown']})

# ===============================
# Intent Map
# ===============================

INTENT_HANDLERS = {
    'Welcome': handle_welcome,
    'Menu_CheckAvailability': handle_menu_check,
    'Menu_BookRoom': handle_menu_book,
    'Menu_CancelBooking': handle_menu_cancel,
    'Menu_LibraryInfo': handle_library_info,

    # Check Flow (single driver)
    'CheckAvailability_Date': handle_flow,
    'ProvideRoomSize': handle_flow,
    'ProvideTime': handle_flow,

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
    req = request.get_json()
    intent = req['queryResult']['intent']['displayName']
    raw_turn_params = req.get("queryResult", {}).get("parameters", {}) or {}
    _dbg_kv("RAW TURN PARAMS", raw_turn_params) 
    logging.info(f"==============================📥 Incoming Intent: {intent}==============================")
    handler = INTENT_HANDLERS.get(intent, handle_default)
    response = handler(req)
    logging.info(f"📤 Fulfillment response: {response.get_json() if hasattr(response, 'get_json') else response}")
    return response

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
