# ===============================
# 📦 Imports
# ===============================
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime, date, timedelta, time as dtime
from dateutil import parser
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import json

# ===============================
# 📜 Logging
# ===============================
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# ===============================
# 🧠 Session Store (local, in-memory)
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


# ===============================
# 🧪 Debug log of context flow
# ===============================
def log_input_output_contexts(req):
    session = req.get('session', 'unknown_session')
    input_contexts = req.get('queryResult', {}).get('outputContexts', [])
    logging.info(f"🔍 [Session: {session}] Input Contexts:")
    for ctx in input_contexts:
        logging.info(json.dumps(ctx, indent=2, default=str))


def log_context_update(req, context_name, new_params):
    session = req.get('session', 'unknown_session')
    logging.debug(f"📦 Updating context: {context_name} in session {session}")
    logging.debug(f"🔁 New parameters to merge into '{context_name}': {new_params}")
    for ctx in req.get("queryResult", {}).get("outputContexts", []):
        if context_name in ctx.get("name", ""):
            logging.debug(f"📤 Existing parameters in '{context_name}': {ctx.get('parameters')}")
            break


def _log_input_contexts(req):
    session = req.get("session", "")
    contexts = req.get("queryResult", {}).get("outputContexts", [])
    logging.info(f"🔍 [Session: {session}] Input Contexts:")
    for ctx in contexts:
        logging.info(json.dumps(ctx, indent=2, default=str))
        
def get_from_ctx(req, ctx_suffix, key): #helps getting data from context
    for c in req.get("queryResult", {}).get("outputContexts", []):
        name = c.get("name", "").lower()
        if name.endswith(f"/{ctx_suffix.lower()}"):
            v = (c.get("parameters") or {}).get(key)
            if v not in ("", None, []):
                return v
    return None



# ===============================
# 🚀 Flask
# ===============================
app = Flask(__name__)
CORS(app)


# ===============================
# ⏰ Opening hours
# ===============================
ALLOW_UNTIL_MIDNIGHT = False

# ===============================
# 📊 Google Sheets
# ===============================
scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)

SHEET_TITLE = 'library-bot-sheet'
REQUIRED_HEADERS = ["Student ID", "Category", "Size", "Date", "Time"]

# Check if the room is ready to book
def _is_ready_to_book(state: dict) -> bool:
    """All core fields must be present before we can proceed to booking."""
    return bool(
        state.get("date") and
        state.get("booking_time") and
        state.get("room_size")    
    )

def _ensure_headers(ws):
    try:
        first_row = ws.row_values(1)
        if first_row != REQUIRED_HEADERS:
            ws.resize(rows=max(ws.row_count, 1), cols=len(REQUIRED_HEADERS))
            ws.update('A1:E1', [REQUIRED_HEADERS])
            logging.info(f"✅ Headers ensured: {REQUIRED_HEADERS}")
    except Exception:
        logging.exception("❌ Failed to ensure headers on sheet")


try:
    sheet = client.open(SHEET_TITLE).sheet1
    _ensure_headers(sheet)
    logging.info(f"✅ Connected to Google Sheets: {SHEET_TITLE} / sheet1")
except Exception:
    logging.exception("❌ Failed to open Google Sheet. Check title/share/permissions.")
    raise


def get_all_bookings():
    return sheet.get_all_records()


def append_booking(student_id, category, size, date_str, time_str):
    try:
        row = [student_id, category, size, date_str, time_str]
        sheet.append_row(row)
        logging.info(f"📝 Appended row: {row}")
        return True
    except Exception:
        logging.exception("❌ append_booking failed")
        return False


# ===============================
# 🧱 Context names
# ===============================
CTX_MENU = "awaiting_menu"
CTX_BOOKING = "booking_info"
CTX_CHECK_FLOW = "check_flow"
CTX_READY_TO_BOOK = "ready_to_book"
CTX_AWAIT_CONFIRM = "awaiting_confirmation"


def _get_ctx_params(req, ctx_name=CTX_BOOKING):
    for c in req['queryResult'].get('outputContexts', []):
        if ctx_name in c.get('name', ''):
            logging.info(f"🔍 Found context '{ctx_name}': {json.dumps(c.get('parameters', {}), indent=2, default=str)}")
            return c.get('parameters', {}) or {}
    logging.info(f"⚠ Context '{ctx_name}' not found in request")
    return {}


def _has_ctx(req, ctx_name):
    for c in req['queryResult'].get('outputContexts', []):
        if ctx_name in c.get('name', '') and c.get('lifespanCount', 0) > 0:
            return True
    return False

def get_param(req, name, ctx_name="booking_info"):
    # 1) This turn (slot-filled)
    val = req.get("queryResult", {}).get("parameters", {}).get(name)
    if val not in ("", None, []):
        return val
    # 2) From context (previous turns)
    for c in req.get("queryResult", {}).get("outputContexts", []):
        if ctx_name in c.get("name", ""):
            v = c.get("parameters", {}).get(name)
            if v not in ("", None, []):
                return v
    return None

def _resolve_date(req):
    # explicit typed date (from @re_date)
    explicit = get_param(req, "explicit_date")
    if explicit not in ("", None, []):
        return explicit
    # selector (today/tomorrow)
    sel = get_param(req, "date")
    if sel in ("today", "tomorrow"):  # from @date_selector
        return sel
    # legacy fallback
    return sel

# ===============================
# 👣 Step-aware getters (current turn → step context → booking_info)
# ===============================

def get_from_ctx(req, ctx_suffix, key): #helps getting data from context
    for c in req.get("queryResult", {}).get("outputContexts", []):
        name = c.get("name", "").lower()
        if name.endswith(f"/{ctx_suffix.lower()}"):
            v = (c.get("parameters") or {}).get(key)
            if v not in ("", None, []):
                return v
    return None

def get_param_from_steps(req, key, step_ctx_suffix, booking_ctx="booking_info"): # sets the priority in which the context is checked
    # 1) current turn slot
    v = req.get("queryResult", {}).get("parameters", {}).get(key)
    if v not in ("", None, []):
        return v
    # 2) this step's specific context (e.g., checkavailability_time)
    v = get_from_ctx(req, step_ctx_suffix, key)
    if v not in ("", None, []):
        return v
    # 3) fallback → consolidated sticky context
    return get_param(req, key, ctx_name=booking_ctx)

def collect_by_steps(req):
    return {
        # Date comes from checkDate step → output context: prompt_time
        "date":          get_param_from_steps(req, "date",          "prompt_time"),
        "explicit_date": get_param_from_steps(req, "explicit_date", "prompt_time"),

        # Time comes from ProvideTime step → output context: prompt_size
        "booking_time":  get_param_from_steps(req, "booking_time",  "prompt_size"),

        # Room size comes from ProvideRoomSize step → output context: prompt_category
        "room_size":     get_param_from_steps(req, "room_size",     "prompt_category"),

        # Category comes from ChooseCategory step → output context: awaiting_confirmation
        "room_category": get_param_from_steps(req, "room_category", "awaiting_confirmation"),

        # Student ID is also asked at confirmation time
        "student_id":    get_param_from_steps(req, "student_id",    "awaiting_confirmation"),
    }

# ===============================
# Friendlier display output
# ===============================
ROOM_TYPE_DISPLAY = {
    "SOLO-1": "Solo room",
    "DISCUSSION-S": "Small discussion room",
    "DISCUSSION-M": "Medium discussion room",
    "DISCUSSION-L": "Large discussion room",
}

def _display_room_type(code: str) -> str:
    """Return a friendly display name for an internal room_type code."""
    return ROOM_TYPE_DISPLAY.get(code, code or "room")





# ===============================
# 🔤 Schema normalization (camel→snake) + safe merge
# ===============================
SNAKE_KEYS = {
    "student_id": "student_id",
    "room_category": "room_category",
    "room_size": "room_size",
    "date": "date",
    "booking_time": "booking_time",
    "time": "time",
    "room_type": "room_type"
}

CAMEL_TO_SNAKE = {
    "roomCategory": "room_category",
    "roomSize": "room_size",
    "StudentID": "student_id",
    "studentId": "student_id",
    "RoomCategory": "room_category",
    "RoomSize": "room_size",
    "TimePeriod": "booking_time",
    "Date": "date",
    "roomType": "room_type",
}

ALLOWED_KEYS = set(SNAKE_KEYS.values())  # allowed schema keys


def _to_snake_params(p: dict) -> dict:
    out = {}
    for k, v in (p or {}).items():
        if v in ("", None, []):
            continue
        k_snake = CAMEL_TO_SNAKE.get(k, k)
        if k_snake in ALLOWED_KEYS:
            out[k_snake] = v
    return out


def _merge_ctx_params(existing: dict, new_params: dict) -> dict:
    ex = _to_snake_params(existing or {})
    nw = _to_snake_params(new_params or {})
    merged = {**ex, **nw}
    for k in ALLOWED_KEYS:
        merged.setdefault(k, "")
    return merged


def _ctx_obj(req, params: dict, ctx_name=CTX_BOOKING, lifespan=5):
    return {
        "name": f"{req['session']}/contexts/{ctx_name}",
        "lifespanCount": lifespan,
        "parameters": params
    }

# NEW: combine session buffer + existing context as the base state
# session buffer takes precedence over prior context
# 会话缓冲优先于先前的上下文

def _get_buffered_params(req) -> dict:
    ctx = _get_ctx_params(req, CTX_BOOKING)
    ssn = get_stored_params(get_session_id(req))
    return _merge_ctx_params(ctx, ssn)

def _buffer_to_event_params(req):
    st = collect_by_steps(req)
    return {k: v for k, v in st.items() if v not in ("", None, [])}

# Example:
    return jsonify({
        "fulfillmentText": "...",
        "outputContexts": _sticky_outcontexts(req, state),
        "followupEventInput": {
            "name": "EVT_BOOK_READY",
            "languageCode": "en",
            "parameters": _buffer_to_event_params(req)
        }
    })



# ===============================
# ✨ Normalizer + Sticky Contexts
# ===============================

def _norm_params(p: dict) -> dict:
    p = p or {}
    def pick(*names):
        for n in names:
            if n in p and p[n] not in (None, "", []):
                return p[n]
        return None
    return {
        "student_id":   pick("student_id", "StudentID", "studentId"),
        "roomCategory": pick("roomCategory", "room_category", "RoomCategory"),
        "roomSize":     pick("roomSize", "room_size", "RoomSize"),
        "date":         pick("date", "date-time", "date_time", "Date"),
        "booking_time": pick("booking_time", "time_period", "TimePeriod"),
    }


STICKY_LIFESPAN = 50


def _sticky_outcontexts(req, booking_params=None, extra_ctx=None, keep_menu=False):
    """Generate outputContexts based on the **session buffer as source of truth**,
    merge with incoming booking_params, then persist to both Dialogflow and session_store.
    基于会话缓冲作为真值来源，合并新参数，再同时写回 Dialogflow 上下文与本地缓存。
    """
    log_context_update(req, CTX_BOOKING, booking_params or {})
    session_id = get_session_id(req)
    out = []

    # 1) start from buffered params (session buffer > existing context)
    base = _get_buffered_params(req)

    # 2) merge new incoming params
    merged = _merge_ctx_params(base, booking_params or {})
    logging.info(f"📌 (buffer-first) Merged params for context: {json.dumps(merged, indent=2, default=str)}")

    # 3) write to Dialogflow contexts
    out.append(_ctx_obj(req, merged, CTX_BOOKING, lifespan=STICKY_LIFESPAN))

    # 4) mirror to local session store (becomes the new buffer)
    update_session_store(session_id, merged)

    # 5) any extra contexts
    for item in (extra_ctx or []):
        if isinstance(item, tuple) and len(item) == 2:
            nm, life = item
            out.append({"name": f"{req['session']}/contexts/{nm}", "lifespanCount": life})
        elif isinstance(item, str):
            out.append({"name": f"{req['session']}/contexts/{item}", "lifespanCount": STICKY_LIFESPAN})

    if not keep_menu:
        out.append({"name": f"{req['session']}/contexts/{CTX_MENU}", "lifespanCount": 0})

    logging.info("📤 OutputContexts generated (buffer-first):")
    for ctx in out:
        logging.info(json.dumps(ctx, indent=2, default=str))
    return out


def _reset_all_to_menu(req):
    # Clear server-side memory too
    session_id = get_session_id(req)
    session_store.pop(session_id, None)
    return [
        {"name": f"{req['session']}/contexts/{CTX_BOOKING}", "lifespanCount": 0},
        {"name": f"{req['session']}/contexts/{CTX_CHECK_FLOW}", "lifespanCount": 0},
        {"name": f"{req['session']}/contexts/{CTX_READY_TO_BOOK}", "lifespanCount": 0},
        {"name": f"{req['session']}/contexts/{CTX_AWAIT_CONFIRM}", "lifespanCount": 0},
        {"name": f"{req['session']}/contexts/{CTX_MENU}", "lifespanCount": 5},
    ]


# ===============================
# 🤖 Responses
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
    "outside_hours": "⚠ Booking time must be between 8 AM and 10 PM (or until midnight during exam period).",
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
    "library_info": "Library hours: 8:00 AM - 10:00 PM daily. Solo rooms fit 1 person; discussion rooms fit 2-6 people."
}


# ===============================
# 🗓 Date parsing (robust)
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


# ===============================
# ⏱ Time period parsing
# ===============================

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
        opening_dt = start_obj.replace(hour=8, minute=0, second=0, microsecond=0)
        if ALLOW_UNTIL_MIDNIGHT:
            closing_dt = start_obj.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        else:
            closing_dt = start_obj.replace(hour=22, minute=0, second=0, microsecond=0)
        if not (opening_dt <= start_obj < end_obj <= closing_dt):
            return False, RESPONSE['outside_hours'], None, None, None
        duration_hours = (end_obj - start_obj).total_seconds() / 3600.0
        if duration_hours - 2.0 > 1e-6:
            return False, RESPONSE['too_long'], None, None, None
        time_str = f"{start_obj.strftime('%I:%M %p')} to {end_obj.strftime('%I:%M %p')}"
        return True, None, time_str, start_obj, end_obj
    except Exception:
        logging.exception("Time parsing failed")
        return False, RESPONSE['invalid_time'], None, None, None

# ===============================
# Room Allocation
# ===============================
def _go_back_to_size(req, state, prompt_text):
    """
    Move the user back to the 'ProvideRoomSize' step by:
    - Setting prompt_size context so the size intent can trigger.
    - Dropping prompt_category/awaiting_confirmation so they don't interfere.
    Note: we don't need to erase old room_size; the new slot (current turn) will override it.
    """
    return jsonify({
        "fulfillmentText": prompt_text,
        "outputContexts": _sticky_outcontexts(
            req,
            state,
            extra_ctx=[
                ("prompt_size", 5),
                ("prompt_category", 0),
                (CTX_AWAIT_CONFIRM, 0),
            ]
        )
    })


def _size_to_int(room_size):
    """
    Best-effort integer headcount from room_size.
    - If already int, return it.
    - If label (Small/Medium/Large), map to a representative size.
    - Else return None (unknown).
    """
    if isinstance(room_size, int):
        return room_size

    if isinstance(room_size, float):
        # if it's a whole number (1.0, 4.0), round it
        if room_size.is_integer():
            return int(room_size)
        return None  # reject non-whole floats like 2.5

    if isinstance(room_size, str) and room_size.strip().isdigit():
        return int(room_size.strip())

    return None

def _auto_category_from_size(room_size):
    """
    Returns 'solo' when headcount == 1, 'discussion' when >1, or None if unknown.
    """
    n = _size_to_int(room_size)
    if n is None:
        return None
    return "solo" if n == 1 else "discussion"


def assign_room_if(room_size, room_category):
    """
    Decide room_type based on room_size (int) and room_category ("solo"/"discussion").
    With auto-assignment, no need to check for conflicts anymore.
    """
    n = _size_to_int(room_size)
    cat = (room_category or "").strip().lower()

    if cat == "solo":
        return {"ok": True, "room_type": "SOLO-1", "capacity": (1, 1), "note": ""}

    if cat == "discussion":
        if 2 <= n <= 3:
            return {"ok": True, "room_type": "DISCUSSION-S", "capacity": (2, 3), "note": ""}
        if 4 <= n <= 6:
            return {"ok": True, "room_type": "DISCUSSION-M", "capacity": (4, 6), "note": ""}
        if 7 <= n <= 9:
            return {"ok": True, "room_type": "DISCUSSION-L", "capacity": (7, 9), "note": ""}
        return {"ok": False, "room_type": "", "capacity": None,
                "note": "Discussion rooms support 2–9 people."}

    return {"ok": False, "room_type": "", "capacity": None,
            "note": "room_category must be SOLO or DISCUSSION."}


# ===============================
# 🤖 Intent Handlers
# ===============================

def handle_flow(req):
    """
    Single flow driver that:
    - Ensures date, time, size are present (prompts for what's missing).
    - Validates time period.
    - Auto-assigns room_category from room_size.
    - Triggers booking when ready.
    """
    state = collect_by_steps(req)

    # 1) Date
    date_param = state.get("explicit_date") or state.get("date")
    date_obj = parse_date(date_param)
    if not date_obj:
        return jsonify({
            "fulfillmentText": "📅 Which date would you like to book — today or tomorrow?",
            "outputContexts": _sticky_outcontexts(req, state)
        })

    # Normalize date string early so it persists in sticky context
    state["date"] = date_obj.strftime("%d/%m/%Y")

    # 2) Time
    if not state.get("booking_time"):
        return jsonify({
            "fulfillmentText": "🕒 What time would you like? (e.g., 2 PM to 4 PM)",
            "outputContexts": _sticky_outcontexts(req, state)
        })

    ok, msg, time_str, _, _ = parse_and_validate_timeperiod(state["booking_time"])
    if not ok:
        return jsonify({
            "fulfillmentText": msg,
            "outputContexts": _sticky_outcontexts(req, state)
        })

    if not state.get("time"):
        state["time"] = time_str

    # 3) Size
    if not state.get("room_size"):
        return jsonify({
            "fulfillmentText": "👥 How many people will use the room? (e.g., 1 or 3)",
            "outputContexts": _sticky_outcontexts(req, state)
        })

    # 4) Auto-assign category from size
    # logging.debug(f"[room_size raw] type={type(state.get('room_size'))} value={repr(state.get('room_size'))}")
    auto_cat = _auto_category_from_size(state.get("room_size"))
    if not auto_cat:
        return jsonify({
            "fulfillmentText": "I couldn't understand the group size. Please enter a number (e.g., 1 or 3).",
            "outputContexts": _sticky_outcontexts(req, state)
        })
    state["room_category"] = auto_cat

    # 5) All set → trigger booking
    return jsonify({
        "fulfillmentText": f"Great — assigning a {auto_cat.upper()} room and checking availability...",
        "outputContexts": _sticky_outcontexts(req, state),
        "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"}
    })


def handle_welcome(req):
    lines = [ln for ln in RESPONSE['welcome'].split("\n") if ln.strip()]
    return jsonify({
        "fulfillmentMessages": [{"text": {"text": [ln]}} for ln in lines],
        "outputContexts": _reset_all_to_menu(req)
    })


def handle_menu_check(req):
    # Clear any stale booking_time on entering check flow; persist locally too
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


def handle_check_availability(req):
    state = collect_by_steps(req)
    # Expecting the date at this step
    if not state.get("date") and not state.get("explicit_date"):
        return jsonify({
            "fulfillmentText": "📅 Which date would you like to check — today or tomorrow?",
            "outputContexts": _sticky_outcontexts(req, state)
        })
    # Keep asking next step
    return jsonify({
        "fulfillmentText": "Got it. What time would you like? (e.g., 2 PM to 5 PM)",
        "outputContexts": _sticky_outcontexts(req, state)
    })

def handle_provide_time(req):
    state = collect_by_steps(req)

    if not state.get("booking_time"):
        return jsonify({
            "fulfillmentText": "🕒 What time would you like? e.g., 2 PM to 5 PM.",
            "outputContexts": _sticky_outcontexts(req, state)
        })

    ok, msg, time_str, _, _ = parse_and_validate_timeperiod(state["booking_time"])
    if not ok:
        return jsonify({
            "fulfillmentText": msg,
            "outputContexts": _sticky_outcontexts(req, state)
        })

    state["time"] = time_str

    # If time completes the required set, jump to booking
    if _is_ready_to_book(state):
        return jsonify({
            "fulfillmentText": "Great—checking rooms...",
            "outputContexts": _sticky_outcontexts(req, state),
            "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"}
        })

    # Otherwise ask next slot
    return jsonify({
        "fulfillmentText": "👥 How many people will be using the room?",
        "outputContexts": _sticky_outcontexts(req, state, extra_ctx=[("prompt_category", 5)])
    })


def handle_provide_size(req):
    state = collect_by_steps(req)
    size_val = state.get("room_size")

    if size_val in ("", None, []):
        return jsonify({
            "fulfillmentText": "How many people will use the room? (e.g., 1 or 3)",
            "outputContexts": _sticky_outcontexts(req, state, extra_ctx=[("prompt_size", 5)])
        })

    # Auto-assign category from size
    logging.debug(f"[room_size raw] type={type(state.get('room_size'))} value={repr(state.get('room_size'))}")
    auto_cat = _auto_category_from_size(size_val)
    if not auto_cat:
        return jsonify({
            "fulfillmentText": "I couldn't understand the group size. Please enter a number (e.g., 1 or 3).",
            "outputContexts": _sticky_outcontexts(req, state, extra_ctx=[("prompt_size", 5)])
        })
    logging.debug(f"[room_size raw] type={type(state.get('room_size'))} value={repr(state.get('room_size'))}")

    state["room_category"] = auto_cat  # persist
    # Do NOT push prompt_category anymore (we no longer ask for category)

    if _is_ready_to_book(state):
        return jsonify({
            "fulfillmentText": f"Got it. Assigning a {auto_cat.upper()} room and checking availability...",
            "outputContexts": _sticky_outcontexts(req, state),
            "followupEventInput": {"name": "EVT_BOOK", "languageCode": "en"}
        })

    # If something else is missing, continue the flow naturally
    return jsonify({
        "fulfillmentText": "Noted. Which date would you like to book — today or tomorrow?",
        "outputContexts": _sticky_outcontexts(req, state)
    })



def handle_book_room(req):
    state = collect_by_steps(req)

    # Validate date & time
    date_obj = parse_date(state.get("date"))
    ok, msg, time_str, _, _ = parse_and_validate_timeperiod(state.get("booking_time"))
    if not date_obj:
        return jsonify({
            "fulfillmentText": "⚠ Please provide a valid date (today/tomorrow).",
            "outputContexts": _sticky_outcontexts(req, state)
        })
    if not ok:
        return jsonify({
            "fulfillmentText": msg,
            "outputContexts": _sticky_outcontexts(req, state)
        })

    if not state.get("time"):
        state["time"] = time_str

    size_text = state.get("room_size")
    state["date"] = date_obj.strftime("%d/%m/%Y")
    size_text = state.get("room_size")
    state["room_size"] = size_text

    cat = (state.get("room_category") or "").strip().lower()
    if not cat:
        cat = _auto_category_from_size(size_text)
        if not cat:
            return jsonify({
                "fulfillmentText": "I couldn't understand the group size. Please enter a number (e.g., 1 or 3).",
                "outputContexts": _sticky_outcontexts(req, state)
            })
        state["room_category"] = cat

    res = assign_room_if(room_size=state.get("room_size"), room_category=state.get("room_category"))
    if not res["ok"]:
        return jsonify({
            "fulfillmentText": res["note"],
            "outputContexts": _sticky_outcontexts(req, state)
        })
    state["room_type"] = res["room_type"]

    # Clean size display (remove .0)
    size_val = state.get("room_size")
    if isinstance(size_val, float) and size_val.is_integer():
        size_display = str(int(size_val))
    else:
        size_display = str(size_val)

    # Friendly room type
    room_type_str = _display_room_type(state.get("room_type"))
    date_str = state['date']
    time_str = state['time']

    return jsonify({
    "fulfillmentText": (
        f"Let me confirm your booking: a {room_type_str} "
        f"for {size_display} person{'s' if size_display != '1' else ''} "
        f"on {date_str} from {time_str}. "
        "Say 'Yes' to confirm or 'No' to cancel."
    ),
    "outputContexts": _sticky_outcontexts(req, state, extra_ctx=[("awaiting_confirmation", 5)])
})


def handle_confirm_booking(req):
    params = _get_ctx_params(req, CTX_BOOKING)

    # also backfill from local session store if needed
    if not params:
        params = get_stored_params(get_session_id(req))

    student_id = params.get('student_id')
    room_category = params.get('room_category')
    room_size = params.get('room_size')
    date_str = params.get('date')
    time_str = params.get('time')

    if not student_id or not str(student_id).isdigit() or len(str(student_id)) != 7:
        return jsonify({
            "fulfillmentText": "Please enter your 7-digit student ID.",
            "outputContexts": _sticky_outcontexts(req, booking_params=params, extra_ctx=[(CTX_AWAIT_CONFIRM, 5)])
        })

    if all([student_id, room_category, room_size, date_str, time_str]):
        ok = append_booking(student_id, room_category, room_size, date_str, time_str)
        if ok:
            return jsonify({"fulfillmentText": RESPONSE['confirm_success']})
        else:
            return jsonify({"fulfillmentText": "⚠ I couldn't save your booking. Please try again later or contact staff."})
    else:
        return jsonify({"fulfillmentText": RESPONSE['confirm_failed']})


def handle_cancel_booking(req):
    # (Optional) you could remove a row from Sheet by student+date; for now just respond
    return jsonify({"fulfillmentText": RESPONSE['cancel'], "outputContexts": _sticky_outcontexts(req)})


def handle_cancel_after_confirmation(req):
    return jsonify({"fulfillmentText": RESPONSE['cancel_confirm'], "outputContexts": _sticky_outcontexts(req)})


def handle_library_info(req):
    return jsonify({"fulfillmentText": RESPONSE["library_info"], "outputContexts": _sticky_outcontexts(req)})


def handle_default(req):
    return jsonify({"fulfillmentText": RESPONSE['unknown']})


# ===============================
# 🧠 Intent Map
# ===============================
INTENT_HANDLERS = {
    'Welcome': handle_welcome,
    'Menu_CheckAvailability': handle_menu_check,
    'Menu_BookRoom': handle_menu_book,
    'Menu_CancelBooking': handle_menu_cancel,
    'Menu_LibraryInfo': handle_menu_info,
    
    #Check Flow
    'CheckAvailability_Date': handle_flow,
    'ProvideRoomSize': handle_flow,
    'ProvideTime': handle_flow,
    # 'ChooseCategory': handle_flow,
    
    #Book
    'book_room': handle_book_room,
    'ConfirmBooking': handle_confirm_booking,
    'CancelBooking': handle_cancel_booking,
    'CancelAfterConfirmation': handle_cancel_after_confirmation,
    'LibraryInfo': handle_library_info
}


# ===============================
# 🌐 Webhook entry
# ===============================
@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json()
    intent = req['queryResult']['intent']['displayName']
    logging.info(f"\n==============================\n📥 Incoming Intent: {intent}\n==============================")
    log_input_output_contexts(req)
    handler = INTENT_HANDLERS.get(intent, handle_default)
    response = handler(req)
    logging.info(f"📤 Fulfillment response: {response.get_json() if hasattr(response, 'get_json') else response}\n")
    return response


# ===============================
# 🧪 Debug endpoints
# ===============================
@app.route('/debug/test-sheets', methods=['GET'])
def debug_test_sheets():
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        test_row = ["9999999", "debug", 0, ts, "00:00–00:30"]
        sheet.append_row(test_row)
        return jsonify({"ok": True, "wrote": test_row})
    except Exception as e:
        logging.exception("❌ /debug/test-sheets failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/debug/session', methods=['GET'])
def debug_session_dump():
    """Dump in-memory session_store for quick inspection."""
    try:
        return jsonify({"ok": True, "session_store": session_store})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == '__main__':
    app.run(port=5000, debug=True)
