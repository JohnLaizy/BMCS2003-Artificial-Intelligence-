# ===============================
# 📦 导入模块
# ===============================
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime, date, timedelta, time as dtime
from dateutil import parser
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging

# ===============================
# 📜 日志配置
# ===============================
logging.basicConfig(level=logging.INFO)

# ===============================
# 🚀 Flask 应用
# ===============================
app = Flask(__name__)
CORS(app)

# ===============================
# ⏰ 开放时间（考试期间可设置到 24:00）
# ===============================
ALLOW_UNTIL_MIDNIGHT = False

# ===============================
# 📊 Google Sheets 连接
# ===============================
scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)

SHEET_TITLE = 'library-bot-sheet'
REQUIRED_HEADERS = ["Student ID", "Category", "Size", "Date", "Time"]

# 确保表头存在
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

# 工具函数：读取和写入 Google Sheets
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
# 🧱 上下文名称定义
# ===============================
CTX_MENU = "awaiting_menu"            
CTX_BOOKING = "booking_info"          
CTX_CHECK_FLOW = "check_flow"         
CTX_READY_TO_BOOK = "ready_to_book"   
CTX_AWAIT_CONFIRM = "awaiting_confirmation"  

# 工具函数：获取、合并、构建 context
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

def _merge_ctx_params(old_params: dict, new_params: dict) -> dict:
    merged = dict(old_params or {})
    for k, v in (new_params or {}).items():
        if v not in (None, "", []):
            merged[k] = v
    return merged

def _ctx_obj(req, params: dict, ctx_name=CTX_BOOKING, lifespan=5):
    return {
        "name": f"{req['session']}/contexts/{ctx_name}",
        "lifespanCount": lifespan,
        "parameters": params
    }

# ===============================
# 🤖 回复文本（英文）
# ===============================
RESPONSE = {
    "welcome": (
        "Hi! Welcome to the Library Booking Bot.\n"
        "1️⃣ Check availability\n"
        "2️⃣ Make a booking\n"
        "3️⃣ Cancel a booking\n"
        "4️⃣ Library information\n\n"
        "👉 You can type a number or say: 'I want to book tomorrow at 2 PM'."
    ),
    "already_booked": "⚠ You already booked for that day (one per day).",
    "invalid_date": "⚠ Invalid date format: {}",
    "invalid_time": "⚠ Invalid time format. Please provide both start and end clearly.",
    "outside_hours": "⚠ Booking time must be between 8 AM and 10 PM (or until midnight during exam period).",
    "too_long": "⚠ You can only book up to 3 hours per session.",
    "missing_date_checkAvailability": "⚠ Which date do you want to check? Today or tomorrow?",
    "missing_date": "⚠ Please provide a date: today or tomorrow?",
    "missing_time": "⚠ Please provide a time range, e.g. 2 PM to 5 PM.",
    "missing_time_checkAvailability": "⚠ What time would you like to check? For example: 2 PM to 5 PM.",
    "missing_people": "How many people will be using the room?",
    "confirm": "Let me confirm: You want to book a {} room for {} people on {} from {}, correct? Say 'Yes' to confirm.",
    "confirm_success": "✅ Your booking has been saved successfully.",
    "confirm_failed": "⚠ Booking failed. Missing information.",
    "cancel": "🖑 Your booking has been cancelled.",
    "unknown": "Sorry, I didn’t understand that.",
    "cancel_confirm": "Got it. The booking has been cancelled.",
    "library_info": "Library hours: 8:00 AM – 10:00 PM daily. Solo rooms fit 1 person; discussion rooms fit 2–6 people."
}

# ===============================
# 🗓 日期解析
# ===============================
def parse_date(date_param):
    if not date_param:
        return None
    try:
        if isinstance(date_param, dict) and 'date_time' in date_param:
            dt = parser.isoparse(date_param['date_time'])
            return dt.date()
        elif isinstance(date_param, str):
            s = date_param.strip().lower()
            if s == 'today':
                return date.today()
            elif s == 'tomorrow':
                return date.today() + timedelta(days=1)
            else:
                dt = parser.isoparse(date_param)
                return dt.date()
    except Exception:
        logging.exception("Date parsing error")
        return None

# ===============================
# ⏱ 时间段解析与校验
# ===============================
def parse_and_validate_timeperiod(time_period):
    """
    返回: (ok: bool, msg: str|None, time_str: str|None, start_obj, end_obj)
      - 必须在同一天
      - 8:00 ≤ start < end ≤ 22:00（考试期 ≤ 24:00）
      - 时长 ≤ 3 小时
    """
    if not time_period or not isinstance(time_period, dict):
        return False, RESPONSE['missing_time'], None, None, None

    start_time = time_period.get('startTime')
    end_time = time_period.get('endTime')
    if not start_time or not end_time:
        return False, RESPONSE['missing_time'], None, None, None

    try:
        start_obj = parser.isoparse(start_time)
        end_obj = parser.isoparse(end_time)

        if start_obj.date() != end_obj.date():
            return False, RESPONSE['invalid_time'], None, None, None

        opening = dtime(8, 0, 0)
        closing_hour = 24 if ALLOW_UNTIL_MIDNIGHT else 22
        closing = dtime(closing_hour % 24, 0, 0)

        if not (opening <= start_obj.time() < end_obj.time() <= closing):
            return False, RESPONSE['outside_hours'], None, None, None

        duration_hours = (end_obj - start_obj).total_seconds() / 3600.0
        if duration_hours - 3.0 > 1e-6:
            return False, RESPONSE['too_long'], None, None, None

        time_str = f"{start_obj.strftime('%I:%M %p')} to {end_obj.strftime('%I:%M %p')}"
        return True, None, time_str, start_obj, end_obj

    except Exception:
        logging.exception("Time parsing failed")
        return False, RESPONSE['invalid_time'], None, None, None

# ===============================
# 🤖 Welcome + Menu
# ===============================
def handle_welcome(req):
    # 多行菜单回复
    lines = [ln for ln in RESPONSE['welcome'].split("\n") if ln.strip()]
    return jsonify({
        "fulfillmentMessages": [{"text": {"text": [ln]}} for ln in lines],
        "outputContexts": [
            {"name": f"{req['session']}/contexts/{CTX_MENU}", "lifespanCount": 5}
        ]
    })

def _menu_followup(req, event_name: str, text: str):
    return jsonify({
        "fulfillmentText": text,
        "followupEventInput": {
            "name": event_name,
            "languageCode": "en",
            "parameters": _get_ctx_params(req, CTX_BOOKING)
        }
    })

def handle_menu_check(req):
    fresh = {"booking_time": None}
    return jsonify({
        "fulfillmentText": "Entering availability check. Which date would you like to check — today or tomorrow?",
        "outputContexts": [
            _ctx_obj(req, fresh, CTX_BOOKING, lifespan=5),
            _ctx_obj(req, {}, CTX_CHECK_FLOW, lifespan=5),
            {"name": f"{req['session']}/contexts/{CTX_MENU}", "lifespanCount": 0},
            {"name": f"{req['session']}/contexts/{CTX_READY_TO_BOOK}", "lifespanCount": 0}
        ],
        "followupEventInput": {"name": "EVT_CHECK", "languageCode": "en"}
    })

def handle_menu_book(req):
    if not _has_ctx(req, CTX_READY_TO_BOOK):
        return jsonify({
            "fulfillmentText": "Let's check availability first. Which date would you like — today or tomorrow?",
            "outputContexts": [
                _ctx_obj(req, {}, CTX_CHECK_FLOW, lifespan=5)
            ]
        })
    return _menu_followup(req, "EVT_BOOK", "Proceeding to booking. Please enter your 7-digit student ID.")

def handle_menu_cancel(req):
    return _menu_followup(req, "EVT_CANCEL", "Okay, let's cancel a booking. Please provide your 7-digit student ID and the date.")

def handle_menu_info(req):
    return jsonify({"fulfillmentText": RESPONSE["library_info"]})

# ===============================
# 🔎 CheckAvailability（检查可用性）
# ===============================
def handle_check_availability(req):
    # 确保进入查询流程上下文
    if not _has_ctx(req, CTX_CHECK_FLOW):
        return jsonify({
            "fulfillmentText": "We'll check availability now. Which date would you like — today or tomorrow?",
            "outputContexts": [
                _ctx_obj(req, {}, CTX_CHECK_FLOW, lifespan=5)
            ]
        })

    parameters = req['queryResult'].get('parameters', {})
    room_category = parameters.get('room_category')
    room_size = parameters.get('room_size')
    date_param = parameters.get('date') or parameters.get('date-time')
    time_period = parameters.get('booking_time')

    # ===============================
    # 🕒 ❶ 判断用户是否明确输入了时间（没有就丢弃旧的）
    # ===============================
    user_text = (req['queryResult'].get('queryText') or '').lower()
    has_time_words = any(w in user_text for w in [' to ', '-', 'from', 'until', 'pm', 'am', ':'])
    if not has_time_words:
        time_period = None

    # ===============================
    # 🕒 ❷ 校验时间（如果输入了时间，先验证是否合法）
    # ===============================
    if time_period:
        ok, msg, _, _, _ = parse_and_validate_timeperiod(time_period)
        if not ok:
            old = _get_ctx_params(req, CTX_BOOKING)
            merged = _merge_ctx_params(old, {
                "roomCategory": room_category,
                "roomSize": room_size,
                "date": old.get("date"),   # ⚠ 保留已有日期
                "booking_time": None       # ❌ 清掉错误时间
            })
            return jsonify({
                "fulfillmentText": f"{msg} Please enter a new time period within opening hours, max 3 hours (e.g. 2 PM to 5 PM).",
                "outputContexts": [
                    _ctx_obj(req, merged, CTX_BOOKING, lifespan=5),
                    _ctx_obj(req, {}, CTX_CHECK_FLOW, lifespan=5)
                ]
            })

    # ===============================
    # 📅 ❸ 校验日期（新增逻辑：优先复用 context 里的日期）
    # ===============================
    old = _get_ctx_params(req, CTX_BOOKING)
    date_obj = parse_date(date_param)

    if not date_obj and old.get("date"):
        # ✅ 如果 context 里已经存了日期，就直接复用，不要再问
        date_obj = parse_date(old.get("date"))

    if not date_obj:
        # ❌ 如果用户和 context 都没有日期 → 必须追问
        merged = _merge_ctx_params(old, {
            "roomCategory": room_category,
            "roomSize": room_size,
            "booking_time": time_period   # ⚠ 保留已通过校验的时间，避免重复输入
        })
        return jsonify({
            "fulfillmentText": RESPONSE['missing_date_checkAvailability'],
            "outputContexts": [
                _ctx_obj(req, merged, CTX_BOOKING, lifespan=5),
                _ctx_obj(req, {}, CTX_CHECK_FLOW, lifespan=5)
            ]
        })

    date_str = date_obj.strftime("%d/%m/%Y")

    # ===============================
    # 👥 ❹ 人数缺失 → 追问
    # ===============================
    if not room_size:
        merged = _merge_ctx_params(old, {
            "roomCategory": room_category,
            "date": date_str,
            "booking_time": time_period
        })
        return jsonify({
            "fulfillmentText": RESPONSE['missing_people'],
            "outputContexts": [
                _ctx_obj(req, merged, CTX_BOOKING, lifespan=5),
                _ctx_obj(req, {}, CTX_CHECK_FLOW, lifespan=5)
            ]
        })

    # ===============================
    # ✅ ❺ 信息齐全 → 下发 ready_to_book
    # ===============================
    ok, msg, time_str, _, _ = parse_and_validate_timeperiod(time_period)
    merged = _merge_ctx_params(old, {
        "roomCategory": room_category,
        "roomSize": room_size,
        "date": date_str,
        "booking_time": time_period
    })
    return jsonify({
        "fulfillmentText": f"Great. I have a {room_category} room for {room_size} people on {date_str} from {time_str}. Say 'Book' to proceed.",
        "outputContexts": [
            _ctx_obj(req, merged, CTX_BOOKING, lifespan=10),
            _ctx_obj(req, {}, CTX_READY_TO_BOOK, lifespan=3)
        ]
    })

# ===============================
# 🏷 book_room
# ===============================
def handle_book_room(req):
    if not _has_ctx(req, CTX_READY_TO_BOOK):
        return jsonify({
            "fulfillmentText": "We need to confirm date, time and number of people first. Which date would you like — today or tomorrow?",
            "outputContexts": [
                _ctx_obj(req, {}, CTX_CHECK_FLOW, lifespan=5)
            ]
        })

    parameters = req['queryResult'].get('parameters', {})
    student_id = parameters.get('student_id')
    room_category = parameters.get('roomCategory') or parameters.get('room_category')
    room_size = parameters.get('roomSize') or parameters.get('room_size')
    date_param = parameters.get('date') or parameters.get('date-time')
    time_period = parameters.get('booking_time')

    # 从 context 补全
    for context in req['queryResult'].get('outputContexts', []):
        if CTX_BOOKING in context['name']:
            ctx_params = context.get('parameters', {})
            student_id = student_id or ctx_params.get('student_id')
            room_category = room_category or ctx_params.get('roomCategory') or ctx_params.get('room_category')
            room_size = room_size or ctx_params.get('roomSize') or ctx_params.get('room_size')
            date_param = date_param or ctx_params.get('date')
            time_period = time_period or ctx_params.get('booking_time')

    # 校验学号
    if not student_id or not str(student_id).isdigit() or len(str(student_id)) != 7:
        return jsonify({"fulfillmentText": "⚠ Invalid student ID. It must be a 7-digit number."})

    # 校验日期
    date_obj = parse_date(date_param)
    if not date_obj:
        return jsonify({"fulfillmentText": RESPONSE['missing_date']})
    today = date.today()
    tomorrow = today + timedelta(days=1)
    if date_obj not in (today, tomorrow):
        return jsonify({"fulfillmentText": "⚠ You can only book for today or tomorrow."})
    date_str = date_obj.strftime("%d/%m/%Y")

    # 校验时间
    ok, msg, time_str, _, _ = parse_and_validate_timeperiod(time_period)
    if not ok:
        return jsonify({"fulfillmentText": msg})

    # 人数/房型联动
    try:
        people = int(room_size) if room_size is not None else None
    except Exception:
        return jsonify({"fulfillmentText": "⚠ Please provide a valid number of people."})
    if people == 1 and not room_category:
        room_category = 'solo'
    elif people is not None and people >= 2 and not room_category:
        room_category = 'discussion'
    if room_category == 'solo' and people is None:
        people = 1

    # 检查是否已预约
    for row in get_all_bookings():
        if str(row.get('Student ID')) == str(student_id) and row.get('Date') == date_str:
            return jsonify({"fulfillmentText": RESPONSE['already_booked']})

    # 返回确认
    merged = _merge_ctx_params(_get_ctx_params(req, CTX_BOOKING), {
        "student_id": student_id,
        "roomCategory": room_category,
        "roomSize": people,
        "date": date_str,
        "time": time_str
    })
    return jsonify({
        "fulfillmentText": RESPONSE['confirm'].format(room_category, people, date_str, time_str),
        "outputContexts": [
            _ctx_obj(req, merged, CTX_BOOKING, lifespan=10),
            {"name": f"{req['session']}/contexts/{CTX_AWAIT_CONFIRM}", "lifespanCount": 5}
        ]
    })

# ===============================
# ✅ ConfirmBooking
# ===============================
def handle_confirm_booking(req):
    def clean(val):
        return val[0] if isinstance(val, list) else val

    student_id = room_category = room_size = date_str = time_str = None
    for context in req['queryResult'].get('outputContexts', []):
        if CTX_BOOKING in context['name']:
            params = context.get('parameters', {})
            student_id = clean(params.get('student_id'))
            room_category = clean(params.get('roomCategory'))
            room_size = clean(params.get('roomSize'))
            date_str = clean(params.get('date'))
            time_str = clean(params.get('time'))
            break

    if not student_id:
        return jsonify({
            "fulfillmentText": "Please enter your 7-digit student ID.",
            "outputContexts": [
                _ctx_obj(req, _get_ctx_params(req, CTX_BOOKING), CTX_BOOKING, lifespan=5),
                {"name": f"{req['session']}/contexts/{CTX_AWAIT_CONFIRM}", "lifespanCount": 5}
            ]
        })

    if not str(student_id).isdigit() or len(str(student_id)) != 7:
        return jsonify({
            "fulfillmentText": "⚠ Invalid student ID. It must be a 7-digit number.",
            "outputContexts": [
                _ctx_obj(req, _get_ctx_params(req, CTX_BOOKING), CTX_BOOKING, lifespan=5),
                {"name": f"{req['session']}/contexts/{CTX_AWAIT_CONFIRM}", "lifespanCount": 5}
            ]
        })

    if all([student_id, room_category, room_size, date_str, time_str]):
        ok = append_booking(student_id, room_category, room_size, date_str, time_str)
        if ok:
            return jsonify({"fulfillmentText": RESPONSE['confirm_success']})
        else:
            return jsonify({"fulfillmentText": "⚠ I couldn't save your booking. Please try again later or contact staff."})
    else:
        return jsonify({"fulfillmentText": RESPONSE['confirm_failed']})

# ===============================
# ❌ Cancel
# ===============================
def handle_cancel_booking(req):
    return jsonify({"fulfillmentText": RESPONSE['cancel']})

def handle_cancel_after_confirmation(req):
    return jsonify({"fulfillmentText": RESPONSE['cancel_confirm']})

def handle_library_info(req):
    return jsonify({"fulfillmentText": RESPONSE["library_info"]})

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

    'CheckAvailability': handle_check_availability,
    'book_room': handle_book_room,
    'ConfirmBooking': handle_confirm_booking,
    'CancelBooking': handle_cancel_booking,
    'CancelAfterConfirmation': handle_cancel_after_confirmation,
    'LibraryInfo': handle_library_info
}

# ===============================
# 🌐 Webhook 入口
# ===============================
@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json()
    intent = req['queryResult']['intent']['displayName']
    logging.info(f"Incoming intent: {intent}, parameters: {req['queryResult'].get('parameters')}")
    handler = INTENT_HANDLERS.get(intent, handle_default)
    return handler(req)

# ===============================
# 🧪 调试端点
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

if __name__ == '__main__':
    app.run(port=5000, debug=True)
