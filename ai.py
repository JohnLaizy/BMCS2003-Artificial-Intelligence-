# ===============================
# 📦 导入模块：Flask + Google Sheets + 时间解析
# ===============================
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime, date, timedelta
from dateutil import parser
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging

# ===============================
# 📜 日志设置
# ===============================
logging.basicConfig(level=logging.INFO)

# ===============================
# 🚀 初始化 Flask 应用
# ===============================
app = Flask(__name__)
CORS(app)

# ===============================
# ⏰ 设置允许时间（考试期间延长时段）
# ===============================
ALLOW_UNTIL_MIDNIGHT = False

# ===============================
# 📊 连接 Google Sheets
# ===============================
# ⚠️ 使用推荐的新版 scope，避免权限问题
scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)
try:
    sheet = client.open('library-bot-sheet').sheet1
    logging.info("✅ Connected to Google Sheets: library-bot-sheet / sheet1")
except Exception as e:
    logging.exception("❌ Failed to open Google Sheet. Check title/share/permissions.")
    raise

# ===============================
# 📁 工具函数：表格操作
# ===============================
def get_all_bookings():
    return sheet.get_all_records()

def append_booking(student_id, category, size, date_str, time_str):
    try:
        row = [student_id, category, size, date_str, time_str]
        sheet.append_row(row)
        logging.info(f"📝 Appended row: {row}")
        return True
    except Exception as e:
        logging.exception("❌ append_booking failed")
        return False
    
    
# ===============================
# 🧰 Context 工具：读取/合并/写回 booking_info
# ===============================
def _get_ctx_params(req, ctx_name='booking_info'):
    for c in req['queryResult'].get('outputContexts', []):
        if ctx_name in c.get('name', ''):
            return c.get('parameters', {}) or {}
    return {}

def _merge_ctx_params(old_params: dict, new_params: dict) -> dict:
    merged = dict(old_params or {})
    for k, v in (new_params or {}).items():
        if v not in (None, "", []):  # 仅在新值有效时覆盖
            merged[k] = v
    return merged

def _ctx_obj(req, params: dict, ctx_name='booking_info', lifespan=5):
    return {
        "name": f"{req['session']}/contexts/{ctx_name}",
        "lifespanCount": lifespan,
        "parameters": params
    }

# ===============================
# 🚣 回应文本集中管理
# ===============================
RESPONSE = {
    "welcome": "Hi! Welcome to the Library Booking Bot.",
    "already_booked": "⚠ You have already booked a room for that day. One booking per day is allowed.",
    "invalid_date": "⚠ Invalid date format: {}",
    "invalid_time": "⚠ Invalid time format. Please enter both start and end time clearly.",
    "outside_hours": "⚠ Booking time must be between 8 AM and 10 PM (or 12 AM during exam period).",
    "too_long": "⚠ You can only book up to 3 hours per session.",
    "missing_date": "⚠ Please tell me which date you want to book. Today or tomorrow?",
    "missing_time": "⚠ What time would you like to book? (e.g. 2 PM to 5 PM)",
    "missing_people": "How many people will be using the room?",
    "confirm": "Let me confirm: You want to book a {} room for {} people on {} from {}, right? Please say 'Yes' to confirm.",
    "confirm_success": "✅ Your booking has been saved successfully.",
    "confirm_failed": "⚠ Booking failed. Missing information.",
    "cancel": "🖑 Your booking has been cancelled.",
    "unknown": "Sorry, I didn’t understand that.",
    "cancel_confirm": "Got it. The booking has been cancelled. If you'd like to book again, just let me know!"
}

# ===============================
# 🗓 分析日期字段（统一返回 date 对象）
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
    except Exception as e:
        logging.exception("Date parsing error")
        return None

# ===============================
# ⏱️ 工具函数：解析与校验时间段
# ===============================
def parse_and_validate_timeperiod(time_period):
    """
    返回 (ok: bool, message: str|None, time_str: str|None)
    - 校验 8:00-22:00（或 24:00），最长 3 小时
    - 成功则返回 12 小时制 time_str
    """
    if not time_period or not isinstance(time_period, dict):
        return False, RESPONSE['missing_time'], None
    start_time = time_period.get('startTime')
    end_time = time_period.get('endTime')
    if not start_time or not end_time:
        return False, RESPONSE['missing_time'], None
    try:
        start_obj = parser.isoparse(start_time)
        end_obj = parser.isoparse(end_time)

        opening_time = 8
        closing_time = 24 if ALLOW_UNTIL_MIDNIGHT else 22
        if not (opening_time <= start_obj.hour < closing_time and opening_time < end_obj.hour <= closing_time):
            return False, RESPONSE['outside_hours'], None

        duration = (end_obj - start_obj).total_seconds() / 3600
        if duration > 3:
            return False, RESPONSE['too_long'], None

        time_str = f"{start_obj.strftime('%I:%M %p')} to {end_obj.strftime('%I:%M %p')}"
        return True, None, time_str
    except Exception:
        logging.exception("Time parsing failed")
        return False, RESPONSE['invalid_time'], None

# ===============================
# 🤖 意图处理函数
# ===============================
def handle_welcome(req):
    return jsonify({"fulfillmentText": RESPONSE['welcome']})

def handle_check_availability(req):
    parameters = req['queryResult'].get('parameters', {})
    room_category = parameters.get('room_category')
    room_size = parameters.get('room_size')
    date_param = parameters.get('date') or parameters.get('date-time')
    time_period = parameters.get('booking_time')

    # 👉 解析日期
    date_obj = parse_date(date_param)
    if not date_obj:
        return jsonify({"fulfillmentText": RESPONSE['missing_date']})
    date_str = date_obj.strftime("%d/%m/%Y")

    # ✅ 缺时间 → 追问时间并保留上下文（合并旧值，防丢参）
    if not time_period:
        old = _get_ctx_params(req, 'booking_info')
        merged = _merge_ctx_params(old, {
            "roomCategory": room_category,
            "roomSize": room_size,
            "date": date_str
        })
        return jsonify({
            "fulfillmentText": f"Great. For {date_str}, {RESPONSE['missing_time']}",
            "outputContexts": [_ctx_obj(req, merged, 'booking_info', lifespan=5)]
        })

    # ✅ 已给时间但没给人数 → 先做时长校验，再追问人数，并保留时间到 context
    ok, msg, time_str = parse_and_validate_timeperiod(time_period)
    if not ok:
        return jsonify({"fulfillmentText": msg})
    if not room_size:
        old = _get_ctx_params(req, 'booking_info')
        merged = _merge_ctx_params(old, {
            "roomCategory": room_category,
            "date": date_str,
            "booking_time": time_period
        })
        return jsonify({
            "fulfillmentText": RESPONSE['missing_people'],
            "outputContexts": [_ctx_obj(req, merged, 'booking_info', lifespan=5)]
        })

    # 🔄 都齐了就正常回应（也把信息写回 context）
    old = _get_ctx_params(req, 'booking_info')
    merged = _merge_ctx_params(old, {
        "roomCategory": room_category,
        "roomSize": room_size,
        "date": date_str,
        "booking_time": time_period
    })
    return jsonify({
        "fulfillmentText": f"Let me check availability for a {room_category} room for {room_size} people on {date_str} from {time_str}. Yes to Confirm, No to Cancel.",
        "outputContexts": [_ctx_obj(req, merged, 'booking_info', lifespan=5)]
    })

def handle_book_room(req):
    parameters = req['queryResult'].get('parameters', {})
    student_id = parameters.get('student_id')
    room_category = parameters.get('roomCategory') or parameters.get('room_category')
    room_size = parameters.get('roomSize') or parameters.get('room_size')
    date_param = parameters.get('date') or parameters.get('date-time')
    time_period = parameters.get('booking_time')

    # 👀 从 context 补全遗漏参数
    for context in req['queryResult'].get('outputContexts', []):
        if 'booking_info' in context['name']:
            ctx_params = context.get('parameters', {})
            student_id = student_id or ctx_params.get('student_id')
            room_category = room_category or ctx_params.get('roomCategory') or ctx_params.get('room_category')
            room_size = room_size or ctx_params.get('roomSize') or ctx_params.get('room_size')
            date_param = date_param or ctx_params.get('date')
            time_period = time_period or ctx_params.get('booking_time')

    # 🆔 学号格式校验
    if not student_id or not str(student_id).isdigit() or len(str(student_id)) != 7:
        return jsonify({"fulfillmentText": "⚠ Invalid student ID format. Must be 7-digit number."})

    # 📅 日期校验（只允许今天/明天）
    date_obj = parse_date(date_param)
    if not date_obj:
        return jsonify({"fulfillmentText": RESPONSE['missing_date']})
    today = date.today()
    tomorrow = today + timedelta(days=1)
    if date_obj not in (today, tomorrow):
        return jsonify({"fulfillmentText": "⚠ You can only book for today or tomorrow."})
    date_str = date_obj.strftime("%d/%m/%Y")

    # ⏰ 时间必填 + 校验
    ok, msg, time_str = parse_and_validate_timeperiod(time_period)
    if not ok:
        return jsonify({"fulfillmentText": msg})

    # 👤 人数/房型联动（纯数字）
    people = None
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

    # ✅ 检查是否已预约该日
    for row in get_all_bookings():
        if str(row.get('Student ID')) == str(student_id) and row.get('Date') == date_str:
            return jsonify({"fulfillmentText": RESPONSE['already_booked']})

    # ✅ 输出确认 + 设置 context（附带 awaiting_confirmation，便于“no/yes”意图触发）
    old = _get_ctx_params(req, 'booking_info')
    merged = _merge_ctx_params(old, {
        "student_id": student_id,
        "roomCategory": room_category,
        "roomSize": people,
        "date": date_str,
        "time": time_str
    })
    return jsonify({
        "fulfillmentText": RESPONSE['confirm'].format(room_category, people, date_str, time_str),
        "outputContexts": [
            _ctx_obj(req, merged, 'booking_info', lifespan=5),
            {"name": f"{req['session']}/contexts/awaiting_confirmation", "lifespanCount": 5}
        ]
    })

def handle_confirm_booking(req):
    def clean(val):
        return val[0] if isinstance(val, list) else val

    student_id = room_category = room_size = date_str = time_str = None
    for context in req['queryResult'].get('outputContexts', []):
        if 'booking_info' in context['name']:
            params = context.get('parameters', {})
            student_id = clean(params.get('student_id'))
            room_category = clean(params.get('roomCategory'))
            room_size = clean(params.get('roomSize'))
            date_str = clean(params.get('date'))
            time_str = clean(params.get('time'))
            break

    # 学号缺失时由 webhook 追问（避免 DF slot-filling 抢问）
    if not student_id:
        old = _get_ctx_params(req, 'booking_info')
        return jsonify({
            "fulfillmentText": "Please enter your 7-digit student ID to complete the booking.",
            "outputContexts": [
                _ctx_obj(req, old, 'booking_info', lifespan=5),
                {"name": f"{req['session']}/contexts/awaiting_confirmation", "lifespanCount": 5}
            ]
        })

    # 学号格式再校验一层
    if not str(student_id).isdigit() or len(str(student_id)) != 7:
        old = _get_ctx_params(req, 'booking_info')
        return jsonify({
            "fulfillmentText": "⚠ Invalid student ID format. Must be 7-digit number.",
            "outputContexts": [
                _ctx_obj(req, old, 'booking_info', lifespan=5),
                {"name": f"{req['session']}/contexts/awaiting_confirmation", "lifespanCount": 5}
            ]
        })

    if all([student_id, room_category, room_size, date_str, time_str]):
        ok = append_booking(student_id, room_category, room_size, date_str, time_str)
        if ok:
            return jsonify({"fulfillmentText": RESPONSE['confirm_success']})
        else:
            return jsonify({"fulfillmentText": "⚠ I couldn't save your booking to the sheet. Please try again later or contact staff."})
    else:
        return jsonify({"fulfillmentText": RESPONSE['confirm_failed']})

def handle_cancel_booking(req):
    return jsonify({"fulfillmentText": RESPONSE['cancel']})

def handle_cancel_after_confirmation(req):
    # “No / cancel” 在确认阶段触发此意图
    return jsonify({"fulfillmentText": RESPONSE['cancel_confirm']})

def handle_default(req):
    return jsonify({"fulfillmentText": RESPONSE['unknown']})

# ===============================
# 🧠 意图对应表
# ===============================
INTENT_HANDLERS = {
    'Welcome': handle_welcome,
    'CheckAvailability': handle_check_availability,
    'book_room': handle_book_room,
    'ConfirmBooking': handle_confirm_booking,
    'CancelBooking': handle_cancel_booking,
    'CancelAfterConfirmation': handle_cancel_after_confirmation,
    'RejectConfirmation': handle_cancel_after_confirmation  # 提供为 "No" 的捕捉
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
# ▶️ 本地启动
# ===============================
if __name__ == '__main__':
    app.run(port=5000, debug=True)
