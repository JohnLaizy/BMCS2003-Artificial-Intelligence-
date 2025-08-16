from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dateutil.parser import parse

app = Flask(__name__)
CORS(app)

# Setup Google Sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)
sheet = client.open('library-bot-sheet').sheet1 # Open the Google Sheet by name

# Webhook entry point
@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json()
    intent = req['queryResult']['intent']['displayName']

    if intent == 'Welcome':
        return jsonify({"fulfillmentText": "Hi! Welcome to the Library Booking Bot."})

    elif intent == 'CheckAvailability':
        room_category = req['queryResult']['parameters'].get('room_category')
        room_size = req['queryResult']['parameters'].get('room_size')
        date = req['queryResult']['parameters'].get('date')

        try:
            # Parse ISO string and extract only the date part
            full_datetime = parse(date_param['date_time'])  # Requires dateutil
            date_str = full_datetime.date().strftime("%d/%m/%Y")  # Only date
        except:
            date_str = "unknown date"

        return jsonify({
            "fulfillmentText": f"Let me check availability for a {room_category} room for {room_size} people on {date_str}."
        })

    elif intent == 'book_room':
        student_id = req['queryResult']['parameters'].get('student_id')
        room_category = req['queryResult']['parameters'].get('roomCategory')
        room_size = req['queryResult']['parameters'].get('roomSize')
        date_param = req['queryResult']['parameters'].get('date')
        time_period = req['queryResult']['parameters'].get('booking_time')

        try:
            date_obj = datetime.fromisoformat(date_param['date_time'])
            date_str = date_obj.strftime("%d/%m/%Y")
        except:
            date_str = "unknown date"

        try:
            start_time = time_period.get('startTime')
            end_time = time_period.get('endTime')
            start = datetime.fromisoformat(start_time).strftime("%I:%M %p") if start_time else "?"
            end = datetime.fromisoformat(end_time).strftime("%I:%M %p") if end_time else "?"
            time_str = f"{start} to {end}"
        except:
            time_str = "unknown time"
        
        # Save values in output context for ConfirmBooking
        return jsonify({
            "fulfillmentText": f"Let me confirm: You want to book a {room_category} room for {room_size} people on {date_str} from {time_str}, right? Please say 'Yes' to confirm or 'No' to cancel.",
            "outputContexts": [
                {
                    "name": f"{req['session']}/contexts/booking_info",
                    "lifespanCount": 5,
                    "parameters": {
                        "student_id": student_id,
                        "roomCategory": room_category,
                        "roomSize": room_size,
                        "date": date_str,
                        "time": time_str
                    }
                }
            ]
        })

    elif intent == 'ConfirmBooking':# Clean each parameter to make sure it's a string (not a list)
        def clean(val):
            return val[0] if isinstance(val, list) else val    
        # Get info from output context
        student_id = room_category = room_size = date = time_str = None
        for context in req['queryResult']['outputContexts']:
            if 'booking_info' in context['name']:
                params = context['parameters']
                student_id = clean(params.get('student_id'))
                room_category = clean(params.get('roomCategory'))
                room_size = clean(params.get('roomSize'))
                date = clean(params.get('date'))
                time_str = clean(params.get('time'))
                break

        # Save to Google Sheet
        if all([student_id, room_category, room_size, date, time_str]):
            sheet.append_row([student_id, room_category, room_size, date, time_str])
            return jsonify({"fulfillmentText": "✅ Your booking has been saved successfully."})
        else:
            return jsonify({"fulfillmentText": "⚠️ Something went wrong. I couldn't save your booking."})

    elif intent == 'CancelBooking':
        return jsonify({"fulfillmentText": "Your booking has been cancelled."})

    else:
        return jsonify({"fulfillmentText": "Sorry, I didn’t understand that."})

if __name__ == '__main__':
    app.run(port=5000, debug=True)
