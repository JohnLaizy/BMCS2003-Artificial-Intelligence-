# BMCS2003-Artificial-Intelligence-
ai chatbot code

# dialogflow link
https://dialogflow.cloud.google.com/#/editAgent/library-bot-ff9l/

# google sheet link
https://docs.google.com/spreadsheets/d/1igT4vrPlRggAv1PVqzYDtlAIEyWTsjOay4eBDh8NPlE/edit?gid=0#gid=0

# Install requirements
pip install -r requirements.txt

# Run code
$env:GOOGLE_SA_KEYFILE = "./credentials.json"
$env:SPREADSHEET_ID    = "1igT4vrPlRggAv1PVqzYDtlAIEyWTsjOay4eBDh8NPlE"
$env:PORT              = "5000"
python ai.py