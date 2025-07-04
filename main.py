import os
import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Env vars
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
GENIE_DATABRICKS_TOKEN = os.getenv("GENIE_DATABRICKS_TOKEN")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")  # space ID
DATABRICKS_URL = os.getenv("DATABRICKS_URL")  # e.g., https://adb-xxx.azuredatabricks.net

@app.post("/slack/events")
async def slack_events(request: Request):
    data = await request.json()
    print("🔔 Full event received:", data)
    
    # Slack verification event
    if data.get("type") == "url_verification":
        return PlainTextResponse(content=data["challenge"])

    # Handle actual Slack message events
    if data.get("type") == "event_callback":
        event = data.get("event", {})
        if event.get("type") in ["app_mention", "message"]:
            user = event.get("user")
            text = event.get("text")
            channel = event.get("channel")
            thread_ts = event.get("thread_ts", event.get("ts"))

            print(f"📩 Slack Msg: {text} from user {user}")

            # Step 1: Call Genie
            genie_answer = call_genie(question=text)

            # Step 2: Reply back to Slack
            post_to_slack(channel, thread_ts, genie_answer)

    return PlainTextResponse(content="ok")

def call_genie(question: str):
    """
    Send user question to Genie Space API
    """
    url = f"{DATABRICKS_URL}/api/genie/v1/spaces/{GENIE_SPACE_ID}/ask"
    headers = {
        "Authorization": f"Bearer {GENIE_DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "question": question,
        "user": {
            "email": "slackbot@dream11.com"  # Replace with actual user if required
        }
    }
    print("🔁 Calling Genie with:", payload)
    try:
        response = requests.post(url, headers=headers, json=payload)
        print("📬 Genie response status:", response.status_code)
        print("📬 Genie response body:", response.text)
        if response.status_code == 200:
            data = response.json()
            answer = data.get("answer", "✅ Genie responded but no answer found.")
            sql = data.get("sql", "")
            return f"*Answer:*\n{answer}\n\n```sql\n{sql}\n```" if sql else answer
        else:
            return f"⚠️ Genie error: {response.status_code} — {response.text}"
    except Exception as e:
        return f"❌ Failed to contact Genie: {e}"

def post_to_slack(channel: str, thread_ts: str, message: str):
    """
    Posts the response back to Slack in the same thread.
    """
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "channel": channel,
        "text": message,
        "thread_ts": thread_ts
    }
    print("📤 Posting to Slack:", payload)
    response = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)
    print("📨 Slack response:", response.status_code, response.text)
    print(f"✅ Slack post status: {response.status_code}")

@app.get("/")
def health_check():
    return {"status": "✅ App running"}


if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 8000))  # Render sets this PORT
    uvicorn.run("main:app", host="0.0.0.0", port=port)
