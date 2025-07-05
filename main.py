import os
import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import uvicorn

app = FastAPI()

# 🔐 Environment variables (make sure these are set in Render)
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
GENIE_TOKEN = os.getenv("GENIE_TOKEN")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")
GENIE_URL = "https://adb-204242957656703.3.azuredatabricks.net/ai-genie/api/chat"

# 👤 Replace with your bot's user ID from Slack logs
BOT_USER_ID = os.getenv("BOT_USER_ID", "U0947J55Y75")

@app.get("/")
def root():
    return {"status": "ok"}


def query_genie(question: str):
    headers = {
        "Authorization": f"Bearer {GENIE_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "space_id": GENIE_SPACE_ID,
        "messages": [{"role": "user", "content": question}]
    }

    try:
        response = requests.post(GENIE_URL, headers=headers, json=payload)
        response.raise_for_status()
        print("✅ Genie API Response:", response.json())
        return response.json()
    except Exception as e:
        print(f"❌ Genie API Error: {e}")
        return {"messages": [{"content": "Sorry, there was an error getting the answer from Genie."}]}


def post_to_slack(channel: str, text: str):
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "channel": channel,
        "text": text
    }

    try:
        resp = requests.post(url, headers=headers, json=payload)
        print(f"📤 Slack Post Status: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"❌ Slack Post Error: {e}")


@app.post("/slack/events")
async def slack_events(request: Request):
    data = await request.json()
    print("🔔 Received Slack Event:", data)

    # Step 1: Slack URL verification
    if data.get("type") == "url_verification":
        return PlainTextResponse(content=data["challenge"])

    # Step 2: Handle mentions
    if data.get("type") == "event_callback":
        event = data.get("event", {})
        if event.get("type") == "app_mention":
            user = event.get("user")
            channel = event.get("channel")
            text = event.get("text", "")

            # Step 3: Strip @mention to extract user question
            question = text.replace(f"<@{BOT_USER_ID}>", "").strip()
            print(f"📨 Question from {user}: {question}")

            # Step 4: Query Genie
            genie_response = query_genie(question)
            answer = genie_response.get("messages", [{}])[0].get("content", "Genie returned no response.")

            # Step 5: Respond to Slack
            post_to_slack(channel, answer)

    return PlainTextResponse("ok")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    print(f"🚀 Starting on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port)
