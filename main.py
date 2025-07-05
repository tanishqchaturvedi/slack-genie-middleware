import os
import time
import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import uvicorn

app = FastAPI()

# ✅ Load environment variables
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_URL = os.getenv("DATABRICKS_URL")  # e.g. https://adb-xxx.azuredatabricks.net
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")

# Genie API URL
GENIE_API_URL = f"{DATABRICKS_URL}/api/genie/v1/spaces/{GENIE_SPACE_ID}"

# Headers
SLACK_HEADERS = {
    "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
    "Content-type": "application/json"
}
GENIE_HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/slack/events")
async def slack_events(request: Request):
    data = await request.json()
    print("🔔 Received Slack Event:", data)

    # Slack URL verification challenge
    if data.get("type") == "url_verification":
        return PlainTextResponse(content=data["challenge"])

    if data.get("type") == "event_callback":
        event = data["event"]
        if "bot_id" in event:
            return PlainTextResponse("ignore bot events")

        user = event.get("user")
        channel = event.get("channel")
        text = event.get("text", "")

        # Remove bot mention
        question = " ".join(w for w in text.split() if not w.startswith("<@"))

        try:
            # Start Genie conversation
            start_payload = {"messages": [{"role": "user", "content": question}]}
            start_response = requests.post(
                f"{GENIE_API_URL}/conversations/start",
                headers=GENIE_HEADERS,
                json=start_payload
            )
            start_response.raise_for_status()
            convo_id = start_response.json()["conversation_id"]

            # Poll for assistant's response
            answer = "❌ No response from Genie."
            for _ in range(10):
                time.sleep(2)
                messages_resp = requests.get(
                    f"{GENIE_API_URL}/conversations/{convo_id}/messages",
                    headers=GENIE_HEADERS
                )
                messages_resp.raise_for_status()
                messages = messages_resp.json().get("messages", [])
                completed = [
                    m for m in messages if m["role"] == "assistant" and m.get("status") == "COMPLETED"
                ]
                if completed:
                    answer = completed[0]["content"]
                    break

            # Send answer to Slack
            slack_payload = {
                "channel": channel,
                "text": f":speech_balloon: *Answer:* {answer}"
            }
            slack_resp = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers=SLACK_HEADERS,
                json=slack_payload
            )
            print("📤 Slack Post Status:", slack_resp.status_code, "-", slack_resp.text)

        except Exception as e:
            print("❌ Genie API Error:", str(e))
            error_payload = {
                "channel": channel,
                "text": f":x: Genie API Error: {str(e)}"
            }
            requests.post(
                "https://slack.com/api/chat.postMessage",
                headers=SLACK_HEADERS,
                json=error_payload
            )

    return PlainTextResponse("ok")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    print(f"🚀 Starting on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port)
