import os
import time
import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_URL = os.getenv("DATABRICKS_URL")  # e.g., https://adb-xxx.azuredatabricks.net
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

app = FastAPI()


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

    # Handle app_mention event
    if data.get("type") == "event_callback":
        event = data.get("event", {})
        if event.get("type") == "app_mention":
            user = event.get("user")
            text = event.get("text")
            channel = event.get("channel")

            question = text.split('>', 1)[-1].strip()  # Remove bot mention
            print(f"📨 Question from {user}: {question}")

            # Step 1: Start Genie conversation
            start_url = f"{DATABRICKS_URL}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"
            response = requests.post(start_url, headers=headers, json={"content": question})
            if response.status_code != 200:
                post_to_slack(channel, f"❌ Failed to start Genie conversation: {response.text}")
                return PlainTextResponse("ok")

            convo_id = response.json()["conversation"]["id"]
            msg_id = response.json()["message"]["id"]

            # Step 2: Poll until Genie responds
            message_content = None
            for _ in range(10):  # try for 10 times
                time.sleep(2)
                poll_url = f"{DATABRICKS_URL}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{convo_id}/messages/{msg_id}"
                poll_response = requests.get(poll_url, headers=headers)
                if poll_response.status_code != 200:
                    continue

                status = poll_response.json().get("status")
                if status == "COMPLETED":
                    message_content = poll_response.json().get("content", "")
                    break

            if not message_content:
                message_content = "⚠️ Genie did not respond in time."

            # Step 3: Post back to Slack
            post_to_slack(channel, f"💬 *Answer:* {message_content}")

    return PlainTextResponse("ok")


def post_to_slack(channel, message):
    slack_url = "https://slack.com/api/chat.postMessage"
    slack_headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    slack_data = {
        "channel": channel,
        "text": message
    }

    response = requests.post(slack_url, headers=slack_headers, json=slack_data)
    print("📤 Slack Post Status:", response.status_code, "-", response.text)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "3000"))
    print(f"🚀 Starting on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port)
