import os
import time
import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import uvicorn

app = FastAPI()

# 🔐 Load config from env variables
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_URL = os.getenv("DATABRICKS_URL")  # e.g., https://adb-xxx.azuredatabricks.net
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")

HEADERS = {
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

    # ✅ Slack challenge verification
    if data.get("type") == "url_verification":
        return PlainTextResponse(content=data["challenge"])

    # ✅ Handle actual messages
    if data.get("type") == "event_callback":
        event = data.get("event", {})

        # 🚫 Prevent echo loops: ignore bot messages
        if event.get("subtype") == "bot_message" or "bot_id" in event:
            return PlainTextResponse("Ignored bot message")

        if event.get("type") == "app_mention":
            user_id = event.get("user")
            channel_id = event.get("channel")
            thread_ts = event.get("ts")
            full_text = event.get("text", "")
            question = extract_question_from_text(full_text)

            print(f"📨 Question from {user_id}: {question}")

            # 🔁 Step 1: Call Genie API
            start_url = f"{DATABRICKS_URL}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"
            payload = {
                "content": question
            }

            try:
                res = requests.post(start_url, headers=HEADERS, json=payload)
                res.raise_for_status()
                convo = res.json()
                convo_id = convo["conversation_id"]
                msg_id = convo["message_id"]
                print("🧠 Started conversation:", convo_id, msg_id)

                # 🔁 Step 2: Poll for final response
                final_answer = poll_for_answer(convo_id, msg_id)
                post_to_slack(channel_id, final_answer, thread_ts=thread_ts)

            except requests.exceptions.HTTPError as e:
                print("❌ Genie API Error:", e)
                post_to_slack(channel_id, f":x: Genie API Error: {str(e)}", thread_ts=thread_ts)

    return PlainTextResponse("ok")


def extract_question_from_text(text):
    """Removes the <@BOT_ID> mention and returns user question"""
    parts = text.strip().split(' ', 1)
    return parts[1] if len(parts) > 1 else text


def poll_for_answer(convo_id, msg_id, timeout=15):
    poll_url = f"{DATABRICKS_URL}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{convo_id}/messages/{msg_id}"

    for _ in range(timeout):
        time.sleep(1)
        res = requests.get(poll_url, headers=HEADERS)
        if res.status_code == 200:
            message = res.json()

            if message.get("status") == "COMPLETED":
                # 📄 Case 1: Natural language response
                if message.get("content"):
                    return f":speech_balloon: *Answer:*\n{message['content']}"

                # 📊 Case 2: SQL + attachment result
                attachments = message.get("attachments", [])
                if attachments:
                    query = attachments[0].get("query", {}).get("query", "[No SQL]")
                    return f":speech_balloon: *SQL:*\n```{query}```\n*Result:* _Query ran successfully_"

                return ":speech_balloon: *Answer:* _No useful response_"

    return ":hourglass_flowing_sand: Timeout waiting for Genie response."


def post_to_slack(channel, text, thread_ts=None):
    slack_url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "channel": channel,
        "text": text
    }
    if thread_ts:
        payload["thread_ts"] = thread_ts

    response = requests.post(slack_url, headers=headers, json=payload)
    print("📤 Slack Post Status:", response.status_code, "-", response.text)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    print(f"🚀 Starting on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port)
