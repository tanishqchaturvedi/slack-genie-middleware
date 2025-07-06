import os
import time
import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import uvicorn
from collections import deque

app = FastAPI()

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_URL = os.getenv("DATABRICKS_URL")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")

# In-memory store for recent Slack event_ids to avoid duplicate processing
PROCESSED_EVENT_IDS = deque(maxlen=1000)

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/slack/events")
async def slack_events(request: Request):
    # ✅ Handle Slack retries
    if "X-Slack-Retry-Num" in request.headers:
        print("⚠️ Slack retry detected. Ignoring duplicate event.")
        return PlainTextResponse("OK", status_code=200)

    data = await request.json()
    print("🔔 Received Slack Event:", data)

    # ✅ Handle Slack URL verification challenge
    if data.get("type") == "url_verification":
        return PlainTextResponse(content=data["challenge"])

    # ✅ Deduplication using event_id
    event_id = data.get("event_id")
    if event_id in PROCESSED_EVENT_IDS:
        print(f"⚠️ Skipping duplicate event: {event_id}")
        return PlainTextResponse("ok")
    PROCESSED_EVENT_IDS.append(event_id)

    # ✅ Process the event
    if data.get("type") == "event_callback":
        event = data.get("event", {})

        # 🛑 Ignore bot messages and bot-to-bot loops
        if event.get("subtype") == "bot_message" or event.get("bot_id"):
            print(f"🛑 Ignored bot message or bot_id event_id={event_id}")
            return PlainTextResponse("ok")

        if event.get("type") in ["app_mention", "message"]:
            user_id = event.get("user")
            channel_id = event.get("channel")
            full_text = event.get("text", "")
            thread_ts = event.get("thread_ts") or event.get("ts")
            question = extract_question_from_text(full_text)

            print(f"📨 Question from {user_id}: {question}")

            try:
                # Start Genie conversation
                start_url = f"{DATABRICKS_URL}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"
                payload = {"content": question}
                res = requests.post(start_url, headers=HEADERS, json=payload)
                res.raise_for_status()

                convo = res.json()
                convo_id = convo["conversation_id"]
                msg_id = convo["message_id"]

                print("🧠 Started conversation:", convo_id, msg_id)

                answer = poll_for_answer(convo_id, msg_id, question)
                post_to_slack(channel_id, answer, thread_ts)

            except Exception as e:
                print("❌ Genie API Error:", str(e))
                post_to_slack(channel_id, f":x: Genie API Error: {str(e)}", thread_ts)

    return PlainTextResponse("ok")

def extract_question_from_text(text: str) -> str:
    return text.split(">", 1)[-1].strip() if ">" in text else text.strip()

def poll_for_answer(convo_id, msg_id, question, timeout=30):
    poll_url = f"{DATABRICKS_URL}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{convo_id}/messages/{msg_id}"

    for _ in range(timeout):
        time.sleep(1)
        res = requests.get(poll_url, headers=HEADERS)
        if res.status_code == 200:
            msg = res.json()
            if msg.get("status") == "COMPLETED":
                attachments = msg.get("attachments", [])
                if attachments:
                    attachment = attachments[0]
                    query = attachment.get("query", {}).get("query")
                    row_count = attachment.get("query", {}).get("query_result_metadata", {}).get("row_count")
                    attachment_id = attachment.get("attachment_id")

                    result_text = ""
                    if attachment_id:
                        result_url = f"{DATABRICKS_URL}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{convo_id}/messages/{msg_id}/attachments/{attachment_id}/query-result"
                        result_res = requests.get(result_url, headers=HEADERS)
                        if result_res.status_code == 200:
                            result_json = result_res.json()
                            try:
                                rows = result_json["statement_response"]["result"]["data_array"]
                                columns = result_json["statement_response"]["manifest"]["schema"]["columns"]
                                if rows and columns:
                                    headers = [col["name"] for col in columns]
                                    first_row = rows[0]
                                    result_text = "\n".join(f"*{h}:* {v}" for h, v in zip(headers, first_row))
                            except Exception as e:
                                print("⚠️ Failed parsing result:", e)

                    return (
                        f":speech_balloon: *Question:*\n{question}\n\n"
                        f":bar_chart: *SQL:*\n```sql\n{query}\n```\n\n"
                        f"🧾 *Rows Returned:* {row_count}\n\n"
                        f":page_facing_up: *Results:*\n{result_text or '_No data returned_'}"
                    )
    return ":hourglass_flowing_sand: Timed out waiting for Genie response."

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
