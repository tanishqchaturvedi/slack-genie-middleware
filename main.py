import os
import time
import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import uvicorn

app = FastAPI()

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_URL = os.getenv("DATABRICKS_URL")  # e.g., https://adb-xxxx.azuredatabricks.net
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

    if data.get("type") == "url_verification":
        return PlainTextResponse(content=data["challenge"])

    if data.get("type") == "event_callback":
        event = data.get("event", {})
        if event.get("type") == "app_mention":
            user_id = event.get("user")
            channel_id = event.get("channel")
            thread_ts = event.get("ts")
            full_text = event.get("text", "")
            question = extract_question_from_text(full_text)

            print(f"📨 Question from {user_id}: {question}")

            try:
                convo_id, msg_id = start_conversation(question)
                final_sql, final_result = poll_for_answer(convo_id, msg_id)
                reply = f":speech_balloon: *SQL:*\n```{final_sql}```\n*Result:* {final_result}"
                post_to_slack(channel_id, reply, thread_ts)

            except Exception as e:
                print("❌ Genie API Error:", str(e))
                post_to_slack(channel_id, f":x: Genie API Error: {str(e)}", thread_ts)

    return PlainTextResponse("ok")


def extract_question_from_text(text):
    parts = text.strip().split(' ', 1)
    return parts[1] if len(parts) > 1 else text


def start_conversation(question):
    url = f"{DATABRICKS_URL}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"
    payload = {"content": question}

    res = requests.post(url, headers=HEADERS, json=payload)
    res.raise_for_status()

    data = res.json()["message"]
    convo_id = data["conversation_id"]
    msg_id = data["id"]

    print("🧠 Started conversation:", convo_id, msg_id)
    return convo_id, msg_id


def poll_for_answer(convo_id, msg_id, timeout=30):
    url = f"{DATABRICKS_URL}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{convo_id}/messages/{msg_id}"

    for _ in range(timeout):
        time.sleep(1)
        res = requests.get(url, headers=HEADERS)
        if res.status_code == 200:
            message = res.json()
            if message.get("status") == "COMPLETED":
                sql, rows = extract_sql_and_rows(message)
                return sql, rows

    return "[timeout]", "[No result]"


def extract_sql_and_rows(message):
    attachments = message.get("attachments", [])
    if not attachments:
        return "[No SQL]", "[No attachments]"

    attachment = attachments[0]
    query = attachment.get("query", {}).get("query", "[No SQL]")
    attachment_id = attachment.get("attachment_id")

    if not attachment_id:
        return query, "[No attachment_id]"

    result_url = (
        f"{DATABRICKS_URL}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/"
        f"conversations/{message['conversation_id']}/messages/{message['message_id']}/query-result/{attachment_id}"
    )
    res = requests.get(result_url, headers=HEADERS)
    res.raise_for_status()
    result_json = res.json()

    rows = result_json.get("statement_response", {}).get("result", {}).get("data_array", [])
    if rows:
        formatted = "\n".join(["\t".join(map(str, row)) for row in rows[:5]])
        return query, f"```\n{formatted}\n```"
    return query, "_No rows returned._"


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
