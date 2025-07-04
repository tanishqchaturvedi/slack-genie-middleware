from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

@app.post("/slack/events")
async def slack_events(request: Request):
    data = await request.json()

    # ✅ 1. Handle Slack URL verification
    if data.get("type") == "url_verification":
        return JSONResponse(content={"challenge": data["challenge"]})

    # ✅ 2. Handle actual events like app_mention or message
    if data.get("type") == "event_callback":
        event = data.get("event", {})
        if event.get("type") in ["app_mention", "message"]:
            user = event.get("user")
            text = event.get("text")
            channel = event.get("channel")
            thread_ts = event.get("thread_ts", event.get("ts"))
            
            print("📩 New Slack Message:")
            print("User:", user)
            print("Text:", text)
            print("Channel:", channel)
            print("Thread TS:", thread_ts)

    return JSONResponse(content={"ok": True})
