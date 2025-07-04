from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

app = FastAPI()

@app.post("/slack/events")
async def slack_events(request: Request):
    data = await request.json()

    # ✅ Respond to Slack's URL verification
    if data.get("type") == "url_verification":
        challenge = data.get("challenge")
        return PlainTextResponse(content=challenge)

    # Handle other event types (optional for now)
    return PlainTextResponse(content="ok")
