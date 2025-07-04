from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

@app.post("/slack/events")
async def slack_events(request: Request):
    data = await request.json()

    # ✅ Correct: respond to Slack's URL verification
    if data.get("type") == "url_verification":
        challenge = data.get("challenge")
        return JSONResponse(content={"challenge": challenge})

    return JSONResponse(content={"ok": True})
