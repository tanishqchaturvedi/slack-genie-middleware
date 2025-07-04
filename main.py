import os
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import uvicorn

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/slack/events")
async def slack_events(request: Request):
    data = await request.json()
    print("🔔 Received Slack Event:", data)

    if data.get("type") == "url_verification":
        return PlainTextResponse(content=data["challenge"])

    return PlainTextResponse("ok")

if __name__ == "__main__":
    # ✅ Use only PORT from environment
    port = int(os.environ.get("PORT", "3000"))  # DO NOT default to 10000
    print(f"🚀 Starting on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port)
