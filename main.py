import os
import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

app = FastAPI()

@app.get("/")
def health_check():
    return {"status": "✅ Genie middleware is running"}

@app.post("/slack/events")
async def slack_events(request: Request):
    data = await request.json()
    print("🔔 Event received:", data)
    
    # Slack URL verification
    if data.get("type") == "url_verification":
        return PlainTextResponse(content=data["challenge"])
    
    # Example event handling (add your logic later)
    return PlainTextResponse("ok")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))  # no fallback to 10000!
    uvicorn.run("main:app", host="0.0.0.0", port=port)
