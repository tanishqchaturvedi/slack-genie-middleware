# slack-genie-middleware

# Slack ↔ Genie Middleware (FastAPI)

A minimal FastAPI server to capture Slack events, verify users, and later call Genie running inside Databricks.

## Endpoint

- `POST /slack/events`: Handles Slack webhook events

## Deploying

Deploy this repo on Render or Railway to expose a public HTTPS URL.

## Coming Next
- Connect to Genie (Databricks)
- Post reply back to Slack
