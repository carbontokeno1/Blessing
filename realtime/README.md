# Blessing Webull real-time bridge

This service uses the official Webull OpenAPI Python SDK to subscribe to US stock QUOTE, SNAPSHOT and TICK streams over MQTT, then exposes a read-only WebSocket feed to the Blessing web UI.

## Required server environment variables

- `WEBULL_APP_KEY` — Webull App Key
- `WEBULL_APP_SECRET` — Webull App Secret
- `WEBULL_REGION` — defaults to `us` for US equities (as shown in Webull US SDK examples)
- `WEBULL_HTTP_HOST` — defaults to `api.webull.com`
- `WEBULL_MQTT_HOST` — defaults to `data-api.webull.com`
- `WEBULL_SYMBOLS` — defaults to AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AMD
- `ALLOWED_ORIGINS` — comma-separated browser origins

Never place App Secret in the frontend, GitHub, or chat. Add secrets only in the hosting provider's secret environment-variable settings.

## Run

```sh
pip install -r realtime/requirements.txt
uvicorn app:app --app-dir realtime --host 0.0.0.0 --port 10000
```

## Endpoints

- `GET /health` — process health
- `GET /status` — connection and freshness status
- `GET /quotes` — latest raw decoded quote payloads
- `WS /stream` — read-only live stream for the UI

No synthetic prices are generated. If credentials or the OpenAPI market-data entitlement are missing, the service stays in a waiting/error state and publishes no quotes.

The official SDK streaming callback payload schema can evolve. The bridge preserves decoded payloads and only maps a symbol when it can identify one confidently. Validate the exact quote fields against the approved Webull account before using them for strategy calculations.
