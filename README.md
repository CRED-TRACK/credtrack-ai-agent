# credtrack-ai-agent

Spring Boot service that polls Gmail for bank statement emails, extracts structured data using either Ollama or Gemini, and writes it to the CredTrack backend.

## Architecture

```
EmailPollScheduler (every 1h)
  └── EmailPipelineCoordinator (Akka)
        └── EmailFetcherActor (per user)
              └── StatementExtractorActor (per email)
                    └── StatementWriterActor → POST /internal/statements
```

## Prerequisites

- Java 21
- Maven
- Either:
  - [Ollama](https://ollama.com) running locally, or
  - a Gemini API key

## LLM options

### Option 1: Ollama only

1. Install Ollama:
   ```bash
   brew install ollama
   ```

2. Start the Ollama server:
   ```bash
   ollama serve
   ```

3. Pull the model (first time only):
   ```bash
   ollama pull llama3.1
   ```

4. Verify it's running:
   ```bash
   curl http://localhost:11434/api/tags
   ```

Ollama runs at `http://localhost:11434` by default — matches `spring.ai.ollama.base-url` in `application.properties`.

### Option 2: Gemini with Ollama fallback

Set:

```bash
export LLM_PRIMARY_PROVIDER=gemini
export LLM_FALLBACK_PROVIDER=ollama
export GEMINI_API_KEY=...
export GEMINI_MODEL=gemini-2.5-flash-lite
```

If Gemini fails or is rate-limited, the agent will try Ollama next.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GOOGLE_CLIENT_ID` | Google OAuth client ID | — |
| `GOOGLE_CLIENT_SECRET` | Google OAuth client secret | — |
| `BACKEND_BASE_URL` | CredTrack backend URL | `http://localhost:8080` |
| `INTERNAL_SERVICE_KEY` | Shared secret for `/internal` endpoints | `changeme-dev-key` |
| `LLM_PRIMARY_PROVIDER` | `ollama` or `gemini` | `ollama` |
| `LLM_FALLBACK_PROVIDER` | `ollama`, `gemini`, or `none` | `none` |
| `OLLAMA_BASE_URL` | Ollama base URL | `http://localhost:11434` |
| `OLLAMA_MODEL` | Ollama fallback model name | `llama3.1` |
| `GEMINI_API_KEY` | Gemini Developer API key | — |
| `GEMINI_MODEL` | Gemini model name | `gemini-2.5-flash-lite` |
| `GEMINI_BASE_URL` | Gemini API base URL | `https://generativelanguage.googleapis.com` |
| `GEMINI_MAX_OUTPUT_TOKENS` | Gemini max output tokens | `1024` |
| `GEMINI_TEMPERATURE` | Gemini sampling temperature | `0.1` |
| `GEMINI_TIMEOUT_SECONDS` | Gemini request timeout | `60` |
| `GMAIL_POLL_INTERVAL_MS` | Poll interval in ms | `10800000` (3 hours) |

Recommended Gemini deployment profile:

```bash
export LLM_PRIMARY_PROVIDER=gemini
export LLM_FALLBACK_PROVIDER=ollama
export GEMINI_MODEL=gemini-2.5-flash-lite
export OLLAMA_MODEL=llama3.1
```

## Running

```bash
export GOOGLE_CLIENT_ID=...
export GOOGLE_CLIENT_SECRET=...
mvn spring-boot:run
```

The agent starts on port `8081` and polls Gmail every hour.

## Deployment

The container is now deployment-ready with env-driven runtime config:

- HTTP port comes from `PORT`
- Actuator health is available at `/actuator/health`
- Akka canonical host/port can be overridden with:
  - `AKKA_CANONICAL_HOSTNAME`
  - `AKKA_CANONICAL_PORT`
- Akka snapshots default to `/tmp/akka-snapshots`

Minimum production env vars:

```bash
PORT=8081
BACKEND_BASE_URL=https://<your-backend-url>
INTERNAL_SERVICE_KEY=<shared-secret>
GOOGLE_CLIENT_ID=<google-client-id>
GOOGLE_CLIENT_SECRET=<google-client-secret>
LLM_PRIMARY_PROVIDER=gemini
LLM_FALLBACK_PROVIDER=ollama
GEMINI_API_KEY=<gemini-api-key>
GEMINI_MODEL=gemini-2.5-flash-lite
```

Optional but recommended:

```bash
OLLAMA_MODEL=llama3.1
GEMINI_TIMEOUT_SECONDS=60
GEMINI_MAX_OUTPUT_TOKENS=1024
GEMINI_TEMPERATURE=0.1
AKKA_SNAPSHOT_DIR=/tmp/akka-snapshots
```

If your deployment platform does not run Ollama in the same environment, set:

```bash
LLM_FALLBACK_PROVIDER=none
```

Build and run locally as a container:

```bash
docker build -t credtrack-ai-agent .
docker run --rm -p 8081:8081 --env-file .env credtrack-ai-agent
```
