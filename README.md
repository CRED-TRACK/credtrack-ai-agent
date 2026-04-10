# credtrack-ai-agent

Spring Boot service that polls Gmail for bank statement emails, extracts structured data using a local LLM, and writes it to the CredTrack backend.

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
- [Ollama](https://ollama.com) running locally

## Setting up Ollama

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

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GOOGLE_CLIENT_ID` | Google OAuth client ID | — |
| `GOOGLE_CLIENT_SECRET` | Google OAuth client secret | — |
| `BACKEND_BASE_URL` | CredTrack backend URL | `http://localhost:8080` |
| `INTERNAL_SERVICE_KEY` | Shared secret for `/internal` endpoints | `changeme-dev-key` |
| `GMAIL_POLL_INTERVAL_MS` | Poll interval in ms | `3600000` (1 hour) |

## Running

```bash
export GOOGLE_CLIENT_ID=...
export GOOGLE_CLIENT_SECRET=...
mvn spring-boot:run
```

The agent starts on port `8081` and polls Gmail every hour.
