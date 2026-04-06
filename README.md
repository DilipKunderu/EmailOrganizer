# Email Organizer Agent

A self-improving Gmail organizer that runs perpetually on your machine (or in the cloud), automatically labels, archives, unsubscribes from fluff, and learns from your behavior over time.

## Features

- **13-label taxonomy** with hard anti-sprawl cap (4 action + 6 category + 3 system)
- **Aggressive unsubscribe** engine driven by engagement scoring (zero-engagement senders get auto-unsubscribed)
- **Pluggable LLM** backend (OpenAI, Anthropic, Ollama, llama.cpp, or rules-only)
- **Self-improving rules** via a sidecar learner that promotes LLM decisions into deterministic rules
- **IMAP IDLE** for instant new-mail detection (no polling)
- **Background crawl** that gradually processes your entire mailbox
- **Tiered guardrails** with dry-run, quarantine, circuit breaker, and undo
- **Daily digest** sent as a self-email
- **Deployable locally** (macOS launchd + SQLite) or in the **cloud** (Docker + PostgreSQL)

## Quick Start (Local)

### 1. Prerequisites

- Python 3.11+
- A Google Cloud project with Gmail API enabled
- OAuth 2.0 credentials (Desktop app type)

### 2. Setup

```bash
cd EmailOrganizer
pip install -r requirements.txt
```

### 3. Google OAuth Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project, enable the Gmail API
3. Create OAuth 2.0 credentials (Desktop application)
4. Download the JSON and save as `~/.emailorganizer/credentials.json`

### 4. First Run

```bash
# Single run (authenticates, creates labels, processes inbox)
python -m src.main --once

# Install as persistent service
python -m src.main --install-service

# Check status
python -m src.main --status
```

### 5. Configure LLM (Optional)

Edit `config/settings.yaml`:

```yaml
llm:
  provider: ollama        # or openai, anthropic, llamacpp, none
  model: llama3.2:8b
```

For cloud providers, set the API key in `.env`:
```
OPENAI_API_KEY=sk-...
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `--once` | Single sync cycle |
| `--install-service` | Install as launchd daemon |
| `--uninstall-service` | Remove launchd daemon |
| `--status` | Show daemon health |
| `--reset-crawl` | Restart background crawl |
| `--undo-last-run` | Reverse last batch of actions |
| `--learn-now` | Run the rule learner |
| `--install-learner` | Install learner as launchd service |
| `--learner-status` | Show learner stats and LLM dependency trend |
| `--reset-rules` | Wipe all auto-rules |

## Architecture

Hexagonal (ports & adapters) design. Core pipeline is deployment-agnostic:

```
MailNotifier -> ThreadFetcher -> Enricher -> EngagementTracker
  -> HybridClassifier -> ActionPlanner -> Guardrails -> BatchExecutor -> Gmail API
```

Six port interfaces with swappable adapters:
- **StateStore**: SQLite (local) / PostgreSQL (cloud)
- **MailNotifier**: IMAP IDLE (both) / Pub/Sub (cloud)
- **ProcessManager**: launchd (local) / Docker health endpoint (cloud)
- **ConfigLoader**: YAML files (local) / env vars + DB (cloud)
- **LockManager**: fcntl.flock (local) / PG advisory locks (cloud)
- **Auth**: single token file (local) / encrypted DB tokens (cloud)

## Cloud Deployment

```bash
docker-compose up -d
```

## License

MIT
