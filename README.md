# 🦙 Ollama Cluster Router & Discovery Daemon

A smart, self-healing reverse proxy that automatically connects multiple Ollama instances across your network into one unified AI cluster.

Instead of remembering which machine has `llama3` or `qwen3-coder:30b`, you point your applications (Open WebUI, Continue.dev, LangChain, etc.) to **`http://localhost:8000`**. The router scans your network, catalogs models, and routes every prompt to an available machine hosting that model.

---

## 🚀 Quickstart (Beginner-Friendly)

### 1. Clone & Install Dependencies

```bash
git clone https://github.com/pratikgujral/multi-agent-ollama-proxy.git
cd multi-agent-ollama-proxy
pip install -r requirements.txt
```

### 2. Configure Your Network Subnets

Create or edit `config.json` in the project root:

```json
{
  "subnets": [
    "10.237.0.0/16",
    "192.168.1.0/24"
  ]
}
```
*(Specify the subnets where your Ollama nodes live. Invalid subnets are automatically validated and skipped).*

### 3. Start the Router

```bash
python ollama_router.py
```

Open **`http://localhost:8000/dashboard`** in your browser to watch active nodes and models appear in real time!

---

## 🧭 How It Works

```
                                      ┌───> [Node 10.237.1.10] (llama3:8b)
[Your App / Open WebUI]               │
         │                            ├───> [Node 10.237.1.11] (qwen3-coder:30b)
         ▼                            │
  http://localhost:8000 ──────────────┼───> [Node 10.237.1.12] (qwen3-coder:30b)
  (Ollama Router Daemon)               │     (load-balanced randomly)
                                      └───> [Node 10.237.1.13] (fallback node)
```

1. **Subnet Auto-Discovery**: Concurrently scans target IP ranges on port `11434` and queries each active node's `/api/tags` endpoint in parallel.
2. **Dynamic Model Routing**:
   - **Model-Specific**: Requests specifying a `model` (e.g. `/api/generate`, `/api/chat`) route directly to a node hosting that model. If multiple nodes host it, traffic is load-balanced across them.
   - **Cluster-Wide Tags**: `GET /api/tags` returns the deduplicated union of all models available on the entire cluster.
   - **Transparent Fallback**: Generic requests (like `/api/version`) route to any healthy node.
   - **Missing Model**: Returns a prompt `404 Not Found` if no machine has the requested model.
3. **High Performance**: Uses a persistent, shared connection pool (`httpx.AsyncClient`) for high throughput streaming inference without TCP handshake overhead.
4. **Self-Healing Registry**: Machines that turn off or disconnect are automatically pruned on each cycle.

---

## 🛠️ API & Web Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/dashboard` | `GET` | Live browser dashboard monitoring online nodes & hosted models |
| `/health` | `GET` | Service health, uptime, active node count, and model count |
| `/cluster/status` | `GET` | JSON snapshot of all active nodes and their models |
| `/cluster/status/stream` | `GET` | Real-time Server-Sent Events (SSE) stream for live updates |
| `/cluster/scan` | `POST` | Trigger an immediate manual network scan cycle |
| `/api/tags` | `GET` | Aggregated list of all models across all cluster nodes |
| `/*` (All Ollama routes) | `ANY` | Transparent proxy to backend nodes (`/api/generate`, `/api/chat`, etc.) |

---

## ⚙️ Configuration & Options

You can configure the router using CLI flags or environment variables:

| Setting | CLI Flag | Environment Variable | Default | Description |
|---|---|---|---|---|
| Port | `--port` | `PORT` | `8000` | Port for the router to listen on |
| Concurrency | `--max-concurrent-pings` | `CONCURRENCY_LIMIT` | `50` | Maximum simultaneous scan sockets |
| Config File | `--config` | `CONFIG_FILE` | `config.json` | Path to subnets JSON file |
| Scan Interval | — | `SCAN_INTERVAL` | `300` | Seconds between discovery sweeps |
| Database Path | — | `DB_FILE` | `ollama_cluster.db` | Path to SQLite registry |
| Disable Scanner | `--no-scan` | `DISABLE_SCAN=true` | `false` | Router-only mode without scanning |

**Example using CLI flags:**
```bash
python ollama_router.py --port 9000 --max-concurrent-pings 100
```

**Example using Environment Variables (Docker / Uvicorn friendly):**
```bash
PORT=8000 SCAN_INTERVAL=180 python ollama_router.py
```

---

## 🔌 Connecting Client Applications

Because the router streams raw HTTP bytes transparently, any client that talks to Ollama can use it seamlessly.

### 1. Open WebUI

Set `OLLAMA_BASE_URL` to point to the router:

```bash
docker run -d -p 3000:8080 \
  -e OLLAMA_BASE_URL=http://host.docker.internal:8000 \
  --name open-webui \
  ghcr.io/open-webui/open-webui:main
```

### 2. Continue.dev (VS Code / JetBrains)

Point your Continue `config.yaml` to the router's port:

```yaml
models:
  - name: "Cluster: Qwen Coder (30B)"
    provider: "ollama"
    model: "qwen3-coder:30b"
    apiBase: "http://localhost:8000"
    roles:
      - chat
      - edit
```

### 3. Python SDKs (LangChain, OpenAI, LiteLLM)

```python
from langchain_community.chat_models import ChatOllama

# Point directly to the router
llm = ChatOllama(
    base_url="http://localhost:8000",
    model="llama3.1:8b"
)
print(llm.invoke("Explain quantum computing in one sentence."))
```

---

## 📊 Exporting Cluster Reports

Generate a styled Excel spreadsheet report (`.xlsx`) documenting all nodes, last-seen timestamps, and hosted models:

```bash
python export_to_excel.py
```

---

## 🧪 Running Tests

The test suite covers endpoints, proxying, concurrent state fetching, configuration handling, and health checks:

```bash
python -m pytest tests/ -v
```