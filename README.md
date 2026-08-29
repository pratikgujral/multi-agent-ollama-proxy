# Ollama Cluster Router & Discovery Daemon

A smart, self-healing reverse proxy that turns a network of individual Ollama machines into one massive, unified AI cluster.

Instead of hardcoding different IP addresses for different models across your team or applications, you point everything to a single endpoint (`localhost:8000`). This router automatically scans your network to find active Ollama nodes, remembers which models live where, and seamlessly routes your requests to the right machine.

If a machine goes offline, the router automatically drops it. If a new model is downloaded, the router instantly discovers it.

## Architecture & Visuals

### 1. Concurrent Subnet Discovery
The daemon aggressively but safely sweeps entire corporate networks (multiple `/16` or `/24` subnets) to find active Ollama nodes without exhausting OS socket limits or triggering Intrusion Detection Systems (IDS).


### 2. Smart Routing & Load Balancing
The router intercepts your request, reads the requested model, and checks its internal database:
* **Exclusive Routing:** If a model is only on one node, the request goes straight there.
* **Load Balancing:** If multiple nodes have the same model (e.g., `qwen3-coder:30b`), requests are load-balanced randomly across them.
* **Transparent Fallback:** Unrecognized or model-agnostic requests (like `GET /api/version`) are safely routed to a random healthy node.
* **Missing Models:** If a model isn't anywhere on the network, it instantly returns a `404 Not Found`.


### 3. Self-Healing Garbage Collection
Nodes that drop offline or stop serving models are automatically pruned from the SQLite registry during the background scan cycle, preventing black-hole routing.

---

## Prerequisites

* Python 3.8+
* An active internal network with machines running Ollama on port `11434`.

## Installation & Setup

1. Clone or download this repository.
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `config.json` file in the root directory to define the IP subnets you want the router to scan:
   ```json
   {
     "subnets": [
       "10.237.0.0/16",
       "10.208.0.0/16"
     ]
   }
   ```
4. Start the router daemon:
   ```bash
   python ollama_router.py
   ```

**Optional Startup Arguments:**
* `--port`: The port to host the API router on (default: `8000`).
* `--max-concurrent-pings`: Throttles the scanner to prevent IDS alerts (default: `50`).
* `--no-scan`: Disables background scanning. The router will rely entirely on the existing SQLite database.
* `--config`: Path to a custom JSON configuration file (default: `config.json`).

---

## 🔌 Connecting Client Applications

Because the router uses transparent byte-streaming, it behaves exactly like a native Ollama endpoint. You can plug it into any AI tool.

### 1. Continue.dev (VS Code / JetBrains)
To use the cluster for coding assistance, point Continue to the router port in your `config.yaml` file. 

* **Mac/Linux:** `~/.continue/config.yaml`
* **Windows:** `%USERPROFILE%\.continue\config.yaml`

```yaml
models:
  - name: "Cluster: Qwen Coder (30B)"
    provider: "ollama"
    model: "qwen3-coder:30b"
    apiBase: "http://localhost:8000"
    roles:
      - chat
      - edit
  
  - name: "Cluster: Autocomplete"
    provider: "ollama"
    model: "qwen2.5-coder:1.5b-base"
    apiBase: "http://localhost:8000"
    roles:
      - autocomplete
```

### 2. Open WebUI
To use the cluster as the backend for Open WebUI, simply set the `OLLAMA_BASE_URL` environment variable when launching the WebUI docker container.

```bash
docker run -d -p 3000:8080 \
  -e OLLAMA_BASE_URL=http://localhost:8000 \
  -v open-webui:/app/backend/data \
  --name open-webui \
  ghcr.io/open-webui/open-webui:main
```

### 3. Python SDKs (LangChain, OpenAI, LiteLLM)
You can point standard SDKs directly to the router. It supports both native Ollama endpoints and Ollama's OpenAI-compatibility layer (`/v1`).

```python
from langchain_community.chat_models import ChatOllama

# Point directly to the router's port
llm = ChatOllama(
    base_url="http://localhost:8000", 
    model="llama3.1:8b"
)
```

---

## 🛠️ API Endpoints

The router supports all standard Ollama endpoints (`/api/generate`, `/api/chat`, `/api/embed`, `/v1/messages`), plus custom management endpoints:

* **`GET /api/tags`**: Dynamically aggregates and returns a deduplicated union of every model available across your entire cluster.
* **`GET /cluster/status`**: Returns a JSON map of all currently active nodes and the specific models they are hosting.
* **`POST /cluster/scan`**: Forces an immediate asynchronous network sweep (bypassing the 5-minute background timer).

```bash
# Force an instant network scan
curl -X POST http://localhost:8000/cluster/scan
```

---

## 📊 Exporting Cluster Reports

To generate a snapshot of your current network topology and available models for auditing or infrastructure monitoring, run the standalone export script:

```bash
python export_to_excel.py
```
This script reads the `ollama_cluster.db` SQLite database and generates a professionally styled `.xlsx` file detailing cluster capacity, model distribution, and active IP addresses.