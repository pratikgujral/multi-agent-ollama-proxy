# Ollama Cluster Router & Discovery Daemon

A dynamic service discovery daemon and reverse proxy for Ollama clusters. This tool automatically scans designated network subnets for active Ollama nodes, registers their available models in a centralized SQLite registry, and seamlessly routes incoming LLM generation requests to capable machines. 

It is designed to provide a highly available, single-endpoint interface (`localhost:8000`) for multi-agent frameworks, decoupling your application logic from shifting internal IP addresses.

## Architecture & Visuals

### 1. Concurrent Subnet Scanning
The daemon uses asynchronous semaphores to aggressively sweep multiple `/16` or `/24` subnets simultaneously without exhausting OS socket limits or triggering IDS rate-limiters.

> **[Video Placeholder: `assets/concurrent-scan.mp4` showing the scanner threading across IPs]**
<!-- Replace this line with the GitHub generated video link: https://github.com/user-attachments/assets/... -->

### 2. Smart Routing & Load Balancing
The router parses the requested model from the JSON payload and queries the database for capable nodes:
* **Exclusive Routing:** If a model (e.g., `llama3:70b`) is only available on a single specific node, the router smartly sends all requests for that model directly to that node.
* **Load Balancing:** If multiple nodes host the requested model, traffic is distributed randomly among them.
* **Fallback Prevention:** If a requested model is not found anywhere in the cluster, the router immediately blocks the request and returns a `404 Not Found` error.

> **[Video Placeholder: `assets/load-balancing.mp4` showing requests routing to nodes]**
<!-- Replace this line with the GitHub generated video link -->

### 3. Garbage Collection
Nodes that drop offline or stop serving models are automatically pruned from the SQLite registry during the next background scan cycle, preventing black-hole routing.

---

## Features

* **Automated Node Discovery:** Asynchronously sweeps massive subnets.
* **Dynamic Configuration:** Targets are loaded from a JSON configuration file, allowing for hot-reloading of subnets without restarting the router.
* **Smart Routing:** Proxies requests to a healthy node serving the specifically requested model.
* **Garbage Collection:** Automatically prunes dead or offline nodes.
* **Stealth Scanning:** Configurable concurrency limits and micro-delays to evade Intrusion Detection Systems.
* **Reporting:** Built-in utility to export the live cluster topology to a professionally styled Excel report.

## Prerequisites

* Python 3.8+
* An active internal network with machines running Ollama on port `11434`.

## Installation

1. Clone or download this repository.
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `config.json` file in the root directory to define the subnets you want to scan:
   ```json
   {
     "subnets": [
       "10.237.0.0/16",
       "10.208.0.0/16"
     ]
   }
   ```

## Usage

### 1. Starting the Router
Run the main daemon. By default, it will look for `config.json` and start the API on port 8000.

```bash
python ollama_router.py
```

**Optional Arguments:**
* `--config`: Path to a custom JSON configuration file (default: `config.json`).
* `--port`: The port to host the API router on (default: `8000`).
* `--max-concurrent-pings`: Throttles the scanner to prevent IDS alerts (default: `50`).
* `--no-scan`: Disables the background scanning loop. The router will rely entirely on the existing SQLite database.

**Example with arguments:**
```bash
python ollama_router.py --config ./tests/config_staging.json --port 8080 --no-scan
```

### 2. Manual Scan Trigger
The daemon automatically scans the network every 5 minutes based on the `config.json`. To force an immediate scan, hit the manual trigger endpoint:

```bash
curl -X POST http://localhost:8000/cluster/scan
```

### 3. Application Integration
Update your LLM frameworks to point to this router instead of a specific node. The proxy behaves exactly like a native Ollama endpoint.

**Example (Python / LangChain):**
```python
from langchain_community.chat_models import ChatOllama

# Point directly to the router's port
llm = ChatOllama(
    base_url="http://localhost:8000", 
    model="llama3:8b"
)
```

### 4. Exporting Cluster Reports
To generate a snapshot of your current network topology and available models, run the export script:

```bash
python export_to_excel.py
```
This will read the `ollama_cluster.db` SQLite database and generate a styled `.xlsx` 