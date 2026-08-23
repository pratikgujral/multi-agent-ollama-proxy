# Ollama Cluster Router & Discovery Daemon

A dynamic service discovery daemon and reverse proxy for Ollama clusters. This tool automatically scans designated network subnets for active Ollama nodes, registers their available models in a centralized SQLite registry, and seamlessly routes incoming LLM generation requests to capable machines. 

It is designed to provide a highly available, single-endpoint interface (`localhost:8000`) for multi-agent frameworks, decoupling your application logic from shifting internal IP addresses.

## Features

* **Automated Node Discovery:** Asynchronously sweeps massive subnets (e.g., `/16`) to find active Ollama instances.
* **Dynamic Routing:** Intercepts standard Ollama API requests and proxies them to a healthy node serving the specifically requested model.
* **Garbage Collection:** Automatically prunes dead or offline nodes from the routing tables.
* **Stealth Scanning:** Configurable concurrency limits and micro-delays to avoid triggering network Intrusion Detection Systems (IDS).
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

## Usage

### 1. Starting the Router
Run the main daemon, specifying the subnets you want to monitor. 

```bash
python ollama_router.py \
  --subnets 10.237.0.0/16 10.208.0.0/16 \
  --port 8000 \
  --max-concurrent-pings 20
```

**Arguments:**
* `--subnets`: (Required) Space-separated list of CIDR blocks to scan.
* `--port`: (Optional) The port to host the API router on (default: `8000`).
* `--max-concurrent-pings`: (Optional) Throttles the scanner to prevent IDS alerts (default: `50`).

### 2. Manual Scan Trigger
The daemon automatically scans the network every 5 minutes. To force an immediate scan (e.g., after spinning up a new server), hit the manual trigger endpoint:

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
This will read the `ollama_cluster.db` SQLite database and generate a styled `.xlsx` file (e.g., `Ollama_Cluster_Report_YYYYMMDD_HHMMSS.xlsx`) in your directory.