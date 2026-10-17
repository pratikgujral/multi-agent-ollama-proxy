"""
Prerequisites:
pip install fastapi uvicorn httpx tqdm
"""
# Standard Library Imports
import argparse
import asyncio
import ipaddress
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

# Third-Party Imports
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from tqdm.asyncio import tqdm_asyncio

# Default Configuration
CONFIG_FILE = "config.json"
DB_FILE = "ollama_cluster.db"
SCAN_INTERVAL = 300  
CONCURRENCY_LIMIT = 1000
DISABLE_SCAN = False

# Global event for manual scan triggering
trigger_scan_event = asyncio.Event()

# --- LOGGING CONFIGURATION ---
def setup_logger():
    # Create a custom logger
    logger = logging.getLogger("OllamaRouter")
    logger.setLevel(logging.INFO)

    # Prevent duplicate logs if function is called multiple times
    if not logger.handlers:
        # 1. Rotating File Handler (Max 5MB per file, keep 3 backups)
        file_handler = RotatingFileHandler(
            "ollama_router.log", maxBytes=5*1024*1024, backupCount=3
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s', 
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # 2. Console Handler (for terminal output)
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(message)s') # Keep terminal clean
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
    return logger

log = setup_logger()
# -----------------------------

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS nodes
                     (ip TEXT PRIMARY KEY, last_seen DATETIME)''')
        c.execute('''CREATE TABLE IF NOT EXISTS models
                     (node_ip TEXT, model_name TEXT, 
                      PRIMARY KEY (node_ip, model_name))''')
        conn.commit()

async def check_port(ip: str, port: int = 11434, timeout: float = 1.0) -> bool:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), timeout=timeout
        )
        writer.close()
        await writer.wait_closed()
        return True
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
        return False

# Helper to query real-time node state
async def fetch_current_cluster_state():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT DISTINCT ip FROM nodes")
        nodes = [row[0] for row in c.fetchall()]

    if not nodes:
        return {"active_nodes": []}

    sem = asyncio.Semaphore(min(20, len(nodes)))
    async with httpx.AsyncClient(timeout=1.5) as client:
        async def verify_node(ip: str):
            async with sem:
                try:
                    resp = await client.get(f"http://{ip}:11434/api/tags")
                    if resp.status_code == 200:
                        models = [m.get("name") for m in resp.json().get("models", [])]
                        return {"ip": ip, "models": sorted(models), "status": "online"}
                except Exception:
                    pass
            return {"ip": ip, "status": "offline"}

        results = await asyncio.gather(*(verify_node(ip) for ip in nodes))
    active_nodes = [{"ip": r["ip"], "models": r["models"]} for r in results if r["status"] == "online"]
    return {"active_nodes": active_nodes}

def get_target_subnets():
    """Reads the latest subnets from the JSON configuration file."""
    if not os.path.exists(CONFIG_FILE):
        log.warning(f"[-] Config file {CONFIG_FILE} not found. Skipping scan.")
        return []
        
    try:
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
            raw_subnets = config.get("subnets", [])
    except json.JSONDecodeError:
        log.error("[-] Invalid JSON in config file. Skipping scan.")
        return []
    
    validated = []
    for subnet in raw_subnets:
        try:
            ipaddress.ip_network(subnet, strict=False)
            validated.append(subnet)
        except ValueError:
            log.warning(f"[-] Skipping invalid subnet entry: '{subnet}'")
    return validated
    
async def execute_scan_cycle():
    """Performs the actual network sweep and database update."""
    # Fetch the subnets dynamically at the start of every cycle
    current_subnets = get_target_subnets()
    
    if not current_subnets:
        log.info("[*] No subnets configured or valid in config.json. Waiting for next cycle.")
        return

    log.info(f"[*] Scanning subnets: {', '.join(current_subnets)}...")
    
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async def bounded_check(ip):
        async with sem:
            return await check_port(ip)
            
    all_hosts = []
    for subnet in current_subnets:
        network = ipaddress.ip_network(subnet, strict=False)
        all_hosts.extend(list(network.hosts()))
        
    log.info(f"[*] Discovered {len(all_hosts)} total IP addresses to ping.")
    
    tasks = [bounded_check(str(ip)) for ip in all_hosts]
    
    # Progress bar only goes to stdout, so we don't log it directly 
    # to avoid flooding the file with carriage returns
    results = await tqdm_asyncio.gather(*tasks, desc="Ping Progress", unit="ip")
    
    active_ips = [str(ip) for ip, is_active in zip(all_hosts, results) if is_active]
    
    valid_nodes = []
    
    if active_ips:
        log.info(f"[*] Found {len(active_ips)} active endpoints. Fetching model tags...")
    
    tag_sem = asyncio.Semaphore(min(20, max(len(active_ips), 1)))

    async def fetch_tags(client: httpx.AsyncClient, ip: str):
        async with tag_sem:
            try:
                resp = await client.get(f"http://{ip}:11434/api/tags")
                if resp.status_code == 200:
                    models = resp.json().get("models", [])
                    return ip, models
            except httpx.RequestError:
                pass
        return ip, None

    async with httpx.AsyncClient(timeout=5.0) as client:
        tag_results = await asyncio.gather(*(fetch_tags(client, ip) for ip in active_ips))

    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        for ip, models in tag_results:
            if models is not None:
                c.execute("INSERT OR REPLACE INTO nodes (ip, last_seen) VALUES (?, datetime('now'))", (ip,))
                c.execute("DELETE FROM models WHERE node_ip = ?", (ip,))
                for m in models:
                    c.execute("INSERT INTO models (node_ip, model_name) VALUES (?, ?)", (ip, m["name"]))
                
                valid_nodes.append(ip)
                log.info(f"[+] Node {ip} registered with {len(models)} models.")
        
        if valid_nodes:
            placeholders = ','.join('?' for _ in valid_nodes)
            c.execute(f"DELETE FROM nodes WHERE ip NOT IN ({placeholders})", valid_nodes)
            c.execute(f"DELETE FROM models WHERE node_ip NOT IN ({placeholders})", valid_nodes)
        else:
            c.execute("DELETE FROM nodes")
            c.execute("DELETE FROM models")
            log.warning("[-] No valid models found during this cycle. Routing tables cleared.")
        
        conn.commit()

async def scan_network_loop():
    """Background loop that waits for interval OR a manual trigger."""
    while True:
        await execute_scan_cycle()
        
        try:
            await asyncio.wait_for(trigger_scan_event.wait(), timeout=SCAN_INTERVAL)
            log.info("\n[*] Manual scan triggered via API.")
        except asyncio.TimeoutError:
            pass
        finally:
            trigger_scan_event.clear()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if not DISABLE_SCAN:
        task = asyncio.create_task(scan_network_loop())
        yield
        task.cancel()
    else:
        log.info("[*] Running in router-only mode. Background scanner is disabled.")
        yield

app = FastAPI(lifespan=lifespan)

@app.post("/cluster/scan")
async def trigger_manual_scan():
    """Manually forces a cluster scan cycle."""
    if trigger_scan_event.is_set():
        return JSONResponse(status_code=409, content={"message": "Scan already queued or in progress."})
    
    trigger_scan_event.set()
    return {"message": "Manual scan triggered successfully."}

@app.get("/cluster/status")
async def get_status():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT node_ip, GROUP_CONCAT(model_name) FROM models GROUP BY node_ip")
        rows = c.fetchall()
        return {"active_nodes": [{"ip": row[0], "models": row[1].split(",")} for row in rows]}

@app.get("/cluster/status/stream")
async def stream_cluster_status(request: Request):
    """
    Server-Sent Events (SSE) endpoint that streams the real-time cluster state
    every 3 seconds to connected browser clients.
    """
    async def event_generator():
        while True:
            # Detect if client disconnected (tab closed / navigation) to stop polling
            if await request.is_disconnected():
                break

            status_data = await fetch_current_cluster_state()
            
            # SSE protocol format: 'data: <payload>\n\n'
            yield f"data: {json.dumps(status_data)}\n\n"
            
            await asyncio.sleep(3)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Prevents Nginx/proxy buffering if deployed behind reverse proxy
        }
    )

@app.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Ollama Cluster Live Monitor</title>
        <style>
            body { font-family: system-ui, -apple-system, sans-serif; background: #0f172a; color: #f8fafc; padding: 2rem; }
            h1 { font-size: 1.5rem; margin-bottom: 0.5rem; display: flex; align-items: center; gap: 0.5rem; }
            .pulse { width: 10px; height: 10px; background: #22c55e; border-radius: 50%; box-shadow: 0 0 8px #22c55e; }
            .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 1.25rem; margin-top: 1.5rem; }
            .card { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 1.25rem; }
            .ip { font-size: 1.1rem; font-weight: 600; color: #38bdf8; margin-bottom: 0.5rem; }
            .model-tag { display: inline-block; background: #334155; color: #e2e8f0; font-size: 0.8rem; padding: 0.25rem 0.5rem; border-radius: 4px; margin: 0.2rem; }
            #node-count { color: #94a3b8; font-size: 0.95rem; }
        </style>
    </head>
    <body>
        <h1><div class="pulse"></div> Ollama Cluster Live Monitor</h1>
        <div id="node-count">Connecting to cluster stream...</div>
        <div class="grid" id="cluster-grid"></div>

        <script>
            const eventSource = new EventSource('/cluster/status/stream');
            const grid = document.getElementById('cluster-grid');
            const nodeCount = document.getElementById('node-count');

            eventSource.onmessage = (event) => {
                const data = JSON.parse(event.data);
                const nodes = data.active_nodes || [];
                
                nodeCount.textContent = `Active Nodes: ${nodes.length} | Last Synced: ${new Date().toLocaleTimeString()}`;
                
                if (nodes.length === 0) {
                    grid.innerHTML = '<div class="card" style="grid-column: 1/-1;">No active Ollama nodes detected.</div>';
                    return;
                }

                grid.innerHTML = nodes.map(node => `
                    <div class="card">
                        <div class="ip">🖥️ ${node.ip}</div>
                        <div><strong>Models (${node.models.length}):</strong></div>
                        <div style="margin-top: 0.5rem;">
                            ${node.models.map(m => `<span class="model-tag">${String(m).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}</span>`).join('')}
                        </div>
                    </div>
                `).join('');
            };

            eventSource.onerror = (err) => {
                nodeCount.textContent = 'Connection lost. Reconnecting...';
            };
        </script>
    </body>
    </html>
    """
# --- DYNAMIC SDK ROUTING ---

def get_node_for_model(model_req: str) -> str:
    """Queries the SQLite database for a random active node hosting the requested model."""
    if not model_req:
        raise HTTPException(status_code=400, detail="Missing 'model' parameter in payload")
        
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT node_ip FROM models WHERE model_name = ? ORDER BY RANDOM() LIMIT 1", (model_req,))
        row = c.fetchone()
        
    if not row:
        log.warning(f"[-] Blocked request: Model '{model_req}' not found on cluster.")
        raise HTTPException(status_code=404, detail=f"Model '{model_req}' not found")
        
    return row[0]

@app.get("/api/tags")
async def get_cluster_tags():
    """
    Returns the distinct union of all models available across the cluster,
    formatted to match Ollama's native /api/tags response structure.
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            # Fetch distinct model names available across all active nodes
            c.execute("SELECT DISTINCT model_name FROM models ORDER BY model_name ASC")
            rows = c.fetchall()
            
        models_list = []
        current_time = datetime.now(timezone.utc).isoformat()
        
        for (model_name,) in rows:
            # Infer the base model family from the tag (e.g. 'qwen3-coder:30b' -> 'qwen3-coder')
            base_family = model_name.split(":")[0] if ":" in model_name else model_name
            
            models_list.append({
                "name": model_name,
                "model": model_name,
                "modified_at": current_time,
                "size": 0,
                "digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                "details": {
                    "parent_model": "",
                    "format": "gguf",
                    "family": base_family,
                    "families": [base_family],
                    "parameter_size": "",
                    "quantization_level": ""
                }
            })
            
        return JSONResponse(content={"models": models_list})

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch cluster models: {str(e)}")

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD"])
async def proxy_catch_all(path: str, request: Request):
    """
    Transparently proxies raw bytes to the cluster.
    Passes original status codes and headers to prevent client stream crashes.
    """
    body = await request.body()
    model_req = None
    
    # Attempt to extract a model name if it's a POST request
    if request.method in ["POST", "PUT"] and body:
        try:
            payload = json.loads(body)
            model_req = payload.get("model") or payload.get("name")
        except json.JSONDecodeError:
            pass 

    # 1. Dynamic Routing (Model Specific)
    if model_req:
        node_ip = get_node_for_model(model_req)
        
    # 2. Generic Fallback Routing (No Model Specified)
    else:
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            c.execute("SELECT DISTINCT ip FROM nodes ORDER BY RANDOM() LIMIT 1")
            row = c.fetchone()
            
        if not row:
            raise HTTPException(status_code=503, detail="No active nodes available in cluster")
        node_ip = row[0]

    target_url = f"http://{node_ip}:11434/{path}"
    log.info(f"[~] Routing {request.method} /{path} -> {node_ip}")
    
    # 3. Establish connection and grab original headers
    request_connection_tokens = {
        token.strip().lower()
        for token in request.headers.get("connection", "").split(",")
        if token.strip()
    }
    excluded_request_headers = {
        "host", "content-length", "connection", "keep-alive",
        "proxy-authenticate", "proxy-authorization", "te", "trailer",
        "transfer-encoding", "upgrade", *request_connection_tokens,
    }
    proxy_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in excluded_request_headers
    }
    client = httpx.AsyncClient(timeout=120.0)
    
    req = client.build_request(
        method=request.method, 
        url=target_url, 
        content=body, 
        headers=proxy_headers
    )
    
    try:
        # We use stream=True to hold the connection open
        resp = await client.send(req, stream=True)
    except asyncio.CancelledError:
        await client.aclose()
        raise
    except Exception as e:
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"Target node unreachable: {str(e)}")

    # Filter out hop-by-hop headers that FastAPI will automatically handle
    excluded_headers = {"content-encoding", "content-length", "transfer-encoding", "connection"}
    # 4. Stream generator with strict cleanup
    async def stream_generator():
        try:
            async for chunk in resp.aiter_bytes():
                yield chunk
        finally:
            await resp.aclose()
            await client.aclose()

    response = StreamingResponse(
        stream_generator(),
        status_code=resp.status_code
    )
    response.raw_headers = [
        (name, value)
        for name, value in resp.headers.raw
        if name.lower() not in excluded_headers
    ]
    return response

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ollama Cluster Router & Discovery Daemon")
    parser.add_argument(
        "--port", 
        type=int, 
        default=8000, 
        help="Port to run the API router on (default: 8000)"
    )
    parser.add_argument(
        "--max-concurrent-pings", 
        type=int, 
        default=50, 
        help="Maximum number of simultaneous network sockets to open."
    )
    parser.add_argument(
        "--no-scan", 
        action="store_true", 
        help="Run the router only, relying on the existing database without background scanning."
    )
    parser.add_argument(
        "--config", 
        type=str, 
        default="config.json", 
        help="Path to the JSON configuration file containing target subnets (default: config.json)"
    )
    
    args = parser.parse_args()
    
    # --- OVERRIDE THE GLOBAL CONFIG ---
    CONCURRENCY_LIMIT = args.max_concurrent_pings
    DISABLE_SCAN = args.no_scan  
    CONFIG_FILE = args.config
    
    log.info(f"[*] Starting router on port {args.port} using config '{CONFIG_FILE}' with max concurrency {CONCURRENCY_LIMIT}...")
    uvicorn.run(app, host="0.0.0.0", port=args.port)