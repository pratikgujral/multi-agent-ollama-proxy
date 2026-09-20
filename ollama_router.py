"""
Prerequisites:
pip install fastapi uvicorn httpx tqdm
"""
import argparse
import asyncio
import os
import sqlite3
import ipaddress
import json
import uvicorn
import logging
from logging.handlers import RotatingFileHandler
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
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

def get_target_subnets():
    """Reads the latest subnets from the JSON configuration file."""
    if not os.path.exists(CONFIG_FILE):
        log.warning(f"[-] Config file {CONFIG_FILE} not found. Skipping scan.")
        return []
        
    try:
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
            return config.get("subnets", [])
    except json.JSONDecodeError:
        log.error("[-] Invalid JSON in config file. Skipping scan.")
        return []
    
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
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            for ip in active_ips:
                try:
                    resp = await client.get(f"http://{ip}:11434/api/tags")
                    if resp.status_code == 200:
                        models = resp.json().get("models", [])
                        c.execute("INSERT OR REPLACE INTO nodes (ip, last_seen) VALUES (?, datetime('now'))", (ip,))
                        c.execute("DELETE FROM models WHERE node_ip = ?", (ip,))
                        for m in models:
                            c.execute("INSERT INTO models (node_ip, model_name) VALUES (?, ?)", (ip, m["name"]))
                        
                        valid_nodes.append(ip)
                        log.info(f"[+] Node {ip} registered with {len(models)} models.")
                except httpx.RequestError:
                    pass
            
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
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            c.execute("SELECT node_ip FROM models WHERE model_name = ? ORDER BY RANDOM() LIMIT 1", (model_req,))
            row = c.fetchone()
            
        if not row:
            log.warning(f"[-] Blocked request: Model '{model_req}' not found on cluster.")
            raise HTTPException(status_code=404, detail=f"Model '{model_req}' not found")
        node_ip = row[0]
        
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
    except Exception as e:
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"Target node unreachable: {str(e)}")

    # Filter out hop-by-hop headers that FastAPI will automatically handle
    excluded_headers = {"content-encoding", "content-length", "transfer-encoding", "connection"}
    resp_headers = {k: v for k, v in resp.headers.items() if k.lower() not in excluded_headers}

    # 4. Stream generator with strict cleanup
    async def stream_generator():
        try:
            async for chunk in resp.aiter_bytes():
                yield chunk
        finally:
            await resp.aclose()
            await client.aclose()

    return StreamingResponse(
        stream_generator(),
        status_code=resp.status_code,
        headers=resp_headers
    )

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