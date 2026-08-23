"""
Prerequisites:
pip install fastapi uvicorn httpx tqdm
"""
import argparse
import asyncio
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
SUBNETS = ["192.168.1.0/24"]
DB_FILE = "ollama_cluster.db"
SCAN_INTERVAL = 300  
CONCURRENCY_LIMIT = 1000

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

async def execute_scan_cycle():
    """Performs the actual network sweep and database update."""
    log.info(f"[*] Scanning subnets: {', '.join(SUBNETS)}...")
    
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async def bounded_check(ip):
        async with sem:
            # Introduce a micro-delay to throttle overall velocity 
            # and evade basic rate-limiting detection.
            await asyncio.sleep(0.01)
            return await check_port(ip)
            
    all_hosts = []
    for subnet in SUBNETS:
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
    task = asyncio.create_task(scan_network_loop())
    yield
    task.cancel()

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

@app.post("/{path:path}")
async def proxy_ollama(path: str, request: Request):
    body = await request.body()
    try:
        payload = json.loads(body)
        model_req = payload.get("model")
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    
    if not model_req:
        raise HTTPException(status_code=400, detail="Missing 'model' parameter in payload")
    
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT node_ip FROM models WHERE model_name = ? ORDER BY RANDOM() LIMIT 1", (model_req,))
        row = c.fetchone()
        
    if not row:
        log.warning(f"[-] Blocked request: Model '{model_req}' not found on cluster.")
        raise HTTPException(status_code=404, detail=f"Model '{model_req}' not found")
        
    target_url = f"http://{row[0]}:11434/{path}"
    log.info(f"[~] Routing request for '{model_req}' -> {row[0]}")
    
    async def stream_proxy():
        async with httpx.AsyncClient() as client:
            proxy_headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length")}
            req = client.build_request(method=request.method, url=target_url, content=body, headers=proxy_headers)
            async with client.stream(req.method, req.url, content=req.content, headers=req.headers) as resp:
                async for chunk in resp.aiter_bytes():
                    yield chunk

    return StreamingResponse(stream_proxy())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ollama Cluster Router & Discovery Daemon")
    parser.add_argument(
        "--subnets", 
        nargs="+", 
        required=True, 
        help="List of CIDR subnets to scan (e.g., 10.237.0.0/16 10.208.0.0/16)"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=8000, 
        help="Port to run the API router on (default: 8000)"
    )
    # --- NEW ARGUMENT ---
    parser.add_argument(
        "--max-concurrent-pings", 
        type=int, 
        default=500, 
        help="Maximum number of simultaneous network sockets to open. Lower this to avoid triggering security alerts (default: 500)"
    )
    # --------------------
    
    args = parser.parse_args()
    
    SUBNETS.clear()
    SUBNETS.extend(args.subnets)
    
    # --- OVERRIDE THE GLOBAL CONFIG ---
    CONCURRENCY_LIMIT = args.max_concurrent_pings
    
    log.info(f"[*] Starting router on port {args.port} with max concurrency of {CONCURRENCY_LIMIT}...")
    uvicorn.run(app, host="0.0.0.0", port=args.port)