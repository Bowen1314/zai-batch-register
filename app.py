import asyncio
import os
import secrets
import string
import time
import csv
import io
import re
import json
import base64
import binascii
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from pydantic import BaseModel, ValidationError

from config_manager import load_config, save_config, load_tasks, save_tasks
from email_service import EmailService
from register_worker import RegisterWorker
from config_schema import AppConfig, BatchStartRequest
from http_client import request_json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

app = FastAPI(title="Z.ai Batch Register WebUI")
templates = Jinja2Templates(directory=TEMPLATES_DIR)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# In-memory state
config = load_config()
tasks: List[Dict[str, Any]] = load_tasks()

# Automatically recover any tasks left in RUNNING/VERIFYING/QUEUED state from a previous restart
recovered_tasks = False
for t in tasks:
    if t.get("status") in ["RUNNING", "VERIFYING", "WAITING_CAPTCHA", "QUEUED_CAPTCHA", "QUEUED", "WAITING_EMAIL"]:
        t["status"] = "PENDING"
        t["message"] = "未完成，已自动重置为待执行"
        recovered_tasks = True
if recovered_tasks:
    save_tasks(tasks)

active_websockets: List[WebSocket] = []
active_cdp_sessions: Dict[str, Any] = {}
is_running_batch = False
stop_requested = False
save_lock = asyncio.Lock()
captcha_lock = asyncio.Lock()
latest_captcha_frames: Dict[str, str] = {}
batch_queue_task: Optional[asyncio.Task] = None
scheduled_task_ids: set[str] = set()
active_task_ids: set[str] = set()
running_concurrency = 0
ACTIVE_STATUSES = {"RUNNING", "VERIFYING", "WAITING_CAPTCHA", "QUEUED_CAPTCHA", "QUEUED", "WAITING_EMAIL"}


def validated_config(data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return AppConfig.model_validate(data).model_dump()
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors(include_input=False, include_context=False)) from exc


def ensure_batch_running(concurrency: int) -> bool:
    global batch_queue_task, is_running_batch, stop_requested, running_concurrency
    if batch_queue_task is not None and not batch_queue_task.done():
        stop_requested = False
        return False
    # Reserve the queue before yielding, so concurrent requests reuse its handle.
    stop_requested = False
    is_running_batch = True
    running_concurrency = concurrency
    batch_queue_task = asyncio.create_task(run_batch_queue(concurrency))
    return True

def get_email_service() -> EmailService:
    cfg = load_config()
    return EmailService(
        host=cfg.get("imap_host", ""),
        port=int(cfg.get("imap_port", 993)),
        ssl=bool(cfg.get("imap_ssl", True)),
        username=cfg.get("imap_user", ""),
        password=cfg.get("imap_pass", ""),
        folder=cfg.get("imap_folder", "INBOX")
    )

def cdp_callback(task_id: str, action: str, data: Any = None):
    if action == "START":
        active_cdp_sessions[task_id] = data
        t = next((x for x in tasks if x["id"] == task_id), None)
        email = t.get("email", "") if t else ""
        asyncio.create_task(broadcast("CAPTCHA_START", {"task_id": task_id, "email": email}))
    elif action == "FRAME":
        latest_captcha_frames[task_id] = data
        asyncio.create_task(broadcast("CAPTCHA_FRAME", {"task_id": task_id, "image": data}))
    elif action == "STOP":
        active_cdp_sessions.pop(task_id, None)
        latest_captcha_frames.pop(task_id, None)
        asyncio.create_task(broadcast("CAPTCHA_STOP", {"task_id": task_id}))

async def broadcast(event_type: str, data: Any):
    payload = {"type": event_type, "data": data, "timestamp": time.time()}
    disconnected = []
    for ws in active_websockets:
        try:
            await ws.send_json(payload)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        if ws in active_websockets:
            active_websockets.remove(ws)

def sync_log_callback(task_id: str, message: str, level: str):
    asyncio.create_task(broadcast("LOG", {
        "task_id": task_id,
        "message": message,
        "level": level,
        "time": time.strftime("%H:%M:%S")
    }))

def on_task_status_change(task: Dict[str, Any]):
    save_tasks(tasks)
    asyncio.create_task(broadcast("TASK_UPDATED", task))

def generate_random_password(length: int = 14) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    while True:
        pwd = "".join(secrets.choice(alphabet) for _ in range(length))
        if (any(c.islower() for c in pwd)
            and any(c.isupper() for c in pwd)
            and any(c.isdigit() for c in pwd)
            and any(c in "!@#$%^&*" for c in pwd)):
            return pwd

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_websockets.append(websocket)
    try:
        await websocket.send_json({"type": "INIT", "tasks": tasks, "config": get_masked_config(), "running": is_running_batch, "concurrency": running_concurrency})

        # Synchronize a new viewer without replacing objects held by active workers.
        for tid in list(active_cdp_sessions.keys()):
            t = next((x for x in tasks if x["id"] == tid), None)
            email = t.get("email", "") if t else ""
            await websocket.send_json({"type": "CAPTCHA_START", "data": {"task_id": tid, "email": email}})
            if tid in latest_captcha_frames:
                await websocket.send_json({"type": "CAPTCHA_FRAME", "data": {"task_id": tid, "image": latest_captcha_frames[tid]}})

        while True:
            text = await websocket.receive_text()
            try:
                import json
                msg = json.loads(text)
                if msg.get("type") == "REMOTE_MOUSE":
                    task_id = msg.get("task_id")
                    cdp = active_cdp_sessions.get(task_id)
                    if cdp:
                        event_type = msg.get("event")  # mousePressed, mouseMoved, mouseReleased
                        x = int(msg.get("x", 0))
                        y = int(msg.get("y", 0))
                        btn = msg.get("button", "left")
                        
                        payload = {"type": event_type, "x": x, "y": y}
                        if event_type == "mousePressed":
                            payload["button"] = btn
                            payload["clickCount"] = 1
                            payload["buttons"] = 1
                        elif event_type == "mouseReleased":
                            payload["button"] = btn
                            payload["buttons"] = 0
                        elif event_type == "mouseMoved":
                            if msg.get("is_down"):
                                payload["buttons"] = 1
                                payload["button"] = "left"
                        await cdp.send("Input.dispatchMouseEvent", payload)
            except Exception:
                pass
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in active_websockets:
            active_websockets.remove(websocket)

@app.get("/", response_class=HTMLResponse)
async def index_page(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.get("/verify", response_class=HTMLResponse)
async def verify_page(request: Request):
    return templates.TemplateResponse(request=request, name="verify.html")

@app.get("/api/config")
async def get_config_api():
    return get_masked_config()

@app.post("/api/config")
async def update_config_api(data: Dict[str, Any] = Body(...)):
    global config
    current = load_config()
    if data.get("imap_pass") == "******":
        data["imap_pass"] = current.get("imap_pass", "")
    if data.get("proxy_password") == "******":
        data["proxy_password"] = current.get("proxy_password", "")
    save_config(validated_config({**current, **data}))
    config = load_config()
    await broadcast("CONFIG_UPDATED", get_masked_config())
    return {"success": True, "config": get_masked_config()}

def get_masked_config():
    cfg = load_config()
    m = cfg.copy()
    if m.get("imap_pass"):
        m["imap_pass"] = "******"
    if m.get("proxy_password"):
        m["proxy_password"] = "******"
    return m

@app.post("/api/config/test-proxy")
async def test_proxy_api(data: Dict[str, Any] = Body(...)):
    import requests
    server = data.get("proxy_server", "").strip()
    if not server:
        return {"success": False, "message": "请提供代理服务器地址 (例如: http://1.2.3.4:8080 或 socks5://1.2.3.4:1080)"}
    
    current = load_config()
    pwd = data.get("proxy_password")
    if pwd == "******" or not pwd:
        pwd = current.get("proxy_password", "")
    username = data.get("proxy_username", "").strip()
    
    parsed_proxy = server
    if username and pwd and "@" not in server:
        parts = server.split("://", 1)
        if len(parts) == 2:
            parsed_proxy = f"{parts[0]}://{username}:{pwd}@{parts[1]}"
        else:
            parsed_proxy = f"http://{username}:{pwd}@{server}"
            
    proxies = {
        "http": parsed_proxy,
        "https": parsed_proxy
    }
    
    def run_test():
        urls = ["https://api.ipify.org?format=json", "https://httpbin.org/ip", "https://ipinfo.io/json"]
        last_err = None
        for u in urls:
            try:
                r = requests.get(u, proxies=proxies, timeout=10)
                if r.status_code == 200:
                    resp_data = r.json()
                    ip = resp_data.get("ip") or resp_data.get("origin")
                    return {"success": True, "ip": ip, "message": f"代理测试连通成功！当前出口 IP 为: {ip}"}
            except Exception as e:
                last_err = e
        return {"success": False, "message": f"代理连接失败: {str(last_err)}"}
        
    res = await asyncio.to_thread(run_test)
    return res

@app.get("/api/config/disabled-proxies")
async def get_disabled_proxies_api():
    from proxy_manager import get_disabled_proxies
    return {"disabled": list(get_disabled_proxies().values())}

@app.post("/api/config/reset-proxies")
async def reset_proxies_api():
    from proxy_manager import reset_disabled_proxies
    reset_disabled_proxies()
    return {"success": True, "message": "已重置所有被禁用的代理，全部恢复为可用状态"}

@app.post("/api/config/test-proxies")
async def test_proxies_api(data: Dict[str, Any] = Body(...)):
    from proxy_manager import test_proxies_async
    raw_text = data.get("proxy_pool", "") or ""
    lines = [l.strip() for l in raw_text.splitlines() if l.strip() and not l.strip().startswith("#")]
    if not lines and data.get("proxy_server"):
        lines.append(data["proxy_server"].strip())
    if not lines:
        return {"success": False, "message": "未输入任何有效代理地址，请在文本框中每行填写一个代理"}
    
    results = await test_proxies_async(lines)
    success_count = sum(1 for r in results if r.get("success"))
    return {
        "success": True,
        "total": len(results),
        "available": success_count,
        "results": results
    }

@app.post("/api/config/test-email")
async def test_email_api(data: Dict[str, Any] = Body(...)):
    current = load_config()
    pwd = data.get("imap_pass")
    if pwd == "******" or not pwd:
        pwd = current.get("imap_pass", "")
    
    svc = EmailService(
        host=data.get("imap_host", ""),
        port=int(data.get("imap_port", 993)),
        ssl=bool(data.get("imap_ssl", True)),
        username=data.get("imap_user", ""),
        password=pwd,
        folder=data.get("imap_folder", "INBOX")
    )
    res = await asyncio.to_thread(svc.test_connection)
    return res

@app.get("/api/tasks")
async def get_tasks_api():
    return tasks

class BatchAddRequest(BaseModel):
    batch_text: str
    default_password: Optional[str] = None
    auto_start: Optional[bool] = False

@app.post("/api/tasks/add")
async def add_tasks_api(req: BatchAddRequest):
    cfg = validated_config(load_config())
    lines = [line.strip() for line in req.batch_text.splitlines() if line.strip()]
    added = []
    existing_emails = {t["email"].lower() for t in tasks}
    
    for line in lines:
        line_str = line.strip()
        if not line_str:
            continue
            
        parts = [p.strip() for p in line_str.replace("，", ",").split(",")]
        name = ""
        email = ""
        pwd = req.default_password or cfg["default_password"]
        
        if len(parts) == 1:
            raw = parts[0]
            if "@" in raw:
                email = raw
                name = email.split("@")[0]
            else:
                # 用户仅输入英文名字，例如 "Alex" 或 "David Miller"
                name = raw
                clean_name = re.sub(r'[^a-zA-Z0-9_\-\.]', '', name.lower().replace(" ", "_"))
                if not clean_name:
                    clean_name = f"user_{secrets.token_hex(3)}"
                email = f"verify+{clean_name}@gotclass.xyz"
        elif len(parts) == 2:
            if "@" in parts[1]:
                name, email = parts[0], parts[1]
            elif "@" in parts[0]:
                email, name = parts[0], parts[1]
            else:
                name = parts[0]
                clean_name = re.sub(r'[^a-zA-Z0-9_\-\.]', '', name.lower().replace(" ", "_"))
                email = f"verify+{clean_name}@gotclass.xyz"
                pwd = parts[1]
        elif len(parts) >= 3:
            name, email, pwd = parts[0], parts[1], parts[2]
            if "@" not in email:
                clean_name = re.sub(r'[^a-zA-Z0-9_\-\.]', '', email.lower().replace(" ", "_"))
                email = f"verify+{clean_name}@gotclass.xyz"

        # 确保邮箱唯一（若重复则自动附加短随机后缀）
        base_email = email
        dup_counter = 1
        while email.lower() in existing_emails:
            name_part, domain_part = base_email.split("@")
            email = f"{name_part}_{dup_counter}@{domain_part}"
            dup_counter += 1
        existing_emails.add(email.lower())
            
        if not pwd:
            pwd = generate_random_password()
            
        task_id = f"task_{secrets.token_hex(4)}"
        task = {
            "id": task_id,
            "name": name,
            "email": email,
            "password": pwd,
            "status": "PENDING",
            "message": "等待执行",
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        tasks.append(task)
        added.append(task)
        
    save_tasks(tasks)
    await broadcast("TASKS_ADDED", added)

    # 若勾选了自动启动
    if req.auto_start and added:
        scheduled_task_ids.update(t["id"] for t in added)
        ensure_batch_running(cfg["concurrency"])

    return {"success": True, "count": len(added), "auto_started": bool(req.auto_start)}

@app.post("/api/tasks/clear")
async def clear_tasks_api(clear_type: str = Body(default="all")):
    global tasks
    if clear_type != "completed" and active_task_ids:
        raise HTTPException(status_code=409, detail="正在执行任务，请暂停并等待当前任务结束后清空")
    if clear_type == "completed":
        tasks = [t for t in tasks if t.get("status") not in ["SUCCESS", "FAILED"]]
    else:
        scheduled_task_ids.clear()
        tasks = []
    save_tasks(tasks)
    await broadcast("TASKS_CLEARED", {"tasks": tasks})
    return {"success": True, "tasks": tasks}

@app.delete("/api/tasks/{task_id}")
async def delete_task_api(task_id: str):
    global tasks
    if task_id in active_task_ids:
        raise HTTPException(status_code=409, detail="任务正在执行，暂不能删除")
    scheduled_task_ids.discard(task_id)
    tasks = [t for t in tasks if t["id"] != task_id]
    save_tasks(tasks)
    await broadcast("TASK_DELETED", {"task_id": task_id})
    return {"success": True}

async def run_batch_queue(concurrency: int = 2):
    global is_running_batch, batch_queue_task, running_concurrency
    
    async def worker_loop(worker_idx: int):
        while not stop_requested:
            task_to_run = None
            async with save_lock:
                for t in tasks:
                    if t["id"] in scheduled_task_ids and t["id"] not in active_task_ids and t["status"] == "PENDING":
                        scheduled_task_ids.discard(t["id"])
                        active_task_ids.add(t["id"])
                        t["status"] = "QUEUED"
                        t["message"] = f"进入并发通道 {worker_idx + 1} 准备执行..."
                        task_to_run = t
                        break
            
            if not task_to_run:
                # 检查是否还有其他通道正在执行未结束的任务
                if not active_task_ids:
                    break
                await asyncio.sleep(0.05)
                continue

            try:
                save_tasks(tasks)
                await broadcast("TASK_UPDATED", task_to_run)
                worker = RegisterWorker(cfg, email_svc, log_callback=sync_log_callback, cdp_callback=cdp_callback, captcha_lock=captcha_lock)
                await worker.execute_task(task_to_run, on_status_change=on_task_status_change)
            except Exception as e:
                task_to_run["status"] = "FAILED"
                task_to_run["message"] = f"执行异常: {e}"
                on_task_status_change(task_to_run)
                sync_log_callback(task_to_run["id"], f"通道执行异常: {e}", "error")
            finally:
                active_task_ids.discard(task_to_run["id"])

    workers = []
    completed = False
    try:
        cfg = validated_config(load_config())
        email_svc = get_email_service()
        await broadcast("BATCH_STATUS", {"running": True, "concurrency": concurrency})
        workers = [asyncio.create_task(worker_loop(i)) for i in range(concurrency)]
        await asyncio.gather(*workers)
        completed = True
    finally:
        for worker_task in workers:
            if not worker_task.done():
                worker_task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        is_running_batch = False
        batch_queue_task = None
        running_concurrency = 0
        scheduled_task_ids.intersection_update(t["id"] for t in tasks if t["status"] == "PENDING")
        # A request can enqueue work as the last worker exits. Do not strand it.
        if completed and scheduled_task_ids and not stop_requested:
            ensure_batch_running(concurrency)
        await broadcast("BATCH_STATUS", {"running": is_running_batch, "concurrency": running_concurrency})
        sync_log_callback("SYSTEM", "批量注册流水线已结束/暂停", "info")

@app.post("/api/tasks/start")
async def start_batch_api(data: Optional[BatchStartRequest] = None):
    cfg = validated_config(load_config())
    concurrency = data.concurrency if data and data.concurrency is not None else cfg["concurrency"]
    if cfg.get("concurrency") != concurrency:
        cfg["concurrency"] = concurrency
        save_config(cfg)

    for t in tasks:
        if t["status"] != "SUCCESS" and t["status"] not in ACTIVE_STATUSES and t["id"] not in active_task_ids:
            t["status"] = "PENDING"
            t["message"] = "等待执行"
            scheduled_task_ids.add(t["id"])
    if scheduled_task_ids:
        ensure_batch_running(concurrency)
    save_tasks(tasks)
    await broadcast("TASKS_LOADED", tasks)

    return {"success": True, "concurrency": running_concurrency or concurrency}

@app.post("/api/tasks/pause")
async def pause_batch_api():
    global stop_requested
    stop_requested = True
    return {"success": True, "message": "已发送暂停指令，当前账号处理完成后将停止"}

@app.post("/api/tasks/{task_id}/retry")
async def retry_task_api(task_id: str):
    target = next((t for t in tasks if t["id"] == task_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Task not found")
        
    if task_id in active_task_ids or task_id in scheduled_task_ids or target["status"] in ACTIVE_STATUSES:
        raise HTTPException(status_code=409, detail="任务已在队列中或正在执行")
    cfg = validated_config(load_config())
    target["status"] = "PENDING"
    target["message"] = "准备重试"
    save_tasks(tasks)
    scheduled_task_ids.add(task_id)
    ensure_batch_running(cfg["concurrency"])
    await broadcast("TASK_UPDATED", target)
    return {"success": True}

@app.post("/api/tasks/{task_id}/manual-verify")
async def manual_verify_api(task_id: str, data: Dict[str, str] = Body(...)):
    verify_url = data.get("verify_url", "").strip()
    if not verify_url:
        raise HTTPException(status_code=400, detail="请提供验证链接或Token")
        
    target = next((t for t in tasks if t["id"] == task_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Task not found")
        
    cfg = load_config()
    worker = RegisterWorker(cfg, None, log_callback=sync_log_callback)
    res = await worker.verify_token_manually(verify_url, password=target.get("password", ""))
    
    if res["success"]:
        target["status"] = "SUCCESS"
        target["message"] = "已手动激活完成并提取凭证！"
        target["verify_url"] = verify_url
        target["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        if res.get("web_jwt"):
            target["web_jwt"] = res["web_jwt"]
            target["jwt"] = res["web_jwt"]
        if res.get("zcode_data"):
            zd = res["zcode_data"]
            target["jwt"] = zd.get("cli_jwt")
            target["cli_jwt"] = zd.get("cli_jwt")
            target["device_mid"] = zd.get("device_mid")
            target["sub"] = zd.get("sub")
            target["access_token"] = zd.get("access_token")
    else:
        target["message"] = f"手动激活失败: {res['message']}"
        
    save_tasks(tasks)
    await broadcast("TASK_UPDATED", target)
    return res

@app.post("/api/tasks/{task_id}/extract-jwt")
async def extract_jwt_api(task_id: str):
    target = next((t for t in tasks if t["id"] == task_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Task not found")
        
    cfg = load_config()
    proxy_cfg = None
    if cfg.get("proxy_enabled") and cfg.get("proxy_server"):
        proxy_server = cfg["proxy_server"].strip()
        proxy_cfg = {"server": proxy_server}
        if cfg.get("proxy_username"):
            proxy_cfg["username"] = cfg["proxy_username"].strip()
        if cfg.get("proxy_password"):
            proxy_cfg["password"] = cfg["proxy_password"]

    from zcode_token import exchange_web_jwt_to_zcode_cli, parse_jwt_sub, generate_device_mid

    # 1. 优先尝试通过已有的 web_jwt 极速换取官方 ZCode CLI 凭据 (无须人机验证，3秒静默完成)
    if target.get("web_jwt") and not target["web_jwt"].startswith("guest-"):
        sync_log_callback(task_id, "正在通过现有 Web 会话极速换取官方 ZCode Coding Plan JWT...", "info")
        zcode_res = await exchange_web_jwt_to_zcode_cli(target["web_jwt"], proxy_cfg)
        if zcode_res and zcode_res.get("cli_jwt"):
            cli_jwt = zcode_res["cli_jwt"]
            sub = zcode_res.get("sub") or parse_jwt_sub(cli_jwt)
            device_mid = zcode_res.get("device_mid") or generate_device_mid(sub or cli_jwt)
            target["jwt"] = cli_jwt
            target["cli_jwt"] = cli_jwt
            target["device_mid"] = device_mid
            target["sub"] = sub
            target["message"] = "🎉 已成功提取官方 ZCode Coding Plan JWT 凭据！"
            save_tasks(tasks)
            await broadcast("TASK_UPDATED", target)
            return {"success": True, "message": "官方 ZCode JWT 提取成功！", "jwt": cli_jwt, "device_mid": device_mid}

    # 2. 若无有效 web_jwt 或换取失败，则启动静默后台浏览器执行完整登录并提取
    sync_log_callback(task_id, "正在启动静默浏览器登录流程提取凭据...", "info")
    worker = RegisterWorker(cfg, None, log_callback=sync_log_callback, cdp_callback=cdp_callback)
    res = await worker.login_and_extract_jwt(target["email"], target["password"], task_id=task_id)
    if res.get("success"):
        if res.get("web_jwt"):
            target["web_jwt"] = res["web_jwt"]
        if res.get("cli_jwt"):
            target["jwt"] = res["cli_jwt"]
            target["cli_jwt"] = res["cli_jwt"]
            target["device_mid"] = res.get("device_mid", "")
            target["sub"] = res.get("sub", "")
            target["access_token"] = res.get("access_token", "")
        elif res.get("web_jwt"):
            target["jwt"] = res["web_jwt"]
        target["message"] = "🎉 已重新登录并成功提取官方最新 JWT！"
        save_tasks(tasks)
        await broadcast("TASK_UPDATED", target)
        return {"success": True, "message": "JWT 提取成功！", "jwt": target.get("jwt"), "device_mid": target.get("device_mid")}
    else:
        return {"success": False, "message": f"提取失败: {res.get('message')}"}

@app.post("/api/tasks/{task_id}/save-jwt")
async def save_jwt_api(task_id: str, data: Dict[str, str] = Body(...)):
    target = next((t for t in tasks if t["id"] == task_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Task not found")
        
    jwt_val = data.get("jwt", "").strip()
    if not jwt_val:
        raise HTTPException(status_code=400, detail="JWT 不能为空")
        
    from zcode_token import parse_jwt_sub, generate_device_mid
    sub = parse_jwt_sub(jwt_val)
    device_mid = generate_device_mid(sub or jwt_val)
    
    target["jwt"] = jwt_val
    target["cli_jwt"] = jwt_val
    target["sub"] = sub
    target["device_mid"] = device_mid
    target["message"] = "已手动录入 JWT 凭证"
    
    save_tasks(tasks)
    await broadcast("TASK_UPDATED", target)
    return {"success": True, "message": "JWT 保存成功！"}

@app.post("/api/oauth-cli/start/{task_id}")
async def oauth_cli_start_api(task_id: str):
    target = next((t for t in tasks if t["id"] == task_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Task not found")
        
    poll_token = secrets.token_hex(32)
    try:
        res = await request_json(
            "POST", "https://zcode.z.ai/api/v1/oauth/cli/init",
            headers={"Authorization": f"Bearer {poll_token}"},
            json={"provider": "zai"}, timeout=10,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail="无法连接官方 OAuth 服务") from exc
    
    if not isinstance(res, dict) or res.get("code") != 0 or not isinstance(res.get("data"), dict):
        return {"success": False, "message": "向官方发起 OAuth 流程失败"}
        
    flow_id = res["data"].get("flow_id")
    authorize_url = res["data"].get("authorize_url")
    if not isinstance(flow_id, str) or not isinstance(authorize_url, str):
        raise HTTPException(status_code=502, detail="官方 OAuth 响应不完整")
    
    sync_log_callback(task_id, f"已生成官方 OAuth CLI 授权会话，正在等待授权: {target['email']}", "info")
    
    async def poll_flow():
        start_t = time.time()
        while time.time() - start_t < 300:
            await asyncio.sleep(2)
            try:
                p_res = await request_json(
                    "GET", f"https://zcode.z.ai/api/v1/oauth/cli/poll/{flow_id}",
                    headers={"Authorization": f"Bearer {poll_token}"},
                    timeout=10
                )
                p_data = p_res.get("data", {})
                if p_data.get("status") == "ready" and p_data.get("token"):
                    cli_jwt = p_data["token"]
                    user_info = p_data.get("user", {})
                    access_token = p_data.get("zai", {}).get("access_token", "")
                    
                    from zcode_token import parse_jwt_sub, generate_device_mid
                    sub = parse_jwt_sub(cli_jwt)
                    device_mid = generate_device_mid(sub or cli_jwt)
                    
                    target["jwt"] = cli_jwt
                    target["cli_jwt"] = cli_jwt
                    target["sub"] = sub
                    target["device_mid"] = device_mid
                    target["access_token"] = access_token
                    target["status"] = "SUCCESS"
                    target["message"] = f"🎉 官方 OAuth 授权成功！已绑定账号 {user_info.get('email', target['email'])}"
                    save_tasks(tasks)
                    await broadcast("TASK_UPDATED", target)
                    sync_log_callback(task_id, f"🎉 账号 {target['email']} 已成功提取官方 Coding Plan 专用 JWT！", "success")
                    break
            except Exception:
                pass
                
    asyncio.create_task(poll_flow())
    return {
        "success": True,
        "authorize_url": authorize_url,
        "flow_id": flow_id
    }

@app.get("/api/export-tokens")
async def export_tokens_txt():
    """Exports tokens.txt format compatible with D3-vin/Zcode2Api (one JWT per line)."""
    current_tasks = load_tasks()
    lines = []
    for t in current_tasks:
        jwt = t.get("cli_jwt") or t.get("jwt")
        if isinstance(jwt, str) and len(jwt.split(".")) == 3:
            try:
                # 严格过滤游客 guest token
                p = jwt.split(".")[1]
                p += "=" * (-len(p) % 4)
                data = json.loads(base64.urlsafe_b64decode(p))
                if not isinstance(data, dict) or str(data.get("email", "")).lower().startswith("guest-"):
                    continue
            except (ValueError, TypeError, binascii.Error):
                continue
            lines.append(jwt)
            
    content = "\n".join(lines)
    return Response(
        content=content,
        media_type="text/plain",
        headers={"Content-Disposition": "attachment; filename=tokens.txt"}
    )

@app.get("/api/export")
async def export_csv():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "姓名", "邮箱", "密码", "CLI_JWT(ZCode/桌面客户端)", "Web_JWT", "Device_MID(防3012)", "状态", "完成时间"])
    
    for t in tasks:
        writer.writerow([
            t.get("id"),
            t.get("name"),
            t.get("email"),
            t.get("password"),
            t.get("cli_jwt") or t.get("jwt", ""),
            t.get("web_jwt", ""),
            t.get("device_mid", ""),
            t.get("status"),
            t.get("completed_at", "")
        ])
        
    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=z_ai_registered_accounts.csv"}
    )
