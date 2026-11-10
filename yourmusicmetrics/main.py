import json
import sys
import time
from pathlib import Path
from threading import Lock

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import analyzer

app = FastAPI(title="Your Music Metrics")

MAX_BODY = 512 * 1024 * 1024
STATS_FILE = HERE.parent / "stats.json"
STATS_LOCK = Lock()


def _load_stats():
    if STATS_FILE.exists():
        try:
            return json.loads(STATS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"visits": 0, "uploads": 0, "files": 0, "zip_count": 0, "json_count": 0}


def _save_stats(stats):
    tmp = STATS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATS_FILE)


@app.middleware("http")
async def limit_body(request: Request, call_next):
    if request.method == "POST":
        cl = request.headers.get("content-length", "")
        if cl.isdigit() and int(cl) > MAX_BODY:
            return JSONResponse(
                {"detail": "файл больше 512 МБ"}, status_code=413)
    response = await call_next(request)
    return response


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    with STATS_LOCK:
        stats = _load_stats()
        stats["visits"] = stats.get("visits", 0) + 1
        stats["last_visit"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _save_stats(stats)
    return (HERE / "static" / "index.html").read_text(encoding="utf-8")


@app.post("/analyze")
async def analyze(request: Request, files: list[UploadFile] = File(...), fmt: str = "html"):
    if not files:
        raise HTTPException(status_code=400, detail="файлы не переданы")
    zip_count = sum(1 for f in files if (f.filename or "").lower().endswith(".zip"))
    json_count = sum(1 for f in files if (f.filename or "").lower().endswith(".json"))
    uploads = [(await f.read(), f.filename or "") for f in files]

    if fmt == "json":
        try:
            events = analyzer.load_uploads(uploads)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        result = analyzer.analyze(events)
        if "error" in result:
            raise HTTPException(status_code=422, detail=result["error"])
        missing = int(result.get("missing", 0))
        note = result.get("note")
        with STATS_LOCK:
            stats = _load_stats()
            stats["uploads"] = stats.get("uploads", 0) + 1
            stats["files"] = stats.get("files", 0) + len(files)
            stats["zip_count"] = stats.get("zip_count", 0) + zip_count
            stats["json_count"] = stats.get("json_count", 0) + json_count
            stats["missing_total"] = stats.get("missing_total", 0) + missing
            if missing:
                stats["uploads_with_missing"] = stats.get("uploads_with_missing", 0) + 1
            if note:
                stats["last_missing_note"] = note
                stats["last_missing"] = missing
            stats["last_upload"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            _save_stats(stats)
        payload = dict(result)
        payload.pop("data", None)
        payload["data"] = result["data"]
        return JSONResponse(payload)

    def stream():
        def ev(stage, **kw):
            return json.dumps({"stage": stage, **kw},
                              ensure_ascii=False) + "\n"
        try:
            yield ev("parse")
            events = analyzer.load_uploads(uploads)
        except ValueError as e:
            yield ev("error", detail=str(e))
            return
        yield ev("race", frames=analyzer.race_frames(events, 120))
        yield ev("analyze")
        result = analyzer.analyze(events)
        if "error" in result:
            yield ev("error", detail=result["error"])
            return
        missing = int(result.get("missing", 0))
        note = result.get("note")
        with STATS_LOCK:
            stats = _load_stats()
            stats["uploads"] = stats.get("uploads", 0) + 1
            stats["files"] = stats.get("files", 0) + len(files)
            stats["zip_count"] = stats.get("zip_count", 0) + zip_count
            stats["json_count"] = stats.get("json_count", 0) + json_count
            stats["missing_total"] = stats.get("missing_total", 0) + missing
            if missing:
                stats["uploads_with_missing"] = stats.get("uploads_with_missing", 0) + 1
            if note:
                stats["last_missing_note"] = note
                stats["last_missing"] = missing
            stats["last_upload"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            _save_stats(stats)
        yield ev("render")
        html = analyzer.render_dashboard(result["data"])
        yield ev("done", html=html)

    return StreamingResponse(stream(), media_type="application/x-ndjson")