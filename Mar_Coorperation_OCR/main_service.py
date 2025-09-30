# main_service.py
import os
import threading
import time
import subprocess
import shlex
from pathlib import Path
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging

# ========== Logging ==========
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG = LOG_DIR / "pipeline_run.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(RUN_LOG, encoding="utf-8"),
        logging.StreamHandler()
    ],
)

logger = logging.getLogger("main_service")

# ========== Globals / In-memory config & state ==========
ENV_PATH = Path(".env")  # the OCR script reads .env via pydantic BaseSettings

class ConfigModel(BaseModel):
    DB_SERVER: str
    DB_DRIVER: str = "ODBC Driver 17 for SQL Server"
    DB_USER: str
    DB_PASS: str
    DB_NAME: str
    pdf_folder: str
    temp_excel: str
    output_excel: str
    # optional extra settings
    extra: Optional[Dict[str, Any]] = None

app = FastAPI(title="OCR Controller Service")

_state = {
    "config": None,         # ConfigModel dict
    "run_thread": None,     # threading.Thread
    "process": None,        # subprocess.Popen
    "status": "idle",       # idle | running | error | completed
    "last_start": None,
    "last_end": None,
    "last_error": None,
    "pid": None,
}

# ========== Helpers ==========
def write_env_file(cfg: ConfigModel):
    """
    Write a .env file for Marathi_OCR_updated_v1.Settings to read.
    Keep variable names same as in your Settings dataclass.
    """
    lines = []
    mapping = {
        "DB_SERVER": cfg.DB_SERVER,
        "DB_DRIVER": cfg.DB_DRIVER,
        "DB_USER": cfg.DB_USER,
        "DB_PASS": cfg.DB_PASS,
        "DB_NAME": cfg.DB_NAME,
        "pdf_folder": cfg.pdf_folder,
        "temp_excel": cfg.temp_excel,
        "output_excel": cfg.output_excel,
    }
    for k, v in mapping.items():
        # ensure Windows-friendly paths preserved
        val = str(v).replace("\\", "\\\\")
        lines.append(f'{k}="{val}"')

    if cfg.extra:
        for k, v in cfg.extra.items():
            lines.append(f'{k}="{v}"')

    ENV_PATH.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Wrote .env with provided configuration")

def run_pipeline_subprocess(python_exe: str = "python", args: Optional[str] = None, logfile: Path = RUN_LOG):
    """
    Spawn the user's OCR script as subprocess. Keep stdout/stderr logged to file.
    Uses .env in current directory (which the script loads via pydantic BaseSettings).
    """
    # default: run the script (no 'api' arg) so it triggers main-run mode
    script = "Marathi_OCR_updated_v1.py"
    cmd = f'{python_exe} {shlex.quote(script)}'
    if args:
        cmd += f" {args}"

    logger.info(f"Starting pipeline subprocess: {cmd}")
    logfile_parent = logfile.parent
    logfile_parent.mkdir(parents=True, exist_ok=True)

    # open logfile for append to capture subprocess output
    with open(logfile, "ab") as out:
        # Note: we don't wait here; the wrapper thread will monitor
        proc = subprocess.Popen(
            shlex.split(cmd),
            stdout=out,
            stderr=subprocess.STDOUT,
            cwd=str(Path.cwd()),
            env=os.environ.copy()
        )
    return proc

def _background_run(python_exe: str = "python", args: Optional[str] = None):
    """
    Real worker run function executed inside a thread.
    It sets state, spawns subprocess, waits for it, and updates status.
    """
    try:
        _state["status"] = "running"
        _state["last_start"] = time.time()
        proc = run_pipeline_subprocess(python_exe=python_exe, args=args, logfile=RUN_LOG)
        _state["process"] = proc
        _state["pid"] = proc.pid
        logger.info(f"Pipeline subprocess started with PID {proc.pid}")

        # Wait until process finishes
        ret = proc.wait()
        if ret == 0:
            _state["status"] = "completed"
            logger.info("Pipeline subprocess completed successfully")
        else:
            _state["status"] = "error"
            _state["last_error"] = f"Process exited with code {ret}"
            logger.error(f"Pipeline subprocess exited with code {ret}")
    except Exception as exc:
        _state["status"] = "error"
        _state["last_error"] = str(exc)
        logger.exception("Background run exception")
    finally:
        _state["last_end"] = time.time()
        _state["process"] = None
        _state["pid"] = None
        _state["run_thread"] = None

# ========== API Endpoints ==========

@app.post("/config", summary="Save processing configuration (.env is written)")
def set_config(cfg: ConfigModel):
    """
    Persist config (in-memory) and write .env that the OCR script will read.
    """
    # validate paths quickly
    if not Path(cfg.pdf_folder).exists():
        raise HTTPException(status_code=400, detail="pdf_folder path does not exist on server")

    # store in memory
    _state["config"] = cfg.dict()
    write_env_file(cfg)
    logger.info("Configuration saved in memory and .env written")
    return {"status": "ok", "message": "Configuration saved and .env written to project root."}

@app.post("/process", summary="Start the OCR pipeline (background)")
def start_process(python_exe: Optional[str] = "python", args: Optional[str] = None):
    """
    Starts the pipeline in a background thread. The thread will spawn the real OCR script
    as a subprocess so the heavy job runs separately (so FastAPI remains responsive).

    - python_exe: path to python executable to run the script (default: "python")
    - args: optional command-line args to append (if your script supports them)
    """
    if _state["status"] == "running":
        raise HTTPException(status_code=409, detail="Pipeline already running")

    if not _state["config"]:
        raise HTTPException(status_code=400, detail="Configuration missing. POST /config first.")

    # start background thread
    thread = threading.Thread(target=_background_run, kwargs={"python_exe": python_exe, "args": args}, daemon=True)
    _state["run_thread"] = thread
    thread.start()
    logger.info("Background thread launched to run pipeline")
    return {"status": "started", "pid": _state.get("pid"), "message": "Pipeline started in background."}

@app.get("/status", summary="Get current run status")
def get_status():
    s = {
        "status": _state["status"],
        "last_start": _state["last_start"],
        "last_end": _state["last_end"],
        "pid": _state["pid"],
        "last_error": _state["last_error"],
        "config_present": bool(_state["config"]),
    }
    return s

@app.get("/logs", summary="Return the tail of run log")
def get_logs(lines: int = 400):
    """
    Returns last N lines of the run log to help debugging.
    """
    if not RUN_LOG.exists():
        return {"log": "", "message": "Log not created yet."}

    with open(RUN_LOG, "r", encoding="utf-8", errors="ignore") as f:
        all_lines = f.readlines()
    tail = all_lines[-lines:]
    return {"log": "".join(tail)}

@app.post("/stop", summary="Stop currently running pipeline (best-effort)")
def stop_pipeline():
    """
    Attempts to terminate the spawned subprocess. Best-effort only.
    """
    proc = _state.get("process")
    if not proc:
        raise HTTPException(status_code=400, detail="No pipeline subprocess running")

    proc.terminate()
    logger.info("Sent terminate signal to subprocess")
    return {"status": "stopping", "pid": proc.pid}

# ========== health ========
@app.get("/health")
def health():
    return {"ok": True, "service": "OCR Controller"}

# ========== Startup info ==========
@app.on_event("startup")
def startup_event():
    logger.info("OCR Controller service started. Visit /docs for API UI.")
