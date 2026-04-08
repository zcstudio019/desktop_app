"""FastAPI application entrypoint for local development and ECS deployment."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(
    level=getattr(logging, (os.getenv("LOG_LEVEL") or "INFO").upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.ai_service import AIServiceError
from services.feishu_service import FeishuServiceError
from services.ocr_service import OCRServiceError
from services.wiki_service import WikiServiceError

from .models.schemas import HealthResponse
from .routers import application, auth, chat, customer, dashboard, feishu, file, scheme, wiki

GENERIC_SERVER_ERROR_MESSAGE = "服务暂时不可用，请稍后重试。"
GENERIC_AI_ERROR_MESSAGE = "AI 服务暂时不可用，请稍后重试。"
GENERIC_OCR_ERROR_MESSAGE = "文件识别失败，请检查文件内容是否清晰完整。"
GENERIC_FEISHU_ERROR_MESSAGE = "外部服务暂时不可用，请稍后重试。"
GENERIC_WIKI_ERROR_MESSAGE = "产品库服务暂时不可用，请稍后重试。"


def _parse_cors_origins() -> list[str]:
    raw = (
        os.getenv("CORS_ORIGINS")
        or "http://localhost:5173,http://127.0.0.1:5173,http://localhost:5174,http://127.0.0.1:5174"
    )
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def _should_serve_static() -> bool:
    return (os.getenv("SERVE_STATIC_FILES") or "false").lower() == "true"


app = FastAPI(title="Loan Assistant API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(file.router, prefix="/api")
app.include_router(feishu.router, prefix="/api")
app.include_router(feishu.storage_router, prefix="/api")
app.include_router(application.router, prefix="/api")
app.include_router(scheme.router, prefix="/api")
app.include_router(chat.router, prefix="/api")
app.include_router(dashboard.router, prefix="/api")
app.include_router(wiki.router, prefix="/api")
app.include_router(auth.router, prefix="/api")
app.include_router(customer.router, prefix="/api")

static_dir = PROJECT_ROOT / "dist"

if _should_serve_static() and static_dir.exists() and (static_dir / "index.html").exists():
    logger.info("Serving built frontend from %s", static_dir)
    assets_dir = static_dir / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    @app.get("/favicon.ico")
    async def favicon():
        icon_path = static_dir / "favicon.ico"
        if icon_path.exists():
            return FileResponse(icon_path)
        raise HTTPException(status_code=404, detail="Favicon not found")

    @app.get("/vite.svg")
    async def vite_svg():
        svg_path = static_dir / "vite.svg"
        if svg_path.exists():
            return FileResponse(svg_path)
        raise HTTPException(status_code=404, detail="File not found")


@app.get("/api/health", response_model=HealthResponse, tags=["Health"])
async def health_check() -> HealthResponse:
    return HealthResponse(status="ok")


@app.exception_handler(FeishuServiceError)
async def feishu_exception_handler(_request: Request, exc: FeishuServiceError):
    logger.error("Feishu service error: %s", exc)
    return JSONResponse(status_code=500, content={"error": GENERIC_FEISHU_ERROR_MESSAGE})


@app.exception_handler(OCRServiceError)
async def ocr_exception_handler(_request: Request, exc: OCRServiceError):
    logger.error("OCR service error: %s", exc)
    return JSONResponse(status_code=400, content={"error": GENERIC_OCR_ERROR_MESSAGE})


@app.exception_handler(AIServiceError)
async def ai_exception_handler(_request: Request, exc: AIServiceError):
    logger.error("AI service error: %s", exc)
    return JSONResponse(status_code=500, content={"error": GENERIC_AI_ERROR_MESSAGE})


@app.exception_handler(WikiServiceError)
async def wiki_exception_handler(_request: Request, exc: WikiServiceError):
    logger.error("Wiki service error: %s", exc)
    return JSONResponse(status_code=500, content={"error": GENERIC_WIKI_ERROR_MESSAGE})


@app.exception_handler(Exception)
async def global_exception_handler(_request: Request, exc: Exception):
    logger.error("Unhandled exception: %s", exc, exc_info=True)
    return JSONResponse(status_code=500, content={"error": GENERIC_SERVER_ERROR_MESSAGE})


def _sanitize_http_error_detail(detail: object) -> str:
    message = str(detail)
    raw_provider_markers = [
        "AI service error:",
        "Feishu service error:",
        "OCR service error:",
        "Wiki service error:",
        "Internal server error:",
        "Chat processing error:",
        "invalid_request_error",
        "Error code:",
        "{'error':",
    ]
    if any(marker in message for marker in raw_provider_markers):
        if "OCR service error:" in message:
            return GENERIC_OCR_ERROR_MESSAGE
        if "Wiki service error:" in message:
            return GENERIC_WIKI_ERROR_MESSAGE
        if "Feishu service error:" in message:
            return GENERIC_FEISHU_ERROR_MESSAGE
        if "AI service error:" in message or "invalid_request_error" in message or "Error code:" in message:
            return GENERIC_AI_ERROR_MESSAGE
        return GENERIC_SERVER_ERROR_MESSAGE
    return message


@app.exception_handler(HTTPException)
async def http_exception_handler(_request: Request, exc: HTTPException):
    logger.error("HTTP exception: %s - %s", exc.status_code, exc.detail)
    return JSONResponse(status_code=exc.status_code, content={"error": _sanitize_http_error_detail(exc.detail)})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_request: Request, exc: RequestValidationError):
    logger.error("Validation error: %s", exc.errors())
    error_messages = []
    for error in exc.errors():
        loc = " -> ".join(str(item) for item in error.get("loc", []))
        msg = error.get("msg", "Unknown error")
        error_messages.append(f"{loc}: {msg}")
    return JSONResponse(status_code=422, content={"error": f"参数校验失败：{'; '.join(error_messages)}"})


if _should_serve_static() and static_dir.exists() and (static_dir / "index.html").exists():

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="API endpoint not found")

        index_file = static_dir / "index.html"
        if index_file.exists():
            return FileResponse(
                index_file,
                headers={
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )
        raise HTTPException(status_code=404, detail="Not found")
