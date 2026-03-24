"""
FastAPI Backend for Loan Assistant

This is the main entry point for the FastAPI application.
It configures CORS middleware, includes all routers, provides
a health check endpoint, and handles global exceptions.

In production mode (when static/ directory exists), it also serves
the React frontend as static files.

Requirements:
- 6.2: All error responses SHALL have consistent structure with error field
- 6.3: Service errors SHALL be caught and returned with appropriate HTTP status codes
"""

import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request

from contextlib import asynccontextmanager
from sqlalchemy import text
from .database import engine

print("🚀 强制测试数据库连接...")

try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ MySQL 连接成功！")
except Exception as e:
    print("❌ MySQL 连接失败：", e)
# Configure logging - must be done before any logger is created
# 日志级别可通过环境变量 LOG_LEVEL 配置，默认 INFO
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Import service error classes for exception handlers
import sys

from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .models.schemas import HealthResponse
from .routers import application, auth, chat, customer, dashboard, feishu, file, scheme, wiki

# Add parent directory to path to import services
sys.path.insert(0, str(Path(__file__).parent.parent))
from services.ai_service import AIServiceError
from services.feishu_service import FeishuServiceError
from services.ocr_service import OCRServiceError
from services.wiki_service import WikiServiceError

# Configure logging
logger = logging.getLogger(__name__)

GENERIC_SERVER_ERROR_MESSAGE = "服务暂时不可用，请稍后重试。"
GENERIC_AI_ERROR_MESSAGE = "AI 服务暂时不可用，请稍后重试。"
GENERIC_OCR_ERROR_MESSAGE = "文件识别失败，请检查文件内容是否清晰完整。"
GENERIC_FEISHU_ERROR_MESSAGE = "外部服务暂时不可用，请稍后重试。"
GENERIC_WIKI_ERROR_MESSAGE = "产品库服务暂时不可用，请稍后重试。"

# Create FastAPI application
app = FastAPI(
    title="Loan Assistant API",
    #description="REST API backend for the Loan Assistant application",
    version="0.1.0",
    #lifespan=lifespan
)

from .database import Base, engine

# 自动创建数据库表（连接RDS时会建表）
Base.metadata.create_all(bind=engine)

# Configure CORS middleware
# 桌面应用：限制为本地开发/生产地址
_allowed_origins = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173,http://localhost:5174,http://127.0.0.1:5174,http://localhost:8000,http://127.0.0.1:8000",
).split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
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


# ==================== Static File Serving (Production) ====================
# In production, serve the React frontend from the static/ directory

# Determine static directory path
# When running from build: backend/main.py -> app/backend/main.py, static is at app/static
# When running from source: backend/main.py -> desktop_app/backend/main.py, static is at desktop_app/static
_static_dir = Path(__file__).parent.parent / "static"

if _static_dir.exists() and (_static_dir / "index.html").exists():
    logger.info(f"Production mode: serving static files from {_static_dir}")

    # Mount assets directory for JS/CSS files
    _assets_dir = _static_dir / "assets"
    if _assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=_assets_dir), name="assets")

    # Serve other static files (favicon, etc.)
    @app.get("/favicon.ico")
    async def favicon():
        favicon_path = _static_dir / "favicon.ico"
        if favicon_path.exists():
            return FileResponse(favicon_path)
        raise HTTPException(status_code=404, detail="Favicon not found")

    @app.get("/vite.svg")
    async def vite_svg():
        svg_path = _static_dir / "vite.svg"
        if svg_path.exists():
            return FileResponse(svg_path)
        raise HTTPException(status_code=404, detail="File not found")
else:
    logger.info("Development mode: static files not found, API-only mode")


@app.get("/api/health", response_model=HealthResponse, tags=["Health"])
async def health_check() -> HealthResponse:
    """
    Health check endpoint.

    Returns the current status of the API service.
    Used for monitoring and deployment verification.
    """
    return HealthResponse(status="ok")


# ==================== Exception Handlers ====================
# Requirement 6.2: All error responses SHALL have consistent structure with error field
# Requirement 6.3: Service errors SHALL be caught and returned with appropriate HTTP status codes


@app.exception_handler(FeishuServiceError)
async def feishu_exception_handler(request: Request, exc: FeishuServiceError):
    """
    Handle Feishu service errors.

    Returns HTTP 500 for Feishu-related errors (authentication, API, network).

    Validates: Requirements 6.2, 6.3
    """
    logger.error(f"Feishu service error: {exc}")
    return JSONResponse(status_code=500, content={"error": GENERIC_FEISHU_ERROR_MESSAGE})


@app.exception_handler(OCRServiceError)
async def ocr_exception_handler(request: Request, exc: OCRServiceError):
    """
    Handle OCR service errors.

    Returns HTTP 400 for OCR-related errors (image issues, configuration).

    Validates: Requirements 6.2, 6.3
    """
    logger.error(f"OCR service error: {exc}")
    return JSONResponse(status_code=400, content={"error": GENERIC_OCR_ERROR_MESSAGE})


@app.exception_handler(AIServiceError)
async def ai_exception_handler(request: Request, exc: AIServiceError):
    """
    Handle AI service errors.

    Returns HTTP 500 for AI-related errors (API timeout, rate limit, connection).

    Validates: Requirements 6.2, 6.3
    """
    logger.error(f"AI service error: {exc}")
    return JSONResponse(status_code=500, content={"error": GENERIC_AI_ERROR_MESSAGE})


@app.exception_handler(WikiServiceError)
async def wiki_exception_handler(request: Request, exc: WikiServiceError):
    """
    Handle Wiki service errors.

    Returns HTTP 500 for Wiki-related errors (authentication, API, network).

    Validates: Requirements 6.2, 6.3
    """
    logger.error(f"Wiki service error: {exc}")
    return JSONResponse(status_code=500, content={"error": GENERIC_WIKI_ERROR_MESSAGE})


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Handle all unexpected errors.

    Returns HTTP 500 with a generic error message for security.
    Logs the full exception for debugging.

    Validates: Requirements 6.2, 6.3
    """
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"error": GENERIC_SERVER_ERROR_MESSAGE})


def _sanitize_http_error_detail(detail: object) -> str:
    """Return a user-safe HTTP error detail string."""
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
async def http_exception_handler(request: Request, exc: HTTPException):
    """
    Handle HTTPException to ensure consistent error response structure.

    Converts FastAPI's default {"detail": "..."} format to {"error": "..."}.

    Validates: Requirements 6.2
    """
    logger.error(f"HTTP exception: {exc.status_code} - {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": _sanitize_http_error_detail(exc.detail)},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Handle request validation errors.

    Returns HTTP 422 with validation error details in consistent format.

    Validates: Requirements 6.2, 6.4, 6.5
    """
    logger.error(f"Validation error: {exc.errors()}")
    # Format validation errors into a readable message
    error_messages = []
    for error in exc.errors():
        loc = " -> ".join(str(item) for item in error.get("loc", []))
        msg = error.get("msg", "Unknown error")
        error_messages.append(f"{loc}: {msg}")

    return JSONResponse(status_code=422, content={"error": f"Validation error: {'; '.join(error_messages)}"})


# ==================== SPA Catch-All Route (Production) ====================
# This must be at the very end, after all other routes
# It serves index.html for any non-API routes to support client-side routing

if _static_dir.exists() and (_static_dir / "index.html").exists():

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """
        Catch-all route for SPA.

        All API routes are handled above (prefixed with /api).
        This route serves index.html for all other paths,
        allowing React Router to handle client-side routing.

        Cache-Control: no-cache ensures browser always revalidates index.html,
        preventing stale cached versions from serving old JS bundles.
        """
        # Don't serve index.html for API routes (they should 404 if not found)
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="API endpoint not found")

        index_file = _static_dir / "index.html"
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

