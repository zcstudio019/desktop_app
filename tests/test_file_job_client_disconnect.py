from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_backend_handles_client_disconnect_during_form_read() -> None:
    source = (ROOT / "backend/routers/file.py").read_text(encoding="utf-8")

    assert "from starlette.requests import ClientDisconnect" in source
    assert "except ClientDisconnect" in source
    assert "client_disconnected_during_form_read" in source
    assert "status_code=499" in source
    assert "客户端在文件上传过程中断开连接，请重新上传" in source


def test_frontend_file_job_create_timeout_is_120_seconds() -> None:
    source = (ROOT / "src/services/api.ts").read_text(encoding="utf-8")

    assert "FILE_JOB_CREATE_TIMEOUT_MS = 120000" in source
    assert "}, 15000)" not in source
    assert "文件上传超时，请检查网络或稍后重试" in source


def test_upload_page_uses_upload_and_background_processing_copy() -> None:
    source = (ROOT / "src/components/UploadPage.tsx").read_text(encoding="utf-8")

    assert "文件正在上传并创建处理任务，请稍候..." in source
    assert "文件已上传，正在后台 OCR/提取..." in source
