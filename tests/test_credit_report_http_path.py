"""Actual TCP/HTTP path probe; storage and LLM are explicitly test fixtures.

This verifies local execution, never claims to verify a deployed instance or
real customer facts. CREDIT_REPORT_EXPECT_MARKER=1 is used only during the
temporary source-marker experiment.
"""
import json
import os
import runpy
import socket
import threading
import time
import urllib.request
from pathlib import Path

import uvicorn
from fastapi import FastAPI


def test_final_credit_report_over_real_http(monkeypatch, tmp_path):
    fixtures = runpy.run_path(str(Path(__file__).with_name("test_assistant_credit_one_page_report.py")))
    storage = fixtures["complete_storage"]()
    import backend.services as services
    import backend.services.sqlalchemy_storage_service as sql_storage
    monkeypatch.setattr(services, "get_storage_service", lambda: storage)
    monkeypatch.setattr(sql_storage, "SQLAlchemyStorageService", lambda: storage)
    from backend.routers import chat
    from backend.services import assistant_credit_report_service as reports
    from backend.services import credit_report_markdown_renderer as renderer
    monkeypatch.setattr(chat, "storage_service", storage)
    monkeypatch.setattr(chat, "ai_service", fixtures["FakeAI"]())

    calls = []
    # Observe the real functions without replacing their implementations/results.
    def observe(module, name):
        original = getattr(module, name)
        def traced(*args, **kwargs):
            event = {"file": original.__code__.co_filename, "function": original.__name__}
            if name == "render_markdown_table":
                assert isinstance(args[0], list)
                assert all(isinstance(header, str) for header in args[0])
                event["headers"] = args[0]
            calls.append(event)
            return original(*args, **kwargs)
        monkeypatch.setattr(module, name, traced)
    for name in ("build_credit_report_model", "build_report_context", "render_credit_one_page_report"):
        observe(reports, name)
    observe(renderer, "render_markdown_table")

    app = FastAPI()
    app.include_router(chat.router, prefix="/api")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    address = f"http://127.0.0.1:{listener.getsockname()[1]}/api/chat"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started and thread.is_alive() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert server.started
        body = {"messages": [{"role": "user", "content": "根据上海意川建筑科技有限公司的资料生成征信一页纸"}]}
        request = urllib.request.Request(address, data=json.dumps(body, ensure_ascii=False).encode("utf-8"), headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(request, timeout=30) as response:
            status = response.status
            payload = json.load(response)
        # Exactly the mapping used by ChatPage.doSend; the HTTP field is message.
        assistant_message = {"role": "assistant", "content": payload["message"]}
        content = assistant_message["content"]
        assert status == 200
        assert payload["intent"] == "credit_one_page_report"
        assert payload["data"]["reportStatus"] == "completed"
        assert content.splitlines()[0] == "# 征信速览报告"
        for header in (
            "| 主体信息 | 内容 |",
            "| 指标 | 当前情况 | 口径/来源 |",
            "| 序号 | 贷款机构 | 机构类别 | 贷款类型 | 合同金额 | 当前余额 | 发放日期 | 到期日期 | 状态/备注 |",
            "| 发卡行 | 币种 | 信用额度 | 已用额度 | 使用率 | 逾期 | 备注 |",
            "| 时间范围 | 贷款审批 | 信用卡审批 | 担保资格审查 | 法人资信审查 |",
            "| 检查项 | 状态 | 当前情况 | 判断依据 | 优化方向 |",
        ):
            assert header in content
        assert "| **主体信息内容** |" not in content
        assert "| **指标当前情况口径/来源** |" not in content
        assert "个人与企业关系：法定代表人 / 实际控制人" in content
        marker_present = "<!-- CREDIT_REPORT_RENDERER_V1 -->" in content
        assert marker_present == (os.environ.get("CREDIT_REPORT_EXPECT_MARKER") == "1")
        artifact = tmp_path / "http_response_and_trace.json"
        artifact.write_text(json.dumps({"test_data_only": True, "address": address, "http_status": status, "marker_present": marker_present, "calls": calls, "response": payload}, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"http_status": status, "marker_present": marker_present, "headers_verified": 6, "artifact": str(artifact)}, ensure_ascii=False))
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
