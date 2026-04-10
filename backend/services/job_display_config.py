from __future__ import annotations

from typing import Any


JOB_DISPLAY_CONFIG: dict[str, dict[str, Any]] = {
    "chat_extract": {
        "jobType": "chat_extract",
        "jobTypeLabel": "资料提取",
        "targetPage": "customerData",
        "defaultStatusText": {
            "pending": "已接收文件",
            "running": "正在提取结构化内容",
            "success": "资料提取已完成",
            "failed": "资料提取失败",
        },
        "supportsContinueView": True,
        "supportsViewResult": True,
        "supportsDirectNavigate": True,
    },
    "risk_report": {
        "jobType": "risk_report",
        "jobTypeLabel": "风险报告",
        "targetPage": "chat",
        "defaultStatusText": {
            "pending": "已提交风险报告任务",
            "running": "正在生成风险报告",
            "success": "风险报告已完成",
            "failed": "风险报告失败",
        },
        "supportsContinueView": True,
        "supportsViewResult": True,
        "supportsDirectNavigate": True,
    },
    "scheme_match": {
        "jobType": "scheme_match",
        "jobTypeLabel": "方案匹配",
        "targetPage": "scheme",
        "defaultStatusText": {
            "pending": "已提交方案匹配任务",
            "running": "正在生成融资方案匹配结果",
            "success": "方案匹配已完成",
            "failed": "方案匹配失败",
        },
        "supportsContinueView": True,
        "supportsViewResult": True,
        "supportsDirectNavigate": True,
    },
    "application_generate": {
        "jobType": "application_generate",
        "jobTypeLabel": "申请表生成",
        "targetPage": "application",
        "defaultStatusText": {
            "pending": "已提交申请表生成任务",
            "running": "正在生成申请表",
            "success": "申请表生成已完成",
            "failed": "申请表生成失败",
        },
        "supportsContinueView": True,
        "supportsViewResult": True,
        "supportsDirectNavigate": True,
    },
}

DEFAULT_JOB_DISPLAY_CONFIG: dict[str, Any] = {
    "jobType": "generic",
    "jobTypeLabel": "处理任务",
    "targetPage": None,
    "defaultStatusText": {
        "pending": "任务已提交",
        "running": "正在处理任务",
        "success": "任务已完成",
        "failed": "任务失败",
    },
    "supportsContinueView": True,
    "supportsViewResult": True,
    "supportsDirectNavigate": False,
}


def get_job_display_config(job_type: str | None) -> dict[str, Any]:
    if not job_type:
        return DEFAULT_JOB_DISPLAY_CONFIG
    return JOB_DISPLAY_CONFIG.get(job_type, DEFAULT_JOB_DISPLAY_CONFIG)


def get_job_type_label(job_type: str | None) -> str:
    return str(get_job_display_config(job_type).get("jobTypeLabel") or "处理任务")


def get_job_target_page(job_type: str | None) -> str | None:
    target_page = get_job_display_config(job_type).get("targetPage")
    return str(target_page) if target_page else None


def get_job_status_text(job_type: str | None, status: str | None) -> str:
    config = get_job_display_config(job_type)
    status_text = (config.get("defaultStatusText") or {}).get(status or "")
    if status_text:
        return str(status_text)
    return str((DEFAULT_JOB_DISPLAY_CONFIG.get("defaultStatusText") or {}).get(status or "", "任务处理中"))


def build_job_result_summary(job_type: str | None, result_payload: dict[str, Any] | None, customer_name: str | None) -> str | None:
    if not result_payload:
        return None

    customer_label = (customer_name or "").strip() or "当前客户"

    if job_type == "chat_extract":
        return "资料提取已完成，可查看提取结果并同步到资料汇总。"

    if job_type == "risk_report":
        overall = (result_payload.get("report_json") or {}).get("overall_assessment") or {}
        score = overall.get("total_score")
        if score is not None:
            return f"{customer_label}风险报告已生成，综合评分 {score} 分。"
        return f"{customer_label}风险报告已生成。"

    if job_type == "scheme_match":
        return f"{customer_label}的融资方案匹配结果已生成。"

    if job_type == "application_generate":
        return f"{customer_label}的申请表已生成。"

    return "任务已完成。"
