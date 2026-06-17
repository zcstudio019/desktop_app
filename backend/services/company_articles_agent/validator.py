from __future__ import annotations

from .schema import CompanyArticlesResult


def validate_company_articles(result: CompanyArticlesResult) -> CompanyArticlesResult:
    warnings: list[str] = []
    required = {
        "章程标题": result.title,
        "公司名称": result.company_name,
        "公司住所": result.company_address,
        "经营范围": result.business_scope,
        "注册资本": result.registered_capital,
    }
    for label, value in required.items():
        if not value or value == "未识别":
            warnings.append(f"{label}未识别")

    shareholder_total = sum(float(item.subscribed_amount_number or 0) for item in result.shareholders)
    registered_amount = float(result.registered_capital_amount or 0)
    is_consistent = bool(registered_amount) and abs(shareholder_total - registered_amount) < 0.01
    total_text = f"{shareholder_total:g}万元" if shareholder_total else "未识别"
    message = "出资额合计与注册资本一致" if is_consistent else "股东出资额合计与注册资本不一致，请人工复核"
    result.capital_check = {
        "registered_capital_amount": registered_amount if registered_amount else None,
        "shareholder_total_amount": shareholder_total if shareholder_total else None,
        "shareholder_total_amount_text": total_text,
        "is_consistent": is_consistent,
        "message": message,
    }
    if not result.shareholders:
        warnings.append("股东及出资表未识别")
    if result.page_count < 2:
        warnings.append("页数较少，请确认是否已完成多页 OCR 合并")
    result.extraction_status = "success" if not warnings else "partial"
    result.warnings = list(dict.fromkeys([*result.warnings, *warnings]))
    return result
