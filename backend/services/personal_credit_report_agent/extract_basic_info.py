from __future__ import annotations

from typing import Any

from .evidence import first_match, value_after_label
from .schema import default_basic_info


def extract_basic_info(sections: dict[str, Any], source_file: str | None = None) -> dict[str, Any]:
    try:
        text = "\n".join(
            str(sections.get(key) or "")
            for key in ("report_basic_info", "personal_basic_info", "full_text")
        )
        result = default_basic_info()
        result.update(
            {
                "report_number": value_after_label(text, ("报告编号", "报告号码", "报告号")),
                "report_time": value_after_label(text, ("报告时间", "报告日期", "生成时间", "查询时间")),
                "name": value_after_label(text, ("姓名", "被查询者姓名")),
                "id_type": value_after_label(text, ("证件类型", "证件名称")),
                "id_number": value_after_label(text, ("证件号码", "证件号", "身份证号码")),
                "marital_status": value_after_label(text, ("婚姻状况", "婚姻状态")),
                "source_file": source_file or "",
            }
        )
        if not result["id_number"]:
            result["id_number"] = first_match(text, (r"([1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx])",))
        return result
    except Exception:
        result = default_basic_info()
        result["source_file"] = source_file or ""
        return result
