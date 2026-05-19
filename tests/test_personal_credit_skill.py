from __future__ import annotations

from backend.extraction_skills import get_skill
from backend.extraction_skills.personal_credit import PersonalCreditSkill


MINIMAL_PERSONAL_CREDIT_TEXT = """
个人信用报告
报告编号:2025031104013907986945
报告时间:2025-03-11 04:01:39
姓名: 张三
证件类型: 身份证
证件号码: 310110198211172732
"""


def test_personal_credit_skill_registered() -> None:
    assert isinstance(get_skill("personal_credit_report"), PersonalCreditSkill)
    assert isinstance(get_skill("personal_credit"), PersonalCreditSkill)
    assert isinstance(get_skill("个人征信"), PersonalCreditSkill)


def test_personal_credit_skill_extract_minimal() -> None:
    skill = PersonalCreditSkill()

    result = skill.extract(
        raw_text=MINIMAL_PERSONAL_CREDIT_TEXT,
        filename="personal-credit.txt",
    )

    assert result["type"] == "personal_credit_report"
    assert result["skill_name"] == "personal_credit_report"
    assert isinstance(result["extracted_json"], dict)
    assert isinstance(result["markdown_summary"], str)
    assert "personal_credit_report" in result["schema_version"]
    assert result["data"] is result["extracted_json"]
