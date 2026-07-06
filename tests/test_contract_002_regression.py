from __future__ import annotations

from pathlib import Path

from backend.celery_app import FILE_PROCESS_TASK_NAME, HEAVY_QUEUE_NAME, INDEX_REBUILD_TASK_NAME
from backend.services.contract_agent import ContractAgent


CONTRACT_002_FILENAME = "合同002：临空12号地块国际商务花园四期项目（除桩基）-机电安装工程（南区） (1).pdf"


def _contract_002_pages() -> list[dict[str, str | int]]:
    toc = """目录
通用合同条款....................-11-
12.3工程款支付................-40-
16.结算........................-46-
18.违约责任....................-49-
22.争议解决....................-57-
专用合同条款....................-77-
"""
    page7 = """机电安装工程专业分包合同（南区）
承包人：上海建工集团股份有限公司
分包人：上海意川建筑科技有限公司
总包工程名称：临空12号地块国际商务花园四期项目（除桩基）
分包工程名称：临空12号地块国际商务花园四期项目（除桩基）-机电安装工程（南区）
分包工程地点：长宁区基地东至协和路，西至广顺北路，南至北翟路绿化带，北至通协路
分包工程承包范围和内容：包括但不限于5#~9#楼地上、地下室、室外总体的电气工程、给排水工程、人防工程（水、电、风）、雨水回收工程、通风防排烟工程、
预埋套管工程、弱电及消防报警预埋工程、防火封堵及抗震支架（地下室除电缆按末端设备服务功能区域或号房划分外，其余按物理位置划分）等机电安装工程施工及相关图纸深化、相关方案编制、评审等一切与机电安装相关的工作。
承包方式：包工包料（除甲供外）、包工期、包质量、包安全、包文明施工、包工程一切保险费、包环境保护、
包工程整体协调配合管理、包监测检测、包验收、包竣工验收备案、包维修保修的施工专业分包方式。
计划开工日期：2022年10月1日，具体开工日期以承包人书面通知为准。计划完工日期：2024年6月30日
合同工期：638天
质量标准：符合总包合同约定的分包工程质量标准，并达到一次性验收合格；施工期间无死亡事故、无重大伤残事故，达到上海市文明工地标准。
"""
    page8 = """签约合同价暂定为含税：人民币188,491,296.13元
大写：壹亿捌仟捌佰肆拾玖万壹仟贰佰玖拾陆元壹角叁分
不含增值税签约合同价：人民币172927794.60元
增值税税率：9%
安全文明施工费（含税）：大写：零元（￥0元）
分包合同文件构成：合同协议书；中标通知书（如有）；专用合同条款及其附件；通用合同条款；技术标准和要求；图纸目录；
"""
    page9 = """已标价工程量清单或预算书；招标文件（如有）；投标函及其附录（如有）；其他分包合同文件。
分包人承诺确保工程质量和安全，不进行转包及违法分包。
分包人承诺在缺陷责任期及保修期内承担相应工程维修责任。
本合同于2022年10月__日签订
本合同在上海市长宁区签订
"""
    page10 = """承包人（盖章）：上海建工集团股份有限公司    分包人（盖章）：上海意川建筑科技有限公司
地址：东大名路666号    地址：上海市松江区佘山镇沈砖公路3129弄1
号1幢3楼A区213室
邮政编码：200080    邮政编码：201600
统一社会信用代码：91310000631189305E    统一社会信用代码：91310118MA1JP7UB2B
开户银行：建行上海第二支行    开户银行：上海银行股份有限公司浦西支行
账号：31001502500055390033    账号：03005029359
纳税人性质：一般纳税人    纳税人性质：一般纳税人
本合同自双方加盖公章或合同专用章并经法定代表人或其委托代理人签字（章）后生效。
本合同一式_捌_份，均具有同等法律效力，承包人执_肆_份，分包人执肆_份
"""
    pages: list[dict[str, str | int]] = [{"page": number, "text": ""} for number in range(1, 11)]
    pages[1]["text"] = toc
    pages[6]["text"] = page7
    pages[7]["text"] = page8
    pages[8]["text"] = page9
    pages[9]["text"] = page10
    return pages


def _run_contract_002():
    pages = _contract_002_pages()
    return ContractAgent().run(
        {
            "text": "\n".join(str(page["text"]) for page in pages),
            "raw_pages": pages,
            "filename": CONTRACT_002_FILENAME,
        }
    )


def test_contract_002_construction_subcontract_markdown_baseline() -> None:
    result = _run_contract_002()
    markdown = result.display_markdown

    expected_lines = (
        "- 资料类型：合同",
        "- 合同类型：建设工程专业分包合同",
        "- 提取状态：部分成功",
        "- 合同名称：机电安装工程专业分包合同（南区）",
        "- 项目名称：临空12号地块国际商务花园四期项目（除桩基）",
        "- 签订日期：2022年10月（具体日期未填写，需人工复核）",
        "- 签订地点：本合同在上海市长宁区签订",
        "- 合同页数：10",
        "- 合同生效条件：双方加盖公章或合同专用章，并经法定代表人或其委托代理人签字（章）后生效",
        "- 合同份数：本合同一式捌份，均具有同等法律效力，承包人执肆份，分包人执肆份",
        "| 甲方/承包人/发包人 | 上海建工集团股份有限公司 | 91310000631189305E | 未识别 | 未识别 | 未识别 | 东大名路666号 |",
        "| 乙方/分包人 | 上海意川建筑科技有限公司 | 91310118MA1JP7UB2B | 未识别 | 未识别 | 未识别 | 上海市松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室 |",
        "- 工程或服务地点：长宁区基地东至协和路，西至广顺北路，南至北翟路绿化带，北至通协路",
        "- 合同金额：人民币 188,491,296.13 元",
        "- 大写金额：壹亿捌仟捌佰肆拾玖万壹仟贰佰玖拾陆元壹角叁分",
        "- 小写金额：188,491,296.13 元",
        "- 含税金额：188,491,296.13 元",
        "- 不含税金额：172,927,794.60 元",
        "- 税率：9%",
        "- 税额：15,563,501.53 元（根据含税金额和不含税金额推算，需人工复核）",
        "- 安全文明施工费：0 元",
        "- 合同价格形式：未识别",
        "- 金额校验：大写金额与小写金额基本一致；税额根据含税金额和不含税金额推算，需人工复核",
        "- 金额识别状态：部分成功",
        "- 开始日期：2022年10月1日，具体开工日期以承包人书面通知为准",
        "- 结束日期：2024年6月30日",
            "- 合同工期：638天",
        "- 付款方式：未识别（当前PDF未包含工程款支付正文条款）",
        "- 结算方式：未识别（当前PDF未包含结算正文条款）",
        "- 发票要求：未识别（当前PDF未包含发票正文条款）",
        "- 收款账户：开户银行：上海银行股份有限公司浦西支行；账号：03005029359",
        "### 签章信息",
        "- 甲方签章：有",
        "- 乙方签章：有",
        "- 签章页：第 10 页",
        "- OCR质量：可用",
        "- 关键字段完整度：部分完整",
        "- 文件完整性：当前PDF疑似仅包含合同协议书、目录及签章页，通用/专用条款正文未包含在本文件中",
    )
    for expected in expected_lines:
        assert expected in markdown

    for scope_token in ("5#~9#楼", "电气工程", "给排水工程", "人防工程", "雨水回收工程", "通风防排烟工程", "弱电及消防报警预埋工程", "防火封堵及抗震支架"):
        assert scope_token in markdown
    for quality_token in ("一次性验收合格", "无死亡事故", "无重大伤残事故", "上海市文明工地标准"):
        assert quality_token in markdown
    for review_token in ("签订日期具体日期未填写", "付款条款正文缺失", "结算条款正文缺失", "发票条款正文缺失", "违约责任正文缺失", "争议解决正文缺失", "税额为系统推算值需复核"):
        assert review_token in markdown


def test_contract_002_structured_data_baseline() -> None:
    data = _run_contract_002().structured_data_dict()
    parties = data["parties"]
    amount = data["amount"]

    assert data["doc_type"] == "contract"
    assert data["contract_category"] == "construction_subcontract"
    assert data["contract_category_name"] == "建设工程专业分包合同"
    assert data["extraction_status"] == "partial"
    assert data["title"] == "机电安装工程专业分包合同（南区）"
    assert data["project_name"] == "临空12号地块国际商务花园四期项目（除桩基）"
    assert data["signing_date"] == "2022年10月（具体日期未填写，需人工复核）"
    assert data["signing_place"] == "本合同在上海市长宁区签订"
    assert data["page_count"] == 10
    assert data["effective_condition"] == "双方加盖公章或合同专用章，并经法定代表人或其委托代理人签字（章）后生效"
    assert data["copies"] == "本合同一式捌份，均具有同等法律效力，承包人执肆份，分包人执肆份"
    assert parties[0]["name"] == "上海建工集团股份有限公司"
    assert parties[0]["unified_social_credit_code"] == "91310000631189305E"
    assert parties[0]["address"] == "东大名路666号"
    assert parties[1]["name"] == "上海意川建筑科技有限公司"
    assert parties[1]["unified_social_credit_code"] == "91310118MA1JP7UB2B"
    assert parties[1]["address"] == "上海市松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室"
    assert amount["tax_excluded_amount"] == "172,927,794.60 元"
    assert amount["tax_excluded_amount_source"] == "ocr"
    assert amount["tax_rate"] == "9%"
    assert amount["tax_amount"] == "15,563,501.53 元（根据含税金额和不含税金额推算，需人工复核）"
    assert amount["tax_amount_source"] == "calculated"
    assert amount["tax_amount_calculation_basis"] == "included_minus_excluded"
    assert amount.get("price_form_source") == "missing"
    assert data["settlement"]["receiving_account"] == "开户银行：上海银行股份有限公司浦西支行；账号：03005029359"


def test_contract_002_forbidden_regressions() -> None:
    markdown = _run_contract_002().display_markdown
    lower_markdown = markdown.lower()
    for forbidden in ("owner type", "contract category", "evidence", "raw_text", "source_page", "confidence", "markdown result"):
        assert forbidden not in lower_markdown
    for forbidden in (
        "乙方/分包人 | 上海意川建筑科技有限公司 | 91310000631189305E",
        "乙方/分包人 | 上海意川建筑科技有限公司 | 91310118MA1JP7UB2B | 未识别 | 未识别 | 未识别 | 东大名路666号",
        "收款账户：开户银行：建行上海第二支行",
        "收款账户：开户银行：上海银行股份有限公司浦西支行；账号：31001502500055390033",
        "甲方/承包人/发包人 | 上海银行股份有限公司",
        "### 签\n章信息",
    ):
        assert forbidden not in markdown
    assert "```json" not in lower_markdown
    assert "{\n" not in markdown


def test_contract_002_file_process_job_does_not_wait_for_index_rebuild() -> None:
    file_router_source = Path("backend/routers/file.py").read_text(encoding="utf-8")
    file_task_source = Path("backend/tasks/file_process_tasks.py").read_text(encoding="utf-8")
    index_task_source = Path("backend/tasks/index_rebuild_tasks.py").read_text(encoding="utf-8")

    assert FILE_PROCESS_TASK_NAME == "backend.tasks.file_process_tasks.run_file_process_job"
    assert INDEX_REBUILD_TASK_NAME == "backend.tasks.index_rebuild_tasks.rebuild_customer_index_task"
    assert "@celery_app.task(name=FILE_PROCESS_TASK_NAME" in file_task_source
    assert "default_retry_delay=30, queue=HEAVY_QUEUE_NAME" in index_task_source
    assert HEAVY_QUEUE_NAME == "heavy"

    parse_complete_index = file_router_source.index("[File Job] parse completed before index rebuild")
    success_update_index = file_router_source.index('status="success"', parse_complete_index)
    dispatch_index = file_router_source.index("_dispatch_customer_index_rebuild", success_update_index)
    completed_without_waiting_index = file_router_source.index("[File Job] completed without waiting index rebuild", dispatch_index)
    assert parse_complete_index < success_update_index < dispatch_index < completed_without_waiting_index
    assert "indexing_status\": \"queued\"" in file_router_source
    assert "检索索引后台刷新中" in file_router_source
