import pytest

from backend.services.extraction_skills.shuimui_report import extract_shuimui_report
from backend.services.shuimui_report_fetcher import fetch_shuimui_report, parse_shuimui_sn


SAMPLE_URL = "https://shuimui.szsmjr.com/index.html#/query/result?sn=S_207001c4662c4d9ab17881b3051f62ab"
SAMPLE_BODY_TEXT = """水母报告
转发报告
上海煜禧贸易有限公司
基本信息
纳税信息
发票信息
供应商信息
报告创建时间

2026-03-17

企业名称

上海煜禧贸易有限公司复制

当前法人姓名

刘聪 复制

法人占股比例

100%

成立日期

2013-01-29

统一信用代码

91310114060938092E 复制

行业分类

厨具卫具及日用杂品批发

注册资金

--

注册类型

私营有限责任公司

注册区域

上海市宝山区水产路1439号12幢117室

最近一次社保缴费记录

有

社保人数

6

应缴费额

9624 元

股东名称

刘聪

参股比例

100.00%

法人/股东变更

变更类型

法定代表人变更

变更时间

2018-12-04

变更前

马艳

变更后

刘聪

银税互动授权记录

无
"""


def test_parse_sn_from_hash_route():
    assert parse_shuimui_sn(SAMPLE_URL) == "S_207001c4662c4d9ab17881b3051f62ab"


def test_reject_non_shuimui_domain():
    with pytest.raises(ValueError, match="当前仅支持 shuimui.szsmjr.com"):
        parse_shuimui_sn("https://example.com/index.html#/query/result?sn=S_123")


def test_missing_sn_has_clear_error():
    with pytest.raises(ValueError, match="未识别到水母报告编号"):
        parse_shuimui_sn("https://shuimui.szsmjr.com/index.html#/query/result")


@pytest.mark.asyncio
async def test_http_empty_shell_falls_back_to_playwright(monkeypatch):
    async def fake_http(_url: str):
        return "root app", 200, 1024

    async def fake_playwright(_url: str):
        return (
            "水母报告\n企业名称：上海测试有限公司\n统一社会信用代码：91310000MA1TEST123\n"
            "报告编号：S_207001c4662c4d9ab17881b3051f62ab\n税务 发票 风险",
            "",
            "水母报告",
        )

    monkeypatch.setattr("backend.services.shuimui_report_fetcher._fetch_http_text", fake_http)
    monkeypatch.setattr("backend.services.shuimui_report_fetcher._fetch_playwright_text", fake_playwright)

    result = await fetch_shuimui_report(SAMPLE_URL)

    assert result.success is True
    assert "上海测试有限公司" in result.raw_text


@pytest.mark.asyncio
async def test_playwright_unavailable_returns_clear_error(monkeypatch):
    async def fake_http(_url: str):
        return "root app", 200, 1024

    async def fake_playwright(_url: str):
        return "", "playwright_unavailable:No module named playwright", ""

    monkeypatch.setattr("backend.services.shuimui_report_fetcher._fetch_http_text", fake_http)
    monkeypatch.setattr("backend.services.shuimui_report_fetcher._fetch_playwright_text", fake_playwright)

    result = await fetch_shuimui_report(SAMPLE_URL)

    assert result.success is False
    assert result.error_code == "PLAYWRIGHT_UNAVAILABLE"
    assert "服务器未安装 Playwright/Chromium" in result.error_message


def test_shuimui_markdown_is_chinese_structured_not_json():
    content = extract_shuimui_report(
        "水母报告\n企业名称：上海测试有限公司\n统一社会信用代码：91310000MA1TEST123\n法定代表人：张三",
        source_url=SAMPLE_URL,
        sn="S_207001c4662c4d9ab17881b3051f62ab",
        ai_service=None,
    )

    markdown = content["report_markdown"]
    assert markdown.startswith("## 水母报告")
    assert "### 企业基本信息" in markdown
    assert "* 企业名称：上海测试有限公司" in markdown
    assert "```json" not in markdown
    assert '"doc_type"' not in markdown
    assert "raw_text" not in markdown


@pytest.mark.asyncio
async def test_report_text_with_authorization_record_is_success_not_auth(monkeypatch):
    async def fake_http(_url: str):
        return "root app", 200, 1024

    async def fake_playwright(_url: str):
        return SAMPLE_BODY_TEXT, "", "水母报告"

    monkeypatch.setattr("backend.services.shuimui_report_fetcher._fetch_http_text", fake_http)
    monkeypatch.setattr("backend.services.shuimui_report_fetcher._fetch_playwright_text", fake_playwright)

    result = await fetch_shuimui_report(SAMPLE_URL)

    assert result.success is True
    assert result.error_code == ""


def test_sample_shuimui_text_extracts_expected_fields():
    content = extract_shuimui_report(
        SAMPLE_BODY_TEXT,
        source_url=SAMPLE_URL,
        sn="S_207001c4662c4d9ab17881b3051f62ab",
        ai_service=None,
    )
    markdown = content["report_markdown"]

    assert "企业名称：上海煜禧贸易有限公司" in markdown
    assert "统一社会信用代码：91310114060938092E" in markdown
    assert "法定代表人：刘聪" in markdown
    assert "法人占股比例：100%" in markdown
    assert "注册资本：未识别" not in markdown
    assert "注册类型：私营有限责任公司" in markdown
    assert "注册地址：上海市宝山区水产路1439号12幢117室" in markdown
    assert "行业分类：厨具卫具及日用杂品批发" in markdown
    assert "报告创建时间：2026-03-17" in markdown
    assert "社保人数：6" in markdown
    assert "应缴费额：9624 元" in markdown
    assert "股东名称：刘聪" in markdown
    assert "参股比例：100.00%" in markdown
    assert "变更类型：法定代表人变更" in markdown
    assert "变更时间：2018-12-04" in markdown
    assert "变更前：马艳" in markdown
    assert "变更后：刘聪" in markdown
    assert "授权记录：无" in markdown
    assert "未识别" not in markdown
    assert "document type" not in markdown.lower()
    assert "structured json" not in markdown.lower()


def test_shuimui_registration_capital_from_llm_is_not_used_when_page_is_empty():
    class FakeAiService:
        def extract(self, *_args, **_kwargs):
            return '{"注册资本":"1000000"}'

    raw_text = """水母报告
企业名称
上海煜禧贸易有限公司
统一信用代码
91310114060938092E
注册资金
--
注册类型
私营有限责任公司
"""
    content = extract_shuimui_report(
        raw_text,
        source_url=SAMPLE_URL,
        sn="S_207001c4662c4d9ab17881b3051f62ab",
        ai_service=FakeAiService(),
    )
    markdown = content["report_markdown"]

    assert "注册资本：1000000" not in markdown
    assert "注册资本：" not in markdown
    assert "注册资金：" not in markdown
    assert "注册类型：私营有限责任公司" in markdown


def test_sample_shuimui_table_rows_do_not_use_headers_as_values():
    raw_text = """水母报告
企业名称
上海煜禧贸易有限公司
统一信用代码
91310114060938092E
最近一次社保缴费记录
社保人数        应缴费额(元)
6       9624
股东明细
股东名称        参股比例
刘聪    100.00%
法人/股东变更
变更类型        变更时间        变更前  变更后
法定代表人变更  2018-12-04
马艳
刘聪
银税互动授权记录
无
"""
    content = extract_shuimui_report(
        raw_text,
        source_url=SAMPLE_URL,
        sn="S_207001c4662c4d9ab17881b3051f62ab",
        ai_service=None,
    )
    markdown = content["report_markdown"]

    assert "社保人数：6" in markdown
    assert "应缴费额：9624 元" in markdown
    assert "股东名称：刘聪" in markdown
    assert "参股比例：100.00%" in markdown
    assert "变更类型：法定代表人变更" in markdown
    assert "变更时间：2018-12-04" in markdown
    assert "变更前：马艳" in markdown
    assert "变更后：刘聪" in markdown
    assert "社保人数：应缴费额" not in markdown
    assert "股东名称：参股比例" not in markdown
    assert "变更类型：变更时间" not in markdown
    assert "最近一次社保缴费记录：社保人数" not in markdown


def test_shuimui_extracts_tax_invoice_supplier_tabs():
    raw_text = f"""{SAMPLE_BODY_TEXT}

### 页签：纳税信息
纳税信用等级
A
纳税人种类
一般纳税人
税款所属期        税种        纳税金额        申报状态
2026-02        增值税        12000 元        已申报

### 页签：发票信息
近1个月开票金额(元)
357033
近3个月开票金额(元)
1416707
近6月开票金额(元)
3029176
近12个月开票金额(元)
5653114
近24个月开票金额(元)
14002123
近3月开票环比增长率
-12.14%
近6开票环比增长率
15.44%
近12月开票环比增长率
-32.29%
近45日是否有开票记录
有
近3个月下游客户统计
20
近12月下游客户数量(家)
31.0
近12个月下游开票张数
268
近12个月作废发票数量占比
--%
近12个月最大连续未开票间隔天数（销项）
21
近12月断票月数(不含2月)
--
近12月最长连续断票月数
--
近12个月红冲金额占比
2.2%
近12月红冲发票张数占比
4.1%
近三年开票信息报表（元）
月份        2024        2025        2026
1月        896241        522681        413127
2月        1290200        689459        357033
年度汇总        9323309        6095093        1137794

### 页签：供应商信息
排名        供应商名称        采购额(元)        金额占比(%)        是否关联方
1        上海某某供应链有限公司        180000        35        否
2        苏州某某商贸有限公司        90000        17        否
"""
    content = extract_shuimui_report(
        raw_text,
        source_url=SAMPLE_URL,
        sn="S_207001c4662c4d9ab17881b3051f62ab",
        ai_service=None,
    )
    markdown = content["report_markdown"]

    assert "### 纳税信息" in markdown
    assert "纳税信用等级：A" in markdown
    assert "纳税人种类：一般纳税人" in markdown
    assert "纳税状态：正常" not in markdown
    assert "纳税金额：12000 元" not in markdown
    assert "### 发票信息" in markdown
    assert "#### 开票金额汇总（不含本月）" in markdown
    assert "近1个月开票金额(元)：357,033" in markdown
    assert "近24个月开票金额(元)：14,002,123" in markdown
    assert "#### 开票金额环比增长率（不含本月）" in markdown
    assert "近6月开票环比增长率：15.44%" in markdown
    assert "近12月开票环比增长率：-32.29%" in markdown
    assert "#### 开票活跃度与客户情况" in markdown
    assert "近12个月作废发票数量占比：--%" in markdown
    assert "近12月断票月数(不含2月)：--" in markdown
    assert "#### 近三年开票信息报表（元）" in markdown
    assert "| 1月 | 896,241 | 522,681 | 413,127 |" in markdown
    assert "| 年度汇总 | 9,323,309 | 6,095,093 | 1,137,794 |" in markdown
    assert "### 前十供应商" in markdown
    assert "供应商 1：上海某某供应链有限公司，交易金额：180,000.00 元，占比：35.00%，是否关联方：否" in markdown
    assert "供应商 2：苏州某某商贸有限公司，交易金额：90,000.00 元，占比：17.00%，是否关联方：否" in markdown
    assert "### 供应商信息" not in markdown
    assert "未识别" not in markdown
    assert "structured json" not in markdown.lower()


def test_shuimui_extracts_internal_capture_json_without_displaying_json():
    raw_text = """水母报告
__SHUIMUI_REPORT_CAPTURE_JSON__
{"sections":{"tax_info":{"label":"纳税信息","text":"纳税信用等级\\nB\\n当前欠税余额（元）\\n无","tables_text":""},"invoice_info":{"label":"发票信息","text":"近1个月开票金额(元)\\n880000","tables_text":""},"supplier_info":{"label":"供应商信息","text":"供应商名称        交易金额        占比\\n上海接口供应商        100000 元        20%","tables_text":""}},"api_json":[{"url":"https://shuimui.szsmjr.com/api/report","status":200,"field_summary":["纳税信用等级"],"payload":{"纳税信息":{"纳税人种类":"一般纳税人"},"发票信息":{"近3个月开票金额(元)":1200000}}}]}
__END_SHUIMUI_REPORT_CAPTURE_JSON__
### 页签：基本信息
企业名称
上海测试有限公司
统一信用代码
91310000MA1TEST123
"""
    content = extract_shuimui_report(
        raw_text,
        source_url=SAMPLE_URL,
        sn="S_207001c4662c4d9ab17881b3051f62ab",
        ai_service=None,
    )
    markdown = content["report_markdown"]

    assert "### 纳税信息" in markdown
    assert "纳税信用等级：B" in markdown
    assert "当前欠税余额（元）：无" in markdown
    assert "纳税人种类：一般纳税人" in markdown
    assert "### 发票信息" in markdown
    assert "近1个月开票金额(元)：880,000" in markdown
    assert "近3个月开票金额(元)：1,200,000" in markdown
    assert "### 前十供应商" in markdown
    assert "供应商 1：上海接口供应商，交易金额：100,000.00 元，占比：20.00%" in markdown
    assert "__SHUIMUI_REPORT_CAPTURE_JSON__" not in markdown
    assert "api_json" not in markdown


def test_shuimui_authorization_record_does_not_leak_tables_text():
    raw_text = """水母报告
企业名称
上海测试有限公司
统一信用代码
91310000MA1TEST123
银税互动授权记录
无", "tables_text": "社保人数\t应缴费额(元)\n6\t9624"
"""
    content = extract_shuimui_report(
        raw_text,
        source_url=SAMPLE_URL,
        sn="S_207001c4662c4d9ab17881b3051f62ab",
        ai_service=None,
    )
    markdown = content["report_markdown"]

    assert "授权记录：无" in markdown
    assert "tables_text" not in markdown
    assert "raw_text" not in markdown
    assert "structured json" not in markdown.lower()
    assert "None" not in markdown
    assert "null" not in markdown
    assert "[{" not in markdown


def test_shuimui_splits_suppliers_and_sales_customers():
    supplier_rows = "\n".join(
        f"供应商{i}        {1000000 + i * 1000}        {20 - i / 10:.2f}"
        for i in range(1, 11)
    )
    customer_rows = "\n".join(
        f"客户{i}        {2000000 + i * 1000}        {40 - i / 10:.2f}        否"
        for i in range(1, 4)
    )
    raw_text = f"""水母报告
企业名称
上海测试有限公司
统一信用代码
91310000MA1TEST123

### 页签：供应商信息
供应商名称        交易金额        占比
{supplier_rows}
客户名称        交易金额        占比        是否关联方
{customer_rows}
"""
    content = extract_shuimui_report(
        raw_text,
        source_url=SAMPLE_URL,
        sn="S_207001c4662c4d9ab17881b3051f62ab",
        ai_service=None,
    )
    markdown = content["report_markdown"]
    supplier_section = markdown.split("### 前十供应商", 1)[1].split("### 前十销售客户", 1)[0]
    customer_section = markdown.split("### 前十销售客户", 1)[1]

    assert "### 前十供应商" in markdown
    assert "### 前十销售客户" in markdown
    assert "供应商 1：供应商1，交易金额：1,001,000.00 元，占比：19.90%" in supplier_section
    assert "客户1" not in supplier_section
    assert "客户 1：客户1，交易金额：2,001,000.00 元，占比：39.90%，是否关联方：否" in customer_section
    assert "[{" not in markdown
    assert "主要上游客户" not in markdown
    assert "上下游交易" not in markdown


def test_shuimui_tax_late_fee_records_and_invoice_summary_are_clean():
    raw_text = """水母报告
企业名称
上海测试有限公司
统一信用代码
91310000MA1TEST123

### 页签：纳税信息
纳税信用等级
A
纳税人种类
一般纳税人
近12月欠税记录次数
0
当前欠税余额（元）
无
近3个月滞纳金金额(元)
无
近12个月滞纳金金额(元)
无
近12月滞纳金次数
无
近12月增税销售额（元）
5653114
近24月增税销售额（元）
14002123
近12月完税总额(元)
27370
近24月完税总额(元)
461946
近12月增税应纳额(元)
20663
近12月0申报月数(月)
0
近12月最长连续0纳税申报月数
0
资产金额（去年年报）
18509049.74
营业利润额（去年年报）
-1014150.98
负债率（去年年报）
105.53%
营业净利率（去年年报）
-16.64%
滞纳金情况
滞纳金时间        滞纳金金额        状态
2023-10-23        8.47        已缴清
2023-04-26        0.71        已缴清
2023-04-18        6.96        已缴清
4月        2024.00        2025
1月        18775.00        10105
年度汇总        443603.00        32853
税务处罚
登记日期        违法违章信息        违法违章状态        详细信息
2024-08-16        逾期未缴纳税款        责令限期缴纳税款        查看
2024-08-16        逾期未缴纳税款        责令限期缴纳税款        查看
近三年纳税信息完税表(元)
年份/期间        增值税销售额（元）        完税总额（元）        增值税应纳额（元）        0申报月数
2024        5653114        27370        20663        0
2025        14002123        461946        32853        0
近三年纳税信息完税表(元)
月份        2024        2025        2026
1月        18775        10105        10711
2月        6853        6494        406
年度汇总        443603        32853        11117
1月        896241        522681        413127
2月        1290200        689459        357033
年度汇总        9323309        6095093        1137794
完税表
税款所属期        税种        应纳税额（元）        实缴税额（元）        入库日期        状态
2024-04        增值税        2024.00        2024.00        2024-05-15        已缴清

### 页签：发票信息
近1个月开票金额(元)
357033
近3个月开票金额(元)
1416707
近6月开票金额(元)
3029176
近12个月开票金额(元)
5653114
近24个月开票金额(元)
14002123
近3月开票环比增长率
-12.14%
近6开票环比增长率
15.44%
近12月开票环比增长率
-32.29%
近45日是否有开票记录
有
近3个月下游客户统计
20
近12月下游客户数量(家)
31.0
近12个月下游开票张数
268
近12个月作废发票数量占比
--%
近12个月最大连续未开票间隔天数（销项）
21
近12月断票月数(不含2月)
--
近12月最长连续断票月数
--
近12个月红冲金额占比
2.2%
近12月红冲发票张数占比
4.1%
近三年开票信息报表（元）
月份        2024        2025        2026
1月        896241        522681        413127
2月        1290200        689459        357033
年度汇总        9323309        6095093        1137794
"""
    content = extract_shuimui_report(
        raw_text,
        source_url=SAMPLE_URL,
        sn="S_207001c4662c4d9ab17881b3051f62ab",
        ai_service=None,
    )
    markdown = content["report_markdown"]

    assert "### 纳税信息" in markdown
    assert "纳税信用等级：A" in markdown
    assert "纳税人种类：一般纳税人" in markdown
    assert "近12月欠税记录次数：0" in markdown
    assert "当前欠税余额（元）：无" in markdown
    assert "近3个月滞纳金金额(元)：无" in markdown
    assert "近12个月滞纳金金额(元)：无" in markdown
    assert "近12月滞纳金次数：无" in markdown
    assert "近12月增税销售额（元）：5,653,114" in markdown
    assert "近24月增税销售额（元）：14,002,123" in markdown
    assert "近12月完税总额(元)：27,370" in markdown
    assert "近24月完税总额(元)：461,946" in markdown
    assert "近12月增税应纳额(元)：20,663" in markdown
    assert "近12月0申报月数(月)：0" in markdown
    assert "近12月最长连续0纳税申报月数：0" in markdown
    assert "#### 滞纳金情况" in markdown
    assert "记录 1：2023-10-23，滞纳金金额：8.47 元，状态：已缴清" in markdown
    assert "记录 2：2023-04-26，滞纳金金额：0.71 元，状态：已缴清" in markdown
    assert "记录 3：2023-04-18，滞纳金金额：6.96 元，状态：已缴清" in markdown
    assert "4月，滞纳金金额：2,024.00 元，状态：2025" not in markdown
    assert "年度汇总，滞纳金金额：443,603.00 元，状态：32853" not in markdown
    assert "#### 税务处罚" in markdown
    assert "记录 1：登记日期：2024-08-16，违法违章信息：逾期未缴纳税款，违法违章状态：责令限期缴纳税款" in markdown
    assert "记录 2：登记日期：2024-08-16" not in markdown
    assert "详细信息：查看" not in markdown
    assert "#### 近三年纳税信息完税表(元)" in markdown
    tax_table_section = markdown.split("#### 近三年纳税信息完税表(元)", 1)[1].split("### 财报信息", 1)[0]
    assert "| 月份 | 2024 | 2025 | 2026 |" in markdown
    assert "| 1月 | 18,775 | 10,105 | 10,711 |" in markdown
    assert "| 2月 | 6,853 | 6,494 | 406 |" in markdown
    assert "| 年度汇总 | 443,603 | 32,853 | 11,117 |" in markdown
    assert "896,241" not in tax_table_section
    assert "1,290,200" not in tax_table_section
    assert "9,323,309" not in tax_table_section
    assert tax_table_section.count("| 1月 |") == 1
    assert tax_table_section.count("| 年度汇总 |") == 1
    assert "滞纳金时间" not in tax_table_section
    assert "登记日期" not in tax_table_section
    assert "供应商名称" not in tax_table_section
    assert "客户名称" not in tax_table_section
    assert "#### 完税表" not in markdown
    assert "### 财报信息" in markdown
    assert "资产金额（去年年报）：18,509,049.74" in markdown
    assert "营业利润额（去年年报）：-1,014,150.98" in markdown
    assert "负债率（去年年报）：105.53%" in markdown
    assert "营业净利率（去年年报）：-16.64%" in markdown
    assert "### 发票信息" in markdown
    assert "#### 开票金额汇总（不含本月）" in markdown
    assert "近1个月开票金额(元)：357,033" in markdown
    assert "近3个月开票金额(元)：1,416,707" in markdown
    assert "近6月开票金额(元)：3,029,176" in markdown
    assert "近12个月开票金额(元)：5,653,114" in markdown
    assert "近24个月开票金额(元)：14,002,123" in markdown
    assert "#### 开票金额环比增长率（不含本月）" in markdown
    assert "近3月开票环比增长率：-12.14%" in markdown
    assert "近6月开票环比增长率：15.44%" in markdown
    assert "近12月开票环比增长率：-32.29%" in markdown
    assert "#### 开票活跃度与客户情况" in markdown
    assert "近45日是否有开票记录：有" in markdown
    assert "近3个月下游客户统计：20" in markdown
    assert "近12月下游客户数量(家)：31.0" in markdown
    assert "近12个月下游开票张数：268" in markdown
    assert "近12个月作废发票数量占比：--%" in markdown
    assert "近12个月最大连续未开票间隔天数（销项）：21" in markdown
    assert "近12月断票月数(不含2月)：--" in markdown
    assert "近12月最长连续断票月数：--" in markdown
    assert "近12个月红冲金额占比：2.2%" in markdown
    assert "近12月红冲发票张数占比：4.1%" in markdown
    assert "#### 近三年开票信息报表（元）" in markdown
    invoice_table_section = markdown.split("#### 近三年开票信息报表（元）", 1)[1]
    assert "| 月份 | 2024 | 2025 | 2026 |" in invoice_table_section
    assert "| 1月 | 896,241 | 522,681 | 413,127 |" in invoice_table_section
    assert "| 2月 | 1,290,200 | 689,459 | 357,033 |" in invoice_table_section
    assert "| 年度汇总 | 9,323,309 | 6,095,093 | 1,137,794 |" in invoice_table_section
    assert "18,775" not in invoice_table_section
    assert "443,603" not in invoice_table_section
    assert "未明确" not in markdown
    assert "未识别" not in markdown
