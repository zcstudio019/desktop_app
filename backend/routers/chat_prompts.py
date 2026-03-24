"""
Prompt templates and JSON structures for loan application generation.

Extracted from chat.py to keep functions under 50 lines.
Contains:
- Application JSON structure templates (personal / enterprise)
- Field mapping guide for enterprise applications
- Prompt builders for AI generation
"""

import json as json_module


def _get_personal_json_template() -> str:
    """Return the JSON structure template for personal loan applications.

    完整匹配 沪上银贷款申请表 (2).docx 的 9 个分组。

    Returns:
        str: JSON structure string with placeholder "xxx" values.
    """
    return """{
  "个人基本信息": {
    "申请日期": "xxx", "申请贷款类型": "xxx", "姓名": "xxx",
    "性别": "xxx", "年龄": "xxx", "婚姻状况": "xxx",
    "身份证号": "xxx", "学历": "xxx", "手机号码": "xxx",
    "备用联系方式": "xxx", "居住地址": "xxx", "居住时长": "xxx"
  },
  "职业与收入信息": {
    "工作单位全称": "xxx", "单位性质": "xxx", "岗位职务": "xxx",
    "入职时间": "xxx", "月均收入（税后）": "xxx", "收入来源": "xxx",
    "近6个月银行流水均值": "xxx"
  },
  "社保公积金信息": {
    "社保缴纳情况": "xxx", "公积金缴纳情况": "xxx", "缴纳说明": "xxx"
  },
  "信用卡使用详情": {
    "持有数量": "xxx", "授信总额": "xxx", "当前已用额度": "xxx",
    "使用率": "xxx", "还款记录": "xxx"
  },
  "资产信息": { "流动资产": "xxx", "固定资产": "xxx" },
  "负债信息": { "未结清负债总额": "xxx", "负债明细": "xxx" },
  "征信与负面记录": {
    "近6个月硬查询次数": "xxx", "近24个月逾期记录": "xxx",
    "负面记录声明": "xxx", "负面记录说明": "xxx"
  },
  "贷款申请详情": {
    "申请额度": "xxx", "期望年化利率": "xxx", "贷款期限": "xxx",
    "还款方式": "xxx", "贷款用途": "xxx", "特殊要求": "xxx"
  },
  "声明与签字": {
    "申请人声明": "本人确认以上信息真实有效，知晓提供虚假信息将承担法律责任；授权金融机构查询个人征信报告",
    "申请人签字": "xxx", "签字日期": "xxx"
  }
}"""


def _get_enterprise_json_template() -> str:
    """Return the JSON structure template for enterprise loan applications.

    完整匹配 企业客户贷款申请表.docx 的 7 个分组（不含资料提交清单）。

    Returns:
        str: JSON structure string with placeholder "xxx" values.
    """
    return """{
  "企业基本信息": {
    "企业名称": "xxx", "统一社会信用代码": "xxx", "注册地址": "xxx",
    "实际经营地址": "xxx", "成立时间": "xxx", "经营状态": "xxx",
    "近1年是否变更": "xxx", "经营场地": "xxx",
    "法定代表人姓名": "xxx", "法定代表人身份证号": "xxx", "法定代表人年龄": "xxx",
    "实际控制人姓名": "xxx", "实际控制人持股比例": "xxx", "实际控制人控制路径": "xxx",
    "纳税评级": "xxx", "近12个月纳税": "xxx", "行业类型": "xxx",
    "是否禁入行业": "xxx", "科技属性": "xxx",
    "对公电话": "xxx", "经办人": "xxx", "经办人手机号": "xxx"
  },
  "贷款申请信息": {
    "贷款类型": "xxx", "核心用途": "xxx", "期望额度": "xxx",
    "期望期限": "xxx", "利率接受范围": "xxx",
    "是否需随借随还/无还本续贷": "xxx", "还款方式": "xxx",
    "担保方式": "xxx", "抵押物类型": "xxx", "抵押物位置": "xxx",
    "抵押物预评估价": "xxx", "抵押物是否按揭": "xxx",
    "质押物": "xxx", "质押物价值": "xxx", "保证人": "xxx", "其他增信方式": "xxx"
  },
  "还款能力信息": {
    "近1年营业收入": "xxx", "近1年月均收入": "xxx",
    "主要交易对手（前5名）": "xxx", "近12个月开票": "xxx",
    "资产负债率": "xxx", "流动比率": "xxx", "毛利率": "xxx",
    "近1年经营活动现金流净额": "xxx",
    "他行经营贷笔数": "xxx", "他行经营贷余额": "xxx",
    "其他负债类型": "xxx", "其他负债余额": "xxx", "信用卡使用率": "xxx",
    "货币资金": "xxx", "应收账款": "xxx", "存货": "xxx",
    "固定资产": "xxx", "固定资产成新率": "xxx", "净资产总额": "xxx"
  },
  "征信及负债相关声明": {
    "当前逾期": "xxx", "当前逾期金额": "xxx", "当前逾期时长": "xxx",
    "近2年逾期": "xxx", "近3个月查询次数": "xxx", "近6个月查询次数": "xxx",
    "对外担保": "xxx", "被担保人": "xxx", "担保金额": "xxx",
    "担保期限": "xxx", "是否有代偿风险": "xxx",
    "隐形负债": "xxx", "隐形负债类型": "xxx", "隐形负债金额": "xxx",
    "隐形负债还款期限": "xxx",
    "诉讼/仲裁记录": "xxx", "案件类型": "xxx", "立案时间": "xxx",
    "涉案金额": "xxx", "审理进度": "xxx", "对经营影响": "xxx",
    "其他负面记录": "xxx", "负面记录类型": "xxx", "负面记录具体情况": "xxx"
  },
  "共同借款人/配偶信息": {
    "姓名": "xxx", "身份证号": "xxx", "与借款人关系": "xxx",
    "婚姻状况": "xxx",
    "当前逾期": "xxx", "当前逾期金额": "xxx", "当前逾期时长": "xxx",
    "近2年逾期": "xxx", "近3个月查询次数": "xxx", "近6个月查询次数": "xxx",
    "是否存在负面记录": "xxx", "负面记录说明": "xxx",
    "现有负债": "xxx", "负债类型": "xxx", "负债金额": "xxx", "还款状态": "xxx",
    "货币资金": "xxx",
    "房产": "xxx", "房产位置": "xxx", "房产估值": "xxx",
    "车辆": "xxx", "车辆品牌型号": "xxx", "车辆估值": "xxx",
    "其他资产": "xxx", "其他资产估值": "xxx"
  },
  "特殊偏好与附加要求": {
    "办理效率要求": "xxx", "办理渠道偏好": "xxx", "其他需求": "xxx"
  },
  "声明与承诺": {
    "企业盖章": "xxx", "法定代表人签字": "xxx",
    "共同借款人/配偶签字": "xxx", "申请日期": "xxx"
  }
}"""


# 企业贷款申请表字段映射指南（从客户资料提取数据的映射关系）
_ENTERPRISE_FIELD_MAPPING_GUIDE = """## 数据提取指南（字段映射表）
请严格按照以下映射关系，从客户资料中提取数据填入申请表：

### 企业基本信息
| 申请表字段 | 从客户资料提取 |
|-----------|---------------|
| 企业名称 | 企业身份信息.企业名称 |
| 统一社会信用代码 | 企业身份信息.统一社会信用代码 |
| 注册地址 | 企业身份信息.注册地址 |
| 实际经营地址 | 企业身份信息.办公/经营地址 |
| 成立时间 | 企业身份信息.成立日期 |
| 经营状态 | 企业身份信息.经营状态 |
| 法定代表人姓名 | 法定代表人信息.姓名 |
| 法定代表人身份证号 | 法定代表人信息.身份证号 |
| 法定代表人年龄 | 从身份证号第7-10位提取出生年份，用当前年份(2026)减去出生年份 |
| 实际控制人姓名 | 实际控制人.姓名 |
| 实际控制人持股比例 | 主要出资人中该人的出资比例（如"60%"） |
| 行业类型 | 企业身份信息.所属行业 |

### 还款能力信息（从财务数据/资产负债表/利润表/现金流量表提取）
| 申请表字段 | 从客户资料提取 |
|-----------|---------------|
| 近1年营业收入 | 利润表.营业收入 |
| 近1年月均收入 | 营业收入÷12 计算 |
| 货币资金 | 资产.货币资金 |
| 应收账款 | 资产.应收账款 |
| 存货 | 资产.存货 |
| 固定资产 | 资产.固定资产 |
| 净资产总额 | 所有者权益.所有者权益合计 |
| 资产负债率 | 负债合计÷资产总计×100% |
| 流动比率 | 流动资产合计÷流动负债合计 |
| 毛利率 | (营业收入-营业成本)÷营业收入×100% |
| 近1年经营活动现金流净额 | 现金流量表.经营活动现金流量净额 |
| 主要交易对手（前5名） | 交易对手结构.主要收入对手Top5 |

### 征信及负债信息（从企业/个人征信提取）
| 申请表字段 | 从客户资料提取 |
|-----------|---------------|
| 他行经营贷笔数 | 未结清信贷.合计.账户数 |
| 他行经营贷余额 | 未结清信贷.合计.余额（单位：万元） |
| 当前逾期 | 信用评估结论中是否有逾期记录，无则填"无" |
| 当前逾期金额 | 未结清信贷明细.逾期金额，无则填"0" |
| 对外担保 | 有对外担保明细则填"是"，否则填"否" |
| 担保金额 | 对外担保明细中所有担保余额之和（单位：万元） |
| 诉讼/仲裁记录 | 公共记录.民事判决数 + 强制执行数，为0则填"无" |

### 抵押物信息（从抵押物信息提取）
| 申请表字段 | 从客户资料提取 |
|-----------|---------------|
| 抵押物类型 | 抵押物基本信息.抵押物类型 |
| 抵押物位置 | 不动产信息.房屋坐落 |
| 担保方式 | 未结清信贷明细.担保方式 |

### 流水信息（从企业/个人流水提取）
| 申请表字段 | 从客户资料提取 |
|-----------|---------------|
| 主要交易对手（前5名） | 主要收入对手Top5 的对手方名称列表 |"""


def _build_filled_prompt(customer_json: str, json_structure: str) -> str:
    """Build prompt for generating application with customer data.

    Args:
        customer_json: Customer data as JSON string.
        json_structure: Expected JSON output structure.

    Returns:
        str: Complete prompt for AI generation.
    """
    return f"""【重要】你必须且只能返回 JSON 格式，禁止返回 Markdown、列表或其他任何格式。

你是一个专业的贷款申请表填写助手。请根据以下客户资料，生成贷款申请表数据。

## 重要规则
1. **仔细阅读客户资料中的所有内容**，包括 Markdown 格式的详细信息
2. **尽可能多地提取信息**填写申请表，不要遗漏任何可用数据
3. 对于客户资料中确实没有的信息，填写"待补充"
4. **绝对禁止编造以下关键字段**：
   - 期望额度、期望期限、利率、贷款金额
   这些字段如果客户资料中没有，必须填写"待补充"
5. **必须返回纯 JSON 格式**，禁止返回 Markdown 格式（如 **加粗**、# 标题、- 列表等）

{_ENTERPRISE_FIELD_MAPPING_GUIDE}

## 客户资料
{customer_json}

## 输出格式要求
请按以下 JSON 结构输出，每个分组作为一个对象：
{json_structure}

## 注意
- 只返回 JSON，不要有其他文字
- 所有字段值都是字符串类型
- 没有数据的字段填写"待补充"
- **优先填写有数据的字段，尽量减少"待补充"的数量**

【再次强调】你的输出必须是以 {{ 开头、以 }} 结尾的纯 JSON，禁止输出任何 Markdown 格式（如标题、列表、加粗等）。
"""


def _build_blank_prompt(json_structure: str) -> str:
    """Build prompt for generating blank application template.

    Args:
        json_structure: Expected JSON output structure.

    Returns:
        str: Complete prompt for AI generation.
    """
    return f"""【重要】你必须且只能返回 JSON 格式，禁止返回 Markdown、列表或其他任何格式。

你是一个专业的贷款申请表填写助手。请生成一份空白的贷款申请表数据。

## 重要规则
1. 所有字段都填写"待补充"
2. **必须返回纯 JSON 格式**，禁止返回 Markdown 格式

## 输出格式要求
请按以下 JSON 结构输出，每个分组作为一个对象：
{json_structure}

## 注意
- 只返回 JSON，不要有其他文字
- 所有字段值都是字符串类型
- 所有字段填写"待补充"

【再次强调】你的输出必须是以 {{ 开头、以 }} 结尾的纯 JSON，禁止输出任何 Markdown 格式。
"""


def build_generation_prompt(
    template: str, customer_data: dict, customer_found: bool, loan_type: str = "enterprise"
) -> str:
    """Build the prompt for AI to generate the application in JSON format.

    Args:
        template: The application template (for reference).
        customer_data: Customer data from Feishu (empty dict if not found).
        customer_found: Whether customer data was found.
        loan_type: Either "enterprise" or "personal".

    Returns:
        Complete prompt for AI generation (requesting JSON output).
    """
    json_structure = _get_personal_json_template() if loan_type == "personal" else _get_enterprise_json_template()

    if customer_found and customer_data:
        customer_json = json_module.dumps(customer_data, ensure_ascii=False, indent=2)
        return _build_filled_prompt(customer_json, json_structure)

    return _build_blank_prompt(json_structure)
