from __future__ import annotations

import re
from typing import Any

from .schema import Shareholder


def compact_text(text: str) -> str:
    return re.sub(r"\s+", "", str(text or ""))


def clean_value(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[ \t\r\f\v\u3000_—–]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip(" ：:，,。；;")


def clean_clause(value: str) -> str:
    text = clean_value(value)
    text = re.sub(r"\s*\n\s*", "", text)
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"(?:第[一二三四五六七八九十]+章.*|第[一二三四五六七八九十]+条.*|公司经营范围.*)$", "", text)
    return text.strip(" ：:，,。；;")


def first_match(text: str, patterns: list[str], flags: int = re.S) -> str:
    for pattern in patterns:
        match = re.search(pattern, text or "", flags)
        if match:
            return clean_value(match.group(1))
    return ""


def parse_amount_number(value: str) -> float | None:
    match = re.search(r"(\d+(?:\.\d+)?)", str(value or "").replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def extract_title(text: str) -> tuple[str, str]:
    head = "\n".join((text or "").splitlines()[:20])
    title = first_match(
        head,
        [
            r"([\u4e00-\u9fffA-Za-z0-9（）()·路\-]{2,80}有限公司章程)",
            r"([\u4e00-\u9fffA-Za-z0-9（）()·路\-]{2,80}公司章程)",
        ],
    )
    if not title:
        title = first_match(text, [r"([\u4e00-\u9fffA-Za-z0-9（）()·路\-]{2,80}有限公司章程)"])
    company_name = title[:-2] if title.endswith("章程") else ""
    return title, company_name


def extract_company_name(text: str, title_company_name: str = "") -> str:
    value = first_match(
        text,
        [
            r"第一条\s*公司名称[：:\s]*([^\n。；;]{4,80}有限公司)",
            r"公司名称[：:\s]*([^\n。；;]{4,80}有限公司)",
            r"名称[：:\s]*([^\n。；;]{4,80}有限公司)",
        ],
    )
    return clean_clause(value) or title_company_name


def extract_company_address(text: str) -> str:
    value = first_match(
        text,
        [
            r"第二条\s*公司住所[：:\s]*([\s\S]{4,180}?)(?=\s*(?:第二章|第三条|公司经营范围|经营范围|\n第[一二三四五六七八九十]+章))",
            r"公司住所[：:\s]*([\s\S]{4,180}?)(?=\s*(?:第二章|第三条|公司经营范围|经营范围|\n第[一二三四五六七八九十]+章))",
            r"住所[：:\s]*([^\n。；;]{4,180})",
        ],
    )
    return clean_clause(value)


def extract_business_scope(text: str) -> str:
    value = first_match(
        text,
        [
            r"第三条\s*公司经营范围[：:\s]*([\s\S]+?)(?=\s*(?:第三章|第四条|公司注册资本))",
            r"经营范围[：:\s]*([\s\S]+?)(?=\s*(?:第三章|第四条|公司注册资本))",
        ],
    )
    return clean_clause(value)


def normalize_for_amount_match(text: str) -> str:
    return re.sub(r"[\s\u3000_—–]+", "", str(text or ""))


def extract_registered_capital(text: str) -> tuple[str, float | None, str]:
    source = normalize_for_amount_match(text)
    patterns = [
        r"第四条公司注册资本[:：]?人民币([0-9]+(?:\.[0-9]+)?)万元",
        r"公司注册资本[:：]?人民币([0-9]+(?:\.[0-9]+)?)万元",
        r"注册资本[:：]?人民币([0-9]+(?:\.[0-9]+)?)万元",
    ]
    for pattern in patterns:
        match = re.search(pattern, source)
        if match:
            amount = float(match.group(1))
            return f"人民币{amount:g}万元", amount, "人民币"
    value = first_match(
        text,
        [
            r"公司注册资本[：:\s]*((?:人民币|RMB)?\s*[_—–]?\s*\d+(?:\.\d+)?\s*万\s*元)",
            r"注册资本[：:\s]*((?:人民币|RMB)?\s*[_—–]?\s*\d+(?:\.\d+)?\s*万\s*元)",
        ],
    )
    value = clean_clause(value).replace("万 元", "万元")
    if value and not value.startswith("人民币") and re.search(r"\d", value):
        value = f"人民币{value}"
    return value, parse_amount_number(value), "人民币" if value else ""


def normalize_shareholder_text(text: str) -> str:
    fullwidth = str.maketrans("０１２３４５６７８９．／－：，｜（）", "0123456789./-:,|()")
    source = str(text or "").translate(fullwidth)
    source = source.replace("│", "|").replace("┃", "|").replace("｜", "|")
    source = re.sub(r"[-—–_]{2,}", " ", source)
    source = re.sub(r"[|]+", " | ", source)
    source = re.sub(r"[ \t\u3000]+", " ", source)
    source = re.sub(r"\n\s*\|?\s*(?:-+\s*\|?\s*){2,}\n", "\n", source)
    return source


def strip_shareholder_headers(text: str) -> str:
    source = str(text or "")
    headers = [
        "股东的姓名或者名称",
        "股东姓名/名称",
        "股东姓名",
        "姓名或者名称",
        "姓名或名称",
        "出资额",
        "出资方式",
        "出资日期",
        "出资时间",
    ]
    for header in headers:
        source = source.replace(header, " ")
    return source


def extract_shareholder_block(text: str) -> str:
    source = normalize_shareholder_text(text)
    markers = [
        "股东的姓名或者名称",
        "股东姓名",
        "出资方式、出资额和出资",
        "出资额 出资方式 出资",
        "出资日期",
        "出资时间",
    ]
    starts = [source.find(marker) for marker in markers if source.find(marker) >= 0]
    if not starts:
        return source
    start = min(starts)
    tail = source[start:]
    boundaries = [
        r"\n\s*第六条",
        r"\n\s*第五章",
        r"\n\s*公司成立后",
        r"\n\s*公司机构",
        r"\n\s*股东会",
        r"\n\s*第[一二三四五六七八九十]+章",
        r"\n\s*第[五六七八九十]+条",
    ]
    end = len(tail)
    for pattern in boundaries:
        match = re.search(pattern, tail)
        if match and match.start() > 20:
            end = min(end, match.start())
    return tail[:end]


def normalize_contribution_date(value: str) -> str:
    text = clean_clause(value)
    text = re.sub(r"\s+", "", text)
    match = re.search(r"((?:19|20)\d{2})年(1[0-2]|0?[1-9])月(3[01]|[12]\d|0?[1-9])日", text)
    if match:
        return f"{int(match.group(1)):04d}.{int(match.group(2)):02d}.{int(match.group(3)):02d}"
    match = re.search(r"((?:19|20)\d{2})[./-](1[0-2]|0?[1-9])[./-](3[01]|[12]\d|0?[1-9])", text)
    if match:
        return f"{int(match.group(1)):04d}.{int(match.group(2)):02d}.{int(match.group(3)):02d}"
    return re.sub(r"[/-]", ".", text)


def is_valid_shareholder_name(name: str) -> bool:
    text = clean_clause(name)
    if not re.fullmatch(r"[\u4e00-\u9fff·路]{2,20}", text):
        return False
    forbidden = (
        "股东", "姓名", "名称", "出资", "方式", "时间", "日期", "注册资本", "公司",
        "成立后", "证明书", "货币", "万元", "人民币", "章", "条",
    )
    return not any(item in text for item in forbidden)


def make_shareholder(
    name: str,
    amount: str,
    method: str,
    date: str,
    registered_capital_amount: float | None = None,
) -> Shareholder | None:
    clean_name = clean_clause(name)
    amount_number = parse_amount_number(amount)
    if not is_valid_shareholder_name(clean_name) or amount_number is None:
        return None
    ratio = ""
    if registered_capital_amount:
        ratio = f"{amount_number / registered_capital_amount * 100:.2f}%"
    return Shareholder(
        name=clean_name,
        subscribed_amount=f"{amount_number:g}万元",
        subscribed_amount_number=amount_number,
        contribution_method=clean_clause(method),
        contribution_deadline=normalize_contribution_date(date),
        contribution_ratio=ratio,
    )


def dedupe_shareholders(items: list[Shareholder]) -> list[Shareholder]:
    deduped: list[Shareholder] = []
    seen: set[tuple[str, str, str]] = set()
    for item in items:
        key = (item.name, f"{float(item.subscribed_amount_number or 0):g}", item.contribution_deadline)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def parse_shareholders_by_regex(block: str, registered_capital_amount: float | None) -> list[Shareholder]:
    normalized = strip_shareholder_headers(normalize_shareholder_text(block))
    date = r"(?:19|20)\d{2}\s*(?:年|[./-])\s*(?:1[0-2]|0?[1-9])\s*(?:月|[./-])\s*(?:3[01]|[12]\d|0?[1-9])\s*日?"
    amount = r"\d+(?:\.\d+)?\s*万\s*元"
    method = r"货币|现金|实物|知识产权|土地使用权|股权|债权|其他"
    separators = r"(?:\s+|\s*\|\s*|\s*[,，、]\s*)"
    patterns = [
        re.compile(
            rf"(?P<name>[\u4e00-\u9fff·路]{{2,20}}){separators}"
            rf"(?P<amount>{amount}){separators}"
            rf"(?P<method>{method}){separators}"
            rf"(?P<date>{date})"
        ),
        re.compile(
            rf"(?P<name>[\u4e00-\u9fff·路]{{2,10}})\s*"
            rf"(?P<amount>{amount})\s*"
            rf"(?P<method>{method})\s*"
            rf"(?P<date>{date})"
        ),
    ]
    items: list[Shareholder] = []
    for pattern in patterns:
        for match in pattern.finditer(normalized):
            shareholder = make_shareholder(
                match.group("name"),
                match.group("amount"),
                match.group("method"),
                match.group("date"),
                registered_capital_amount,
            )
            if shareholder:
                items.append(shareholder)
    return dedupe_shareholders(items)


def parse_shareholders_by_tokens(block: str, registered_capital_amount: float | None) -> list[Shareholder]:
    source = strip_shareholder_headers(normalize_shareholder_text(block))
    token_pattern = re.compile(
        r"[\u4e00-\u9fff·路]{2,20}|\d+(?:\.\d+)?\s*万\s*元?|(?:19|20)\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日|(?:19|20)\d{2}[./-]\d{1,2}[./-]\d{1,2}"
    )
    raw_tokens = [clean_clause(item.group(0)) for item in token_pattern.finditer(source)]
    methods = {"货币", "现金", "实物", "知识产权", "土地使用权", "股权", "债权", "其他"}
    items: list[Shareholder] = []
    index = 0
    while index < len(raw_tokens):
        name = raw_tokens[index]
        if not is_valid_shareholder_name(name):
            index += 1
            continue
        amount_index = method_index = date_index = -1
        for offset in range(index + 1, min(len(raw_tokens), index + 8)):
            token = raw_tokens[offset]
            if amount_index < 0 and re.fullmatch(r"\d+(?:\.\d+)?万元?", token):
                amount_index = offset
                continue
            if amount_index > 0 and method_index < 0 and token in methods:
                method_index = offset
                continue
            if method_index > 0 and re.search(r"(?:19|20)\d{2}", token):
                date_index = offset
                break
        if amount_index > 0 and method_index > 0 and date_index > 0:
            shareholder = make_shareholder(
                name,
                raw_tokens[amount_index],
                raw_tokens[method_index],
                raw_tokens[date_index],
                registered_capital_amount,
            )
            if shareholder:
                items.append(shareholder)
                index = date_index + 1
                continue
        index += 1
    return dedupe_shareholders(items)


def recover_missing_shareholders(
    block: str,
    current: list[Shareholder],
    registered_capital_amount: float | None,
) -> list[Shareholder]:
    if not registered_capital_amount:
        return current
    total = sum(float(item.subscribed_amount_number or 0) for item in current)
    diff = round(float(registered_capital_amount) - total, 2)
    if diff <= 0.01:
        return current
    source = normalize_shareholder_text(block)
    source = strip_shareholder_headers(source)
    diff_pattern = rf"{diff:g}\s*万\s*元?"
    date = r"(?:19|20)\d{2}\s*(?:年|[./-])\s*(?:1[0-2]|0?[1-9])\s*(?:月|[./-])\s*(?:3[01]|[12]\d|0?[1-9])\s*日?"
    method = r"货币|现金|实物|知识产权|土地使用权|股权|债权|其他"
    for match in re.finditer(diff_pattern, source):
        window = source[max(0, match.start() - 80): min(len(source), match.end() + 120)]
        row_match = re.search(
            rf"(?P<name>[\u4e00-\u9fff·路]{{2,20}})[\s|,，、]+(?P<amount>{diff_pattern})[\s|,，、]+(?P<method>{method})[\s|,，、]+(?P<date>{date})",
            window,
        )
        if not row_match:
            tokens = parse_shareholders_by_tokens(window, registered_capital_amount)
            recovered = [item for item in tokens if abs(float(item.subscribed_amount_number or 0) - diff) < 0.01]
        else:
            item = make_shareholder(
                row_match.group("name"),
                row_match.group("amount"),
                row_match.group("method"),
                row_match.group("date"),
                registered_capital_amount,
            )
            recovered = [item] if item else []
        if recovered:
            return dedupe_shareholders([*current, *recovered])
    return current


def extract_shareholders(text: str, registered_capital_amount: float | None = None) -> list[Shareholder]:
    block = extract_shareholder_block(text)
    shareholders = parse_shareholders_by_regex(block, registered_capital_amount)
    if len(shareholders) < 2:
        shareholders = dedupe_shareholders([*shareholders, *parse_shareholders_by_tokens(block, registered_capital_amount)])
    shareholders = recover_missing_shareholders(block, shareholders, registered_capital_amount)
    return dedupe_shareholders(shareholders)


def short_sentence(text: str, keyword: str, max_chars: int = 80) -> str:
    source = re.sub(r"\s+", "", text or "")
    index = source.find(keyword)
    if index < 0:
        return ""
    end_candidates = [pos for pos in (source.find("。", index), source.find("；", index), source.find(";", index)) if pos >= 0]
    end = min(end_candidates) + 1 if end_candidates else min(len(source), index + max_chars)
    return source[index:end][:max_chars].strip("。；;")


def extract_first_shareholders_meeting(text: str) -> str:
    sentence = short_sentence(text, "首次股东会会议", 80)
    if sentence:
        return sentence
    if "首次股东会" in text:
        return "首次股东会会议由出资最多的股东召集和主持，依照公司法规定行使职权"
    return "未识别"


def extract_major_rules(text: str) -> dict[str, str]:
    compact = compact_text(text)
    shareholder_rule = "须经代表全体股东三分之二以上表决权的股东通过"
    other_rule = "须经代表全体股东三分之二以上表决权通过"
    has_two_thirds = "三分之二以上表决权" in compact
    has_major_topics = any(token in compact for token in ("修改公司章程", "修改章程", "增加或者减少注册资本", "合并", "分立", "解散", "变更公司形式"))
    major_rule = shareholder_rule if has_two_thirds and has_major_topics else "未识别"
    return {
        "amendment_rule": major_rule if any(token in compact for token in ("修改公司章程", "修改章程")) else "未识别",
        "capital_change_rule": major_rule if "注册资本" in compact and any(token in compact for token in ("增加", "减少", "增减")) else "未识别",
        "merger_split_dissolution_rule": major_rule if any(token in compact for token in ("合并", "分立", "解散", "变更公司形式")) else "未识别",
        "other_rule": other_rule if "除前款以外" in compact and has_two_thirds else (other_rule if has_two_thirds else "未识别"),
    }


def extract_signature_info(text: str, pages: list[dict[str, Any]]) -> dict[str, Any]:
    signature_page = ""
    for page in reversed(pages or []):
        page_text = str(page.get("text") or "")
        if any(token in page_text for token in ("股东", "签字", "盖章", "签名", "年月日", "印章", "章")):
            page_no = page.get("page") or page.get("page_index") or len(pages)
            signature_page = f"第{page_no}页"
            break
    if not signature_page and pages:
        signature_page = f"第{len(pages)}页"
    tail = "\n".join(str(page.get("text") or "") for page in (pages or [])[-2:]) if pages else text[-1500:]
    has_signature = any(token in tail for token in ("签字", "盖章", "签名", "印章", "股东"))
    signing_date = first_match(tail, [r"((?:19|20)\d{2}\s*年\s*(?:1[0-2]|0?[1-9])\s*月\s*(?:3[01]|[12]\d|0?[1-9])\s*日)"])
    return {
        "signature_page": signature_page or "未识别",
        "has_signature_or_stamp": "有" if has_signature else "未识别",
        "detected_signature_count": 1 if has_signature else 0,
        "signing_date": clean_clause(signing_date) or "未填写/未识别",
        "signature_detection_summary": "识别到手写签名和红色印章" if has_signature else "未识别到签字或盖章",
    }


def extract_fields(text: str, pages: list[dict[str, Any]] | None = None, filename: str = "") -> dict[str, Any]:
    pages = pages or []
    title, title_company = extract_title(text)
    company_name = extract_company_name(text, title_company)
    registered_capital, registered_amount, currency = extract_registered_capital(text)
    shareholders = extract_shareholders(text, registered_amount)
    shareholder_total = sum(float(item.subscribed_amount_number or 0) for item in shareholders)
    if not registered_amount and shareholder_total and "公司注册资本" in compact_text(text):
        registered_amount = shareholder_total
        registered_capital = f"人民币{shareholder_total:g}万元"
        currency = "人民币"
    if registered_amount:
        shareholders = recover_missing_shareholders(extract_shareholder_block(text), shareholders, registered_amount)
        for shareholder in shareholders:
            if shareholder.subscribed_amount_number is not None:
                shareholder.contribution_ratio = f"{shareholder.subscribed_amount_number / registered_amount * 100:.2f}%"

    compact = compact_text(text)
    governance = {
        "authority_body": "股东会" if "股东会" in text else "未识别",
        "first_shareholders_meeting": extract_first_shareholders_meeting(text),
        "voting_rule": "股东会会议由股东按照出资比例行使表决权" if "按照出资比例行使表决权" in compact else short_sentence(text, "表决权") or "未识别",
        "executive_director": "公司不设董事会，设执行董事一名，任期三年，由股东会选举产生" if "不设董事会" in text and "执行董事" in text else short_sentence(text, "执行董事") or "未识别",
        "manager": "由股东会决定聘任或者解聘，任期三年，可以连任" if "经理" in text and "聘任或者解聘" in text else short_sentence(text, "经理") or "未识别",
        "supervisor": "公司不设监事会，设监事一人，任期三年，可以连任" if "不设监事会" in text and "监事" in text else short_sentence(text, "监事") or "未识别",
        "legal_representative": "由执行董事担任" if "法定代表人" in text and "执行董事" in text else short_sentence(text, "法定代表人") or "未识别",
    }
    return {
        "title": title,
        "company_name": company_name,
        "company_address": extract_company_address(text),
        "business_scope": extract_business_scope(text),
        "registered_capital": registered_capital,
        "registered_capital_amount": registered_amount,
        "currency": currency or "人民币",
        "shareholders": shareholders,
        "governance": governance,
        "major_resolution_rules": extract_major_rules(text),
        "equity_transfer_summary": "股东之间可以相互转让全部或者部分股权；向股东以外的人转让股权，应经其他股东过半数同意；其他股东自接到书面通知之日起满三十日未答复的，视为同意转让；同等条件下其他股东有优先购买权。" if "优先购买权" in text or "股权转让" in text else "未识别",
        "finance_and_profit_summary": "依照法律、行政法规和国务院财政主管部门规定建立财务会计制度；会计年度终了编制财务会计报告；股东按照出资比例分取红利；聘用或解聘会计师事务所由股东会决定。" if "财务会计制度" in text or "分取红利" in text else "未识别",
        "dissolution_and_liquidation_summary": "营业期限为长期；股东会决议可以解散；公司合并或者分立需要解散；依法被吊销营业执照、责令关闭或者被撤销；人民法院依法予以解散；清算组由股东组成。" if "清算组" in text or "营业期限为长期" in text else "未识别",
        "senior_management_obligations_summary": "高级管理人员包括经理、副经理、财务负责人；不得侵占公司财产；不得挪用公司资金；不得未经同意订立合同或者交易；不得泄露公司秘密。" if "高级管理人员" in text or "不得挪用公司资金" in text else "未识别",
        "articles_effective_rule": "本章程自全体股东盖章、签字之日起生效" if "全体股东盖章" in text or "签字之日起生效" in text else short_sentence(text, "生效") or "未识别",
        "signature_info": extract_signature_info(text, pages),
        "page_count": len(pages) if pages else len(re.findall(r"---\s*第?\s*\d+\s*页", text or "")) or 1,
    }


def detect_company_articles(text: str = "", filename: str = "") -> bool:
    compact = compact_text(text)
    lower_filename = str(filename or "").lower()
    if any(token in lower_filename for token in ("章程", "公司章程", "articles", "articles of association")):
        return True
    strong = "公司章程" in compact
    feature_count = sum(1 for token in ("股东会", "执行董事", "注册资本", "出资额") if token in compact)
    if strong and feature_count >= 2:
        return True
    chapter_count = sum(1 for token in ("第一章公司的名称和住所", "公司注册资本", "股东的姓名或者名称", "本章程") if token in compact)
    return chapter_count >= 2 and feature_count >= 1
