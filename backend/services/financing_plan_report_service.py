"""Financing plan selection, deterministic version diff and frozen reports.

This layer only reads frozen requirement, matching and plan records. It never
runs OCR, product matching, catalog parsing or an LLM.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, inspect as sa_inspect, select

from backend.database import Base, SessionLocal
from backend.db_models import (
    FinancingPlan, FinancingPlanCondition, FinancingPlanGap, FinancingPlanItem,
    FinancingPlanMaterial, FinancingPlanReportSnapshot, FinancingPlanSelection,
    FinancingPlanVersion, FinancingRequirement, ManualCandidateOverride,
    ProductMatchSnapshot,
)

logger = logging.getLogger(__name__)
DISCLAIMER = "方案为当前资料和产品规则下的融资规划建议，最终授信额度、期限、利率及审批结果以金融机构实际审核为准。"
_pdf_slots = asyncio.Semaphore(2)


class FinancingPlanReportError(ValueError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _load(value: str | None, fallback):
    try:
        parsed = json.loads(value or "")
        return parsed if isinstance(parsed, type(fallback)) else fallback
    except (TypeError, ValueError):
        return fallback


def format_report_amount_wan(value: Any, *, missing_label: str = "资料不足") -> str:
    """Format a stored yuan amount for business-facing reports without losing precision."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return missing_label
    amount_wan = Decimal(str(value)) / Decimal("10000")
    rendered = format(amount_wan, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return f"{rendered or '0'}万元"


def _display_count(value: Any) -> str:
    """Preserve a computed zero while keeping missing/uncomputed counts explicit."""
    if value is None:
        return "未计算"
    return str(value)


def _escape(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _customer_safe_text(value: Any) -> str:
    return re.sub(r"\b[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)+\b", "相关资料", str(value or ""), flags=re.I)


class FinancingPlanReportService:
    def __init__(self, session_factory=SessionLocal, *, ensure_schema: bool = True):
        self.session_factory = session_factory
        self.ensure_schema = ensure_schema

    def _prepare(self) -> None:
        if not self.ensure_schema:
            return
        bind = self.session_factory.kw.get("bind")
        if bind is not None:
            Base.metadata.create_all(bind, tables=[FinancingPlanSelection.__table__, FinancingPlanReportSnapshot.__table__])

    @staticmethod
    def _version_bundle(db, version_id: str) -> dict[str, Any]:
        version = db.scalar(select(FinancingPlanVersion).where(FinancingPlanVersion.plan_version_id == version_id))
        if version is None:
            raise LookupError(f"融资方案版本不存在：{version_id}")
        plan = db.scalar(select(FinancingPlan).where(FinancingPlan.financing_plan_id == version.financing_plan_id))
        items = list(db.scalars(select(FinancingPlanItem).where(FinancingPlanItem.plan_version_id == version_id).order_by(FinancingPlanItem.sequence_no, FinancingPlanItem.id)))
        conditions = list(db.scalars(select(FinancingPlanCondition).where(FinancingPlanCondition.plan_version_id == version_id).order_by(FinancingPlanCondition.sort_order, FinancingPlanCondition.id)))
        materials = list(db.scalars(select(FinancingPlanMaterial).where(FinancingPlanMaterial.plan_version_id == version_id).order_by(FinancingPlanMaterial.id)))
        gaps = list(db.scalars(select(FinancingPlanGap).where(FinancingPlanGap.plan_version_id == version_id).order_by(FinancingPlanGap.id)))
        return {
            "plan": plan,
            "version": version,
            "items": items,
            "conditions": conditions,
            "materials": materials,
            "gaps": gaps,
        }

    def create_selection(self, *, customer_id: str, requirement_id: str,
                         primary_plan_version_id: str | None,
                         backup_plan_version_ids: list[str],
                         conditional_plan_version_ids: list[str],
                         selected_by: str, notes: str = "") -> dict[str, Any]:
        self._prepare()
        backup = list(dict.fromkeys(backup_plan_version_ids))
        conditional = list(dict.fromkeys(conditional_plan_version_ids))
        chosen = ([primary_plan_version_id] if primary_plan_version_id else []) + backup + conditional
        if len(chosen) != len(set(chosen)):
            raise FinancingPlanReportError("同一方案版本不能同时承担多个方案角色")
        with self.session_factory.begin() as db:
            requirement = db.scalar(select(FinancingRequirement).where(
                FinancingRequirement.requirement_id == requirement_id,
                FinancingRequirement.customer_id == customer_id,
            ))
            if requirement is None or requirement.status != "confirmed":
                raise FinancingPlanReportError("融资需求必须处于已确认状态")
            for version_id in chosen:
                bundle = self._version_bundle(db, version_id)
                plan, version = bundle["plan"], bundle["version"]
                if plan is None or plan.customer_id != customer_id or plan.requirement_id != requirement_id:
                    raise FinancingPlanReportError("所选方案版本不属于当前客户融资需求")
                if version.status != "confirmed":
                    raise FinancingPlanReportError("只有已校验并确认的方案版本可以进入定稿选择")
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            row = FinancingPlanSelection(
                selection_id=uuid.uuid4().hex, customer_id=customer_id, requirement_id=requirement_id,
                primary_plan_version_id=primary_plan_version_id,
                backup_plan_version_ids_json=_json(backup), conditional_plan_version_ids_json=_json(conditional),
                status="draft", selected_by=selected_by, selected_at=now, notes=notes,
            )
            db.add(row)
            db.flush()
            selection_id = row.selection_id
        return self.get_selection(selection_id)

    def finalize_selection(self, selection_id: str, *, actor: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            row = db.scalar(select(FinancingPlanSelection).where(FinancingPlanSelection.selection_id == selection_id))
            if row is None:
                raise LookupError("方案定稿记录不存在")
            if row.status == "finalized":
                return self.get_selection(selection_id)
            if row.status != "draft":
                raise FinancingPlanReportError("当前方案定稿状态不允许确认")
            row.status = "finalized"
            row.finalized_by = actor
            row.finalized_at = datetime.now(timezone.utc).replace(tzinfo=None)
        return self.get_selection(selection_id)

    def get_selection(self, selection_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(FinancingPlanSelection).where(FinancingPlanSelection.selection_id == selection_id))
            if row is None:
                raise LookupError("方案定稿记录不存在")
            return {
                "selection_id": row.selection_id, "customer_id": row.customer_id,
                "requirement_id": row.requirement_id, "primary_plan_version_id": row.primary_plan_version_id,
                "backup_plan_version_ids": _load(row.backup_plan_version_ids_json, []),
                "conditional_plan_version_ids": _load(row.conditional_plan_version_ids_json, []),
                "status": row.status, "selected_by": row.selected_by,
                "selected_at": row.selected_at.isoformat() if row.selected_at else None,
                "finalized_by": row.finalized_by,
                "finalized_at": row.finalized_at.isoformat() if row.finalized_at else None,
                "notes": row.notes,
            }

    def compare_plan_versions(self, version_a: str, version_b: str) -> dict[str, Any]:
        with self.session_factory() as db:
            a, b = self._version_bundle(db, version_a), self._version_bundle(db, version_b)
            if a["plan"] is None or b["plan"] is None or a["plan"].customer_id != b["plan"].customer_id:
                raise FinancingPlanReportError("只能比较同一客户的融资方案版本")
            va, vb = a["version"], b["version"]
            amount_changes = {
                key: {"from": str(getattr(va, key)), "to": str(getattr(vb, key))}
                for key in ("target_amount", "covered_amount", "funding_gap")
                if Decimal(getattr(va, key)) != Decimal(getattr(vb, key))
            }
            ia = {x.product_version_id: x for x in a["items"]}; ib = {x.product_version_id: x for x in b["items"]}
            product_changes = {
                "added": [self._item_public(ib[k]) for k in sorted(ib.keys() - ia.keys())],
                "removed": [self._item_public(ia[k]) for k in sorted(ia.keys() - ib.keys())],
                "changed": [],
            }
            for key in sorted(ia.keys() & ib.keys()):
                changes = {}
                for field in ("proposed_amount", "proposed_term_months", "item_role"):
                    left, right = getattr(ia[key], field), getattr(ib[key], field)
                    if str(left) != str(right): changes[field] = {"from": str(left), "to": str(right)}
                if changes: product_changes["changed"].append({"product_version_id": key, "product_name": ib[key].product_name, "changes": changes})
            condition_changes = self._row_diff(a["conditions"], b["conditions"], "title", "status")
            material_changes = self._row_diff(a["materials"], b["materials"], "material_name", "status")
            gap_changes = self._row_diff(a["gaps"], b["gaps"], "description", "severity")
            count_changes = {
                "product_count": {"from": len(a["items"]), "to": len(b["items"])},
                "institution_count": {"from": len({x.institution_name for x in a["items"]}), "to": len({x.institution_name for x in b["items"]})},
            }
            changed_groups = sum(bool(value) for value in (amount_changes, product_changes["added"], product_changes["removed"], product_changes["changed"], condition_changes["added"], condition_changes["removed"], condition_changes["changed"], material_changes["added"], material_changes["removed"], material_changes["changed"], gap_changes["added"], gap_changes["removed"], gap_changes["changed"]))
            return {
                "customer_id": a["plan"].customer_id,
                "from_version": {"id": version_a, "version_no": va.version_no},
                "to_version": {"id": version_b, "version_no": vb.version_no},
                "amount_changes": amount_changes, "product_changes": product_changes,
                "count_changes": count_changes,
                "condition_changes": condition_changes, "material_changes": material_changes,
                "gap_changes": gap_changes,
                "summary": f"V{va.version_no}至V{vb.version_no}共涉及{changed_groups}类结构化变化。" if changed_groups else "两个版本的结构化内容一致。",
            }

    @staticmethod
    def _row_diff(left, right, identity: str, status: str) -> dict[str, Any]:
        a = {str(getattr(x, identity)): x for x in left}; b = {str(getattr(x, identity)): x for x in right}
        return {
            "added": sorted(b.keys() - a.keys()), "removed": sorted(a.keys() - b.keys()),
            "changed": [{"name": key, "from": str(getattr(a[key], status)), "to": str(getattr(b[key], status))}
                        for key in sorted(a.keys() & b.keys()) if str(getattr(a[key], status)) != str(getattr(b[key], status))],
        }

    @staticmethod
    def _item_public(row) -> dict[str, Any]:
        return {"product_version_id": row.product_version_id, "product_code": row.external_product_code,
                "institution_name": row.institution_name, "product_name": row.product_name,
                "proposed_amount": str(row.proposed_amount), "proposed_term_months": row.proposed_term_months,
                "item_role": row.item_role}

    def _build_report_payload(self, db, selection: FinancingPlanSelection, report_type: str) -> dict[str, Any]:
        requirement = db.scalar(select(FinancingRequirement).where(FinancingRequirement.requirement_id == selection.requirement_id))
        if requirement is None: raise LookupError("融资需求不存在")
        snapshot = db.scalar(select(ProductMatchSnapshot).where(
            ProductMatchSnapshot.customer_id == selection.customer_id,
            ProductMatchSnapshot.requirement_id == selection.requirement_id,
        ).order_by(ProductMatchSnapshot.generated_at.desc(), ProductMatchSnapshot.id.desc()))
        if snapshot is None: raise FinancingPlanReportError("缺少来源产品匹配快照")
        ids = ([selection.primary_plan_version_id] if selection.primary_plan_version_id else []) + _load(selection.backup_plan_version_ids_json, []) + _load(selection.conditional_plan_version_ids_json, [])
        roles = ({selection.primary_plan_version_id: "primary"} if selection.primary_plan_version_id else {})
        roles.update({x: "backup" for x in _load(selection.backup_plan_version_ids_json, [])})
        roles.update({x: "conditional" for x in _load(selection.conditional_plan_version_ids_json, [])})
        plans = []
        for version_id in ids:
            bundle = self._version_bundle(db, version_id); version = bundle["version"]
            plans.append({
                "role": roles[version_id], "plan_version_id": version_id, "version_no": version.version_no,
                "generation_status": version.generation_status, "target_amount": str(version.target_amount),
                "covered_amount": str(version.covered_amount), "funding_gap": str(version.funding_gap),
                "items": [self._item_public(x) for x in bundle["items"]],
                "conditions": [{"title": x.title, "status": x.status, "required": bool(x.required)} for x in bundle["conditions"]],
                "materials": [{"name": x.material_name, "status": x.status, "required": bool(x.required)} for x in bundle["materials"]],
                "gaps": [{"type": x.gap_type, "description": x.description, "severity": x.severity} for x in bundle["gaps"]],
                "created_by": version.created_by, "confirmed_by": version.confirmed_by,
                "created_at": version.created_at.isoformat() if version.created_at else None,
                "confirmed_at": version.confirmed_at.isoformat() if version.confirmed_at else None,
            })
        summary = _load(snapshot.summary_json, {})
        facts = _load(snapshot.facts_json, {})
        missing_primary = selection.primary_plan_version_id is None
        next_actions = (["完成人工复核候选产品", "完善产品规则", "补充必要资料", "重新执行匹配和组合"] if missing_primary else ["按条件清单准备资料", "提交金融机构进一步审核"])
        payload = {
            "report_type": report_type, "title": "融资规划方案", "customer_id": selection.customer_id,
            "customer_name": requirement.borrower_entity or selection.customer_id,
            "requirement": {"amount": str(requirement.requested_amount or 0), "purpose": requirement.purpose_detail or requirement.financing_purpose or "未填写", "term": f"{requirement.term_value or 0}{'个月' if requirement.term_unit == 'month' else requirement.term_unit or ''}", "version": requirement.version},
            "matching_summary": summary, "data_quality": facts.get("data_quality", {}),
            "plans": plans, "primary_status": "暂未形成正式主方案" if missing_primary else "已选定主方案",
            "next_actions": next_actions, "disclaimer": DISCLAIMER,
            "selection": {"id": selection.selection_id, "status": selection.status, "selected_by": selection.selected_by, "selected_at": selection.selected_at.isoformat() if selection.selected_at else None},
            "_frozen_source": {"requirement_version": requirement.version, "match_snapshot_id": snapshot.snapshot_id, "facts_hash": snapshot.facts_hash, "catalog_hash": snapshot.catalog_version_hash},
        }
        if report_type == "internal":
            payload["source"] = dict(payload["_frozen_source"])
            payload["data_overview"] = {key: facts.get(key, {}) for key in ("credit", "financial", "cashflow", "asset")}
            payload["audit"] = {
                "selection_notes": selection.notes,
                "manual_overrides": [{"product_version_id": x.product_version_id, "operator": x.operator_name, "reason": x.reason, "created_at": x.created_at.isoformat() if x.created_at else None}
                                     for x in db.scalars(select(ManualCandidateOverride).where(ManualCandidateOverride.match_snapshot_id == snapshot.snapshot_id))],
            }
        else:
            payload["selection"] = {"status": selection.status}
            for plan in payload["plans"]:
                plan.pop("plan_version_id", None)
                plan.pop("created_by", None); plan.pop("confirmed_by", None)
                plan.pop("created_at", None); plan.pop("confirmed_at", None)
                for item in plan["items"]:
                    item.pop("product_version_id", None)
                for condition in plan["conditions"]:
                    condition["title"] = _customer_safe_text(condition["title"])
                for material in plan["materials"]:
                    material["name"] = _customer_safe_text(material["name"])
                for gap in plan["gaps"]:
                    gap["description"] = _customer_safe_text(gap["description"])
        return payload

    def generate_report(self, selection_id: str, report_type: str, *, generated_by: str) -> dict[str, Any]:
        if report_type not in {"internal", "customer"}: raise FinancingPlanReportError("报告类型仅支持 internal 或 customer")
        self._prepare()
        with self.session_factory.begin() as db:
            selection = db.scalar(select(FinancingPlanSelection).where(FinancingPlanSelection.selection_id == selection_id))
            if selection is None: raise LookupError("方案定稿记录不存在")
            payload = self._build_report_payload(db, selection, report_type)
            report_version = int(db.scalar(select(func.max(FinancingPlanReportSnapshot.report_version)).where(
                FinancingPlanReportSnapshot.plan_selection_id == selection_id,
                FinancingPlanReportSnapshot.report_type == report_type,
            )) or 0) + 1
            source = payload.pop("_frozen_source")
            rendered = render_financing_plan_html(payload)
            row = FinancingPlanReportSnapshot(
                report_id=uuid.uuid4().hex, plan_selection_id=selection_id,
                primary_plan_version_id=selection.primary_plan_version_id,
                report_type=report_type, report_version=report_version,
                structured_payload_json=_json(payload), rendered_html=rendered,
                source_requirement_version=source["requirement_version"], source_match_snapshot_id=source["match_snapshot_id"],
                source_facts_hash=source["facts_hash"], source_catalog_hash=source["catalog_hash"],
                generated_by=generated_by, generated_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
            db.add(row); db.flush(); report_id = row.report_id
        return self.get_report(report_id)

    def get_report(self, report_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(FinancingPlanReportSnapshot).where(FinancingPlanReportSnapshot.report_id == report_id))
            if row is None: raise LookupError("融资方案报告不存在")
            return {"report_id": row.report_id, "plan_selection_id": row.plan_selection_id,
                    "primary_plan_version_id": row.primary_plan_version_id, "report_type": row.report_type,
                    "report_version": row.report_version, "structured_payload": _load(row.structured_payload_json, {}),
                    "rendered_html": row.rendered_html, "source_requirement_version": row.source_requirement_version,
                    "source_match_snapshot_id": row.source_match_snapshot_id, "source_facts_hash": row.source_facts_hash,
                    "source_catalog_hash": row.source_catalog_hash, "generated_by": row.generated_by,
                    "generated_at": row.generated_at.isoformat() if row.generated_at else None}


def render_financing_plan_html(payload: dict[str, Any]) -> str:
    e = _escape
    role_labels = {"primary": "主方案", "backup": "备选方案", "conditional": "条件性方案"}
    status_labels = {"pending": "待完成", "satisfied": "已满足", "waived": "已豁免", "not_applicable": "不适用", "missing": "缺失", "available": "已有", "uploaded": "已上传", "verified": "已核验"}
    plan_html = []
    for plan in payload.get("plans", []):
        rows = "".join(f"<tr><td>{e(x['institution_name'])}</td><td>{e(x['product_name'])}</td><td>{format_report_amount_wan(x.get('proposed_amount'))}</td><td>{e(x['proposed_term_months'])}个月</td></tr>" for x in plan["items"])
        conditions = "".join(f"<li>{e(x['title'])}（{e(status_labels.get(x['status'], '待确认'))}）</li>" for x in plan["conditions"]) or "<li>暂无</li>"
        materials = "".join(f"<li>{e(x['name'])}（{e(status_labels.get(x['status'], '待确认'))}）</li>" for x in plan["materials"]) or "<li>暂无</li>"
        gaps = "".join(f"<li>{e(x['description'])}</li>" for x in plan["gaps"]) or "<li>当前未记录额外缺口</li>"
        plan_html.append(f"<section class='card'><h2>{role_labels.get(plan['role'], e(plan['role']))}</h2><p>目标金额：{format_report_amount_wan(plan.get('target_amount'))}　覆盖金额：{format_report_amount_wan(plan.get('covered_amount'))}　未覆盖金额：{format_report_amount_wan(plan.get('funding_gap'))}</p><table><thead><tr><th>机构</th><th>产品</th><th>规划金额</th><th>规划期限</th></tr></thead><tbody>{rows}</tbody></table><h3>当前条件</h3><ul>{conditions}</ul><h3>材料清单</h3><ul>{materials}</ul><h3>融资缺口与风险</h3><ul>{gaps}</ul></section>")
    if not plan_html:
        plan_html.append(f"<section class='card empty'><h2>暂未形成正式主方案</h2><p>当前产品库中尚无可直接形成正式融资方案的产品组合。</p><p>目标金额：{format_report_amount_wan(payload['requirement'].get('amount'))}　当前覆盖：{format_report_amount_wan(0)}　当前缺口：{format_report_amount_wan(payload['requirement'].get('amount'))}</p></section>")
    audit = ""
    internal_summary = ""
    if payload.get("report_type") == "internal":
        audit_data = payload.get("audit", {})
        matching = payload.get("matching_summary", {})
        internal_summary = f"<section><h2>当前数据与产品匹配摘要</h2><p>符合当前已知硬条件：{e(_display_count(matching.get('eligible')))}　条件性匹配：{e(_display_count(matching.get('conditional')))}　不符合明确硬条件：{e(_display_count(matching.get('ineligible')))}　需要人工复核：{e(_display_count(matching.get('manual_review')))}　产品配置异常：{e(_display_count(matching.get('product_configuration_error')))}</p><p>数据质量信息已按生成时点冻结在报告快照中。</p></section>"
        audit = f"<section><h2>版本与审计信息</h2><p>选择记录：{e(payload['selection']['id'])}　操作人：{e(payload['selection']['selected_by'])}</p><p>来源匹配快照：{e(payload['source']['match_snapshot_id'])}</p><p>人工放行记录：{len(audit_data.get('manual_overrides', []))}条</p></section>"
    actions = "".join(f"<li>{e(x)}</li>" for x in payload.get("next_actions", []))
    return f"""<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'><style>
@page{{size:A4;margin:16mm}}*{{box-sizing:border-box}}body{{font-family:'Microsoft YaHei','Noto Sans CJK SC',sans-serif;color:#1f2937;font-size:12px;line-height:1.65}}h1{{font-size:24px}}h2{{font-size:17px;border-bottom:1px solid #ddd;padding-bottom:4px}}h3{{font-size:13px}}.meta,.card{{border:1px solid #d1d5db;border-radius:8px;padding:12px;margin:12px 0;break-inside:avoid}}table{{width:100%;border-collapse:collapse;table-layout:fixed}}th,td{{border:1px solid #ddd;padding:6px;word-break:break-word}}.notice{{margin-top:18px;color:#64748b;border-top:1px solid #ddd;padding-top:10px}}.empty{{background:#fffbeb}}
</style></head><body><h1>{e(payload['title'])}</h1><div class='meta'><p>客户：{e(payload['customer_name'])}</p><p>融资需求：{format_report_amount_wan(payload['requirement'].get('amount'))}　用途：{e(payload['requirement']['purpose'])}　期限：{e(payload['requirement']['term'])}</p><p>主方案状态：{e(payload['primary_status'])}</p></div>{internal_summary}{''.join(plan_html)}<section><h2>下一步</h2><ol>{actions}</ol></section>{audit}<p class='notice'>{e(payload['disclaimer'])}</p></body></html>"""


async def render_financing_plan_pdf(rendered_html: str) -> bytes:
    try:
        from playwright.async_api import async_playwright
        async with _pdf_slots, async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                context = await browser.new_context(java_script_enabled=False, service_workers="block")
                await context.route("**/*", lambda route: route.abort())
                page = await context.new_page(); await page.set_content(rendered_html, wait_until="load", timeout=30000)
                await page.emulate_media(media="print"); await page.evaluate("document.fonts.ready")
                return await page.pdf(format="A4", print_background=True, prefer_css_page_size=True)
            finally:
                await browser.close()
    except Exception:
        logger.exception("Financing plan PDF renderer unavailable")
        raise FinancingPlanReportError("PDF生成暂不可用，请检查浏览器渲染环境") from None


def validate_llm_report_enhancement(base: dict[str, Any], enhanced: dict[str, Any]) -> bool:
    """Validate a future wording-only enhancement against frozen business data."""
    def signature(payload: dict[str, Any]):
        return [(
            plan.get("role"), str(plan.get("target_amount")), str(plan.get("covered_amount")),
            str(plan.get("funding_gap")),
            sorted((item.get("product_code"), item.get("institution_name"), item.get("product_name"),
                    str(item.get("proposed_amount")), str(item.get("proposed_term_months")))
                   for item in plan.get("items", [])),
        ) for plan in payload.get("plans", [])]
    return signature(base) == signature(enhanced) and base.get("requirement") == enhanced.get("requirement")


def use_validated_llm_enhancement(base: dict[str, Any], enhanced: dict[str, Any] | None) -> dict[str, Any]:
    """Fall back to the deterministic template payload on any inconsistency."""
    return enhanced if enhanced is not None and validate_llm_report_enhancement(base, enhanced) else base
