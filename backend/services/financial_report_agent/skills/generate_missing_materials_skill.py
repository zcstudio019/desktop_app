from __future__ import annotations

from ..schema import MissingMaterialSuggestion, RiskFinding


def generate_missing_materials(findings: list[RiskFinding]) -> list[MissingMaterialSuggestion]:
    materials = [
        MissingMaterialSuggestion(material="近三年及最近一期财务报表、审计报告或纳税申报佐证", reason="核验报表连续性与真实性", priority="high"),
        MissingMaterialSuggestion(material="主要银行账户流水及贷款合同/还本付息计划", reason="核验经营现金流与债务偿付来源", priority="high"),
    ]
    codes = {item.code for item in findings}
    if codes & {"high_other_receivables", "high_prepayments", "abnormal_accounts_receivable_growth"}:
        materials.append(MissingMaterialSuggestion(material="应收、其他应收及预付款明细、账龄与关联方清单", reason="核验资产质量和关联交易", priority="high"))
    if codes & {"abnormal_inventory_growth"}:
        materials.append(MissingMaterialSuggestion(material="存货明细、库龄及盘点/减值资料", reason="核验存货变现能力", priority="medium"))
    if codes & {"continuous_negative_operating_cash_flow", "financing_covers_operating_gap"}:
        materials.append(MissingMaterialSuggestion(material="现金流缺口说明、订单和回款计划", reason="核验融资用途及第一还款来源", priority="high"))
    return materials
