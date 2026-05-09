"""Prompt templates reserved for optional LLM-based section extractors.

The current agent implementation is rule-first. These prompts define the
contract for future per-section LLM retries without changing the orchestrator
output schema.
"""

BASIC_INFO_PROMPT = """
You extract only report metadata from the provided enterprise credit report
section. Return strict JSON. Do not invent missing fields. Include evidence_text
for every extracted value.
"""

LOAN_SECTION_PROMPT = """
You extract only loan rows from this section. Do not include bills, letters of
credit, guarantees, credit lines, comprehensive credit, or trade finance as
loans. Return an array of strict JSON records with evidence_text. If none, return
[].
"""

CREDIT_LINE_PROMPT = """
You extract only credit line records from this section. 综合授信, 授信额度, and
额度循环标志 belong here. Do not infer loans from credit lines. Return strict JSON
with evidence_text for every row.
"""

BILL_LC_GUARANTEE_PROMPT = """
You extract only the business type named by the caller. 银行承兑汇票, 信用证, and
保函 must remain separate and must not enter loan arrays. Return strict JSON and
empty arrays when absent.
"""
