# Agent Regression Report

- Generated At: 2026-05-09T09:47:08.315190+00:00
- Mode: Rule
- Total Cases: 3
- Passed: 3
- Failed: 0
- Drift Cases: 0
- Overall Stability Score: **100**

## Version Fingerprint

| Item | Value |
|------|-------|
| Commit | 9b1972291f90e6da2fdc3ef7f9aff22be6bbadd9 |
| Branch | main |
| App Version | 1.2.2 |
| Model | deepseek-chat |
| RiskAgent Prompt Hash | 5627a31aa2b8 |
| FinancingJudgementAgent Prompt Hash | 440065fb082d |
| RiskAgent Schema Hash | d82e05b88210 |
| FinancingJudgementAgent Schema Hash | 96cbbf1f68be |
| RiskAgent Rule Hash | 9c80e893d97f |
| Compliance Guard Hash | f3dd596b65dc |
| Snapshot Version | v1 |

## Fingerprint Changes

| Item | Previous | Current | Changed |
|------|----------|---------|---------|
| Model | deepseek-chat | deepseek-chat | — |
| RiskAgent Prompt | 5627a31aa2b8 | 5627a31aa2b8 | — |
| FinancingJudgementAgent Prompt | 440065fb082d | 440065fb082d | — |
| RiskAgent Schema | d82e05b88210 | d82e05b88210 | — |
| FinancingJudgementAgent Schema | 96cbbf1f68be | 96cbbf1f68be | — |
| RiskAgent Rule | 9c80e893d97f | 9c80e893d97f | — |
| Compliance Guard | f3dd596b65dc | f3dd596b65dc | — |

## Case List

| Case | Regression | Snapshot | Critical | High | Medium | Low | Possible Causes |
|------|------------|----------|----------|------|--------|-----|-----------------|
| case_001_basic | ✅ PASS | 🟢 STABLE | 0 | 0 | 0 | 0 | - |
| case_002_high_risk | ✅ PASS | 🟢 STABLE | 0 | 0 | 0 | 0 | - |
| case_003_missing_materials | ✅ PASS | 🟢 STABLE | 0 | 0 | 0 | 0 | - |

## Critical Drift

- None

## High Drift

- None

## Compliance Warnings

- None

## Validation Errors

- None

## LLM Usage Summary

| Agent | LLM Used | Fallback Used | Retry Count |
|-------|----------|---------------|-------------|
| RiskAgent | False | False | 0 |
| FinancingJudgementAgent | False | False | 0 |
| RiskAgent | False | False | 0 |
| FinancingJudgementAgent | False | False | 0 |
| RiskAgent | False | False | 0 |
| FinancingJudgementAgent | False | False | 0 |

## Top Risks

- case_002_high_risk: 授信使用率过高
- case_002_high_risk: 多头授信
- case_002_high_risk: 对外担保风险
- case_002_high_risk: 逾期或分类异常

## Fallback Summary

- None
