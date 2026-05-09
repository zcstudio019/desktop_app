# Agent regression fixtures

这些 fixture 用于企业融资 Agent 工作流的离线回归测试，不依赖真实客户、数据库或上传文件。

## 目录结构

每个 case 一个目录：

```text
case_xxx/
  input_context.json
  expected_assertions.json
```

`input_context.json` 模拟 `build_customer_ai_context(customer_id)` 的输出，可包含：

- `customer_profile`
- `documents`
- `extractions`
- `enterprise_credit`
- `credit_summary`
- `latest_agent_report`

`expected_assertions.json` 描述测试断言，例如：

- `risk_level_in`
- `must_have_risk_types`
- `must_have_missing_materials`
- `must_include_disclaimer`
- `forbidden_terms`
- `estimated_amount_must_contain`

## 如何新增 case

1. 新建 `tests/fixtures/agent_cases/case_xxx_name/`。
2. 放入脱敏后的 `input_context.json`。
3. 放入期望断言 `expected_assertions.json`。
4. 运行 `python scripts/test_agent_regression.py --case case_xxx_name`。

## 运行测试

规则模式：

```bash
python scripts/test_agent_regression.py
```

LLM 模式：

```bash
python scripts/test_agent_regression.py --use-llm
```

指定 case：

```bash
python scripts/test_agent_regression.py --case case_001_basic
```

## 注意

不要提交真实客户敏感资料。手机号、身份证号、银行账号、完整企业名称、详细地址等必须脱敏或使用模拟数据。
