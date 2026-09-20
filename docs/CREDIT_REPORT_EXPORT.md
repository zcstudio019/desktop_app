# 征信速览报告 HTML / PDF

## 数据流

`generate_credit_one_page_report` 保持原分析及 Markdown 输出不变，成功后保存同一份 model、narrative、generated_at 和 Markdown。HTML 直接读取冻结 DTO，PDF 打印 HTML；不解析 Markdown，不重新调用 LLM。

复用 `customer_financing_diagnostic_reports` 快照表，`report_version=credit_one_page_report_v1` 隔离征信报告。融资诊断历史及详情查询排除该类型；无新增客户表字段或数据库迁移。

## 接口

报告成功后的 `data` 包含 `reportId`、`previewUrl`、`pdfUrl`。不需要另行 POST 创建快照。

- `GET /api/customers/{customer_id}/credit-report/snapshots/{report_id}/preview`
- `GET /api/customers/{customer_id}/credit-report/snapshots/{report_id}/export/pdf`

沿用 Bearer 登录认证及客户访问权限；跨客户/不存在的快照返回 404，无权访问返回 403，缺少认证返回 401。PDF 环境不可用返回中文 503。响应禁止缓存。旧消息没有冻结 DTO，不能从旧 Markdown 逆向导出，需要重新生成一次报告。

## 页面与依赖

五页主报告依次为：主体及指标；贷款/信用卡；担保/逾期/公共记录；查询/指标检查；路线图/综合结论。正常页预算为企业贷款7条、个人贷款10条、信用卡3条、相关责任7条；超出记录完整进入五页主报告后的附页。极端长文本允许自然续页，不使用裁切或隐藏溢出。

使用现有 `playwright` Python 依赖和 Chromium；生产已安装浏览器及文泉驿中文字体，无新增PDF库或字体文件。离线渲染拦截所有资源请求、禁用文档脚本及 service workers，A4、12mm 页边距、打印背景、优先CSS纸张尺寸。每个进程最多同时运行两次PDF生成。

HTML 所有业务值均 escape，固定字段白名单输出，不展示原始 evidence、模型提示词、内部字段。金额仅格式化现有 value/unit，不推断单位。

## 验收

```sh
PYTHONPATH=. venv/bin/python -m pytest -q tests/test_credit_report_html_pdf.py tests/test_assistant_credit_one_page_report.py tests/test_credit_report_production_adaptation.py tests/test_financing_diagnostic_report_snapshot.py tests/test_financing_diagnostic_report_export.py
```

生产HTTP验收脚本：`scripts/verify_credit_report_pdf.py --output-dir <private-directory>`。需要由授权人员通过正常登录取得会话，在进程环境 `CREDIT_REPORT_QA_TOKEN` 中提供。不得把令牌写入文件、命令参数、仓库或日志。`--report-id` 可复用现有冻结报告，不再调用LLM。脚本验证响应、五页PDF、关键金额一致性和客户/提取记录未变；生成HTML、PDF、Markdown及逐页PNG。敏感验收产物只能放在私有目录。

人工验收：登录工作台生成报告，在报告消息下点击“预览报告”和“下载 PDF”，核对五页、中文、表格、金额以及末页结论。前端刷新后需加载新静态资源。
