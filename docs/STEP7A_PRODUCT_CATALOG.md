# Step 7A 产品结构化发布层

飞书三份产品文档仍是编辑源。`financing_products`、`financing_product_versions`、`financing_product_rules` 是正式发布层；旧 `/api/scheme` 继续读取原有全文缓存。

## 建表

部署时执行 `python -m backend.scripts.init_product_catalog`，只创建以上三张表。管理员 API 首次调用也会检查并创建缺失的表；不会修改已有表的列。

## 管理员流程

所有 `/api/product-catalog` 接口要求管理员身份。

1. `POST /api/product-catalog/import`，提交 `product_category` 与对应的 `wiki_node_token`。仅接受 `WikiService.PRODUCT_DOCS` 中批准的类别与节点组合。导入会读取飞书文档，并为变化的产品正文建立新 Draft；相同来源快照重复导入不会增版。
2. `GET /api/product-catalog/versions?status=draft` 或 `GET /api/product-catalog/versions/{version_id}` 检查结构化字段、规则、来源正文、哈希与 `field_review_json`。`extracted_review` 表示自动提取待人工确认；`needs_review` 表示表述不明确；`insufficient_data` 表示未发现可靠依据。
3. `PATCH /api/product-catalog/versions/{version_id}` 的 `fields` 中编辑额度、期限、地区、担保、材料、有效期及审核结果。审核结果逐项标为 `confirmed` 或 `acknowledged_unknown`。缺失字段应保留未知状态，不要填 0 或 false。
4. 用 `POST /api/product-catalog/versions/{version_id}/rules` 的 `rule` 添加经核对的规则；误录规则可用对应 DELETE 路由删除。规则必须使用字段白名单、固定操作符、正确值类型和来源原文。材料清单只存入 `materials_json`。
5. `POST /api/product-catalog/versions/{version_id}/publish` 验证后发布，并将旧已发布版本标为 `superseded`。已发布内容和规则不可修改；银行变更须重新导入 Draft。`POST /api/product-catalog/versions/{version_id}/disable` 可停用已发布版本。
6. `GET /api/product-catalog/active?as_of_date=YYYY-MM-DD` 只返回该日有效的 `published` 版本。此查询尚未接入客户匹配。

版本状态：`draft`、`needs_review`、`published`、`expired`、`superseded`、`disabled`。到期产品即使状态仍显示 `published`，也会被有效期查询排除；管理员可另行标记 `expired`。

导入保存的是每个产品段落的 `source_snapshot` 与 SHA-256、飞书节点及文档 token、导入时间。来源文档若提供可解析的修改时间，也保存到 `source_updated_at`。未写明的利率、地区、额度上限和期限不会被推断为确定值。
