# Step 7A 产品结构化发布层

正式编辑源为 `backend/data/product_catalog_sources/` 中六份 Markdown；`financing_products`、`financing_product_versions`、`financing_product_rules` 是审核发布层。飞书导入与 WikiService 保留为兼容能力。旧 `/api/scheme` 仍使用飞书全文缓存，尚未切换到 ProductCatalog。

## 来源与建表

六份 Markdown 分别映射 `guarantee_fund`、`personal_mortgage`、`personal_credit`、`technology_enterprise`、`enterprise_mortgage`、`enterprise_credit`，不会合并企业信用与企业抵押正文。产品内部主键是 `product_id`，Markdown 的 `external_product_code` 是唯一外部编号。`source_file`、`source_update_date`、单产品段落原文、SHA-256 和导入时间随每个版本保存。表格原值保存在 `raw_fields_json`。

部署时执行 `python -m backend.scripts.init_product_catalog`。脚本创建缺失的三张表，并为已有 Step 7A 表补充 Step 7A.2 列和外部编号唯一索引。管理员 API 首次使用时也会执行同一检查。

## 管理员流程

所有 `/api/product-catalog` 接口要求管理员身份。管理页面中的“本地 Markdown 产品库”展示六个来源的文件更新时间、解析数、唯一数、待审核数、冲突数与已发布数，提供同步、查看产品和查看冲突。

1. `GET /api/product-catalog/sources` 只读扫描六份文件；`GET /api/product-catalog/sources/conflicts` 查看同编号不同正文的冲突双方与哈希；`GET /api/product-catalog/sources/{category}/products` 查看解析清单。
2. `POST /api/product-catalog/sources/{category}/sync` 或 `POST /api/product-catalog/sync-local` 生成 Draft。同编号且同正文哈希自动去重；同编号不同内容跳过并报冲突。同一文件未变化时重复同步不会新建版本，正文变化时新建 Draft，不覆盖 Published。
3. `GET /api/product-catalog/versions?status=draft` 或 `GET /api/product-catalog/versions/{version_id}` 检查结构化字段、原始表格、规则、来源正文、哈希与 `field_review_json`。`extracted_review` 表示自动提取待确认；`needs_review` 表示表述不明确；`insufficient_data` 表示未发现可靠依据。
4. `PATCH /api/product-catalog/versions/{version_id}` 编辑草稿字段和审核结果，审核结果逐项标为 `confirmed` 或 `acknowledged_unknown`。缺失字段保留未知状态。规则通过对应规则 API 新增或删除，必须使用字段白名单、固定操作符、正确值类型及来源原文。材料不作为准入规则。
5. `POST /api/product-catalog/versions/{version_id}/publish` 验证后发布，并将旧已发布版本标为 `superseded`。已发布内容和规则不可修改；Markdown 变化需重新导入 Draft。`POST /api/product-catalog/versions/{version_id}/disable` 可停用。
6. `GET /api/product-catalog/active?as_of_date=YYYY-MM-DD` 只返回该日有效的 Published 版本。此查询尚未接入客户匹配。

版本状态：`draft`、`needs_review`、`published`、`expired`、`superseded`、`disabled`。到期产品即使状态仍为 `published`，也会被有效期查询排除。

`POST /api/product-catalog/import/feishu`（原 `/import`）仍可按原有批准节点导入飞书草稿，作为兼容入口。Dashboard 的飞书全文缓存按钮只服务旧方案匹配。
