"""Back up the configured catalog and sync six Markdown sources into Draft only.

Run from the repository root: python -m backend.scripts.sync_local_product_catalog
"""

from __future__ import annotations

import gzip
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.database import DB_BACKEND, engine
from backend.services.markdown_product_import_service import SOURCE_DIR, scan_sources
from backend.services.product_catalog_service import ProductCatalogService

TABLES = ("financing_products", "financing_product_versions", "financing_product_rules")
BACKUP_DIR = Path(__file__).resolve().parents[1] / "data" / "product_catalog_backups"


def main() -> int:
    source_scan = scan_sources(SOURCE_DIR)
    if any(source.missing for source in source_scan["sources"]):
        print("同步终止：六份本地产品源未全部就绪。", file=sys.stderr)
        return 2
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            backup = {"captured_at": datetime.now(timezone.utc).isoformat(), "backend": DB_BACKEND,
                      "tables": {table: [dict(row) for row in connection.execute(text(f"SELECT * FROM {table}")).mappings()]
                                 if inspector.has_table(table) else None for table in TABLES}}
    except SQLAlchemyError:
        print("同步终止：项目运行数据库连接失败；尚未执行写入。", file=sys.stderr)
        return 3

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_file = BACKUP_DIR / f"catalog-before-sync-{stamp}.json.gz"
    with gzip.open(backup_file, "wt", encoding="utf-8") as target:
        json.dump(backup, target, ensure_ascii=False, default=str)

    service = ProductCatalogService()
    try:
        first = service.import_markdown_sources("local_markdown_sync")
        second = service.import_markdown_sources("local_markdown_sync")
        if second["created_drafts"]:
            raise RuntimeError("重复同步出现新版本，需调查幂等性")
        versions = service.list_versions()
        active = service.get_active_products(date.today())
    except Exception:
        print(f"同步失败；写入前备份位于 {backup_file}。请检查数据库状态。", file=sys.stderr)
        raise

    report = {"captured_at": datetime.now(timezone.utc).isoformat(), "source_parsed": source_scan["parsed_count"],
              "source_unique_codes": source_scan["unique_count"], "source_conflicts": source_scan["conflicts"],
              "created_drafts": first["created_drafts"], "unchanged": first["unchanged"],
              "repeat_created_drafts": second["created_drafts"],
              "draft_versions": sum(row["version"]["status"] in {"draft", "needs_review"} for row in versions),
              "published_versions": sum(row["version"]["status"] == "published" for row in versions),
              "active_versions": len(active), "backup_file": str(backup_file)}
    report_file = BACKUP_DIR / f"catalog-sync-report-{stamp}.json"
    report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "source_conflicts"}, ensure_ascii=False))
    print(f"冲突编号：{', '.join(row['external_product_code'] for row in source_scan['conflicts'])}")
    print(f"详细同步报告：{report_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
