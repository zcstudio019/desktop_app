"""Create Step 7A catalog tables and conflict records in the configured database."""

from backend.database import Base, engine
from backend.db_models import FinancingProduct, FinancingProductRule, FinancingProductVersion, FinancingProductConflict
from backend.services.product_catalog_schema import ensure_markdown_catalog_schema


def main() -> None:
    Base.metadata.create_all(
        bind=engine,
        tables=[FinancingProduct.__table__, FinancingProductVersion.__table__, FinancingProductRule.__table__, FinancingProductConflict.__table__],
        checkfirst=True,
    )
    ensure_markdown_catalog_schema(engine)


if __name__ == "__main__":
    main()
