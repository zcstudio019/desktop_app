"""Create only the three Step 7A catalog tables in the configured database."""

from backend.database import Base, engine
from backend.db_models import FinancingProduct, FinancingProductRule, FinancingProductVersion
from backend.services.product_catalog_schema import ensure_markdown_catalog_schema


def main() -> None:
    Base.metadata.create_all(
        bind=engine,
        tables=[FinancingProduct.__table__, FinancingProductVersion.__table__, FinancingProductRule.__table__],
        checkfirst=True,
    )
    ensure_markdown_catalog_schema(engine)


if __name__ == "__main__":
    main()
