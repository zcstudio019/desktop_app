"""Create only the three Step 7A catalog tables in the configured database."""

from backend.database import Base, engine
from backend.db_models import FinancingProduct, FinancingProductRule, FinancingProductVersion


def main() -> None:
    Base.metadata.create_all(
        bind=engine,
        tables=[FinancingProduct.__table__, FinancingProductVersion.__table__, FinancingProductRule.__table__],
        checkfirst=True,
    )


if __name__ == "__main__":
    main()
