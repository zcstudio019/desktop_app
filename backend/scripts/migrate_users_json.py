"""One-off migration script for legacy users.json -> user_accounts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from backend.database import Base, SessionLocal, engine
from backend.db_models import UserAccount

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
USERS_FILE = DATA_DIR / "users.json"
PASSWORD_ALGO = "pbkdf2_sha256"
PASSWORD_ITERATIONS = 600000
LEGACY_PASSWORD_ALGO = "sha256_legacy"


def main() -> None:
    Base.metadata.create_all(bind=engine, tables=[UserAccount.__table__], checkfirst=True)
    if not USERS_FILE.exists():
        print("legacy users.json not found, skipped")
        return

    with open(USERS_FILE, encoding="utf-8") as f:
        data = json.load(f)
    users = data.get("users") or []
    migrated = 0

    with SessionLocal() as db:
        for user in users:
            username = (user.get("username") or "").strip()
            if not username:
                continue
            existing = db.query(UserAccount).filter(UserAccount.username == username).first()
            if existing:
                continue
            db.add(
                UserAccount(
                    username=username,
                    role=(user.get("role") or "user").strip() or "user",
                    password_hash=user.get("password_hash") or "",
                    salt=user.get("salt") or "",
                    password_algo=user.get("password_algo") or LEGACY_PASSWORD_ALGO,
                    password_iterations=int(user.get("password_iterations") or PASSWORD_ITERATIONS),
                    security_question=user.get("security_question") or "",
                    security_answer_hash=user.get("security_answer_hash") or "",
                    security_answer_salt=user.get("security_answer_salt") or "",
                    security_answer_algo=user.get("security_answer_algo") or PASSWORD_ALGO,
                    security_answer_iterations=int(user.get("security_answer_iterations") or PASSWORD_ITERATIONS),
                    display_name=user.get("display_name") or "",
                    phone=user.get("phone") or "",
                    created_at=user.get("created_at") or datetime.now(timezone.utc).isoformat(),
                    last_login_at=user.get("last_login_at") or "",
                    updated_at=user.get("updated_at") or "",
                )
            )
            migrated += 1
        db.commit()

    print(f"migrated users: {migrated}")


if __name__ == "__main__":
    main()
