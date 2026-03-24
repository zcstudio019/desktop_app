"""
Auth router.

Handles user authentication, registration, password reset, and admin user
management. User data is stored in ``data/users.json``.
"""

import hashlib
import json
import logging
import os
import re
import secrets
import shutil
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException

from ..middleware.auth import create_access_token, get_current_user, require_admin
from ..models.schemas import (
    ForgotPasswordRequest,
    LoginRequest,
    LoginResponse,
    RegisterRequest,
    RegisterResponse,
    ResetPasswordRequest,
    SecurityQuestionResponse,
    UserInfo,
)

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent.parent / "data"
USERS_FILE = DATA_DIR / "users.json"

PASSWORD_ALGO = "pbkdf2_sha256"
PASSWORD_ITERATIONS = int(os.getenv("PASSWORD_HASH_ITERATIONS", "600000"))
LEGACY_PASSWORD_ALGO = "sha256_legacy"
RELEASE_DIR_PATTERN = re.compile(r"^(?P<prefix>.+)_v(?P<version>\d+(?:\.\d+)*)$")

LOGIN_FAILED_MESSAGE = "用户名或密码错误"
EMPTY_CREDENTIALS_MESSAGE = "用户名和密码不能为空"
USER_EXISTS_MESSAGE = "用户名已存在"
USER_NOT_FOUND_MESSAGE = "用户不存在"
RESET_PASSWORD_SUCCESS_MESSAGE = "密码重置成功"
DELETE_USER_SUCCESS_MESSAGE = "账号删除成功"
PASSWORD_REQUIRED_MESSAGE = "用户名和新密码不能为空"
USERNAME_REQUIRED_MESSAGE = "用户名不能为空"
REQUIRED_FIELDS_MESSAGE = "所有字段不能为空"
PASSWORD_TOO_SHORT_MESSAGE = "新密码长度至少 6 位"
SECURITY_QUESTION_REQUIRED_MESSAGE = "该用户未设置密保问题，请联系管理员重置密码"
SECURITY_ANSWER_INVALID_MESSAGE = "密保答案错误"
FORBIDDEN_DELETE_SELF_MESSAGE = "不能删除当前登录账号"
FORBIDDEN_DELETE_LAST_ADMIN_MESSAGE = "至少需要保留一个管理员账号"


def _get_admin_credential(env_var: str, default_desc: str) -> str:
    """Get admin credential from environment variable or generate a secure fallback."""
    value = os.getenv(env_var)
    if value:
        return value

    if env_var == "ADMIN_USERNAME":
        value = f"admin_{secrets.token_hex(8)}"
    elif env_var == "ADMIN_PASSWORD":
        value = secrets.token_urlsafe(16)
    elif env_var == "ADMIN_SALT":
        value = secrets.token_hex(32)
    else:
        value = secrets.token_hex(16)

    logger.warning("%s: %s not set in .env, generated secure random value", default_desc, env_var)
    return value


ADMIN_USERNAME = _get_admin_credential("ADMIN_USERNAME", "Admin username")
ADMIN_PASSWORD = _get_admin_credential("ADMIN_PASSWORD", "Admin password")
ADMIN_SALT = _get_admin_credential("ADMIN_SALT", "Admin salt")

router = APIRouter(prefix="/auth", tags=["Authentication"])


def _hash_password_legacy(password: str, salt: str) -> str:
    """Legacy SHA256 hash retained only for migration compatibility."""
    return hashlib.sha256((password + salt).encode("utf-8")).hexdigest()


def _hash_pbkdf2(password: str, salt: str, iterations: int) -> str:
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    return digest.hex()


def _get_hash_params(record: dict, prefix: str = "") -> tuple[str, int]:
    algo_key = f"{prefix}algo" if prefix else "password_algo"
    iterations_key = f"{prefix}iterations" if prefix else "password_iterations"
    algo = record.get(algo_key) or LEGACY_PASSWORD_ALGO

    raw_iterations = record.get(iterations_key)
    try:
        iterations = int(raw_iterations) if raw_iterations is not None else PASSWORD_ITERATIONS
    except (TypeError, ValueError):
        iterations = PASSWORD_ITERATIONS

    return algo, iterations


def _hash_secret(value: str, salt: str, *, algo: str, iterations: int) -> str:
    if algo == LEGACY_PASSWORD_ALGO:
        return _hash_password_legacy(value, salt)
    if algo == PASSWORD_ALGO:
        return _hash_pbkdf2(value, salt, iterations)
    raise ValueError(f"Unsupported password algorithm: {algo}")


def _verify_secret(value: str, salt: str, expected_hash: str, *, algo: str, iterations: int) -> bool:
    actual_hash = _hash_secret(value, salt, algo=algo, iterations=iterations)
    return secrets.compare_digest(actual_hash, expected_hash)


def _set_password_fields(user: dict, password: str) -> None:
    salt = secrets.token_hex(16)
    user["salt"] = salt
    user["password_algo"] = PASSWORD_ALGO
    user["password_iterations"] = PASSWORD_ITERATIONS
    user["password_hash"] = _hash_secret(password, salt, algo=PASSWORD_ALGO, iterations=PASSWORD_ITERATIONS)


def _set_security_answer_fields(user: dict, answer: str) -> None:
    salt = secrets.token_hex(16)
    normalized_answer = answer.strip().lower()
    user["security_answer_salt"] = salt
    user["security_answer_algo"] = PASSWORD_ALGO
    user["security_answer_iterations"] = PASSWORD_ITERATIONS
    user["security_answer_hash"] = _hash_secret(
        normalized_answer,
        salt,
        algo=PASSWORD_ALGO,
        iterations=PASSWORD_ITERATIONS,
    )


def _load_users() -> list[dict]:
    if not USERS_FILE.exists():
        return []
    try:
        with open(USERS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("users") or []
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to load users file: %s", exc)
        return []


def _save_users(users: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump({"users": users}, f, ensure_ascii=False, indent=2)


def _get_optional_admin_username(env_var: str) -> str | None:
    value = os.getenv(env_var)
    if not value:
        return None
    normalized = value.strip()
    return normalized or None


def _get_seed_admin_accounts() -> list[dict[str, str]]:
    accounts = [
        {
            "username": ADMIN_USERNAME,
            "password": ADMIN_PASSWORD,
            "salt": ADMIN_SALT,
        }
    ]

    index = 2
    while True:
        username = _get_optional_admin_username(f"ADMIN_USERNAME_{index}")
        if not username:
            break

        accounts.append(
            {
                "username": username,
                "password": _get_admin_credential(f"ADMIN_PASSWORD_{index}", f"Admin #{index} password"),
                "salt": _get_admin_credential(f"ADMIN_SALT_{index}", f"Admin #{index} salt"),
            }
        )
        index += 1

    return accounts


def _parse_release_dir_name(name: str) -> tuple[str, tuple[int, ...]] | None:
    match = RELEASE_DIR_PATTERN.match(name)
    if not match:
        return None

    version = tuple(int(part) for part in match.group("version").split("."))
    return match.group("prefix"), version


def _find_previous_release_users_file() -> Path | None:
    current_release_dir = DATA_DIR.parent
    parsed_current = _parse_release_dir_name(current_release_dir.name)
    if not parsed_current:
        return None

    prefix, current_version = parsed_current
    parent_dir = current_release_dir.parent

    try:
        sibling_dirs = [path for path in parent_dir.iterdir() if path.is_dir() and path != current_release_dir]
    except OSError as exc:
        logger.warning("Failed to scan sibling release directories for account migration: %s", exc)
        return None

    candidates: list[tuple[tuple[int, ...], float, Path]] = []
    for sibling_dir in sibling_dirs:
        parsed_sibling = _parse_release_dir_name(sibling_dir.name)
        if not parsed_sibling:
            continue

        sibling_prefix, sibling_version = parsed_sibling
        if sibling_prefix != prefix or sibling_version >= current_version:
            continue

        users_file = sibling_dir / "data" / "users.json"
        if not users_file.is_file():
            continue

        try:
            modified_at = users_file.stat().st_mtime
        except OSError:
            modified_at = 0.0

        candidates.append((sibling_version, modified_at, users_file))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return candidates[0][2]


def _migrate_users_file_from_previous_release() -> bool:
    if USERS_FILE.exists():
        return False

    previous_users_file = _find_previous_release_users_file()
    if not previous_users_file:
        return False

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(previous_users_file, USERS_FILE)
        logger.info("Migrated users file from previous release: %s", previous_users_file)
        return True
    except OSError as exc:
        logger.warning("Failed to migrate users file from previous release %s: %s", previous_users_file, exc)
        return False


def _init_users_file() -> None:
    _migrate_users_file_from_previous_release()
    users = _load_users()
    existing_usernames = {user.get("username") for user in users}
    initialized_usernames: list[str] = []

    for account in _get_seed_admin_accounts():
        if account["username"] in existing_usernames:
            continue

        admin_user = {
            "username": account["username"],
            "role": "admin",
            "created_at": "2026-02-08T00:00:00",
        }
        admin_user["salt"] = account["salt"]
        admin_user["password_algo"] = PASSWORD_ALGO
        admin_user["password_iterations"] = PASSWORD_ITERATIONS
        admin_user["password_hash"] = _hash_secret(
            account["password"],
            account["salt"],
            algo=PASSWORD_ALGO,
            iterations=PASSWORD_ITERATIONS,
        )
        users.append(admin_user)
        existing_usernames.add(account["username"])
        initialized_usernames.append(account["username"])

    if not initialized_usernames:
        return

    _save_users(users)
    logger.info("Admin accounts initialized: %s", ", ".join(initialized_usernames))


_init_users_file()


def _authenticate_password(user: dict, password: str) -> bool:
    salt = user.get("salt") or ""
    password_hash = user.get("password_hash") or ""
    algo, iterations = _get_hash_params(user)
    if not salt or not password_hash:
        return False
    return _verify_secret(password, salt, password_hash, algo=algo, iterations=iterations)


def _needs_password_upgrade(user: dict) -> bool:
    algo, iterations = _get_hash_params(user)
    return algo != PASSWORD_ALGO or iterations != PASSWORD_ITERATIONS


def _count_admin_users(users: list[dict]) -> int:
    return sum(1 for user in users if (user.get("role") or "user") == "admin")


@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest) -> LoginResponse:
    username = request.username.strip()
    password = request.password

    if not username or not password:
        raise HTTPException(status_code=401, detail=LOGIN_FAILED_MESSAGE)

    users = _load_users()
    user = next((item for item in users if item.get("username") == username), None)
    if not user or not _authenticate_password(user, password):
        raise HTTPException(status_code=401, detail=LOGIN_FAILED_MESSAGE)

    if _needs_password_upgrade(user):
        _set_password_fields(user, password)
        _save_users(users)
        logger.info("Upgraded password hash for user '%s' during login", username)

    role = user.get("role") or "user"
    token = create_access_token(username, role)
    logger.info("User '%s' logged in successfully", username)
    return LoginResponse(token=token, username=username, role=role)


@router.post("/register", response_model=RegisterResponse)
async def register(request: RegisterRequest) -> RegisterResponse:
    username = request.username.strip()
    password = request.password

    if not username or not password:
        raise HTTPException(status_code=400, detail=EMPTY_CREDENTIALS_MESSAGE)

    users = _load_users()
    if any(user.get("username") == username for user in users):
        raise HTTPException(status_code=400, detail=USER_EXISTS_MESSAGE)

    new_user = {
        "username": username,
        "role": "user",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _set_password_fields(new_user, password)

    if request.security_question and request.security_answer:
        new_user["security_question"] = request.security_question
        _set_security_answer_fields(new_user, request.security_answer)

    users.append(new_user)
    _save_users(users)

    logger.info("New user '%s' registered", username)
    return RegisterResponse(username=username, role="user")


@router.post("/reset-password")
async def reset_password(request: ResetPasswordRequest, current_user: dict = Depends(require_admin)) -> dict:
    target_username = request.username.strip()
    new_password = request.new_password

    if not target_username or not new_password:
        raise HTTPException(status_code=400, detail=PASSWORD_REQUIRED_MESSAGE)

    users = _load_users()
    user = next((item for item in users if item.get("username") == target_username), None)
    if not user:
        raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)

    _set_password_fields(user, new_password)
    _save_users(users)

    logger.info("Admin '%s' reset password for user '%s'", current_user["username"], target_username)
    return {"success": True, "message": RESET_PASSWORD_SUCCESS_MESSAGE}


@router.get("/security-question", response_model=SecurityQuestionResponse)
async def get_security_question(username: str) -> SecurityQuestionResponse:
    username = username.strip()
    if not username:
        raise HTTPException(status_code=400, detail=USERNAME_REQUIRED_MESSAGE)

    users = _load_users()
    user = next((item for item in users if item.get("username") == username), None)
    if not user:
        raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)

    question = user.get("security_question") or ""
    return SecurityQuestionResponse(has_question=bool(question), question=question)


@router.post("/forgot-password")
async def forgot_password(request: ForgotPasswordRequest) -> dict:
    target_username = request.username.strip()
    answer = request.security_answer.strip().lower()
    new_password = request.new_password

    if not target_username or not answer or not new_password:
        raise HTTPException(status_code=400, detail=REQUIRED_FIELDS_MESSAGE)
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail=PASSWORD_TOO_SHORT_MESSAGE)

    users = _load_users()
    user = next((item for item in users if item.get("username") == target_username), None)
    if not user:
        raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)

    answer_hash = user.get("security_answer_hash") or ""
    answer_salt = user.get("security_answer_salt") or ""
    if not answer_hash or not answer_salt:
        raise HTTPException(status_code=400, detail=SECURITY_QUESTION_REQUIRED_MESSAGE)

    answer_algo, answer_iterations = _get_hash_params(user, prefix="security_answer_")
    if not _verify_secret(answer, answer_salt, answer_hash, algo=answer_algo, iterations=answer_iterations):
        raise HTTPException(status_code=400, detail=SECURITY_ANSWER_INVALID_MESSAGE)

    _set_password_fields(user, new_password)
    _save_users(users)

    logger.info("User '%s' reset password via security question", target_username)
    return {"success": True, "message": RESET_PASSWORD_SUCCESS_MESSAGE}


@router.get("/me", response_model=UserInfo)
async def get_me(current_user: dict = Depends(get_current_user)) -> UserInfo:
    return UserInfo(username=current_user["username"], role=current_user["role"])


@router.get("/users", response_model=list[UserInfo])
async def list_users(current_user: dict = Depends(require_admin)) -> list[UserInfo]:
    users = _load_users()
    return [
        UserInfo(
            username=user.get("username") or "",
            role=user.get("role") or "user",
            created_at=user.get("created_at"),
        )
        for user in users
    ]


@router.delete("/users/{username}")
async def delete_user(username: str, current_user: dict = Depends(require_admin)) -> dict:
    target_username = username.strip()
    if not target_username:
        raise HTTPException(status_code=400, detail=USER_NOT_FOUND_MESSAGE)

    if target_username == current_user["username"]:
        raise HTTPException(status_code=400, detail=FORBIDDEN_DELETE_SELF_MESSAGE)

    users = _load_users()
    target_user = next((item for item in users if item.get("username") == target_username), None)
    if not target_user:
        raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)

    if (target_user.get("role") or "user") == "admin" and _count_admin_users(users) <= 1:
        raise HTTPException(status_code=400, detail=FORBIDDEN_DELETE_LAST_ADMIN_MESSAGE)

    remaining_users = [item for item in users if item.get("username") != target_username]
    _save_users(remaining_users)
    logger.info("Admin '%s' deleted user '%s'", current_user["username"], target_username)
    return {"success": True, "message": DELETE_USER_SUCCESS_MESSAGE}
