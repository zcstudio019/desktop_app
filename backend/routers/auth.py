"""Auth router backed by SQLAlchemy user storage."""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from backend.database import SessionLocal
from backend.db_models import UserAccount
from ..middleware.auth import create_access_token, get_current_user, require_admin
from ..models.schemas import (
    ChangeCurrentUserPasswordRequest,
    ForgotPasswordRequest,
    LoginRequest,
    LoginResponse,
    RegisterRequest,
    RegisterResponse,
    ResetPasswordRequest,
    SecurityQuestionResponse,
    SetCurrentUserSecurityQuestionRequest,
    UpdateCurrentUserProfileRequest,
    UserInfo,
)

logger = logging.getLogger(__name__)

PASSWORD_ALGO = "pbkdf2_sha256"
PASSWORD_ITERATIONS = int(os.getenv("PASSWORD_HASH_ITERATIONS", "600000"))
LEGACY_PASSWORD_ALGO = "sha256_legacy"

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
CURRENT_PASSWORD_INVALID_MESSAGE = "当前密码不正确"
UPDATE_PROFILE_SUCCESS_MESSAGE = "账号信息已更新"
SECURITY_QUESTION_SET_SUCCESS_MESSAGE = "密保问题已设置"

router = APIRouter(prefix="/auth", tags=["Authentication"])


def _get_admin_credential(env_var: str, default_desc: str) -> str:
    value = (os.getenv(env_var) or "").strip()
    if value:
        return value
    if env_var == "ADMIN_USERNAME":
        value = f"admin_{secrets.token_hex(8)}"
    elif env_var == "ADMIN_PASSWORD":
        value = secrets.token_urlsafe(16)
    else:
        value = secrets.token_hex(16)
    logger.warning("%s: %s not set, generated secure fallback", default_desc, env_var)
    return value


ADMIN_USERNAME = _get_admin_credential("ADMIN_USERNAME", "Admin username")
ADMIN_PASSWORD = _get_admin_credential("ADMIN_PASSWORD", "Admin password")


def _hash_password_legacy(password: str, salt: str) -> str:
    return hashlib.sha256((password + salt).encode("utf-8")).hexdigest()


def _hash_pbkdf2(password: str, salt: str, iterations: int) -> str:
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    return digest.hex()


def _hash_secret(value: str, salt: str, *, algo: str, iterations: int) -> str:
    if algo == LEGACY_PASSWORD_ALGO:
        return _hash_password_legacy(value, salt)
    if algo == PASSWORD_ALGO:
        return _hash_pbkdf2(value, salt, iterations)
    raise ValueError(f"Unsupported hash algorithm: {algo}")


def _verify_secret(value: str, salt: str, expected_hash: str, *, algo: str, iterations: int) -> bool:
    actual_hash = _hash_secret(value, salt, algo=algo, iterations=iterations)
    return secrets.compare_digest(actual_hash, expected_hash)


def _normalize_time(value: str | None) -> str | None:
    if not value:
        return None
    return value


def _row_to_user_info(row: UserAccount) -> UserInfo:
    return UserInfo(
        username=row.username,
        role=row.role or "user",
        created_at=_normalize_time(row.created_at),
        last_login_at=_normalize_time(row.last_login_at),
        updated_at=_normalize_time(row.updated_at),
        display_name=row.display_name or None,
        phone=row.phone or None,
        has_security_question=bool(row.security_question),
    )


def _set_password_fields(row: UserAccount, password: str) -> None:
    salt = secrets.token_hex(16)
    row.salt = salt
    row.password_algo = PASSWORD_ALGO
    row.password_iterations = PASSWORD_ITERATIONS
    row.password_hash = _hash_secret(password, salt, algo=PASSWORD_ALGO, iterations=PASSWORD_ITERATIONS)


def _set_security_answer_fields(row: UserAccount, answer: str) -> None:
    salt = secrets.token_hex(16)
    normalized_answer = answer.strip().lower()
    row.security_answer_salt = salt
    row.security_answer_algo = PASSWORD_ALGO
    row.security_answer_iterations = PASSWORD_ITERATIONS
    row.security_answer_hash = _hash_secret(
        normalized_answer,
        salt,
        algo=PASSWORD_ALGO,
        iterations=PASSWORD_ITERATIONS,
    )


def _authenticate_password(row: UserAccount, password: str) -> bool:
    if not row.salt or not row.password_hash:
        return False
    algo = row.password_algo or LEGACY_PASSWORD_ALGO
    iterations = int(row.password_iterations or PASSWORD_ITERATIONS)
    return _verify_secret(password, row.salt, row.password_hash, algo=algo, iterations=iterations)


def _needs_password_upgrade(row: UserAccount) -> bool:
    return (row.password_algo or LEGACY_PASSWORD_ALGO) != PASSWORD_ALGO or int(row.password_iterations or 0) != PASSWORD_ITERATIONS


def _count_admin_users(db) -> int:
    return db.query(UserAccount).filter(UserAccount.role == "admin").count()


def _ensure_seed_admin_account() -> None:
    with SessionLocal() as db:
        existing = db.query(UserAccount).filter(UserAccount.username == ADMIN_USERNAME).first()
        if existing:
            return

        admin = UserAccount(
            username=ADMIN_USERNAME,
            role="admin",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        _set_password_fields(admin, ADMIN_PASSWORD)
        db.add(admin)
        db.commit()
        logger.info("Seeded admin account '%s' into SQLAlchemy storage", ADMIN_USERNAME)

_ensure_seed_admin_account()


@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest) -> LoginResponse:
    username = request.username.strip()
    password = request.password
    if not username or not password:
        raise HTTPException(status_code=401, detail=LOGIN_FAILED_MESSAGE)

    with SessionLocal() as db:
        user = db.query(UserAccount).filter(UserAccount.username == username).first()
        if not user or not _authenticate_password(user, password):
            raise HTTPException(status_code=401, detail=LOGIN_FAILED_MESSAGE)

        if _needs_password_upgrade(user):
            _set_password_fields(user, password)

        user.last_login_at = datetime.now(timezone.utc).isoformat()
        db.commit()
        role = user.role or "user"

    token = create_access_token(username, role)
    logger.info("User '%s' logged in successfully", username)
    return LoginResponse(token=token, username=username, role=role)


@router.post("/register", response_model=RegisterResponse)
async def register(request: RegisterRequest) -> RegisterResponse:
    username = request.username.strip()
    password = request.password
    if not username or not password:
        raise HTTPException(status_code=400, detail=EMPTY_CREDENTIALS_MESSAGE)

    with SessionLocal() as db:
        if db.query(UserAccount).filter(UserAccount.username == username).first():
            raise HTTPException(status_code=400, detail=USER_EXISTS_MESSAGE)

        row = UserAccount(
            username=username,
            role="user",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        _set_password_fields(row, password)
        if request.security_question and request.security_answer:
            row.security_question = request.security_question.strip()
            _set_security_answer_fields(row, request.security_answer)
        db.add(row)
        db.commit()

    logger.info("New user '%s' registered", username)
    return RegisterResponse(username=username, role="user")


@router.post("/reset-password")
async def reset_password(request: ResetPasswordRequest, current_user: dict = Depends(require_admin)) -> dict:
    target_username = request.username.strip()
    new_password = request.new_password
    if not target_username or not new_password:
        raise HTTPException(status_code=400, detail=PASSWORD_REQUIRED_MESSAGE)

    with SessionLocal() as db:
        user = db.query(UserAccount).filter(UserAccount.username == target_username).first()
        if not user:
            raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)
        _set_password_fields(user, new_password)
        user.updated_at = datetime.now(timezone.utc).isoformat()
        db.commit()

    logger.info("Admin '%s' reset password for user '%s'", current_user["username"], target_username)
    return {"success": True, "message": RESET_PASSWORD_SUCCESS_MESSAGE}


@router.get("/security-question", response_model=SecurityQuestionResponse)
async def get_security_question(username: str) -> SecurityQuestionResponse:
    username = username.strip()
    if not username:
        raise HTTPException(status_code=400, detail=USERNAME_REQUIRED_MESSAGE)

    with SessionLocal() as db:
        user = db.query(UserAccount).filter(UserAccount.username == username).first()
        if not user:
            raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)
        question = user.security_question or ""
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

    with SessionLocal() as db:
        user = db.query(UserAccount).filter(UserAccount.username == target_username).first()
        if not user:
            raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)
        if not user.security_answer_hash or not user.security_answer_salt:
            raise HTTPException(status_code=400, detail=SECURITY_QUESTION_REQUIRED_MESSAGE)

        answer_algo = user.security_answer_algo or PASSWORD_ALGO
        answer_iterations = int(user.security_answer_iterations or PASSWORD_ITERATIONS)
        if not _verify_secret(
            answer,
            user.security_answer_salt,
            user.security_answer_hash,
            algo=answer_algo,
            iterations=answer_iterations,
        ):
            raise HTTPException(status_code=400, detail=SECURITY_ANSWER_INVALID_MESSAGE)

        _set_password_fields(user, new_password)
        user.updated_at = datetime.now(timezone.utc).isoformat()
        db.commit()

    logger.info("User '%s' reset password via security question", target_username)
    return {"success": True, "message": RESET_PASSWORD_SUCCESS_MESSAGE}


@router.get("/me", response_model=UserInfo)
async def get_me(current_user: dict = Depends(get_current_user)) -> UserInfo:
    try:
        with SessionLocal() as db:
            user = db.query(UserAccount).filter(UserAccount.username == current_user["username"]).first()
            if not user:
                raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)
            if not user.last_login_at and current_user.get("token_iat"):
                user.last_login_at = datetime.fromtimestamp(float(current_user["token_iat"]), tz=timezone.utc).isoformat()
                db.commit()
            return _row_to_user_info(user)
    except HTTPException as exc:
        logger.exception("/api/auth/me returned HTTPException")
        return JSONResponse(status_code=exc.status_code, content={"error": str(exc.detail)})
    except Exception as exc:
        logger.exception("error detail")
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.put("/me", response_model=UserInfo)
async def update_me(
    request: UpdateCurrentUserProfileRequest,
    current_user: dict = Depends(get_current_user),
) -> UserInfo:
    with SessionLocal() as db:
        user = db.query(UserAccount).filter(UserAccount.username == current_user["username"]).first()
        if not user:
            raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)
        user.display_name = (request.display_name or "").strip()
        user.phone = (request.phone or "").strip()
        user.updated_at = datetime.now(timezone.utc).isoformat()
        db.commit()
        db.refresh(user)
        logger.info("User '%s' updated profile", current_user["username"])
        return _row_to_user_info(user)


@router.put("/security-question", response_model=UserInfo)
async def set_current_user_security_question(
    request: SetCurrentUserSecurityQuestionRequest,
    current_user: dict = Depends(get_current_user),
) -> UserInfo:
    question = request.security_question.strip()
    answer = request.security_answer.strip()
    if not question or not answer:
        raise HTTPException(status_code=400, detail=REQUIRED_FIELDS_MESSAGE)

    with SessionLocal() as db:
        user = db.query(UserAccount).filter(UserAccount.username == current_user["username"]).first()
        if not user:
            raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)
        user.security_question = question
        _set_security_answer_fields(user, answer)
        user.updated_at = datetime.now(timezone.utc).isoformat()
        db.commit()
        db.refresh(user)
        return _row_to_user_info(user)


@router.post("/change-password")
async def change_password(
    request: ChangeCurrentUserPasswordRequest,
    current_user: dict = Depends(get_current_user),
) -> dict:
    if len(request.new_password) < 6:
        raise HTTPException(status_code=400, detail=PASSWORD_TOO_SHORT_MESSAGE)

    with SessionLocal() as db:
        user = db.query(UserAccount).filter(UserAccount.username == current_user["username"]).first()
        if not user:
            raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)
        if not _authenticate_password(user, request.current_password):
            raise HTTPException(status_code=400, detail=CURRENT_PASSWORD_INVALID_MESSAGE)
        _set_password_fields(user, request.new_password)
        user.updated_at = datetime.now(timezone.utc).isoformat()
        db.commit()

    logger.info("User '%s' changed password", current_user["username"])
    return {"success": True, "message": RESET_PASSWORD_SUCCESS_MESSAGE}


@router.get("/users", response_model=list[UserInfo])
async def list_users(current_user: dict = Depends(require_admin)) -> list[UserInfo]:
    with SessionLocal() as db:
        rows = db.query(UserAccount).order_by(UserAccount.role.desc(), UserAccount.username.asc()).all()
        return [_row_to_user_info(row) for row in rows]


@router.delete("/users/{username}")
async def delete_user(username: str, current_user: dict = Depends(require_admin)) -> dict:
    target_username = username.strip()
    if not target_username:
        raise HTTPException(status_code=400, detail=USER_NOT_FOUND_MESSAGE)
    if target_username == current_user["username"]:
        raise HTTPException(status_code=400, detail=FORBIDDEN_DELETE_SELF_MESSAGE)

    with SessionLocal() as db:
        target_user = db.query(UserAccount).filter(UserAccount.username == target_username).first()
        if not target_user:
            raise HTTPException(status_code=404, detail=USER_NOT_FOUND_MESSAGE)
        if (target_user.role or "user") == "admin" and _count_admin_users(db) <= 1:
            raise HTTPException(status_code=400, detail=FORBIDDEN_DELETE_LAST_ADMIN_MESSAGE)
        db.delete(target_user)
        db.commit()

    logger.info("Admin '%s' deleted user '%s'", current_user["username"], target_username)
    return {"success": True, "message": DELETE_USER_SUCCESS_MESSAGE}
