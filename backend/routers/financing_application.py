"""Financing application execution endpoints."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Response
from urllib.parse import quote
from pydantic import BaseModel, Field

from backend.middleware.auth import get_current_user
from backend.routers.financing_requirement import require_customer_access
from backend.services.financing_application_service import FinancingApplicationError, FinancingApplicationService
from backend.services.application_material_service import ApplicationMaterialError, ApplicationMaterialMatchingService
from backend.services.financing_communication_service import FinancingCommunicationError, FinancingCommunicationService
from backend.services.financing_outcome_service import FinancingOutcomeError, FinancingOutcomeService

router = APIRouter(tags=["Financing Applications"])
applications = FinancingApplicationService()
materials = ApplicationMaterialMatchingService()
communications = FinancingCommunicationService()
outcomes = FinancingOutcomeService()


class CreateApplicationsRequest(BaseModel):
    customer_id: str
    plan_version_id: str
    responsible_user_id: str | None = None
    responsible_user_name: str | None = None


class SubmitRequest(BaseModel):
    submitted_amount: Decimal | None = None
    submission_channel: str | None = None
    submission_reference: str | None = None
    submission_notes: str = ""


class SupplementRequest(BaseModel):
    description: str = ""
    required_materials: list[str] = Field(default_factory=list)
    due_date: date | None = None
    requested_by_bank: str | None = None
    notes: str = ""


class SupplementUpdateRequest(BaseModel):
    complete: bool = False


class MaterialUpdateRequest(BaseModel):
    status: str
    file_reference: str | None = None
    notes: str | None = None


class ApplicationMaterialCreateRequest(BaseModel):
    material_type: str
    material_name: str
    owner_type: str = "enterprise"
    owner_id: str | None = None
    owner_name: str | None = None
    required: bool = True
    required_verified: bool = False
    source_type: str = "manual"
    source_id: str | None = None
    supplement_request_id: str | None = None


class MaterialSelectRequest(BaseModel):
    document_id: str


class MaterialReplaceRequest(BaseModel):
    document_id: str
    rejection_reason: str


class SupplementPackageSubmitRequest(BaseModel):
    submission_reference: str | None = None


class ContactCreateRequest(BaseModel):
    contact_type: str
    customer_id: str | None = None
    institution_name: str | None = None
    branch_name: str | None = None
    name: str
    title: str | None = None
    department: str | None = None
    mobile: str | None = None
    phone: str | None = None
    email: str | None = None
    wechat: str | None = None
    is_primary: bool = False
    related_person_id: str | None = None
    notes: str = ""


class ContactUpdateRequest(BaseModel):
    branch_name: str | None = None
    name: str | None = None
    title: str | None = None
    department: str | None = None
    mobile: str | None = None
    phone: str | None = None
    email: str | None = None
    wechat: str | None = None
    is_primary: bool | None = None
    related_person_id: str | None = None
    status: str | None = None
    notes: str | None = None


class ApplicationContactRequest(BaseModel):
    contact_id: str
    role: str = "handler"
    is_primary: bool = False


class CommunicationCreateRequest(BaseModel):
    contact_id: str | None = None
    communication_side: str
    channel: str
    direction: str
    feedback_tag: str | None = None
    subject: str
    content: str
    occurred_at: datetime | None = None
    outcome: str = "info_only"
    follow_up_required: bool = False
    next_follow_up_at: datetime | None = None
    related_task_id: str | None = None
    related_supplement_id: str | None = None
    related_review_feedback_id: str | None = None
    related_approval_record_id: str | None = None
    internal_note: str = ""


class VoidCommunicationRequest(BaseModel):
    reason: str


class CommunicationTaskRequest(BaseModel):
    title: str | None = None
    assignee_user_id: str | None = None
    assignee_user_name: str | None = None
    due_date: date | None = None
    priority: str = "normal"


class CommunicationSupplementRequest(BaseModel):
    required_materials: list[str] = Field(default_factory=list)
    due_date: date | None = None


class CommunicationReviewFeedbackRequest(BaseModel):
    feedback_type: str = "general"


class FollowUpCreateRequest(BaseModel):
    application_id: str
    communication_record_id: str | None = None
    related_task_id: str | None = None
    follow_up_type: str
    title: str
    description: str = ""
    assignee_user_id: str | None = None
    assignee_user_name: str | None = None
    due_at: datetime
    priority: str = "normal"


class FollowUpUpdateRequest(BaseModel):
    status: str | None = None
    due_at: datetime | None = None
    priority: str | None = None
    result: str | None = None


class ReviewFeedbackRequest(BaseModel):
    feedback_type: str = "general"
    feedback_date: datetime | None = None
    institution_contact: str | None = None
    content: str


class ApprovalRequest(BaseModel):
    approved_amount: Decimal
    approved_term_months: int
    approved_interest_rate: Decimal | None = None
    approval_reference: str | None = None
    approved_at: datetime | None = None
    approval_expiry_date: date | None = None
    guarantee_method: str | None = None
    repayment_method: str | None = None
    conditions: list[dict] = Field(default_factory=list)
    notes: str = ""


class RejectionRequest(BaseModel):
    rejection_reason: str
    rejection_code: str | None = None
    rejected_at: datetime | None = None
    notes: str = ""


class DisbursementRequest(BaseModel):
    disbursed_amount: Decimal
    disbursed_at: datetime | None = None
    disbursement_reference: str | None = None
    recipient_name: str | None = None
    recipient_account_masked: str | None = None
    purpose: str | None = None
    notes: str = ""


class StageAdvanceRequest(BaseModel):
    target_stage_code: str


class TaskCompleteRequest(BaseModel):
    notes: str = ""


class TaskBlockRequest(BaseModel):
    block_reason: str


class TaskUnblockRequest(BaseModel):
    reason: str = ""
    target_status: str = "in_progress"


class ApprovalConditionRequest(BaseModel):
    status: str


class TaskUpdateRequest(BaseModel):
    status: str
    priority: str | None = None
    assignee_user_id: str | None = None
    assignee_user_name: str | None = None
    due_date: date | None = None
    notes: str | None = None


class OutcomeFinalizeRequest(BaseModel):
    rejection_reasons: list[dict] = Field(default_factory=list)
    disbursement_variance_reason: str | None = None
    final_notes: str = ""
    source_type: str = "execution"


class OutcomeCorrectionRequest(BaseModel):
    changes: dict = Field(default_factory=dict)
    reason: str


class CloseApplicationRequest(BaseModel):
    close_reason: str
    final_result_confirmed: bool = False


class RuleFeedbackRequest(BaseModel):
    product_version_id: str
    rule_id: str | None = None
    application_id: str
    feedback_type: str
    expected_result: str = ""
    actual_bank_feedback: str
    evidence_source: str
    evidence_reference: str | None = None


class RuleFeedbackUpdateRequest(BaseModel):
    status: str


def _require_write(user: dict) -> None:
    if str(user.get("role") or "").lower() not in {"admin", "operator"}:
        raise HTTPException(status_code=403, detail="当前账号只有查看权限")


def _actor(user: dict) -> tuple[str, str]:
    actor_id = str(user.get("username") or "")
    return actor_id, str(user.get("display_name") or actor_id)


async def _authorized(application_id: str, user: dict) -> dict:
    try:
        value = applications.get_application(application_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await require_customer_access(value["customer_id"], user)
    return value


async def _mutate(application_id: str, user: dict, action):
    _require_write(user)
    await _authorized(application_id, user)
    try:
        return action()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FinancingApplicationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ApplicationMaterialError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


async def _material_mutate(application_id: str, user: dict, action):
    return await _mutate(application_id, user, action)


def _communication_call(action):
    try:
        return action()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (FinancingCommunicationError, FinancingApplicationError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _outcome_call(action):
    try:
        return action()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FinancingOutcomeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/financing-contacts")
async def list_financing_contacts(customer_id: str | None = None, institution_name: str | None = None,
                                  search: str | None = None, user: dict = Depends(get_current_user)):
    if customer_id:
        await require_customer_access(customer_id, user)
    elif str(user.get("role") or "").lower() not in {"admin", "operator"}:
        raise HTTPException(status_code=403, detail="请从有权查看的融资申请中查看联系人")
    reveal = str(user.get("role") or "").lower() in {"admin", "operator"}
    return {"contacts": communications.list_contacts(customer_id=customer_id,
        institution_name=institution_name, search=search, reveal_sensitive=reveal)}


@router.post("/financing-contacts")
async def create_financing_contact(request: ContactCreateRequest, user: dict = Depends(get_current_user)):
    _require_write(user)
    if request.customer_id: await require_customer_access(request.customer_id, user)
    actor_id, _ = _actor(user)
    return _communication_call(lambda: communications.create_contact(**request.model_dump(), actor_id=actor_id))


@router.patch("/financing-contacts/{contact_id}")
async def update_financing_contact(contact_id: str, request: ContactUpdateRequest,
                                   user: dict = Depends(get_current_user)):
    _require_write(user)
    current = _communication_call(lambda: communications.get_contact(contact_id, reveal_sensitive=True))
    if current.get("customer_id"): await require_customer_access(current["customer_id"], user)
    return _communication_call(lambda: communications.update_contact(
        contact_id, request.model_dump(exclude_unset=True)))


@router.get("/financing-applications/{application_id}/contacts")
async def list_application_contacts(application_id: str, user: dict = Depends(get_current_user)):
    await _authorized(application_id, user)
    reveal = str(user.get("role") or "").lower() in {"admin", "operator"}
    return {"contacts": communications.list_application_contacts(application_id, reveal_sensitive=reveal)}


@router.post("/financing-applications/{application_id}/contacts")
async def attach_application_contact(application_id: str, request: ApplicationContactRequest,
                                     user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: communications.attach_contact(
        application_id, request.contact_id, role=request.role, is_primary=request.is_primary,
        actor_id=actor_id, actor_name=actor_name))


@router.get("/financing-applications/{application_id}/communications")
async def list_application_communications(application_id: str, side: str | None = None,
                                          user: dict = Depends(get_current_user)):
    await _authorized(application_id, user)
    include_internal = str(user.get("role") or "").lower() in {"admin", "operator"}
    return {"communications": communications.list_communications(
        application_id, side=side, include_internal=include_internal)}


@router.post("/financing-applications/{application_id}/communications")
async def create_application_communication(application_id: str, request: CommunicationCreateRequest,
                                           user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: communications.create_communication(
        application_id, **request.model_dump(), actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-communications/{communication_id}/void")
async def void_financing_communication(communication_id: str, request: VoidCommunicationRequest,
                                      user: dict = Depends(get_current_user)):
    if str(user.get("role") or "").lower() != "admin":
        raise HTTPException(status_code=403, detail="只有管理员可以作废沟通记录")
    record = _communication_call(lambda: communications.get_communication(communication_id, include_internal=True))
    await _authorized(record["application_id"], user); actor_id, actor_name = _actor(user)
    return _communication_call(lambda: communications.void_communication(
        communication_id, reason=request.reason, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-communications/{communication_id}/create-task")
async def communication_create_task(communication_id: str, request: CommunicationTaskRequest,
                                    user: dict = Depends(get_current_user)):
    record = _communication_call(lambda: communications.get_communication(communication_id, include_internal=True))
    actor_id, actor_name = _actor(user)
    return await _mutate(record["application_id"], user, lambda: communications.create_task_from_communication(
        communication_id, **request.model_dump(), actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-communications/{communication_id}/create-supplement")
async def communication_create_supplement(communication_id: str,
                                          request: CommunicationSupplementRequest,
                                          user: dict = Depends(get_current_user)):
    record = _communication_call(lambda: communications.get_communication(communication_id, include_internal=True))
    actor_id, actor_name = _actor(user)
    return await _mutate(record["application_id"], user, lambda: communications.create_supplement_from_communication(
        communication_id, **request.model_dump(), actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-communications/{communication_id}/create-review-feedback")
async def communication_create_review_feedback(communication_id: str,
                                               request: CommunicationReviewFeedbackRequest,
                                               user: dict = Depends(get_current_user)):
    record = _communication_call(lambda: communications.get_communication(communication_id, include_internal=True))
    actor_id, actor_name = _actor(user)
    return await _mutate(record["application_id"], user, lambda: communications.create_review_feedback_from_communication(
        communication_id, feedback_type=request.feedback_type, actor_id=actor_id, actor_name=actor_name))


@router.get("/financing-followups")
async def list_financing_followups(application_id: str | None = None, customer_id: str | None = None,
                                   assignee_user_id: str | None = None, status: str | None = None,
                                   user: dict = Depends(get_current_user)):
    if application_id: await _authorized(application_id, user)
    elif customer_id: await require_customer_access(customer_id, user)
    elif str(user.get("role") or "").lower() not in {"admin", "operator"}:
        raise HTTPException(status_code=403, detail="无权查看全局跟进")
    return {"follow_ups": communications.list_follow_ups(application_id=application_id,
        customer_id=customer_id, assignee_user_id=assignee_user_id, status=status)}


@router.post("/financing-followups")
async def create_financing_followup(request: FollowUpCreateRequest,
                                    user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(request.application_id, user, lambda: communications.create_follow_up(
        **request.model_dump(), actor_id=actor_id, actor_name=actor_name))


@router.patch("/financing-followups/{follow_up_id}")
async def update_financing_followup(follow_up_id: str, request: FollowUpUpdateRequest,
                                    user: dict = Depends(get_current_user)):
    follow_up = _communication_call(lambda: communications.get_follow_up(follow_up_id))
    actor_id, actor_name = _actor(user)
    return await _mutate(follow_up["application_id"], user, lambda: communications.update_follow_up(
        follow_up_id, **request.model_dump(), actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-followups/{follow_up_id}/complete")
async def complete_financing_followup(follow_up_id: str, request: FollowUpUpdateRequest,
                                      user: dict = Depends(get_current_user)):
    follow_up = _communication_call(lambda: communications.get_follow_up(follow_up_id))
    actor_id, actor_name = _actor(user)
    return await _mutate(follow_up["application_id"], user, lambda: communications.update_follow_up(
        follow_up_id, status="completed", due_at=None, priority=None, result=request.result,
        actor_id=actor_id, actor_name=actor_name))


@router.get("/financing-applications/{application_id}/timeline")
async def financing_application_timeline(application_id: str, user: dict = Depends(get_current_user)):
    await _authorized(application_id, user)
    include_internal = str(user.get("role") or "").lower() in {"admin", "operator"}
    return {"timeline": communications.unified_timeline(application_id, include_internal=include_internal)}


@router.post("/financing-applications/from-plan")
async def create_from_plan(request: CreateApplicationsRequest, user: dict = Depends(get_current_user)):
    _require_write(user); await require_customer_access(request.customer_id, user)
    actor_id, _ = _actor(user)
    try:
        if applications.plan_version_customer_id(request.plan_version_id) != request.customer_id:
            raise HTTPException(status_code=403, detail="方案不属于当前客户")
        rows = applications.create_applications_from_confirmed_plan(
            request.plan_version_id, created_by=actor_id,
            responsible_user_id=request.responsible_user_id,
            responsible_user_name=request.responsible_user_name,
        )
        return {"applications": rows, "count": len(rows)}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FinancingApplicationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/customers/{customer_id}/financing-applications")
async def list_applications(customer_id: str, requirement_id: str | None = None, user: dict = Depends(get_current_user)):
    await require_customer_access(customer_id, user)
    rows = applications.list_applications(customer_id, requirement_id)
    return {"applications": rows, "count": len(rows)}


@router.get("/financing-applications")
async def list_all_applications(customer_id: str | None = None, institution_name: str | None = None,
                                status: str | None = None, responsible_user_id: str | None = None,
                                overdue_only: bool = False, date_from: date | None = None,
                                date_to: date | None = None, user: dict = Depends(get_current_user)):
    return {"applications": applications.list_all_applications(
        customer_id=customer_id, institution_name=institution_name, status=status,
        responsible_user_id=responsible_user_id, overdue_only=overdue_only,
        date_from=date_from, date_to=date_to,
    )}


@router.get("/financing-execution/dashboard")
async def execution_dashboard(customer_id: str | None = None, institution_name: str | None = None,
                              status: str | None = None, responsible_user_id: str | None = None,
                              overdue_only: bool = False, date_from: date | None = None,
                              date_to: date | None = None, user: dict = Depends(get_current_user)):
    result = applications.dashboard(customer_id=customer_id, institution_name=institution_name,
                                    status=status, responsible_user_id=responsible_user_id,
                                    overdue_only=overdue_only, date_from=date_from, date_to=date_to)
    result["follow_up_metrics"] = communications.follow_up_metrics(
        [row["application_id"] for row in result["applications"]])
    return result


@router.get("/financing-applications/{application_id}")
async def get_application(application_id: str, user: dict = Depends(get_current_user)):
    return await _authorized(application_id, user)


@router.get("/financing-applications/{application_id}/materials")
async def list_application_materials(application_id: str, user: dict = Depends(get_current_user)):
    await _authorized(application_id, user)
    return materials.list_materials(application_id)


@router.get("/financing-applications/{application_id}/materials/{material_id}/extraction")
async def get_application_material_extraction(application_id: str, material_id: str,
                                              user: dict = Depends(get_current_user)):
    await _authorized(application_id, user)
    try:
        return materials.get_material_extraction(application_id, material_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ApplicationMaterialError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/financing-applications/{application_id}/materials/match")
async def match_application_materials(application_id: str, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _material_mutate(application_id, user, lambda: materials.match_customer_materials_to_application(
        application_id, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/materials")
async def create_application_material(application_id: str, request: ApplicationMaterialCreateRequest,
                                      user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _material_mutate(application_id, user, lambda: materials.add_material(
        application_id, material_type=request.material_type, material_name=request.material_name,
        owner_type=request.owner_type, owner_id=request.owner_id, owner_name=request.owner_name,
        required=request.required, required_verified=request.required_verified,
        source_type=request.source_type, source_id=request.source_id,
        supplement_request_id=request.supplement_request_id,
        actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/materials/{material_id}/select")
async def select_application_material(application_id: str, material_id: str, request: MaterialSelectRequest,
                                      user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _material_mutate(application_id, user, lambda: materials.select_customer_material(
        application_id, material_id, request.document_id, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/materials/{material_id}/verify")
async def verify_application_material(application_id: str, material_id: str,
                                      user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _material_mutate(application_id, user, lambda: materials.verify_material(
        application_id, material_id, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/materials/{material_id}/replace")
async def replace_application_material(application_id: str, material_id: str, request: MaterialReplaceRequest,
                                       user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _material_mutate(application_id, user, lambda: materials.replace_material(
        application_id, material_id, request.document_id, rejection_reason=request.rejection_reason,
        actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/packages")
async def create_application_package(application_id: str, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _material_mutate(application_id, user, lambda: materials.create_application_package(
        application_id, actor_id=actor_id, actor_name=actor_name))


@router.get("/financing-applications/{application_id}/packages")
async def list_application_packages(application_id: str, user: dict = Depends(get_current_user)):
    await _authorized(application_id, user)
    return materials.list_package_overview(application_id)


@router.get("/financing-application-packages/{package_id}")
async def get_application_package(package_id: str, user: dict = Depends(get_current_user)):
    try: package = materials.get_application_package(package_id)
    except LookupError as exc: raise HTTPException(status_code=404, detail=str(exc)) from exc
    await _authorized(package["application_id"], user)
    return package


@router.post("/financing-application-packages/{package_id}/freeze")
async def freeze_application_package(package_id: str, user: dict = Depends(get_current_user)):
    try: package = materials.get_application_package(package_id)
    except LookupError as exc: raise HTTPException(status_code=404, detail=str(exc)) from exc
    actor_id, actor_name = _actor(user)
    return await _material_mutate(package["application_id"], user, lambda: materials.freeze_application_package(
        package_id, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-application-packages/{package_id}/submission-package")
async def create_submission_package(package_id: str, user: dict = Depends(get_current_user)):
    try: package = materials.get_application_package(package_id)
    except LookupError as exc: raise HTTPException(status_code=404, detail=str(exc)) from exc
    actor_id, actor_name = _actor(user)
    return await _material_mutate(package["application_id"], user, lambda: materials.create_submission_package(
        package_id, actor_id=actor_id, actor_name=actor_name))


@router.get("/financing-submission-packages/{submission_id}")
async def get_submission_package(submission_id: str, user: dict = Depends(get_current_user)):
    try: package = materials.get_submission_package(submission_id)
    except LookupError as exc: raise HTTPException(status_code=404, detail=str(exc)) from exc
    await _authorized(package["application_id"], user)
    return package


@router.get("/financing-submission-packages/{submission_id}/manifest")
async def get_submission_manifest(submission_id: str, user: dict = Depends(get_current_user)):
    package = await get_submission_package(submission_id, user)
    return package["manifest"]


@router.get("/financing-submission-packages/{submission_id}/zip")
async def download_submission_zip(submission_id: str, user: dict = Depends(get_current_user)):
    package = await get_submission_package(submission_id, user)
    try: file_name, content = materials.build_zip(package["submission_package_id"])
    except ApplicationMaterialError as exc: raise HTTPException(status_code=422, detail=str(exc)) from exc
    return Response(content=content, media_type="application/zip",
                    headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(file_name)}"})


@router.post("/financing-supplements/{supplement_id}/packages")
async def create_supplement_package(supplement_id: str, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    try:
        application_id = materials.supplement_application_id(supplement_id)
        return await _material_mutate(application_id, user, lambda: materials.create_supplement_package(
            supplement_id, actor_id=actor_id, actor_name=actor_name))
    except LookupError as exc: raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/financing-submission-packages/{submission_id}/submit")
async def submit_submission_package(submission_id: str, user: dict = Depends(get_current_user)):
    try: package = materials.get_submission_package(submission_id)
    except LookupError as exc: raise HTTPException(status_code=404, detail=str(exc)) from exc
    actor_id, actor_name = _actor(user)
    return await _material_mutate(package["application_id"], user, lambda: materials.mark_submission_package_submitted(
        submission_id, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-supplement-packages/{package_id}/submit")
async def submit_supplement_package(package_id: str, request: SupplementPackageSubmitRequest,
                                    user: dict = Depends(get_current_user)):
    try: package = materials.get_supplement_package(package_id)
    except LookupError as exc: raise HTTPException(status_code=404, detail=str(exc)) from exc
    actor_id, actor_name = _actor(user)
    return await _material_mutate(package["application_id"], user, lambda: materials.submit_supplement_package(
        package_id, submission_reference=request.submission_reference,
        actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/start-preparation")
async def start_preparation(application_id: str, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.start_preparation(application_id, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/ready-to-submit")
async def ready_to_submit(application_id: str, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.mark_ready_to_submit(application_id, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/submit")
async def submit(application_id: str, request: SubmitRequest, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.submit_application(
        application_id, submitted_amount=request.submitted_amount, submission_channel=request.submission_channel,
        submission_reference=request.submission_reference, submission_notes=request.submission_notes,
        actor_id=actor_id, actor_name=actor_name,
    ))


@router.post("/financing-applications/{application_id}/supplement")
async def request_supplement(application_id: str, request: SupplementRequest, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.request_supplement(
        application_id, description=request.description, required_materials=request.required_materials,
        due_date=request.due_date, actor_id=actor_id, actor_name=actor_name,
        requested_by_bank=request.requested_by_bank, notes=request.notes,
    ))


@router.post("/financing-applications/{application_id}/supplements")
async def create_supplement(application_id: str, request: SupplementRequest, user: dict = Depends(get_current_user)):
    return await request_supplement(application_id, request, user)


@router.patch("/financing-applications/{application_id}/supplements/{supplement_id}")
async def update_supplement(application_id: str, supplement_id: str, request: SupplementUpdateRequest,
                            user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    if not request.complete: raise HTTPException(status_code=422, detail="本次未提交可执行的补件动作")
    return await _mutate(application_id, user, lambda: applications.complete_supplement(
        application_id, supplement_id, actor_id=actor_id, actor_name=actor_name))


@router.patch("/financing-applications/{application_id}/materials/{material_id}")
async def update_application_material(application_id: str, material_id: str, request: MaterialUpdateRequest,
                                      user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.update_application_material(
        application_id, material_id, status=request.status, file_reference=request.file_reference,
        notes=request.notes, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/review-feedback")
async def review_feedback(application_id: str, request: ReviewFeedbackRequest, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.add_review_feedback(
        application_id, feedback_type=request.feedback_type, content=request.content,
        feedback_date=request.feedback_date, institution_contact=request.institution_contact,
        actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/under-review")
async def under_review(application_id: str, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.mark_under_review(application_id, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/approval")
async def approval(application_id: str, request: ApprovalRequest, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.record_approval(
        application_id, approved_amount=request.approved_amount, approved_term_months=request.approved_term_months,
        approved_interest_rate=request.approved_interest_rate, approval_reference=request.approval_reference,
        approved_at=request.approved_at, notes=request.notes, actor_id=actor_id, actor_name=actor_name,
        guarantee_method=request.guarantee_method, repayment_method=request.repayment_method,
        approval_expiry_date=request.approval_expiry_date, conditions=request.conditions,
    ))


@router.post("/financing-applications/{application_id}/approval-conditions/{condition_id}")
async def update_approval_condition(application_id: str, condition_id: str, request: ApprovalConditionRequest,
                                    user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.update_approval_condition(
        application_id, condition_id, status=request.status, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/rejection")
async def rejection(application_id: str, request: RejectionRequest, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.record_rejection(
        application_id, rejection_reason=request.rejection_reason, rejection_code=request.rejection_code,
        rejected_at=request.rejected_at, notes=request.notes, actor_id=actor_id, actor_name=actor_name,
    ))


@router.post("/financing-applications/{application_id}/disbursement")
async def disbursement(application_id: str, request: DisbursementRequest, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.record_disbursement(
        application_id, disbursed_amount=request.disbursed_amount, disbursed_at=request.disbursed_at,
        disbursement_reference=request.disbursement_reference, actor_id=actor_id, actor_name=actor_name,
        recipient_name=request.recipient_name, recipient_account_masked=request.recipient_account_masked,
        purpose=request.purpose, notes=request.notes,
    ))


@router.post("/financing-applications/{application_id}/disbursements")
async def create_disbursement(application_id: str, request: DisbursementRequest, user: dict = Depends(get_current_user)):
    return await disbursement(application_id, request, user)


@router.post("/financing-applications/{application_id}/advance-stage")
async def advance_stage(application_id: str, request: StageAdvanceRequest, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.advance_application_stage(
        application_id, request.target_stage_code, actor_id=actor_id, actor_name=actor_name))


@router.patch("/financing-applications/{application_id}/tasks/{task_id}")
async def update_task(application_id: str, task_id: str, request: TaskUpdateRequest, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.update_task(
        application_id, task_id, status=request.status, actor_id=actor_id, actor_name=actor_name,
        assignee_user_id=request.assignee_user_id, assignee_user_name=request.assignee_user_name,
        priority=request.priority, due_date=request.due_date, notes=request.notes,
    ))


@router.post("/financing-applications/tasks/{task_id}/complete")
async def complete_task(task_id: str, request: TaskCompleteRequest,
                        user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    try: application_id = applications.task_application_id(task_id)
    except LookupError as exc: raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _mutate(application_id, user, lambda: applications.complete_task(
        application_id, task_id, notes=request.notes, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/tasks/{task_id}/block")
async def block_task(task_id: str, request: TaskBlockRequest,
                     user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    try: application_id = applications.task_application_id(task_id)
    except LookupError as exc: raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _mutate(application_id, user, lambda: applications.block_task(
        application_id, task_id, reason=request.block_reason, actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/tasks/{task_id}/unblock")
async def unblock_task(task_id: str, request: TaskUnblockRequest,
                       user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    try: application_id = applications.task_application_id(task_id)
    except LookupError as exc: raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _mutate(application_id, user, lambda: applications.unblock_task(
        application_id, task_id, reason=request.reason, target_status=request.target_status,
        actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/retry")
async def retry(application_id: str, user: dict = Depends(get_current_user)):
    actor_id, actor_name = _actor(user)
    return await _mutate(application_id, user, lambda: applications.retry_application(application_id, actor_id=actor_id, actor_name=actor_name))


@router.get("/financing-applications/{application_id}/outcome")
async def get_application_outcome(application_id: str, user: dict = Depends(get_current_user)):
    await _authorized(application_id, user)
    return {"outcome": _outcome_call(lambda: outcomes.get_outcome(application_id))}


@router.post("/financing-applications/{application_id}/outcome/finalize")
async def finalize_application_outcome(application_id: str, request: OutcomeFinalizeRequest,
                                       user: dict = Depends(get_current_user)):
    _require_write(user); await _authorized(application_id, user)
    actor_id, actor_name = _actor(user)
    return _outcome_call(lambda: outcomes.finalize_outcome(
        application_id, rejection_reasons=request.rejection_reasons,
        disbursement_variance_reason=request.disbursement_variance_reason,
        final_notes=request.final_notes, actor_id=actor_id, actor_name=actor_name,
        source_type=request.source_type,
    ))


@router.post("/financing-applications/{application_id}/outcome/correct")
async def correct_application_outcome(application_id: str, request: OutcomeCorrectionRequest,
                                      user: dict = Depends(get_current_user)):
    if str(user.get("role") or "").lower() != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可修正已定稿结果")
    await _authorized(application_id, user)
    actor_id, actor_name = _actor(user)
    return _outcome_call(lambda: outcomes.correct_outcome(
        application_id, changes=request.changes, reason=request.reason,
        actor_id=actor_id, actor_name=actor_name))


@router.post("/financing-applications/{application_id}/close")
async def close_financing_application(application_id: str, request: CloseApplicationRequest,
                                      user: dict = Depends(get_current_user)):
    _require_write(user); await _authorized(application_id, user)
    actor_id, actor_name = _actor(user)
    return _outcome_call(lambda: outcomes.close_application(
        application_id, close_reason=request.close_reason,
        final_result_confirmed=request.final_result_confirmed,
        actor_id=actor_id, actor_name=actor_name))


@router.get("/products/{product_id}/execution-metrics")
async def product_execution_metrics(product_id: str, product_version_id: str | None = None,
                                    user: dict = Depends(get_current_user)):
    return _outcome_call(lambda: outcomes.product_execution_metrics(
        product_id, product_version_id=product_version_id))


@router.get("/products/{product_id}/rule-feedback")
async def list_product_rule_feedback(product_id: str, user: dict = Depends(get_current_user)):
    return {"items": _outcome_call(lambda: outcomes.list_rule_feedback(product_id))}


@router.post("/products/{product_id}/rule-feedback")
async def create_product_rule_feedback(product_id: str, request: RuleFeedbackRequest,
                                       user: dict = Depends(get_current_user)):
    _require_write(user)
    current = await _authorized(request.application_id, user)
    if current["product_id"] != product_id:
        raise HTTPException(status_code=422, detail="产品与融资申请不一致")
    actor_id, _ = _actor(user)
    return _outcome_call(lambda: outcomes.create_rule_feedback(
        product_id, product_version_id=request.product_version_id, rule_id=request.rule_id,
        application_id=request.application_id, feedback_type=request.feedback_type,
        expected_result=request.expected_result,
        actual_bank_feedback=request.actual_bank_feedback,
        evidence_source=request.evidence_source,
        evidence_reference=request.evidence_reference, actor_id=actor_id))


@router.patch("/product-rule-feedback/{feedback_id}")
async def update_product_rule_feedback(feedback_id: str, request: RuleFeedbackUpdateRequest,
                                       user: dict = Depends(get_current_user)):
    if str(user.get("role") or "").lower() != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可审核产品规则反馈")
    actor_id, _ = _actor(user)
    return _outcome_call(lambda: outcomes.update_rule_feedback(
        feedback_id, status=request.status, actor_id=actor_id))


@router.get("/customers/{customer_id}/financing-review")
async def customer_financing_review(customer_id: str, requirement_id: str | None = None,
                                    user: dict = Depends(get_current_user)):
    await require_customer_access(customer_id, user)
    return _outcome_call(lambda: outcomes.customer_financing_review(
        customer_id, requirement_id=requirement_id))


@router.get("/financing-requirements/{requirement_id}/execution-summary")
async def financing_requirement_execution_summary(requirement_id: str,
                                                   user: dict = Depends(get_current_user)):
    try:
        customer_id = applications.requirement_customer_id(requirement_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await require_customer_access(customer_id, user)
    return _outcome_call(lambda: outcomes.customer_financing_review(
        customer_id, requirement_id=requirement_id))
