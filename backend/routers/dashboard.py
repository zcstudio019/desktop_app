"""
Dashboard Router

Provides endpoints for dashboard statistics and recent activities.

Endpoints:
- GET /dashboard/stats - Get dashboard statistics
- GET /dashboard/activities - Get recent activities
"""

import asyncio
import logging
import sys
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

# Add desktop_app to path for imports
desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from services.feishu_service import FeishuService, FeishuServiceError

from backend.services import get_storage_service, supports_structured_storage
from backend.services.risk_assessment_service import RiskAssessmentService
from ..services.activity_service import (
    get_dashboard_stats,
    get_recent_activities,
)
from ..middleware.auth import get_current_user

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

# Initialize services
feishu_service = FeishuService()
storage_service = get_storage_service()
HAS_DB_STORAGE = supports_structured_storage(storage_service)
risk_assessment_service = RiskAssessmentService()
FEISHU_STATS_TIMEOUT_SECONDS = 5
DASHBOARD_STATS_FAILED_MESSAGE = "获取工作台统计失败，请稍后重试。"
ACTIVITIES_FAILED_MESSAGE = "获取最近活动失败，请稍后重试。"


# ==================== Response Models ====================


class DashboardStatsResponse(BaseModel):
    """Dashboard statistics response model."""

    todayUploads: int
    pending: int
    completed: int
    totalCustomers: int
    pendingMaterialCustomers: int = 0
    reportedCustomers: int = 0
    highRiskCustomers: int = 0


class ActivityResponse(BaseModel):
    """Single activity response model."""

    id: str
    type: str
    time: str
    createdAt: str = ""
    status: str
    fileName: str = ""
    fileType: str = ""
    customerName: str = ""
    customerId: str = ""
    username: str = ""
    title: str = ""
    description: str = ""
    metadata: dict = {}


class ActivitiesResponse(BaseModel):
    """Activities list response model."""

    activities: list[ActivityResponse]


# ==================== Endpoints ====================


async def _load_feishu_records_for_stats() -> list[dict] | None:
    """Load Feishu records without blocking the server in local mode or on slow networks."""
    if HAS_DB_STORAGE:
        logger.info("Structured DB storage enabled; skipping Feishu fetch for dashboard stats")
        return None

    try:
        return await asyncio.wait_for(
            asyncio.to_thread(feishu_service.get_all_records),
            timeout=FEISHU_STATS_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "Timed out fetching Feishu records for dashboard stats after %s seconds; falling back to local stats",
            FEISHU_STATS_TIMEOUT_SECONDS,
        )
        return None
    except FeishuServiceError:
        raise
    except Exception as exc:
        logger.warning("Unexpected error fetching Feishu records for dashboard stats: %s", exc)
        return None


@router.get("/stats", response_model=DashboardStatsResponse)
async def get_stats(_current_user: dict = Depends(get_current_user)) -> DashboardStatsResponse:
    """
    Get dashboard statistics.

    Returns:
        DashboardStatsResponse with:
        - todayUploads: Number of files uploaded today
        - pending: Number of customers with partial processing
        - completed: Number of customers with full processing
        - totalCustomers: Total number of customers in Feishu
    """
    logger.info("Getting dashboard stats")

    try:
        # In local mode we compute entirely from local activity/storage data.
        # In Feishu mode the fetch is isolated in a worker thread and timed out
        # so one slow upstream call cannot stall every other API route.
        feishu_records = await _load_feishu_records_for_stats()

        # Get baseline activity stats.
        stats = get_dashboard_stats(feishu_records)

        if HAS_DB_STORAGE:
            customers = await storage_service.list_customers()
            stats["totalCustomers"] = len(customers)

            pending_material_customers = 0
            reported_customers = 0
            high_risk_customers = 0

            for customer in customers:
                if not isinstance(customer, dict):
                    logger.warning("Skipping invalid customer row in dashboard stats: %r", customer)
                    continue

                customer_id = customer.get("customer_id") or ""
                if not customer_id:
                    continue

                try:
                    latest_report = await storage_service.get_latest_customer_risk_report(customer_id)
                    if isinstance(latest_report, dict):
                        reported_customers += 1
                        risk_level = (
                            ((latest_report.get("report_json") or {}).get("overall_assessment") or {}).get("risk_level") or ""
                        )
                        if str(risk_level).lower() == "high":
                            high_risk_customers += 1
                    elif latest_report:
                        logger.warning("Skipping invalid risk report snapshot in dashboard stats for %s: %r", customer_id, latest_report)

                    material_summary = await risk_assessment_service.summarize_customer_materials(storage_service, customer_id)
                    if isinstance(material_summary, dict) and material_summary.get("missing_items"):
                        pending_material_customers += 1
                    elif material_summary and not isinstance(material_summary, dict):
                        logger.warning("Skipping invalid material summary in dashboard stats for %s: %r", customer_id, material_summary)
                except Exception as customer_exc:
                    logger.warning("Skipping customer during dashboard stats %s due to error: %s", customer_id, customer_exc)
                    continue

            stats["pendingMaterialCustomers"] = pending_material_customers
            stats["reportedCustomers"] = reported_customers
            stats["highRiskCustomers"] = high_risk_customers

        logger.info(f"Dashboard stats: {stats}")
        return DashboardStatsResponse(**stats)

    except FeishuServiceError as e:
        logger.error(f"Feishu service error: {e}")
        # Return stats without Feishu data
        stats = get_dashboard_stats(None)
        return DashboardStatsResponse(**stats)
    except Exception as e:
        logger.error(f"Error getting dashboard stats: {e}")
        raise HTTPException(status_code=500, detail=DASHBOARD_STATS_FAILED_MESSAGE) from e


@router.get("/activities", response_model=ActivitiesResponse)
async def get_activities(limit: int = 10, _current_user: dict = Depends(get_current_user)) -> ActivitiesResponse:
    """
    Get recent activities.

    Args:
        limit: Maximum number of activities to return (default: 10)

    Returns:
        ActivitiesResponse with list of recent activities
    """
    logger.info(f"Getting recent activities, limit: {limit}")

    try:
        activities = get_recent_activities(limit)

        # Convert to response model
        activity_responses = []
        for activity in activities:
            activity_responses.append(
                ActivityResponse(
                    id=activity.get("id", ""),
                    type=activity.get("type", ""),
                    time=activity.get("time", ""),
                    createdAt=activity.get("createdAt", ""),
                    status=activity.get("status", "completed"),
                    fileName=activity.get("fileName", ""),
                    fileType=activity.get("fileType", ""),
                    customerName=activity.get("customerName", ""),
                    customerId=activity.get("customerId", ""),
                    username=activity.get("username", ""),
                    title=activity.get("title", ""),
                    description=activity.get("description", ""),
                    metadata=activity.get("metadata", {}) or {},
                )
            )

        logger.info(f"Returning {len(activity_responses)} activities")
        return ActivitiesResponse(activities=activity_responses)

    except Exception as e:
        logger.error(f"Error getting activities: {e}")
        raise HTTPException(status_code=500, detail=ACTIVITIES_FAILED_MESSAGE) from e
