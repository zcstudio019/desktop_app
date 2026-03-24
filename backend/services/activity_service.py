"""
Activity Service

Manages activity logging and dashboard statistics.
Stores data in a local JSON file for persistence.

Features:
- Track file uploads, application generation, and scheme matching
- Calculate dashboard statistics (today uploads, pending, completed, total customers)
- Maintain customer status (hasApplication, hasMatching)
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any

# Configure logging
logger = logging.getLogger(__name__)

# File lock for thread-safe operations.
# Use a re-entrant lock because load_activity_log() may initialize the file
# by calling save_activity_log() while already holding the lock.
_file_lock = RLock()


def _get_data_file_path() -> Path:
    """Get the path to the activity log file.

    Supports both development and portable environments.

    Returns:
        Path to activity_log.json
    """
    # Try multiple possible locations
    possible_paths = [
        # Development: desktop_app/data/activity_log.json
        Path(__file__).parent.parent.parent / "data" / "activity_log.json",
        # Portable: same directory as backend
        Path(__file__).parent.parent / "data" / "activity_log.json",
        # Fallback: current working directory
        Path.cwd() / "data" / "activity_log.json",
    ]

    for path in possible_paths:
        if path.exists():
            return path
        # Check if parent directory exists (we can create the file)
        if path.parent.exists():
            return path

    # Default to first option and create directory if needed
    default_path = possible_paths[0]
    default_path.parent.mkdir(parents=True, exist_ok=True)
    return default_path


def _get_empty_data() -> dict[str, Any]:
    """Get empty data structure."""
    return {"customers": {}, "activities": []}


def load_activity_log() -> dict[str, Any]:
    """Load activity log from local JSON file.

    Returns:
        Dictionary with customers and activities data
    """
    file_path = _get_data_file_path()

    with _file_lock:
        try:
            if not file_path.exists():
                logger.info(f"Activity log file not found, creating: {file_path}")
                save_activity_log(_get_empty_data())
                return _get_empty_data()

            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
                logger.debug(f"Loaded activity log with {len(data.get('activities', []))} activities")
                return data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse activity log: {e}")
            return _get_empty_data()
        except Exception as e:
            logger.error(f"Failed to load activity log: {e}")
            return _get_empty_data()


def save_activity_log(data: dict[str, Any]) -> bool:
    """Save activity log to local JSON file.

    Args:
        data: Dictionary with customers and activities data

    Returns:
        True if save successful, False otherwise
    """
    file_path = _get_data_file_path()

    with _file_lock:
        try:
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.debug(f"Saved activity log to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save activity log: {e}")
            return False


def add_activity(
    activity_type: str,
    filename: str | None = None,
    customer: str | None = None,
    status: str = "completed",
    document_type: str | None = None,
) -> str | None:
    """Add a new activity record.

    Args:
        activity_type: Type of activity (upload, application, matching)
        filename: Name of the file (for upload activities)
        customer: Customer name
        status: Activity status (completed, processing, error)
        document_type: Type of document (for upload activities)

    Returns:
        Activity ID if successful, None otherwise
    """
    data = load_activity_log()

    activity_id = str(uuid.uuid4())[:8]
    activity = {"id": activity_id, "type": activity_type, "time": datetime.now(tz=timezone.utc).isoformat(), "status": status}

    if filename:
        activity["filename"] = filename
    if customer:
        activity["customer"] = customer
    if document_type:
        activity["documentType"] = document_type

    # Add to beginning of list (most recent first)
    data["activities"].insert(0, activity)

    # Keep only last 100 activities
    data["activities"] = data["activities"][:100]

    if save_activity_log(data):
        logger.info(f"Added activity: {activity_type} for {customer or filename}")
        return activity_id
    return None


def update_customer_status(
    customer: str, has_application: bool | None = None, has_matching: bool | None = None
) -> bool:
    """Update customer status.

    Args:
        customer: Customer name
        has_application: Whether customer has application generated
        has_matching: Whether customer has scheme matched

    Returns:
        True if update successful, False otherwise
    """
    if not customer:
        return False

    data = load_activity_log()

    # Initialize customer if not exists
    if customer not in data["customers"]:
        data["customers"][customer] = {
            "hasApplication": False,
            "hasMatching": False,
            "lastUpdate": datetime.now(tz=timezone.utc).isoformat(),
        }

    # Update status
    if has_application is not None:
        data["customers"][customer]["hasApplication"] = has_application
    if has_matching is not None:
        data["customers"][customer]["hasMatching"] = has_matching

    data["customers"][customer]["lastUpdate"] = datetime.now(tz=timezone.utc).isoformat()

    if save_activity_log(data):
        logger.info(f"Updated customer status: {customer}")
        return True
    return False


def get_dashboard_stats(feishu_records: list[dict] | None = None) -> dict[str, int]:
    """Get dashboard statistics.

    Args:
        feishu_records: Optional list of Feishu records (to avoid duplicate API calls)

    Returns:
        Dictionary with todayUploads, pending, completed, totalCustomers
    """
    data = load_activity_log()
    customers = data.get("customers", {})

    # Calculate pending and completed from local data
    pending = 0
    completed = 0

    for _customer_name, customer_data in customers.items():
        has_app = customer_data.get("hasApplication", False)
        has_match = customer_data.get("hasMatching", False)

        if has_app and has_match:
            completed += 1
        elif has_app or has_match:
            pending += 1

    # Calculate today uploads from activities
    today = datetime.now(tz=timezone.utc).date().isoformat()
    today_uploads = 0

    for activity in data.get("activities", []):
        if activity.get("type") == "upload":
            activity_time = activity.get("time", "")
            if activity_time.startswith(today):
                today_uploads += 1

    # Total customers from Feishu (if provided) or local data
    total_customers = len(feishu_records) if feishu_records is not None else len(customers)

    return {
        "todayUploads": today_uploads,
        "pending": pending,
        "completed": completed,
        "totalCustomers": total_customers,
    }


def get_recent_activities(limit: int = 10) -> list[dict[str, Any]]:
    """Get recent activities.

    Args:
        limit: Maximum number of activities to return

    Returns:
        List of recent activities
    """
    data = load_activity_log()
    activities = data.get("activities", [])

    # Format activities for frontend
    formatted = []
    for activity in activities[:limit]:
        formatted_activity = {
            "id": activity.get("id", ""),
            "type": activity.get("type", ""),
            "time": _format_relative_time(activity.get("time", "")),
            "status": activity.get("status", "completed"),
        }

        # Add optional fields
        if "filename" in activity:
            formatted_activity["fileName"] = activity["filename"]
        if "customer" in activity:
            formatted_activity["customerName"] = activity["customer"]
        if "documentType" in activity:
            formatted_activity["fileType"] = activity["documentType"]

        formatted.append(formatted_activity)

    return formatted


def _format_relative_time(iso_time: str) -> str:
    """Format ISO time string to relative time.

    Args:
        iso_time: ISO format time string

    Returns:
        Relative time string (e.g., "10 分钟前")
    """
    if not iso_time:
        return "未知时间"

    try:
        dt = datetime.fromisoformat(iso_time)
        now = datetime.now(tz=timezone.utc)
        diff = now - dt

        seconds = diff.total_seconds()

        if seconds < 60:
            return "刚刚"
        elif seconds < 3600:
            minutes = int(seconds / 60)
            return f"{minutes} 分钟前"
        elif seconds < 86400:
            hours = int(seconds / 3600)
            return f"{hours} 小时前"
        elif seconds < 604800:
            days = int(seconds / 86400)
            return f"{days} 天前"
        else:
            return dt.strftime("%Y-%m-%d")
    except Exception:
        return "未知时间"
