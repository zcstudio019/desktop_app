"""Backend services module"""

import sys
from pathlib import Path

# Add desktop_app to path for imports
desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from config import LOCAL_DB_PATH, USE_LOCAL_STORAGE

from .local_storage_service import LocalStorageService

# 尝试导入 FeishuService（可能不在 backend/services 目录）
try:
    from services.feishu_service import FeishuService
except ImportError:
    # 如果导入失败，创建一个占位类
    class FeishuService:
        """FeishuService 占位类（实际服务在 services/ 目录）"""
        def __init__(self):
            raise NotImplementedError("FeishuService 需要从 services/ 目录导入")


def get_storage_service() -> LocalStorageService | FeishuService:
    """
    根据配置返回存储服务实例

    Returns:
        LocalStorageService: 当 USE_LOCAL_STORAGE=True 时返回本地存储服务
        FeishuService: 当 USE_LOCAL_STORAGE=False 时返回飞书存储服务

    Requirements:
        - 2.1: 支持通过 USE_LOCAL_STORAGE 环境变量切换存储后端
        - 2.2: 本地存储使用 LOCAL_DB_PATH 配置的数据库路径
    """
    if USE_LOCAL_STORAGE:
        return LocalStorageService(db_path=LOCAL_DB_PATH)
    else:
        # 导入实际的 FeishuService
        from services.feishu_service import FeishuService as RealFeishuService
        return RealFeishuService()


__all__ = ['FeishuService', 'LocalStorageService', 'get_storage_service']
