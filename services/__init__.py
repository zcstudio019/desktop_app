"""服务模块

提供各种外部服务的封装，包括：
- AIService: DeepSeek AI 服务
- OCRService: 百度 OCR 服务
- FeishuService: 飞书多维表格服务
- WikiService: 飞书知识库服务（产品库访问）
- FileService: 文件处理服务

每个服务都有对应的自定义异常类，便于错误处理。
"""
from .ai_service import (
    AIService,
    AIServiceError,
    AIAPIError,
    AITimeoutError,
    AIRateLimitError
)
from .ocr_service import (
    OCRService,
    OCRServiceError,
    OCRConfigError,
    OCRImageError,
    OCRAPIError,
    OCRQuotaError
)
from .feishu_service import (
    FeishuService,
    FeishuServiceError,
    FeishuConfigError,
    FeishuAuthError,
    FeishuAPIError,
    FeishuNetworkError
)
from .wiki_service import (
    WikiService,
    WikiServiceError,
    WikiConfigError,
    WikiAuthError,
    WikiAPIError,
    WikiNetworkError
)
from .file_service import FileService

__all__ = [
    # AI 服务
    "AIService",
    "AIServiceError",
    "AIAPIError",
    "AITimeoutError",
    "AIRateLimitError",
    # OCR 服务
    "OCRService",
    "OCRServiceError",
    "OCRConfigError",
    "OCRImageError",
    "OCRAPIError",
    "OCRQuotaError",
    # 飞书服务
    "FeishuService",
    "FeishuServiceError",
    "FeishuConfigError",
    "FeishuAuthError",
    "FeishuAPIError",
    "FeishuNetworkError",
    # 知识库服务
    "WikiService",
    "WikiServiceError",
    "WikiConfigError",
    "WikiAuthError",
    "WikiAPIError",
    "WikiNetworkError",
    # 文件服务
    "FileService"
]
