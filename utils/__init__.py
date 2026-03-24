"""工具模块

提供以下功能：
- JSON 解析与修复
- 重试处理与断路器
- 检查点管理（断点恢复）
- 任务日志（7 维度可观测性）
- 上下文压缩
- 交接 Schema 验证
- 记忆分层管理
- 内置 Reflection 模式
"""
from .json_parser import parse_json, clean_json_string
from .retry_handler import (
    RetryHandler,
    RetryConfig,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
    ErrorCategory,
    with_circuit_breaker,
    with_retry_and_circuit_breaker,
    create_ai_retry_handler,
    create_ocr_retry_handler,
    create_feishu_retry_handler,
    is_ai_error_retriable,
    is_ocr_error_retriable,
    is_feishu_error_retriable,
)
from .checkpoint_manager import (
    CheckpointManager,
    Checkpoint,
    create_task_checkpoint,
    load_task_checkpoint,
)
from .task_logger import (
    TaskLogger,
    LogDimension,
    LogLevel,
    LogEntry,
    create_task_logger,
    log_task_start,
    log_task_end,
)
from .context_compressor import (
    ContextCompressor,
    CompressionStrategy,
    CompressionResult,
    Message,
    smart_compress,
    compress_for_subagent,
)
from .handoff_validator import (
    HandoffValidator,
    TaskHandoff,
    TaskType,
    TaskPriority,
    ValidationError,
    validate_handoff,
    create_handoff,
    handoff_to_prompt,
)
from .memory_manager import (
    MemoryManager,
    MemoryLayer,
    MemoryItem,
    create_memory_manager,
)
from .reflection_engine import (
    ReflectionEngine,
    ReflectionPhase,
    QualityLevel,
    ReflectionResult,
    ReviewFinding,
    create_reflection_engine,
    quick_reflect,
)

__all__ = [
    # JSON 解析
    "parse_json",
    "clean_json_string",
    # 重试处理
    "RetryHandler",
    "RetryConfig",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "CircuitState",
    "ErrorCategory",
    "with_circuit_breaker",
    "with_retry_and_circuit_breaker",
    "create_ai_retry_handler",
    "create_ocr_retry_handler",
    "create_feishu_retry_handler",
    "is_ai_error_retriable",
    "is_ocr_error_retriable",
    "is_feishu_error_retriable",
    # 检查点管理
    "CheckpointManager",
    "Checkpoint",
    "create_task_checkpoint",
    "load_task_checkpoint",
    # 任务日志
    "TaskLogger",
    "LogDimension",
    "LogLevel",
    "LogEntry",
    "create_task_logger",
    "log_task_start",
    "log_task_end",
    # 上下文压缩
    "ContextCompressor",
    "CompressionStrategy",
    "CompressionResult",
    "Message",
    "smart_compress",
    "compress_for_subagent",
    # 交接验证
    "HandoffValidator",
    "TaskHandoff",
    "TaskType",
    "TaskPriority",
    "ValidationError",
    "validate_handoff",
    "create_handoff",
    "handoff_to_prompt",
    # 记忆管理
    "MemoryManager",
    "MemoryLayer",
    "MemoryItem",
    "create_memory_manager",
    # 反思引擎
    "ReflectionEngine",
    "ReflectionPhase",
    "QualityLevel",
    "ReflectionResult",
    "ReviewFinding",
    "create_reflection_engine",
    "quick_reflect",
]
