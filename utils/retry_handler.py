"""错误处理与重试工具

提供指数退避重试、断路器模式、错误分类等功能。

基于业界最佳实践：
- 指数退避 + 抖动（防止雷鸣群效应）
- 断路器模式（防止级联故障）
- 错误分类（可重试 vs 不可重试）

Usage:
    from utils.retry_handler import RetryHandler, CircuitBreaker
    
    # 使用重试装饰器
    @RetryHandler.with_retry(max_retries=3)
    def call_api():
        ...
    
    # 使用断路器
    breaker = CircuitBreaker("api_name")
    if breaker.can_execute():
        try:
            result = call_api()
            breaker.record_success()
        except Exception as e:
            breaker.record_failure()
"""
import time
import random
import logging
import functools
from typing import Callable, TypeVar, Any, Optional, Type, Tuple
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ErrorCategory(Enum):
    """错误分类"""
    RETRIABLE = "retriable"      # 可重试：网络超时、限流、5xx
    NON_RETRIABLE = "non_retriable"  # 不可重试：4xx、参数错误、认证失败


@dataclass
class RetryConfig:
    """重试配置"""
    max_retries: int = 3          # 最大重试次数
    base_delay: float = 1.0       # 基础等待时间（秒）
    max_delay: float = 30.0       # 最大等待时间（秒）
    jitter: float = 0.1           # 抖动比例（0-1）
    
    # 可重试的异常类型（默认为空，需要调用方指定）
    retriable_exceptions: Tuple[Type[Exception], ...] = field(default_factory=tuple)
    
    # 可重试的 HTTP 状态码
    retriable_status_codes: Tuple[int, ...] = (429, 500, 502, 503, 504)


class RetryHandler:
    """重试处理器
    
    实现指数退避 + 抖动的重试策略。
    
    公式：delay = min(base_delay * 2^attempt + jitter, max_delay)
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
    
    def calculate_delay(self, attempt: int) -> float:
        """计算第 N 次重试的等待时间
        
        Args:
            attempt: 重试次数（从 0 开始）
            
        Returns:
            等待时间（秒）
        """
        # 指数退避
        delay = min(
            self.config.base_delay * (2 ** attempt),
            self.config.max_delay
        )
        
        # 添加抖动（防止同步重试）
        jitter_amount = delay * self.config.jitter
        delay += random.uniform(0, jitter_amount)
        
        return delay
    
    def is_retriable(self, exception: Exception) -> bool:
        """判断异常是否可重试
        
        Args:
            exception: 捕获的异常
            
        Returns:
            True 如果可重试
        """
        # 检查是否在可重试异常列表中
        if self.config.retriable_exceptions:
            if isinstance(exception, self.config.retriable_exceptions):
                return True
        
        # 检查 HTTP 状态码（如果异常包含 status_code 属性）
        status_code = getattr(exception, 'status_code', None)
        if status_code and status_code in self.config.retriable_status_codes:
            return True
        
        # 检查错误码（如果异常包含 error_code 属性）
        error_code = getattr(exception, 'error_code', None)
        if error_code:
            # 常见的可重试错误码
            retriable_error_codes = {429, 500, 502, 503, 504, 17, 18}  # 包含百度 OCR 限流码
            if error_code in retriable_error_codes:
                return True
        
        return False
    
    def execute_with_retry(
        self, 
        func: Callable[..., T], 
        *args, 
        **kwargs
    ) -> T:
        """执行函数，失败时自动重试
        
        Args:
            func: 要执行的函数
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            函数返回值
            
        Raises:
            最后一次重试的异常
        """
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                # 判断是否可重试
                if not self.is_retriable(e):
                    logger.warning(f"不可重试错误，立即失败: {type(e).__name__}: {e}")
                    raise
                
                # 检查是否还有重试机会
                if attempt >= self.config.max_retries:
                    logger.error(f"重试 {self.config.max_retries} 次后仍失败: {e}")
                    raise
                
                # 计算等待时间
                delay = self.calculate_delay(attempt)
                logger.warning(
                    f"第 {attempt + 1} 次重试失败: {type(e).__name__}: {e}，"
                    f"等待 {delay:.2f} 秒后重试"
                )
                time.sleep(delay)
        
        # 不应该到达这里
        raise last_exception
    
    @classmethod
    def with_retry(
        cls,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        jitter: float = 0.1,
        retriable_exceptions: Tuple[Type[Exception], ...] = ()
    ):
        """装饰器：为函数添加重试功能
        
        Usage:
            @RetryHandler.with_retry(max_retries=3)
            def call_api():
                ...
        """
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> T:
                config = RetryConfig(
                    max_retries=max_retries,
                    base_delay=base_delay,
                    max_delay=max_delay,
                    jitter=jitter,
                    retriable_exceptions=retriable_exceptions
                )
                handler = cls(config)
                return handler.execute_with_retry(func, *args, **kwargs)
            return wrapper
        return decorator


class CircuitState(Enum):
    """断路器状态"""
    CLOSED = "closed"      # 正常状态，允许请求
    OPEN = "open"          # 熔断状态，拒绝请求
    HALF_OPEN = "half_open"  # 半开状态，允许少量请求测试


@dataclass
class CircuitBreakerConfig:
    """断路器配置"""
    failure_threshold: int = 3       # 失败阈值，达到后熔断
    success_threshold: int = 2       # 半开状态下成功阈值，达到后恢复
    timeout: float = 30.0            # 熔断超时时间（秒），超时后进入半开状态
    half_open_max_calls: int = 1     # 半开状态下允许的最大请求数


class CircuitBreaker:
    """断路器
    
    防止级联故障，当服务持续失败时自动熔断。
    
    状态转换：
    - CLOSED → OPEN：连续失败达到阈值
    - OPEN → HALF_OPEN：超时后自动转换
    - HALF_OPEN → CLOSED：测试请求成功
    - HALF_OPEN → OPEN：测试请求失败
    
    Usage:
        breaker = CircuitBreaker("api_name")
        
        if breaker.can_execute():
            try:
                result = call_api()
                breaker.record_success()
            except Exception as e:
                breaker.record_failure()
                raise
        else:
            raise CircuitBreakerOpenError("服务暂时不可用")
    """
    
    # 全局断路器注册表（按名称共享状态）
    _instances: dict = {}
    
    def __new__(cls, name: str, config: Optional[CircuitBreakerConfig] = None):
        """单例模式：同名断路器共享状态"""
        if name not in cls._instances:
            instance = super().__new__(cls)
            instance._initialized = False
            cls._instances[name] = instance
        return cls._instances[name]
    
    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        if self._initialized:
            return
        
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._half_open_calls = 0
        self._initialized = True
    
    @property
    def state(self) -> CircuitState:
        """获取当前状态（自动检查超时转换）"""
        if self._state == CircuitState.OPEN:
            # 检查是否超时，应该转换到半开状态
            if self._last_failure_time:
                elapsed = (datetime.now() - self._last_failure_time).total_seconds()
                if elapsed >= self.config.timeout:
                    logger.info(f"断路器 [{self.name}] 超时，从 OPEN 转换到 HALF_OPEN")
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    self._success_count = 0
        
        return self._state
    
    def can_execute(self) -> bool:
        """检查是否允许执行请求"""
        current_state = self.state  # 触发状态检查
        
        if current_state == CircuitState.CLOSED:
            return True
        
        if current_state == CircuitState.OPEN:
            return False
        
        # HALF_OPEN 状态：限制请求数量
        if current_state == CircuitState.HALF_OPEN:
            if self._half_open_calls < self.config.half_open_max_calls:
                self._half_open_calls += 1
                return True
            return False
        
        return False
    
    def record_success(self):
        """记录成功"""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                logger.info(f"断路器 [{self.name}] 恢复，从 HALF_OPEN 转换到 CLOSED")
                self._reset()
        elif self._state == CircuitState.CLOSED:
            # 成功时重置失败计数
            self._failure_count = 0
    
    def record_failure(self):
        """记录失败"""
        self._failure_count += 1
        self._last_failure_time = datetime.now()
        
        if self._state == CircuitState.HALF_OPEN:
            # 半开状态下失败，立即熔断
            logger.warning(f"断路器 [{self.name}] 半开状态测试失败，重新熔断")
            self._state = CircuitState.OPEN
            self._half_open_calls = 0
        elif self._state == CircuitState.CLOSED:
            if self._failure_count >= self.config.failure_threshold:
                logger.warning(
                    f"断路器 [{self.name}] 连续失败 {self._failure_count} 次，熔断"
                )
                self._state = CircuitState.OPEN
    
    def _reset(self):
        """重置断路器状态"""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._half_open_calls = 0
    
    def force_open(self):
        """强制熔断（用于手动干预）"""
        logger.warning(f"断路器 [{self.name}] 被强制熔断")
        self._state = CircuitState.OPEN
        self._last_failure_time = datetime.now()
    
    def force_close(self):
        """强制恢复（用于手动干预）"""
        logger.info(f"断路器 [{self.name}] 被强制恢复")
        self._reset()
    
    def get_status(self) -> dict:
        """获取断路器状态信息"""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time.isoformat() if self._last_failure_time else None,
        }


class CircuitBreakerOpenError(Exception):
    """断路器熔断异常"""
    pass


def with_circuit_breaker(
    breaker_name: str,
    config: Optional[CircuitBreakerConfig] = None
):
    """装饰器：为函数添加断路器保护
    
    Usage:
        @with_circuit_breaker("api_name")
        def call_api():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        breaker = CircuitBreaker(breaker_name, config)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            if not breaker.can_execute():
                raise CircuitBreakerOpenError(
                    f"断路器 [{breaker_name}] 已熔断，服务暂时不可用"
                )
            
            try:
                result = func(*args, **kwargs)
                breaker.record_success()
                return result
            except Exception as e:
                breaker.record_failure()
                raise
        
        # 暴露断路器实例，方便外部访问状态
        wrapper.circuit_breaker = breaker
        return wrapper
    
    return decorator


def with_retry_and_circuit_breaker(
    breaker_name: str,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: float = 0.1,
    retriable_exceptions: Tuple[Type[Exception], ...] = (),
    breaker_config: Optional[CircuitBreakerConfig] = None
):
    """组合装饰器：重试 + 断路器
    
    先检查断路器，再执行重试逻辑。
    
    Usage:
        @with_retry_and_circuit_breaker("api_name", max_retries=3)
        def call_api():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        breaker = CircuitBreaker(breaker_name, breaker_config)
        retry_config = RetryConfig(
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=max_delay,
            jitter=jitter,
            retriable_exceptions=retriable_exceptions
        )
        retry_handler = RetryHandler(retry_config)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            if not breaker.can_execute():
                raise CircuitBreakerOpenError(
                    f"断路器 [{breaker_name}] 已熔断，服务暂时不可用"
                )
            
            try:
                result = retry_handler.execute_with_retry(func, *args, **kwargs)
                breaker.record_success()
                return result
            except Exception as e:
                breaker.record_failure()
                raise
        
        wrapper.circuit_breaker = breaker
        wrapper.retry_handler = retry_handler
        return wrapper
    
    return decorator


# ==================== 服务专用配置 ====================

# AI 服务可重试异常（需要在使用时导入）
AI_RETRIABLE_STATUS_CODES = (429, 500, 502, 503, 504)

# OCR 服务可重试错误码
OCR_RETRIABLE_ERROR_CODES = {17, 18}  # 17=每日超限, 18=QPS超限


def create_ai_retry_handler() -> RetryHandler:
    """创建 AI 服务专用的重试处理器
    
    配置：
    - max_retries: 3
    - base_delay: 1.0s
    - max_delay: 30.0s
    - jitter: 10%
    - 可重试状态码: 429, 500, 502, 503, 504
    """
    config = RetryConfig(
        max_retries=3,
        base_delay=1.0,
        max_delay=30.0,
        jitter=0.1,
        retriable_status_codes=AI_RETRIABLE_STATUS_CODES
    )
    return RetryHandler(config)


def create_ocr_retry_handler() -> RetryHandler:
    """创建 OCR 服务专用的重试处理器
    
    配置：
    - max_retries: 2（OCR 配额有限，减少重试次数）
    - base_delay: 2.0s（OCR QPS 限制，增加基础延迟）
    - max_delay: 30.0s
    - jitter: 10%
    """
    config = RetryConfig(
        max_retries=2,
        base_delay=2.0,
        max_delay=30.0,
        jitter=0.1,
    )
    return RetryHandler(config)


def create_feishu_retry_handler() -> RetryHandler:
    """创建飞书服务专用的重试处理器
    
    配置：
    - max_retries: 3
    - base_delay: 1.0s
    - max_delay: 30.0s
    - jitter: 10%
    - 可重试状态码: 429, 500, 502, 503, 504
    """
    config = RetryConfig(
        max_retries=3,
        base_delay=1.0,
        max_delay=30.0,
        jitter=0.1,
        retriable_status_codes=(429, 500, 502, 503, 504)
    )
    return RetryHandler(config)


def is_ai_error_retriable(exception: Exception) -> bool:
    """判断 AI 服务异常是否可重试
    
    可重试：
    - APITimeoutError
    - RateLimitError  
    - APIConnectionError
    - 状态码 429, 5xx
    
    不可重试：
    - APIError (4xx)
    - 其他异常
    """
    # 检查异常类型名称（避免导入依赖）
    exc_name = type(exception).__name__
    
    # 可重试的异常类型
    if exc_name in ('APITimeoutError', 'RateLimitError', 'APIConnectionError',
                    'AITimeoutError', 'AIRateLimitError'):
        return True
    
    # 检查状态码
    status_code = getattr(exception, 'status_code', None)
    if status_code:
        if status_code == 429 or 500 <= status_code < 600:
            return True
        if 400 <= status_code < 500:
            return False  # 4xx 不可重试
    
    return False


def is_ocr_error_retriable(exception: Exception) -> bool:
    """判断 OCR 服务异常是否可重试
    
    可重试：
    - OCRQuotaError (error_code 17, 18)
    
    不可重试：
    - OCRConfigError
    - OCRImageError
    - 其他 OCRAPIError
    """
    exc_name = type(exception).__name__
    
    # 配额错误可重试
    if exc_name == 'OCRQuotaError':
        return True
    
    # 配置和图片错误不可重试
    if exc_name in ('OCRConfigError', 'OCRImageError'):
        return False
    
    # 检查错误码
    error_code = getattr(exception, 'error_code', None)
    if error_code in OCR_RETRIABLE_ERROR_CODES:
        return True
    
    return False


def is_feishu_error_retriable(exception: Exception) -> bool:
    """判断飞书服务异常是否可重试
    
    可重试：
    - FeishuNetworkError
    - 网络超时
    - 5xx 错误
    
    不可重试：
    - FeishuAuthError
    - FeishuConfigError
    - 4xx 错误
    """
    exc_name = type(exception).__name__
    
    # 网络错误可重试
    if exc_name == 'FeishuNetworkError':
        return True
    
    # 认证和配置错误不可重试
    if exc_name in ('FeishuAuthError', 'FeishuConfigError'):
        return False
    
    # 检查状态码
    status_code = getattr(exception, 'status_code', None)
    if status_code:
        if status_code == 429 or 500 <= status_code < 600:
            return True
        if 400 <= status_code < 500:
            return False
    
    # 检查是否是网络相关异常
    exc_str = str(exception).lower()
    if any(keyword in exc_str for keyword in ['timeout', 'connection', 'network']):
        return True
    
    return False
