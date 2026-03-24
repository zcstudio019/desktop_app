"""百度 OCR 服务

提供图片和 PDF 的文字识别功能，使用百度 OCR API。

Requirements:
- 3.1: 图片使用百度 OCR 识别
- 3.5: OCR 失败时返回描述性错误信息
"""
import base64
import logging
from aip import AipOcr
from config import BAIDU_OCR_API_KEY, BAIDU_OCR_SECRET_KEY, BAIDU_OCR_APP_ID
from utils.retry_handler import (
    create_ocr_retry_handler,
    is_ocr_error_retriable,
    CircuitBreaker,
    CircuitBreakerConfig,
)

# 配置日志
logger = logging.getLogger(__name__)

# OCR 服务断路器（全局共享，3 次失败后熔断，60 秒后尝试恢复）
_ocr_circuit_breaker = CircuitBreaker(
    "baidu_ocr",
    CircuitBreakerConfig(failure_threshold=3, timeout=60.0)
)


# ==================== 自定义异常类 ====================
# Requirement 3.5: OCR 失败时返回描述性错误信息

class OCRServiceError(Exception):
    """OCR 服务异常基类
    
    所有 OCR 相关异常的基类，便于统一捕获和处理。
    
    **Validates: Requirement 3.5**
    """
    pass


class OCRConfigError(OCRServiceError):
    """OCR 配置错误
    
    当 API 配置不完整或无效时抛出。
    """
    pass


class OCRImageError(OCRServiceError):
    """OCR 图片错误
    
    当图片格式、大小或质量不符合要求时抛出。
    """
    pass


class OCRAPIError(OCRServiceError):
    """OCR API 调用错误
    
    当百度 OCR API 返回错误时抛出。
    """
    def __init__(self, message: str, error_code: int = None):
        super().__init__(message)
        self.error_code = error_code


class OCRQuotaError(OCRServiceError):
    """OCR 配额错误
    
    当 API 调用次数超限时抛出。
    """
    pass


class OCRService:
    """百度 OCR 服务
    
    提供图片和 PDF 的文字识别功能。
    
    **Validates: Requirements 3.1, 3.5**
    """
    
    def __init__(self):
        """初始化百度 OCR 客户端
        
        使用配置文件中的 APP_ID、API_KEY 和 SECRET_KEY
        """
        self.client = AipOcr(
            BAIDU_OCR_APP_ID or "",
            BAIDU_OCR_API_KEY or "",
            BAIDU_OCR_SECRET_KEY or ""
        )
        logger.debug("OCRService 初始化完成")
    
    def recognize_image(self, image_bytes: bytes) -> str:
        """识别图片中的文字
        
        支持自动重试（指数退避 + 抖动）和断路器保护。
        
        Args:
            image_bytes: 图片字节数据（应小于 4MB）
            
        Returns:
            识别出的文字内容，每行用换行符分隔
            
        Raises:
            OCRConfigError: 配置不完整
            OCRImageError: 图片大小超限
            OCRQuotaError: API 调用次数超限（重试后仍失败）
            OCRAPIError: API 调用失败（重试后仍失败）
            
        **Validates: Requirements 3.1, 3.5**
        
        WHEN an image file is uploaded, THE OCR_Service SHALL convert it 
        to text using Baidu OCR
        """
        logger.info(f"开始 OCR 识别，图片大小: {len(image_bytes)} 字节")
        
        # 检查配置是否完整（配置错误不重试）
        # Requirement 3.5: 返回描述性错误信息
        if not BAIDU_OCR_API_KEY or not BAIDU_OCR_SECRET_KEY:
            logger.error("百度 OCR 配置不完整")
            raise OCRConfigError(
                "百度 OCR 配置不完整，请在配置文件中设置 API_KEY 和 SECRET_KEY"
            )
        
        # 检查图片大小（图片错误不重试）
        if len(image_bytes) > 4 * 1024 * 1024:
            logger.error(f"图片大小超限: {len(image_bytes)} 字节")
            raise OCRImageError(
                "图片大小超过 4MB 限制，请先压缩图片后重试"
            )
        
        # 检查断路器状态
        if not _ocr_circuit_breaker.can_execute():
            logger.warning("OCR 服务断路器已熔断，拒绝请求")
            raise OCRAPIError("OCR 服务暂时不可用（断路器熔断），请稍后重试")
        
        retry_handler = create_ocr_retry_handler()
        
        def _do_recognize():
            """实际执行识别的内部函数"""
            # 使用通用文字识别（高精度）
            result = self.client.basicAccurate(image_bytes)
            
            # 检查 API 错误
            if "error_code" in result:
                error_code = result.get("error_code")
                error_msg = result.get("error_msg", "未知错误")
                
                # 常见错误码的友好提示
                # Requirement 3.5: 返回描述性错误信息
                error_hints = {
                    110: "Access Token 无效，请检查 API 配置",
                    111: "Access Token 过期，请刷新配置",
                    17: "每日调用量超限，请明天再试",
                    18: "QPS 超限，请稍后重试",
                    216100: "请求参数错误",
                    216101: "图片格式不支持，请使用 PNG、JPG 或 JPEG 格式",
                    216102: "图片大小超限，请压缩后重试",
                    216103: "图片分辨率过低，请使用更清晰的图片",
                    216110: "APP_ID 不存在，请检查配置",
                    216111: "APP_ID 不合法，请检查配置",
                }
                
                hint = error_hints.get(error_code, error_msg)
                logger.warning(f"OCR API 错误 ({error_code}): {hint}")
                
                # 根据错误类型抛出不同异常
                if error_code in [17, 18]:
                    raise OCRQuotaError(f"OCR 调用超限: {hint}")
                elif error_code in [110, 111, 216110, 216111]:
                    raise OCRConfigError(f"OCR 配置错误: {hint}")
                elif error_code in [216101, 216102, 216103]:
                    raise OCRImageError(f"图片问题: {hint}")
                else:
                    raise OCRAPIError(f"OCR 识别失败: {hint}", error_code)
            
            # 提取识别结果
            words_result = result.get("words_result", [])
            
            if not words_result:
                logger.warning("OCR 未识别到文字内容")
                return "[未识别到文字内容]"
            
            text_lines = [item["words"] for item in words_result]
            return "\n".join(text_lines)
        
        retry_handler.is_retriable = lambda e: is_ocr_error_retriable(e)
        
        try:
            result_text = retry_handler.execute_with_retry(_do_recognize)
            _ocr_circuit_breaker.record_success()
            text_lines_count = len(result_text.split('\n')) if result_text else 0
            logger.info(f"OCR 识别完成，识别到 {text_lines_count} 行文字")
            return result_text
        except (OCRConfigError, OCRImageError):
            # 配置和图片错误不记录到断路器（不是服务问题）
            raise
        except Exception as e:
            _ocr_circuit_breaker.record_failure()
            logger.error(f"OCR 识别失败（重试后）: {e}")
            if isinstance(e, (OCRQuotaError, OCRAPIError)):
                raise
            raise OCRAPIError(f"OCR 识别失败: {str(e)}")
    
    def recognize_pdf(self, pdf_bytes: bytes) -> str:
        """识别 PDF 中的文字（需要先转图片）
        
        Raises:
            OCRAPIError: PDF 识别失败
        """
        logger.info(f"开始 PDF OCR 识别，文件大小: {len(pdf_bytes)} 字节")
        
        # 百度 OCR 支持 PDF 直接识别
        try:
            pdf_base64 = base64.b64encode(pdf_bytes).decode()
            result = self.client.basicAccurate(pdf_bytes, {"pdf_file": pdf_base64})
            
            if "error_code" in result:
                # 如果直接识别失败，返回提示
                logger.warning("PDF 直接识别失败，建议转换为图片")
                return "[PDF 识别需要转换为图片，请使用图片格式上传]"
            
            words_result = result.get("words_result", [])
            text_lines = [item["words"] for item in words_result]
            result_text = "\n".join(text_lines)
            logger.info(f"PDF OCR 识别完成，识别到 {len(text_lines)} 行文字")
            return result_text
        except Exception as e:
            logger.error(f"PDF OCR 失败: {e}")
            raise OCRAPIError(f"PDF OCR 失败: {str(e)}")
    
    def recognize_table(self, image_bytes: bytes) -> dict:
        """识别表格
        
        Raises:
            OCRAPIError: 表格识别失败
        """
        logger.info(f"开始表格识别，图片大小: {len(image_bytes)} 字节")
        
        try:
            result = self.client.tableRecognition(image_bytes)
            logger.info("表格识别完成")
            return result
        except Exception as e:
            logger.error(f"表格识别失败: {e}")
            raise OCRAPIError(f"表格识别失败: {str(e)}")
