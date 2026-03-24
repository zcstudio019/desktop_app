"""DeepSeek AI 服务

提供 AI 数据提取功能，使用 DeepSeek API 从文本内容中提取结构化数据。

Requirements:
- 4.1: 根据资料类型加载对应提示词
- 4.2: 调用 DeepSeek API，temperature=0.1
- 3.4: 验证 AI 输出不编造关键字段
"""
import re
import logging
from typing import Union
from openai import OpenAI, APIError, APIConnectionError, RateLimitError, APITimeoutError
from config import DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL
from utils.retry_handler import (
    create_ai_retry_handler,
    is_ai_error_retriable,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
)

# 配置日志
logger = logging.getLogger(__name__)

# AI 服务断路器（全局共享，3 次失败后熔断，30 秒后尝试恢复）
_ai_circuit_breaker = CircuitBreaker(
    "deepseek_api",
    CircuitBreakerConfig(failure_threshold=3, timeout=30.0)
)


class AIServiceError(Exception):
    """AI 服务异常基类"""
    pass


class AIAPIError(AIServiceError):
    """API 调用错误"""
    pass


class AITimeoutError(AIServiceError):
    """API 超时错误"""
    pass


class AIRateLimitError(AIServiceError):
    """API 速率限制错误"""
    pass


class AIService:
    """DeepSeek AI 服务
    
    提供 AI 数据提取功能，使用 DeepSeek API 从文本内容中提取结构化数据。
    
    Attributes:
        client: OpenAI 客户端实例，配置为使用 DeepSeek API
        
    Requirements:
        - 4.2: 调用 DeepSeek API，temperature=0.1
    """
    
    def __init__(self):
        """初始化 AI 服务
        
        创建 OpenAI 客户端实例，配置 DeepSeek API 密钥和基础 URL。
        """
        self.client = OpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
            max_retries=0,
        )
        logger.debug("AIService 初始化完成")
    
    def extract(
        self,
        prompt: str,
        content: str,
        model: str = "deepseek-chat",
        timeout: float | None = None,
        max_tokens: int = 8192,
    ) -> str:
        """调用 AI 提取信息
        
        使用指定的提示词和内容调用 DeepSeek API，提取结构化数据。
        支持自动重试（指数退避 + 抖动）和断路器保护。
        
        Args:
            prompt: 系统提示词，定义提取规则和输出格式
                   （由 prompts 模块根据资料类型加载）
            content: 待提取的文本内容（来自 OCR 或文件读取）
            model: 使用的模型名称，默认为 "deepseek-chat"
            
        Returns:
            str: AI 返回的提取结果（通常为 JSON 格式字符串）
            
        Raises:
            AITimeoutError: API 调用超时（重试后仍失败）
            AIRateLimitError: API 速率限制（重试后仍失败）
            AIAPIError: 其他 API 错误或断路器熔断
            
        Requirements:
            - 4.1: 根据资料类型加载对应提示词（prompt 参数由调用方提供）
            - 4.2: 调用 DeepSeek API，temperature=0.1
            
        Example:
            >>> ai = AIService()
            >>> result = ai.extract("提取姓名和年龄", "张三，25岁")
            >>> "张三" in result
            True
        """
        content_len = len(content)
        prompt_len = len(prompt)
        print(f"[DEBUG] 开始 AI 提取，模型: {model}，内容长度: {content_len} 字符，提示词长度: {prompt_len} 字符")
        logger.info(f"开始 AI 提取，模型: {model}，内容长度: {content_len} 字符，提示词长度: {prompt_len} 字符")
        
        # 检查断路器状态
        if not _ai_circuit_breaker.can_execute():
            logger.warning("AI 服务断路器已熔断，拒绝请求")
            raise AIAPIError("AI 服务暂时不可用（断路器熔断），请稍后重试")
        
        # 检查内容长度，超出限制时使用分批处理
        # DeepSeek 最大上下文 128K tokens，预留 prompt（约 5K）和输出（8K）空间
        # 实际测试发现：Excel 数据（含数字、英文）token 比例约 1.9:1，不是 3:1
        # 115K tokens × 1.9 ≈ 218K 字符，保守设置为 200K
        MAX_CONTENT_LENGTH = 200000
        print(f"[DEBUG] 分批处理检查: {content_len} > {MAX_CONTENT_LENGTH} = {content_len > MAX_CONTENT_LENGTH}")
        logger.info(f"分批处理检查: {content_len} > {MAX_CONTENT_LENGTH} = {content_len > MAX_CONTENT_LENGTH}")
        if content_len > MAX_CONTENT_LENGTH:
            logger.warning(f"输入内容过长 ({len(content)} 字符)，启用分批处理")
            return self._extract_large_content(prompt, content, model, MAX_CONTENT_LENGTH)
        
        # 使用重试处理器
        retry_handler = create_ai_retry_handler()
        
        def _do_extract():
            """实际执行提取的内部函数"""
            try:
                # Requirement 4.2: 调用 DeepSeek API，temperature=0.1
                # 踩坑点 #8: AI 输出 JSON 可能被截断，设置足够大的 max_tokens
                response = self.client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": content}
                    ],
                    temperature=0.1,  # 使用低温度确保提取结果一致性
                    max_tokens=max_tokens,   # 防止输出被截断
                    timeout=timeout,
                )
                
                result = response.choices[0].message.content
                return result
                
            except APITimeoutError as e:
                logger.warning(f"AI API 超时（将重试）: {e}")
                raise AITimeoutError(f"AI 调用超时: {str(e)}")
            except RateLimitError as e:
                logger.warning(f"AI API 速率限制（将重试）: {e}")
                raise AIRateLimitError(f"AI 调用频率过高: {str(e)}")
            except APIConnectionError as e:
                logger.warning(f"AI API 连接错误（将重试）: {e}")
                raise AIAPIError(f"无法连接到 AI 服务: {str(e)}")
            except APIError as e:
                # 4xx 错误不重试
                status_code = getattr(e, 'status_code', None) or 0
                if 400 <= status_code < 500:
                    logger.error(f"AI API 客户端错误（不重试）: {e}")
                    raise AIAPIError(f"AI 服务返回错误: {str(e)}")
                logger.warning(f"AI API 错误（将重试）: {e}")
                raise AIAPIError(f"AI 服务返回错误: {str(e)}")
        
        # 配置重试处理器的错误判断
        retry_handler.is_retriable = lambda e: is_ai_error_retriable(e)
        
        try:
            result = retry_handler.execute_with_retry(_do_extract)
            _ai_circuit_breaker.record_success()
            logger.info(f"AI 提取完成，结果长度: {len(result) if result else 0} 字符")
            return result
        except Exception as e:
            _ai_circuit_breaker.record_failure()
            logger.error(f"AI 提取失败（重试后）: {e}")
            # 重新包装异常
            if isinstance(e, (AITimeoutError, AIRateLimitError, AIAPIError)):
                raise
            raise AIAPIError(f"AI 调用失败: {str(e)}")
    
    def _extract_large_content(
        self, 
        prompt: str, 
        content: str, 
        model: str,
        max_chunk_size: int,
        max_workers: int = 3
    ) -> str:
        """分批处理大文件内容，并行调用 AI，合并提取结果
        
        将大文件按 Sheet 或行数拆分成多个批次，并行调用 AI 提取，
        最后合并所有批次的结果为一个完整的 JSON。
        
        Args:
            prompt: 系统提示词
            content: 待提取的大文本内容
            model: 使用的模型名称
            max_chunk_size: 每批次最大字符数
            max_workers: 最大并行数，默认 3（DeepSeek API 无并发限制）
            
        Returns:
            str: 合并后的 JSON 字符串
        """
        import json
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        logger.info(f"开始分批处理，总内容长度: {len(content)} 字符，每批最大: {max_chunk_size} 字符")
        print(f"[DEBUG] 启用分批处理，内容长度: {len(content)} 字符")
        
        # 拆分内容为多个批次
        chunks = self._split_content_into_chunks(content, max_chunk_size)
        logger.info(f"拆分为 {len(chunks)} 个批次，并行数: {max_workers}")
        print(f"[DEBUG] 拆分为 {len(chunks)} 个批次，并行数: {max_workers}")
        
        # 并行提取
        all_results = [None] * len(chunks)  # 预分配保持顺序
        
        def process_chunk(index: int, chunk: str) -> tuple[int, dict | None]:
            """处理单个批次，返回 (索引, 结果)"""
            logger.info(f"处理批次 {index+1}/{len(chunks)}，长度: {len(chunk)} 字符")
            try:
                result = self._extract_single_chunk(prompt, chunk, model)
                return (index, result)
            except Exception as e:
                logger.error(f"批次 {index+1} 处理失败: {e}")
                return (index, None)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            futures = {
                executor.submit(process_chunk, i, chunk): i 
                for i, chunk in enumerate(chunks)
            }
            
            # 收集结果（按完成顺序，但结果按原始顺序存储）
            for future in as_completed(futures):
                index, result = future.result()
                if result:
                    all_results[index] = result
                    print(f"[DEBUG] 批次 {index+1} 完成")
        
        # 过滤掉 None 结果
        valid_results = [r for r in all_results if r is not None]
        
        if not valid_results:
            raise AIAPIError("所有批次处理均失败，无法提取数据")
        
        # 合并结果
        merged_result = self._merge_extraction_results(valid_results)
        logger.info(f"分批处理完成，合并后结果长度: {len(merged_result)} 字符")
        print(f"[DEBUG] 分批处理完成，成功 {len(valid_results)}/{len(chunks)} 批次")
        
        return merged_result
    
    def _split_content_into_chunks(self, content: str, max_chunk_size: int) -> list[str]:
        """将内容拆分为多个批次
        
        优先按 Sheet 拆分（多 Sheet Excel），否则按行数拆分。
        """
        sheet_marker = "=== Sheet:"
        
        # 检测是否为多 Sheet Excel
        if sheet_marker in content:
            return self._split_by_sheets(content, max_chunk_size)
        
        # 普通文本按行拆分
        return self._split_by_lines(content, max_chunk_size)
    
    def _split_by_sheets(self, content: str, max_chunk_size: int) -> list[str]:
        """按 Sheet 拆分多 Sheet Excel 内容"""
        sheet_marker = "=== Sheet:"
        parts = content.split(sheet_marker)
        
        chunks = []
        current_chunk = ""
        
        # 第一部分可能是前缀内容
        prefix = parts[0].strip()
        if prefix:
            current_chunk = prefix + "\n"
        
        for i, part in enumerate(parts[1:], 1):
            sheet_content = sheet_marker + part
            
            # 如果单个 Sheet 就超限，需要进一步拆分
            if len(sheet_content) > max_chunk_size:
                # 先保存当前累积的内容
                if current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = ""
                # 对大 Sheet 按行拆分
                sheet_chunks = self._split_by_lines(sheet_content, max_chunk_size)
                chunks.extend(sheet_chunks)
            elif len(current_chunk) + len(sheet_content) > max_chunk_size:
                # 当前批次已满，开始新批次
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = sheet_content
            else:
                # 添加到当前批次
                current_chunk += "\n" + sheet_content if current_chunk else sheet_content
        
        # 添加最后一个批次
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks if chunks else [content[:max_chunk_size]]
    
    def _split_by_lines(self, content: str, max_chunk_size: int) -> list[str]:
        """按行数拆分内容，保留表头"""
        lines = content.split('\n')
        
        if len(lines) <= 10:
            return [content[:max_chunk_size]]
        
        # 假设前 5 行是表头
        header_lines = lines[:5]
        header = '\n'.join(header_lines)
        data_lines = lines[5:]
        
        chunks = []
        current_lines = []
        current_size = len(header)
        
        for line in data_lines:
            line_size = len(line) + 1  # +1 for newline
            
            if current_size + line_size > max_chunk_size - 100:  # 预留空间
                # 当前批次已满
                if current_lines:
                    chunk = header + '\n' + '\n'.join(current_lines)
                    chunks.append(chunk)
                current_lines = [line]
                current_size = len(header) + line_size
            else:
                current_lines.append(line)
                current_size += line_size
        
        # 添加最后一个批次
        if current_lines:
            chunk = header + '\n' + '\n'.join(current_lines)
            chunks.append(chunk)
        
        return chunks if chunks else [content[:max_chunk_size]]
    
    def _extract_single_chunk(self, prompt: str, chunk: str, model: str) -> dict | None:
        """提取单个批次的内容，返回解析后的字典"""
        import json
        
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": chunk}
                ],
                temperature=0.1,
                max_tokens=8192
            )
            
            result_text = response.choices[0].message.content or ""
            
            # 尝试解析 JSON
            # 清理可能的 markdown 代码块标记
            clean_text = result_text.strip()
            if clean_text.startswith("```json"):
                clean_text = clean_text[7:]
            if clean_text.startswith("```"):
                clean_text = clean_text[3:]
            if clean_text.endswith("```"):
                clean_text = clean_text[:-3]
            clean_text = clean_text.strip()
            
            try:
                return json.loads(clean_text)
            except json.JSONDecodeError:
                # 返回原始文本，后续合并时处理
                logger.warning("批次结果不是有效 JSON，保留原始文本")
                return {"_raw_text": result_text}
                
        except Exception as e:
            logger.error(f"单批次提取失败: {e}")
            return None
    
    def _merge_extraction_results(self, results: list[dict]) -> str:
        """合并多个批次的提取结果
        
        策略：
        - 对于相同的顶级 key，合并其内容
        - 对于数组类型，合并数组元素
        - 对于字典类型，后面的值覆盖前面的（除非是"无"）
        - 信用评估结论取最后一个批次的（基于完整数据）
        """
        import json
        
        if len(results) == 1:
            return json.dumps(results[0], ensure_ascii=False, indent=2)
        
        merged = {}
        
        for result in results:
            if not isinstance(result, dict):
                continue
            
            # 跳过原始文本标记
            if "_raw_text" in result:
                continue
            
            for key, value in result.items():
                if key not in merged:
                    merged[key] = value
                elif isinstance(value, dict) and isinstance(merged[key], dict):
                    # 合并字典，非"无"值覆盖
                    for k, v in value.items():
                        if v and v != "无" and str(v).strip():
                            merged[key][k] = v
                        elif k not in merged[key]:
                            merged[key][k] = v
                elif isinstance(value, list) and isinstance(merged[key], list):
                    # 合并数组
                    merged[key].extend(value)
                elif value and value != "无" and str(value).strip():
                    # 非空值覆盖
                    merged[key] = value
        
        return json.dumps(merged, ensure_ascii=False, indent=2)
    
    def chat_with_reasoning(self, system_prompt: str, user_message: str) -> tuple[str, str]:
        """调用 AI 并返回思考过程和最终答案
        
        使用 DeepSeek deepseek-reasoner 模型，获取 AI 的推理过程（Chain of Thought）。
        支持自动重试（指数退避 + 抖动）和断路器保护。
        
        注意：只有 deepseek-reasoner 模型支持 reasoning_content 输出，
        deepseek-chat 模型不支持此功能。
        
        Args:
            system_prompt: 系统提示词
            user_message: 用户消息
            
        Returns:
            tuple[str, str]: (最终答案, 思考过程)
            
        Raises:
            AITimeoutError: API 调用超时（重试后仍失败）
            AIRateLimitError: API 速率限制（重试后仍失败）
            AIAPIError: 其他 API 错误或断路器熔断
        """
        logger.info(f"开始带思考过程的 AI 调用（deepseek-reasoner），消息长度: {len(user_message)} 字符")
        
        # 检查断路器状态
        if not _ai_circuit_breaker.can_execute():
            logger.warning("AI 服务断路器已熔断，拒绝请求")
            raise AIAPIError("AI 服务暂时不可用（断路器熔断），请稍后重试")
        
        retry_handler = create_ai_retry_handler()
        
        def _do_chat():
            """实际执行调用的内部函数"""
            try:
                # 使用 deepseek-reasoner 模型获取 Chain of Thought
                # 参考：https://api-docs.deepseek.com/guides/reasoning_model
                response = self.client.chat.completions.create(
                    model="deepseek-reasoner",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message}
                    ],
                )
                
                content = response.choices[0].message.content or ""
                # deepseek-reasoner 返回 reasoning_content 字段
                reasoning = getattr(response.choices[0].message, 'reasoning_content', None) or ""
                
                return content, reasoning
                
            except APITimeoutError as e:
                logger.warning(f"AI API 超时（将重试）: {e}")
                raise AITimeoutError(f"AI 调用超时: {str(e)}")
            except RateLimitError as e:
                logger.warning(f"AI API 速率限制（将重试）: {e}")
                raise AIRateLimitError(f"AI 调用频率过高: {str(e)}")
            except APIConnectionError as e:
                logger.warning(f"AI API 连接错误（将重试）: {e}")
                raise AIAPIError(f"无法连接到 AI 服务: {str(e)}")
            except APIError as e:
                status_code = getattr(e, 'status_code', None) or 0
                if 400 <= status_code < 500:
                    logger.error(f"AI API 客户端错误（不重试）: {e}")
                    raise AIAPIError(f"AI 服务返回错误: {str(e)}")
                logger.warning(f"AI API 错误（将重试）: {e}")
                raise AIAPIError(f"AI 服务返回错误: {str(e)}")
        
        retry_handler.is_retriable = lambda e: is_ai_error_retriable(e)
        
        try:
            content, reasoning = retry_handler.execute_with_retry(_do_chat)
            _ai_circuit_breaker.record_success()
            logger.info(f"AI 调用完成，答案长度: {len(content)} 字符，思考过程长度: {len(reasoning)} 字符")
            return content, reasoning
        except Exception as e:
            _ai_circuit_breaker.record_failure()
            logger.error(f"AI 调用失败（重试后）: {e}")
            if isinstance(e, (AITimeoutError, AIRateLimitError, AIAPIError)):
                raise
            raise AIAPIError(f"AI 调用失败: {str(e)}")
    
    def classify(self, content: str) -> str:
        """判断资料类型
        
        使用 AI 分析文本内容，判断其属于哪种资料类型。
        使用详细的判断规则提示词，提高分类准确性。
        
        Args:
            content: 待分类的文本内容
            
        Returns:
            str: 资料类型名称，如 "个人征信"、"企业征信" 等
            
        Note:
            - 为避免 token 消耗过大，限制分类内容长度
            - 对于多 Sheet Excel，采用智能采样确保所有 Sheet 内容都能被看到
        """
        from prompts import DOCUMENT_TYPE_DETECTION_PROMPT
        
        logger.debug(f"开始分类，内容长度: {len(content)} 字符")
        
        # 智能采样：处理多 Sheet Excel
        sampled_content = self._smart_sample_for_classification(content)
        logger.debug(f"采样后内容长度: {len(sampled_content)} 字符")
        
        result = self.extract(DOCUMENT_TYPE_DETECTION_PROMPT, sampled_content)
        classified_type = (result.strip() if result else "未知") or "未知"  # 踩坑点 #16
        logger.info(f"分类结果: {classified_type}")
        return classified_type
    
    def _smart_sample_for_classification(self, content: str, max_chars: int = 6000) -> str:
        """智能采样用于分类
        
        对于多 Sheet Excel，确保每个 Sheet 的内容都能被采样到。
        对于普通文本，直接截取前 max_chars 字符。
        
        Args:
            content: 原始内容
            max_chars: 最大字符数，默认 6000
            
        Returns:
            采样后的内容
        """
        # 检测是否为多 Sheet Excel（通过 "=== Sheet:" 标记）
        sheet_marker = "=== Sheet:"
        if sheet_marker not in content:
            # 普通文本，直接截取
            return content[:max_chars]
        
        # 多 Sheet Excel：从每个 Sheet 采样
        sheets = content.split(sheet_marker)
        
        if len(sheets) <= 1:
            return content[:max_chars]
        
        # 第一部分可能是空的（如果内容以 sheet_marker 开头）
        # 或者是 sheet_marker 之前的内容
        prefix = sheets[0].strip()
        sheet_contents = sheets[1:]  # 每个元素格式: "SheetName ===\n内容..."
        
        # 计算每个 Sheet 的采样字符数
        num_sheets = len(sheet_contents)
        chars_per_sheet = (max_chars - len(prefix)) // num_sheets
        # 确保每个 Sheet 至少有 500 字符
        chars_per_sheet = max(chars_per_sheet, 500)
        
        sampled_parts = []
        if prefix:
            sampled_parts.append(prefix)
        
        for sheet_part in sheet_contents:
            # 恢复 sheet_marker 并采样
            full_sheet = sheet_marker + sheet_part
            sampled_parts.append(full_sheet[:chars_per_sheet])
        
        result = "\n".join(sampled_parts)
        
        # 最终限制总长度
        return result[:max_chars]
    
    def _smart_truncate_content(self, content: str, max_chars: int = 100000) -> str:
        """智能截断内容，保留关键信息
        
        对于流水类数据（表格结构），保留表头 + 采样数据行。
        对于多 Sheet Excel，每个 Sheet 均匀采样。
        对于普通文本，保留首尾部分。
        
        Args:
            content: 原始内容
            max_chars: 最大字符数，默认 100000
            
        Returns:
            采样后的内容
        """
        if len(content) <= max_chars:
            return content
        
        # 检测是否为多 Sheet Excel
        sheet_marker = "=== Sheet:"
        if sheet_marker in content:
            return self._smart_truncate_multi_sheet(content, max_chars)
        
        # 检测是否为表格数据（包含多行，且有表头特征）
        lines = content.split('\n')
        if len(lines) > 50:  # 超过 50 行，可能是表格数据
            return self._smart_truncate_table(content, max_chars)
        
        # 普通文本：保留首尾
        half = max_chars // 2
        return content[:half] + "\n\n... [内容过长，中间部分已省略] ...\n\n" + content[-half:]
    
    def _smart_truncate_multi_sheet(self, content: str, max_chars: int) -> str:
        """智能截断多 Sheet Excel 内容
        
        每个 Sheet 均匀分配字符数，保留表头和采样数据。
        """
        sheet_marker = "=== Sheet:"
        sheets = content.split(sheet_marker)
        
        if len(sheets) <= 1:
            return content[:max_chars]
        
        prefix = sheets[0].strip()
        sheet_contents = sheets[1:]
        
        # 计算每个 Sheet 的配额
        num_sheets = len(sheet_contents)
        chars_per_sheet = (max_chars - len(prefix) - 200) // num_sheets  # 预留拼接空间
        chars_per_sheet = max(chars_per_sheet, 2000)  # 每个 Sheet 至少 2000 字符
        
        sampled_parts = []
        if prefix:
            sampled_parts.append(prefix)
        
        for sheet_part in sheet_contents:
            full_sheet = sheet_marker + sheet_part
            # 对每个 Sheet 内容进行智能采样
            sampled_sheet = self._smart_truncate_table(full_sheet, chars_per_sheet)
            sampled_parts.append(sampled_sheet)
        
        return "\n".join(sampled_parts)
    
    def _smart_truncate_table(self, content: str, max_chars: int) -> str:
        """智能截断表格数据，保留表头和采样行
        
        策略：保留前 N 行（含表头）+ 中间采样 + 最后 M 行
        确保能看到数据的整体结构和分布。
        """
        lines = content.split('\n')
        
        if len(lines) <= 20:
            return content[:max_chars]
        
        # 估算每行平均字符数
        total_chars = len(content)
        avg_chars_per_line = total_chars / len(lines)
        
        # 计算可以保留的行数
        max_lines = int(max_chars / avg_chars_per_line) if avg_chars_per_line > 0 else 500
        max_lines = max(max_lines, 30)  # 至少保留 30 行
        
        if len(lines) <= max_lines:
            return content[:max_chars]
        
        # 分配行数：表头区(20%) + 采样区(60%) + 尾部区(20%)
        header_lines = max(int(max_lines * 0.2), 10)  # 前 10-20 行（含表头）
        tail_lines = max(int(max_lines * 0.2), 5)     # 最后 5-10 行
        sample_lines = max_lines - header_lines - tail_lines
        
        # 从中间区域均匀采样
        middle_start = header_lines
        middle_end = len(lines) - tail_lines
        middle_range = middle_end - middle_start
        
        if middle_range <= sample_lines:
            # 中间区域不够大，直接取全部
            sampled = lines[:max_lines]
        else:
            # 均匀采样中间区域
            step = middle_range / sample_lines if sample_lines > 0 else 1
            sampled_middle_indices = [int(middle_start + i * step) for i in range(sample_lines)]
            
            sampled = (
                lines[:header_lines] +  # 表头区
                [f"... [已采样 {sample_lines} 行，共 {middle_range} 行] ..."] +
                [lines[i] for i in sampled_middle_indices if i < len(lines)] +  # 采样区
                [f"... [尾部 {tail_lines} 行] ..."] +
                lines[-tail_lines:]  # 尾部区
            )
        
        result = '\n'.join(sampled)
        
        # 最终长度检查
        if len(result) > max_chars:
            return result[:max_chars]
        
        return result
    
    def match_scheme(
        self, 
        customer_data: dict, 
        products: str,
        credit_type: str = "enterprise"
    ) -> str:
        """方案匹配
        
        根据客户资料和产品库，使用 AI 推荐最合适的贷款方案。
        从提示词文件加载对应的匹配提示词，替换模板变量后调用 AI。
        
        Args:
            customer_data: 客户资料字典，包含征信、流水、资产等信息
            products: 产品库内容（字符串格式）
            credit_type: 信用类型，可选值：
                - "personal": 个人贷
                - "enterprise_credit": 企业信用贷
                - "enterprise_mortgage": 企业抵押贷
                - "enterprise": 企业贷（同时使用信用贷和抵押贷提示词）
            
        Returns:
            str: AI 生成的方案匹配结果（Markdown 格式），包含推荐方案和风险提示
            
        Raises:
            AITimeoutError: API 调用超时（60s）
            AIRateLimitError: API 速率限制
            AIAPIError: 其他 API 错误
            
        Requirements:
            - 2.5: 根据客户数据和产品库匹配合适的贷款方案
            
        Example:
            >>> ai = AIService()
            >>> result = ai.match_scheme(
            ...     customer_data={"企业名称": "测试公司", "年纳税": "50万"},
            ...     products="产品A: 税贷，年纳税≥10万",
            ...     credit_type="enterprise_credit"
            ... )
            >>> "方案" in result
            True
        """
        import json
        from prompts import load_prompts, get_cached_prompts
        
        logger.info(f"开始方案匹配，信用类型: {credit_type}")
        
        # 确保提示词已加载
        prompts = get_cached_prompts()
        if not prompts:
            prompts = load_prompts()
        
        # 根据信用类型选择提示词文件
        prompt_file_map = {
            "personal": "个人贷_匹配提示词.md",
            "enterprise_credit": "企业信用贷_匹配提示词.md",
            "enterprise_mortgage": "企业抵押贷_匹配提示词.md",
        }
        
        # 获取提示词内容
        if credit_type == "enterprise":
            # 企业贷：合并信用贷和抵押贷提示词
            credit_prompt = prompts.get("企业信用贷_匹配提示词.md", "")
            mortgage_prompt = prompts.get("企业抵押贷_匹配提示词.md", "")
            if not credit_prompt and not mortgage_prompt:
                logger.warning("未找到企业贷匹配提示词文件")
                prompt_template = self._get_default_match_prompt()
            else:
                # 使用信用贷提示词作为主模板（因为结构更完整）
                prompt_template = credit_prompt if credit_prompt else mortgage_prompt
        else:
            prompt_file = prompt_file_map.get(credit_type)
            if not prompt_file:
                logger.warning(f"未知的信用类型: {credit_type}，使用默认提示词")
                prompt_template = self._get_default_match_prompt()
            else:
                prompt_template = prompts.get(prompt_file, "")
                if not prompt_template:
                    logger.warning(f"未找到提示词文件: {prompt_file}")
                    prompt_template = self._get_default_match_prompt()
        
        # 将客户数据转换为字符串格式
        if isinstance(customer_data, dict):
            customer_info = json.dumps(customer_data, ensure_ascii=False, indent=2)
        else:
            customer_info = str(customer_data)
        
        # 构建消息结构：指令放 system，数据放 user
        # 避免超长 system prompt 导致 AI 忽略产品库内容而编造产品
        system_prompt = prompt_template.replace(
            "{{zhishiku}}", "[产品知识库见用户消息]"
        ).replace(
            "{{input}}", "[客户资料见用户消息]"
        )
        
        user_message = (
            "## 产品知识库（你只能从以下产品中推荐，严禁编造）\n\n"
            f"{products}\n\n"
            "---\n\n"
            "## 客户资料\n\n"
            f"{customer_info}\n\n"
            "---\n\n"
            "请严格根据【以上产品知识库】中的具体银行和产品名称进行方案匹配。\n"
            "⚠️ 你推荐的每一个产品必须能在上方产品知识库中找到原文出处，"
            "禁止推荐知识库中不存在的产品。"
        )
        
        logger.debug(
            f"方案匹配 system 长度: {len(system_prompt)}, "
            f"user 长度: {len(user_message)} 字符"
        )
        
        # 调用 AI API，超时交由上游和外层重试机制控制（不设置硬超时）
        retry_handler = create_ai_retry_handler()
        
        # 检查断路器状态
        if not _ai_circuit_breaker.can_execute():
            logger.warning("AI 服务断路器已熔断，拒绝请求")
            raise AIAPIError("AI 服务暂时不可用（断路器熔断），请稍后重试")
        
        def _do_match():
            """实际执行匹配的内部函数"""
            try:
                response = self.client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message}
                    ],
                    temperature=0.0
                )
                
                result = response.choices[0].message.content
                return result if result else ""
                
            except APITimeoutError as e:
                logger.warning(f"方案匹配超时（将重试）: {e}")
                raise AITimeoutError(f"方案匹配超时: {str(e)}")
            except RateLimitError as e:
                logger.warning(f"方案匹配速率限制（将重试）: {e}")
                raise AIRateLimitError(f"AI 调用频率过高: {str(e)}")
            except APIConnectionError as e:
                logger.warning(f"方案匹配连接错误（将重试）: {e}")
                raise AIAPIError(f"无法连接到 AI 服务: {str(e)}")
            except APIError as e:
                status_code = getattr(e, 'status_code', None) or 0
                if 400 <= status_code < 500:
                    logger.error(f"方案匹配客户端错误（不重试）: {e}")
                    raise AIAPIError(f"AI 服务返回错误: {str(e)}")
                logger.warning(f"方案匹配 API 错误（将重试）: {e}")
                raise AIAPIError(f"AI 服务返回错误: {str(e)}")
        
        retry_handler.is_retriable = lambda e: is_ai_error_retriable(e)
        
        try:
            result = retry_handler.execute_with_retry(_do_match)
            _ai_circuit_breaker.record_success()
            logger.info(f"方案匹配完成，结果长度: {len(result)} 字符")
            return result
        except Exception as e:
            _ai_circuit_breaker.record_failure()
            logger.error(f"方案匹配失败（重试后）: {e}")
            if isinstance(e, (AITimeoutError, AIRateLimitError, AIAPIError)):
                raise
            raise AIAPIError(f"方案匹配失败: {str(e)}")
    
    def _get_default_match_prompt(self) -> str:
        """获取默认的方案匹配提示词
        
        当无法加载提示词文件时使用的备用提示词。
        
        Returns:
            str: 默认提示词内容
        """
        return """# 贷款方案匹配

## 角色
你是专业的银行贷款顾问，负责根据客户资料匹配合适的贷款产品。

## 输入
- 产品知识库：{{zhishiku}}
- 客户资料：{{input}}

## 核心原则
1. 只推荐知识库中存在的产品
2. 逐条核对产品的准入条件
3. 不符合条件的产品要说明原因
4. 每个推荐产品必须标注"来源：产品库-xxx"

## 输出格式

### 一、客户资料摘要
| 项目 | 内容 |
|------|------|
| ... | ... |

### 二、推荐方案
#### 方案1：【银行名称】产品名称
- 可贷额度：xxx万
- 参考利率：x.xx%
- 准入条件核对：
  - ✅ 条件1
  - ✅ 条件2
- 来源：产品库-xxx

### 三、不推荐的产品及原因
| 产品 | 不符合原因 |
|------|-----------|
| ... | ... |

### 四、需补充信息
1. xxx
"""


def validate_no_fabrication(
    ai_output: str, 
    customer_data: dict
) -> dict:
    """验证 AI 输出是否编造了关键字段
    
    检查 AI 生成的申请表中，关键字段（期望额度、期望期限、利率、贷款金额）
    是否被编造。这些字段应该填写"待补充"或来自客户原始数据。
    
    Args:
        ai_output: AI 生成的申请表内容（Markdown 格式）
        customer_data: 客户原始数据字典
        
    Returns:
        dict: {
            "is_valid": bool,  # True 表示没有检测到编造
            "warnings": list[str],  # 警告信息列表
            "fabricated_fields": list[str]  # 被编造的字段名列表
        }
        
    Requirements:
        - 3.4: 验证 AI 输出不编造关键字段
        
    Example:
        >>> result = validate_no_fabrication(
        ...     "期望额度：500万元",
        ...     {"企业名称": "测试公司"}
        ... )
        >>> result["is_valid"]
        False
        >>> "期望额度" in result["fabricated_fields"]
        True
    """
    # 关键字段的正则模式
    # 格式：字段名：值 或 字段名: 值，值可能在 | 分隔的表格中
    critical_field_patterns = [
        # 期望额度相关
        (r"期望额度[：:]\s*(.+?)(?:\s*\||$|\n)", "期望额度"),
        (r"申请额度[：:]\s*(.+?)(?:\s*\||$|\n)", "申请额度"),
        (r"贷款额度[：:]\s*(.+?)(?:\s*\||$|\n)", "贷款额度"),
        # 期望期限相关
        (r"期望期限[：:]\s*(.+?)(?:\s*\||$|\n)", "期望期限"),
        (r"贷款期限[：:]\s*(.+?)(?:\s*\||$|\n)", "贷款期限"),
        (r"申请期限[：:]\s*(.+?)(?:\s*\||$|\n)", "申请期限"),
        # 利率相关
        (r"(?<!参考)利率[：:]\s*(.+?)(?:\s*\||$|\n)", "利率"),
        (r"年化利率[：:]\s*(.+?)(?:\s*\||$|\n)", "年化利率"),
        (r"期望年化利率[：:]\s*(.+?)(?:\s*\||$|\n)", "期望年化利率"),
        (r"期望利率[：:]\s*(.+?)(?:\s*\||$|\n)", "期望利率"),
        # 贷款金额相关
        (r"贷款金额[：:]\s*(.+?)(?:\s*\||$|\n)", "贷款金额"),
        (r"借款金额[：:]\s*(.+?)(?:\s*\||$|\n)", "借款金额"),
    ]
    
    # 表示编造的模式（具体数值）
    fabrication_patterns = [
        r'\d+\.?\d*万元?',  # 500万, 500万元, 100.5万
        r'\d+\.?\d*元',     # 5000000元
        r'\d+\.?\d*%',      # 4.5%, 5%
        r'\d+个月',         # 36个月
        r'\d+年',           # 5年
        r'\d+期',           # 12期
    ]
    
    # 有效值（不算编造）
    valid_values = [
        "待补充", "待确认", "待定", "未知", "无", 
        "", "-", "—", "N/A", "n/a", "/", "暂无"
    ]
    
    # 从客户数据中提取所有值（递归）
    customer_values = set()
    
    def extract_values(data, prefix=""):
        """递归提取客户数据中的所有值"""
        if isinstance(data, dict):
            for k, v in data.items():
                extract_values(v, f"{prefix}.{k}" if prefix else k)
        elif isinstance(data, list):
            for item in data:
                extract_values(item, prefix)
        else:
            if data is not None:
                # 添加原始值和字符串形式
                customer_values.add(str(data))
                # 如果是数字，也添加常见格式
                if isinstance(data, (int, float)):
                    customer_values.add(f"{data}万")
                    customer_values.add(f"{data}万元")
                    customer_values.add(f"{data}%")
                    customer_values.add(f"{data}个月")
                    customer_values.add(f"{data}年")
    
    extract_values(customer_data)
    
    warnings = []
    fabricated_fields = []
    
    for pattern, field_name in critical_field_patterns:
        matches = re.findall(pattern, ai_output, re.IGNORECASE | re.MULTILINE)
        for match in matches:
            value = match.strip()
            
            # 跳过有效值（待补充等）
            if value.lower() in [v.lower() for v in valid_values]:
                continue
            
            # 跳过来自客户数据的值
            if value in customer_values:
                continue
            
            # 检查是否包含编造模式
            for fab_pattern in fabrication_patterns:
                if re.search(fab_pattern, value):
                    warning = f"检测到可能编造的{field_name}: {value}"
                    warnings.append(warning)
                    fabricated_fields.append(field_name)
                    logger.warning(warning)
                    break
    
    # 去重
    fabricated_fields = list(set(fabricated_fields))
    
    result = {
        "is_valid": len(fabricated_fields) == 0,
        "warnings": warnings,
        "fabricated_fields": fabricated_fields
    }
    
    if not result["is_valid"]:
        logger.warning(f"AI 输出验证失败，编造了以下字段: {fabricated_fields}")
    else:
        logger.debug("AI 输出验证通过，未检测到编造的关键字段")
    
    return result


