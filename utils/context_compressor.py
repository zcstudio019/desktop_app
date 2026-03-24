"""上下文压缩器

防止上下文窗口溢出，保持长对话连贯性，降低 token 成本。

4 种压缩策略：
1. Write - 持久化到外部存储
2. Select - 只检索相关信息
3. Compress - 摘要或裁剪
4. Isolate - 使用子代理分离

Usage:
    from utils.context_compressor import ContextCompressor, CompressionStrategy
    
    # 创建压缩器
    compressor = ContextCompressor(max_tokens=4000)
    
    # 压缩对话历史
    compressed = compressor.compress_conversation(messages)
    
    # 压缩工具结果
    compressed_result = compressor.compress_tool_result(result, max_lines=50)
"""
import json
import logging
from typing import Optional, Dict, Any, List, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import re

logger = logging.getLogger(__name__)


class CompressionStrategy(Enum):
    """压缩策略"""
    WRITE = "write"       # 持久化到外部
    SELECT = "select"     # 只检索相关
    COMPRESS = "compress" # 摘要或裁剪
    ISOLATE = "isolate"   # 子代理分离


@dataclass
class Message:
    """消息结构"""
    role: str  # user/assistant/system
    content: str
    timestamp: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {"role": self.role, "content": self.content}
        if self.timestamp:
            result["timestamp"] = self.timestamp
        if self.metadata:
            result["metadata"] = self.metadata
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """从字典创建"""
        return cls(
            role=data.get("role") or "user",
            content=data.get("content") or "",
            timestamp=data.get("timestamp"),
            metadata=data.get("metadata") or {},
        )


@dataclass
class CompressionResult:
    """压缩结果"""
    original_tokens: int
    compressed_tokens: int
    compression_ratio: float
    strategy_used: str
    summary: Optional[str] = None
    
    @property
    def tokens_saved(self) -> int:
        """节省的 token 数"""
        return self.original_tokens - self.compressed_tokens


class ContextCompressor:
    """上下文压缩器
    
    根据配置自动选择压缩策略，保持关键信息。
    
    压缩规则：
    - 最近 3 轮对话：完整保留
    - 3-10 轮对话：保留关键决策和代码变更
    - 10 轮以上：压缩为一句话摘要
    - 工具调用结果：只保留关键输出
    """
    
    # 默认配置
    DEFAULT_MAX_TOKENS = 4000
    RECENT_TURNS_FULL = 3      # 完整保留的最近轮数
    RECENT_TURNS_PARTIAL = 10  # 部分保留的轮数
    
    def __init__(
        self,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        recent_turns_full: int = RECENT_TURNS_FULL,
        recent_turns_partial: int = RECENT_TURNS_PARTIAL,
    ):
        """初始化压缩器
        
        Args:
            max_tokens: 最大 token 数
            recent_turns_full: 完整保留的最近轮数
            recent_turns_partial: 部分保留的轮数
        """
        self.max_tokens = max_tokens
        self.recent_turns_full = recent_turns_full
        self.recent_turns_partial = recent_turns_partial
    
    def estimate_tokens(self, text: str) -> int:
        """估算 token 数（简单估算：字符数 / 4）
        
        Args:
            text: 文本内容
            
        Returns:
            估算的 token 数
        """
        # 简单估算：英文约 4 字符/token，中文约 2 字符/token
        # 这里使用混合估算
        chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text))
        other_chars = len(text) - chinese_chars
        return chinese_chars // 2 + other_chars // 4
    
    def compress_conversation(
        self,
        messages: List[Union[Dict[str, Any], Message]],
        target_tokens: Optional[int] = None,
    ) -> tuple[List[Dict[str, Any]], CompressionResult]:
        """压缩对话历史
        
        Args:
            messages: 消息列表
            target_tokens: 目标 token 数（默认使用 max_tokens）
            
        Returns:
            (压缩后的消息列表, 压缩结果)
        """
        target = target_tokens or self.max_tokens
        
        # 转换为 Message 对象
        msg_objects = []
        for m in messages:
            if isinstance(m, Message):
                msg_objects.append(m)
            else:
                msg_objects.append(Message.from_dict(m))
        
        # 计算原始 token 数
        original_tokens = sum(self.estimate_tokens(m.content) for m in msg_objects)
        
        # 如果不需要压缩
        if original_tokens <= target:
            return (
                [m.to_dict() for m in msg_objects],
                CompressionResult(
                    original_tokens=original_tokens,
                    compressed_tokens=original_tokens,
                    compression_ratio=1.0,
                    strategy_used="none",
                )
            )
        
        # 分离消息
        total_turns = len(msg_objects)
        
        if total_turns <= self.recent_turns_full:
            # 消息很少，尝试压缩单条消息
            compressed = self._compress_individual_messages(msg_objects, target)
        else:
            # 分层压缩
            compressed = self._layered_compression(msg_objects, target)
        
        compressed_tokens = sum(self.estimate_tokens(m["content"]) for m in compressed)
        
        return (
            compressed,
            CompressionResult(
                original_tokens=original_tokens,
                compressed_tokens=compressed_tokens,
                compression_ratio=compressed_tokens / original_tokens if original_tokens > 0 else 1.0,
                strategy_used="layered",
            )
        )
    
    def _layered_compression(
        self,
        messages: List[Message],
        target_tokens: int,
    ) -> List[Dict[str, Any]]:
        """分层压缩
        
        - 最近 N 轮：完整保留
        - 中间轮次：提取关键信息
        - 早期轮次：生成摘要
        """
        total = len(messages)
        
        # 分层
        recent = messages[-self.recent_turns_full:]  # 最近 3 轮
        middle_end = total - self.recent_turns_full
        middle_start = max(0, middle_end - (self.recent_turns_partial - self.recent_turns_full))
        middle = messages[middle_start:middle_end] if middle_start < middle_end else []
        older = messages[:middle_start] if middle_start > 0 else []
        
        result = []
        
        # 1. 早期消息生成摘要
        if older:
            summary = self._generate_summary(older)
            result.append({
                "role": "system",
                "content": f"[历史摘要] {summary}"
            })
        
        # 2. 中间消息提取关键信息
        if middle:
            for m in middle:
                key_content = self._extract_key_content(m.content)
                if key_content:
                    result.append({
                        "role": m.role,
                        "content": key_content
                    })
        
        # 3. 最近消息完整保留
        for m in recent:
            result.append(m.to_dict())
        
        return result
    
    def _compress_individual_messages(
        self,
        messages: List[Message],
        target_tokens: int,
    ) -> List[Dict[str, Any]]:
        """压缩单条消息"""
        result = []
        tokens_per_message = target_tokens // len(messages) if messages else target_tokens
        
        for m in messages:
            content = m.content
            if self.estimate_tokens(content) > tokens_per_message:
                content = self._truncate_content(content, tokens_per_message)
            result.append({"role": m.role, "content": content})
        
        return result
    
    def _generate_summary(self, messages: List[Message]) -> str:
        """生成消息摘要
        
        简单实现：提取关键词和决策
        """
        # 提取用户请求
        user_requests = []
        decisions = []
        
        for m in messages:
            if m.role == "user":
                # 提取第一句话作为请求摘要
                first_line = m.content.split('\n')[0][:100]
                if first_line:
                    user_requests.append(first_line)
            elif m.role == "assistant":
                # 提取决策关键词
                if "选择" in m.content or "决定" in m.content or "方案" in m.content:
                    # 提取包含关键词的句子
                    for line in m.content.split('\n'):
                        if any(kw in line for kw in ["选择", "决定", "方案", "完成"]):
                            decisions.append(line[:80])
                            break
        
        parts = []
        if user_requests:
            parts.append(f"用户请求: {'; '.join(user_requests[:3])}")
        if decisions:
            parts.append(f"关键决策: {'; '.join(decisions[:3])}")
        
        return " | ".join(parts) if parts else "早期对话内容"
    
    def _extract_key_content(self, content: str) -> str:
        """提取关键内容
        
        保留：
        - 代码块
        - 决策说明
        - 错误信息
        """
        lines = content.split('\n')
        key_lines = []
        in_code_block = False
        
        for line in lines:
            # 代码块
            if line.strip().startswith('```'):
                in_code_block = not in_code_block
                key_lines.append(line)
                continue
            
            if in_code_block:
                key_lines.append(line)
                continue
            
            # 关键词行
            keywords = ["错误", "成功", "完成", "失败", "选择", "决定", "修改", "创建", "删除"]
            if any(kw in line for kw in keywords):
                key_lines.append(line)
        
        return '\n'.join(key_lines) if key_lines else ""
    
    def _truncate_content(self, content: str, max_tokens: int) -> str:
        """截断内容到指定 token 数"""
        # 简单实现：按字符截断
        max_chars = max_tokens * 3  # 估算
        if len(content) <= max_chars:
            return content
        
        return content[:max_chars] + "...[已截断]"
    
    def compress_tool_result(
        self,
        result: Union[str, Dict[str, Any], List[Any]],
        max_lines: int = 50,
        max_chars: int = 2000,
    ) -> str:
        """压缩工具调用结果
        
        Args:
            result: 工具结果
            max_lines: 最大行数
            max_chars: 最大字符数
            
        Returns:
            压缩后的结果字符串
        """
        # 转换为字符串
        if isinstance(result, dict):
            text = json.dumps(result, ensure_ascii=False, indent=2)
        elif isinstance(result, list):
            text = json.dumps(result, ensure_ascii=False, indent=2)
        else:
            text = str(result)
        
        lines = text.split('\n')
        
        # 行数限制
        if len(lines) > max_lines:
            half = max_lines // 2
            lines = lines[:half] + [f"... [省略 {len(lines) - max_lines} 行] ..."] + lines[-half:]
            text = '\n'.join(lines)
        
        # 字符数限制
        if len(text) > max_chars:
            text = text[:max_chars] + f"\n... [已截断，原长度 {len(text)} 字符]"
        
        return text
    
    def compress_code_output(
        self,
        code: str,
        keep_signatures: bool = True,
        keep_docstrings: bool = True,
        max_lines: int = 100,
    ) -> str:
        """压缩代码输出
        
        Args:
            code: 代码内容
            keep_signatures: 保留函数签名
            keep_docstrings: 保留文档字符串
            max_lines: 最大行数
            
        Returns:
            压缩后的代码
        """
        lines = code.split('\n')
        
        if len(lines) <= max_lines:
            return code
        
        result_lines = []
        in_docstring = False
        docstring_char = None
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            
            # 检测文档字符串
            if '"""' in stripped or "'''" in stripped:
                if not in_docstring:
                    in_docstring = True
                    docstring_char = '"""' if '"""' in stripped else "'''"
                    if keep_docstrings:
                        result_lines.append(line)
                elif docstring_char in stripped:
                    in_docstring = False
                    if keep_docstrings:
                        result_lines.append(line)
                continue
            
            if in_docstring:
                if keep_docstrings:
                    result_lines.append(line)
                continue
            
            # 保留函数/类定义
            if stripped.startswith(('def ', 'class ', 'async def ')):
                result_lines.append(line)
                continue
            
            # 保留导入
            if stripped.startswith(('import ', 'from ')):
                result_lines.append(line)
                continue
            
            # 保留装饰器
            if stripped.startswith('@'):
                result_lines.append(line)
                continue
            
            # 其他行：如果还有空间就保留
            if len(result_lines) < max_lines:
                result_lines.append(line)
        
        if len(result_lines) < len(lines):
            result_lines.append(f"# ... [省略 {len(lines) - len(result_lines)} 行]")
        
        return '\n'.join(result_lines)


# ==================== 便捷函数 ====================

def smart_compress(
    context: List[Dict[str, Any]],
    max_tokens: int = 4000,
) -> List[Dict[str, Any]]:
    """智能压缩上下文
    
    Args:
        context: 上下文消息列表
        max_tokens: 最大 token 数
        
    Returns:
        压缩后的消息列表
    """
    compressor = ContextCompressor(max_tokens=max_tokens)
    compressed, _ = compressor.compress_conversation(context)
    return compressed


def compress_for_subagent(
    context: List[Dict[str, Any]],
    task_description: str,
    max_tokens: int = 2000,
) -> List[Dict[str, Any]]:
    """为子代理压缩上下文
    
    Args:
        context: 原始上下文
        task_description: 任务描述
        max_tokens: 最大 token 数
        
    Returns:
        适合子代理的压缩上下文
    """
    compressor = ContextCompressor(max_tokens=max_tokens)
    
    # 生成摘要
    messages = [Message.from_dict(m) for m in context]
    summary = compressor._generate_summary(messages)
    
    return [
        {"role": "system", "content": f"[上下文摘要] {summary}"},
        {"role": "user", "content": task_description},
    ]
