"""任务日志器

结构化记录任务执行过程，支持 7 维度可观测性。

7 个可观测性维度：
1. Prompts - 代理看到什么
2. Decisions - 为什么选择该路径
3. Tool calls - 采取什么行动
4. Tool results - 发生了什么
5. Agent state - 决策时知道什么
6. Errors - 失败、重试、降级
7. Outcomes - 用户得到什么

Usage:
    from utils.task_logger import TaskLogger, LogDimension
    
    # 创建日志器
    logger = TaskLogger("task_id")
    
    # 记录不同维度
    logger.log_prompt("用户请求：实现文件上传功能")
    logger.log_decision("选择方案 A", reason="性能更好")
    logger.log_tool_call("readFile", {"path": "a.py"})
    logger.log_tool_result("readFile", {"lines": 100})
    logger.log_error("APIError", "连接超时", retriable=True)
    logger.log_outcome("success", "文件上传功能已实现")
    
    # 导出日志
    logger.export_ndjson("task_log.ndjson")
"""
import json
import logging
from typing import Optional, Dict, Any, List, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from enum import Enum

logger = logging.getLogger(__name__)


class LogDimension(Enum):
    """日志维度"""
    PROMPT = "prompt"           # 代理看到什么
    DECISION = "decision"       # 为什么选择该路径
    TOOL_CALL = "tool_call"     # 采取什么行动
    TOOL_RESULT = "tool_result" # 发生了什么
    AGENT_STATE = "agent_state" # 决策时知道什么
    ERROR = "error"             # 失败、重试、降级
    OUTCOME = "outcome"         # 用户得到什么


class LogLevel(Enum):
    """日志级别"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass
class LogEntry:
    """日志条目"""
    timestamp: str
    dimension: str
    level: str
    message: str
    task_id: str
    data: Dict[str, Any] = field(default_factory=dict)
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    agent_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = asdict(self)
        # 移除 None 值
        return {k: v for k, v in result.items() if v is not None}
    
    def to_ndjson(self) -> str:
        """转换为 NDJSON 格式"""
        return json.dumps(self.to_dict(), ensure_ascii=False)


class TaskLogger:
    """任务日志器
    
    记录任务执行过程中的 7 个可观测性维度。
    
    存储结构：
        .kiro/logs/
        ├── 2026-02-02_task_001.ndjson
        └── summary.json
    """
    
    DEFAULT_LOG_DIR = ".kiro/logs"
    
    def __init__(
        self,
        task_id: str,
        log_dir: Optional[str] = None,
        agent_id: Optional[str] = None,
        trace_id: Optional[str] = None,
    ):
        """初始化任务日志器
        
        Args:
            task_id: 任务唯一标识
            log_dir: 日志存储目录（默认 .kiro/logs）
            agent_id: 代理 ID（可选）
            trace_id: 追踪 ID（可选）
        """
        self.task_id = task_id
        self.log_dir = Path(log_dir or self.DEFAULT_LOG_DIR)
        self.agent_id = agent_id
        self.trace_id = trace_id or self._generate_trace_id()
        self._span_counter = 0
        self._entries: List[LogEntry] = []
        self._start_time = datetime.now()
        
        # 确保目录存在
        self.log_dir.mkdir(parents=True, exist_ok=True)
    
    def _generate_trace_id(self) -> str:
        """生成追踪 ID"""
        return f"{self.task_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    def _generate_span_id(self) -> str:
        """生成 span ID"""
        self._span_counter += 1
        return f"{self.trace_id}_{self._span_counter:04d}"
    
    def _create_entry(
        self,
        dimension: LogDimension,
        level: LogLevel,
        message: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> LogEntry:
        """创建日志条目"""
        entry = LogEntry(
            timestamp=datetime.now().isoformat(),
            dimension=dimension.value,
            level=level.value,
            message=message,
            task_id=self.task_id,
            data=data or {},
            trace_id=self.trace_id,
            span_id=self._generate_span_id(),
            agent_id=self.agent_id,
        )
        self._entries.append(entry)
        return entry
    
    # ==================== 7 维度日志方法 ====================
    
    def log_prompt(
        self,
        prompt: str,
        context: Optional[Dict[str, Any]] = None,
        level: LogLevel = LogLevel.INFO,
    ) -> LogEntry:
        """记录 Prompt（代理看到什么）
        
        Args:
            prompt: 提示内容
            context: 上下文信息
            level: 日志级别
        """
        return self._create_entry(
            LogDimension.PROMPT,
            level,
            prompt,
            {"context": context} if context else None,
        )
    
    def log_decision(
        self,
        decision: str,
        reason: Optional[str] = None,
        alternatives: Optional[List[str]] = None,
        level: LogLevel = LogLevel.INFO,
    ) -> LogEntry:
        """记录 Decision（为什么选择该路径）
        
        Args:
            decision: 决策内容
            reason: 决策原因
            alternatives: 备选方案
            level: 日志级别
        """
        data = {}
        if reason:
            data["reason"] = reason
        if alternatives:
            data["alternatives"] = alternatives
        
        return self._create_entry(
            LogDimension.DECISION,
            level,
            decision,
            data if data else None,
        )
    
    def log_tool_call(
        self,
        tool_name: str,
        params: Optional[Dict[str, Any]] = None,
        level: LogLevel = LogLevel.INFO,
    ) -> LogEntry:
        """记录 Tool Call（采取什么行动）
        
        Args:
            tool_name: 工具名称
            params: 调用参数
            level: 日志级别
        """
        return self._create_entry(
            LogDimension.TOOL_CALL,
            level,
            f"调用工具: {tool_name}",
            {"tool": tool_name, "params": params} if params else {"tool": tool_name},
        )
    
    def log_tool_result(
        self,
        tool_name: str,
        result: Any,
        success: bool = True,
        duration_ms: Optional[int] = None,
        level: LogLevel = LogLevel.INFO,
    ) -> LogEntry:
        """记录 Tool Result（发生了什么）
        
        Args:
            tool_name: 工具名称
            result: 执行结果
            success: 是否成功
            duration_ms: 执行时间（毫秒）
            level: 日志级别
        """
        data = {
            "tool": tool_name,
            "success": success,
            "result": result if isinstance(result, (str, int, float, bool, list, dict)) else str(result),
        }
        if duration_ms is not None:
            data["duration_ms"] = duration_ms
        
        return self._create_entry(
            LogDimension.TOOL_RESULT,
            level,
            f"工具结果: {tool_name} ({'成功' if success else '失败'})",
            data,
        )
    
    def log_agent_state(
        self,
        state: Dict[str, Any],
        description: Optional[str] = None,
        level: LogLevel = LogLevel.DEBUG,
    ) -> LogEntry:
        """记录 Agent State（决策时知道什么）
        
        Args:
            state: 状态数据
            description: 状态描述
            level: 日志级别
        """
        return self._create_entry(
            LogDimension.AGENT_STATE,
            level,
            description or "代理状态快照",
            {"state": state},
        )
    
    def log_error(
        self,
        error_type: str,
        error_message: str,
        retriable: bool = False,
        retry_count: int = 0,
        stack_trace: Optional[str] = None,
        level: LogLevel = LogLevel.ERROR,
    ) -> LogEntry:
        """记录 Error（失败、重试、降级）
        
        Args:
            error_type: 错误类型
            error_message: 错误信息
            retriable: 是否可重试
            retry_count: 重试次数
            stack_trace: 堆栈跟踪
            level: 日志级别
        """
        data = {
            "error_type": error_type,
            "retriable": retriable,
            "retry_count": retry_count,
        }
        if stack_trace:
            data["stack_trace"] = stack_trace
        
        return self._create_entry(
            LogDimension.ERROR,
            level,
            f"{error_type}: {error_message}",
            data,
        )
    
    def log_outcome(
        self,
        status: str,
        result: Union[str, Dict[str, Any]],
        user_feedback: Optional[str] = None,
        level: LogLevel = LogLevel.INFO,
    ) -> LogEntry:
        """记录 Outcome（用户得到什么）
        
        Args:
            status: 状态（success/failure/partial）
            result: 结果
            user_feedback: 用户反馈
            level: 日志级别
        """
        data = {
            "status": status,
            "result": result,
        }
        if user_feedback:
            data["user_feedback"] = user_feedback
        
        return self._create_entry(
            LogDimension.OUTCOME,
            level,
            f"任务结果: {status}",
            data,
        )
    
    # ==================== 导出方法 ====================
    
    def export_ndjson(self, filename: Optional[str] = None) -> Path:
        """导出为 NDJSON 格式
        
        Args:
            filename: 文件名（可选，默认使用日期+任务ID）
            
        Returns:
            导出文件路径
        """
        if not filename:
            date_str = datetime.now().strftime("%Y-%m-%d")
            filename = f"{date_str}_{self.task_id}.ndjson"
        
        filepath = self.log_dir / filename
        
        with open(filepath, "w", encoding="utf-8") as f:
            for entry in self._entries:
                f.write(entry.to_ndjson() + "\n")
        
        logger.info(f"日志已导出: {filepath}")
        return filepath
    
    def export_summary(self) -> Dict[str, Any]:
        """导出任务摘要
        
        Returns:
            摘要字典
        """
        end_time = datetime.now()
        duration = (end_time - self._start_time).total_seconds()
        
        # 统计各维度日志数量
        dimension_counts = {}
        for entry in self._entries:
            dim = entry.dimension
            dimension_counts[dim] = dimension_counts.get(dim, 0) + 1
        
        # 统计错误
        errors = [e for e in self._entries if e.dimension == LogDimension.ERROR.value]
        
        # 获取最终结果
        outcomes = [e for e in self._entries if e.dimension == LogDimension.OUTCOME.value]
        final_outcome = outcomes[-1] if outcomes else None
        
        return {
            "task_id": self.task_id,
            "trace_id": self.trace_id,
            "start_time": self._start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "total_entries": len(self._entries),
            "dimension_counts": dimension_counts,
            "error_count": len(errors),
            "final_status": final_outcome.data.get("status") if final_outcome else None,
        }
    
    def get_entries(
        self,
        dimension: Optional[LogDimension] = None,
        level: Optional[LogLevel] = None,
    ) -> List[LogEntry]:
        """获取日志条目
        
        Args:
            dimension: 过滤维度（可选）
            level: 过滤级别（可选）
            
        Returns:
            日志条目列表
        """
        entries = self._entries
        
        if dimension:
            entries = [e for e in entries if e.dimension == dimension.value]
        
        if level:
            entries = [e for e in entries if e.level == level.value]
        
        return entries


# ==================== 便捷函数 ====================

def create_task_logger(
    task_id: str,
    agent_id: Optional[str] = None,
) -> TaskLogger:
    """创建任务日志器
    
    Args:
        task_id: 任务 ID
        agent_id: 代理 ID（可选）
        
    Returns:
        TaskLogger 实例
    """
    return TaskLogger(task_id, agent_id=agent_id)


def log_task_start(task_logger: TaskLogger, description: str) -> LogEntry:
    """记录任务开始
    
    Args:
        task_logger: 日志器
        description: 任务描述
        
    Returns:
        日志条目
    """
    return task_logger.log_prompt(f"任务开始: {description}")


def log_task_end(
    task_logger: TaskLogger,
    success: bool,
    result: Union[str, Dict[str, Any]],
) -> LogEntry:
    """记录任务结束
    
    Args:
        task_logger: 日志器
        success: 是否成功
        result: 结果
        
    Returns:
        日志条目
    """
    status = "success" if success else "failure"
    return task_logger.log_outcome(status, result)
