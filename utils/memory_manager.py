"""记忆分层管理器

实现短期/中期/长期三层记忆管理，优化上下文使用。

功能：
- 短期记忆：当前任务上下文（最近 N 条消息）
- 中期记忆：会话级别摘要（任务完成后压缩）
- 长期记忆：持久化知识（跨会话保留）

Usage:
    from utils.memory_manager import (
        MemoryManager,
        MemoryLayer,
        create_memory_manager,
    )
    
    # 创建管理器
    manager = create_memory_manager(task_id="task_001")
    
    # 添加短期记忆
    manager.add_short_term("用户上传了财务报表")
    
    # 提升到中期记忆
    manager.promote_to_medium("财务报表已解析，包含资产负债表")
    
    # 保存长期记忆
    manager.save_long_term("企业名称", "上海昭晟机电")
    
    # 获取相关上下文
    context = manager.get_relevant_context(query="财务数据")
"""
import json
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
import hashlib

logger = logging.getLogger(__name__)


class MemoryLayer(Enum):
    """记忆层级"""
    SHORT_TERM = "short_term"    # 短期：当前任务
    MEDIUM_TERM = "medium_term"  # 中期：会话摘要
    LONG_TERM = "long_term"      # 长期：持久知识


@dataclass
class MemoryItem:
    """记忆条目"""
    content: str                  # 内容
    layer: str                    # 层级
    timestamp: str                # 时间戳
    relevance_score: float = 1.0  # 相关性分数
    tags: List[str] = field(default_factory=list)  # 标签
    metadata: Dict[str, Any] = field(default_factory=dict)  # 元数据
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MemoryItem":
        """从字典创建"""
        return cls(
            content=data.get("content") or "",
            layer=data.get("layer") or MemoryLayer.SHORT_TERM.value,
            timestamp=data.get("timestamp") or datetime.now().isoformat(),
            relevance_score=data.get("relevance_score") or 1.0,
            tags=data.get("tags") or [],
            metadata=data.get("metadata") or {},
        )


class MemoryManager:
    """记忆分层管理器
    
    管理三层记忆结构，优化上下文使用效率。
    """
    
    # 默认配置
    DEFAULT_SHORT_TERM_LIMIT = 20   # 短期记忆条数限制
    DEFAULT_MEDIUM_TERM_LIMIT = 50  # 中期记忆条数限制
    DEFAULT_LONG_TERM_DIR = "data/memory"  # 长期记忆存储目录
    
    def __init__(
        self,
        task_id: str,
        short_term_limit: int = DEFAULT_SHORT_TERM_LIMIT,
        medium_term_limit: int = DEFAULT_MEDIUM_TERM_LIMIT,
        long_term_dir: Optional[str] = None,
    ):
        """初始化记忆管理器
        
        Args:
            task_id: 任务 ID
            short_term_limit: 短期记忆条数限制
            medium_term_limit: 中期记忆条数限制
            long_term_dir: 长期记忆存储目录
        """
        self.task_id = task_id
        self.short_term_limit = short_term_limit
        self.medium_term_limit = medium_term_limit
        self.long_term_dir = Path(long_term_dir or self.DEFAULT_LONG_TERM_DIR)
        
        # 三层记忆存储
        self._short_term: List[MemoryItem] = []
        self._medium_term: List[MemoryItem] = []
        self._long_term: Dict[str, Any] = {}
        
        # 加载长期记忆
        self._load_long_term()
    
    # ==================== 短期记忆 ====================
    
    def add_short_term(
        self,
        content: str,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """添加短期记忆
        
        Args:
            content: 记忆内容
            tags: 标签列表
            metadata: 元数据
        """
        item = MemoryItem(
            content=content,
            layer=MemoryLayer.SHORT_TERM.value,
            timestamp=datetime.now().isoformat(),
            tags=tags or [],
            metadata=metadata or {},
        )
        
        self._short_term.append(item)
        
        # 超出限制时，将最旧的提升到中期
        if len(self._short_term) > self.short_term_limit:
            oldest = self._short_term.pop(0)
            self._promote_item(oldest)
        
        logger.debug(f"添加短期记忆: {content[:50]}...")
    
    def get_short_term(self, limit: Optional[int] = None) -> List[MemoryItem]:
        """获取短期记忆
        
        Args:
            limit: 返回条数限制
            
        Returns:
            记忆列表
        """
        if limit:
            return self._short_term[-limit:]
        return self._short_term.copy()
    
    def clear_short_term(self) -> None:
        """清空短期记忆"""
        self._short_term.clear()
        logger.debug("清空短期记忆")
    
    # ==================== 中期记忆 ====================
    
    def promote_to_medium(
        self,
        content: str,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """添加中期记忆
        
        Args:
            content: 记忆内容
            tags: 标签列表
            metadata: 元数据
        """
        item = MemoryItem(
            content=content,
            layer=MemoryLayer.MEDIUM_TERM.value,
            timestamp=datetime.now().isoformat(),
            tags=tags or [],
            metadata=metadata or {},
        )
        
        self._medium_term.append(item)
        
        # 超出限制时，压缩最旧的
        if len(self._medium_term) > self.medium_term_limit:
            self._compress_medium_term()
        
        logger.debug(f"添加中期记忆: {content[:50]}...")
    
    def _promote_item(self, item: MemoryItem) -> None:
        """将记忆条目提升到中期"""
        item.layer = MemoryLayer.MEDIUM_TERM.value
        self._medium_term.append(item)
    
    def _compress_medium_term(self) -> None:
        """压缩中期记忆（保留最近一半）"""
        half = self.medium_term_limit // 2
        self._medium_term = self._medium_term[-half:]
        logger.debug(f"压缩中期记忆，保留 {half} 条")
    
    def get_medium_term(self, limit: Optional[int] = None) -> List[MemoryItem]:
        """获取中期记忆
        
        Args:
            limit: 返回条数限制
            
        Returns:
            记忆列表
        """
        if limit:
            return self._medium_term[-limit:]
        return self._medium_term.copy()
    
    # ==================== 长期记忆 ====================
    
    def save_long_term(self, key: str, value: Any) -> None:
        """保存长期记忆
        
        Args:
            key: 键名
            value: 值
        """
        self._long_term[key] = {
            "value": value,
            "updated_at": datetime.now().isoformat(),
        }
        self._persist_long_term()
        logger.debug(f"保存长期记忆: {key}")
    
    def get_long_term(self, key: str, default: Any = None) -> Any:
        """获取长期记忆
        
        Args:
            key: 键名
            default: 默认值
            
        Returns:
            记忆值
        """
        item = self._long_term.get(key)
        if item:
            return item.get("value")
        return default
    
    def _load_long_term(self) -> None:
        """加载长期记忆"""
        file_path = self._get_long_term_path()
        if file_path.exists():
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    self._long_term = json.load(f)
                logger.debug(f"加载长期记忆: {file_path}")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"加载长期记忆失败: {e}")
                self._long_term = {}
    
    def _persist_long_term(self) -> None:
        """持久化长期记忆"""
        file_path = self._get_long_term_path()
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(self._long_term, f, ensure_ascii=False, indent=2)
            logger.debug(f"持久化长期记忆: {file_path}")
        except IOError as e:
            logger.error(f"持久化长期记忆失败: {e}")
    
    def _get_long_term_path(self) -> Path:
        """获取长期记忆文件路径"""
        # 使用任务 ID 的哈希作为文件名
        task_hash = hashlib.md5(self.task_id.encode()).hexdigest()[:8]
        return self.long_term_dir / f"memory_{task_hash}.json"
    
    # ==================== 上下文检索 ====================
    
    def get_relevant_context(
        self,
        query: Optional[str] = None,
        max_items: int = 10,
        include_layers: Optional[List[MemoryLayer]] = None,
    ) -> str:
        """获取相关上下文
        
        Args:
            query: 查询关键词（用于过滤）
            max_items: 最大返回条数
            include_layers: 包含的层级
            
        Returns:
            格式化的上下文字符串
        """
        layers = include_layers or [
            MemoryLayer.SHORT_TERM,
            MemoryLayer.MEDIUM_TERM,
        ]
        
        items: List[MemoryItem] = []
        
        # 收集各层记忆
        if MemoryLayer.SHORT_TERM in layers:
            items.extend(self._short_term)
        if MemoryLayer.MEDIUM_TERM in layers:
            items.extend(self._medium_term)
        
        # 按关键词过滤
        if query:
            items = [
                item for item in items
                if query.lower() in item.content.lower()
                or any(query.lower() in tag.lower() for tag in item.tags)
            ]
        
        # 按时间排序，取最近的
        items.sort(key=lambda x: x.timestamp, reverse=True)
        items = items[:max_items]
        
        # 格式化输出
        if not items:
            return ""
        
        lines = ["## 相关上下文", ""]
        for item in items:
            layer_name = {
                MemoryLayer.SHORT_TERM.value: "短期",
                MemoryLayer.MEDIUM_TERM.value: "中期",
            }.get(item.layer, "未知")
            
            lines.append(f"- [{layer_name}] {item.content}")
        
        # 添加长期记忆
        if MemoryLayer.LONG_TERM in (include_layers or []):
            if self._long_term:
                lines.append("")
                lines.append("### 长期记忆")
                for key, data in self._long_term.items():
                    value = data.get("value") if isinstance(data, dict) else data
                    lines.append(f"- {key}: {value}")
        
        return "\n".join(lines)
    
    def get_summary(self) -> Dict[str, Any]:
        """获取记忆摘要
        
        Returns:
            摘要信息
        """
        return {
            "task_id": self.task_id,
            "short_term_count": len(self._short_term),
            "medium_term_count": len(self._medium_term),
            "long_term_keys": list(self._long_term.keys()),
            "short_term_limit": self.short_term_limit,
            "medium_term_limit": self.medium_term_limit,
        }


# ==================== 便捷函数 ====================

def create_memory_manager(
    task_id: str,
    short_term_limit: int = MemoryManager.DEFAULT_SHORT_TERM_LIMIT,
    medium_term_limit: int = MemoryManager.DEFAULT_MEDIUM_TERM_LIMIT,
    long_term_dir: Optional[str] = None,
) -> MemoryManager:
    """创建记忆管理器
    
    Args:
        task_id: 任务 ID
        short_term_limit: 短期记忆限制
        medium_term_limit: 中期记忆限制
        long_term_dir: 长期记忆目录
        
    Returns:
        MemoryManager 实例
    """
    return MemoryManager(
        task_id=task_id,
        short_term_limit=short_term_limit,
        medium_term_limit=medium_term_limit,
        long_term_dir=long_term_dir,
    )
