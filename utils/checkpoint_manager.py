"""检查点管理器

支持长任务断点恢复，防止进度丢失。

功能：
- 阶段完成后自动保存状态
- 失败时可从断点恢复
- 支持暂停/继续工作流

Usage:
    from utils.checkpoint_manager import CheckpointManager
    
    # 创建检查点管理器
    manager = CheckpointManager("task_id")
    
    # 保存检查点
    manager.save_checkpoint(
        stage="design_complete",
        state={"files_modified": ["a.py", "b.py"]},
        metadata={"user": "test"}
    )
    
    # 恢复检查点
    checkpoint = manager.load_latest_checkpoint()
    if checkpoint:
        print(f"从阶段 {checkpoint.stage} 恢复")
"""
import json
import os
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class Checkpoint:
    """检查点数据结构"""
    task_id: str
    stage: str
    state: Dict[str, Any]
    timestamp: str
    version: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    completed_steps: List[str] = field(default_factory=list)
    pending_steps: List[str] = field(default_factory=list)
    files_modified: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Checkpoint":
        """从字典创建"""
        return cls(
            task_id=data.get("task_id") or "",
            stage=data.get("stage") or "",
            state=data.get("state") or {},
            timestamp=data.get("timestamp") or datetime.now().isoformat(),
            version=data.get("version") or 1,
            metadata=data.get("metadata") or {},
            completed_steps=data.get("completed_steps") or [],
            pending_steps=data.get("pending_steps") or [],
            files_modified=data.get("files_modified") or [],
        )


class CheckpointManager:
    """检查点管理器
    
    管理任务执行过程中的检查点，支持断点恢复。
    
    存储结构：
        .kiro/checkpoints/
        ├── {task_id}_checkpoint_1.json
        ├── {task_id}_checkpoint_2.json
        └── {task_id}_latest.json
    """
    
    DEFAULT_CHECKPOINT_DIR = ".kiro/checkpoints"
    
    def __init__(
        self, 
        task_id: str, 
        checkpoint_dir: Optional[str] = None,
        max_checkpoints: int = 10
    ):
        """初始化检查点管理器
        
        Args:
            task_id: 任务唯一标识
            checkpoint_dir: 检查点存储目录（默认 .kiro/checkpoints）
            max_checkpoints: 最大保留检查点数量
        """
        self.task_id = task_id
        self.checkpoint_dir = Path(checkpoint_dir or self.DEFAULT_CHECKPOINT_DIR)
        self.max_checkpoints = max_checkpoints
        self._version = 0
        
        # 确保目录存在
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载当前版本号
        self._load_version()
    
    def _load_version(self):
        """加载当前版本号"""
        latest_path = self._get_latest_path()
        if latest_path.exists():
            try:
                with open(latest_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._version = data.get("version") or 0
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"加载版本号失败: {e}")
                self._version = 0
    
    def _get_checkpoint_path(self, version: int) -> Path:
        """获取指定版本的检查点路径"""
        return self.checkpoint_dir / f"{self.task_id}_checkpoint_{version}.json"
    
    def _get_latest_path(self) -> Path:
        """获取最新检查点路径"""
        return self.checkpoint_dir / f"{self.task_id}_latest.json"
    
    def save_checkpoint(
        self,
        stage: str,
        state: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        completed_steps: Optional[List[str]] = None,
        pending_steps: Optional[List[str]] = None,
        files_modified: Optional[List[str]] = None,
    ) -> Checkpoint:
        """保存检查点
        
        Args:
            stage: 当前阶段名称
            state: 状态数据
            metadata: 元数据（可选）
            completed_steps: 已完成步骤（可选）
            pending_steps: 待处理步骤（可选）
            files_modified: 已修改文件（可选）
            
        Returns:
            保存的检查点对象
        """
        self._version += 1
        
        checkpoint = Checkpoint(
            task_id=self.task_id,
            stage=stage,
            state=state,
            timestamp=datetime.now().isoformat(),
            version=self._version,
            metadata=metadata or {},
            completed_steps=completed_steps or [],
            pending_steps=pending_steps or [],
            files_modified=files_modified or [],
        )
        
        # 保存到版本文件
        checkpoint_path = self._get_checkpoint_path(self._version)
        self._write_checkpoint(checkpoint_path, checkpoint)
        
        # 更新 latest 文件
        latest_path = self._get_latest_path()
        self._write_checkpoint(latest_path, checkpoint)
        
        # 清理旧检查点
        self._cleanup_old_checkpoints()
        
        logger.info(f"检查点已保存: {stage} (版本 {self._version})")
        return checkpoint
    
    def _write_checkpoint(self, path: Path, checkpoint: Checkpoint):
        """写入检查点文件"""
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(checkpoint.to_dict(), f, ensure_ascii=False, indent=2)
        except IOError as e:
            logger.error(f"写入检查点失败: {e}")
            raise
    
    def load_latest_checkpoint(self) -> Optional[Checkpoint]:
        """加载最新检查点
        
        Returns:
            检查点对象，如果不存在返回 None
        """
        latest_path = self._get_latest_path()
        if not latest_path.exists():
            return None
        
        try:
            with open(latest_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return Checkpoint.from_dict(data)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"加载检查点失败: {e}")
            return None
    
    def load_checkpoint(self, version: int) -> Optional[Checkpoint]:
        """加载指定版本的检查点
        
        Args:
            version: 版本号
            
        Returns:
            检查点对象，如果不存在返回 None
        """
        checkpoint_path = self._get_checkpoint_path(version)
        if not checkpoint_path.exists():
            return None
        
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return Checkpoint.from_dict(data)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"加载检查点 v{version} 失败: {e}")
            return None
    
    def list_checkpoints(self) -> List[Checkpoint]:
        """列出所有检查点
        
        Returns:
            检查点列表（按版本号排序）
        """
        checkpoints = []
        for path in self.checkpoint_dir.glob(f"{self.task_id}_checkpoint_*.json"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    checkpoints.append(Checkpoint.from_dict(data))
            except (json.JSONDecodeError, IOError):
                continue
        
        return sorted(checkpoints, key=lambda c: c.version)
    
    def _cleanup_old_checkpoints(self):
        """清理旧检查点，保留最近 N 个"""
        checkpoints = self.list_checkpoints()
        if len(checkpoints) > self.max_checkpoints:
            # 删除最旧的检查点
            for checkpoint in checkpoints[:-self.max_checkpoints]:
                path = self._get_checkpoint_path(checkpoint.version)
                try:
                    path.unlink()
                    logger.debug(f"删除旧检查点: v{checkpoint.version}")
                except IOError:
                    pass
    
    def delete_all_checkpoints(self):
        """删除所有检查点"""
        for path in self.checkpoint_dir.glob(f"{self.task_id}_*.json"):
            try:
                path.unlink()
            except IOError:
                pass
        self._version = 0
        logger.info(f"已删除任务 {self.task_id} 的所有检查点")
    
    def get_recovery_info(self) -> Optional[Dict[str, Any]]:
        """获取恢复信息
        
        Returns:
            恢复信息字典，包含阶段、已完成步骤、待处理步骤等
        """
        checkpoint = self.load_latest_checkpoint()
        if not checkpoint:
            return None
        
        return {
            "task_id": checkpoint.task_id,
            "stage": checkpoint.stage,
            "timestamp": checkpoint.timestamp,
            "version": checkpoint.version,
            "completed_steps": checkpoint.completed_steps,
            "pending_steps": checkpoint.pending_steps,
            "files_modified": checkpoint.files_modified,
            "can_resume": len(checkpoint.pending_steps) > 0,
        }


def create_task_checkpoint(
    task_id: str,
    stage: str,
    state: Dict[str, Any],
    **kwargs
) -> Checkpoint:
    """便捷函数：创建任务检查点
    
    Args:
        task_id: 任务 ID
        stage: 阶段名称
        state: 状态数据
        **kwargs: 其他参数传递给 save_checkpoint
        
    Returns:
        检查点对象
    """
    manager = CheckpointManager(task_id)
    return manager.save_checkpoint(stage, state, **kwargs)


def load_task_checkpoint(task_id: str) -> Optional[Checkpoint]:
    """便捷函数：加载任务检查点
    
    Args:
        task_id: 任务 ID
        
    Returns:
        检查点对象，如果不存在返回 None
    """
    manager = CheckpointManager(task_id)
    return manager.load_latest_checkpoint()
