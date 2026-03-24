"""交接 Schema 验证器

验证子代理任务交接的数据结构，确保必填字段完整。

功能：
- 定义标准的任务交接 Schema
- 验证任务描述完整性
- 提供验证错误的详细信息

Usage:
    from utils.handoff_validator import (
        TaskHandoff, 
        validate_handoff,
        HandoffValidator
    )
    
    # 创建任务交接
    handoff = TaskHandoff(
        task_id="task_001",
        task_type="backend_dev",
        description="实现文件上传功能",
        context={"tech_stack": "FastAPI"},
        expected_output=["file_service.py"],
        acceptance_criteria=["支持 PDF/Excel 上传"]
    )
    
    # 验证交接
    is_valid, errors = validate_handoff(handoff)
"""
import logging
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class TaskType(Enum):
    """任务类型"""
    PM = "pm"                       # 产品经理
    TECH_LEAD = "tech_lead"         # 技术总监
    CONTEXT_GATHER = "context"      # 代码探索
    BACKEND_DEV = "backend_dev"     # 后端开发
    FRONTEND_DEV = "frontend_dev"   # 前端开发
    QA_ENGINEER = "qa_engineer"     # 测试工程师
    CODE_REVIEWER = "code_reviewer" # 代码审查
    SECURITY = "security"           # 安全分析
    PERFORMANCE = "performance"     # 性能工程
    DEVOPS = "devops"               # DevOps
    DATABASE = "database"           # 数据库
    API_DESIGN = "api_design"       # API 设计


class TaskPriority(Enum):
    """任务优先级"""
    P0 = "P0"  # 紧急
    P1 = "P1"  # 高
    P2 = "P2"  # 中
    P3 = "P3"  # 低


@dataclass
class TaskHandoff:
    """任务交接数据结构
    
    定义子代理任务的标准格式，确保信息完整。
    """
    # 必填字段
    task_id: str                              # 任务唯一标识
    task_type: str                            # 任务类型（见 TaskType）
    description: str                          # 任务描述
    
    # 上下文信息
    context: Dict[str, Any] = field(default_factory=dict)  # 背景信息
    constraints: List[str] = field(default_factory=list)   # 约束条件
    
    # 输入输出
    input_files: List[str] = field(default_factory=list)   # 输入文件
    expected_output: List[str] = field(default_factory=list)  # 预期输出
    
    # 验收标准
    acceptance_criteria: List[str] = field(default_factory=list)  # 验收标准
    
    # 元数据
    priority: str = "P1"                      # 优先级
    estimated_complexity: int = 3             # 复杂度（1-5）
    dependencies: List[str] = field(default_factory=list)  # 依赖的任务 ID
    
    # 时间戳
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskHandoff":
        """从字典创建"""
        return cls(
            task_id=data.get("task_id") or "",
            task_type=data.get("task_type") or "",
            description=data.get("description") or "",
            context=data.get("context") or {},
            constraints=data.get("constraints") or [],
            input_files=data.get("input_files") or [],
            expected_output=data.get("expected_output") or [],
            acceptance_criteria=data.get("acceptance_criteria") or [],
            priority=data.get("priority") or "P1",
            estimated_complexity=data.get("estimated_complexity") or 3,
            dependencies=data.get("dependencies") or [],
            created_at=data.get("created_at") or datetime.now().isoformat(),
        )
    
    def to_prompt(self) -> str:
        """转换为子代理提示词格式"""
        lines = [
            f"## 任务：{self.description}",
            "",
            f"**任务 ID**：{self.task_id}",
            f"**类型**：{self.task_type}",
            f"**优先级**：{self.priority}",
            f"**复杂度**：{self.estimated_complexity}/5",
            "",
        ]
        
        if self.context:
            lines.append("### 背景信息")
            for key, value in self.context.items():
                lines.append(f"- {key}：{value}")
            lines.append("")
        
        if self.constraints:
            lines.append("### 约束条件")
            for constraint in self.constraints:
                lines.append(f"- {constraint}")
            lines.append("")
        
        if self.input_files:
            lines.append("### 输入文件")
            for f in self.input_files:
                lines.append(f"- {f}")
            lines.append("")
        
        if self.expected_output:
            lines.append("### 预期输出")
            for output in self.expected_output:
                lines.append(f"- {output}")
            lines.append("")
        
        if self.acceptance_criteria:
            lines.append("### 验收标准")
            for criteria in self.acceptance_criteria:
                lines.append(f"- [ ] {criteria}")
            lines.append("")
        
        if self.dependencies:
            lines.append("### 依赖任务")
            for dep in self.dependencies:
                lines.append(f"- {dep}")
            lines.append("")
        
        return "\n".join(lines)


@dataclass
class ValidationError:
    """验证错误"""
    field: str
    message: str
    severity: str = "error"  # error, warning


class HandoffValidator:
    """交接验证器
    
    验证 TaskHandoff 的完整性和有效性。
    """
    
    # 必填字段
    REQUIRED_FIELDS = ["task_id", "task_type", "description"]
    
    # 任务类型对应的必填字段
    TYPE_REQUIRED_FIELDS = {
        TaskType.BACKEND_DEV.value: ["expected_output"],
        TaskType.FRONTEND_DEV.value: ["expected_output"],
        TaskType.QA_ENGINEER.value: ["input_files", "acceptance_criteria"],
        TaskType.CODE_REVIEWER.value: ["input_files"],
    }
    
    # 字段长度限制
    MIN_DESCRIPTION_LENGTH = 10
    MAX_DESCRIPTION_LENGTH = 2000
    
    def __init__(self, strict: bool = False):
        """初始化验证器
        
        Args:
            strict: 严格模式，警告也视为错误
        """
        self.strict = strict
    
    def validate(self, handoff: TaskHandoff) -> Tuple[bool, List[ValidationError]]:
        """验证任务交接
        
        Args:
            handoff: 任务交接对象
            
        Returns:
            (是否有效, 错误列表)
        """
        errors: List[ValidationError] = []
        
        # 1. 验证必填字段
        errors.extend(self._validate_required_fields(handoff))
        
        # 2. 验证字段格式
        errors.extend(self._validate_field_formats(handoff))
        
        # 3. 验证任务类型特定字段
        errors.extend(self._validate_type_specific(handoff))
        
        # 4. 验证逻辑一致性
        errors.extend(self._validate_consistency(handoff))
        
        # 判断是否有效
        if self.strict:
            is_valid = len(errors) == 0
        else:
            is_valid = all(e.severity != "error" for e in errors)
        
        return is_valid, errors
    
    def _validate_required_fields(self, handoff: TaskHandoff) -> List[ValidationError]:
        """验证必填字段"""
        errors = []
        
        for field_name in self.REQUIRED_FIELDS:
            value = getattr(handoff, field_name, None)
            if not value:
                errors.append(ValidationError(
                    field=field_name,
                    message=f"必填字段 '{field_name}' 不能为空",
                    severity="error"
                ))
        
        return errors
    
    def _validate_field_formats(self, handoff: TaskHandoff) -> List[ValidationError]:
        """验证字段格式"""
        errors = []
        
        # 验证描述长度
        if handoff.description:
            if len(handoff.description) < self.MIN_DESCRIPTION_LENGTH:
                errors.append(ValidationError(
                    field="description",
                    message=f"描述太短，至少需要 {self.MIN_DESCRIPTION_LENGTH} 个字符",
                    severity="warning"
                ))
            elif len(handoff.description) > self.MAX_DESCRIPTION_LENGTH:
                errors.append(ValidationError(
                    field="description",
                    message=f"描述太长，最多 {self.MAX_DESCRIPTION_LENGTH} 个字符",
                    severity="error"
                ))
        
        # 验证任务类型
        valid_types = [t.value for t in TaskType]
        if handoff.task_type and handoff.task_type not in valid_types:
            errors.append(ValidationError(
                field="task_type",
                message=f"无效的任务类型 '{handoff.task_type}'，有效值：{valid_types}",
                severity="warning"
            ))
        
        # 验证优先级
        valid_priorities = [p.value for p in TaskPriority]
        if handoff.priority and handoff.priority not in valid_priorities:
            errors.append(ValidationError(
                field="priority",
                message=f"无效的优先级 '{handoff.priority}'，有效值：{valid_priorities}",
                severity="warning"
            ))
        
        # 验证复杂度
        if not 1 <= handoff.estimated_complexity <= 5:
            errors.append(ValidationError(
                field="estimated_complexity",
                message=f"复杂度必须在 1-5 之间，当前值：{handoff.estimated_complexity}",
                severity="warning"
            ))
        
        return errors
    
    def _validate_type_specific(self, handoff: TaskHandoff) -> List[ValidationError]:
        """验证任务类型特定字段"""
        errors = []
        
        required_fields = self.TYPE_REQUIRED_FIELDS.get(handoff.task_type, [])
        
        for field_name in required_fields:
            value = getattr(handoff, field_name, None)
            if not value:
                errors.append(ValidationError(
                    field=field_name,
                    message=f"任务类型 '{handoff.task_type}' 需要提供 '{field_name}'",
                    severity="warning"
                ))
        
        return errors
    
    def _validate_consistency(self, handoff: TaskHandoff) -> List[ValidationError]:
        """验证逻辑一致性"""
        errors = []
        
        # 高复杂度任务应该有验收标准
        if handoff.estimated_complexity >= 4 and not handoff.acceptance_criteria:
            errors.append(ValidationError(
                field="acceptance_criteria",
                message="高复杂度任务（≥4）建议提供验收标准",
                severity="warning"
            ))
        
        # 开发任务应该有预期输出
        dev_types = [TaskType.BACKEND_DEV.value, TaskType.FRONTEND_DEV.value]
        if handoff.task_type in dev_types and not handoff.expected_output:
            errors.append(ValidationError(
                field="expected_output",
                message="开发任务建议提供预期输出文件",
                severity="warning"
            ))
        
        return errors


# ==================== 便捷函数 ====================

def validate_handoff(handoff: TaskHandoff, strict: bool = False) -> Tuple[bool, List[ValidationError]]:
    """验证任务交接
    
    Args:
        handoff: 任务交接对象
        strict: 严格模式
        
    Returns:
        (是否有效, 错误列表)
    """
    validator = HandoffValidator(strict=strict)
    return validator.validate(handoff)


def create_handoff(
    task_id: str,
    task_type: str,
    description: str,
    **kwargs
) -> TaskHandoff:
    """创建任务交接
    
    Args:
        task_id: 任务 ID
        task_type: 任务类型
        description: 任务描述
        **kwargs: 其他字段
        
    Returns:
        TaskHandoff 对象
    """
    return TaskHandoff(
        task_id=task_id,
        task_type=task_type,
        description=description,
        **kwargs
    )


def handoff_to_prompt(handoff: TaskHandoff) -> str:
    """将任务交接转换为提示词
    
    Args:
        handoff: 任务交接对象
        
    Returns:
        格式化的提示词字符串
    """
    return handoff.to_prompt()
