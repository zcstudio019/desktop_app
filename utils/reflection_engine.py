"""内置 Reflection 模式引擎

实现生成→审查→改进的循环，提升输出质量。

功能：
- 生成初始输出
- 自动审查输出质量
- 根据审查结果改进
- 支持多轮迭代

Usage:
    from utils.reflection_engine import (
        ReflectionEngine,
        ReflectionResult,
        create_reflection_engine,
    )
    
    # 创建引擎
    engine = create_reflection_engine(max_iterations=3)
    
    # 执行反思循环
    result = engine.reflect(
        initial_output="初始代码...",
        review_criteria=["代码是否正确", "是否有错误处理"],
    )
    
    print(result.final_output)
    print(result.improvements)
"""
import logging
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class ReflectionPhase(Enum):
    """反思阶段"""
    GENERATE = "generate"   # 生成
    REVIEW = "review"       # 审查
    IMPROVE = "improve"     # 改进
    COMPLETE = "complete"   # 完成


class QualityLevel(Enum):
    """质量等级"""
    EXCELLENT = "excellent"  # 优秀
    GOOD = "good"            # 良好
    ACCEPTABLE = "acceptable"  # 可接受
    NEEDS_IMPROVEMENT = "needs_improvement"  # 需改进
    POOR = "poor"            # 差


@dataclass
class ReviewFinding:
    """审查发现"""
    category: str           # 类别
    description: str        # 描述
    severity: str           # 严重程度：critical, warning, info
    suggestion: str         # 改进建议
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category,
            "description": self.description,
            "severity": self.severity,
            "suggestion": self.suggestion,
        }


@dataclass
class ReflectionIteration:
    """反思迭代记录"""
    iteration: int                    # 迭代次数
    phase: str                        # 当前阶段
    output: str                       # 当前输出
    findings: List[ReviewFinding]     # 审查发现
    quality_score: float              # 质量分数（0-1）
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "iteration": self.iteration,
            "phase": self.phase,
            "output": self.output[:200] + "..." if len(self.output) > 200 else self.output,
            "findings_count": len(self.findings),
            "quality_score": self.quality_score,
            "timestamp": self.timestamp,
        }


@dataclass
class ReflectionResult:
    """反思结果"""
    initial_output: str               # 初始输出
    final_output: str                 # 最终输出
    iterations: List[ReflectionIteration]  # 迭代历史
    total_iterations: int             # 总迭代次数
    final_quality: QualityLevel       # 最终质量
    improvements: List[str]           # 改进列表
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_iterations": self.total_iterations,
            "final_quality": self.final_quality.value,
            "improvements_count": len(self.improvements),
            "improvements": self.improvements,
        }


class ReflectionEngine:
    """反思引擎
    
    实现生成→审查→改进的循环。
    """
    
    # 默认配置
    DEFAULT_MAX_ITERATIONS = 3
    DEFAULT_QUALITY_THRESHOLD = 0.8
    
    # 默认审查标准
    DEFAULT_CRITERIA = [
        "输出是否完整",
        "是否有明显错误",
        "是否符合要求",
        "代码是否可运行",
        "是否有错误处理",
    ]
    
    def __init__(
        self,
        max_iterations: int = DEFAULT_MAX_ITERATIONS,
        quality_threshold: float = DEFAULT_QUALITY_THRESHOLD,
        review_fn: Optional[Callable[[str, List[str]], List[ReviewFinding]]] = None,
        improve_fn: Optional[Callable[[str, List[ReviewFinding]], str]] = None,
    ):
        """初始化反思引擎
        
        Args:
            max_iterations: 最大迭代次数
            quality_threshold: 质量阈值（达到后停止迭代）
            review_fn: 自定义审查函数
            improve_fn: 自定义改进函数
        """
        self.max_iterations = max_iterations
        self.quality_threshold = quality_threshold
        self._review_fn = review_fn or self._default_review
        self._improve_fn = improve_fn or self._default_improve
    
    def reflect(
        self,
        initial_output: str,
        review_criteria: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> ReflectionResult:
        """执行反思循环
        
        Args:
            initial_output: 初始输出
            review_criteria: 审查标准
            context: 上下文信息
            
        Returns:
            反思结果
        """
        criteria = review_criteria or self.DEFAULT_CRITERIA
        current_output = initial_output
        iterations: List[ReflectionIteration] = []
        improvements: List[str] = []
        
        for i in range(self.max_iterations):
            logger.debug(f"反思迭代 {i + 1}/{self.max_iterations}")
            
            # 审查阶段
            findings = self._review_fn(current_output, criteria)
            quality_score = self._calculate_quality(findings)
            
            iteration = ReflectionIteration(
                iteration=i + 1,
                phase=ReflectionPhase.REVIEW.value,
                output=current_output,
                findings=findings,
                quality_score=quality_score,
            )
            iterations.append(iteration)
            
            # 检查是否达到质量阈值
            if quality_score >= self.quality_threshold:
                logger.debug(f"质量达标 ({quality_score:.2f} >= {self.quality_threshold})")
                break
            
            # 改进阶段
            if findings:
                improved_output = self._improve_fn(current_output, findings)
                
                # 记录改进
                for finding in findings:
                    if finding.severity in ["critical", "warning"]:
                        improvements.append(finding.suggestion)
                
                current_output = improved_output
        
        # 确定最终质量等级
        final_quality = self._determine_quality_level(
            iterations[-1].quality_score if iterations else 0
        )
        
        return ReflectionResult(
            initial_output=initial_output,
            final_output=current_output,
            iterations=iterations,
            total_iterations=len(iterations),
            final_quality=final_quality,
            improvements=improvements,
        )
    
    def _default_review(
        self,
        output: str,
        criteria: List[str],
    ) -> List[ReviewFinding]:
        """默认审查函数（基于规则）
        
        Args:
            output: 待审查输出
            criteria: 审查标准
            
        Returns:
            审查发现列表
        """
        findings: List[ReviewFinding] = []
        
        # 基本检查
        if not output or not output.strip():
            findings.append(ReviewFinding(
                category="completeness",
                description="输出为空",
                severity="critical",
                suggestion="需要生成有效输出",
            ))
            return findings
        
        # 长度检查
        if len(output) < 50:
            findings.append(ReviewFinding(
                category="completeness",
                description="输出过短，可能不完整",
                severity="warning",
                suggestion="检查输出是否完整",
            ))
        
        # 代码检查（如果包含代码）
        if "```" in output or "def " in output or "class " in output:
            # 检查是否有错误处理
            if "try" not in output and "except" not in output:
                findings.append(ReviewFinding(
                    category="error_handling",
                    description="代码缺少错误处理",
                    severity="warning",
                    suggestion="添加 try-except 错误处理",
                ))
            
            # 检查是否有类型注解
            if "def " in output and "->" not in output:
                findings.append(ReviewFinding(
                    category="type_hints",
                    description="函数缺少返回类型注解",
                    severity="info",
                    suggestion="添加类型注解提高可读性",
                ))
        
        # 检查常见问题
        if "TODO" in output or "FIXME" in output:
            findings.append(ReviewFinding(
                category="completeness",
                description="包含未完成的 TODO/FIXME",
                severity="warning",
                suggestion="完成或移除 TODO/FIXME 标记",
            ))
        
        return findings
    
    def _default_improve(
        self,
        output: str,
        findings: List[ReviewFinding],
    ) -> str:
        """默认改进函数（返回原输出 + 改进建议）
        
        实际使用时应该替换为 AI 调用。
        
        Args:
            output: 原输出
            findings: 审查发现
            
        Returns:
            改进后的输出
        """
        # 默认实现只是添加注释
        # 实际使用时应该调用 AI 进行改进
        suggestions = [f.suggestion for f in findings if f.severity != "info"]
        
        if not suggestions:
            return output
        
        # 添加改进建议作为注释
        improvement_note = "\n".join([f"# 改进建议: {s}" for s in suggestions])
        
        return f"{improvement_note}\n\n{output}"
    
    def _calculate_quality(self, findings: List[ReviewFinding]) -> float:
        """计算质量分数
        
        Args:
            findings: 审查发现
            
        Returns:
            质量分数（0-1）
        """
        if not findings:
            return 1.0
        
        # 根据严重程度扣分
        deductions = {
            "critical": 0.3,
            "warning": 0.1,
            "info": 0.02,
        }
        
        total_deduction = sum(
            deductions.get(f.severity, 0) for f in findings
        )
        
        return max(0, 1.0 - total_deduction)
    
    def _determine_quality_level(self, score: float) -> QualityLevel:
        """确定质量等级
        
        Args:
            score: 质量分数
            
        Returns:
            质量等级
        """
        if score >= 0.95:
            return QualityLevel.EXCELLENT
        elif score >= 0.8:
            return QualityLevel.GOOD
        elif score >= 0.6:
            return QualityLevel.ACCEPTABLE
        elif score >= 0.4:
            return QualityLevel.NEEDS_IMPROVEMENT
        else:
            return QualityLevel.POOR


# ==================== 便捷函数 ====================

def create_reflection_engine(
    max_iterations: int = ReflectionEngine.DEFAULT_MAX_ITERATIONS,
    quality_threshold: float = ReflectionEngine.DEFAULT_QUALITY_THRESHOLD,
    review_fn: Optional[Callable] = None,
    improve_fn: Optional[Callable] = None,
) -> ReflectionEngine:
    """创建反思引擎
    
    Args:
        max_iterations: 最大迭代次数
        quality_threshold: 质量阈值
        review_fn: 自定义审查函数
        improve_fn: 自定义改进函数
        
    Returns:
        ReflectionEngine 实例
    """
    return ReflectionEngine(
        max_iterations=max_iterations,
        quality_threshold=quality_threshold,
        review_fn=review_fn,
        improve_fn=improve_fn,
    )


def quick_reflect(
    output: str,
    criteria: Optional[List[str]] = None,
) -> ReflectionResult:
    """快速反思（使用默认配置）
    
    Args:
        output: 待反思输出
        criteria: 审查标准
        
    Returns:
        反思结果
    """
    engine = create_reflection_engine()
    return engine.reflect(output, criteria)
