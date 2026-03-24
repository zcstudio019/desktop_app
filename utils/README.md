# 工具模块使用指南

> 子代理优化工具模块的业务集成指南
> 
> 创建日期：2026-02-02

---

## 模块概览

| 模块 | 用途 | 业务场景 |
|------|------|---------|
| `checkpoint_manager` | 检查点管理 | 长任务断点恢复 |
| `task_logger` | 7维度结构化日志 | 任务追踪与调试 |
| `context_compressor` | 上下文压缩 | 长对话/大上下文处理 |
| `retry_handler` | 重试+断路器 | API 调用容错 |
| `handoff_validator` | 交接Schema验证 | 子代理任务分配 |
| `memory_manager` | 记忆分层管理 | 上下文持久化 |
| `reflection_engine` | 内置Reflection | 输出质量改进 |

---

## 1. 检查点管理（CheckpointManager）

### 适用场景
- 多阶段长任务（如：批量文件处理、数据迁移）
- 需要断点恢复的任务
- 任务状态需要持久化

### 使用示例

```python
from utils import CheckpointManager, Checkpoint, create_task_checkpoint, load_task_checkpoint

# 方式1：使用便捷函数
# 创建检查点
create_task_checkpoint(
    task_id="batch_upload_001",
    phase="processing",
    data={
        "total_files": 100,
        "processed": 45,
        "current_file": "report.pdf"
    }
)

# 恢复检查点
checkpoint = load_task_checkpoint("batch_upload_001")
if checkpoint:
    print(f"从阶段 {checkpoint.phase} 恢复，已处理 {checkpoint.data['processed']} 个文件")

# 方式2：使用管理器（更多控制）
manager = CheckpointManager(checkpoint_dir="./checkpoints")

# 保存检查点
checkpoint = manager.save(
    task_id="batch_upload_001",
    phase="processing",
    data={"processed": 45, "total": 100}
)

# 列出所有检查点
checkpoints = manager.list_checkpoints("batch_upload_001")

# 加载最新检查点
latest = manager.load_latest("batch_upload_001")

# 清理旧检查点（保留最新5个）
manager.cleanup("batch_upload_001", keep_count=5)
```

### 业务集成建议

```python
# 在 ai_service.py 中集成
async def batch_extract_data(files: List[str]) -> Dict:
    """批量提取数据，支持断点恢复"""
    task_id = f"batch_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # 尝试恢复
    checkpoint = load_task_checkpoint(task_id)
    start_index = checkpoint.data.get("processed", 0) if checkpoint else 0
    results = checkpoint.data.get("results", {}) if checkpoint else {}
    
    for i, file in enumerate(files[start_index:], start=start_index):
        try:
            result = await extract_single_file(file)
            results[file] = result
            
            # 每处理10个文件保存一次检查点
            if (i + 1) % 10 == 0:
                create_task_checkpoint(
                    task_id=task_id,
                    phase="processing",
                    data={"processed": i + 1, "results": results}
                )
        except Exception as e:
            # 出错时保存检查点
            create_task_checkpoint(
                task_id=task_id,
                phase="error",
                data={"processed": i, "results": results, "error": str(e)}
            )
            raise
    
    return results
```

---

## 2. 任务日志（TaskLogger）

### 适用场景
- 需要追踪任务执行过程
- 调试复杂流程
- 生产环境问题排查

### 7个可观测维度
1. **Prompts** - 代理看到什么
2. **Decisions** - 为什么选择该路径
3. **Tool calls** - 采取什么行动
4. **Tool results** - 发生了什么
5. **Agent state** - 决策时知道什么
6. **Errors** - 失败、重试、降级
7. **Outcomes** - 用户得到什么

### 使用示例

```python
from utils import TaskLogger, LogDimension, LogLevel, create_task_logger, log_task_start, log_task_end

# 方式1：使用便捷函数
log_task_start("extract_data", {"file": "report.pdf"})
# ... 执行任务 ...
log_task_end("extract_data", {"status": "success", "fields_extracted": 15})

# 方式2：使用日志器（更多控制）
logger = create_task_logger(
    task_id="extract_data_001",
    log_dir="./logs"
)

# 记录不同维度
logger.log(LogDimension.PROMPTS, "开始提取财务数据", {"file": "report.pdf"})
logger.log(LogDimension.DECISIONS, "选择 OCR 模式", {"reason": "文件是图片格式"})
logger.log(LogDimension.TOOL_CALLS, "调用百度 OCR", {"image_size": "1024x768"})
logger.log(LogDimension.TOOL_RESULTS, "OCR 完成", {"text_length": 5000})
logger.log(LogDimension.ERRORS, "JSON 解析失败", {"error": "Unexpected token"}, level=LogLevel.WARNING)
logger.log(LogDimension.OUTCOMES, "提取完成", {"fields": 15, "confidence": 0.95})

# 查询日志
errors = logger.query(dimension=LogDimension.ERRORS)
recent = logger.query(limit=10)
```

### 业务集成建议

```python
# 在 feishu_service.py 中集成
from utils import create_task_logger, LogDimension

async def save_to_feishu(data: Dict, record_id: str) -> bool:
    """保存数据到飞书，带完整日志"""
    logger = create_task_logger(f"feishu_save_{record_id}")
    
    logger.log(LogDimension.PROMPTS, "开始保存到飞书", {"record_id": record_id})
    
    try:
        logger.log(LogDimension.TOOL_CALLS, "调用飞书 API", {"fields": list(data.keys())})
        result = await feishu_api.update_record(record_id, data)
        logger.log(LogDimension.TOOL_RESULTS, "API 返回", {"success": True})
        logger.log(LogDimension.OUTCOMES, "保存成功", {"updated_fields": len(data)})
        return True
    except Exception as e:
        logger.log(LogDimension.ERRORS, "保存失败", {"error": str(e)}, level=LogLevel.ERROR)
        raise
```

---

## 3. 上下文压缩（ContextCompressor）

### 适用场景
- 长对话上下文管理
- 子代理任务上下文传递
- 大文档处理

### 4种压缩策略
1. **RECURSIVE_SUMMARY** - 递归摘要
2. **HIERARCHICAL** - 层级压缩
3. **SELECTIVE** - 选择性保留
4. **TOOL_CLEANUP** - 工具结果清理

### 使用示例

```python
from utils import ContextCompressor, CompressionStrategy, smart_compress, compress_for_subagent

# 方式1：使用便捷函数
messages = [
    {"role": "user", "content": "分析这份报告..."},
    {"role": "assistant", "content": "好的，我来分析..."},
    # ... 更多消息
]

# 智能压缩（保留最近3轮，压缩其他）
compressed = smart_compress(messages, max_tokens=4000)

# 为子代理压缩（更激进）
subagent_context = compress_for_subagent(messages, max_tokens=2000)

# 方式2：使用压缩器（更多控制）
compressor = ContextCompressor(
    max_tokens=4000,
    recent_turns_to_keep=3,
    strategy=CompressionStrategy.HIERARCHICAL
)

result = compressor.compress(messages)
print(f"压缩前: {result.original_tokens} tokens")
print(f"压缩后: {result.compressed_tokens} tokens")
print(f"压缩率: {result.compression_ratio:.1%}")
```

### 业务集成建议

```python
# 在 ai_service.py 中集成
from utils import smart_compress

async def chat_with_context(messages: List[Dict], new_message: str) -> str:
    """带上下文压缩的对话"""
    # 添加新消息
    messages.append({"role": "user", "content": new_message})
    
    # 检查是否需要压缩（假设模型上下文限制 8000 tokens）
    if estimate_tokens(messages) > 6000:  # 留 2000 给响应
        messages = smart_compress(messages, max_tokens=6000)
    
    response = await call_deepseek(messages)
    return response
```

---

## 4. 重试处理（RetryHandler）+ 断路器（CircuitBreaker）

### 适用场景
- 外部 API 调用（DeepSeek、飞书、百度 OCR）
- 网络不稳定场景
- 需要防止级联故障

### 已集成位置
`ai_service.py` 已使用 `create_ai_retry_handler`

### 使用示例

```python
from utils import (
    RetryHandler, RetryConfig,
    CircuitBreaker, CircuitBreakerConfig,
    with_retry_and_circuit_breaker,
    create_ai_retry_handler,
    create_feishu_retry_handler,
    create_ocr_retry_handler
)

# 方式1：使用预配置的处理器
ai_handler = create_ai_retry_handler()
result = await ai_handler.execute(call_deepseek, prompt)

feishu_handler = create_feishu_retry_handler()
result = await feishu_handler.execute(feishu_api.update, data)

# 方式2：使用装饰器
@with_retry_and_circuit_breaker(
    service_name="deepseek",
    max_retries=3,
    base_delay=1.0
)
async def call_ai(prompt: str) -> str:
    return await deepseek_client.chat(prompt)

# 方式3：自定义配置
handler = RetryHandler(RetryConfig(
    max_retries=3,
    base_delay=1.0,
    max_delay=30.0,
    exponential_base=2.0,
    jitter=True
))

breaker = CircuitBreaker(CircuitBreakerConfig(
    failure_threshold=5,
    recovery_timeout=60.0,
    half_open_max_calls=3
))
```

### 错误分类
| HTTP 状态码 | 分类 | 处理方式 |
|------------|------|---------|
| 429 | 限流 | 可重试，指数退避 |
| 5xx | 服务器错误 | 可重试，指数退避 |
| 4xx | 客户端错误 | 不可重试，立即失败 |
| 超时 | 网络问题 | 可重试，增加超时 |

---

## 5. 交接验证（HandoffValidator）

### 适用场景
- 子代理任务分配
- 任务描述标准化
- 确保任务信息完整

### 使用示例

```python
from utils import (
    TaskHandoff, TaskType, TaskPriority,
    validate_handoff, create_handoff, handoff_to_prompt
)

# 创建任务交接
handoff = create_handoff(
    task_id="impl_file_detector",
    task_type=TaskType.BACKEND_DEV,
    title="实现文件类型检测模块",
    description="根据文件内容自动识别文件类型（PDF/Excel/图片等）",
    context={
        "tech_stack": "Python 3.12",
        "existing_modules": ["file_service.py", "ocr_service.py"]
    },
    expected_output=["services/file_type_detector.py"],
    acceptance_criteria=[
        "支持 PDF、Excel、Word、图片格式",
        "准确率 > 95%",
        "有完整的错误处理"
    ],
    priority=TaskPriority.HIGH
)

# 验证任务
errors, warnings = validate_handoff(handoff)
if errors:
    print(f"任务定义有误: {errors}")
if warnings:
    print(f"建议改进: {warnings}")

# 转换为子代理提示词
prompt = handoff_to_prompt(handoff)
# 可直接用于 invokeSubAgent
```

### 业务集成建议

```python
# 在主策划流程中使用
from utils import create_handoff, handoff_to_prompt, TaskType

def delegate_to_subagent(task_info: Dict) -> str:
    """标准化任务委派"""
    handoff = create_handoff(
        task_id=task_info["id"],
        task_type=TaskType[task_info["type"]],
        title=task_info["title"],
        description=task_info["description"],
        context=task_info.get("context", {}),
        expected_output=task_info.get("output", []),
        acceptance_criteria=task_info.get("criteria", [])
    )
    
    errors, _ = validate_handoff(handoff)
    if errors:
        raise ValueError(f"任务定义不完整: {errors}")
    
    return handoff_to_prompt(handoff)
```

---

## 6. 记忆管理（MemoryManager）

### 适用场景
- 跨会话上下文保持
- 项目知识库管理
- 长期记忆持久化

### 记忆分层
| 层级 | 内容 | 特点 |
|------|------|------|
| 短期 | 当前任务上下文 | 完整保留，会话结束清除 |
| 中期 | 最近会话摘要 | 压缩形式，保留数小时 |
| 长期 | 关键事实和关系 | 提取存储，持久化 |

### 使用示例

```python
from utils import MemoryManager, MemoryLayer, create_memory_manager

# 创建记忆管理器
manager = create_memory_manager(
    storage_dir="./memory",
    short_term_limit=100,
    mid_term_limit=50
)

# 添加记忆
manager.add(
    layer=MemoryLayer.SHORT_TERM,
    content="用户上传了财务报表",
    metadata={"file": "report.pdf", "type": "财务数据"}
)

manager.add(
    layer=MemoryLayer.LONG_TERM,
    content="企业名称: ABC公司, 注册资本: 1000万",
    metadata={"entity": "ABC公司", "source": "营业执照"}
)

# 查询记忆
recent = manager.query(layer=MemoryLayer.SHORT_TERM, limit=10)
facts = manager.query(
    layer=MemoryLayer.LONG_TERM,
    filter={"entity": "ABC公司"}
)

# 获取上下文摘要
context = manager.get_context_summary()
```

---

## 7. 反思引擎（ReflectionEngine）

### 适用场景
- AI 输出质量改进
- 自动审查和修正
- 迭代优化

### 反思流程
```
生成初稿 → 审查问题 → 改进输出 → 再次审查 → ... → 达到质量标准
```

### 使用示例

```python
from utils import ReflectionEngine, create_reflection_engine, quick_reflect

# 方式1：快速反思（单次）
improved = quick_reflect(
    content="这是AI生成的初稿...",
    review_criteria=["准确性", "完整性", "格式规范"]
)

# 方式2：使用引擎（多轮迭代）
engine = create_reflection_engine(
    max_iterations=3,
    quality_threshold=0.8
)

result = engine.reflect(
    initial_content="初始输出...",
    review_fn=lambda content: review_output(content),  # 自定义审查函数
    improve_fn=lambda content, findings: improve_output(content, findings)  # 自定义改进函数
)

print(f"迭代次数: {result.iterations}")
print(f"最终质量: {result.quality_score}")
print(f"改进内容: {result.final_content}")
```

### 业务集成建议

```python
# 在 ai_service.py 中集成
from utils import create_reflection_engine

async def extract_with_reflection(content: str, prompt: str) -> Dict:
    """带反思的数据提取"""
    engine = create_reflection_engine(max_iterations=2)
    
    async def review(output: str) -> List[str]:
        """审查提取结果"""
        findings = []
        try:
            data = json.loads(output)
            if not data.get("企业名称"):
                findings.append("缺少企业名称")
            if not data.get("财务数据"):
                findings.append("缺少财务数据")
        except json.JSONDecodeError:
            findings.append("JSON 格式错误")
        return findings
    
    async def improve(output: str, findings: List[str]) -> str:
        """根据审查结果改进"""
        improve_prompt = f"请修正以下问题: {findings}\n原始输出: {output}"
        return await call_deepseek(improve_prompt)
    
    result = await engine.reflect_async(
        initial_content=await call_deepseek(prompt),
        review_fn=review,
        improve_fn=improve
    )
    
    return json.loads(result.final_content)
```

---

## 快速参考

### 导入方式

```python
# 导入所有工具
from utils import (
    # 检查点
    CheckpointManager, create_task_checkpoint, load_task_checkpoint,
    # 日志
    TaskLogger, LogDimension, create_task_logger,
    # 压缩
    ContextCompressor, smart_compress, compress_for_subagent,
    # 重试
    RetryHandler, CircuitBreaker, create_ai_retry_handler,
    # 交接
    TaskHandoff, validate_handoff, handoff_to_prompt,
    # 记忆
    MemoryManager, create_memory_manager,
    # 反思
    ReflectionEngine, quick_reflect
)
```

### 推荐集成优先级

| 优先级 | 模块 | 集成位置 | 原因 |
|-------|------|---------|------|
| P0 | retry_handler | ai_service.py | ✅ 已集成 |
| P1 | task_logger | 所有服务 | 调试和问题排查 |
| P1 | checkpoint_manager | 批量任务 | 断点恢复 |
| P2 | context_compressor | 长对话场景 | 上下文管理 |
| P2 | reflection_engine | AI 输出 | 质量改进 |
| P3 | memory_manager | 跨会话场景 | 上下文持久化 |
| P3 | handoff_validator | 子代理调用 | 任务标准化 |

---

*最后更新：2026-02-02*
