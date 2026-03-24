"""提示词模块

提供提示词加载和缓存功能。

Requirements:
- 8.1: 应用启动时从 "提示词" 目录加载所有提示词
- 8.3: 正确映射资料类型名称到对应的提示词文件
- 8.4: 加载后缓存提示词供后续使用
"""
import logging
from pathlib import Path
from typing import Optional

# 配置日志
logger = logging.getLogger(__name__)

# 模块级缓存：存储已加载的提示词
# key: 提示词文件名（相对路径），value: 提示词内容
_prompts_cache: dict[str, str] = {}

# 默认提示词目录
# 支持多个候选路径，按优先级查找：
# 1. 打包环境：当前工作目录/prompts/（提示词 .md 文件）
# 2. 开发环境：工作区根目录/提示词/
def _find_prompts_dir() -> Path:
    """查找提示词目录，支持开发环境和打包环境
    
    查找优先级：
    1. prompts 模块目录本身（打包后 .md 文件在这里）
    2. 当前工作目录/prompts/
    3. 开发环境：工作区根目录/提示词/
    """
    # 去重候选路径
    seen = set()
    candidates = []
    
    raw_candidates = [
        Path(__file__).parent,   # 打包环境：prompts 模块目录本身
        Path.cwd() / "prompts",  # 打包环境：当前目录/prompts/
        Path(__file__).parent.parent.parent / "提示词",  # 开发环境
    ]
    
    for c in raw_candidates:
        resolved = c.resolve()
        if resolved not in seen:
            seen.add(resolved)
            candidates.append(c)
    
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            # 使用 rglob 检查目录及子目录下是否有 .md 文件
            md_files = list(candidate.rglob("*.md"))
            if md_files:
                logger.info(f"找到提示词目录: {candidate} (含 {len(md_files)} 个 .md 文件)")
                return candidate
    
    # 都没找到，返回默认路径（会在 load_prompts 中报警告）
    logger.warning(f"未找到有效的提示词目录，候选路径: {[str(c) for c in candidates]}")
    return candidates[0] if candidates else Path.cwd() / "prompts"

DEFAULT_PROMPTS_DIR = _find_prompts_dir()


def load_prompts(prompts_dir: Optional[Path] = None) -> dict[str, str]:
    """加载所有提示词文件
    
    从指定目录（包括子目录）加载所有 .md 文件，并缓存到模块级字典中。
    
    Args:
        prompts_dir: 提示词目录路径，默认为工作区根目录下的 "提示词" 文件夹
        
    Returns:
        dict: 提示词字典，key 为相对于 prompts_dir 的文件路径，value 为文件内容
        
    Requirements:
        - 8.1: 应用启动时从 "提示词" 目录加载所有提示词
        - 8.4: 加载后缓存提示词供后续使用
        
    Example:
        >>> prompts = load_prompts()
        >>> "企业征信提取_提示词.md" in prompts
        True
    """
    global _prompts_cache
    
    if prompts_dir is None:
        prompts_dir = DEFAULT_PROMPTS_DIR
    
    prompts_dir = Path(prompts_dir)
    
    if not prompts_dir.exists():
        logger.warning(f"提示词目录不存在: {prompts_dir}")
        return {}
    
    if not prompts_dir.is_dir():
        logger.warning(f"提示词路径不是目录: {prompts_dir}")
        return {}
    
    loaded_prompts: dict[str, str] = {}
    
    # 递归遍历所有 .md 文件
    for md_file in prompts_dir.rglob("*.md"):
        try:
            # 计算相对路径作为 key
            relative_path = md_file.relative_to(prompts_dir)
            # 使用正斜杠统一路径分隔符
            key = str(relative_path).replace("\\", "/")
            
            content = md_file.read_text(encoding="utf-8")
            loaded_prompts[key] = content
            logger.debug(f"已加载提示词: {key}")
            
        except Exception as e:
            logger.warning(f"加载提示词文件失败 {md_file}: {e}")
            continue
    
    # 更新缓存
    _prompts_cache.update(loaded_prompts)
    
    logger.info(f"共加载 {len(loaded_prompts)} 个提示词文件")
    return loaded_prompts


def get_prompt_for_type(data_type: str, prompts: Optional[dict[str, str]] = None) -> str:
    """获取指定资料类型的提示词
    
    根据资料类型名称，从 DATA_TYPE_CONFIG 中查找对应的提示词文件名，
    然后从提示词字典中获取内容。
    
    Args:
        data_type: 资料类型名称，如 "企业征信提取"、"个人征信提取" 等
        prompts: 提示词字典，如果为 None 则使用缓存
        
    Returns:
        str: 提示词内容，如果找不到则返回空字符串
        
    Requirements:
        - 8.3: 正确映射资料类型名称到对应的提示词文件
        - 8.4: 使用缓存的提示词
        
    Example:
        >>> prompt = get_prompt_for_type("企业征信提取")
        >>> len(prompt) > 0
        True
    """
    # 延迟导入避免循环依赖
    from config import DATA_TYPE_CONFIG
    
    # 使用传入的字典或缓存
    if prompts is None:
        prompts = _prompts_cache
        # 如果缓存为空，尝试加载
        if not prompts:
            prompts = load_prompts()
    
    # 从配置中获取提示词文件名
    config = DATA_TYPE_CONFIG.get(data_type)
    if config is None:
        logger.warning(f"未知的资料类型: {data_type}")
        return ""
    
    prompt_file = config.get("prompt_file")
    if not prompt_file:
        logger.warning(f"资料类型 {data_type} 没有配置提示词文件")
        return ""
    
    # 从提示词字典中获取内容
    prompt_content = prompts.get(prompt_file, "")
    
    if not prompt_content:
        logger.warning(f"找不到提示词文件: {prompt_file} (资料类型: {data_type})")
    
    return prompt_content


def get_cached_prompts() -> dict[str, str]:
    """获取当前缓存的提示词字典
    
    Returns:
        dict: 缓存的提示词字典的副本
    """
    return _prompts_cache.copy()


def clear_cache() -> None:
    """清除提示词缓存
    
    主要用于测试或需要重新加载提示词的场景。
    """
    global _prompts_cache
    _prompts_cache.clear()
    logger.debug("提示词缓存已清除")


# 文档类型自动识别提示词
# 用于 AIService.classify() 方法，判断上传文档的类型
DOCUMENT_TYPE_DETECTION_PROMPT = """## 角色
你是金融数据识别分析师，负责判断材料类别并透传原始数据。

## 任务
1. 分析输入内容，判断属于以下5种类别之一
2. 将输入的原始数据完整输出

## 类别（8选1）
- 企业征信
- 个人征信
- 企业流水
- 个人流水
- 财务数据
- 抵押物信息
- 水母报告
- 个人纳税公积金

## 判断规则（按优先级排序，优先匹配排在前面的类别）

### 财务数据（优先级最高）
- 含"资产负债表"、"利润表"、"现金流量表"任一关键词
- 含"财务报表"、"会计科目"、"财务指标"
- 含"经营活动产生的现金流量"、"投资活动产生的现金流量"、"筹资活动产生的现金流量"
- 含"营业收入"、"营业成本"、"净利润"、"资产总计"、"负债合计"
- 注意：现金流量表是财务报表，不是银行流水！

### 个人征信
- 含"个人信用报告"或"个人征信"
- 含"姓名"+"证件类型:身份证"+"证件号码"
- 含个人信用卡、贷记卡、个人贷款记录
- 来源"中国人民银行征信中心"且主体为自然人

### 企业征信
- 含"企业信用报告"或"企业征信"
- 含"统一社会信用代码"
- 含企业名称 + 企业授信/对公贷款记录

### 企业流水（优先于个人流水判断）
- 含对公账户/企业账户交易明细（逐笔交易记录）
- 含企业名称/公司名称 + 银行流水记录
- 特征：有具体的交易日期、交易对手、摘要、借方/贷方金额
- 关键词：**"对公"、"企业"、"公司"、"有限公司"、"单位"、"对公账户"、"基本户"、"一般户"**
- 账户名称为企业/公司名称（如"XX有限公司"、"XX科技公司"）→ 判定为企业流水
- 注意：银行流水是逐笔交易记录，不是汇总的财务报表！
- **即使是 PDF 格式的银行流水，只要账户主体是企业/公司，就是企业流水**

### 个人流水
- 含个人银行账户交易明细（逐笔交易记录）
- 含个人姓名/身份证号 + 收支记录
- 特征：有具体的交易日期、交易对手、摘要、收入/支出金额
- 账户名称为个人姓名（非企业/公司名称）→ 判定为个人流水
- **注意：如果流水中账户持有人是企业/公司名称，应判定为"企业流水"而非"个人流水"**

### 抵押物信息
- 含房产信息：房屋坐落、建筑面积、不动产权证号、产权年限
- 含车辆信息：车牌号、品牌型号、车辆登记证
- 含设备信息：设备编号、设备名称、购置价格
- 含抵押/质押、评估价值、权属人等关键词

### 水母报告
- 含企业基本信息：企业名称、统一社会信用代码、成立时间
- 含纳税信息：纳税信用等级、纳税总额、完税证明
- 含开票信息：开票金额、进项发票、销项发票
- 含供应商/客户信息：供应商名单、客户名单、集中度
- 来源"水母报告"或含上述多项企业经营数据

### 个人纳税公积金
- 含收入纳税明细：个人所得税、应纳税额、扣缴义务人
- 含公积金信息：住房公积金、缴存基数、月缴存额、公积金账户

## 输出要求
只返回类别名称（8选1），不要其他内容。如果无法判断则返回"未知"。

## 注意
1. 即使文本格式混乱（OCR提取），只要能识别关键特征就做判断
2. 材料提到企业（如"为XX公司担保"）但主体是个人，仍判定为个人类
"""


# 保留旧版兼容性
PROMPTS_DIR = DEFAULT_PROMPTS_DIR


def load_prompt(name: str) -> str:
    """加载提示词文件（旧版兼容接口）
    
    Args:
        name: 提示词名称（不含扩展名）
        
    Returns:
        str: 提示词内容
    """
    prompt_file = PROMPTS_DIR / f"{name}.md"
    if prompt_file.exists():
        return prompt_file.read_text(encoding="utf-8")
    return ""
