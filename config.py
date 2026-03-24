"""配置管理"""
import os
from pathlib import Path
from dotenv import load_dotenv

# 加载 .env 文件（支持从任意目录运行）
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

# DeepSeek
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")

# 飞书（支持两种认证方式）
# 方式1：个人访问令牌（简单，适合个人使用）
FEISHU_PERSONAL_BASE_TOKEN = os.getenv("FEISHU_PERSONAL_BASE_TOKEN", "")
# 方式2：应用凭证（推荐，token 自动刷新）
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
# 通用配置
FEISHU_APP_TOKEN = os.getenv("FEISHU_APP_TOKEN", "")
FEISHU_TABLE_ID = os.getenv("FEISHU_TABLE_ID", "")

# 百度 OCR
BAIDU_OCR_APP_ID = os.getenv("BAIDU_OCR_APP_ID", "")
BAIDU_OCR_API_KEY = os.getenv("BAIDU_OCR_API_KEY", "")
BAIDU_OCR_SECRET_KEY = os.getenv("BAIDU_OCR_SECRET_KEY", "")

# 产品库飞书应用（独立凭证，用于访问知识库文档）
# Requirement 2.9: 使用独立的应用凭证访问产品库
WIKI_APP_ID = os.getenv("WIKI_APP_ID", "")
WIKI_APP_SECRET = os.getenv("WIKI_APP_SECRET", "")

# 本地存储配置
# USE_LOCAL_STORAGE: True=使用本地SQLite存储, False=使用飞书多维表格
USE_LOCAL_STORAGE = os.getenv("USE_LOCAL_STORAGE", "true").lower() == "true"
# LOCAL_DB_PATH: SQLite 数据库文件路径（相对于 backend 目录）
LOCAL_DB_PATH = os.getenv("LOCAL_DB_PATH", "data/customers.db")
STORE_ORIGINAL_UPLOAD_FILES = os.getenv("STORE_ORIGINAL_UPLOAD_FILES", "false").lower() == "true"

# 资料类型映射（旧版，保留兼容）
DATA_TYPE_MAP = {
    "个人征信": "personal_credit",
    "企业征信": "enterprise_credit",
    "个人流水": "personal_flow",
    "企业流水": "enterprise_flow",
    "财务数据": "finance",
    "抵押物信息": "collateral",
    "水母报告": "jellyfish",
    "个人纳税/公积金": "tax_fund",
}

# 资料类型配置（新版）
# formats: 支持的文件格式 - "pdf", "image", "excel"
# prompt_file: 对应的提示词文件名
# feishu_field: 飞书多维表格中的字段名（必须与飞书表格实际字段名完全一致）
DATA_TYPE_CONFIG = {
    "企业征信提取": {
        "formats": ["pdf", "image"],
        "prompt_file": "企业征信提取_提示词.md",
        "feishu_field": "企业征信报告"  # 飞书表格字段名已拆分
    },
    "个人征信提取": {
        "formats": ["pdf", "image"],
        "prompt_file": "个人征信提取_提示词.md",
        "feishu_field": "个人征信报告"  # 飞书表格字段名已拆分
    },
    "财务数据提取": {
        "formats": ["pdf", "image", "excel"],
        "prompt_file": "财务数据提取_提示词.md",
        "feishu_field": "财务数据"
    },
    "抵押物信息提取": {
        "formats": ["pdf", "image"],
        "prompt_file": "抵押物信息提取提示词.md",
        "feishu_field": "抵押物信息"
    },
    "水母报告提取": {
        "formats": ["pdf", "image"],
        "prompt_file": "水母报告提取提示词.md",
        "feishu_field": "水母报告"
    },
    "个人收入纳税/公积金": {
        "formats": ["pdf", "image"],
        "prompt_file": "个人纳税/公积金提示词.md",
        "feishu_field": "个人收入纳税/公积金"
    },
    "个人流水提取": {
        "formats": ["pdf", "image", "excel"],
        "prompt_file": "个人流水提取_提示词.md",
        "feishu_field": "个人流水"
    },
    "企业流水提取": {
        "formats": ["pdf", "image", "excel"],
        "prompt_file": "企业流水提取_提示词.md",
        "feishu_field": "企业流水"
    },
    "财务数据excel": {
        "formats": ["excel"],
        "prompt_file": "财务数据提取_提示词.md",
        "feishu_field": "财务数据"
    }
}


def get_allowed_extensions(data_type: str) -> list:
    """根据资料类型返回允许的文件扩展名列表
    
    Args:
        data_type: 资料类型名称，如 "企业征信提取"、"企业流水提取" 等
        
    Returns:
        允许的文件扩展名列表，如 ["pdf", "png", "jpg", "jpeg"] 或 ["xlsx", "xls"]
        
    Requirements:
        - 1.2: 根据选择的资料类型限制文件格式
        - 1.3: "财务数据excel" 只接受 Excel 文件 (.xlsx, .xls)
        - 1.4: 其他资料类型接受 PDF 和图片文件 (.pdf, .png, .jpg, .jpeg)
    """
    config = DATA_TYPE_CONFIG.get(data_type, {})
    formats = config.get("formats", ["pdf", "image"])
    
    extensions = []
    if "pdf" in formats:
        extensions.append("pdf")
    if "image" in formats:
        extensions.extend(["png", "jpg", "jpeg"])
    if "excel" in formats:
        extensions.extend(["xlsx", "xls"])
    
    return extensions
