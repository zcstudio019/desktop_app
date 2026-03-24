"""飞书知识库服务

提供飞书知识库文档的读取功能，用于获取产品库内容。

使用独立的应用凭证访问产品库，与主应用的飞书凭证分离。

Requirements:
- 2.2: 获取产品库文档内容
- 2.9: 使用独立的应用凭证
"""
import json
import logging
import re
import time
from typing import Optional
import requests

from config import (
    WIKI_APP_ID,
    WIKI_APP_SECRET,
)

# 配置日志
logger = logging.getLogger(__name__)


# ==================== 自定义异常类 ====================

class WikiServiceError(Exception):
    """知识库服务异常基类
    
    所有知识库相关异常的基类，便于统一捕获和处理。
    """
    pass


class WikiConfigError(WikiServiceError):
    """知识库配置错误
    
    当 API 配置不完整或无效时抛出。
    """
    pass


class WikiAuthError(WikiServiceError):
    """知识库认证错误
    
    当访问令牌无效或过期时抛出。
    """
    pass


class WikiAPIError(WikiServiceError):
    """知识库 API 调用错误
    
    当飞书 API 返回错误时抛出。
    """
    def __init__(self, message: str, error_code: int = None):
        super().__init__(message)
        self.error_code = error_code


class WikiNetworkError(WikiServiceError):
    """知识库网络错误
    
    当网络连接失败时抛出。
    """
    pass


class WikiService:
    """飞书知识库文档服务
    
    用于访问产品库文档，使用独立的应用凭证。
    
    产品库应用凭证统一从配置文件读取，不在源码中硬编码展示。
    
    **Validates: Requirements 2.2, 2.9**
    """
    
    # 类级别缓存 tenant_access_token
    _tenant_token: Optional[str] = None
    _tenant_token_expire_time: float = 0
    
    # 产品库文档 URL
    # 2026-03-03 更新：企业信用贷和抵押贷拆分为独立文档
    PRODUCT_DOCS = {
        "personal": "https://u3hcz5ydrr.feishu.cn/wiki/Us1tw344ZitzAjkiTp8c8smRntc",
        "enterprise_credit": "https://u3hcz5ydrr.feishu.cn/wiki/RoVMwF7JRiX6n6ke4u9cRAqGnSA",
        "enterprise_mortgage": "https://u3hcz5ydrr.feishu.cn/wiki/QAsRw6bYriSPThkEM4EcpLnxnCb"
    }
    
    # API 基础 URL
    BASE_URL = "https://open.feishu.cn"
    
    def __init__(self):
        """初始化知识库服务
        
        使用产品库专用的应用凭证。
        """
        self.app_id = WIKI_APP_ID
        self.app_secret = WIKI_APP_SECRET
        
        logger.info("知识库服务初始化完成")
    
    def _check_config(self):
        """检查配置是否完整
        
        Raises:
            WikiConfigError: 配置不完整
        """
        if not self.app_id:
            raise WikiConfigError(
                "产品库配置不完整：缺少 WIKI_APP_ID，请在配置文件中设置"
            )
        if not self.app_secret:
            raise WikiConfigError(
                "产品库配置不完整：缺少 WIKI_APP_SECRET，请在配置文件中设置"
            )
    
    def _get_tenant_access_token(self) -> str:
        """获取 tenant_access_token
        
        使用产品库应用的 APP_ID 和 APP_SECRET 获取访问令牌，带缓存机制。
        
        Returns:
            str: tenant_access_token
            
        Raises:
            WikiConfigError: 配置不完整
            WikiAuthError: 获取 token 失败
            WikiNetworkError: 网络错误
        """
        self._check_config()
        
        # 检查缓存是否有效（提前 5 分钟刷新）
        if (WikiService._tenant_token and 
            time.time() < WikiService._tenant_token_expire_time - 300):
            logger.debug("使用缓存的 tenant_access_token")
            return WikiService._tenant_token
        
        logger.info("获取新的产品库 tenant_access_token")
        
        url = f"{self.BASE_URL}/open-apis/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            resp = requests.post(url, json=payload, timeout=10)
            data = resp.json()
        except requests.exceptions.Timeout:
            logger.error("获取 tenant_access_token 超时")
            raise WikiNetworkError("获取产品库访问令牌超时，请检查网络连接")
        except requests.exceptions.ConnectionError:
            logger.error("获取 tenant_access_token 连接失败")
            raise WikiNetworkError("无法连接到飞书服务，请检查网络连接")
        except requests.exceptions.RequestException as e:
            logger.error(f"获取 tenant_access_token 网络错误: {e}")
            raise WikiNetworkError(f"获取产品库访问令牌失败: {str(e)}")
        except json.JSONDecodeError:
            logger.error(f"获取 tenant_access_token 响应解析失败: {resp.text}")
            raise WikiAuthError("获取产品库访问令牌失败：服务器返回无效响应")
        
        if data.get("code") != 0:
            error_msg = data.get("msg", "未知错误")
            logger.error(f"获取 tenant_access_token 失败: {error_msg}")
            raise WikiAuthError(
                f"产品库认证失败: {error_msg}，请检查 WIKI_APP_ID 和 WIKI_APP_SECRET"
            )
        
        # 缓存 token
        WikiService._tenant_token = data.get("tenant_access_token")
        # expire 是秒数，转换为过期时间戳
        expire = data.get("expire", 7200)
        WikiService._tenant_token_expire_time = time.time() + expire
        
        logger.info(f"产品库 tenant_access_token 获取成功，有效期 {expire} 秒")
        return WikiService._tenant_token
    
    def _headers(self) -> dict:
        """获取请求头"""
        token = self._get_tenant_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def _extract_node_token(self, wiki_url: str) -> str:
        """从知识库 URL 中提取 node_token
        
        URL 格式: https://xxx.feishu.cn/wiki/{node_token}
        
        Args:
            wiki_url: 知识库文档 URL
            
        Returns:
            str: node_token
            
        Raises:
            ValueError: URL 格式无效
        """
        if not wiki_url:
            raise ValueError("知识库 URL 不能为空")
        
        # 匹配 /wiki/{node_token} 格式
        # node_token 通常是字母数字组合
        pattern = r'/wiki/([a-zA-Z0-9]+)'
        match = re.search(pattern, wiki_url)
        
        if not match:
            raise ValueError(f"无效的知识库 URL 格式: {wiki_url}")
        
        node_token = match.group(1)
        logger.debug(f"从 URL 提取 node_token: {node_token}")
        return node_token
    
    def _get_node_info(self, node_token: str) -> dict:
        """获取知识库节点信息
        
        API: GET /open-apis/wiki/v2/spaces/get_node
        
        Args:
            node_token: 节点 token
            
        Returns:
            dict: 节点信息，包含 obj_token 和 obj_type
            
        Raises:
            WikiNetworkError: 网络错误
            WikiAPIError: API 错误
        """
        url = f"{self.BASE_URL}/open-apis/wiki/v2/spaces/get_node"
        params = {"token": node_token}
        
        try:
            resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
            data = resp.json()
        except requests.exceptions.Timeout:
            logger.error("获取节点信息超时")
            raise WikiNetworkError("获取产品库文档信息超时，请稍后重试")
        except requests.exceptions.ConnectionError:
            logger.error("获取节点信息连接失败")
            raise WikiNetworkError("无法连接到飞书服务，请检查网络连接")
        except requests.exceptions.RequestException as e:
            logger.error(f"获取节点信息网络错误: {e}")
            raise WikiNetworkError(f"获取产品库文档信息失败: {str(e)}")
        except json.JSONDecodeError:
            logger.error(f"获取节点信息响应解析失败: {resp.text}")
            raise WikiAPIError("获取产品库文档信息失败：服务器返回无效响应")
        
        code = data.get("code", 0)
        if code != 0:
            error_msg = data.get("msg", "未知错误")
            logger.error(f"获取节点信息失败 ({code}): {error_msg}")
            
            # 常见错误码处理
            if code == 99991663 or code == 99991664:
                raise WikiAuthError(f"产品库认证失败: {error_msg}")
            elif code == 131001:
                raise WikiAPIError(f"产品库文档不存在: {error_msg}", code)
            else:
                raise WikiAPIError(f"获取产品库文档信息失败: {error_msg}", code)
        
        node = data.get("data", {}).get("node", {})
        logger.info(f"获取节点信息成功: obj_type={node.get('obj_type')}")
        return node
    
    def _get_document_raw_content(self, document_id: str) -> str:
        """获取文档原始内容
        
        API: GET /open-apis/docx/v1/documents/{document_id}/raw_content
        
        Args:
            document_id: 文档 ID (obj_token)
            
        Returns:
            str: 文档纯文本内容
            
        Raises:
            WikiNetworkError: 网络错误
            WikiAPIError: API 错误
        """
        url = f"{self.BASE_URL}/open-apis/docx/v1/documents/{document_id}/raw_content"
        
        try:
            resp = requests.get(url, headers=self._headers(), timeout=60)
            data = resp.json()
        except requests.exceptions.Timeout:
            logger.error("获取文档内容超时")
            raise WikiNetworkError("获取产品库文档内容超时，请稍后重试")
        except requests.exceptions.ConnectionError:
            logger.error("获取文档内容连接失败")
            raise WikiNetworkError("无法连接到飞书服务，请检查网络连接")
        except requests.exceptions.RequestException as e:
            logger.error(f"获取文档内容网络错误: {e}")
            raise WikiNetworkError(f"获取产品库文档内容失败: {str(e)}")
        except json.JSONDecodeError:
            logger.error(f"获取文档内容响应解析失败: {resp.text}")
            raise WikiAPIError("获取产品库文档内容失败：服务器返回无效响应")
        
        code = data.get("code", 0)
        if code != 0:
            error_msg = data.get("msg", "未知错误")
            logger.error(f"获取文档内容失败 ({code}): {error_msg}")
            
            if code == 99991663 or code == 99991664:
                raise WikiAuthError(f"产品库认证失败: {error_msg}")
            elif code == 230002:
                raise WikiAPIError(f"产品库文档不存在: {error_msg}", code)
            else:
                raise WikiAPIError(f"获取产品库文档内容失败: {error_msg}", code)
        
        content = data.get("data", {}).get("content", "")
        logger.info(f"获取文档内容成功，长度: {len(content)} 字符")
        return content
    
    def get_document_content(self, wiki_url: str) -> str:
        """获取知识库文档内容
        
        从飞书知识库 URL 获取文档的纯文本内容。
        
        流程：
        1. 从 URL 提取 node_token
        2. 调用 get_node API 获取 obj_token
        3. 调用 raw_content API 获取文档内容
        
        Args:
            wiki_url: 知识库文档 URL
            
        Returns:
            str: 文档纯文本内容
            
        Raises:
            ValueError: URL 格式无效
            WikiConfigError: 配置不完整
            WikiAuthError: 认证失败
            WikiNetworkError: 网络错误
            WikiAPIError: API 错误
            
        **Validates: Requirement 2.2**
        """
        logger.info(f"获取知识库文档: {wiki_url}")
        
        # 1. 提取 node_token
        node_token = self._extract_node_token(wiki_url)
        
        # 2. 获取节点信息
        node_info = self._get_node_info(node_token)
        obj_token = node_info.get("obj_token")
        obj_type = node_info.get("obj_type")
        
        if not obj_token:
            raise WikiAPIError("无法获取文档 ID，请检查文档权限")
        
        # 3. 根据文档类型获取内容
        # obj_type: doc=旧版文档, docx=新版文档, sheet=表格, bitable=多维表格
        if obj_type in ["doc", "docx"]:
            content = self._get_document_raw_content(obj_token)
        else:
            # 其他类型暂不支持
            raise WikiAPIError(
                f"不支持的文档类型: {obj_type}，目前仅支持文档类型"
            )
        
        return content
    
    def get_personal_products(self) -> str:
        """获取个人贷款产品库
        
        Returns:
            str: 个人贷款产品库内容
            
        Raises:
            WikiServiceError: 获取失败
            
        **Validates: Requirement 2.3**
        """
        logger.info("获取个人贷款产品库")
        return self.get_document_content(self.PRODUCT_DOCS["personal"])
    
    def get_enterprise_products(self) -> str:
        """获取企业贷款产品库
        
        获取企业贷款产品库（包含信用贷和抵押贷）。
        
        注意：如果 enterprise_credit 和 enterprise_mortgage 指向同一文档，
        只获取一次避免重复。
        
        Returns:
            str: 企业贷款产品库内容
            
        Raises:
            WikiServiceError: 获取失败
            
        **Validates: Requirement 2.4**
        """
        logger.info("获取企业贷款产品库")
        
        credit_url = self.PRODUCT_DOCS["enterprise_credit"]
        mortgage_url = self.PRODUCT_DOCS["enterprise_mortgage"]
        
        # 如果两个 URL 相同，只获取一次（文档包含两种产品）
        if credit_url == mortgage_url:
            logger.info("企业信用贷和抵押贷使用同一文档")
            content = self.get_document_content(credit_url)
            logger.info(f"企业产品库获取完成，长度: {len(content)} 字符")
            return content
        
        # 否则分别获取并合并
        credit_content = self.get_document_content(credit_url)
        mortgage_content = self.get_document_content(mortgage_url)
        
        merged_content = f"""=== 企业信用贷产品库 ===

{credit_content}

=== 企业抵押贷产品库 ===

{mortgage_content}
"""
        
        logger.info(f"企业产品库合并完成，总长度: {len(merged_content)} 字符")
        return merged_content
