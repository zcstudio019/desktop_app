"""飞书多维表格服务（支持应用凭证和个人访问令牌两种方式）

提供飞书多维表格的增删改查功能。

Requirements:
- 5.1-5.4: 智能合并逻辑
- 5.5: 保存成功后显示记录 ID
- 5.6: 保存失败时显示错误信息并允许重试
"""
import json
import logging
import time
import requests

from config import (
    FEISHU_PERSONAL_BASE_TOKEN, 
    FEISHU_APP_TOKEN, 
    FEISHU_TABLE_ID,
    FEISHU_APP_ID,
    FEISHU_APP_SECRET
)

# 配置日志
logger = logging.getLogger(__name__)


# ==================== 自定义异常类 ====================
# Requirement 5.6: 保存失败时显示错误信息并允许重试

class FeishuServiceError(Exception):
    """飞书服务异常基类
    
    所有飞书相关异常的基类，便于统一捕获和处理。
    
    **Validates: Requirement 5.6**
    """
    pass


class FeishuConfigError(FeishuServiceError):
    """飞书配置错误
    
    当 API 配置不完整或无效时抛出。
    """
    pass


class FeishuAuthError(FeishuServiceError):
    """飞书认证错误
    
    当访问令牌无效或过期时抛出。
    """
    pass


class FeishuAPIError(FeishuServiceError):
    """飞书 API 调用错误
    
    当飞书 API 返回错误时抛出。
    """
    def __init__(self, message: str, error_code: int = None):
        super().__init__(message)
        self.error_code = error_code


class FeishuNetworkError(FeishuServiceError):
    """飞书网络错误
    
    当网络连接失败时抛出。
    """
    pass


class FeishuService:
    """飞书多维表格服务
    
    提供飞书多维表格的增删改查功能。
    支持两种认证方式：
    1. 应用凭证（推荐）：使用 APP_ID + APP_SECRET 获取 tenant_access_token
    2. 个人访问令牌：直接使用 PERSONAL_BASE_TOKEN
    
    优先使用应用凭证方式，如果未配置则回退到个人访问令牌。
    
    **Validates: Requirements 5.1-5.6, 1.3**
    """
    
    # 类级别缓存 tenant_access_token
    _tenant_token = None
    _tenant_token_expire_time = 0
    
    # 飞书文本字段最大长度限制
    # Requirement 1.3: 字段值超过最大长度时截断并记录警告
    MAX_FIELD_LENGTH = 100000
    
    # 需要追加的字段（同类型资料上传时追加而非覆盖）
    APPEND_FIELDS = {
        '个人/企业基本信息', '企业征信报告', '个人征信报告', '企业流水', '个人流水',
        '资金需求', '抵押物信息', '财务数据', '公共记录', '风险提示',
        '还款进度', '逾期提醒', '备注', '水母报告', '个人收入纳税/公积金'
    }
    
    # 直接覆盖的字段
    OVERRIDE_FIELDS = {'序号', '客户复贷潜力标签', '上传时间'}
    
    def __init__(self):
        """初始化飞书服务"""
        self.app_id = FEISHU_APP_ID
        self.app_secret = FEISHU_APP_SECRET
        self.personal_base_token = FEISHU_PERSONAL_BASE_TOKEN
        self.app_token = FEISHU_APP_TOKEN
        self.table_id = FEISHU_TABLE_ID
        
        # 判断使用哪种认证方式
        self.use_app_credential = bool(self.app_id and self.app_secret)
        
        if self.use_app_credential:
            logger.info("飞书服务使用应用凭证方式认证")
        else:
            logger.info("飞书服务使用个人访问令牌方式认证")
    
    def _get_tenant_access_token(self) -> str:
        """获取 tenant_access_token（应用凭证方式）
        
        使用 APP_ID 和 APP_SECRET 获取访问令牌，带缓存机制。
        
        Returns:
            str: tenant_access_token
            
        Raises:
            FeishuAuthError: 获取 token 失败
        """
        # 检查缓存是否有效（提前 5 分钟刷新）
        if (FeishuService._tenant_token and 
            time.time() < FeishuService._tenant_token_expire_time - 300):
            return FeishuService._tenant_token
        
        logger.info("获取新的 tenant_access_token")
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            resp = requests.post(url, json=payload, timeout=10)
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"获取 tenant_access_token 网络错误: {e}")
            raise FeishuNetworkError(f"获取访问令牌失败: {str(e)}")
        except json.JSONDecodeError:
            logger.error(f"获取 tenant_access_token 响应解析失败: {resp.text}")
            raise FeishuAuthError("获取访问令牌失败：服务器返回无效响应")
        
        if data.get("code") != 0:
            error_msg = data.get("msg", "未知错误")
            logger.error(f"获取 tenant_access_token 失败: {error_msg}")
            raise FeishuAuthError(f"获取访问令牌失败: {error_msg}，请检查 APP_ID 和 APP_SECRET")
        
        # 缓存 token
        FeishuService._tenant_token = data.get("tenant_access_token")
        # expire 是秒数，转换为过期时间戳
        expire = data.get("expire", 7200)
        FeishuService._tenant_token_expire_time = time.time() + expire
        
        logger.info(f"tenant_access_token 获取成功，有效期 {expire} 秒")
        return FeishuService._tenant_token
    
    def _check_config(self):
        """检查配置是否完整
        
        Raises:
            FeishuConfigError: 配置不完整
        """
        if self.use_app_credential:
            if not self.app_id:
                raise FeishuConfigError(
                    "飞书配置不完整：缺少 FEISHU_APP_ID，请在配置文件中设置"
                )
            if not self.app_secret:
                raise FeishuConfigError(
                    "飞书配置不完整：缺少 FEISHU_APP_SECRET，请在配置文件中设置"
                )
        else:
            if not self.personal_base_token:
                raise FeishuConfigError(
                    "飞书配置不完整：缺少 PERSONAL_BASE_TOKEN 或 APP_ID/APP_SECRET，请在配置文件中设置"
                )
        
        if not self.app_token:
            raise FeishuConfigError(
                "飞书配置不完整：缺少 APP_TOKEN，请在配置文件中设置"
            )
        if not self.table_id:
            raise FeishuConfigError(
                "飞书配置不完整：缺少 TABLE_ID，请在配置文件中设置"
            )
    
    def _headers(self) -> dict:
        """获取请求头，根据认证方式选择 token"""
        if self.use_app_credential:
            token = self._get_tenant_access_token()
        else:
            token = self.personal_base_token
        
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def _truncate_field(self, field_name: str, value: str) -> str:
        """截断超过最大长度的字段值
        
        当字段值超过 MAX_FIELD_LENGTH 时，截断并记录警告日志。
        
        Args:
            field_name: 字段名称，用于日志记录
            value: 字段值
            
        Returns:
            str: 截断后的字段值（如果超长）或原始值
            
        **Validates: Requirement 1.3**
        """
        if not isinstance(value, str):
            return value
        
        if len(value) > self.MAX_FIELD_LENGTH:
            logger.warning(
                f"字段 '{field_name}' 值超过最大长度限制 ({len(value)} > {self.MAX_FIELD_LENGTH})，"
                f"已截断为 {self.MAX_FIELD_LENGTH} 字符"
            )
            return value[:self.MAX_FIELD_LENGTH]
        
        return value
    
    def _truncate_fields(self, fields: dict) -> dict:
        """截断字典中所有超长的字段值
        
        遍历字段字典，对每个字符串值调用 _truncate_field 进行截断检查。
        
        Args:
            fields: 字段字典
            
        Returns:
            dict: 截断处理后的字段字典
            
        **Validates: Requirement 1.3**
        """
        truncated = {}
        for field_name, value in fields.items():
            truncated[field_name] = self._truncate_field(field_name, value)
        return truncated
    
    def _handle_response(self, resp: requests.Response, operation: str) -> dict:
        """处理 API 响应
        
        Args:
            resp: requests 响应对象
            operation: 操作名称，用于错误消息
            
        Returns:
            dict: 响应数据
            
        Raises:
            FeishuNetworkError: 网络错误
            FeishuAuthError: 认证错误
            FeishuAPIError: API 错误
        """
        try:
            data = resp.json()
        except json.JSONDecodeError:
            logger.error(f"飞书 API 响应解析失败: {resp.text}")
            raise FeishuAPIError(f"{operation}失败：服务器返回无效响应")
        
        code = data.get("code", 0)
        
        if code == 0:
            return data
        
        # 处理错误码
        # Requirement 5.6: 返回描述性错误信息
        error_msg = data.get("msg", "未知错误")
        logger.error(f"飞书 API 错误 ({code}): {error_msg}")
        
        # 常见错误码的友好提示
        error_hints = {
            99991663: "访问令牌无效，请检查 PERSONAL_BASE_TOKEN 配置",
            99991664: "访问令牌已过期，请更新 PERSONAL_BASE_TOKEN",
            99991665: "访问令牌权限不足，请检查权限设置",
            1254040: "多维表格不存在，请检查 APP_TOKEN 配置",
            1254041: "数据表不存在，请检查 TABLE_ID 配置",
            1254042: "记录不存在",
            1254043: "字段不存在，请检查字段名称",
            1254044: "字段类型不匹配",
        }
        
        hint = error_hints.get(code, error_msg)
        
        # Requirement 1.5: 对于字段相关错误，尝试提取字段名
        if code in [1254043, 1254044]:
            # 尝试从错误消息中提取字段名
            # 飞书 API 错误消息格式通常包含字段信息
            field_name = self._extract_field_name_from_error(error_msg)
            if field_name:
                if code == 1254043:
                    hint = f"字段 '{field_name}' 不存在，请检查字段名称是否正确"
                elif code == 1254044:
                    # 尝试提取期望的类型
                    expected_type = self._extract_expected_type_from_error(error_msg)
                    if expected_type:
                        hint = f"字段 '{field_name}' 类型不匹配，期望类型: {expected_type}"
                    else:
                        hint = f"字段 '{field_name}' 类型不匹配，请检查字段值格式"
        
        # 根据错误类型抛出不同异常
        if code in [99991663, 99991664, 99991665]:
            raise FeishuAuthError(f"飞书认证失败: {hint}")
        elif code in [1254040, 1254041]:
            raise FeishuConfigError(f"飞书配置错误: {hint}")
        else:
            raise FeishuAPIError(f"{operation}失败: {hint}", code)
    
    def _extract_field_name_from_error(self, error_msg: str) -> str:
        """从错误消息中提取字段名
        
        飞书 API 错误消息可能包含字段名信息，尝试提取。
        
        Args:
            error_msg: 飞书 API 返回的错误消息
            
        Returns:
            str: 提取到的字段名，未找到则返回空字符串
            
        **Validates: Requirement 1.5**
        """
        import re
        
        # 常见的字段名提取模式
        # 飞书 API 错误消息格式可能包含：
        # - "field: xxx" 或 "字段: xxx"
        # - "field \"xxx\"" 或 "field 'xxx'"
        # - "FieldName: xxx"
        patterns = [
            r'field\s*[:\s]\s*["\']([^"\']+)["\']',  # field: "xxx" 或 field "xxx"
            r'字段\s*[:\s]\s*["\']([^"\']+)["\']',   # 字段: "xxx" 或 字段 "xxx"
            r'(?:field|FieldName)[:\s]+(\w+)',  # field: xxx 或 FieldName: xxx (单词字符)
            r'字段[:\s]+(\S+)',   # 字段: xxx (非空白字符)
            r'"field_name"[:\s]+["\']?([^\s"\']+)["\']?',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, error_msg, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return ""
    
    def _extract_expected_type_from_error(self, error_msg: str) -> str:
        """从错误消息中提取期望的字段类型
        
        Args:
            error_msg: 飞书 API 返回的错误消息
            
        Returns:
            str: 提取到的期望类型，未找到则返回空字符串
            
        **Validates: Requirement 1.5**
        """
        import re
        
        # 常见的类型提取模式
        patterns = [
            r'expected\s+type[:\s]+["\']?(\w+)["\']?',  # expected type: xxx
            r'expected[:\s]+["\']?(\w+)["\']?',  # expected: xxx
            r'期望[:\s]+["\']?(\w+)["\']?',  # 期望: xxx
            r'(?<!expected\s)type[:\s]+["\']?(\w+)["\']?',  # type: xxx (不在 expected 后面)
            r'类型[:\s]+["\']?(\w+)["\']?',  # 类型: xxx
        ]
        
        for pattern in patterns:
            match = re.search(pattern, error_msg, re.IGNORECASE)
            if match:
                type_name = match.group(1).strip()
                # 翻译常见类型名
                type_translations = {
                    'text': '文本',
                    'number': '数字',
                    'datetime': '日期时间',
                    'checkbox': '复选框',
                    'url': '链接',
                    'email': '邮箱',
                    'phone': '电话',
                    'attachment': '附件',
                }
                return type_translations.get(type_name.lower(), type_name)
        
        return ""
    
    def search_records(self, search_text: str) -> list:
        """搜索记录
        
        Args:
            search_text: 搜索关键词
            
        Returns:
            list: 匹配的记录列表
            
        Raises:
            FeishuConfigError: 配置不完整
            FeishuAuthError: 认证失败
            FeishuNetworkError: 网络错误
            FeishuAPIError: API 错误
        """
        logger.info(f"搜索飞书记录: {search_text}")
        print(f"[DEBUG] 搜索飞书记录: {search_text}")
        self._check_config()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/search"
        
        # 构建搜索条件
        # 注意：飞书 API 的 filter 条件中，字段名必须与表格中实际存在的字段完全一致
        # 如果字段不存在会返回 InvalidFilter 错误
        # 使用最基础的字段进行搜索，避免字段名不匹配问题
        # 
        # 策略：先尝试获取所有记录，然后在内存中过滤
        # 这样可以避免字段名不匹配的问题
        payload = {
            "page_size": 100  # 获取更多记录
        }
        
        try:
            resp = requests.post(url, headers=self._headers(), json=payload, timeout=30)
        except requests.exceptions.Timeout:
            logger.error("飞书 API 请求超时")
            raise FeishuNetworkError("飞书服务请求超时，请检查网络连接后重试")
        except requests.exceptions.ConnectionError:
            logger.error("飞书 API 连接失败")
            raise FeishuNetworkError("无法连接到飞书服务，请检查网络连接")
        except requests.exceptions.RequestException as e:
            logger.error(f"飞书 API 请求异常: {e}")
            raise FeishuNetworkError(f"飞书服务请求失败: {str(e)}")
        
        data = self._handle_response(resp, "搜索记录")
        all_records = data.get("data", {}).get("items", []) or []
        print(f"[DEBUG] 飞书返回 {len(all_records)} 条记录")
        
        # 在内存中过滤匹配的记录
        matched_records = []
        search_lower = search_text.lower()
        
        for record in all_records:
            fields = record.get("fields") or {}
            # 检查所有字段值是否包含搜索文本
            for field_name, value in fields.items():
                text_value = self._extract_text_value(value)
                if text_value and search_lower in text_value.lower():
                    matched_records.append(record)
                    print(f"[DEBUG] 匹配记录: 字段={field_name}, 值包含搜索词")
                    break  # 找到匹配就跳出内层循环
        
        logger.info(f"搜索到 {len(matched_records)} 条记录（共 {len(all_records)} 条）")
        print(f"[DEBUG] 搜索到 {len(matched_records)} 条匹配记录")
        return matched_records
    
    def batch_search(self, keywords: list[str], match_all: bool = False) -> list[dict]:
        """批量搜索记录
        
        根据多个关键词搜索飞书多维表格记录，支持两种匹配模式：
        - match_all=False（默认）：任一关键词匹配即返回（并集/OR）
        - match_all=True：所有关键词都匹配才返回（交集/AND）
        
        Args:
            keywords: 关键词列表，不能为空
            match_all: True=所有关键词都匹配（交集），False=任一关键词匹配（并集）
            
        Returns:
            list[dict]: 匹配的记录列表（按 record_id 去重）
            
        Raises:
            ValueError: 关键词列表为空
            FeishuConfigError: 配置不完整
            FeishuAuthError: 认证失败
            FeishuNetworkError: 网络错误
            FeishuAPIError: API 错误
            
        Examples:
            >>> service = FeishuService()
            >>> # 搜索包含 "张三" 或 "李四" 的记录
            >>> records = service.batch_search(["张三", "李四"])
            >>> # 搜索同时包含 "张三" 和 "北京" 的记录
            >>> records = service.batch_search(["张三", "北京"], match_all=True)
        """
        # 参数校验
        if not keywords:
            logger.warning("batch_search 调用时关键词列表为空")
            raise ValueError("关键词列表不能为空")
        
        # 过滤空字符串和 None
        valid_keywords = [kw.strip() for kw in keywords if kw and kw.strip()]
        if not valid_keywords:
            logger.warning("batch_search 过滤后关键词列表为空")
            raise ValueError("关键词列表不能全为空值")
        
        logger.info(f"批量搜索开始，关键词: {valid_keywords}，模式: {'交集(AND)' if match_all else '并集(OR)'}")
        
        # 用于存储结果，key 为 record_id
        results_by_id: dict[str, dict] = {}
        # 记录每个关键词匹配到的 record_id 集合（用于交集模式）
        keyword_matches: list[set[str]] = []
        
        # 逐个关键词搜索
        for keyword in valid_keywords:
            try:
                records = self.search_records(keyword)
                logger.debug(f"关键词 '{keyword}' 搜索到 {len(records)} 条记录")
                
                # 记录本次搜索匹配的 record_id
                matched_ids: set[str] = set()
                
                for record in records:
                    record_id = record.get("record_id")
                    if record_id:
                        matched_ids.add(record_id)
                        # 存储记录（后续搜索到的相同记录会覆盖，但内容相同）
                        results_by_id[record_id] = record
                
                keyword_matches.append(matched_ids)
                
            except (FeishuConfigError, FeishuAuthError, FeishuNetworkError, FeishuAPIError):
                # 飞书相关异常直接向上抛出
                raise
            except Exception as e:
                logger.error(f"搜索关键词 '{keyword}' 时发生未知错误: {e}")
                raise FeishuAPIError(f"批量搜索失败: {str(e)}")
        
        # 根据匹配模式筛选结果
        if match_all:
            # 交集模式：取所有关键词匹配结果的交集
            if keyword_matches:
                final_ids = keyword_matches[0]
                for matched_ids in keyword_matches[1:]:
                    final_ids = final_ids & matched_ids
                logger.info(f"交集模式，最终匹配 {len(final_ids)} 条记录")
            else:
                final_ids = set()
        else:
            # 并集模式：取所有关键词匹配结果的并集
            final_ids = set()
            for matched_ids in keyword_matches:
                final_ids = final_ids | matched_ids
            logger.info(f"并集模式，最终匹配 {len(final_ids)} 条记录（已去重）")
        
        # 构建最终结果列表
        final_records = [results_by_id[rid] for rid in final_ids if rid in results_by_id]
        
        logger.info(f"批量搜索完成，返回 {len(final_records)} 条记录")
        return final_records
    
    def create_record(self, fields: dict) -> dict:
        """创建记录
        
        Args:
            fields: 字段数据
            
        Returns:
            dict: 创建的记录
            
        Raises:
            FeishuConfigError: 配置不完整
            FeishuAuthError: 认证失败
            FeishuNetworkError: 网络错误
            FeishuAPIError: API 错误
        """
        logger.info("创建飞书记录")
        self._check_config()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        
        payload = {"fields": fields}
        
        try:
            resp = requests.post(url, headers=self._headers(), json=payload, timeout=30)
        except requests.exceptions.Timeout:
            logger.error("飞书 API 请求超时")
            raise FeishuNetworkError("飞书服务请求超时，请检查网络连接后重试")
        except requests.exceptions.ConnectionError:
            logger.error("飞书 API 连接失败")
            raise FeishuNetworkError("无法连接到飞书服务，请检查网络连接")
        except requests.exceptions.RequestException as e:
            logger.error(f"飞书 API 请求异常: {e}")
            raise FeishuNetworkError(f"飞书服务请求失败: {str(e)}")
        
        data = self._handle_response(resp, "创建记录")
        record = data.get("data", {}).get("record", {})
        logger.info(f"记录创建成功: {record.get('record_id')}")
        return record
    
    def update_record(self, record_id: str, fields: dict) -> dict:
        """更新记录
        
        Args:
            record_id: 记录 ID
            fields: 要更新的字段数据
            
        Returns:
            dict: 更新后的记录
            
        Raises:
            FeishuConfigError: 配置不完整
            FeishuAuthError: 认证失败
            FeishuNetworkError: 网络错误
            FeishuAPIError: API 错误
        """
        logger.info(f"更新飞书记录: {record_id}")
        self._check_config()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{record_id}"
        
        payload = {"fields": fields}
        
        try:
            resp = requests.put(url, headers=self._headers(), json=payload, timeout=30)
        except requests.exceptions.Timeout:
            logger.error("飞书 API 请求超时")
            raise FeishuNetworkError("飞书服务请求超时，请检查网络连接后重试")
        except requests.exceptions.ConnectionError:
            logger.error("飞书 API 连接失败")
            raise FeishuNetworkError("无法连接到飞书服务，请检查网络连接")
        except requests.exceptions.RequestException as e:
            logger.error(f"飞书 API 请求异常: {e}")
            raise FeishuNetworkError(f"飞书服务请求失败: {str(e)}")
        
        data = self._handle_response(resp, "更新记录")
        record = data.get("data", {}).get("record", {})
        logger.info(f"记录更新成功: {record.get('record_id')}")
        return record
    
    def get_all_records(self, page_size: int = 100) -> list:
        """获取所有记录
        
        Args:
            page_size: 每页记录数
            
        Returns:
            list: 所有记录列表
            
        Raises:
            FeishuConfigError: 配置不完整
            FeishuAuthError: 认证失败
            FeishuNetworkError: 网络错误
            FeishuAPIError: API 错误
        """
        logger.info("获取所有飞书记录")
        self._check_config()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        
        all_records = []
        page_token = None
        
        while True:
            params = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
            except requests.exceptions.Timeout:
                logger.error("飞书 API 请求超时")
                raise FeishuNetworkError("飞书服务请求超时，请检查网络连接后重试")
            except requests.exceptions.ConnectionError:
                logger.error("飞书 API 连接失败")
                raise FeishuNetworkError("无法连接到飞书服务，请检查网络连接")
            except requests.exceptions.RequestException as e:
                logger.error(f"飞书 API 请求异常: {e}")
                raise FeishuNetworkError(f"飞书服务请求失败: {str(e)}")
            
            data = self._handle_response(resp, "获取记录")
            
            # 安全获取 items，处理 data 或 items 为 None 的情况
            data_obj = data.get("data") if data else None
            items = data_obj.get("items") if data_obj else None
            if items:
                all_records.extend(items)
            
            page_token = data_obj.get("page_token") if data_obj else None
            if not page_token:
                break
        
        logger.info(f"获取到 {len(all_records)} 条记录")
        return all_records
    
    def _extract_text_value(self, value) -> str:
        """从飞书字段值中提取纯文本
        
        飞书 API 返回的文本字段格式可能是：
        - 纯字符串: "xxx"
        - 富文本数组: [{"text": "xxx"}, {"text": "yyy"}]
        - 单个对象: {"text": "xxx"}
        
        Args:
            value: 飞书字段值
            
        Returns:
            str: 提取的纯文本
        """
        if value is None:
            return ""
        
        # 如果是字符串，直接返回
        if isinstance(value, str):
            return value.strip()
        
        # 如果是列表（富文本数组）
        if isinstance(value, list):
            texts = []
            for item in value:
                if isinstance(item, dict) and "text" in item:
                    texts.append(str(item["text"]))
                elif isinstance(item, str):
                    texts.append(item)
            return "".join(texts).strip()
        
        # 如果是字典（单个富文本对象）
        if isinstance(value, dict):
            if "text" in value:
                return str(value["text"]).strip()
            # 其他情况，尝试转为字符串
            return str(value).strip()
        
        # 其他类型，转为字符串
        return str(value).strip()
    
    def _merge_field_value(self, field_name: str, old_value, new_value: str) -> str:
        """合并字段值：追加字段用分隔符追加，覆盖字段直接替换
        
        Args:
            field_name: 字段名
            old_value: 旧值（可能是飞书富文本格式）
            new_value: 新值（纯文本）
            
        Returns:
            str: 合并后的值
        """
        # 空值处理 - 使用 _extract_text_value 处理飞书格式
        old_str = self._extract_text_value(old_value)
        new_str = str(new_value).strip() if new_value else ""
        
        if not new_str:
            return old_str
        if not old_str:
            return new_str
        
        # 判断是追加还是覆盖
        if field_name in self.APPEND_FIELDS:
            # 检查新内容是否已存在于旧内容中（避免重复追加）
            if new_str in old_str:
                logger.info(f"字段 {field_name} 新内容已存在，跳过追加")
                return old_str
            # 追加模式：用分隔符连接
            merged = f"{old_str}\n\n=== 最新更新 ===\n{new_str}"
            logger.info(f"字段 {field_name} 追加合并")
            return merged
        else:
            # 覆盖模式：检查内容是否相同，相同则跳过
            if new_str == old_str or new_str in old_str:
                logger.info(f"字段 {field_name} 内容相同，跳过更新")
                return old_str
            logger.info(f"字段 {field_name} 覆盖更新")
            return new_str
    
    def _search_in_record_fields(self, record: dict, search_name: str) -> bool:
        """在记录的多个字段中搜索姓名
        
        用于支持个人征信和企业征信的合并：
        - 个人征信的客户名称是个人姓名
        - 企业征信的客户名称是企业名称，但法定代表人信息在字段内容中
        - 通过在多个字段中搜索，可以匹配到法定代表人与个人姓名相同的记录
        
        搜索范围（参考智能合并节点）：
        - 个人/企业基本信息
        - 企业/个人征信
        - 企业/个人流水
        - 财务数据
        - 抵押物信息
        - 水母报告
        - 个人收入纳税/公积金
        - 逾期提醒
        
        Args:
            record: 飞书记录
            search_name: 要搜索的姓名
            
        Returns:
            bool: 是否在记录字段中找到匹配的姓名
        """
        if not search_name:
            return False
        
        fields = record.get("fields") or {}
        
        # 定义搜索字段列表（参考智能合并节点）
        search_fields = [
            '个人/企业基本信息',
            '企业征信报告',
            '个人征信报告',
            '企业流水',
            '个人流水',
            '财务数据',
            '抵押物信息',
            '水母报告',
            '个人收入纳税/公积金',
            '逾期提醒'
        ]
        
        # 合并所有字段的文本内容
        # 踩坑点 #22: 飞书字段可能是富文本格式，需要用 _extract_text_value 提取
        search_texts = []
        for field_name in search_fields:
            field_value = fields.get(field_name)
            if field_value:
                text = self._extract_text_value(field_value)
                if text:
                    search_texts.append(text)
        
        # 合并搜索范围
        combined_text = " ".join(search_texts)
        
        # 检查姓名是否在合并文本中
        if search_name in combined_text:
            logger.info(f"在记录字段中找到匹配姓名: {search_name}")
            return True
        
        return False
    
    def _find_matching_record_in_all(self, search_name: str) -> dict:
        """在所有记录中搜索匹配的记录（增强匹配）
        
        当按名字直接搜索没找到时，获取所有记录并在多个字段中搜索。
        这样可以支持个人征信和企业征信的合并（通过法定代表人匹配）。
        
        Args:
            search_name: 要搜索的姓名
            
        Returns:
            dict: 匹配的记录，未找到返回 None
            
        Raises:
            FeishuConfigError, FeishuAuthError, FeishuNetworkError, FeishuAPIError
        """
        if not search_name:
            return None
        
        logger.info(f"增强搜索：在所有记录的多个字段中搜索 '{search_name}'")
        
        # 获取所有记录
        all_records = self.get_all_records()
        
        if not all_records:
            logger.info("没有任何记录")
            return None
        
        logger.info(f"获取到 {len(all_records)} 条记录，开始字段内搜索")
        
        # 在每条记录的多个字段中搜索
        for record in all_records:
            if self._search_in_record_fields(record, search_name):
                record_id = record.get("record_id", "")
                logger.info(f"增强搜索找到匹配记录: {record_id}")
                return record
        
        logger.info(f"增强搜索未找到匹配记录")
        return None
    
    def smart_merge(self, name: str, new_fields: dict) -> dict:
        """智能合并：搜索匹配记录，找到则追加/更新，否则新增
        
        合并规则：
        - APPEND_FIELDS 中的字段：新内容追加到旧内容后面，用 "=== 最新更新 ===" 分隔
        - OVERRIDE_FIELDS 中的字段：新值直接覆盖旧值
        - 其他字段：有新值则覆盖，无新值保留旧值
        
        增强功能：
        - 在 API 调用前验证字段长度，超长字段自动截断
        - 返回详细的错误信息，包含字段名
        - 支持个人征信和企业征信的合并（通过法定代表人匹配）
        
        匹配逻辑（参考智能合并节点）：
        1. 先按客户名称直接搜索
        2. 如果没找到，在所有记录的多个字段中搜索（支持法定代表人匹配）
        
        Args:
            name: 客户名称，用于搜索匹配记录
            new_fields: 要保存的字段数据
            
        Returns:
            dict: 包含以下字段：
                - success (bool): 操作是否成功
                - record_id (str|None): 记录 ID
                - is_update (bool): 是否为更新操作（False 表示新建）
                - error_message (str|None): 错误信息
                
        **Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 1.3**
        """
        logger.info(f"智能合并，客户名称: {name or '(空)'}")
        
        # Requirement 1.3: 在 API 调用前截断超长字段
        new_fields = self._truncate_fields(new_fields)
        
        # 检查 name 是否为空、None 或 "无"
        # Requirement 5.4: 当 name 为空或 "无" 时直接创建新记录
        if not name or name.strip() == "" or name == "无":
            logger.info("客户名称为空，直接创建新记录")
            try:
                record = self.create_record(new_fields)
                return {
                    "success": True,
                    "record_id": record.get("record_id"),
                    "is_update": False,
                    "error_message": None
                }
            except FeishuConfigError as e:
                logger.error(f"飞书配置错误: {e}")
                return {
                    "success": False,
                    "record_id": None,
                    "is_update": False,
                    "error_message": f"配置错误: {str(e)}"
                }
            except FeishuAuthError as e:
                logger.error(f"飞书认证错误: {e}")
                return {
                    "success": False,
                    "record_id": None,
                    "is_update": False,
                    "error_message": f"认证失败: {str(e)}"
                }
            except FeishuNetworkError as e:
                logger.error(f"飞书网络错误: {e}")
                return {
                    "success": False,
                    "record_id": None,
                    "is_update": False,
                    "error_message": f"网络错误: {str(e)}"
                }
            except FeishuAPIError as e:
                logger.error(f"飞书 API 错误: {e}")
                return {
                    "success": False,
                    "record_id": None,
                    "is_update": False,
                    "error_message": str(e)
                }
            except Exception as e:
                logger.error(f"创建记录未知错误: {e}")
                return {
                    "success": False,
                    "record_id": None,
                    "is_update": False,
                    "error_message": f"创建记录失败: {str(e)}"
                }
        
        # 有名称，先搜索匹配记录
        # Requirement 5.1: 根据客户名称搜索现有记录
        try:
            records = self.search_records(name)
            
            # 增强匹配：如果按名字直接搜索没找到，尝试在所有记录的多个字段中搜索
            # 这样可以支持个人征信和企业征信的合并（通过法定代表人匹配）
            if not records:
                logger.info(f"按名字直接搜索未找到，尝试增强搜索")
                matched_record = self._find_matching_record_in_all(name)
                if matched_record:
                    records = [matched_record]
                    
        except FeishuConfigError as e:
            logger.error(f"飞书配置错误: {e}")
            return {
                "success": False,
                "record_id": None,
                "is_update": False,
                "error_message": f"配置错误: {str(e)}"
            }
        except FeishuAuthError as e:
            logger.error(f"飞书认证错误: {e}")
            return {
                "success": False,
                "record_id": None,
                "is_update": False,
                "error_message": f"认证失败: {str(e)}"
            }
        except FeishuNetworkError as e:
            logger.error(f"飞书网络错误: {e}")
            return {
                "success": False,
                "record_id": None,
                "is_update": False,
                "error_message": f"网络错误: {str(e)}"
            }
        except FeishuAPIError as e:
            logger.error(f"飞书 API 错误: {e}")
            return {
                "success": False,
                "record_id": None,
                "is_update": False,
                "error_message": f"搜索记录失败: {str(e)}"
            }
        except Exception as e:
            logger.error(f"搜索记录未知错误: {e}")
            return {
                "success": False,
                "record_id": None,
                "is_update": False,
                "error_message": f"搜索记录失败: {str(e)}"
            }
        
        if records:
            # Requirement 5.2: 找到匹配记录，执行追加/更新
            record_id = records[0].get("record_id")
            old_fields = records[0].get("fields", {})
            logger.info(f"找到匹配记录: {record_id}，执行追加合并")
            
            # 合并字段：追加字段追加，覆盖字段覆盖
            merged_fields = {}
            has_changes = False
            for field_name, new_value in new_fields.items():
                old_value = old_fields.get(field_name, "")
                merged_value = self._merge_field_value(field_name, old_value, new_value)
                merged_fields[field_name] = merged_value
                # 检查是否有实际变化
                old_str = self._extract_text_value(old_value)
                if merged_value != old_str:
                    has_changes = True
            
            # 如果没有任何变化，跳过更新
            if not has_changes:
                logger.info(f"所有字段内容相同，跳过更新")
                return {
                    "success": True,
                    "record_id": record_id,
                    "is_update": False,  # 标记为未更新
                    "error_message": "数据已存在，无需重复上传",
                    "skipped": True  # 新增标记，表示跳过
                }
            
            # Requirement 1.3: 合并后再次截断（追加可能导致超长）
            merged_fields = self._truncate_fields(merged_fields)
            
            try:
                record = self.update_record(record_id, merged_fields)
                return {
                    "success": True,
                    "record_id": record.get("record_id"),
                    "is_update": True,
                    "error_message": None
                }
            except FeishuConfigError as e:
                logger.error(f"飞书配置错误: {e}")
                return {
                    "success": False,
                    "record_id": record_id,
                    "is_update": True,
                    "error_message": f"配置错误: {str(e)}"
                }
            except FeishuAuthError as e:
                logger.error(f"飞书认证错误: {e}")
                return {
                    "success": False,
                    "record_id": record_id,
                    "is_update": True,
                    "error_message": f"认证失败: {str(e)}"
                }
            except FeishuNetworkError as e:
                logger.error(f"飞书网络错误: {e}")
                return {
                    "success": False,
                    "record_id": record_id,
                    "is_update": True,
                    "error_message": f"网络错误: {str(e)}"
                }
            except FeishuAPIError as e:
                logger.error(f"飞书 API 错误: {e}")
                return {
                    "success": False,
                    "record_id": record_id,
                    "is_update": True,
                    "error_message": str(e)
                }
            except Exception as e:
                logger.error(f"更新记录未知错误: {e}")
                return {
                    "success": False,
                    "record_id": record_id,
                    "is_update": True,
                    "error_message": f"更新记录失败: {str(e)}"
                }
        else:
            # Requirement 5.3: 没找到匹配记录，新增
            logger.info("未找到匹配记录，创建新记录")
            try:
                record = self.create_record(new_fields)
                return {
                    "success": True,
                    "record_id": record.get("record_id"),
                    "is_update": False,
                    "error_message": None
                }
            except FeishuConfigError as e:
                logger.error(f"飞书配置错误: {e}")
                return {
                    "success": False,
                    "record_id": None,
                    "is_update": False,
                    "error_message": f"配置错误: {str(e)}"
                }
            except FeishuAuthError as e:
                logger.error(f"飞书认证错误: {e}")
                return {
                    "success": False,
                    "record_id": None,
                    "is_update": False,
                    "error_message": f"认证失败: {str(e)}"
                }
            except FeishuNetworkError as e:
                logger.error(f"飞书网络错误: {e}")
                return {
                    "success": False,
                    "record_id": None,
                    "is_update": False,
                    "error_message": f"网络错误: {str(e)}"
                }
            except FeishuAPIError as e:
                logger.error(f"飞书 API 错误: {e}")
                return {
                    "success": False,
                    "record_id": None,
                    "is_update": False,
                    "error_message": str(e)
                }
            except Exception as e:
                logger.error(f"创建记录未知错误: {e}")
                return {
                    "success": False,
                    "record_id": None,
                    "is_update": False,
                    "error_message": f"创建记录失败: {str(e)}"
                }
