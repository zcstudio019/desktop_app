"""增强的 JSON 解析器 - 处理 AI 输出的非标准 JSON

Property 7: Enhanced JSON Parsing
*For any* JSON string with common formatting issues (trailing commas, comments, 
unquoted keys), the enhanced JSON parser SHALL either successfully parse it or 
return None, never raising an unhandled exception.

Validates: Requirements 4.4
"""
import json
import re
import logging
from typing import Union, Optional

# 配置日志
logger = logging.getLogger(__name__)


def clean_json_string(content: str) -> str:
    """清理 JSON 字符串，移除注释和修复常见问题
    
    Args:
        content: 可能包含注释的 JSON 字符串
        
    Returns:
        清理后的 JSON 字符串
    """
    if not content:
        return "{}"
    
    # 移除 Markdown 代码块
    if "```json" in content:
        start = content.find("```json") + 7
        end = content.rfind("```")
        if end > start:
            content = content[start:end]
    elif "```" in content:
        start = content.find("```") + 3
        end = content.rfind("```")
        if end > start:
            content = content[start:end]
    
    # 移除多行注释 /* ... */
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    
    # 移除单行注释
    lines = content.split('\n')
    cleaned_lines = []
    
    for line in lines:
        # 跳过纯注释行
        stripped = line.strip()
        if stripped.startswith('//'):
            continue
        
        # 移除行内注释（注意不要移除字符串内的 //）
        in_string = False
        escaped = False
        result = []
        
        for i, ch in enumerate(line):
            if escaped:
                result.append(ch)
                escaped = False
                continue
            
            if ch == '\\' and in_string:
                result.append(ch)
                escaped = True
                continue
            
            if ch == '"':
                in_string = not in_string
                result.append(ch)
                continue
            
            if not in_string and ch == '/' and i + 1 < len(line) and line[i + 1] == '/':
                break  # 遇到行内注释，截断
            
            result.append(ch)
        
        cleaned_lines.append(''.join(result))
    
    content = '\n'.join(cleaned_lines)
    
    # 修复尾部逗号 (在 ] 或 } 之前的逗号)
    content = re.sub(r',(\s*[\]\}])', r'\1', content)
    
    return content.strip()


def _fix_single_quotes(content: str) -> str:
    """将单引号转换为双引号
    
    Args:
        content: JSON 字符串
        
    Returns:
        修复后的字符串
    """
    result = []
    i = 0
    in_double_string = False
    in_single_string = False
    escaped = False
    
    while i < len(content):
        ch = content[i]
        
        if escaped:
            result.append(ch)
            escaped = False
            i += 1
            continue
        
        if ch == '\\':
            result.append(ch)
            escaped = True
            i += 1
            continue
        
        if ch == '"' and not in_single_string:
            in_double_string = not in_double_string
            result.append(ch)
            i += 1
            continue
        
        if ch == "'" and not in_double_string:
            in_single_string = not in_single_string
            result.append('"')  # 转换为双引号
            i += 1
            continue
        
        result.append(ch)
        i += 1
    
    return ''.join(result)


def _strip_whitespace(text: str) -> str:
    """彻底清理前导和尾随空白字符
    
    处理各种空白字符：空格、制表符、换行符、回车符等
    
    Args:
        text: 输入字符串
        
    Returns:
        清理后的字符串
    """
    if not text:
        return text
    
    # 使用 strip() 移除所有前导和尾随空白
    result = text.strip()
    
    # 额外处理：移除 BOM 字符
    if result.startswith('\ufeff'):
        result = result[1:]
    
    # 移除可能的零宽字符
    result = result.strip('\u200b\u200c\u200d\ufeff')
    
    return result


def _fix_truncated_json(text: str) -> str:
    """尝试修复截断的 JSON
    
    通过添加缺失的闭合括号来修复不完整的 JSON。
    支持深度嵌套结构的修复。
    
    Algorithm:
    1. Track bracket stack while parsing
    2. Handle string escaping correctly
    3. If in_string at end, close the string
    4. Remove incomplete trailing tokens (,:")
    5. Add missing closing brackets in reverse order
    
    Args:
        text: 可能被截断的 JSON 字符串
        
    Returns:
        修复后的 JSON 字符串
    """
    if not text:
        logger.debug("_fix_truncated_json: 输入为空")
        return text
    
    original_text = text
    
    # 第一遍扫描：检测是否在字符串中被截断
    in_string = False
    escaped = False
    
    for ch in text:
        if escaped:
            escaped = False
            continue
        
        if ch == '\\' and in_string:
            escaped = True
            continue
        
        if ch == '"':
            in_string = not in_string
    
    # 如果在字符串中被截断，先闭合字符串
    if in_string:
        text += '"'
        logger.debug("_fix_truncated_json: 添加缺失的字符串闭合引号")
    
    # 移除末尾不完整的部分
    text = text.rstrip()
    
    # 循环移除末尾的不完整标记
    max_iterations = 100  # 防止无限循环
    iteration = 0
    
    while text and iteration < max_iterations:
        iteration += 1
        
        # 重新检查当前状态
        in_string = False
        escaped = False
        for ch in text:
            if escaped:
                escaped = False
                continue
            if ch == '\\' and in_string:
                escaped = True
                continue
            if ch == '"':
                in_string = not in_string
        
        # 如果在字符串内，不要移除
        if in_string:
            break
            
        last_char = text[-1]
        
        if last_char == ',':
            # 移除末尾逗号
            text = text[:-1].rstrip()
            logger.debug("_fix_truncated_json: 移除末尾逗号")
        elif last_char == ':':
            # 移除末尾冒号和前面的键名
            text = text[:-1].rstrip()
            logger.debug("_fix_truncated_json: 移除末尾冒号")
            # 继续移除前面的键名（字符串）
            if text and text[-1] == '"':
                # 找到键名的开始引号
                text = text[:-1]  # 移除结束引号
                # 向前找开始引号
                quote_pos = text.rfind('"')
                if quote_pos >= 0:
                    text = text[:quote_pos].rstrip()
                    logger.debug("_fix_truncated_json: 移除不完整的键名")
                    # 如果前面还有逗号，也移除
                    if text and text[-1] == ',':
                        text = text[:-1].rstrip()
                        logger.debug("_fix_truncated_json: 移除键名前的逗号")
        elif last_char == '\\':
            # 移除末尾的转义字符
            text = text[:-1].rstrip()
            logger.debug("_fix_truncated_json: 移除末尾转义字符")
        else:
            break
    
    # 重新计算未闭合的括号
    stack = []
    in_string = False
    escaped = False
    
    for ch in text:
        if escaped:
            escaped = False
            continue
        
        if ch == '\\' and in_string:
            escaped = True
            continue
        
        if ch == '"':
            in_string = not in_string
            continue
        
        if not in_string:
            if ch in '{[':
                stack.append(ch)
            elif ch == '}' and stack and stack[-1] == '{':
                stack.pop()
            elif ch == ']' and stack and stack[-1] == '[':
                stack.pop()
    
    # 添加缺失的闭合括号
    closing_brackets = []
    for bracket in reversed(stack):
        if bracket == '{':
            closing_brackets.append('}')
        elif bracket == '[':
            closing_brackets.append(']')
    
    if closing_brackets:
        text += ''.join(closing_brackets)
        logger.debug(f"_fix_truncated_json: 添加缺失的闭合括号 {''.join(closing_brackets)}")
    
    if text != original_text:
        logger.debug(f"_fix_truncated_json: 修复完成，原长度={len(original_text)}, 新长度={len(text)}")
    
    return text


def parse_json(json_str: str) -> Union[dict, list, None]:
    """增强的 JSON 解析器
    
    解析 JSON 字符串，支持处理 AI 输出的非标准格式：
    - 前导/尾随空白和换行符
    - 尾部逗号
    - 单行注释 (// ...)
    - 多行注释 (/* ... */)
    - 单引号字符串
    - Markdown 代码块包裹
    - 截断的 JSON（缺失闭合括号）
    
    Property 7: Enhanced JSON Parsing
    *For any* JSON string with common formatting issues, this function SHALL 
    either successfully parse it or return None, never raising an unhandled exception.
    
    **Validates: Requirements 1.1, 1.2, 4.4**
    
    Args:
        json_str: JSON 字符串或包含 JSON 的文本
        
    Returns:
        解析后的 dict 或 list，解析失败返回 None
    """
    # 处理空输入
    if not json_str:
        logger.debug("parse_json: 输入为空")
        return None
    
    # 如果已经是 dict 或 list，直接返回
    if isinstance(json_str, (dict, list)):
        return json_str
    
    # 确保是字符串
    if not isinstance(json_str, str):
        logger.debug(f"parse_json: 输入类型不是字符串: {type(json_str)}")
        return None
    
    # 【增强】彻底清理前导和尾随空白字符（包括换行符）
    json_str = _strip_whitespace(json_str)
    if not json_str:
        logger.debug("parse_json: 清理空白后为空")
        return None
    
    # 记录原始输入的前100个字符用于调试
    preview = json_str[:100] + ('...' if len(json_str) > 100 else '')
    logger.debug(f"parse_json: 开始解析，输入预览: {repr(preview)}")
    
    # 1. 首先尝试标准解析
    try:
        result = json.loads(json_str)
        logger.debug("parse_json: 标准解析成功")
        # 如果有 output 字段，递归解析
        if isinstance(result, dict) and "output" in result:
            inner = parse_json(result["output"])
            if inner is not None:
                return inner
        return result
    except json.JSONDecodeError as e:
        logger.debug(f"parse_json: 标准解析失败: {e}")
    except Exception as e:
        logger.debug(f"parse_json: 标准解析异常: {e}")
    
    # 2. 应用修复后再解析
    try:
        fixed = json_str
        
        # 清理注释和 Markdown 代码块
        fixed = clean_json_string(fixed)
        fixed = _strip_whitespace(fixed)
        
        # 尝试解析清理后的内容
        try:
            result = json.loads(fixed)
            logger.debug("parse_json: 清理后解析成功")
            if isinstance(result, dict) and "output" in result:
                inner = parse_json(result["output"])
                if inner is not None:
                    return inner
            return result
        except json.JSONDecodeError as e:
            logger.debug(f"parse_json: 清理后解析失败: {e}")
        
        # 修复单引号
        fixed_quotes = _fix_single_quotes(fixed)
        
        try:
            result = json.loads(fixed_quotes)
            logger.debug("parse_json: 修复单引号后解析成功")
            if isinstance(result, dict) and "output" in result:
                inner = parse_json(result["output"])
                if inner is not None:
                    return inner
            return result
        except json.JSONDecodeError as e:
            logger.debug(f"parse_json: 修复单引号后解析失败: {e}")
        
        # 【增强】尝试修复截断的 JSON（在清理后的内容上）
        fixed_truncated = _fix_truncated_json(fixed)
        
        try:
            result = json.loads(fixed_truncated)
            logger.debug("parse_json: 修复截断后解析成功")
            if isinstance(result, dict) and "output" in result:
                inner = parse_json(result["output"])
                if inner is not None:
                    return inner
            return result
        except json.JSONDecodeError as e:
            logger.debug(f"parse_json: 修复截断后解析失败: {e}")
        
        # 尝试同时修复单引号和截断
        fixed_both = _fix_truncated_json(fixed_quotes)
        
        try:
            result = json.loads(fixed_both)
            logger.debug("parse_json: 修复单引号+截断后解析成功")
            if isinstance(result, dict) and "output" in result:
                inner = parse_json(result["output"])
                if inner is not None:
                    return inner
            return result
        except json.JSONDecodeError as e:
            logger.debug(f"parse_json: 修复单引号+截断后解析失败: {e}")
        
        # 3. 最后尝试从文本中提取 JSON
        extracted = extract_json_from_text(json_str)
        if extracted is not None:
            logger.debug("parse_json: 从文本中提取 JSON 成功")
            return extracted
        
        # 所有方法都失败
        logger.warning(f"parse_json: 所有解析方法都失败，输入预览: {repr(preview)}")
        return None
        
    except Exception as e:
        # 捕获所有异常，确保不抛出未处理异常
        logger.error(f"parse_json: 解析过程中发生异常: {e}")
        return None


def extract_json_from_text(text: str) -> Union[dict, list, None]:
    """从文本中提取 JSON 对象或数组
    
    支持处理截断的 JSON，会尝试修复不完整的结构。
    
    Args:
        text: 可能包含 JSON 的文本
        
    Returns:
        提取的 JSON 对象/数组，失败返回 None
    """
    if not text or not isinstance(text, str):
        return None
    
    try:
        # 先清理空白
        text = _strip_whitespace(text)
        
        # 尝试找到 JSON 对象或数组的起始位置
        obj_start = text.find('{')
        arr_start = text.find('[')
        
        # 确定起始位置和结束字符
        if obj_start == -1 and arr_start == -1:
            logger.debug("extract_json_from_text: 未找到 JSON 起始字符")
            return None
        
        if obj_start == -1:
            start = arr_start
            open_char, close_char = '[', ']'
        elif arr_start == -1:
            start = obj_start
            open_char, close_char = '{', '}'
        else:
            # 选择先出现的
            if obj_start < arr_start:
                start = obj_start
                open_char, close_char = '{', '}'
            else:
                start = arr_start
                open_char, close_char = '[', ']'
        
        # 找到匹配的结束括号
        depth = 0
        in_string = False
        escaped = False
        
        for i, ch in enumerate(text[start:], start):
            if escaped:
                escaped = False
                continue
            
            if ch == '\\' and in_string:
                escaped = True
                continue
            
            if ch == '"':
                in_string = not in_string
                continue
            
            if not in_string:
                if ch == open_char:
                    depth += 1
                elif ch == close_char:
                    depth -= 1
                    if depth == 0:
                        json_str = text[start:i + 1]
                        cleaned = clean_json_string(json_str)
                        try:
                            return json.loads(cleaned)
                        except json.JSONDecodeError as e:
                            logger.debug(f"extract_json_from_text: 完整 JSON 解析失败: {e}")
                            return None
        
        # 如果没有找到完整的 JSON（被截断），尝试修复
        if depth > 0:
            logger.debug(f"extract_json_from_text: 检测到截断的 JSON，深度={depth}")
            json_str = text[start:]
            cleaned = clean_json_string(json_str)
            fixed = _fix_truncated_json(cleaned)
            try:
                return json.loads(fixed)
            except json.JSONDecodeError:
                # 最后尝试：移除尾部逗号后再修复
                fixed = re.sub(r',(\s*[\]\}])', r'\1', fixed)
                try:
                    return json.loads(fixed)
                except json.JSONDecodeError as e:
                    logger.debug(f"extract_json_from_text: 修复后仍然解析失败: {e}")
                    return None
        
        return None
        
    except Exception as e:
        logger.error(f"extract_json_from_text: 提取过程中发生异常: {e}")
        return None


def get_parse_error_preview(json_str: str, max_length: int = 200) -> str:
    """获取解析失败时的原始输出预览
    
    用于在 UI 中显示错误信息时提供上下文。
    
    Args:
        json_str: 原始 JSON 字符串
        max_length: 预览的最大长度
        
    Returns:
        截断后的预览字符串
    """
    if not json_str:
        return "(空输入)"
    
    if not isinstance(json_str, str):
        return f"(非字符串类型: {type(json_str).__name__})"
    
    # 清理不可见字符用于显示
    preview = json_str[:max_length]
    
    # 替换换行符为可见形式
    preview = preview.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
    
    if len(json_str) > max_length:
        preview += f"... (共 {len(json_str)} 字符)"
    
    return preview
