"""本地存储服务 - SQLite 数据库管理"""

import json
import logging
import os
import sqlite3
import uuid
from pathlib import Path

# NOTE:
# This module is retained only as a local SQLite fallback for development.
# Production MySQL / RDS deployments should use SQLAlchemyStorageService instead.

logger = logging.getLogger(__name__)

# 摘要最大字符数（避免单元格内容过长）
_SUMMARY_MAX_CHARS = 200
DEFAULT_RAG_SOURCE_PRIORITY = [
    "customer_profile_markdown",
    "parsed_document_text",
    "scheme_match_summary",
    "application_summary",
]


def _build_extraction_summary(data: dict, max_chars: int = _SUMMARY_MAX_CHARS) -> str:
    """从提取数据中生成有意义的摘要字符串。

    对嵌套字典（如征信报告），展开子字典取实际值；
    对扁平字典，直接取键值对。最多取前 5 个非空字段。

    Args:
        data: 提取的 extracted_data 字典。
        max_chars: 摘要最大字符数。

    Returns:
        str: 摘要字符串，如 "企业名称: xxx; 报告编号: xxx"。
    """
    flat_items: list[tuple[str, str]] = []

    for key, value in data.items():
        if isinstance(value, dict):
            # 嵌套字典：展开取子字段的实际值
            for sub_key, sub_val in value.items():
                if sub_val and not isinstance(sub_val, (dict, list)):
                    flat_items.append((sub_key, str(sub_val).strip()))
        elif isinstance(value, list):
            flat_items.append((key, f"[{len(value)}条]"))
        elif value is not None and str(value).strip():
            flat_items.append((key, str(value).strip()))

    # 过滤空值，取前 5 个
    non_empty = [(k, v) for k, v in flat_items if v][:5]
    if not non_empty:
        return ""

    summary = "; ".join(f"{k}: {v}" for k, v in non_empty)
    if len(summary) > max_chars:
        summary = summary[:max_chars] + "…"
    return summary


class LocalStorageService:
    """本地存储服务 - 提供与 FeishuService 相同的接口"""

    def __init__(self, db_path: str | None = None):
        """
        初始化本地存储服务

        Args:
            db_path: 数据库文件路径,默认为 backend/data/customers.db
        """
        if db_path is None:
            db_path = str(Path(__file__).parent.parent / "data" / "customers.db")
        self.db_path = db_path
        self._ensure_db_exists()

    def _ensure_db_exists(self) -> None:
        """确保数据库文件和目录存在"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        # 创建数据库文件(如果不存在)
        conn = sqlite3.connect(self.db_path)
        conn.close()
        # 初始化表结构
        self._init_tables()

    def _get_connection(self) -> sqlite3.Connection:
        """
        获取数据库连接

        Returns:
            sqlite3.Connection: 数据库连接对象
        """
        conn = sqlite3.connect(self.db_path)
        # 启用外键约束
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _column_exists(self, cursor: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
        """Check whether a SQLite table already contains the given column."""
        cursor.execute(f"PRAGMA table_info({table_name})")
        return any(str(row[1]).lower() == column_name.lower() for row in cursor.fetchall())

    def _init_tables(self) -> None:
        """初始化数据库表结构和索引"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # 创建 customers 表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_id VARCHAR(64) UNIQUE NOT NULL,
                    name VARCHAR(255),
                    phone VARCHAR(50),
                    id_card VARCHAR(100),
                    loan_amount DECIMAL(15,2),
                    loan_purpose VARCHAR(255),
                    income_source VARCHAR(255),
                    monthly_income DECIMAL(15,2),
                    credit_score INTEGER,
                    status VARCHAR(50) DEFAULT 'new',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    uploader VARCHAR(255) DEFAULT '',
                    upload_time VARCHAR(50) DEFAULT '',
                    customer_type VARCHAR(20) DEFAULT 'enterprise'
                )
            ''')

            # 迁移：为已有数据库添加 uploader 和 upload_time 列
            for col_def in [
                "uploader VARCHAR(255) DEFAULT ''",
                "upload_time VARCHAR(50) DEFAULT ''",
                "customer_type VARCHAR(20) DEFAULT 'enterprise'",
            ]:
                try:
                    col_name = col_def.split()[0]
                    if self._column_exists(cursor, "customers", col_name):
                        continue
                    cursor.execute(
                        f"ALTER TABLE customers ADD COLUMN {col_def}"
                    )
                    logger.info(f"[Migration] Added column {col_name} to customers")
                except sqlite3.OperationalError:
                    pass  # 列已存在，忽略

            # 创建 documents 表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id VARCHAR(64) UNIQUE NOT NULL,
                    customer_id VARCHAR(64) NOT NULL,
                    file_name VARCHAR(255),
                    file_path VARCHAR(512),
                    file_type VARCHAR(50),
                    file_size INTEGER,
                    upload_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    feishu_file_id VARCHAR(255),
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
            ''')

            # 创建 extractions 表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS extractions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    extraction_id VARCHAR(64) UNIQUE NOT NULL,
                    doc_id VARCHAR(64) NOT NULL,
                    customer_id VARCHAR(64) NOT NULL,
                    extraction_type VARCHAR(50),
                    extracted_data TEXT,
                    confidence FLOAT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (doc_id) REFERENCES documents(doc_id),
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
            ''')

            # 创建索引
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_customers_customer_id
                ON customers(customer_id)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_customers_status
                ON customers(status)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_customers_created_at
                ON customers(created_at)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_documents_customer_id
                ON documents(customer_id)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_documents_doc_id
                ON documents(doc_id)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_extractions_doc_id
                ON extractions(doc_id)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_extractions_customer_id
                ON extractions(customer_id)
            ''')

            # 创建 table_fields 表（动态字段配置）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customer_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_id VARCHAR(64) UNIQUE NOT NULL,
                    title VARCHAR(255) DEFAULT '',
                    markdown_content TEXT DEFAULT '',
                    source_mode VARCHAR(20) DEFAULT 'auto',
                    source_snapshot_json TEXT DEFAULT '{}',
                    rag_source_priority_json TEXT DEFAULT '[]',
                    risk_report_schema_json TEXT DEFAULT '{}',
                    version INTEGER DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_customer_profiles_customer_id
                ON customer_profiles(customer_id)
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customer_scheme_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id VARCHAR(64) UNIQUE NOT NULL,
                    customer_id VARCHAR(64) NOT NULL,
                    customer_name VARCHAR(255) DEFAULT '',
                    summary_markdown TEXT DEFAULT '',
                    raw_result TEXT DEFAULT '',
                    source VARCHAR(50) DEFAULT 'manual',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_customer_scheme_snapshots_customer_id
                ON customer_scheme_snapshots(customer_id)
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customer_document_chunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chunk_id VARCHAR(64) UNIQUE NOT NULL,
                    customer_id VARCHAR(64) NOT NULL,
                    source_type VARCHAR(50) NOT NULL,
                    source_id VARCHAR(64) DEFAULT '',
                    chunk_index INTEGER DEFAULT 0,
                    chunk_text TEXT NOT NULL,
                    embedding_json TEXT DEFAULT '[]',
                    metadata_json TEXT DEFAULT '{}',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_customer_document_chunks_customer_id
                ON customer_document_chunks(customer_id)
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customer_risk_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id VARCHAR(64) UNIQUE NOT NULL,
                    customer_id VARCHAR(64) NOT NULL,
                    profile_version INTEGER DEFAULT 1,
                    profile_updated_at VARCHAR(64) DEFAULT '',
                    generated_at VARCHAR(64) NOT NULL,
                    report_json TEXT DEFAULT '{}',
                    report_markdown TEXT DEFAULT '',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_customer_risk_reports_customer_id
                ON customer_risk_reports(customer_id)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_customer_risk_reports_generated_at
                ON customer_risk_reports(generated_at)
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS table_fields (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    field_id VARCHAR(64) UNIQUE NOT NULL,
                    field_name VARCHAR(255) NOT NULL,
                    field_key VARCHAR(100) NOT NULL,
                    doc_type VARCHAR(100) DEFAULT '',
                    field_order INTEGER DEFAULT 0,
                    editable BOOLEAN DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_table_fields_field_key
                ON table_fields(field_key)
            ''')

            # 初始化默认字段（仅在表为空时插入）
            cursor.execute('SELECT COUNT(*) FROM table_fields')
            if cursor.fetchone()[0] == 0:
                self._init_default_fields(cursor)

            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"数据库表初始化失败: {e}") from e
        finally:
            conn.close()

    # ==================== 客户相关接口 ====================

    async def create_customer(self, customer_data: dict) -> dict:
        """
        创建新客户

        Args:
            customer_data: 客户数据字典,必须包含 customer_id

        Returns:
            dict: 创建的客户数据

        Raises:
            ValueError: 缺少必需字段
            RuntimeError: 数据库操作失败
        """
        if 'customer_id' not in customer_data:
            raise ValueError("customer_id 是必需字段")

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO customers (
                    customer_id, name, phone, id_card, loan_amount, loan_purpose,
                    income_source, monthly_income, credit_score, status,
                    uploader, upload_time, customer_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                customer_data['customer_id'],
                customer_data.get('name'),
                customer_data.get('phone'),
                customer_data.get('id_card'),
                customer_data.get('loan_amount'),
                customer_data.get('loan_purpose'),
                customer_data.get('income_source'),
                customer_data.get('monthly_income'),
                customer_data.get('credit_score'),
                customer_data.get('status') or 'new',
                customer_data.get('uploader') or '',
                customer_data.get('upload_time') or '',
                customer_data.get('customer_type') or 'enterprise',
            ))
            conn.commit()
            return customer_data
        except sqlite3.IntegrityError as e:
            conn.rollback()
            raise RuntimeError(f"客户 ID 已存在: {customer_data['customer_id']}") from e
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"创建客户失败: {e}") from e
        finally:
            conn.close()

    async def get_customer(self, customer_id: str) -> dict | None:
        """
        获取客户信息

        Args:
            customer_id: 客户唯一标识

        Returns:
            Optional[dict]: 客户数据字典,不存在则返回 None

        Raises:
            RuntimeError: 数据库操作失败
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                'SELECT * FROM customers WHERE customer_id = ?',
                (customer_id,)
            )
            row = cursor.fetchone()

            if row:
                return self._row_to_customer(row)
            return None
        except sqlite3.Error as e:
            raise RuntimeError(f"查询客户失败: {e}") from e
        finally:
            conn.close()

    async def update_customer(
        self,
        customer_id: str,
        update_data: dict
    ) -> dict | None:
        """
        更新客户信息

        Args:
            customer_id: 客户唯一标识
            update_data: 要更新的字段字典

        Returns:
            Optional[dict]: 更新后的客户数据,客户不存在则返回 None

        Raises:
            RuntimeError: 数据库操作失败
        """
        # 允许更新的字段白名单
        allowed_fields = {
            'name', 'phone', 'id_card', 'loan_amount', 'loan_purpose',
            'income_source', 'monthly_income', 'credit_score', 'status',
            'uploader', 'upload_time', 'customer_type',
        }

        # 过滤出允许更新的字段
        filtered_data = {
            k: v for k, v in update_data.items()
            if k in allowed_fields
        }

        if not filtered_data:
            # 没有可更新的字段,直接返回当前数据
            return await self.get_customer(customer_id)

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # 构建 SET 子句
            set_clauses = [f"{field} = ?" for field in filtered_data]
            set_clauses.append("updated_at = CURRENT_TIMESTAMP")

            sql = f"UPDATE customers SET {', '.join(set_clauses)} WHERE customer_id = ?"
            values = [*list(filtered_data.values()), customer_id]

            cursor.execute(sql, values)
            conn.commit()

            # 返回更新后的数据
            return await self.get_customer(customer_id)
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"更新客户失败: {e}") from e
        finally:
            conn.close()

    async def list_customers(
        self,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0
    ) -> list[dict]:
        """
        列出客户列表

        Args:
            status: 客户状态过滤(可选),如 'new', 'processing', 'completed'
            limit: 返回记录数限制,默认 100
            offset: 分页偏移量,默认 0

        Returns:
            list[dict]: 客户数据列表

        Raises:
            RuntimeError: 数据库操作失败
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if status:
                cursor.execute('''
                    SELECT * FROM customers
                    WHERE status = ?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                ''', (status, limit, offset))
            else:
                cursor.execute('''
                    SELECT * FROM customers
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                ''', (limit, offset))

            rows = cursor.fetchall()
            return [self._row_to_customer(row) for row in rows]
        except sqlite3.Error as e:
            raise RuntimeError(f"查询客户列表失败: {e}") from e
        finally:
            conn.close()

    async def delete_customer(self, customer_id: str) -> bool:
        """
        删除客户及其关联数据

        Args:
            customer_id: 客户唯一标识

        Returns:
            bool: 删除成功返回 True,客户不存在返回 False

        Raises:
            RuntimeError: 数据库操作失败
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # 先删除关联的提取记录
            cursor.execute(
                'DELETE FROM extractions WHERE customer_id = ?',
                (customer_id,)
            )

            # 删除关联的文档
            cursor.execute(
                'DELETE FROM documents WHERE customer_id = ?',
                (customer_id,)
            )

            # 删除客户记录
            cursor.execute(
                'DELETE FROM customer_profiles WHERE customer_id = ?',
                (customer_id,)
            )

            cursor.execute(
                'DELETE FROM customer_scheme_snapshots WHERE customer_id = ?',
                (customer_id,)
            )

            cursor.execute(
                'DELETE FROM customer_document_chunks WHERE customer_id = ?',
                (customer_id,)
            )

            cursor.execute(
                'DELETE FROM customers WHERE customer_id = ?',
                (customer_id,)
            )

            deleted = cursor.rowcount > 0
            conn.commit()
            return deleted
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"删除客户失败: {e}") from e
        finally:
            conn.close()

    # ==================== 文档相关接口 ====================

    async def save_document(self, doc_data: dict) -> dict:
        """
        保存文档记录

        Args:
            doc_data: 文档数据字典,必须包含 doc_id, customer_id, file_name, file_path, file_type

        Returns:
            dict: 保存的文档数据

        Raises:
            ValueError: 缺少必需字段
            RuntimeError: 数据库操作失败
        """
        required_fields = ['doc_id', 'customer_id', 'file_name', 'file_path', 'file_type']
        missing_fields = [f for f in required_fields if f not in doc_data]
        if missing_fields:
            raise ValueError(f"缺少必需字段: {', '.join(missing_fields)}")

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT OR REPLACE INTO documents (
                    doc_id, customer_id, file_name, file_path, file_type, file_size, upload_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                doc_data['doc_id'],
                doc_data['customer_id'],
                doc_data['file_name'],
                doc_data['file_path'],
                doc_data['file_type'],
                doc_data.get('file_size'),
                doc_data.get('upload_time') or 'CURRENT_TIMESTAMP'
            ))
            conn.commit()
            return doc_data
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"保存文档失败: {e}") from e
        finally:
            conn.close()

    async def get_document(self, doc_id: str) -> dict | None:
        """
        获取文档信息

        Args:
            doc_id: 文档唯一标识

        Returns:
            Optional[dict]: 文档数据字典,不存在则返回 None

        Raises:
            RuntimeError: 数据库操作失败
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                'SELECT * FROM documents WHERE doc_id = ?',
                (doc_id,)
            )
            row = cursor.fetchone()

            if row:
                return self._row_to_document(row)
            return None
        except sqlite3.Error as e:
            raise RuntimeError(f"查询文档失败: {e}") from e
        finally:
            conn.close()

    async def list_documents(self, customer_id: str) -> list[dict]:
        """
        列出客户的所有文档

        Args:
            customer_id: 客户唯一标识

        Returns:
            list[dict]: 文档数据列表,按上传时间倒序

        Raises:
            RuntimeError: 数据库操作失败
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT * FROM documents
                WHERE customer_id = ?
                ORDER BY upload_time DESC
            ''', (customer_id,))

            rows = cursor.fetchall()
            return [self._row_to_document(row) for row in rows]
        except sqlite3.Error as e:
            raise RuntimeError(f"查询文档列表失败: {e}") from e
        finally:
            conn.close()

    async def delete_document(self, doc_id: str) -> bool:
        """
        删除文档记录及其关联的提取记录

        Args:
            doc_id: 文档唯一标识

        Returns:
            bool: 删除成功返回 True,文档不存在返回 False

        Raises:
            RuntimeError: 数据库操作失败
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # 先删除关联的提取记录
            cursor.execute(
                'DELETE FROM extractions WHERE doc_id = ?',
                (doc_id,)
            )

            # 删除文档记录
            cursor.execute(
                'DELETE FROM documents WHERE doc_id = ?',
                (doc_id,)
            )

            deleted = cursor.rowcount > 0
            conn.commit()
            return deleted
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"删除文档失败: {e}") from e
        finally:
            conn.close()

    # ==================== 提取结果相关接口 ====================

    async def save_extraction(self, extraction_data: dict) -> dict:
        """
        保存提取结果

        Args:
            extraction_data: 提取结果数据字典,必须包含 extraction_id, doc_id, customer_id, extraction_type

        Returns:
            dict: 保存的提取结果数据

        Raises:
            ValueError: 缺少必需字段
            RuntimeError: 数据库操作失败
        """
        required_fields = ['extraction_id', 'doc_id', 'customer_id', 'extraction_type']
        missing_fields = [f for f in required_fields if f not in extraction_data]
        if missing_fields:
            raise ValueError(f"缺少必需字段: {', '.join(missing_fields)}")

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # 将 extracted_data 序列化为 JSON
            extracted_data_json = json.dumps(
                extraction_data.get('extracted_data', {}),
                ensure_ascii=False
            )

            cursor.execute('''
                INSERT OR REPLACE INTO extractions (
                    extraction_id, doc_id, customer_id, extraction_type,
                    extracted_data, confidence
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                extraction_data['extraction_id'],
                extraction_data['doc_id'],
                extraction_data['customer_id'],
                extraction_data['extraction_type'],
                extracted_data_json,
                extraction_data.get('confidence')
            ))
            conn.commit()
            return extraction_data
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"保存提取结果失败: {e}") from e
        finally:
            conn.close()

    async def get_extraction(self, extraction_id: str) -> dict | None:
        """
        获取提取结果

        Args:
            extraction_id: 提取记录唯一标识

        Returns:
            Optional[dict]: 提取结果数据字典,不存在则返回 None

        Raises:
            RuntimeError: 数据库操作失败
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                'SELECT * FROM extractions WHERE extraction_id = ?',
                (extraction_id,)
            )
            row = cursor.fetchone()

            if row:
                return self._row_to_extraction(row)
            return None
        except sqlite3.Error as e:
            raise RuntimeError(f"查询提取结果失败: {e}") from e
        finally:
            conn.close()

    async def get_extractions_by_doc(self, doc_id: str) -> list[dict]:
        """
        获取文档的所有提取结果

        Args:
            doc_id: 文档唯一标识

        Returns:
            list[dict]: 提取结果数据列表,按创建时间倒序

        Raises:
            RuntimeError: 数据库操作失败
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT * FROM extractions
                WHERE doc_id = ?
                ORDER BY created_at DESC
            ''', (doc_id,))

            rows = cursor.fetchall()
            return [self._row_to_extraction(row) for row in rows]
        except sqlite3.Error as e:
            raise RuntimeError(f"查询文档提取结果失败: {e}") from e
        finally:
            conn.close()

    async def get_extractions_by_customer(self, customer_id: str) -> list[dict]:
        """
        获取客户的所有提取结果

        Args:
            customer_id: 客户唯一标识

        Returns:
            list[dict]: 提取结果数据列表,按创建时间倒序

        Raises:
            RuntimeError: 数据库操作失败
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT * FROM extractions
                WHERE customer_id = ?
                ORDER BY created_at DESC
            ''', (customer_id,))

            rows = cursor.fetchall()
            return [self._row_to_extraction(row) for row in rows]
        except sqlite3.Error as e:
            raise RuntimeError(f"查询客户提取结果失败: {e}") from e
        finally:
            conn.close()

    async def update_extraction(self, extraction_id: str, field: str, value: str) -> bool:
        """
        更新单条 extraction 的某个字段值

        Args:
            extraction_id: extraction 唯一标识
            field: 要更新的字段名（extracted_data 内的 key）
            value: 新的字段值

        Returns:
            bool: 更新成功返回 True，未找到记录返回 False

        Raises:
            RuntimeError: 数据库操作失败
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                'SELECT extracted_data FROM extractions WHERE extraction_id = ?',
                (extraction_id,)
            )
            row = cursor.fetchone()
            if not row:
                return False

            try:
                extracted_data = json.loads(row[0]) if row[0] else {}
            except json.JSONDecodeError:
                extracted_data = {}

            target = extracted_data
            parts = [part for part in field.split(".") if part]
            if not parts:
                return False
            for part in parts[:-1]:
                existing = target.get(part)
                if not isinstance(existing, dict):
                    existing = {}
                    target[part] = existing
                target = existing
            target[parts[-1]] = value

            cursor.execute(
                'UPDATE extractions SET extracted_data = ? WHERE extraction_id = ?',
                (json.dumps(extracted_data, ensure_ascii=False), extraction_id)
            )
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"更新 extraction 字段失败: {e}") from e
        finally:
            conn.close()

    # ==================== 辅助方法 ====================

    async def get_customer_profile(self, customer_id: str) -> dict | None:
        """Get markdown profile for a customer."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                SELECT customer_id, title, markdown_content, source_mode,
                       source_snapshot_json, rag_source_priority_json,
                       risk_report_schema_json, version, created_at, updated_at
                FROM customer_profiles
                WHERE customer_id = ?
                ''',
                (customer_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_customer_profile(row)
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to load customer profile: {e}") from e
        finally:
            conn.close()

    async def upsert_customer_profile(self, profile_data: dict) -> dict:
        """Create or update markdown profile for a customer."""
        customer_id = profile_data.get("customer_id")
        if not customer_id:
            raise ValueError("customer_id is required")

        existing = await self.get_customer_profile(customer_id)
        version = int(existing.get("version", 0)) + 1 if existing else int(profile_data.get("version") or 1)
        source_snapshot_json = json.dumps(profile_data.get("source_snapshot") or {}, ensure_ascii=False)
        rag_source_priority_json = json.dumps(
            profile_data.get("rag_source_priority") or DEFAULT_RAG_SOURCE_PRIORITY,
            ensure_ascii=False,
        )
        risk_report_schema_json = json.dumps(profile_data.get("risk_report_schema") or {}, ensure_ascii=False)

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                INSERT INTO customer_profiles (
                    customer_id, title, markdown_content, source_mode,
                    source_snapshot_json, rag_source_priority_json,
                    risk_report_schema_json, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(customer_id) DO UPDATE SET
                    title = excluded.title,
                    markdown_content = excluded.markdown_content,
                    source_mode = excluded.source_mode,
                    source_snapshot_json = excluded.source_snapshot_json,
                    rag_source_priority_json = excluded.rag_source_priority_json,
                    risk_report_schema_json = excluded.risk_report_schema_json,
                    version = excluded.version,
                    updated_at = CURRENT_TIMESTAMP
                ''',
                (
                    customer_id,
                    profile_data.get("title") or "",
                    profile_data.get("markdown_content") or "",
                    profile_data.get("source_mode") or "auto",
                    source_snapshot_json,
                    rag_source_priority_json,
                    risk_report_schema_json,
                    version,
                ),
            )
            conn.commit()
            saved = await self.get_customer_profile(customer_id)
            if not saved:
                raise RuntimeError("Markdown profile save returned empty result")
            return saved
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"Failed to save customer profile: {e}") from e
        finally:
            conn.close()

    async def delete_customer_profile(self, customer_id: str) -> bool:
        """Delete markdown profile for a customer."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('DELETE FROM customer_profiles WHERE customer_id = ?', (customer_id,))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"Failed to delete customer profile: {e}") from e
        finally:
            conn.close()

    async def save_scheme_snapshot(self, snapshot_data: dict) -> dict:
        """Persist the latest scheme matching summary for a customer."""
        customer_id = snapshot_data.get("customer_id")
        if not customer_id:
            raise ValueError("customer_id is required")

        snapshot_id = snapshot_data.get("snapshot_id") or customer_id
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                INSERT INTO customer_scheme_snapshots (
                    snapshot_id, customer_id, customer_name, summary_markdown, raw_result, source
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_id) DO UPDATE SET
                    customer_name = excluded.customer_name,
                    summary_markdown = excluded.summary_markdown,
                    raw_result = excluded.raw_result,
                    source = excluded.source,
                    updated_at = CURRENT_TIMESTAMP
                ''',
                (
                    snapshot_id,
                    customer_id,
                    snapshot_data.get("customer_name") or "",
                    snapshot_data.get("summary_markdown") or "",
                    snapshot_data.get("raw_result") or "",
                    snapshot_data.get("source") or "manual",
                ),
            )
            conn.commit()
            latest = await self.get_latest_scheme_snapshot(customer_id)
            return latest or {}
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"Failed to save scheme snapshot: {e}") from e
        finally:
            conn.close()

    async def get_latest_scheme_snapshot(self, customer_id: str) -> dict | None:
        """Get the latest scheme summary snapshot for a customer."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                SELECT snapshot_id, customer_id, customer_name, summary_markdown, raw_result, source,
                       created_at, updated_at
                FROM customer_scheme_snapshots
                WHERE customer_id = ?
                ORDER BY updated_at DESC, created_at DESC, id DESC
                LIMIT 1
                ''',
                (customer_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "snapshot_id": row[0],
                "customer_id": row[1],
                "customer_name": row[2],
                "summary_markdown": row[3],
                "raw_result": row[4],
                "source": row[5],
                "created_at": row[6],
                "updated_at": row[7],
            }
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to load scheme snapshot: {e}") from e
        finally:
            conn.close()

    async def replace_customer_chunks(self, customer_id: str, chunks: list[dict]) -> None:
        """Replace all RAG chunks for a customer."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('DELETE FROM customer_document_chunks WHERE customer_id = ?', (customer_id,))
            for chunk in chunks:
                cursor.execute(
                    '''
                    INSERT INTO customer_document_chunks (
                        chunk_id, customer_id, source_type, source_id, chunk_index,
                        chunk_text, embedding_json, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        chunk["chunk_id"],
                        customer_id,
                        chunk.get("source_type") or "",
                        chunk.get("source_id") or "",
                        int(chunk.get("chunk_index") or 0),
                        chunk.get("chunk_text") or "",
                        json.dumps(chunk.get("embedding") or [], ensure_ascii=False),
                        json.dumps(chunk.get("metadata") or {}, ensure_ascii=False),
                    ),
                )
            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"Failed to replace customer chunks: {e}") from e
        finally:
            conn.close()

    async def save_customer_risk_report(self, report_data: dict) -> dict:
        """Persist one generated customer risk report for later comparison."""
        customer_id = report_data.get("customer_id")
        generated_at = report_data.get("generated_at")
        if not customer_id or not generated_at:
            raise ValueError("customer_id and generated_at are required")

        report_id = report_data.get("report_id") or str(uuid.uuid4())[:12]
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                INSERT INTO customer_risk_reports (
                    report_id, customer_id, profile_version, profile_updated_at,
                    generated_at, report_json, report_markdown
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    report_id,
                    customer_id,
                    int(report_data.get("profile_version") or 1),
                    report_data.get("profile_updated_at") or "",
                    generated_at,
                    json.dumps(report_data.get("report_json") or {}, ensure_ascii=False),
                    report_data.get("report_markdown") or "",
                ),
            )
            conn.commit()
            return {
                "report_id": report_id,
                "customer_id": customer_id,
                "profile_version": int(report_data.get("profile_version") or 1),
                "profile_updated_at": report_data.get("profile_updated_at") or "",
                "generated_at": generated_at,
                "report_json": report_data.get("report_json") or {},
                "report_markdown": report_data.get("report_markdown") or "",
            }
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"Failed to save customer risk report: {e}") from e
        finally:
            conn.close()

    async def list_customer_risk_reports(self, customer_id: str, limit: int = 5) -> list[dict]:
        """List recent risk report history for one customer."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                SELECT report_id, customer_id, profile_version, profile_updated_at,
                       generated_at, report_json, report_markdown
                FROM customer_risk_reports
                WHERE customer_id = ?
                ORDER BY datetime(generated_at) DESC, id DESC
                LIMIT ?
                ''',
                (customer_id, max(1, int(limit))),
            )
            rows = cursor.fetchall()
            return [
                {
                    "report_id": row[0],
                    "customer_id": row[1],
                    "profile_version": row[2] or 1,
                    "profile_updated_at": row[3] or "",
                    "generated_at": row[4] or "",
                    "report_json": json.loads(row[5]) if row[5] else {},
                    "report_markdown": row[6] or "",
                }
                for row in rows
            ]
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to list customer risk reports: {e}") from e
        finally:
            conn.close()

    async def get_latest_customer_risk_report(self, customer_id: str) -> dict | None:
        """Get the most recent risk report snapshot for a customer."""
        reports = await self.list_customer_risk_reports(customer_id, limit=1)
        return reports[0] if reports else None

    async def get_customer_chunks(self, customer_id: str) -> list[dict]:
        """List all stored RAG chunks for a customer."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                SELECT chunk_id, customer_id, source_type, source_id, chunk_index,
                       chunk_text, embedding_json, metadata_json, created_at, updated_at
                FROM customer_document_chunks
                WHERE customer_id = ?
                ORDER BY source_type, chunk_index, id
                ''',
                (customer_id,),
            )
            rows = cursor.fetchall()
            return [self._row_to_customer_chunk(row) for row in rows]
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to load customer chunks: {e}") from e
        finally:
            conn.close()

    def _row_to_customer(self, row: tuple) -> dict:
        """
        将数据库行转换为客户字典

        Args:
            row: 数据库查询结果行

        Returns:
            dict: 客户数据字典
        """
        result = {
            'customer_id': row[1],
            'name': row[2],
            'phone': row[3],
            'id_card': row[4],
            'loan_amount': float(row[5]) if row[5] is not None else None,
            'loan_purpose': row[6],
            'income_source': row[7],
            'monthly_income': float(row[8]) if row[8] is not None else None,
            'credit_score': row[9],
            'status': row[10],
            'created_at': row[11],
            'updated_at': row[12],
        }
        # ALTER TABLE 迁移后新增的列（row[13], row[14], row[15]）
        if len(row) > 13:
            result['uploader'] = row[13] or ''
        if len(row) > 14:
            result['upload_time'] = row[14] or ''
        if len(row) > 15:
            result['customer_type'] = row[15] or 'enterprise'
        return result

    def _row_to_document(self, row: tuple) -> dict:
        """
        将数据库行转换为文档字典

        Args:
            row: 数据库查询结果行

        Returns:
            dict: 文档数据字典
        """
        return {
            'doc_id': row[1],
            'customer_id': row[2],
            'file_name': row[3],
            'file_path': row[4],
            'file_type': row[5],
            'file_size': row[6],
            'upload_time': row[7]
        }

    def _row_to_extraction(self, row: tuple) -> dict:
        """
        将数据库行转换为提取结果字典

        Args:
            row: 数据库查询结果行

        Returns:
            dict: 提取结果数据字典
        """
        # 反序列化 extracted_data JSON 字段
        extracted_data = {}
        if row[5]:  # extracted_data 字段
            try:
                extracted_data = json.loads(row[5])
            except json.JSONDecodeError:
                # 如果 JSON 解析失败,返回空字典
                extracted_data = {}

        return {
            'extraction_id': row[1],
            'doc_id': row[2],
            'customer_id': row[3],
            'extraction_type': row[4],
            'extracted_data': extracted_data,
            'confidence': row[6],
            'created_at': row[7]
        }

    def _row_to_customer_profile(self, row: tuple) -> dict:
        """Convert customer profile row to dict."""
        return {
            "customer_id": row[0],
            "title": row[1] or "",
            "markdown_content": row[2] or "",
            "source_mode": row[3] or "auto",
            "source_snapshot": json.loads(row[4]) if row[4] else {},
            "rag_source_priority": json.loads(row[5]) if row[5] else list(DEFAULT_RAG_SOURCE_PRIORITY),
            "risk_report_schema": json.loads(row[6]) if row[6] else {},
            "version": row[7] or 1,
            "created_at": row[8],
            "updated_at": row[9],
        }

    def _row_to_customer_chunk(self, row: tuple) -> dict:
        """Convert customer chunk row to dict."""
        return {
            "chunk_id": row[0],
            "customer_id": row[1],
            "source_type": row[2],
            "source_id": row[3] or "",
            "chunk_index": row[4] or 0,
            "chunk_text": row[5] or "",
            "embedding": json.loads(row[6]) if row[6] else [],
            "metadata": json.loads(row[7]) if row[7] else {},
            "created_at": row[8],
            "updated_at": row[9],
        }

    # ==================== 动态字段配置 ====================

    def _init_default_fields(self, cursor: sqlite3.Cursor) -> None:
        """初始化默认表头字段配置。

        Args:
            cursor: 数据库游标（在事务中调用）
        """
        import uuid

        defaults = [
            ("enterprise_credit", "企业征信报告", "企业征信提取"),
            ("personal_credit", "个人征信报告", "个人征信提取"),
            ("enterprise_flow", "企业流水", "企业流水提取"),
            ("personal_flow", "个人流水", "个人流水提取"),
            ("property_cert", "抵押物信息", "抵押物信息提取"),
            ("financial_report", "财务数据", "财务数据提取"),
            ("water_report", "水母报告", "水母报告提取"),
            ("personal_tax", "个人收入纳税/公积金", "个人收入纳税/公积金"),
            ("fund_demand", "资金需求", ""),
            ("repayment_progress", "还款进度", ""),
            ("overdue_reminder", "逾期提醒", ""),
            ("reloan_potential", "客户复贷潜力标签", ""),
            ("upload_time", "上传时间", ""),
            ("remark", "备注", ""),
            ("upload_account", "上传账号", ""),
        ]
        for idx, (field_key, field_name, doc_type) in enumerate(defaults):
            field_id = str(uuid.uuid4())
            editable = 1 if doc_type == "" else 0
            cursor.execute(
                """INSERT INTO table_fields
                   (field_id, field_name, field_key, doc_type, field_order, editable)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (field_id, field_name, field_key, doc_type, idx, editable),
            )
        logger.info(f"[Init] Inserted {len(defaults)} default table fields")

    async def get_table_fields(self) -> list[dict]:
        """获取所有表头字段配置，按 field_order 排序。

        Returns:
            list[dict]: 字段配置列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT field_id, field_name, field_key, doc_type, "
                "field_order, editable FROM table_fields ORDER BY field_order"
            )
            rows = cursor.fetchall()
            return [
                {
                    "field_id": r[0],
                    "field_name": r[1],
                    "field_key": r[2],
                    "doc_type": r[3],
                    "field_order": r[4],
                    "editable": bool(r[5]),
                }
                for r in rows
            ]
        except sqlite3.Error as e:
            raise RuntimeError(f"获取字段配置失败: {e}") from e
        finally:
            conn.close()

    async def update_table_field(
        self, field_id: str, field_name: str
    ) -> bool:
        """更新表头字段的显示名称。

        Args:
            field_id: 字段唯一标识
            field_name: 新的显示名称

        Returns:
            bool: 更新成功返回 True
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE table_fields SET field_name = ? WHERE field_id = ?",
                (field_name, field_id),
            )
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            conn.rollback()
            raise RuntimeError(f"更新字段配置失败: {e}") from e
        finally:
            conn.close()

    async def get_field_by_doc_type(self, doc_type: str) -> dict | None:
        """根据文档类型查找对应的字段配置。

        Args:
            doc_type: 文档类型（如 '企业征信提取'）

        Returns:
            Optional[dict]: 字段配置，未找到返回 None
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT field_id, field_name, field_key, doc_type, "
                "field_order, editable FROM table_fields WHERE doc_type = ?",
                (doc_type,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "field_id": row[0],
                "field_name": row[1],
                "field_key": row[2],
                "doc_type": row[3],
                "field_order": row[4],
                "editable": bool(row[5]),
            }
        except sqlite3.Error as e:
            raise RuntimeError(f"查询字段配置失败: {e}") from e
        finally:
            conn.close()

    async def get_customer_field_data(
        self, customer_id: str
    ) -> dict[str, object]:
        """获取客户在动态字段表中的数据（按 field_key 映射）。

        遍历所有有 doc_type 的字段，从 extractions 表中查找对应数据。
        OCR 字段返回 {"summary": "...", "full": {...}} 结构，
        可编辑字段返回字符串。

        Args:
            customer_id: 客户 ID

        Returns:
            dict: {field_key: str | {"summary": str, "full": dict}}
        """
        result: dict[str, object] = {}
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT field_key, doc_type FROM table_fields ORDER BY field_order"
            )
            fields = cursor.fetchall()

            customer = await self.get_customer(customer_id)
            extractions = await self.get_extractions_by_customer(customer_id)

            ext_by_type: dict[str, list[dict]] = {}
            for ext in extractions:
                etype = ext.get("extraction_type") or ""
                ext_by_type.setdefault(etype, []).append(ext)

            for field_key, doc_type in fields:
                if field_key == "upload_time":
                    result[field_key] = (
                        (customer.get("upload_time") or "") if customer else ""
                    )
                elif field_key == "upload_account":
                    result[field_key] = (
                        (customer.get("uploader") or "") if customer else ""
                    )
                elif field_key == "remark":
                    result[field_key] = ""
                elif doc_type and doc_type in ext_by_type:
                    ext_list = ext_by_type[doc_type]
                    summaries: list[str] = []
                    full_data: dict[str, object] = {}
                    items: list[dict[str, object]] = []
                    for ext in ext_list:
                        data = ext.get("extracted_data") or {}
                        if isinstance(data, dict):
                            summary = _build_extraction_summary(data)
                            if summary:
                                summaries.append(summary)
                            full_data.update(data)
                            items.append(
                                {
                                    "doc_id": ext.get("doc_id") or "",
                                    "extraction_id": ext.get("extraction_id") or "",
                                    "summary": summary,
                                    "full": data,
                                    "editable": True,
                                    "deletable": True,
                                }
                            )
                    result[field_key] = {
                        "summary": " | ".join(summaries) if summaries else "",
                        "full": full_data,
                        "customer_id": customer_id,
                        "doc_id": items[0]["doc_id"] if items else "",
                        "extraction_id": items[0]["extraction_id"] if items else "",
                        "editable": bool(items),
                        "deletable": bool(items),
                        "items": items,
                    }
                else:
                    result[field_key] = ""

            return result
        except sqlite3.Error as e:
            raise RuntimeError(f"获取客户字段数据失败: {e}") from e
        finally:
            conn.close()
