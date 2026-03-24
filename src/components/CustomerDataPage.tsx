import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowLeft, RefreshCw, Trash2, X } from 'lucide-react';
import {
  deleteCustomer,
  deleteCustomerDocument,
  getCustomersTable,
  getTableFields,
  updateCustomerField,
  updateExtractionField,
  updateTableField,
} from '../services/api';
import type { CellFullData, CustomerTableRow, TableField } from '../services/types';

function isCellFullData(value: unknown): value is CellFullData {
  return typeof value === 'object' && value !== null && 'summary' in value && 'full' in value;
}

function getCellPreview(value: string | CellFullData | undefined): string {
  if (!value) return '';
  return isCellFullData(value) ? value.summary : value;
}

function flattenEditableFields(
  data: Record<string, unknown>,
  prefix = ''
): Array<{ path: string; label: string; value: string }> {
  return Object.entries(data).flatMap(([key, value]) => {
    const path = prefix ? `${prefix}.${key}` : key;
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      return flattenEditableFields(value as Record<string, unknown>, path);
    }
    if (Array.isArray(value)) {
      return [];
    }
    return [{ path, label: path, value: value == null ? '' : String(value) }];
  });
}

interface EditableHeaderProps {
  field: TableField;
  onRename: (fieldId: string, newName: string) => Promise<void>;
}

function EditableHeader({ field, onRename }: EditableHeaderProps): React.ReactElement {
  const [editing, setEditing] = useState(false);
  const [localName, setLocalName] = useState(field.field_name);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!editing) setLocalName(field.field_name);
  }, [editing, field.field_name]);

  useEffect(() => {
    if (editing) inputRef.current?.focus();
  }, [editing]);

  const handleBlur = useCallback(async () => {
    setEditing(false);
    const nextName = localName.trim();
    if (!nextName || nextName === field.field_name) return;
    await onRename(field.field_id, nextName);
  }, [field.field_id, field.field_name, localName, onRename]);

  if (editing) {
    return (
      <input
        ref={inputRef}
        value={localName}
        onChange={(e) => setLocalName(e.target.value)}
        onBlur={() => {
          void handleBlur();
        }}
        onKeyDown={(e) => {
          if (e.key === 'Enter') void handleBlur();
          if (e.key === 'Escape') setEditing(false);
        }}
        className="w-full rounded border border-blue-400 bg-white px-1 py-0.5 text-xs font-medium outline-none"
      />
    );
  }

  return (
    <button
      type="button"
      className="cursor-pointer text-left hover:text-blue-600"
      onDoubleClick={() => setEditing(true)}
      title="双击修改列名"
    >
      {field.field_name}
    </button>
  );
}

type SaveStatus = 'idle' | 'saving' | 'saved' | 'error';

interface EditableCellProps {
  customerId: string;
  fieldKey: string;
  value: string;
  saveStatus: SaveStatus;
  onSave: (customerId: string, field: string, newValue: string) => Promise<void>;
}

function EditableCell({
  customerId,
  fieldKey,
  value,
  saveStatus,
  onSave,
}: EditableCellProps): React.ReactElement {
  const [editing, setEditing] = useState(false);
  const [localValue, setLocalValue] = useState(value);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!editing) setLocalValue(value);
  }, [editing, value]);

  useEffect(() => {
    if (editing) inputRef.current?.focus();
  }, [editing]);

  const handleBlur = useCallback(async () => {
    setEditing(false);
    if (localValue === value) return;
    await onSave(customerId, fieldKey, localValue);
  }, [customerId, fieldKey, localValue, onSave, value]);

  if (editing) {
    return (
      <input
        ref={inputRef}
        value={localValue}
        onChange={(e) => setLocalValue(e.target.value)}
        onBlur={() => {
          void handleBlur();
        }}
        onKeyDown={(e) => {
          if (e.key === 'Enter') void handleBlur();
          if (e.key === 'Escape') {
            setLocalValue(value);
            setEditing(false);
          }
        }}
        className="w-full rounded border border-blue-400 bg-white px-2 py-1 text-sm outline-none"
      />
    );
  }

  return (
    <button
      type="button"
      className="flex min-h-[28px] w-full items-center rounded px-1 py-0.5 text-left hover:bg-blue-50"
      onClick={() => setEditing(true)}
    >
      <span className="flex-1 truncate text-sm text-gray-700">
        {localValue || <span className="italic text-gray-300">点击编辑</span>}
      </span>
      {saveStatus === 'saving' && <span className="ml-1 text-xs text-blue-400">保存中</span>}
      {saveStatus === 'saved' && <span className="ml-1 text-xs text-green-500">已保存</span>}
      {saveStatus === 'error' && <span className="ml-1 text-xs text-red-500">失败</span>}
    </button>
  );
}

interface DetailModalProps {
  title: string;
  cellData: CellFullData;
  onClose: () => void;
  onSaveField: (extractionId: string, path: string, value: string) => Promise<void>;
  onDeleteDocument: (docId: string) => Promise<void>;
}

function DetailModal({
  title,
  cellData,
  onClose,
  onSaveField,
  onDeleteDocument,
}: DetailModalProps): React.ReactElement {
  const items =
    cellData.items && cellData.items.length > 0
      ? cellData.items
      : [
          {
            doc_id: cellData.doc_id || '',
            extraction_id: cellData.extraction_id || '',
            summary: cellData.summary,
            full: cellData.full,
            editable: !!cellData.editable,
            deletable: !!cellData.deletable,
          },
        ];

  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [savingKey, setSavingKey] = useState<string | null>(null);

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div className="mx-4 flex max-h-[82vh] w-full max-w-4xl flex-col rounded-xl bg-white shadow-2xl">
        <div className="flex items-center justify-between border-b border-gray-200 px-5 py-3">
          <div>
            <h2 className="text-sm font-semibold text-gray-800">{title}</h2>
            <p className="mt-1 text-xs text-gray-400">支持修改资料字段和删除单份上传资料</p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded p-1 transition-colors hover:bg-gray-100"
            aria-label="关闭"
          >
            <X className="h-4 w-4 text-gray-500" />
          </button>
        </div>

        <div className="space-y-4 overflow-y-auto px-5 py-4">
          {items.map((item, index) => {
            const editableFields = flattenEditableFields(item.full);
            return (
              <section
                key={item.extraction_id || item.doc_id || index}
                className="rounded-xl border border-gray-200 bg-gray-50 p-4"
              >
                <div className="mb-3 flex items-start justify-between gap-3">
                  <div>
                    <div className="text-sm font-medium text-gray-800">资料 {index + 1}</div>
                    <div className="mt-1 text-xs text-gray-500">{item.summary || '无摘要'}</div>
                  </div>
                  {item.deletable && item.doc_id && (
                    <button
                      type="button"
                      className="inline-flex items-center gap-1 rounded-lg border border-red-200 px-2.5 py-1.5 text-xs text-red-600 hover:bg-red-50"
                      onClick={() => {
                        void onDeleteDocument(item.doc_id);
                      }}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                      删除资料
                    </button>
                  )}
                </div>

                {editableFields.length === 0 && (
                  <div className="text-sm text-gray-400">当前资料没有可编辑的文本字段</div>
                )}

                {editableFields.length > 0 && (
                  <div className="grid gap-3 md:grid-cols-2">
                    {editableFields.map((field) => {
                      const fieldKey = `${item.extraction_id}__${field.path}`;
                      const fieldValue = drafts[fieldKey] ?? field.value;
                      const isSaving = savingKey === fieldKey;
                      return (
                        <label key={fieldKey} className="flex flex-col gap-1">
                          <span className="text-xs font-medium text-gray-500">{field.label}</span>
                          <div className="flex gap-2">
                            <input
                              value={fieldValue}
                              onChange={(e) =>
                                setDrafts((prev) => ({ ...prev, [fieldKey]: e.target.value }))
                              }
                              className="flex-1 rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm outline-none focus:border-blue-400"
                            />
                            <button
                              type="button"
                              disabled={
                                !item.editable ||
                                isSaving ||
                                fieldValue === field.value ||
                                !item.extraction_id
                              }
                              onClick={async () => {
                                if (!item.extraction_id) return;
                                setSavingKey(fieldKey);
                                try {
                                  await onSaveField(item.extraction_id, field.path, fieldValue);
                                } finally {
                                  setSavingKey(null);
                                }
                              }}
                              className="rounded-lg border border-blue-200 px-3 py-2 text-xs text-blue-600 hover:bg-blue-50 disabled:cursor-not-allowed disabled:opacity-50"
                            >
                              {isSaving ? '保存中' : '保存'}
                            </button>
                          </div>
                        </label>
                      );
                    })}
                  </div>
                )}
              </section>
            );
          })}
        </div>
      </div>
    </div>
  );
}

interface DataCellProps {
  fieldName: string;
  cellData: CellFullData;
  onSaveField: (extractionId: string, path: string, value: string) => Promise<void>;
  onDeleteDocument: (docId: string) => Promise<void>;
}

function DataCell({
  fieldName,
  cellData,
  onSaveField,
  onDeleteDocument,
}: DataCellProps): React.ReactElement {
  const [modalOpen, setModalOpen] = useState(false);
  const hasData = Object.keys(cellData.full || {}).length > 0;

  return (
    <>
      <button
        type="button"
        className={`group min-h-[28px] w-full rounded px-1 py-0.5 text-left ${hasData ? 'hover:bg-blue-50' : ''}`}
        onClick={() => {
          if (hasData) setModalOpen(true);
        }}
      >
        {cellData.summary ? (
          <span className="line-clamp-2 text-sm leading-snug text-gray-700">{cellData.summary}</span>
        ) : (
          <span className="text-sm italic text-gray-300">无</span>
        )}
        {hasData && (
          <span className="ml-1 text-xs text-blue-400 opacity-0 transition-opacity group-hover:opacity-100">
            展开
          </span>
        )}
      </button>

      {modalOpen && (
        <DetailModal
          title={fieldName}
          cellData={cellData}
          onClose={() => setModalOpen(false)}
          onSaveField={onSaveField}
          onDeleteDocument={async (docId) => {
            await onDeleteDocument(docId);
            setModalOpen(false);
          }}
        />
      )}
    </>
  );
}

interface CustomerDataPageProps {
  onBack?: () => void;
}

const fixedColumns = [
  { key: 'name', label: '客户名称' },
  { key: 'customer_type', label: '类型' },
];

const CustomerDataPage: React.FC<CustomerDataPageProps> = ({ onBack }) => {
  const [fields, setFields] = useState<TableField[]>([]);
  const [rows, setRows] = useState<CustomerTableRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [saveStates, setSaveStates] = useState<Record<string, SaveStatus>>({});

  const loadData = useCallback(async (signal?: AbortSignal) => {
    const [fieldsData, rowsData] = await Promise.all([getTableFields(signal), getCustomersTable(signal)]);
    setFields(fieldsData);
    setRows(rowsData);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    loadData(controller.signal)
      .catch((err: unknown) => {
        if (!(err instanceof Error) || err.name !== 'AbortError') {
          setError('加载数据失败，请重试。');
        }
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [loadData]);

  const handleRenameField = useCallback(async (fieldId: string, newName: string) => {
    await updateTableField(fieldId, newName);
    setFields((prev) =>
      prev.map((field) => (field.field_id === fieldId ? { ...field, field_name: newName } : field))
    );
  }, []);

  const handleSaveCell = useCallback(async (customerId: string, field: string, newValue: string) => {
    const key = `${customerId}__${field}`;
    setSaveStates((prev) => ({ ...prev, [key]: 'saving' }));
    try {
      await updateCustomerField(customerId, field, newValue);
      setRows((prev) =>
        prev.map((row) => (row.customer_id === customerId ? { ...row, [field]: newValue } : row))
      );
      setSaveStates((prev) => ({ ...prev, [key]: 'saved' }));
    } catch {
      setSaveStates((prev) => ({ ...prev, [key]: 'error' }));
    } finally {
      window.setTimeout(() => {
        setSaveStates((prev) => ({ ...prev, [key]: 'idle' }));
      }, 1500);
    }
  }, []);

  const handleSaveExtraction = useCallback(
    async (customerId: string, extractionId: string, path: string, value: string) => {
      await updateExtractionField(customerId, extractionId, path, value);
      await loadData();
    },
    [loadData]
  );

  const handleDeleteDocument = useCallback(
    async (customerId: string, docId: string) => {
      const confirmed = window.confirm('确认删除这份资料吗？删除后不可恢复。');
      if (!confirmed) return;
      await deleteCustomerDocument(customerId, docId);
      await loadData();
    },
    [loadData]
  );

  const handleDeleteCustomer = useCallback(async (customerId: string) => {
    const confirmed = window.confirm('确认删除整行客户及其全部资料吗？删除后不可恢复。');
    if (!confirmed) return;
    await deleteCustomer(customerId);
    setRows((prev) => prev.filter((row) => row.customer_id !== customerId));
  }, []);

  const rowCountText = useMemo(() => `共 ${rows.length} 位客户`, [rows.length]);

  return (
    <div className="flex h-full flex-col bg-white">
      <div className="flex items-center justify-between border-b border-gray-200 bg-white px-6 py-4">
        <div className="flex items-center gap-3">
          {onBack && (
            <button
              type="button"
              onClick={onBack}
              className="rounded-lg p-1.5 transition-colors hover:bg-gray-100"
              aria-label="返回"
            >
              <ArrowLeft className="h-5 w-5 text-gray-500" />
            </button>
          )}
          <div>
            <h1 className="text-lg font-semibold text-gray-800">客户资料汇总</h1>
            <p className="mt-0.5 text-xs text-gray-400">{rowCountText}，支持列名修改、资料编辑与删除</p>
          </div>
        </div>
        <button
          type="button"
          onClick={() => {
            void loadData();
          }}
          className="inline-flex items-center gap-1 rounded-lg border border-gray-200 px-3 py-1.5 text-sm text-gray-500 transition-colors hover:bg-gray-50 hover:text-gray-700"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          刷新
        </button>
      </div>

      <div className="flex-1 overflow-auto">
        {loading && (
          <div className="flex h-40 items-center justify-center">
            <div className="h-6 w-6 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" />
            <span className="ml-2 text-sm text-gray-500">加载中</span>
          </div>
        )}

        {error && !loading && (
          <div className="flex h-40 items-center justify-center">
            <div className="text-center">
              <p className="mb-2 text-sm text-red-500">{error}</p>
              <button
                type="button"
                className="text-sm text-blue-500 hover:underline"
                onClick={() => {
                  void loadData();
                }}
              >
                重试
              </button>
            </div>
          </div>
        )}

        {!loading && !error && rows.length === 0 && (
          <div className="flex h-40 items-center justify-center">
            <p className="text-sm text-gray-400">暂无客户数据</p>
          </div>
        )}

        {!loading && !error && rows.length > 0 && (
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="sticky top-0 z-10 border-b border-gray-200 bg-gray-50">
                <th className="w-10 border-r border-gray-200 px-3 py-2.5 text-left text-xs font-medium text-gray-500">
                  #
                </th>
                {fixedColumns.map((column) => (
                  <th
                    key={column.key}
                    className="min-w-[110px] border-r border-gray-200 px-3 py-2.5 text-left text-xs font-medium text-gray-500"
                  >
                    {column.label}
                  </th>
                ))}
                {fields.map((field) => (
                  <th
                    key={field.field_key}
                    className="min-w-[160px] max-w-[220px] border-r border-gray-200 px-3 py-2.5 text-left text-xs font-medium text-gray-500 last:border-r-0"
                  >
                    <EditableHeader field={field} onRename={handleRenameField} />
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => {
                const customerId = String(row.customer_id ?? '');
                return (
                  <tr
                    key={customerId || index}
                    className={`border-b border-gray-100 transition-colors hover:bg-blue-50/30 ${
                      index % 2 === 0 ? 'bg-white' : 'bg-gray-50/30'
                    }`}
                  >
                    <td className="border-r border-gray-200 px-3 py-1.5 text-center text-xs text-gray-400">
                      {index + 1}
                    </td>
                    <td className="border-r border-gray-100 px-2 py-1 align-top">
                      <div className="flex min-w-[150px] flex-col gap-2">
                        <EditableCell
                          customerId={customerId}
                          fieldKey="name"
                          value={String(row.name ?? '')}
                          saveStatus={saveStates[`${customerId}__name`] || 'idle'}
                          onSave={handleSaveCell}
                        />
                        <button
                          type="button"
                          className="inline-flex w-fit items-center gap-1 rounded-lg border border-red-200 px-2.5 py-1.5 text-xs text-red-600 hover:bg-red-50"
                          onClick={() => {
                            void handleDeleteCustomer(customerId);
                          }}
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                          删除整行
                        </button>
                      </div>
                    </td>
                    <td className="border-r border-gray-100 px-2 py-1">
                      <span
                        className={`inline-block rounded px-2 py-0.5 text-xs font-medium ${
                          getCellPreview(row.customer_type as string) === 'personal'
                            ? 'bg-green-100 text-green-700'
                            : 'bg-blue-100 text-blue-700'
                        }`}
                      >
                        {getCellPreview(row.customer_type as string) === 'personal' ? '个人' : '企业'}
                      </span>
                    </td>
                    {fields.map((field) => {
                      const cellValue = row[field.field_key];
                      return (
                        <td
                          key={field.field_key}
                          className="max-w-[220px] border-r border-gray-100 px-2 py-1.5 last:border-r-0"
                        >
                          {isCellFullData(cellValue) ? (
                            <DataCell
                              fieldName={field.field_name}
                              cellData={cellValue}
                              onSaveField={async (extractionId, path, value) => {
                                await handleSaveExtraction(
                                  String(cellValue.customer_id || customerId),
                                  extractionId,
                                  path,
                                  value
                                );
                              }}
                              onDeleteDocument={async (docId) => {
                                await handleDeleteDocument(String(cellValue.customer_id || customerId), docId);
                              }}
                            />
                          ) : field.editable ? (
                            <EditableCell
                              customerId={customerId}
                              fieldKey={field.field_key}
                              value={String(cellValue ?? '')}
                              saveStatus={saveStates[`${customerId}__${field.field_key}`] || 'idle'}
                              onSave={handleSaveCell}
                            />
                          ) : (
                            <span className="block truncate px-1 text-sm text-gray-500">
                              {String(cellValue ?? '') || '无'}
                            </span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

export default CustomerDataPage;
