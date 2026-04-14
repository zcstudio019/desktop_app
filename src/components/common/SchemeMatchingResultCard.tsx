import React, { useMemo, useState } from 'react';
import {
  AlertCircle,
  Calendar,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  ClipboardList,
  CreditCard,
  DollarSign,
  FileCheck,
  FileText,
  Percent,
  Target,
  User,
  X,
} from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface ParsedScheme {
  方案名称: string;
  银行名称?: string;
  产品名称?: string;
  可贷额度?: string;
  参考利率?: string;
  贷款期限?: string;
  还款方式?: string;
  匹配理由?: string;
  准入条件?: string[];
  审批说明?: string;
  准备材料?: Record<string, string[]>;
  审批流程?: Array<{ 步骤: string; 内容: string; 预计时间: string }>;
  [key: string]:
    | string
    | string[]
    | Record<string, string[]>
    | Array<{ 步骤: string; 内容: string; 预计时间: string }>
    | undefined;
}

interface ParsedMatchingResult {
  客户资料摘要?: Record<string, string>;
  核心发现?: Record<string, string>;
  推荐方案?: ParsedScheme[];
  不推荐产品?: Array<{ 产品: string; 原因: string }>;
  替代建议?: string[];
  需补充信息?: string[];
  待补充资料?: string[] | Record<string, string>;
  下一步建议?: string;
  准备材料?: Record<string, string[]>;
  审批流程?: Array<{ 步骤: string; 内容: string; 预计时间: string }>;
  rawMarkdown?: string;
}

const attachGlobalSchemeDetails = (result: ParsedMatchingResult): ParsedMatchingResult => {
  if (!result.推荐方案 || result.推荐方案.length === 0) {
    return result;
  }

  const hasGlobalMaterials = Boolean(result.准备材料 && Object.keys(result.准备材料).length > 0);
  const hasGlobalProcess = Boolean(result.审批流程 && result.审批流程.length > 0);

  if (!hasGlobalMaterials && !hasGlobalProcess) {
    return result;
  }

  return {
    ...result,
    推荐方案: result.推荐方案.map((scheme) => ({
      ...scheme,
      准备材料: scheme.准备材料 && Object.keys(scheme.准备材料).length > 0
        ? scheme.准备材料
        : result.准备材料,
      审批流程: scheme.审批流程 && scheme.审批流程.length > 0
        ? scheme.审批流程
        : result.审批流程,
    })),
  };
};

const parseMarkdownToSchemes = (markdown: string): ParsedMatchingResult | null => {
  if (!markdown || typeof markdown !== 'string') {
    return null;
  }

  const result: ParsedMatchingResult = {
    rawMarkdown: markdown,
  };

  try {
    const summaryMatch = markdown.match(/(?:.*)?客户资料摘要[^\n]*\n([\s\S]*?\|[\s\S]*?)(?=\n(?:二、|###|🚨|$))/i);
    if (summaryMatch) {
      const tableRows = summaryMatch[0].match(/\|([^|]+)\|([^|]+)\|/g);
      if (tableRows && tableRows.length > 1) {
        const summary: Record<string, string> = {};
        tableRows.slice(1).forEach((row) => {
          if (row.includes('---')) return;
          const cells = row.split('|').filter((cell) => cell.trim());
          if (cells.length >= 2) {
            const key = cells[0].trim();
            const value = cells[1].trim();
            if (key && value && key !== '项目' && key !== '内容') {
              summary[key] = value;
            }
          }
        });
        if (Object.keys(summary).length > 0) {
          result.客户资料摘要 = summary;
        }
      }
    }

    const schemes: ParsedScheme[] = [];
    const schemeRegex = /(?:#{2,4}\s*)?(?:\*\*)?方案\s*(\d+)[：:]\s*【([^】]+)】([^\n*]+)(?:\*\*)?([\s\S]*?)(?=(?:#{2,4}\s*)?(?:\*\*)?方案\s*\d+|###|##|$)/g;
    let schemeMatch: RegExpExecArray | null;

    while ((schemeMatch = schemeRegex.exec(markdown)) !== null) {
      const [, , bankName, productName, content] = schemeMatch;
      const scheme: ParsedScheme = {
        方案名称: `【${bankName}】${productName.trim()}`,
        银行名称: bankName.trim(),
        产品名称: productName.trim(),
      };

      const lines = content.split('\n');
      const conditions: string[] = [];

      for (const line of lines) {
        const trimmedLine = line.trim();
        if (!trimmedLine || trimmedLine.startsWith('#')) continue;

        const kvMatch = trimmedLine.match(/^-\s*([^：:]+)[：:](.+)$/);
        if (kvMatch) {
          const [, key, value] = kvMatch;
          const cleanKey = key.trim();
          const cleanValue = value.trim();

          if (cleanKey === '准入条件核对' || cleanKey === '准入条件') {
            continue;
          }

          if (cleanKey.includes('额度')) {
            scheme.可贷额度 = cleanValue;
          } else if (cleanKey.includes('利率')) {
            scheme.参考利率 = cleanValue;
          } else if (cleanKey.includes('期限')) {
            scheme.贷款期限 = cleanValue;
          } else if (cleanKey.includes('还款')) {
            scheme.还款方式 = cleanValue;
          } else {
            scheme[cleanKey] = cleanValue;
          }
        }

        const conditionMatch = trimmedLine.match(/^-\s*(✅|⚠️|❌)\s*(.+)$/u);
        if (conditionMatch) {
          conditions.push(`${conditionMatch[1]} ${conditionMatch[2]}`);
        }
      }

      if (conditions.length > 0) {
        scheme.准入条件 = conditions;
      }

      schemes.push(scheme);
    }

    if (schemes.length === 0) {
      const altSchemeRegex = /####\s*方案\s*(\d+)[：:]\s*([^\n]+)([\s\S]*?)(?=####\s*方案|###|$)/g;
      while ((schemeMatch = altSchemeRegex.exec(markdown)) !== null) {
        const [, , title, content] = schemeMatch;
        const scheme: ParsedScheme = {
          方案名称: title.trim(),
        };

        const bankMatch = title.match(/【([^】]+)】(.+)/);
        if (bankMatch) {
          scheme.银行名称 = bankMatch[1].trim();
          scheme.产品名称 = bankMatch[2].trim();
        }

        const lines = content.split('\n');
        const conditions: string[] = [];

        for (const line of lines) {
          const trimmedLine = line.trim();
          if (!trimmedLine || trimmedLine.startsWith('#')) continue;

          const kvMatch = trimmedLine.match(/^-\s*([^：:]+)[：:](.+)$/);
          if (kvMatch) {
            const [, key, value] = kvMatch;
            const cleanKey = key.trim();
            const cleanValue = value.trim();

            if (cleanKey.includes('额度')) {
              scheme.可贷额度 = cleanValue;
            } else if (cleanKey.includes('利率')) {
              scheme.参考利率 = cleanValue;
            } else if (cleanKey.includes('期限')) {
              scheme.贷款期限 = cleanValue;
            } else if (cleanKey.includes('还款')) {
              scheme.还款方式 = cleanValue;
            } else if (!cleanKey.includes('准入条件')) {
              scheme[cleanKey] = cleanValue;
            }
          }

          const conditionMatch = trimmedLine.match(/^-\s*(✅|⚠️|❌)\s*(.+)$/u);
          if (conditionMatch) {
            conditions.push(`${conditionMatch[1]} ${conditionMatch[2]}`);
          }
        }

        if (conditions.length > 0) {
          scheme.准入条件 = conditions;
        }

        if (scheme.方案名称) {
          schemes.push(scheme);
        }
      }
    }

    if (schemes.length > 0) {
      result.推荐方案 = schemes;
    }

    const notRecommendMatch = markdown.match(/###?\s*三、不推荐的产品及原因[\s\S]*?\|[\s\S]*?(?=###|$)/);
    if (notRecommendMatch) {
      const tableRows = notRecommendMatch[0].match(/\|([^|]+)\|([^|]+)\|/g);
      if (tableRows && tableRows.length > 1) {
        const notRecommended: Array<{ 产品: string; 原因: string }> = [];
        tableRows.slice(1).forEach((row) => {
          if (row.includes('---')) return;
          const cells = row.split('|').filter((cell) => cell.trim());
          if (cells.length >= 2) {
            const product = cells[0].trim();
            const reason = cells[1].trim();
            if (product && reason && product !== '产品' && product !== '不符合原因') {
              notRecommended.push({ 产品: product, 原因: reason });
            }
          }
        });
        if (notRecommended.length > 0) {
          result.不推荐产品 = notRecommended;
        }
      }
    }

    const alternativeMatch = markdown.match(/###?\s*四、替代建议[\s\S]*?(?=###|$)/);
    if (alternativeMatch) {
      const listItems = alternativeMatch[0].match(/^-\s+(.+)$/gm);
      if (listItems && listItems.length > 0) {
        result.替代建议 = listItems.map((item) => item.replace(/^-\s+/, '').trim());
      }
    }

    const supplementMatch = markdown.match(/###?\s*五、需补充信息[\s\S]*?(?=###|$)/);
    if (supplementMatch) {
      const listItems = supplementMatch[0].match(/^\d+\.\s+(.+)$/gm);
      if (listItems && listItems.length > 0) {
        result.需补充信息 = listItems.map((item) => item.replace(/^\d+\.\s+/, '').trim());
      }
    }

    const materialsMatch = markdown.match(/(?:#{2,4}\s*)?(?:六、)?准备材料[^\n]*\n([\s\S]*?)(?=(?:#{2,4}\s*)?(?:七、|审批流程|$))/i);
    if (materialsMatch) {
      const materialsContent = materialsMatch[1];
      const materials: Record<string, string[]> = {};
      let currentCategory = '其他材料';

      for (const line of materialsContent.split('\n')) {
        const trimmed = line.trim();
        if (!trimmed) continue;

        const categoryMatch = trimmed.match(/^\*\*([^*]+)\*\*[：:]*$/);
        if (categoryMatch) {
          currentCategory = categoryMatch[1].replace(/[（(][^）)]*[）)]/g, '').trim();
          if (!materials[currentCategory]) materials[currentCategory] = [];
          continue;
        }

        const itemMatch = trimmed.match(/^[-•]\s+(.+)$/);
        if (itemMatch) {
          if (!materials[currentCategory]) materials[currentCategory] = [];
          materials[currentCategory].push(itemMatch[1].trim());
        }
      }

      if (Object.keys(materials).length > 0) {
        result.准备材料 = materials;
      }
    }

    const processMatch = markdown.match(/(?:#{2,4}\s*)?(?:七、)?审批流程[^\n]*\n([\s\S]*?)(?=(?:#{2,4}\s*)?(?:八、|⚠️|---|\n##|\n#|$))/i);
    if (processMatch) {
      const processContent = processMatch[1];
      const steps: Array<{ 步骤: string; 内容: string; 预计时间: string }> = [];
      const tableRows = processContent.match(/\|([^|]+)\|([^|]+)\|([^|]+)\|/g);
      if (tableRows) {
        for (const row of tableRows) {
          if (row.includes('---') || row.includes('步骤') || row.includes('内容')) continue;
          const cells = row.split('|').filter((cell) => cell.trim());
          if (cells.length >= 3) {
            const step = cells[0].trim().replace(/^\d+\.\s*/, '');
            const content = cells[1].trim();
            const time = cells[2].trim();
            if (step && content) {
              steps.push({ 步骤: step, 内容: content, 预计时间: time });
            }
          }
        }
      }

      if (steps.length > 0) {
        result.审批流程 = steps;
      }
    }

    const normalized = attachGlobalSchemeDetails(result);
    const hasUsefulData =
      (normalized.推荐方案 && normalized.推荐方案.length > 0) ||
      (normalized.客户资料摘要 && Object.keys(normalized.客户资料摘要).length > 0) ||
      (normalized.不推荐产品 && normalized.不推荐产品.length > 0) ||
      (normalized.替代建议 && normalized.替代建议.length > 0) ||
      (normalized.需补充信息 && normalized.需补充信息.length > 0) ||
      (normalized.准备材料 && Object.keys(normalized.准备材料).length > 0) ||
      (normalized.审批流程 && normalized.审批流程.length > 0);

    return hasUsefulData ? normalized : null;
  } catch (error) {
    console.error('Failed to parse scheme matching result:', error);
    return null;
  }
};

const DataSectionCard: React.FC<{
  title: string;
  data: Record<string, string>;
  icon: React.ReactNode;
  iconBgClassName: string;
}> = ({ title, data, icon, iconBgClassName }) => (
  <div className="rounded-2xl border border-slate-200 bg-white overflow-hidden">
    <div className="border-b border-slate-100 bg-slate-50 px-4 py-3">
      <div className="flex items-center gap-2">
        <div className={`flex h-8 w-8 items-center justify-center rounded-lg ${iconBgClassName}`}>{icon}</div>
        <span className="text-sm font-semibold text-slate-800">{title}</span>
      </div>
    </div>
    <div className="divide-y divide-slate-100">
      {Object.entries(data).map(([key, value]) => (
        <div key={key} className="grid gap-2 px-4 py-3 sm:grid-cols-[180px_minmax(0,1fr)]">
          <div className="text-sm font-medium text-slate-500">{key}</div>
          <div className="text-sm leading-6 text-slate-700">{value}</div>
        </div>
      ))}
    </div>
  </div>
);

const ParsedSchemeCard: React.FC<{ scheme: ParsedScheme; index: number }> = ({ scheme, index }) => {
  const [showConditions, setShowConditions] = useState(true);
  const keyFields = ['可贷额度', '参考利率', '贷款期限', '还款方式'];
  const excludeFields = ['方案名称', '银行名称', '产品名称', '准入条件', '准备材料', '审批流程', '审批说明', ...keyFields];
  const otherFields = Object.entries(scheme).filter(([key, value]) => !excludeFields.includes(key) && typeof value === 'string');
  const materials = scheme.准备材料;
  const process = scheme.审批流程;

  return (
    <div className="overflow-hidden rounded-[22px] border border-emerald-200 bg-gradient-to-r from-emerald-50 via-white to-cyan-50">
      <div className="flex items-center gap-3 border-b border-emerald-100 px-5 py-4">
        <span className="flex h-9 w-9 items-center justify-center rounded-full bg-emerald-500 text-sm font-bold text-white">
          {index + 1}
        </span>
        <div className="min-w-0 flex-1">
          <div className="text-lg font-semibold text-emerald-900">{scheme.方案名称 || `方案 ${index + 1}`}</div>
          {(scheme.银行名称 || scheme.产品名称) && (
            <div className="mt-1 text-sm text-emerald-700">
              {scheme.银行名称 ? `银行名称：${scheme.银行名称}` : ''}{scheme.银行名称 && scheme.产品名称 ? ' · ' : ''}{scheme.产品名称 ? `产品名称：${scheme.产品名称}` : ''}
            </div>
          )}
        </div>
      </div>

      <div className="grid gap-3 px-5 py-4 md:grid-cols-2">
        {keyFields.map((field) => {
          const value = scheme[field];
          if (!value || typeof value !== 'string') return null;
          const icons: Record<string, React.ReactNode> = {
            可贷额度: <DollarSign className="h-4 w-4" />,
            参考利率: <Percent className="h-4 w-4" />,
            贷款期限: <Calendar className="h-4 w-4" />,
            还款方式: <CreditCard className="h-4 w-4" />,
          };

          return (
            <div key={field} className="flex items-start gap-3 rounded-2xl border border-emerald-100 bg-white/80 p-4">
              <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-emerald-100 text-emerald-700">
                {icons[field] || <FileText className="h-4 w-4" />}
              </div>
              <div className="min-w-0">
                <div className="text-xs font-medium text-slate-500">{field}</div>
                <div className="mt-1 text-sm font-semibold leading-6 text-slate-800">{value}</div>
              </div>
            </div>
          );
        })}
      </div>

      {otherFields.length > 0 && (
        <div className="space-y-2 px-5 pb-4 text-sm leading-6 text-slate-700">
          {otherFields.map(([key, value]) => (
            <div key={key}>
              <span className="font-medium text-slate-500">{key}：</span>
              <span>{value as string}</span>
            </div>
          ))}
        </div>
      )}

      {scheme.准入条件 && scheme.准入条件.length > 0 && (
        <div className="border-t border-emerald-100 px-5 py-4">
          <button
            type="button"
            onClick={() => setShowConditions((current) => !current)}
            className="inline-flex items-center gap-2 text-sm font-medium text-emerald-700"
          >
            {showConditions ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
            准入条件核对
            <span className="rounded-full bg-emerald-100 px-2 py-0.5 text-xs text-emerald-700">{scheme.准入条件.length} 项</span>
          </button>
          {showConditions ? (
            <div className="mt-3 space-y-2 text-sm">
              {scheme.准入条件.map((condition, idx) => (
                <div
                  key={`${condition}-${idx}`}
                  className={
                    condition.startsWith('✅')
                      ? 'text-emerald-700'
                      : condition.startsWith('⚠️')
                        ? 'text-amber-700'
                        : condition.startsWith('❌')
                          ? 'text-rose-700'
                          : 'text-slate-600'
                  }
                >
                  {condition}
                </div>
              ))}
            </div>
          ) : null}
        </div>
      )}

      {materials && Object.keys(materials).length > 0 && (
        <div className="border-t border-emerald-100 px-5 py-4">
          <div className="mb-3 flex items-center gap-2">
            <FileCheck className="h-4 w-4 text-cyan-600" />
            <span className="text-sm font-semibold text-cyan-700">准备材料</span>
          </div>
          <div className="space-y-3">
            {Object.entries(materials).map(([category, items]) => (
              <div
                key={category}
                className="rounded-2xl border border-cyan-100 bg-gradient-to-r from-cyan-50/80 to-white p-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.92)]"
              >
                <div className="mb-2 text-xs font-semibold tracking-wide text-slate-500">{category}</div>
                <div className="flex flex-wrap gap-2">
                  {items.map((item) => (
                    <span
                      key={`${category}-${item}`}
                      className="rounded-md border border-cyan-100 bg-cyan-50 px-3 py-1.5 text-xs font-medium text-cyan-700"
                    >
                      {item}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {((process && process.length > 0) || scheme.审批说明) && (
        <div className="border-t border-emerald-100 px-5 py-4">
          <div className="mb-3 flex items-center gap-2">
            <ClipboardList className="h-4 w-4 text-teal-600" />
            <span className="text-sm font-semibold text-teal-700">审批流程</span>
          </div>
          {process && process.length > 0 ? (
            <div className="rounded-2xl border border-teal-100 bg-gradient-to-r from-teal-50/80 to-white p-3">
              <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600">
              {process.map((step, idx) => (
                <React.Fragment key={`${step.步骤}-${idx}`}>
                  <div className="min-w-[148px] flex-1 rounded-2xl border border-teal-100 bg-white px-3 py-3 shadow-[0_8px_20px_rgba(13,148,136,0.08)]">
                    <div className="flex items-center gap-2">
                    <span className="flex h-6 w-6 items-center justify-center rounded-full bg-teal-100 text-[11px] font-semibold">
                      {idx + 1}
                    </span>
                    <span className="text-sm font-semibold text-teal-700">{step.步骤}</span>
                    </div>
                    <div className="mt-2 text-xs leading-5 text-slate-600">{step.内容}</div>
                    {step.预计时间 ? <div className="mt-2 text-[11px] font-medium text-slate-400">{step.预计时间}</div> : null}
                  </div>
                  {idx < process.length - 1 ? <span className="text-teal-300">→</span> : null}
                </React.Fragment>
              ))}
              </div>
            </div>
          ) : null}
          {scheme.审批说明 ? (
            <div className="mt-3 rounded-xl border border-teal-100 bg-teal-50 px-3 py-2 text-xs leading-6 text-slate-700">
              {scheme.审批说明}
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
};

const normalizeStructuredMatchingData = (data: Record<string, unknown> | null | undefined): ParsedMatchingResult | null => {
  if (!data || typeof data !== 'object') {
    return null;
  }

  const normalized: ParsedMatchingResult = {
    核心发现: typeof data['核心发现'] === 'object' && data['核心发现'] ? data['核心发现'] as Record<string, string> : undefined,
    客户资料摘要: typeof data['客户资料摘要'] === 'object' && data['客户资料摘要'] ? data['客户资料摘要'] as Record<string, string> : undefined,
    推荐方案: Array.isArray(data['推荐方案']) ? data['推荐方案'] as ParsedScheme[] : undefined,
    不推荐产品: Array.isArray(data['不推荐产品']) ? data['不推荐产品'] as Array<{ 产品: string; 原因: string }> : undefined,
    替代建议: Array.isArray(data['替代建议']) ? data['替代建议'] as string[] : undefined,
    需补充信息: Array.isArray(data['需补充信息']) ? data['需补充信息'] as string[] : undefined,
    待补充资料: (Array.isArray(data['待补充资料']) || typeof data['待补充资料'] === 'object') ? data['待补充资料'] as string[] | Record<string, string> : undefined,
    下一步建议: typeof data['下一步建议'] === 'string' ? data['下一步建议'] as string : undefined,
    准备材料: typeof data['准备材料'] === 'object' && data['准备材料'] ? data['准备材料'] as Record<string, string[]> : undefined,
    审批流程: Array.isArray(data['审批流程']) ? data['审批流程'] as Array<{ 步骤: string; 内容: string; 预计时间: string }> : undefined,
  };

  return attachGlobalSchemeDetails(normalized);
};

const SupplementSection: React.FC<{ value: ParsedMatchingResult['待补充资料'] | ParsedMatchingResult['需补充信息'] }> = ({ value }) => {
  if (!value) return null;

  const items = Array.isArray(value)
    ? value.map((item) => ({ label: '', value: item }))
    : Object.entries(value).map(([label, content]) => ({ label, value: content }));

  if (items.length === 0) return null;

  return (
    <div className="rounded-2xl border border-amber-200 bg-gradient-to-r from-amber-50 to-orange-50 p-4">
      <div className="mb-2 flex items-center gap-2">
        <AlertCircle className="h-4 w-4 text-amber-600" />
        <span className="text-sm font-semibold text-amber-700">待补充资料</span>
      </div>
      <div className="space-y-2">
        {items.map((item, idx) => (
          <div key={`${item.label}-${idx}`} className="flex items-start gap-2 text-sm leading-6 text-slate-700">
            <span className="font-medium text-amber-500">{idx + 1}.</span>
            <span>
              {item.label ? <span className="font-medium text-slate-600">{item.label}：</span> : null}
              {item.value}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
};

const ParsedMatchingResultDisplay: React.FC<{ data: ParsedMatchingResult }> = ({ data }) => (
  <div className="space-y-4">
    {data.核心发现 && Object.keys(data.核心发现).length > 0 ? (
      <DataSectionCard
        title="核心发现"
        data={data.核心发现}
        icon={<Target className="h-4 w-4" />}
        iconBgClassName="bg-violet-100 text-violet-600"
      />
    ) : null}

    {data.客户资料摘要 && Object.keys(data.客户资料摘要).length > 0 ? (
      <DataSectionCard
        title="客户资料摘要"
        data={data.客户资料摘要}
        icon={<User className="h-4 w-4" />}
        iconBgClassName="bg-indigo-100 text-indigo-600"
      />
    ) : null}

    {data.推荐方案 && data.推荐方案.length > 0 ? (
      <div className="overflow-hidden rounded-2xl border border-slate-200 bg-white">
        <div className="border-b border-slate-100 bg-gradient-to-r from-emerald-50 to-cyan-50 px-4 py-3">
          <div className="flex items-center gap-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-emerald-100 text-emerald-700">
              <CheckCircle2 className="h-4 w-4" />
            </div>
            <span className="text-sm font-semibold text-slate-800">推荐方案</span>
            <span className="text-xs text-slate-400">({data.推荐方案.length} 个)</span>
          </div>
        </div>
        <div className="space-y-4 p-4">
          {data.推荐方案.map((scheme, idx) => (
            <ParsedSchemeCard key={`${scheme.方案名称}-${idx}`} scheme={scheme} index={idx} />
          ))}
        </div>
      </div>
    ) : null}

    {data.不推荐产品 && data.不推荐产品.length > 0 ? (
      <div className="overflow-hidden rounded-2xl border border-slate-200 bg-white">
        <div className="border-b border-slate-100 bg-slate-50 px-4 py-3">
          <div className="flex items-center gap-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-slate-200 text-slate-600">
              <X className="h-4 w-4" />
            </div>
            <span className="text-sm font-semibold text-slate-800">不推荐产品</span>
          </div>
        </div>
        <div className="space-y-2 p-4">
          {data.不推荐产品.map((item, idx) => (
            <div key={`${item.产品}-${idx}`} className="rounded-xl border border-slate-100 bg-slate-50 p-3">
              <div className="text-sm font-medium text-slate-800">{item.产品}</div>
              <div className="mt-1 text-sm leading-6 text-slate-600">{item.原因}</div>
            </div>
          ))}
        </div>
      </div>
    ) : null}

    {data.替代建议 && data.替代建议.length > 0 ? (
      <div className="rounded-2xl border border-purple-200 bg-gradient-to-r from-purple-50 to-indigo-50 p-4">
        <div className="mb-2 flex items-center gap-2">
          <Target className="h-4 w-4 text-purple-600" />
          <span className="text-sm font-semibold text-purple-700">替代建议</span>
        </div>
        <div className="space-y-2">
          {data.替代建议.map((item, idx) => (
            <div key={`${item}-${idx}`} className="flex items-start gap-2 text-sm leading-6 text-slate-700">
              <span className="text-purple-400">•</span>
              <span>{item}</span>
            </div>
          ))}
        </div>
      </div>
    ) : null}

    <SupplementSection value={data.待补充资料 || data.需补充信息} />

    {data.下一步建议 ? (
      <div className="rounded-2xl border border-purple-200 bg-gradient-to-r from-purple-50 to-indigo-50 p-4">
        <div className="mb-2 flex items-center gap-2">
          <Target className="h-4 w-4 text-purple-600" />
          <span className="text-sm font-semibold text-purple-700">下一步建议</span>
        </div>
        <div className="text-sm leading-6 text-slate-700">{data.下一步建议}</div>
      </div>
    ) : null}
  </div>
);

const SchemeMatchingResultCard: React.FC<{ matchResult: string; matchingData?: Record<string, unknown> | null }> = ({ matchResult, matchingData }) => {
  const structuredData = useMemo(() => normalizeStructuredMatchingData(matchingData), [matchingData]);
  const parsedData = useMemo(() => parseMarkdownToSchemes(matchResult), [matchResult]);

  if (structuredData) {
    return <ParsedMatchingResultDisplay data={structuredData} />;
  }

  if (parsedData) {
    return <ParsedMatchingResultDisplay data={parsedData} />;
  }

  return (
    <div
      className="prose prose-sm max-w-none overflow-x-auto text-slate-700
        prose-headings:text-slate-800 prose-headings:font-semibold
        prose-h1:border-b prose-h1:border-slate-200 prose-h1:pb-2 prose-h1:text-lg
        prose-h2:mt-4 prose-h2:text-base prose-h2:text-blue-700
        prose-h3:text-sm prose-h3:text-slate-700
        prose-h4:text-sm prose-h4:font-medium prose-h4:text-emerald-700
        prose-strong:text-slate-800
        prose-table:w-full prose-table:border-collapse prose-table:text-sm
        prose-th:border prose-th:border-slate-300 prose-th:bg-slate-100 prose-th:px-3 prose-th:py-2 prose-th:text-left
        prose-td:border prose-td:border-slate-300 prose-td:px-3 prose-td:py-2
        prose-ul:my-2 prose-li:my-0.5 prose-p:my-2 prose-p:leading-relaxed"
      data-testid="scheme-matching-markdown-content"
    >
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{matchResult}</ReactMarkdown>
    </div>
  );
};

export default SchemeMatchingResultCard;
