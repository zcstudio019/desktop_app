import React from 'react';
import {
  exportCustomerFinancingDiagnosticReportSnapshotDocx,
  exportCustomerFinancingDiagnosticReportSnapshotPdf,
  getCustomerFinancingDiagnosticReportSnapshot,
  listCustomerFinancingDiagnosticReportSnapshots,
  saveCustomerFinancingDiagnosticReportSnapshot,
} from '../services/api';
import type {
  CustomerFinancingDiagnosticReport,
  FinancingDiagnosticReportSnapshotDetail,
  FinancingDiagnosticReportSnapshotSummary,
} from '../services/types';

const READINESS_LABELS: Record<string, string> = {
  not_ready: '未就绪',
  basic_ready: '基本就绪',
  ready: '已就绪',
};

const REPORT_STATUS_LABELS: Record<string, string> = {
  draft: '草稿报告',
};

const CREDIT_STATUS_LABELS: Record<string, string> = {
  unknown: '未知',
  normal: '正常',
  attention: '需关注',
  risky: '风险较高',
};

const QUERY_RISK_LABELS: Record<string, string> = {
  unknown: '未知',
  low: '低',
  medium: '中',
  high: '高',
};

const OVERALL_STATUS_LABELS: Record<string, string> = {
  not_ready: '暂不建议进件',
  cautious: '谨慎推进',
  recommendable: '可推进',
  high_quality: '优质客户',
};

const PRODUCT_TYPE_LABELS: Record<string, string> = {
  mortgage_loan: '抵押类贷款',
  credit_business_loan: '信用类经营贷',
  tax_invoice_loan: '税票贷/发票贷',
  bank_flow_loan: '流水贷',
  renewal_or_refinance: '续贷/置换',
  short_term_turnover: '短期周转',
  defer_application: '暂缓申请',
};

const FIT_LEVEL_LABELS: Record<string, string> = {
  high: '高',
  medium: '中',
  low: '低',
  not_suitable: '不适合',
};

function formatMoney(value: unknown) {
  if (value === null || value === undefined || value === '') return '未识别';
  return `${value}`;
}

function formatRate(value: unknown) {
  if (value === null || value === undefined || value === '') return '未识别';
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  return `${(numeric * 100).toFixed(2)}%`;
}

function formatConsistency(value: unknown) {
  if (value === null || value === undefined) return '未识别';
  return value ? '一致' : '不一致';
}

function formatDateTime(value: string | undefined) {
  if (!value) return '未记录';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString('zh-CN', {
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatLoanItem(item: Record<string, unknown>) {
  const institution = String(item.institution || item.bank || '未识别机构');
  const balance = item.balance === null || item.balance === undefined || item.balance === '' ? '余额未识别' : `余额${item.balance}`;
  const dueDate = item.due_date ? `到期日${item.due_date}` : '';
  const classification = item.classification ? `五级分类${item.classification}` : '';
  return [institution, balance, dueDate, classification].filter(Boolean).join('，');
}

function formatCreditRecord(item: Record<string, unknown>) {
  const institution = String(item.institution || item.issuer || '未识别机构');
  const amount = item.amount === null || item.amount === undefined || item.amount === '' ? '' : `金额${item.amount}`;
  const months = item.months === null || item.months === undefined || item.months === '' ? '' : `月份${item.months}`;
  const status = item.status ? `状态${item.status}` : '';
  const account = item.account_number ? `账号${item.account_number}` : '';
  return [institution, amount, months, status, account].filter(Boolean).join('，');
}

function ListBlock({ title, items, emptyText }: { title: string; items: string[]; emptyText: string }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
      <div className="text-sm font-semibold text-slate-800">{title}</div>
      {items.length > 0 ? (
        <ul className="mt-2 space-y-1 text-sm text-slate-700">
          {items.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      ) : (
        <div className="mt-2 text-sm text-slate-500">{emptyText}</div>
      )}
    </div>
  );
}

const CustomerFinancingDiagnosticReportPanel: React.FC<{
  customerId?: string | null;
  report: CustomerFinancingDiagnosticReport | null;
  loading?: boolean;
  userRole?: string;
}> = ({ customerId, report, loading, userRole }) => {
  const canSaveSnapshot = userRole === 'admin' || userRole === 'operator';
  const [snapshots, setSnapshots] = React.useState<FinancingDiagnosticReportSnapshotSummary[]>([]);
  const [snapshotDetail, setSnapshotDetail] = React.useState<FinancingDiagnosticReportSnapshotDetail | null>(null);
  const [snapshotLoading, setSnapshotLoading] = React.useState(false);
  const [snapshotSaving, setSnapshotSaving] = React.useState(false);
  const [snapshotExporting, setSnapshotExporting] = React.useState<'docx' | 'pdf' | null>(null);
  const [snapshotError, setSnapshotError] = React.useState<string | null>(null);

  const loadSnapshots = React.useCallback(async () => {
    if (!customerId) {
      setSnapshots([]);
      setSnapshotDetail(null);
      return;
    }
    setSnapshotLoading(true);
    setSnapshotError(null);
    try {
      const result = await listCustomerFinancingDiagnosticReportSnapshots(customerId);
      setSnapshots(result);
    } catch (err) {
      setSnapshotError(err instanceof Error ? err.message : '历史快照加载失败');
      setSnapshots([]);
    } finally {
      setSnapshotLoading(false);
    }
  }, [customerId]);

  React.useEffect(() => {
    void loadSnapshots();
  }, [loadSnapshots]);

  const handleSaveSnapshot = React.useCallback(async () => {
    if (!customerId || snapshotSaving) return;
    setSnapshotSaving(true);
    setSnapshotError(null);
    try {
      const saved = await saveCustomerFinancingDiagnosticReportSnapshot(customerId);
      await loadSnapshots();
      if (saved.report_id) {
        const detail = await getCustomerFinancingDiagnosticReportSnapshot(customerId, saved.report_id);
        setSnapshotDetail(detail);
      }
    } catch (err) {
      setSnapshotError(err instanceof Error ? err.message : '报告快照保存失败');
    } finally {
      setSnapshotSaving(false);
    }
  }, [customerId, loadSnapshots, snapshotSaving]);

  const handleOpenSnapshot = React.useCallback(async (reportId: string) => {
    if (!customerId || !reportId) return;
    setSnapshotLoading(true);
    setSnapshotError(null);
    try {
      const detail = await getCustomerFinancingDiagnosticReportSnapshot(customerId, reportId);
      setSnapshotDetail(detail);
    } catch (err) {
      setSnapshotError(err instanceof Error ? err.message : '历史快照详情加载失败');
    } finally {
      setSnapshotLoading(false);
    }
  }, [customerId]);

  const handleExportSnapshot = React.useCallback(async (format: 'docx' | 'pdf') => {
    if (!customerId || !snapshotDetail?.id || snapshotExporting) return;
    setSnapshotExporting(format);
    setSnapshotError(null);
    try {
      if (format === 'docx') {
        await exportCustomerFinancingDiagnosticReportSnapshotDocx(customerId, snapshotDetail.id);
      } else {
        await exportCustomerFinancingDiagnosticReportSnapshotPdf(customerId, snapshotDetail.id);
      }
    } catch (err) {
      const fallback = format === 'pdf' ? 'PDF 导出暂不可用，请先导出 Word' : 'Word 导出失败';
      setSnapshotError(err instanceof Error ? err.message || fallback : fallback);
    } finally {
      setSnapshotExporting(null);
    }
  }, [customerId, snapshotDetail?.id, snapshotExporting]);

  if (loading) {
    return (
      <section className="border-b border-slate-200 bg-white px-6 py-5 text-sm text-slate-500">
        正在生成客户融资诊断报告...
      </section>
    );
  }

  if (!report) {
    return (
      <section className="border-b border-slate-200 bg-white px-6 py-5">
        <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-center text-sm text-slate-500">
          暂无足够资料，当前为初步报告
        </div>
      </section>
    );
  }

  const summary = report.customer_summary || {};
  const readiness = report.financing_readiness || {
    usable_for_financing: false,
    readiness_level: 'not_ready',
    score: 0,
    summary: '',
  };
  const checklist = report.material_checklist || {
    required_missing: [],
    optional_missing: [],
    recommended_supplements: [],
  };
  const enterpriseCredit = report.enterprise_credit_diagnostic;
  const debtSummary = enterpriseCredit?.debt_summary || {};
  const loanSummary = enterpriseCredit?.loan_summary || {
    active_loan_count: 0,
    upcoming_due_loans: [],
    overdue_loans: [],
    abnormal_classification_loans: [],
  };
  const guaranteeSummary = enterpriseCredit?.guarantee_summary || {
    has_external_guarantee: false,
    external_guarantee_balance: null,
    guarantee_risks: [],
  };
  const personalCredit = report.personal_credit_diagnostic;
  const personalDebt = personalCredit?.debt_summary || {};
  const overdueSummary = personalCredit?.overdue_summary || {
    has_loan_overdue: false,
    has_credit_card_overdue: false,
    overdue_records: [],
  };
  const querySummary = personalCredit?.query_summary || {
    last_3_months_query_count: null,
    last_6_months_query_count: null,
    query_risk_level: 'unknown',
  };
  const seriousNegativeSummary = personalCredit?.serious_negative_summary || {
    has_serious_negative: false,
    items: [],
  };
  const enterpriseFlow = report.enterprise_bank_flow_diagnostic;
  const flowSummary = enterpriseFlow?.summary_metrics || {
    period_start: null,
    period_end: null,
    month_count: 0,
    total_income: null,
    total_expense: null,
    net_income: null,
    average_monthly_income: null,
    average_monthly_expense: null,
    average_monthly_net_income: null,
  };
  const flowQuality = enterpriseFlow?.quality_metrics || {
    stable_month_count: 0,
    zero_or_low_income_month_count: 0,
    large_in_out_count: 0,
    internal_transfer_amount: null,
    internal_transfer_ratio: null,
    real_income_amount: null,
    real_income_ratio: null,
  };
  const flowConsistency = enterpriseFlow?.account_consistency || {
    account_name: '',
    company_name: '',
    is_consistent: null,
    warnings: [],
  };
  const financialStatement = report.financial_statement_diagnostic;
  const financialPeriod = financialStatement?.period || {
    latest_period: null,
    statement_type: null,
  };
  const profitability = financialStatement?.profitability || {
    revenue: null,
    operating_cost: null,
    gross_profit: null,
    net_profit: null,
    net_profit_margin: null,
  };
  const debtCapacity = financialStatement?.debt_capacity || {
    total_assets: null,
    total_liabilities: null,
    owner_equity: null,
    asset_liability_ratio: null,
    short_term_borrowing: null,
    long_term_borrowing: null,
  };
  const liquidity = financialStatement?.liquidity || {
    current_assets: null,
    current_liabilities: null,
    current_ratio: null,
    cash_balance: null,
  };
  const cashFlow = financialStatement?.cash_flow || {
    operating_cash_flow_net: null,
  };
  const comprehensiveAdvice = report.comprehensive_financing_advice || {
    overall_status: 'not_ready',
    financing_readiness_score: 0,
    recommended_product_directions: [],
    main_shortcomings: [],
    key_strengths: [],
    priority_actions: [],
    risk_summary: [],
    sales_follow_up_script: '暂不建议进件，请先补齐资料',
    summary: '暂不建议进件，请先补齐资料',
  };
  const readinessLabel = READINESS_LABELS[readiness.readiness_level] || readiness.readiness_level || '未就绪';
  const reportStatusLabel = REPORT_STATUS_LABELS[report.report_status] || report.report_status || '草稿报告';
  const overallStatusLabel =
    OVERALL_STATUS_LABELS[comprehensiveAdvice.overall_status] || comprehensiveAdvice.overall_status || '暂不建议进件';
  const usableText = readiness.usable_for_financing
    ? '是，已具备初步融资评估条件'
    : '否，请先补充关键资料或处理字段冲突';
  const readinessClass =
    readiness.readiness_level === 'ready'
      ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
      : readiness.readiness_level === 'basic_ready'
        ? 'border-blue-200 bg-blue-50 text-blue-700'
        : 'border-amber-200 bg-amber-50 text-amber-700';
  const overallStatusClass =
    comprehensiveAdvice.overall_status === 'high_quality'
      ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
      : comprehensiveAdvice.overall_status === 'recommendable'
        ? 'border-blue-200 bg-blue-50 text-blue-700'
        : comprehensiveAdvice.overall_status === 'cautious'
          ? 'border-amber-200 bg-amber-50 text-amber-700'
          : 'border-rose-200 bg-rose-50 text-rose-700';

  return (
    <section className="border-b border-slate-200 bg-white px-6 py-5">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold text-slate-900">融资诊断报告</h3>
          <p className="mt-1 text-sm text-slate-500">当前报告为实时生成，先基于 KYC 资料诊断形成初步结论。</p>
        </div>
        <div className="flex flex-wrap gap-2">
          {canSaveSnapshot ? (
            <button
              type="button"
              onClick={() => void handleSaveSnapshot()}
              disabled={!customerId || snapshotSaving}
              className="rounded-full border border-blue-200 bg-blue-50 px-3 py-1.5 text-sm font-semibold text-blue-700 transition hover:bg-blue-100 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {snapshotSaving ? '正在保存...' : '保存当前报告'}
            </button>
          ) : null}
          <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-sm font-semibold text-slate-700">
            {reportStatusLabel}
          </span>
          <span className={`rounded-full border px-3 py-1.5 text-sm font-semibold ${readinessClass}`}>
            {readinessLabel}
          </span>
        </div>
      </div>

      <div className="grid gap-3 xl:grid-cols-4">
        <div className="rounded-xl border border-slate-200 bg-white p-3">
          <div className="text-xs text-slate-500">客户名称</div>
          <div className="mt-1 text-sm font-semibold text-slate-800">{summary.customer_name || '未记录'}</div>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-3">
          <div className="text-xs text-slate-500">客户状态 / 意向等级</div>
          <div className="mt-1 text-sm font-semibold text-slate-800">
            {summary.status || '未记录'} / {summary.intent_level || '未记录'}
          </div>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-3">
          <div className="text-xs text-slate-500">初步融资评估条件</div>
          <div className="mt-1 text-sm font-semibold text-slate-800">{usableText}</div>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-3">
          <div className="text-xs text-slate-500">资料完整度分数</div>
          <div className="mt-1 text-sm font-semibold text-slate-800">{readiness.score}</div>
        </div>
      </div>

      <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm leading-6 text-slate-700">
        {readiness.summary || '暂无足够资料，当前为初步报告'}
      </div>

      <div className="mt-4 rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div>
            <div className="text-sm font-semibold text-slate-900">综合融资建议</div>
            <div className="mt-1 text-xs text-slate-500">
              {comprehensiveAdvice.summary || '暂不建议进件，请先补齐资料'}
            </div>
          </div>
          <span className={`rounded-full border px-3 py-1 text-xs font-semibold ${overallStatusClass}`}>
            {overallStatusLabel}
          </span>
        </div>

        <div className="grid gap-3 xl:grid-cols-3">
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">融资准备度分数</div>
            <div className="mt-1 text-lg font-semibold text-slate-900">
              {comprehensiveAdvice.financing_readiness_score ?? 0}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3 xl:col-span-2">
            <div className="text-xs text-slate-500">客户经理跟进话术</div>
            <div className="mt-1 text-sm leading-6 text-slate-800">
              {comprehensiveAdvice.sales_follow_up_script || '暂不建议进件，请先补齐资料'}
            </div>
          </div>
        </div>

        <div className="mt-3">
          <div className="text-sm font-semibold text-slate-900">推荐产品方向</div>
          {comprehensiveAdvice.recommended_product_directions?.length ? (
            <div className="mt-2 grid gap-3 xl:grid-cols-2">
              {comprehensiveAdvice.recommended_product_directions.map((item) => {
                const productLabel =
                  item.product_name || PRODUCT_TYPE_LABELS[item.product_type] || item.product_type || '未识别产品方向';
                const fitLabel = FIT_LEVEL_LABELS[item.fit_level] || item.fit_level || '未识别';
                return (
                  <div key={`${item.product_type}-${item.fit_level}`} className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="text-sm font-semibold text-slate-800">{productLabel}</div>
                      <span className="rounded-full border border-slate-200 bg-white px-2 py-0.5 text-xs font-semibold text-slate-700">
                        匹配度：{fitLabel}
                      </span>
                    </div>
                    <div className="mt-2 text-sm leading-6 text-slate-600">{item.reason || '暂无匹配原因'}</div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="mt-2 rounded-xl border border-dashed border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-500">
              暂未形成明确产品方向
            </div>
          )}
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-2">
          <ListBlock title="主要优势" items={comprehensiveAdvice.key_strengths || []} emptyText="暂无明确优势信号" />
          <ListBlock title="主要短板" items={comprehensiveAdvice.main_shortcomings || []} emptyText="暂无明确短板" />
          <ListBlock title="风险摘要" items={comprehensiveAdvice.risk_summary || []} emptyText="暂无明确风险摘要" />
          <ListBlock title="优先行动建议" items={comprehensiveAdvice.priority_actions || []} emptyText="暂无明确优先行动建议" />
        </div>
      </div>

      <div className="mt-4 rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div>
            <div className="text-sm font-semibold text-slate-900">企业征信诊断</div>
            <div className="mt-1 text-xs text-slate-500">
              {enterpriseCredit?.has_enterprise_credit_report ? '已读取企业征信结构化结果' : '尚未上传企业征信报告'}
            </div>
          </div>
          <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700">
            {CREDIT_STATUS_LABELS[enterpriseCredit?.credit_status || 'unknown'] || enterpriseCredit?.credit_status || '未知'}
          </span>
        </div>

        <div className="grid gap-3 xl:grid-cols-4">
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">当前未结清借贷余额</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatMoney(debtSummary.total_unsettled_balance)}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">短期 / 中长期借款余额</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {formatMoney(debtSummary.short_term_loan_balance)} / {formatMoney(debtSummary.long_term_loan_balance)}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">授信使用率</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatRate(debtSummary.credit_usage_rate)}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">未结清贷款笔数 / 对外担保</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {loanSummary.active_loan_count || 0} / {guaranteeSummary.has_external_guarantee ? '有' : '无'}
            </div>
          </div>
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-2">
          <ListBlock
            title="即将到期贷款"
            items={(loanSummary.upcoming_due_loans || []).map((item) => formatLoanItem(item))}
            emptyText="暂无 90 天内到期贷款"
          />
          <ListBlock
            title="逾期贷款"
            items={(loanSummary.overdue_loans || []).map((item) => formatLoanItem(item))}
            emptyText="暂无逾期贷款"
          />
          <ListBlock
            title="非正常五级分类"
            items={(loanSummary.abnormal_classification_loans || []).map((item) => formatLoanItem(item))}
            emptyText="暂无非正常五级分类"
          />
          <ListBlock
            title="企业征信建议"
            items={enterpriseCredit?.recommended_actions || []}
            emptyText={enterpriseCredit?.has_enterprise_credit_report ? '暂无企业征信专项建议' : '请补充企业征信报告'}
          />
        </div>

        {enterpriseCredit?.summary ? (
          <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm leading-6 text-slate-700">
            {enterpriseCredit.summary}
          </div>
        ) : null}
      </div>

      <div className="mt-4 rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div>
            <div className="text-sm font-semibold text-slate-900">个人征信诊断</div>
            <div className="mt-1 text-xs text-slate-500">
              {personalCredit?.has_personal_credit_report ? '已读取个人征信结构化结果' : '尚未上传个人征信报告'}
            </div>
          </div>
          <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700">
            {CREDIT_STATUS_LABELS[personalCredit?.credit_status || 'unknown'] || personalCredit?.credit_status || '未知'}
          </span>
        </div>

        <div className="grid gap-3 xl:grid-cols-4">
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">当前贷款余额</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatMoney(personalDebt.loan_balance)}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">信用卡已用额度</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatMoney(personalDebt.credit_card_used_amount)}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">对外担保余额</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatMoney(personalDebt.external_guarantee_balance)}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">查询风险等级</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {QUERY_RISK_LABELS[querySummary.query_risk_level || 'unknown'] || querySummary.query_risk_level || '未知'}
            </div>
          </div>
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-4">
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">最近3个月查询次数</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatMoney(querySummary.last_3_months_query_count)}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">最近6个月查询次数</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatMoney(querySummary.last_6_months_query_count)}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">是否存在逾期</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {overdueSummary.has_loan_overdue || overdueSummary.has_credit_card_overdue ? '是' : '否'}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">是否存在严重负面</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {seriousNegativeSummary.has_serious_negative ? '是' : '否'}
            </div>
          </div>
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-2">
          <ListBlock
            title="逾期记录"
            items={(overdueSummary.overdue_records || []).map((item) => formatCreditRecord(item))}
            emptyText="暂无逾期记录"
          />
          <ListBlock
            title="严重负面记录"
            items={(seriousNegativeSummary.items || []).map((item) => formatCreditRecord(item))}
            emptyText="暂无严重负面记录"
          />
          <ListBlock
            title="个人征信主要风险"
            items={personalCredit?.key_risks || []}
            emptyText={personalCredit?.has_personal_credit_report ? '暂无明确个人征信风险' : '尚未上传个人征信报告'}
          />
          <ListBlock
            title="个人征信建议"
            items={personalCredit?.recommended_actions || []}
            emptyText={personalCredit?.has_personal_credit_report ? '暂无个人征信专项建议' : '请补充个人征信报告'}
          />
        </div>

        {personalCredit?.summary ? (
          <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm leading-6 text-slate-700">
            {personalCredit.summary}
          </div>
        ) : null}
      </div>

      <div className="mt-4 rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div>
            <div className="text-sm font-semibold text-slate-900">企业流水诊断</div>
            <div className="mt-1 text-xs text-slate-500">
              {enterpriseFlow?.has_enterprise_bank_flow ? '已读取企业流水结构化结果' : '尚未上传企业流水'}
            </div>
          </div>
          <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700">
            {CREDIT_STATUS_LABELS[enterpriseFlow?.flow_status || 'unknown'] || enterpriseFlow?.flow_status || '未知'}
          </span>
        </div>

        <div className="grid gap-3 xl:grid-cols-4">
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">流水期间</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {flowSummary.period_start || '未识别'} 至 {flowSummary.period_end || '未识别'}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">总收入 / 总支出</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {formatMoney(flowSummary.total_income)} / {formatMoney(flowSummary.total_expense)}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">净流入</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatMoney(flowSummary.net_income)}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">月均收入 / 月均净流入</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {formatMoney(flowSummary.average_monthly_income)} / {formatMoney(flowSummary.average_monthly_net_income)}
            </div>
          </div>
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-4">
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">可采信经营收入</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatMoney(flowQuality.real_income_amount)}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">内部转账金额 / 占比</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {formatMoney(flowQuality.internal_transfer_amount)} / {formatRate(flowQuality.internal_transfer_ratio)}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">真实收入占比</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatRate(flowQuality.real_income_ratio)}</div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">户名一致性</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatConsistency(flowConsistency.is_consistent)}</div>
          </div>
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-2">
          <ListBlock
            title="企业流水主要风险"
            items={enterpriseFlow?.key_risks || []}
            emptyText={enterpriseFlow?.has_enterprise_bank_flow ? '暂无明确企业流水风险' : '尚未上传企业流水'}
          />
          <ListBlock
            title="企业流水建议"
            items={enterpriseFlow?.recommended_actions || []}
            emptyText={enterpriseFlow?.has_enterprise_bank_flow ? '暂无企业流水专项建议' : '请补充企业流水'}
          />
        </div>

        {flowConsistency.warnings?.length ? (
          <div className="mt-3 rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            {flowConsistency.warnings.join('；')}
          </div>
        ) : null}

        {enterpriseFlow?.summary ? (
          <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm leading-6 text-slate-700">
            {enterpriseFlow.summary}
          </div>
        ) : null}
      </div>

      <div className="mt-4 rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div>
            <div className="text-sm font-semibold text-slate-900">财务数据诊断</div>
            <div className="mt-1 text-xs text-slate-500">
              {financialStatement?.has_financial_statement ? '已读取财务数据结构化结果' : '尚未上传财务报表'}
            </div>
          </div>
          <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700">
            {CREDIT_STATUS_LABELS[financialStatement?.financial_status || 'unknown'] || financialStatement?.financial_status || '未知'}
          </span>
        </div>

        <div className="grid gap-3 xl:grid-cols-4">
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">最近期间 / 报表类型</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {financialPeriod.latest_period || '未识别'} / {financialPeriod.statement_type || '未识别'}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">营业收入 / 营业成本</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {formatMoney(profitability.revenue)} / {formatMoney(profitability.operating_cost)}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">净利润 / 净利率</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {formatMoney(profitability.net_profit)} / {formatRate(profitability.net_profit_margin)}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">经营现金流净额</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{formatMoney(cashFlow.operating_cash_flow_net)}</div>
          </div>
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-4">
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">资产总额 / 负债总额</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {formatMoney(debtCapacity.total_assets)} / {formatMoney(debtCapacity.total_liabilities)}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">所有者权益 / 资产负债率</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {formatMoney(debtCapacity.owner_equity)} / {formatRate(debtCapacity.asset_liability_ratio)}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">流动资产 / 流动负债 / 流动比率</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {formatMoney(liquidity.current_assets)} / {formatMoney(liquidity.current_liabilities)} / {formatMoney(liquidity.current_ratio)}
            </div>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">短期借款 / 长期借款</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">
              {formatMoney(debtCapacity.short_term_borrowing)} / {formatMoney(debtCapacity.long_term_borrowing)}
            </div>
          </div>
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-2">
          <ListBlock
            title="财务数据主要风险"
            items={financialStatement?.key_risks || []}
            emptyText={financialStatement?.has_financial_statement ? '暂无明确财务数据风险' : '尚未上传财务报表'}
          />
          <ListBlock
            title="财务数据建议"
            items={financialStatement?.recommended_actions || []}
            emptyText={financialStatement?.has_financial_statement ? '暂无财务数据专项建议' : '请补充财务报表'}
          />
        </div>

        {financialStatement?.summary ? (
          <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm leading-6 text-slate-700">
            {financialStatement.summary}
          </div>
        ) : null}
      </div>

      <div className="mt-4 grid gap-3 xl:grid-cols-2">
        <ListBlock title="主要风险提醒" items={report.risk_highlights || []} emptyText="暂无明确风险提醒" />
        <ListBlock title="必缺资料" items={checklist.required_missing || []} emptyText="暂无必缺资料" />
        <ListBlock title="可选缺失资料" items={checklist.optional_missing || []} emptyText="暂无可选缺失资料" />
        <ListBlock title="建议下一步" items={report.next_actions || []} emptyText="暂无明确下一步建议" />
      </div>

      {checklist.recommended_supplements?.length ? (
        <div className="mt-4 rounded-xl border border-blue-100 bg-blue-50 px-4 py-3">
          <div className="text-sm font-semibold text-blue-900">推荐补充</div>
          <ul className="mt-2 space-y-1 text-sm text-blue-800">
            {checklist.recommended_supplements.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className="mt-4 rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div>
            <div className="text-sm font-semibold text-slate-900">历史快照报告</div>
            <div className="mt-1 text-xs text-slate-500">已保存的历史版本，后续导出以快照报告为准。</div>
          </div>
          <button
            type="button"
            onClick={() => void loadSnapshots()}
            disabled={!customerId || snapshotLoading}
            className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {snapshotLoading ? '刷新中...' : '刷新历史'}
          </button>
        </div>

        {snapshotError ? (
          <div className="mb-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
            {snapshotError}
          </div>
        ) : null}

        {snapshots.length > 0 ? (
          <div className="grid gap-3 xl:grid-cols-2">
            {snapshots.map((item) => {
              const source = item.source_summary || {};
              const statusLabel = OVERALL_STATUS_LABELS[String(source.overall_status || '')] || String(source.overall_status || '未识别');
              return (
                <button
                  type="button"
                  key={item.id}
                  onClick={() => void handleOpenSnapshot(item.id)}
                  className="rounded-xl border border-slate-200 bg-slate-50 p-3 text-left transition hover:border-blue-200 hover:bg-blue-50"
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="text-sm font-semibold text-slate-900">{item.report_version || '未记录版本'}</div>
                    <span className="rounded-full border border-slate-200 bg-white px-2 py-0.5 text-xs font-semibold text-slate-700">
                      {REPORT_STATUS_LABELS[item.report_status] || item.report_status || '草稿报告'}
                    </span>
                  </div>
                  <div className="mt-2 grid gap-1 text-xs text-slate-600">
                    <div>生成时间：{formatDateTime(item.generated_at)}</div>
                    <div>生成人：{item.generated_by || '未记录'}</div>
                    <div>综合状态：{statusLabel}</div>
                    <div>准备度分数：{source.financing_readiness_score ?? 0}</div>
                  </div>
                  {item.summary ? <div className="mt-2 line-clamp-2 text-sm text-slate-700">{item.summary}</div> : null}
                </button>
              );
            })}
          </div>
        ) : (
          <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-5 text-center text-sm text-slate-500">
            暂无历史快照报告
          </div>
        )}

        {snapshotDetail ? (
          <details className="mt-4 rounded-xl border border-slate-200 bg-white px-4 py-3" open>
            <summary className="cursor-pointer text-sm font-semibold text-slate-800">
              历史快照报告预览：{snapshotDetail.report_version || '未记录版本'} · {formatDateTime(snapshotDetail.generated_at)}
            </summary>
            <div className="mt-3 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => void handleExportSnapshot('docx')}
                disabled={snapshotExporting !== null}
                className="rounded-full border border-blue-200 bg-blue-50 px-3 py-1.5 text-xs font-semibold text-blue-700 transition hover:bg-blue-100 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {snapshotExporting === 'docx' ? 'Word 导出中...' : '导出 Word'}
              </button>
              <button
                type="button"
                onClick={() => void handleExportSnapshot('pdf')}
                disabled={snapshotExporting !== null}
                className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {snapshotExporting === 'pdf' ? 'PDF 导出中...' : '导出 PDF'}
              </button>
            </div>
            <pre className="mt-3 max-h-96 overflow-auto whitespace-pre-wrap rounded-xl bg-slate-950 p-4 text-xs leading-6 text-slate-100">
              {snapshotDetail.report_markdown || '暂无历史报告正文'}
            </pre>
          </details>
        ) : null}
      </div>

      <details className="mt-4 rounded-xl border border-slate-200 bg-white px-4 py-3">
        <summary className="cursor-pointer text-sm font-semibold text-slate-800">当前实时报告 Markdown 预览</summary>
        <pre className="mt-3 max-h-96 overflow-auto whitespace-pre-wrap rounded-xl bg-slate-950 p-4 text-xs leading-6 text-slate-100">
          {report.report_markdown || '暂无报告正文'}
        </pre>
      </details>
    </section>
  );
};

export default CustomerFinancingDiagnosticReportPanel;
