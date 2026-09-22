import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import LocalProductCatalogSection from './LocalProductCatalogSection';
import {
  getCatalogProductHistory, getCatalogRuleOptions, getCatalogVersion, getLocalCatalogConflicts,
  getLocalCatalogProducts, getLocalCatalogSources, listCatalogProducts, listCatalogVersions, syncLocalCatalog,
} from '../../services/api';

vi.mock('../../services/api', () => ({
  getCatalogProductHistory: vi.fn(), getCatalogRuleOptions: vi.fn(), getCatalogVersion: vi.fn(),
  getLocalCatalogConflicts: vi.fn(), getLocalCatalogProducts: vi.fn(), getLocalCatalogSources: vi.fn(),
  listCatalogProducts: vi.fn(), listCatalogVersions: vi.fn(), syncLocalCatalog: vi.fn(),
  patchCatalogDraft: vi.fn(), saveCatalogRule: vi.fn(), deleteCatalogRule: vi.fn(), changeCatalogVersion: vi.fn(),
}));

const categories = [
  ['guarantee_fund', '担保基金'], ['personal_mortgage', '个人抵押'], ['personal_credit', '个人信用'],
  ['technology_enterprise', '科技企业'], ['enterprise_mortgage', '企业抵押'], ['enterprise_credit', '企业信用'],
];
const product = { product_id: 'p1', external_product_code: 'BOCOM-001', product_category: 'enterprise_credit',
  source_ref: '企业信用类产品库.md', institution_name: '交通银行', product_name: '普惠e贷1.0' };
const version = { version_id: 'v1', product_id: 'p1', version_number: 1, status: 'draft', institution_name: '交通银行',
  product_name: '普惠e贷1.0', max_amount: '10000000.00', max_term_months: 36, needs_review: 1, review_status: 'unreviewed',
  review_reasons_json: ['征信要求仅保留自然语言'], field_review_json: {}, raw_fields_json: { 征信要求: '征信良好' },
  source_file: '企业信用类产品库.md', source_update_date: '2026-09-20', source_imported_at: '2026-09-22',
  source_snapshot: '## 1. 【BOCOM-001】 普惠e贷1.0\n| 征信要求 | 征信良好 |', source_snapshot_hash: 'hash1',
  effective_from: null, effective_to: null, published_at: null, published_by: '', min_amount: null, min_term_months: null };

describe('LocalProductCatalogSection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getLocalCatalogSources).mockResolvedValue({ database_status: 'available', totals: { parsed_count: 162, unique_count: 155, duplicate_code_count: 7, conflict_count: 7, needs_review_count: 162 },
      sources: categories.map(([category, label]) => ({ category, label, source_file: `${label}.md`, missing: false,
        file_updated_at: '2026-09-22', source_update_date: '2026-09-20', source_snapshot_hash: 'hash', declared_count: 1,
        parsed_count: 1, unique_count: 1, duplicate_count: 0, conflict_count: 0, needs_review_count: 1,
        draft_count: 1, published_count: 0, last_synced_at: '2026-09-22' })) });
    vi.mocked(getLocalCatalogConflicts).mockResolvedValue({ total: 1, items: [{ external_product_code: 'NJB-001', status: 'duplicate_conflict', needs_review: true,
      conflict_fields: ['最高额度'], sides: [{ source_file: 'A.md', product_name: '产品A', snapshot_hash: 'abc' }, { source_file: 'B.md', product_name: '产品B', snapshot_hash: 'def' }] }] });
    vi.mocked(getLocalCatalogProducts).mockResolvedValue({ items: [], total: 0, missing: false });
    vi.mocked(listCatalogProducts).mockResolvedValue({ items: [{ product, version }], total: 1 });
    vi.mocked(listCatalogVersions).mockResolvedValue({ items: [{ product, version }], total: 1 });
    vi.mocked(getCatalogVersion).mockResolvedValue({ product, version, rules: [] });
    vi.mocked(getCatalogProductHistory).mockResolvedValue({ items: [{ product, version }], total: 1 });
    vi.mocked(getCatalogRuleOptions).mockResolvedValue({ fields: { 'requirement.amount': 'number' }, operators: ['eq'], severities: ['hard'], failure_actions: ['exclude'] });
    vi.mocked(syncLocalCatalog).mockResolvedValue({ created_drafts: 1, unchanged: 0, conflicts: [] });
  });

  it('loads six local sources as the default admin tab', async () => {
    render(<LocalProductCatalogSection />);
    expect(await screen.findByText('担保基金')).toBeTruthy();
    for (const [, label] of categories) expect(screen.getByText(label)).toBeTruthy();
    expect(screen.getAllByText('同步')).toHaveLength(6);
  });

  it('syncs drafts from a source without publishing', async () => {
    render(<LocalProductCatalogSection />);
    await screen.findByText('担保基金');
    fireEvent.click(screen.getAllByText('同步')[0]);
    await waitFor(() => expect(syncLocalCatalog).toHaveBeenCalledWith('guarantee_fund'));
    expect(await screen.findByText(/新增 1 个草稿/)).toBeTruthy();
  });

  it('filters product list by code', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.change(screen.getByLabelText('搜索产品'), { target: { value: 'BOCOM-001' } });
    await waitFor(() => expect(listCatalogProducts).toHaveBeenCalledWith(expect.objectContaining({ search: 'BOCOM-001' })));
    expect(await screen.findByText('BOCOM-001')).toBeTruthy();
  });

  it('opens draft detail with source text and rules', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    expect(await screen.findByRole('heading', { name: '来源原文' })).toBeTruthy();
    expect(screen.getByText(/征信要求 \| 征信良好/)).toBeTruthy();
    expect(screen.getByText('暂无结构化规则')).toBeTruthy();
    expect((screen.getByText('发布') as HTMLButtonElement).disabled).toBe(true);
  });

  it('shows conflict sides and field summary', async () => {
    render(<LocalProductCatalogSection />);
    await screen.findByText('担保基金');
    fireEvent.click(within(screen.getByRole('navigation', { name: '产品库管理导航' })).getByRole('button', { name: '冲突' }));
    expect(await screen.findByText(/NJB-001 · 待处理/)).toBeTruthy();
    expect(screen.getByText(/冲突字段：最高额度/)).toBeTruthy();
    expect(screen.getByText(/SHA-256: abc/)).toBeTruthy();
  });
});
