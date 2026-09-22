import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import LocalProductCatalogSection from './LocalProductCatalogSection';
import { SPACING } from '../../styles/design-tokens';
import {
  getCatalogConflict, getCatalogProductHistory, getCatalogRuleOptions, getCatalogVersion, getLocalCatalogConflicts,
  getLocalCatalogProducts, getLocalCatalogSources, listCatalogProducts, listCatalogVersions, syncLocalCatalog,
  resolveCatalogConflict,
} from '../../services/api';

vi.mock('../../services/api', () => ({
  getCatalogProductHistory: vi.fn(), getCatalogRuleOptions: vi.fn(), getCatalogVersion: vi.fn(),
  getCatalogConflict: vi.fn(), resolveCatalogConflict: vi.fn(),
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
    vi.mocked(getCatalogConflict).mockResolvedValue({ external_product_code: 'NJB-001', status: 'unresolved', conflict: true, needs_review: true,
      conflict_fields: ['最高额度'], differences: [{ field_name: 'product_name', a: '产品A', b: '产品B', kind: 'structured' }],
      source_line_changes: { a_only: ['## 产品A'], b_only: ['## 产品B'] }, sides: [
        { side: 'a', external_product_code: 'NJB-001', product_name: '产品A', institution_name: '南京银行', product_category: 'enterprise_credit', source_file: 'A.md', source_update_date: '2026-09-01', source_snapshot_hash: 'abc', source_snapshot: '## 产品A', raw_fields: { 最高额度: '500万' }, structured_fields: { max_amount: 5000000 } },
        { side: 'b', external_product_code: 'NJB-001', product_name: '产品B', institution_name: '南京银行', product_category: 'enterprise_credit', source_file: 'B.md', source_update_date: '2026-09-02', source_snapshot_hash: 'def', source_snapshot: '## 产品B', raw_fields: { 最高额度: '800万' }, structured_fields: { max_amount: 8000000 } },
      ] });
    vi.mocked(resolveCatalogConflict).mockResolvedValue({ status: 'resolved_keep_a', created_drafts: 1, version_ids: ['v2'] });
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

  it('test_product_detail_has_close_button', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    const close = await screen.findByRole('button', { name: '关闭产品详情' });
    expect(close).toHaveAttribute('title', '关闭');
    expect(close).toHaveTextContent('关闭');
  });

  it('test_product_detail_close_button_not_hidden_by_header', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    const overlay = await screen.findByTestId('product-detail-overlay');
    const detailHeader = screen.getByTestId('product-detail-header');
    const close = screen.getByRole('button', { name: '关闭产品详情' });
    expect(overlay).toHaveStyle({ top: `${SPACING.headerHeight}px` });
    expect(detailHeader).toContainElement(close);
    expect(detailHeader).toHaveClass('sticky', 'top-0', 'z-20');
  });

  it('test_product_detail_starts_below_global_header', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    const overlay = await screen.findByTestId('product-detail-overlay');
    expect(overlay).toHaveStyle({ top: `${SPACING.headerHeight}px` });
    expect(overlay).not.toHaveClass('inset-0');
  });

  it('test_close_button_visible_after_scroll', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    const panel = await screen.findByTestId('product-detail-panel');
    panel.scrollTop = 500;
    fireEvent.scroll(panel);
    expect(screen.getByTestId('product-detail-header')).toHaveClass('sticky', 'top-0');
    expect(screen.getByRole('button', { name: '关闭产品详情' })).toBeVisible();
  });

  it('test_product_detail_height_respects_header', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    const overlay = await screen.findByTestId('product-detail-overlay');
    expect(overlay).toHaveClass('fixed', 'bottom-0');
    expect(overlay).toHaveStyle({ top: `${SPACING.headerHeight}px` });
    expect(screen.getByTestId('product-detail-panel')).toHaveClass('h-full', 'overflow-y-auto');
  });

  it('test_close_button_closes_detail', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    fireEvent.click(await screen.findByRole('button', { name: '关闭产品详情' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: '产品详情' })).toBeNull());
    expect(screen.getByText('产品列表')).toBeTruthy();
  });

  it('test_escape_closes_detail', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    expect(await screen.findByRole('dialog', { name: '产品详情' })).toBeTruthy();
    fireEvent.keyDown(document, { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('dialog', { name: '产品详情' })).toBeNull());
  });

  it('test_unsaved_changes_warn_before_close', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    fireEvent.change(await screen.findByLabelText('银行/机构'), { target: { value: '修改后的机构' } });
    fireEvent.click(screen.getByRole('button', { name: '关闭产品详情' }));
    const warning = await screen.findByRole('alertdialog', { name: '未保存修改' });
    expect(within(warning).getByText('当前有未保存的修改，关闭后将丢失这些内容，是否继续？')).toBeTruthy();
    fireEvent.click(within(warning).getByRole('button', { name: '返回编辑' }));
    expect(screen.getByRole('dialog', { name: '产品详情' })).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: '关闭产品详情' }));
    fireEvent.click(await screen.findByRole('button', { name: '继续关闭' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: '产品详情' })).toBeNull());
  });

  it('test_published_product_closes_without_warning', async () => {
    const published = { ...version, status: 'published', review_status: 'reviewed' };
    vi.mocked(getCatalogVersion).mockResolvedValue({ product, version: published, rules: [] });
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    fireEvent.click(await screen.findByRole('button', { name: '关闭产品详情' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: '产品详情' })).toBeNull());
    expect(screen.queryByRole('alertdialog', { name: '未保存修改' })).toBeNull();
  });

  it('test_close_preserves_current_product_list_state', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    const search = screen.getByLabelText('搜索产品');
    fireEvent.change(search, { target: { value: 'BOCOM' } });
    await waitFor(() => expect(listCatalogProducts).toHaveBeenLastCalledWith(expect.objectContaining({ search: 'BOCOM' })));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    const callsBeforeClose = vi.mocked(listCatalogProducts).mock.calls.length;
    fireEvent.click(await screen.findByRole('button', { name: '关闭产品详情' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: '产品详情' })).toBeNull());
    expect((screen.getByLabelText('搜索产品') as HTMLInputElement).value).toBe('BOCOM');
    expect(vi.mocked(listCatalogProducts).mock.calls.length).toBe(callsBeforeClose);
  });

  it('shows Chinese product field and status labels', async () => {
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    expect((await screen.findAllByText('企业信用')).length).toBeGreaterThan(0);
    expect(screen.getAllByText('草稿').length).toBeGreaterThan(0);
    fireEvent.click(screen.getByText('BOCOM-001'));
    expect(await screen.findByLabelText('产品编号复核状态')).toBeTruthy();
    expect(screen.getAllByRole('option', { name: '已提取，待人工确认' }).length).toBeGreaterThan(0);
    expect(screen.queryByText('external_product_code')).toBeNull();
  });

  it('publish tooltip lists the pending Chinese field name', async () => {
    const reviewedVersion = { ...version, review_status: 'reviewed', effective_from: '2026-09-22',
      guarantee_modes_json: [], collateral_types_json: [], materials_json: [], region_scope_json: [], company_age_months: null,
      field_review_json: { external_product_code: 'reviewed', institution_name: 'reviewed', product_name: 'reviewed', product_category: 'reviewed',
        max_amount: 'reviewed', max_term_months: 'reviewed', guarantee_modes: 'needs_review', collateral_types: 'insufficient_data',
        materials: 'insufficient_data', region_scope: 'insufficient_data', company_age_rule: 'insufficient_data' } };
    vi.mocked(getCatalogVersion).mockResolvedValue({ product, version: reviewedVersion, rules: [] });
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    const publish = await screen.findByRole('button', { name: '发布' });
    expect(publish).toBeDisabled();
    expect(publish).toHaveAttribute('title', '尚未完成复核：担保方式');
    expect(screen.getByText('尚未完成复核：担保方式')).toBeTruthy();
  });

  it('confirms every extracted field without changing unresolved or insufficient fields', async () => {
    const extractedVersion = { ...version, field_review_json: { external_product_code: 'extracted_review', institution_name: 'extracted_review',
      product_name: 'extracted_review', product_category: 'extracted_review', max_amount: 'extracted_review', max_term_months: 'extracted_review',
      guarantee_modes: 'needs_review', collateral_types: 'insufficient_data', materials: 'insufficient_data', region_scope: 'insufficient_data', company_age_rule: 'insufficient_data' } };
    vi.mocked(getCatalogVersion).mockResolvedValue({ product, version: extractedVersion, rules: [] });
    render(<LocalProductCatalogSection />);
    fireEvent.click(screen.getByText('产品列表'));
    fireEvent.click(await screen.findByText('BOCOM-001'));
    fireEvent.click(await screen.findByRole('button', { name: '确认所有已提取字段' }));
    expect((screen.getByLabelText('产品编号复核状态') as HTMLSelectElement).value).toBe('reviewed');
    expect((screen.getByLabelText('担保方式复核状态') as HTMLSelectElement).value).toBe('needs_review');
    expect((screen.getByLabelText('适用地区复核状态') as HTMLSelectElement).value).toBe('insufficient_data');
  });

  it('shows conflict sides and field summary', async () => {
    render(<LocalProductCatalogSection />);
    await screen.findByText('担保基金');
    fireEvent.click(within(screen.getByRole('navigation', { name: '产品库管理导航' })).getByRole('button', { name: '冲突' }));
    expect(await screen.findByText(/NJB-001 · 待处理/)).toBeTruthy();
    expect(screen.getByText(/冲突字段：最高额度/)).toBeTruthy();
    expect(screen.getByText(/SHA-256: abc/)).toBeTruthy();
  });

  it('opens both original sources and requires a human decision', async () => {
    render(<LocalProductCatalogSection />);
    await screen.findByText('担保基金');
    fireEvent.click(within(screen.getByRole('navigation', { name: '产品库管理导航' })).getByRole('button', { name: '冲突' }));
    fireEvent.click(await screen.findByRole('button', { name: /NJB-001.*查看对照与处理/ }));
    const dialog = await screen.findByRole('dialog', { name: '冲突详情' });
    expect(within(dialog).getByText('来源 A')).toBeTruthy();
    expect(within(dialog).getByText('来源 B')).toBeTruthy();
    expect(within(dialog).getByText('原文行差异')).toBeTruthy();
    expect(resolveCatalogConflict).not.toHaveBeenCalled();
    expect((within(dialog).getByText('确认生成待审核草稿') as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(within(dialog).getByLabelText('同一产品，逐字段合并'));
    fireEvent.click(within(dialog).getByText('确认生成待审核草稿'));
    expect(await within(dialog).findByRole('alert')).toHaveTextContent('每个差异字段');
  });
});
