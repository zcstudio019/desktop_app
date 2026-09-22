import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import App from './App';
import { getCurrentUser, getLocalCatalogConflicts, getLocalCatalogSources } from './services/api';

vi.mock('./services/api', () => ({
  getCurrentUser: vi.fn(), getLocalCatalogConflicts: vi.fn(), getLocalCatalogSources: vi.fn(),
}));
vi.mock('./context/AppContext', () => ({ AppProvider: ({ children }: { children: ReactNode }) => <>{children}</> }));
vi.mock('./components/layout/Header', () => ({ default: () => <div data-testid="test-header" /> }));
vi.mock('./pages/Workspace', () => ({ default: () => <div data-testid="workspace-page">工作台页面</div> }));
vi.mock('./components/UploadPage', () => ({ default: () => <div /> }));
vi.mock('./components/ApplicationPage', () => ({ default: () => <div /> }));
vi.mock('./components/SchemeMatch', () => ({ default: () => <div /> }));
vi.mock('./components/ChatPage', () => ({ default: () => <div /> }));
vi.mock('./components/LoginPage', () => ({ default: () => <div /> }));
vi.mock('./components/CustomerListPage', () => ({ default: () => <div /> }));
vi.mock('./components/CustomerDataPage', () => ({ default: () => <div /> }));
vi.mock('./components/AdminUsersPage', () => ({ default: () => <div /> }));

const labels = [
  ['guarantee_fund', '担保基金'], ['personal_mortgage', '个人抵押'], ['personal_credit', '个人信用'],
  ['technology_enterprise', '科技企业'], ['enterprise_mortgage', '企业抵押'], ['enterprise_credit', '企业信用'],
];

function mockUser(role: string) {
  vi.mocked(getCurrentUser).mockResolvedValue({ username: 'tester', role });
}

describe('product catalog app route', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    localStorage.setItem('auth_token', 'test-token');
    window.history.replaceState({}, '', '/workspace');
    vi.mocked(getLocalCatalogSources).mockResolvedValue({ database_status: 'unavailable',
      totals: { parsed_count: 162, unique_count: 155, duplicate_code_count: 7, conflict_count: 7, needs_review_count: 162 },
      sources: labels.map(([category, label]) => ({ category, label, source_file: `${label}.md`, missing: false,
        file_updated_at: '2026-09-22', source_update_date: '2026-09-20', source_snapshot_hash: 'hash',
        declared_count: 1, parsed_count: 1, unique_count: 1, duplicate_count: 0, conflict_count: 0,
        needs_review_count: 1, draft_count: null, published_count: null, last_synced_at: null })) });
    vi.mocked(getLocalCatalogConflicts).mockResolvedValue({ items: [], total: 0 });
  });

  it('test_product_catalog_nav_opens_page', async () => {
    mockUser('admin');
    render(<App />);
    fireEvent.click(await screen.findByTestId('nav-item-产品库管理'));
    expect(window.location.pathname).toBe('/product-catalog');
    expect(await screen.findByTestId('product-catalog-page')).toBeTruthy();
  });

  it('test_product_catalog_route_survives_refresh', async () => {
    mockUser('admin');
    window.history.replaceState({}, '', '/product-catalog');
    const first = render(<App />);
    expect(await screen.findByTestId('product-catalog-page')).toBeTruthy();
    first.unmount();
    render(<App />);
    expect(await screen.findByTestId('product-catalog-page')).toBeTruthy();
    expect(window.location.pathname).toBe('/product-catalog');
  });

  it('test_product_catalog_page_renders_six_sources', async () => {
    mockUser('admin');
    window.history.replaceState({}, '', '/product-catalog');
    render(<App />);
    expect(await screen.findByText('担保基金')).toBeTruthy();
    for (const [, label] of labels) expect(screen.getByText(label)).toBeTruthy();
    expect(screen.getByText('维护本地 Markdown 产品源、产品版本、准入规则与发布状态')).toBeTruthy();
  });

  it.each(['operator', 'viewer'])('rejects direct catalog URL for %s', async (role) => {
    mockUser(role);
    window.history.replaceState({}, '', '/product-catalog');
    render(<App />);
    await waitFor(() => expect(window.location.pathname).toBe('/workspace'));
    expect(screen.getByTestId('workspace-page')).toBeTruthy();
    expect(screen.queryByTestId('nav-item-产品库管理')).toBeNull();
  });
});
