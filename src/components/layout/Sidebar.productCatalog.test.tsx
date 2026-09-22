import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import Sidebar from './Sidebar';

describe('product catalog sidebar entry', () => {
  it('test_admin_sees_product_catalog_nav', () => {
    render(<Sidebar currentPage="workspace" onNavigate={vi.fn()} userRole="admin" />);
    expect(screen.getByTestId('nav-item-产品库管理')).toBeTruthy();
    const labels = screen.getAllByRole('button').map((button) => button.textContent || '');
    expect(labels.findIndex((label) => label.includes('方案匹配'))).toBeLessThan(labels.findIndex((label) => label.includes('产品库管理')));
    expect(labels.findIndex((label) => label.includes('产品库管理'))).toBeLessThan(labels.findIndex((label) => label.includes('AI 对话')));
  });

  it('test_operator_does_not_see_product_catalog_nav', () => {
    render(<Sidebar currentPage="workspace" onNavigate={vi.fn()} userRole="operator" />);
    expect(screen.queryByTestId('nav-item-产品库管理')).toBeNull();
  });

  it('test_viewer_does_not_see_product_catalog_nav', () => {
    render(<Sidebar currentPage="workspace" onNavigate={vi.fn()} userRole="viewer" />);
    expect(screen.queryByTestId('nav-item-产品库管理')).toBeNull();
  });

  it('passes the catalog page id on click', () => {
    const onNavigate = vi.fn();
    render(<Sidebar currentPage="workspace" onNavigate={onNavigate} userRole="admin" />);
    fireEvent.click(screen.getByTestId('nav-item-产品库管理'));
    expect(onNavigate).toHaveBeenCalledWith('product-catalog');
  });
});
