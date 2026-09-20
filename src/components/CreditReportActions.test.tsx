import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { CreditReportActions } from './CreditReportActions';
import { downloadCreditReportPdf, fetchCreditReportArtifact } from '../services/api';

vi.mock('../services/api', () => ({ downloadCreditReportPdf: vi.fn(), fetchCreditReportArtifact: vi.fn() }));
const data = { reportStatus: 'completed', previewUrl: '/preview', pdfUrl: '/pdf' };

describe('CreditReportActions', () => {
  it('only appears for completed report snapshots', () => {
    const { rerender } = render(<CreditReportActions data={{}} />);
    expect(screen.queryByText('下载 PDF')).toBeNull();
    rerender(<CreditReportActions data={data} />);
    expect(screen.getByText('预览报告')).toBeTruthy();
    expect(screen.getByText('下载 PDF')).toBeTruthy();
    rerender(<CreditReportActions data={{ ...data, reportStatus: 'processing' }} />);
    expect(screen.queryByText('预览报告')).toBeNull();
    rerender(<CreditReportActions data={{ ...data, pdfUrl: '' }} />);
    expect(screen.queryByText('预览报告')).toBeNull();
    rerender(<CreditReportActions data={{ ...data, pdfUrl: '', exportMessage: '导出暂不可用' }} />);
    expect(screen.getByText('导出暂不可用')).toBeTruthy();
  });
  it('downloads the frozen PDF link', async () => {
    vi.mocked(downloadCreditReportPdf).mockResolvedValue(undefined);
    render(<CreditReportActions data={data} />);
    fireEvent.click(screen.getByText('下载 PDF'));
    await waitFor(() => expect(downloadCreditReportPdf).toHaveBeenCalledWith('/pdf'));
  });
  it('opens authenticated HTML as a local blob without exposing a token in URLs', async () => {
    const tab = { opener: {}, location: { replace: vi.fn() }, close: vi.fn() };
    vi.spyOn(window, 'open').mockReturnValue(tab as unknown as Window);
    vi.stubGlobal('URL', { createObjectURL: vi.fn(() => 'blob:report'), revokeObjectURL: vi.fn() });
    vi.mocked(fetchCreditReportArtifact).mockResolvedValue({ blob: new Blob(['report']), fileName: '' });
    render(<CreditReportActions data={data} />);
    fireEvent.click(screen.getByText('预览报告'));
    await waitFor(() => expect(tab.location.replace).toHaveBeenCalledWith('blob:report'));
    expect(tab.opener).toBeNull();
    vi.unstubAllGlobals();
  });
  it('shows safe business errors', async () => {
    vi.mocked(downloadCreditReportPdf).mockRejectedValue(new Error('无权查看该客户记录'));
    render(<CreditReportActions data={data} />);
    fireEvent.click(screen.getByText('下载 PDF'));
    expect(await screen.findByRole('alert')).toHaveTextContent('无权查看该客户记录');
  });
});
