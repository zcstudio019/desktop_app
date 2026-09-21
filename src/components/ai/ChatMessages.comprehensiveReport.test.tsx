import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ChatInput from './ChatInput';
import ChatMessages from './ChatMessages';
import { downloadComprehensiveReportPdf, fetchComprehensiveReportArtifact, sendChat } from '../../services/api';
import { useChatStore } from '../../stores/useChatStore';

vi.mock('../../services/api', () => ({
  sendChat: vi.fn(),
  downloadComprehensiveReportPdf: vi.fn(),
  fetchComprehensiveReportArtifact: vi.fn(),
  getCurrentFinancingRequirement: vi.fn().mockResolvedValue(null),
}));

const data = {
  reportStatus: 'completed',
  reportType: 'comprehensive_financing_analysis_report',
  templateVersion: 'comprehensive_financing_analysis_report_v1',
  reportId: 'a'.repeat(32),
  previewUrl: `/api/customers/c1/comprehensive-financing-report/snapshots/${'a'.repeat(32)}/preview`,
  pdfUrl: `/api/customers/c1/comprehensive-financing-report/snapshots/${'a'.repeat(32)}/export/pdf`,
};

function StoreControl() {
  const { messages, addMessage, clearConversation } = useChatStore();
  return <>
    <button onClick={() => addMessage({ role: 'assistant', content: '征信报告', intent: 'credit_one_page_report', data: { ...data, reportType: 'credit_one_page_report' } })}>征信消息</button>
    <button onClick={clearConversation}>清空</button>
    <output data-testid="latest-assistant-message">{JSON.stringify(messages.at(-1) ?? null)}</output>
  </>;
}

describe('workspace assistant comprehensive report', () => {
  beforeEach(() => {
    Element.prototype.scrollIntoView = vi.fn();
    const { unmount } = render(<StoreControl />);
    fireEvent.click(screen.getByText('清空'));
    unmount();
  });
  afterEach(() => vi.clearAllMocks());

  it('keeps frozen snapshot URLs in assistant data and renders distinct actions', async () => {
    vi.mocked(sendChat).mockResolvedValue({
      message: '# 客户综合融资分析报告', intent: 'comprehensive_financing_analysis_report', data, reasoning: null,
    });
    render(<><ChatInput /><ChatMessages /><StoreControl /></>);
    fireEvent.change(screen.getByTestId('ai-message-input'), { target: { value: '生成综合融资分析报告' } });
    fireEvent.click(screen.getByTestId('ai-send-button'));
    expect(await screen.findByText('预览综合报告')).toBeTruthy();
    expect(screen.getByText('下载综合报告')).toBeTruthy();
    expect(JSON.parse(screen.getByTestId('latest-assistant-message').textContent || '{}')).toMatchObject({
      role: 'assistant', intent: 'comprehensive_financing_analysis_report', data,
    });
    await act(async () => { fireEvent.click(screen.getByText('下载综合报告')); });
    expect(downloadComprehensiveReportPdf).toHaveBeenCalledWith(data.pdfUrl);
    expect(fetchComprehensiveReportArtifact).not.toHaveBeenCalled();
    expect(sendChat).toHaveBeenCalledTimes(1);
  });

  it('does not render comprehensive actions for a credit report', () => {
    render(<><ChatMessages /><StoreControl /></>);
    fireEvent.click(screen.getByText('征信消息'));
    expect(screen.queryByText('预览综合报告')).toBeNull();
  });

  it('test_comprehensive_report_button_text_is_chinese', async () => {
    vi.mocked(sendChat).mockResolvedValue({
      message: '# 客户综合融资分析报告', intent: 'comprehensive_financing_analysis_report', data, reasoning: null,
    });
    render(<><ChatInput /><ChatMessages /></>);
    fireEvent.change(screen.getByTestId('ai-message-input'), { target: { value: '生成综合融资分析报告' } });
    fireEvent.click(screen.getByTestId('ai-send-button'));
    expect(await screen.findByText('下载综合报告')).toBeTruthy();
    expect(screen.queryByText('下载综合报告 PDF')).toBeNull();
  });
});
