import { fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ChatInput from './ChatInput';
import ChatMessages from './ChatMessages';
import { sendChat } from '../../services/api';
import { useChatStore } from '../../stores/useChatStore';

vi.mock('../../services/api', () => ({
  sendChat: vi.fn(),
  downloadCreditReportPdf: vi.fn(),
  fetchCreditReportArtifact: vi.fn(),
}));

const reportData = {
  reportStatus: 'completed',
  reportId: 'snapshot-123',
  previewUrl: '/api/customers/1/credit-report/snapshots/snapshot-123/preview',
  pdfUrl: '/api/customers/1/credit-report/snapshots/snapshot-123/export/pdf',
};

function StoreControl() {
  const { messages, addMessage, clearConversation } = useChatStore();
  return <>
    <button onClick={() => addMessage({ role: 'assistant', content: '普通回复' })}>普通消息</button>
    <button onClick={() => addMessage({ role: 'assistant', content: '报告', intent: 'credit_one_page_report', data: reportData })}>报告消息</button>
    <button onClick={clearConversation}>清空</button>
    <output data-testid="latest-assistant-message">{JSON.stringify(messages.at(-1) ?? null)}</output>
  </>;
}

describe('workspace assistant credit report response', () => {
  beforeEach(() => {
    Element.prototype.scrollIntoView = vi.fn();
    const { unmount } = render(<StoreControl />);
    fireEvent.click(screen.getByText('清空'));
    unmount();
  });

  afterEach(() => vi.clearAllMocks());

  it('keeps /api/chat data through the assistant message and renders snapshot actions', async () => {
    vi.mocked(sendChat).mockResolvedValue({
      message: '报告已完成',
      intent: 'credit_one_page_report',
      data: reportData,
      reasoning: null,
    });
    render(<><ChatInput /><ChatMessages /><StoreControl /></>);
    fireEvent.change(screen.getByTestId('ai-message-input'), { target: { value: '生成征信一页纸' } });
    fireEvent.click(screen.getByTestId('ai-send-button'));
    expect(await screen.findByText('预览报告')).toBeTruthy();
    expect(screen.getByText('下载 PDF')).toBeTruthy();
    expect(JSON.parse(screen.getByTestId('latest-assistant-message').textContent || '{}')).toMatchObject({
      role: 'assistant', content: '报告已完成', intent: 'credit_one_page_report', data: reportData,
    });
    expect(sendChat).toHaveBeenCalledTimes(1);
  });

  it('does not show actions for ordinary or incomplete messages', async () => {
    render(<><ChatMessages /><StoreControl /></>);
    fireEvent.click(screen.getByText('普通消息'));
    expect(screen.queryByText('预览报告')).toBeNull();
    fireEvent.click(screen.getByText('报告消息'));
    expect(screen.getByText('预览报告')).toBeTruthy();
    expect(screen.getByText('下载 PDF')).toBeTruthy();
  });
});
