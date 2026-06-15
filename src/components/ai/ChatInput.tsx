import React, { useRef, useState } from 'react';
import { BookOpen, Globe2, Loader2, Mic, Paperclip, Plus, Send, X } from 'lucide-react';
import { sendChat } from '../../services/api';
import type { ChatFile, ChatMessage } from '../../services/types';
import { useChatStore } from '../../stores/useChatStore';

function readFileAsBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || '');
      resolve(result.includes(',') ? result.split(',')[1] : result);
    };
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
  });
}

async function toChatFiles(files: File[]): Promise<ChatFile[]> {
  return Promise.all(
    files.map(async (file) => ({
      name: file.name,
      type: file.type || 'application/octet-stream',
      content: await readFileAsBase64(file),
    }))
  );
}

const ChatInput: React.FC = () => {
  const [value, setValue] = useState('');
  const [localFiles, setLocalFiles] = useState<File[]>([]);
  const [webEnabled, setWebEnabled] = useState(false);
  const [kbEnabled, setKbEnabled] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);
  const {
    messages,
    status,
    sessionId,
    aiContext,
    selectedModel,
    addMessage,
    setStatus,
    setError,
    setSessionId,
  } = useChatStore();

  const busy = status === 'sending';

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>): void => {
    const nextFiles = Array.from(event.target.files || []);
    setLocalFiles((previous) => [...previous, ...nextFiles]);
    event.target.value = '';
  };

  const handleSubmit = async (): Promise<void> => {
    const content = value.trim();
    if ((!content && localFiles.length === 0) || busy) return;

    setValue('');
    setError(null);
    setStatus('sending');

    const filePrefix = localFiles.length
      ? localFiles.map((file) => `[FILE:${file.name}:${file.type || 'application/octet-stream'}]`).join('')
      : '';
    const contextPrefix = [
      `[AIContext:${JSON.stringify(aiContext)}]`,
      `[Model:${selectedModel}]`,
      webEnabled ? '[WebSearch:on]' : '[WebSearch:off]',
      kbEnabled ? '[KnowledgeBase:on]' : '[KnowledgeBase:off]',
    ].join('\n');
    const userMessage: ChatMessage = {
      role: 'user',
      content: `${filePrefix}${content}`,
      clientMessageId: `workspace-user-${Date.now()}`,
      createdAt: new Date().toISOString(),
      deliveryStatus: 'pending',
      status: 'sending',
    };
    const nextMessages = [...messages, userMessage];
    addMessage(userMessage);

    try {
      const files = localFiles.length ? await toChatFiles(localFiles) : undefined;
      setLocalFiles([]);
      const response = await sendChat({
        messages: [
          {
            role: 'system',
            content: contextPrefix,
          },
          ...nextMessages,
        ],
        files,
        sessionId,
      });
      const nextSessionId = (response as { sessionId?: string | null }).sessionId;
      if (nextSessionId) {
        setSessionId(nextSessionId);
      }
      addMessage({
        role: 'assistant',
        content: response.message,
        reasoning: response.reasoning,
        intent: response.intent,
        data: response.data,
        clientMessageId: `workspace-assistant-${Date.now()}`,
        createdAt: new Date().toISOString(),
        messageType: 'text',
      });
      setStatus('idle');
    } catch (error) {
      setError(error instanceof Error ? error.message : '发送失败，请稍后重试。');
      setStatus('error');
    }
  };

  const handleKeyDown = (event: React.KeyboardEvent<HTMLTextAreaElement>): void => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      void handleSubmit();
    }
  };

  return (
    <div className="border-t border-slate-200 bg-white px-4 pb-4 pt-3" data-testid="ai-chat-input">
      {localFiles.length ? (
        <div className="mb-2 flex flex-wrap gap-2">
          {localFiles.map((file, index) => (
            <span key={`${file.name}-${index}`} className="inline-flex max-w-full items-center gap-1 rounded-lg bg-slate-100 px-2 py-1 text-xs text-slate-600">
              <span className="truncate">{file.name}</span>
              <button
                type="button"
                onClick={() => setLocalFiles((files) => files.filter((_, fileIndex) => fileIndex !== index))}
                className="rounded p-0.5 hover:bg-slate-200"
                aria-label="移除附件"
              >
                <X className="h-3 w-3" />
              </button>
            </span>
          ))}
        </div>
      ) : null}

      <div className="rounded-2xl border border-slate-200 bg-white p-2 shadow-[0_12px_30px_rgba(15,23,42,0.12)]">
        <textarea
          ref={textareaRef}
          value={value}
          onChange={(event) => setValue(event.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="向 AI 助手提问，Shift+Enter 换行"
          rows={2}
          disabled={busy}
          className="max-h-32 min-h-[56px] w-full resize-none rounded-xl border-0 px-2 py-2 text-sm text-slate-800 outline-none placeholder:text-slate-400 disabled:opacity-60"
          data-testid="ai-message-input"
        />

        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1">
            <button type="button" className="rounded-lg p-2 text-slate-500 transition-colors hover:bg-slate-100" title="展开更多">
              <Plus className="h-4 w-4" />
            </button>
            <input
              ref={fileInputRef}
              type="file"
              multiple
              onChange={handleFileChange}
              className="hidden"
              accept=".pdf,.xlsx,.xls,.doc,.docx,.png,.jpg,.jpeg"
            />
            <button
              type="button"
              onClick={() => fileInputRef.current?.click()}
              className="rounded-lg p-2 text-slate-500 transition-colors hover:bg-slate-100"
              title="上传文件"
            >
              <Paperclip className="h-4 w-4" />
            </button>
            <button type="button" className="rounded-lg p-2 text-slate-500 transition-colors hover:bg-slate-100" title="语音输入">
              <Mic className="h-4 w-4" />
            </button>
            <button
              type="button"
              onClick={() => setWebEnabled((enabled) => !enabled)}
              className={`rounded-lg p-2 transition-colors hover:bg-slate-100 ${webEnabled ? 'bg-blue-50 text-blue-600' : 'text-slate-500'}`}
              title="联网搜索"
              aria-pressed={webEnabled}
            >
              <Globe2 className="h-4 w-4" />
            </button>
            <button
              type="button"
              onClick={() => setKbEnabled((enabled) => !enabled)}
              className={`rounded-lg p-2 transition-colors hover:bg-slate-100 ${kbEnabled ? 'bg-blue-50 text-blue-600' : 'text-slate-500'}`}
              title="开启知识库"
              aria-pressed={kbEnabled}
            >
              <BookOpen className="h-4 w-4" />
            </button>
          </div>

          <button
            type="button"
            onClick={() => void handleSubmit()}
            disabled={busy || (!value.trim() && localFiles.length === 0)}
            className="flex h-9 w-9 items-center justify-center rounded-full bg-blue-600 text-white transition-colors hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50"
            title="发送"
            data-testid="ai-send-button"
          >
            {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
          </button>
        </div>
      </div>
    </div>
  );
};

export default ChatInput;
