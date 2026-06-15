import React, { useState } from 'react';
import { useNewChatStore } from '../../stores/useNewChatStore';

const ChatInput: React.FC = () => {
  const [inputValue, setInputValue] = useState('');
  const { sending, sendMessage } = useNewChatStore();

  const handleSend = async (): Promise<void> => {
    const content = inputValue.trim();
    if (!content || sending) return;

    setInputValue('');
    await sendMessage(content);
  };

  const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === 'Enter') {
      event.preventDefault();
      void handleSend();
    }
  };

  return (
    <div className="mx-auto flex w-full max-w-3xl gap-2" data-testid="new-chat-input">
      <input
        value={inputValue}
        onChange={(event) => setInputValue(event.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="向 AI 提问..."
        disabled={sending}
        className="h-10 flex-1 rounded-lg border border-slate-300 px-3 text-sm outline-none transition-colors focus:border-slate-500 disabled:bg-slate-100"
      />
      <button
        type="button"
        onClick={() => void handleSend()}
        disabled={sending || !inputValue.trim()}
        className="h-10 rounded-lg bg-slate-900 px-4 text-sm font-medium text-white transition-colors hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
      >
        {sending ? '发送中' : '发送'}
      </button>
    </div>
  );
};

export default ChatInput;
