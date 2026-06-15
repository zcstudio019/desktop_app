import React from 'react';
import { ChevronDown } from 'lucide-react';
import { DEFAULT_CHAT_MODEL, OPENAI_CHAT_MODEL, useChatStore } from '../../stores/useChatStore';

const MODEL_OPTIONS = Array.from(new Set([DEFAULT_CHAT_MODEL, OPENAI_CHAT_MODEL].filter(Boolean)));

const ModelSelector: React.FC = () => {
  const { selectedModel, setSelectedModel } = useChatStore();

  return (
    <label className="relative inline-flex items-center">
      <span className="sr-only">选择模型</span>
      <select
        value={selectedModel}
        onChange={(event) => setSelectedModel(event.target.value)}
        className="h-9 appearance-none rounded-lg border border-slate-200 bg-white pl-3 pr-8 text-xs font-medium text-slate-700 outline-none transition-colors hover:bg-slate-50 focus:border-blue-400"
        data-testid="ai-model-selector"
      >
        {MODEL_OPTIONS.map((model) => (
          <option key={model} value={model}>
            {model}
          </option>
        ))}
      </select>
      <ChevronDown className="pointer-events-none absolute right-2 h-4 w-4 text-slate-400" />
    </label>
  );
};

export default ModelSelector;
