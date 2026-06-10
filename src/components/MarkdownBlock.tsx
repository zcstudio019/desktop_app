import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface MarkdownBlockProps {
  content: string;
  className?: string;
}

const MarkdownBlock: React.FC<MarkdownBlockProps> = ({ content, className = '' }) => {
  return (
    <article className={`prose prose-slate max-w-none text-sm leading-7 text-slate-800 ${className}`}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          h2: ({ children }) => <h2 className="mb-3 mt-0 text-lg font-semibold text-slate-900">{children}</h2>,
          h3: ({ children }) => <h3 className="mb-2 mt-5 text-base font-semibold text-slate-900">{children}</h3>,
          ul: ({ children }) => <ul className="my-2 space-y-1 pl-5">{children}</ul>,
          li: ({ children }) => <li className="pl-1">{children}</li>,
          p: ({ children }) => <p className="my-2">{children}</p>,
        }}
      >
        {content || '暂无内容'}
      </ReactMarkdown>
    </article>
  );
};

export default MarkdownBlock;
