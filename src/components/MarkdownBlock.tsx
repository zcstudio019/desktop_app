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
          h1: ({ children }) => <h1 className="mb-4 mt-6 border-b border-slate-200 pb-2 text-xl font-bold text-slate-950 first:mt-0">{children}</h1>,
          h2: ({ children }) => <h2 className="mb-3 mt-0 text-lg font-semibold text-slate-900">{children}</h2>,
          h3: ({ children }) => <h3 className="mb-2 mt-5 text-base font-semibold text-slate-900">{children}</h3>,
          ul: ({ children }) => <ul className="my-2 space-y-1 pl-5">{children}</ul>,
          li: ({ children }) => <li className="pl-1">{children}</li>,
          p: ({ children }) => <p className="my-2">{children}</p>,
          table: ({ children }) => (
            <div className="my-4 max-w-full overflow-x-auto rounded-lg border border-slate-200">
              <table className="m-0 min-w-full border-collapse text-xs">{children}</table>
            </div>
          ),
          th: ({ children }) => <th className="whitespace-nowrap border-b border-r border-slate-200 bg-slate-100 px-3 py-2 text-left font-semibold text-slate-800 last:border-r-0">{children}</th>,
          td: ({ children }) => <td className="border-b border-r border-slate-100 px-3 py-2 align-top last:border-r-0">{children}</td>,
          blockquote: ({ children }) => <blockquote className="my-3 border-l-4 border-blue-300 bg-blue-50 px-4 py-2 text-slate-700">{children}</blockquote>,
          hr: () => <hr className="my-5 border-slate-200" />,
        }}
      >
        {content || '暂无内容'}
      </ReactMarkdown>
    </article>
  );
};

export default MarkdownBlock;
