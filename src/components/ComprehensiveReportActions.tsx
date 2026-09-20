import { useState } from 'react';
import { downloadComprehensiveReportPdf, fetchComprehensiveReportArtifact } from '../services/api';

export function ComprehensiveReportActions({ data }: { data: Record<string, unknown> }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const preview = typeof data.previewUrl === 'string' ? data.previewUrl : '';
  const pdf = typeof data.pdfUrl === 'string' ? data.pdfUrl : '';
  if (data.reportStatus !== 'completed' || data.reportType !== 'comprehensive_financing_analysis_report') {
    return data.exportMessage ? <p className="text-xs text-amber-700">{String(data.exportMessage)}</p> : null;
  }
  if (!preview || !pdf) return data.exportMessage ? <p className="text-xs text-amber-700">{String(data.exportMessage)}</p> : null;

  async function open(kind: 'preview' | 'pdf') {
    setBusy(true); setError('');
    const tab = kind === 'preview' ? window.open('', '_blank') : null;
    if (tab) tab.opener = null;
    try {
      if (kind === 'pdf') await downloadComprehensiveReportPdf(pdf);
      else {
        if (!tab) throw new Error('浏览器阻止了预览窗口，请允许弹出窗口后重试');
        const { blob } = await fetchComprehensiveReportArtifact(preview);
        const url = URL.createObjectURL(blob);
        tab.location.replace(url);
        window.setTimeout(() => URL.revokeObjectURL(url), 60000);
      }
    } catch (cause) {
      tab?.close();
      setError(cause instanceof Error ? cause.message : '综合报告打开失败，请稍后重试');
    } finally { setBusy(false); }
  }

  return <div className="mt-3 border-t border-gray-200 pt-3" data-testid="comprehensive-report-actions">
    <div className="flex gap-2">
      <button disabled={busy} onClick={() => void open('preview')} className="rounded border border-slate-300 px-3 py-1.5 text-xs text-slate-700 disabled:opacity-50">预览综合报告</button>
      <button disabled={busy} onClick={() => void open('pdf')} className="rounded bg-slate-800 px-3 py-1.5 text-xs text-white disabled:opacity-50">{busy ? '正在读取…' : '下载综合报告 PDF'}</button>
    </div>
    {error && <p role="alert" className="mt-2 text-xs text-red-700">{error}</p>}
  </div>;
}
