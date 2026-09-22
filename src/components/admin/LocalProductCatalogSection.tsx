import React, { useCallback, useEffect, useState } from 'react';
import { RefreshCcw } from 'lucide-react';
import {
  getLocalCatalogConflicts, getLocalCatalogProducts, getLocalCatalogSources, syncLocalCatalog,
  type LocalCatalogConflict, type LocalCatalogProductSummary, type LocalCatalogSourcesResponse,
} from '../../services/api';

const LocalProductCatalogSection: React.FC = () => {
  const [catalog, setCatalog] = useState<LocalCatalogSourcesResponse | null>(null);
  const [products, setProducts] = useState<LocalCatalogProductSummary[]>([]);
  const [conflicts, setConflicts] = useState<LocalCatalogConflict[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');

  const reload = useCallback(async () => {
    try {
      setCatalog(await getLocalCatalogSources());
      setError('');
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '无法读取本地产品库');
    }
  }, []);

  useEffect(() => { void reload(); }, [reload]);

  const showProducts = async (category: string) => {
    setBusy(category);
    setMessage('');
    try {
      const result = await getLocalCatalogProducts(category);
      setProducts(result.items);
      setSelected(category);
      setError('');
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '无法读取产品');
    } finally {
      setBusy(null);
    }
  };

  const showConflicts = async () => {
    setBusy('conflicts');
    try {
      const result = await getLocalCatalogConflicts();
      setConflicts(result.items);
      setSelected('conflicts');
      setError('');
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '无法读取冲突');
    } finally {
      setBusy(null);
    }
  };

  const sync = async (category: string) => {
    setBusy(category);
    setMessage('');
    try {
      const result = await syncLocalCatalog(category);
      setMessage(`已生成 ${result.created_drafts} 个待审核草稿，${result.unchanged} 个来源未变化，${result.conflicts.length} 个冲突编号待处理。未发布产品。`);
      setError('');
      await reload();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '同步失败');
    } finally {
      setBusy(null);
    }
  };

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm" data-testid="local-product-catalog-section">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold text-slate-900">本地 Markdown 产品库</h2>
          <p className="mt-1 text-sm text-slate-500">六份文件是产品编辑源；同步只生成草稿，审核发布后才进入正式目录。</p>
        </div>
        <button type="button" onClick={() => void reload()} className="inline-flex items-center gap-2 rounded-lg border px-3 py-2 text-sm"><RefreshCcw size={16} />刷新状态</button>
      </div>
      {error && <p className="mt-4 rounded-lg bg-rose-50 p-3 text-sm text-rose-700">{error}</p>}
      {message && <p className="mt-4 rounded-lg bg-emerald-50 p-3 text-sm text-emerald-700">{message}</p>}
      <div className="mt-5 overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead className="bg-slate-50 text-left text-slate-600"><tr>
            <th className="p-3">来源</th><th className="p-3">文件更新时间</th><th className="p-3">解析</th><th className="p-3">唯一</th>
            <th className="p-3">待审核</th><th className="p-3">冲突</th><th className="p-3">已发布</th><th className="p-3">操作</th>
          </tr></thead>
          <tbody className="divide-y divide-slate-100">
            {catalog?.sources.map((source) => <tr key={source.category}>
              <td className="p-3 font-medium">{source.label}{source.missing ? '（文件缺失）' : ''}</td>
              <td className="p-3">{source.file_updated_at || '—'}</td>
              <td className="p-3">{source.parsed_count}</td><td className="p-3">{source.unique_count}</td>
              <td className="p-3">{source.needs_review_count}</td><td className="p-3">{source.conflict_count}</td>
              <td className="p-3">{source.published_count ?? '未知'}</td>
              <td className="p-3"><div className="flex gap-2 whitespace-nowrap">
                <button type="button" disabled={!!busy || source.missing} onClick={() => void sync(source.category)} className="rounded-lg bg-blue-600 px-3 py-1.5 text-white disabled:opacity-40">同步</button>
                <button type="button" disabled={!!busy} onClick={() => void showProducts(source.category)} className="rounded-lg border px-3 py-1.5 disabled:opacity-40">查看产品</button>
                <button type="button" disabled={!!busy} onClick={() => void showConflicts()} className="rounded-lg border px-3 py-1.5 disabled:opacity-40">查看冲突</button>
              </div></td>
            </tr>)}
          </tbody>
        </table>
      </div>
      {selected === 'conflicts' && <div className="mt-5 space-y-2 text-sm">
        <h3 className="font-semibold">冲突编号（{conflicts.length}）</h3>
        {conflicts.map((item) => <div key={item.external_product_code} className="rounded-lg bg-amber-50 p-3">
          <b>{item.external_product_code}</b>：{item.sides.map((side) => `${side.source_file} / ${side.product_name} / ${side.snapshot_hash.slice(0, 12)}`).join('；')}
        </div>)}
      </div>}
      {selected && selected !== 'conflicts' && <div className="mt-5 text-sm">
        <h3 className="font-semibold">解析产品（{products.length}）</h3>
        <div className="mt-2 max-h-64 overflow-y-auto rounded-lg border p-3">
          {products.map((item, index) => <p key={`${item.external_product_code}-${index}`} className="py-1">
            {item.external_product_code} · {item.institution_name || '机构待审核'} · {item.product_name}
            {item.duplicate_conflict ? <span className="ml-2 text-amber-700">编号冲突</span> : null}
          </p>)}
        </div>
      </div>}
    </section>
  );
};

export default LocalProductCatalogSection;
