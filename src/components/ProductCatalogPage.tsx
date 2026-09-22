import React from 'react';
import LocalProductCatalogSection from './admin/LocalProductCatalogSection';

const ProductCatalogPage: React.FC = () => (
  <div className="space-y-6 p-6 md:p-8" data-testid="product-catalog-page">
    <header>
      <h1 className="text-2xl font-bold text-slate-900">产品库管理</h1>
      <p className="mt-2 text-sm text-slate-500">维护本地 Markdown 产品源、产品版本、准入规则与发布状态</p>
    </header>
    <LocalProductCatalogSection />
  </div>
);

export default ProductCatalogPage;
