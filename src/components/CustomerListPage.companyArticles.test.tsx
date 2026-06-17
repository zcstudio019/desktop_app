import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { CustomerDetailFieldRenderer } from './CustomerListPage';

const canonicalMarkdown = [
  '## 公司章程',
  '',
  '* 资料类型：公司章程',
  '* 来源文件：乐芙兰章程(新 沃志方).pdf',
  '* 原件状态：可查看',
  '',
  '### 基本信息',
  '',
  '* 章程标题：上海乐芙兰电子商务有限公司章程',
  '* 公司名称：上海乐芙兰电子商务有限公司',
  '* 公司住所：上海市长宁区广顺路33号3幢6层672室',
  '* 注册资本：人民币500万元',
  '* 经营范围：许可项目：食品经营。一般项目：化妆品销售；互联网销售（除销售需要许可的商品）。',
  '* 章程生效规则：本章程自全体股东盖章、签字之日起生效',
  '* 签署日期：未填写/未识别',
  '',
  '### 股东及出资信息',
  '',
  '| 股东姓名/名称 | 出资额 | 出资方式 | 出资时间 | 出资比例 |',
  '|---|---:|---|---|---:|',
  '| 沃志方 | 495万元 | 货币 | 2030.12.31 | 99.00% |',
  '| 李倩 | 5万元 | 货币 | 2030.12.31 | 1.00% |',
  '',
  '* 出资校验：出资额合计与注册资本一致',
  '* 法定代表人：由执行董事担任',
  '* 需人工复核：无',
].join('\n');

describe('CustomerDetailFieldRenderer company articles', () => {
  it('short-circuits company articles payloads to canonical display markdown', () => {
    const { container } = render(
      <CustomerDetailFieldRenderer
        sectionName="公司章程"
        sectionValue={{
          doc_type: 'company_articles',
          extraction_version: 'company_articles_v3_canonical_markdown_only',
          structured_data: {
            title: '上海乐芙兰电子商务有限公司章程',
            company_address: '上海市长宁区广顺路33号3幢6层672室',
            registered_capital_amount: 500,
            governance: { legal_representative: '由执行董事担任' },
          },
          display_markdown: canonicalMarkdown,
          markdown: canonicalMarkdown,
          report_markdown: canonicalMarkdown,
        }}
      />,
    );
    const text = container.textContent || '';
    const lower = text.toLowerCase();

    expect(text).toContain('公司住所：上海市长宁区广顺路33号3幢6层672室');
    expect(text).toContain('注册资本：人民币500万元');
    expect(text).toContain('经营范围：许可项目：食品经营。');
    expect(text).toContain('沃志方');
    expect(text).toContain('99.00%');
    expect(text).toContain('李倩');
    expect(text).toContain('1.00%');
    expect(text).toContain('法定代表人：由执行董事担任');
    expect(text).toContain('需人工复核：无');
    [
      'doc type',
      'doc type name',
      'extraction version',
      'structured data',
      'display markdown',
      'report markdown',
      'markdown：',
      'title：',
      'company address',
      'registered capital amount',
      'capital check',
      'governance',
      'major resolution rules',
      'signature info',
      'page count',
      'registered_capital_amount',
      'legal_representative',
      'business_scope',
      'signature_page',
      'warnings',
      '{',
      '}',
      '法定代表人：暂无',
    ].forEach((item) => expect(lower).not.toContain(item.toLowerCase()));
  });
});
