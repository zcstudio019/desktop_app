import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ExtractionResultCard, getDisplayMarkdown } from './ChatPage';

const companyArticlesMarkdown = [
  '## 公司章程',
  '- 资料类型：公司章程',
  '- 来源文件：乐芙兰章程(新 沃志方).pdf',
  '- 原件状态：可查看',
  '',
  '### 基本信息',
  '- 章程标题：上海乐芙兰电子商务有限公司章程',
  '- 公司名称：上海乐芙兰电子商务有限公司',
  '- 公司住所：上海市长宁区广顺路33号3幢6层672室',
  '- 注册资本：人民币500万元',
  '- 法定代表人：由执行董事担任',
  '',
  '### 股东及出资信息',
  '| 股东姓名/名称 | 出资额 | 出资方式 | 出资时间 | 出资比例 |',
  '|---|---:|---|---|---:|',
  '| 沃志方 | 495万元 | 货币 | 2030.12.31 | 99.00% |',
  '| 李倩 | 5万元 | 货币 | 2030.12.31 | 1.00% |',
  '',
  '- 出资校验：出资额合计与注册资本一致',
  '- 需人工复核：无',
].join('\n');

describe('company articles final display', () => {
  it('finds display markdown through nested cached payloads', () => {
    const markdown = getDisplayMarkdown({
      markdown: '## 公司章程\n- 法定代表人：暂无',
      extracted_json: {
        doc_type: 'company_articles',
        display_markdown: companyArticlesMarkdown,
        structured_data: {
          governance: { legal_representative: '由执行董事担任' },
        },
      },
    });

    expect(markdown).toBe(companyArticlesMarkdown);
  });

  it('renders only one final markdown block instead of object keys', () => {
    const { container } = render(
      <ExtractionResultCard
        files={[
          {
            filename: '乐芙兰章程(新 沃志方).pdf',
            documentType: '公司章程',
            content: {
              extracted_json: {
                doc_type: 'company_articles',
                display_markdown: companyArticlesMarkdown,
                markdown: '## 公司章程\n- 法定代表人：暂无',
                report_markdown: companyArticlesMarkdown,
                structured_data: {
                  registered_capital_amount: 500,
                  capital_check: { message: '出资额合计与注册资本一致' },
                  governance: { legal_representative: '由执行董事担任' },
                  major_resolution_rules: { amendment_rule: '须经代表全体股东三分之二以上表决权的股东通过' },
                  signature_info: { signature_page: '第6页' },
                },
                raw_text_preview: 'raw text preview',
                evidence: { source_pages: [1, 2, 3, 4, 5, 6] },
                metadata: { filename: '乐芙兰章程(新 沃志方).pdf' },
              },
            },
          },
        ]}
      />,
    );
    const text = container.textContent || '';
    const lower = text.toLowerCase();

    expect(text).toContain('资料类型：公司章程');
    expect(text).toContain('章程标题：上海乐芙兰电子商务有限公司章程');
    expect(text).toContain('公司住所：上海市长宁区广顺路33号3幢6层672室');
    expect(text).toContain('注册资本：人民币500万元');
    expect(text).toContain('沃志方');
    expect(text).toContain('495万元');
    expect(text).toContain('99.00%');
    expect(text).toContain('李倩');
    expect(text).toContain('1.00%');
    expect(text).toContain('出资校验：出资额合计与注册资本一致');
    expect(text).toContain('法定代表人：由执行董事担任');
    expect(text).toContain('需人工复核：无');
    expect(text).not.toContain('法定代表人：暂无');
    [
      'doc type',
      'doc type name',
      'agent type',
      'company address',
      'registered capital amount',
      'capital check',
      'governance',
      'major resolution rules',
      'signature info',
      'page count',
      'markdown：',
      'display markdown',
      'report markdown',
      'raw text preview',
      'evidence',
      'metadata',
      'registered_capital_amount',
      'shareholder_total_amount',
      'legal_representative',
      'source_pages',
      'text_length',
      'customer_id',
      '{',
      '}',
    ].forEach((item) => expect(lower).not.toContain(item.toLowerCase()));
  });
});
