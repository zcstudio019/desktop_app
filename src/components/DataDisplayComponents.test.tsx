import { describe, expect, it } from 'vitest';
import { getContractDisplayMarkdown } from './DataDisplayComponents';

describe('getContractDisplayMarkdown', () => {
  it('returns only sanitized markdown for a flat contract result', () => {
    const markdown = getContractDisplayMarkdown({
      doc_type: 'contract',
      owner_type: 'company',
      contract_category: 'construction_subcontract',
      markdown_result: [
        '- owner type：company',
        '- markdown result：## 合同',
        '## 合同',
        '- 资料类型：合同',
        '- evidence：{',
        '  "project_name": {"value": "测试项目", "source_page": 1}',
        '}',
      ].join('\n'),
    });

    expect(markdown).toBe('## 合同\n- 资料类型：合同');
  });

  it('finds markdown inside nested parsed results', () => {
    const markdown = getContractDisplayMarkdown({
      result: {
        parsed_result: {
          agent_type: 'contract_agent',
          display_markdown: '## 合同\n\n- 合同类型：建设工程专业分包合同',
          evidence: { project_name: { value: '测试项目' } },
        },
      },
    });

    expect(markdown).toBe('## 合同\n\n- 合同类型：建设工程专业分包合同');
  });

  it('uses the contract fallback without exposing outer fields', () => {
    expect(getContractDisplayMarkdown({ doc_type: 'contract', evidence: { raw_text: 'secret' } }))
      .toBe('合同解析结果暂不可用，请重新解析或人工复核。');
  });
});
