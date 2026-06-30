import { describe, expect, it } from 'vitest';
import { getContractDisplayMarkdown, sanitizeContractSectionsInProfile } from './DataDisplayComponents';

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

  it('removes the legacy contract object shell from cached profile markdown', () => {
    const profile = [
      '## 合同',
      '- 资料类型：合同',
      '- 来源文件：合同002.pdf',
      '- owner type：company',
      '- contract category：construction_subcontract',
      '- markdown result：## 合同',
      '- 资料类型：合同',
      '- 合同类型：建设工程专业分包合同',
      '',
      '### 合同基本信息',
      '- 合同名称：机电安装工程专业分包合同（南区）',
      '',
      '### 合同主体',
      '| 角色 | 名称 |',
      '| --- | --- |',
      '| 甲方 | 上海建工集团股份有限公司 |',
      '',
      '### 解析质量提示',
      '- 关键字段完整度：部分完整',
      '- evidence：{"project_name":{"source_page":1,"confidence":0.7}}',
    ].join('\n');

    const cleaned = sanitizeContractSectionsInProfile(profile);
    expect(cleaned.startsWith('## 合同\n- 资料类型：合同')).toBe(true);
    expect((cleaned.match(/^## 合同$/gm) || [])).toHaveLength(1);
    ['owner type', 'contract category', 'markdown result', 'evidence', 'source_page', 'confidence']
      .forEach((item) => expect(cleaned.toLowerCase()).not.toContain(item.toLowerCase()));
    expect(cleaned).toContain('### 合同基本信息');
    expect(cleaned).toContain('### 合同主体');
  });
});
