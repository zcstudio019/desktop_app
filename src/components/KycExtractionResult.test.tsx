import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { KycExtractionResult as KycExtractionResultType } from '../services/types';
import KycExtractionResult, { isKycExtractionResult } from './KycExtractionResult';

const marriageMarkdown = `## 结婚证
- 资料类型：结婚证
- 来源文件：林勇结婚证.pdf
- 原件状态：可查看
- 提取状态：成功
- 婚姻状态：已婚
- 结婚证字号：政字第2002208号
- 登记机关：浙江省乐清市民政局
- 发证日期：2022-03-15
- 登记日期：2022-03-15

### 配偶一
- 姓名：林勇
- 性别：男
- 国籍：中国
- 出生日期：1979-03-16
- 身份证号：未识别
- 疑似身份证号：330323790316243

### 配偶二
- 姓名：黄晓回
- 性别：女
- 国籍：中国
- 出生日期：1979-11-08
- 身份证号：未识别
- 疑似身份证号：330323791108192`;

describe('KycExtractionResult', () => {
  it('renders marriage certificate markdown without raw result keys', () => {
    const result: KycExtractionResultType = {
      agent_type: 'kyc_document_agent',
      doc_type: 'marriage_certificate',
      doc_type_name: '结婚证',
      owner_type: 'person',
      extraction_status: 'success',
      fields: {
        certificate_no: '政字第2002208号',
      },
      validation: {
        warnings: ['配偶一身份证号疑似 OCR 缺位'],
      },
      confidence: {
        overall: 0.8,
      },
      evidence: {
        certificate_no: { value: '政字第2002208号' },
      },
      missing_fields: ['配偶一身份证号'],
      markdown: marriageMarkdown,
    };

    const { container } = render(<KycExtractionResult result={result} />);
    const text = container.textContent || '';

    expect(screen.getByText('结婚证')).toBeInTheDocument();
    expect(text).toContain('结婚证字号：政字第2002208号');
    expect(text).toContain('登记机关：浙江省乐清市民政局');
    expect(text).toContain('疑似身份证号：330323790316243');
    expect(text).not.toContain('fields');
    expect(text).not.toContain('validation');
    expect(text).not.toContain('confidence');
    expect(text).not.toContain('evidence');
    expect(text).not.toContain('raw text preview');
    expect(text).not.toContain('metadata');
    expect(text).not.toContain('agent type');
  });

  it('detects nested KYC payloads before generic object rendering', () => {
    expect(isKycExtractionResult({
      content: {
        docType: 'marriage_certificate',
        markdown: marriageMarkdown,
      },
    })).toBe(true);
  });

  it('prefers nested business markdown over legacy raw-json markdown', () => {
    const legacyMarkdown = `## 结婚证
- doc type：marriage_certificate
- fields：{"certificate_no":"政字第2002208号"}
- validation：{"is_valid":true}
- confidence：{"overall":0.8}
- evidence：{"certificate_no":{"value":"政字第2002208号"}}`;

    const { container } = render(
      <KycExtractionResult
        result={{
          agent_type: 'kyc_document_agent',
          doc_type: 'marriage_certificate',
          doc_type_name: '结婚证',
          extraction_status: 'success',
          fields: {},
          markdown: legacyMarkdown,
          extracted_data: {
            agent_type: 'kyc_document_agent',
            doc_type: 'marriage_certificate',
            markdown: marriageMarkdown,
          },
        } as unknown as KycExtractionResultType}
      />,
    );
    const text = container.textContent || '';

    expect(text).toContain('结婚证字号：政字第2002208号');
    expect(text).toContain('登记机关：浙江省乐清市民政局');
    expect(text).not.toContain('fields');
    expect(text).not.toContain('validation');
    expect(text).not.toContain('confidence');
    expect(text).not.toContain('evidence');
    expect(text).not.toContain('doc type');
  });
});
