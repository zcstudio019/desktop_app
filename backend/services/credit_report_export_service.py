"""Frozen report snapshots and offline Chromium PDF export. Never invokes an LLM."""
import asyncio
import copy
import logging
import re
import uuid
from urllib.parse import quote

from backend.services.credit_report_html_renderer import render_credit_report_html

logger = logging.getLogger(__name__)
SNAPSHOT_VERSION = 'credit_one_page_report_v1'
_pdf_slots = asyncio.Semaphore(2)


class CreditReportExportUnavailable(RuntimeError):
    pass


async def freeze_credit_report(storage, customer_id, model, narrative, generated_at, markdown):
    """Use the existing immutable snapshot table; the report kind isolates history."""
    creator = getattr(storage, 'create_financing_diagnostic_report_snapshot', None)
    getter = getattr(storage, 'get_credit_report_snapshot', None)
    if not callable(creator) or not callable(getter):
        return {}
    report_id = uuid.uuid4().hex
    await creator({
        'report_id':report_id, 'customer_id':customer_id,
        'report_version':SNAPSHOT_VERSION, 'report_status':'completed',
        'report_json':copy.deepcopy({'report_type':SNAPSHOT_VERSION, 'model':model, 'narrative':narrative, 'generated_at':generated_at}),
        'report_markdown':markdown, 'generated_at':generated_at,
        'source_summary':{'report_type':SNAPSHOT_VERSION},
    })
    base = f'/api/customers/{quote(customer_id, safe="")}/credit-report/snapshots/{report_id}'
    return {'reportId':report_id, 'previewUrl':base+'/preview', 'pdfUrl':base+'/export/pdf'}


def snapshot_html(snapshot):
    payload = snapshot.get('report_json') or {}
    if payload.get('report_type') != SNAPSHOT_VERSION:
        raise ValueError('未找到该征信报告版本')
    return render_credit_report_html(payload['model'], payload['narrative'], payload['generated_at'])


def pdf_filename(snapshot):
    payload = snapshot.get('report_json') or {}
    subject = (payload.get('model') or {}).get('subjects') or {}
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f\x7f]', '_', str(subject.get('customer_subject') or '未命名客户')).strip(' .')[:80] or '未命名客户'
    date = re.sub(r'\D', '', str(payload.get('generated_at') or '')[:10]) or '日期未记录'
    return f'征信速览报告_{name}_{date}.pdf'


async def render_html_pdf(html):
    """Only trusted local template HTML; deny every network/resource request."""
    try:
        from playwright.async_api import async_playwright
        async with _pdf_slots, async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                context = await browser.new_context(java_script_enabled=False, service_workers='block')
                await context.route('**/*', lambda route: route.abort())
                page = await context.new_page()
                await page.set_content(html, wait_until='load', timeout=30000)
                await page.emulate_media(media='print')
                # Font readiness is evaluated by automation, not by document scripts.
                await page.evaluate('document.fonts.ready')
                return await page.pdf(format='A4', print_background=True, prefer_css_page_size=True)
            finally:
                await browser.close()
    except Exception:
        logger.exception('Credit report PDF renderer unavailable')
        raise CreditReportExportUnavailable('PDF 生成暂不可用，请稍后重试或联系管理员检查浏览器渲染环境。') from None
