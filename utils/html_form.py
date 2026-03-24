"""Markdown 转可编辑 HTML 表单

将 Markdown 格式的申请表转换为可交互的 HTML 表单。
复选框可点击，文本内容可编辑，支持导出修改后的 Markdown。

来源：扣子插件/扣子插件_MD转HTML_Worker托管版.py
"""

import re


def convert_md_to_html(md: str) -> str:
    """将 Markdown 转换为可编辑的 HTML 表单
    
    Args:
        md: Markdown 格式的申请表内容
        
    Returns:
        str: 完整的 HTML 页面
    """
    lines = md.split('\n')
    html_parts = []
    page_title = "信息表"
    in_table = False
    table_rows = []
    current_section = ""
    
    for line in lines:
        line_stripped = line.strip()
        
        # 一级标题 - 页面标题
        if line_stripped.startswith('# ') and not line_stripped.startswith('## '):
            page_title = line_stripped[2:].strip()
            continue
        
        # 二级/三级标题 - 章节标题
        m = re.match(r'^#{2,3}\s+(.+)$', line_stripped)
        if m:
            # 先结束之前的表格
            if in_table and table_rows:
                html_parts.append(build_table(table_rows, current_section))
                table_rows = []
                in_table = False
            current_section = m.group(1)
            continue
        
        # 表格行
        if line_stripped.startswith('|') and '|' in line_stripped[1:]:
            # 跳过分隔行
            if re.match(r'^\|[\s\-:|\s]+\|$', line_stripped):
                continue
            # 跳过表头行
            cells = [c.strip() for c in line_stripped.split('|')[1:-1]]
            if len(cells) >= 2 and cells[0] in ['项目', '字段', '名称', '信息项']:
                in_table = True
                continue
            if len(cells) >= 2:
                in_table = True
                table_rows.append(cells)
            continue
        
        # 非表格行，结束当前表格
        if in_table and table_rows:
            html_parts.append(build_table(table_rows, current_section))
            table_rows = []
            in_table = False
    
    # 处理最后一个表格
    if table_rows:
        html_parts.append(build_table(table_rows, current_section))
    
    return build_page(page_title, ''.join(html_parts))


def build_table(rows: list, section_title: str) -> str:
    """构建表格 HTML，复选框可点击，其他内容用 textarea
    
    Args:
        rows: 表格行数据，每行是一个列表 [项目名, 内容]
        section_title: 章节标题
        
    Returns:
        str: 表格 HTML
    """
    html = '<div class="section">'
    if section_title:
        html += f'<div class="section-title" data-original="{escape_html(section_title)}">{escape_html(section_title)}</div>'
    
    html += '<div class="table-wrap">'
    for i, row in enumerate(rows):
        if len(row) >= 2:
            col1 = row[0]  # 项目名
            col2 = row[1]  # 内容
            # 检查是否包含复选框
            if '□' in col2 or '☑' in col2:
                col2_html = build_checkbox_content(col2, i)
                html += f'''<div class="row" data-col1="{escape_attr(col1)}" data-has-checkbox="true">
                    <div class="col1">{escape_html(col1)}</div>
                    <div class="col2 checkbox-cell">{col2_html}</div>
                </div>'''
            else:
                html += f'''<div class="row" data-col1="{escape_attr(col1)}">
                    <div class="col1">{escape_html(col1)}</div>
                    <div class="col2"><textarea class="cell-input" data-original="{escape_attr(col2)}">{escape_html(col2)}</textarea></div>
                </div>'''
    html += '</div></div>'
    return html


def build_checkbox_content(text: str, row_idx: int) -> str:
    """将包含□和☑的文本转换为可点击的复选框，所有文本都可编辑
    
    Args:
        text: 包含复选框符号的文本
        row_idx: 行索引，用于生成唯一 ID
        
    Returns:
        str: 复选框 HTML
    """
    html = f'<div class="checkbox-wrap" data-original="{escape_attr(text)}">'
    
    # 按分号分割成多个部分
    segments = re.split(r'(；|;)', text)
    
    cb_idx = 0
    text_idx = 0
    for seg_idx, segment in enumerate(segments):
        segment = segment.strip()
        if not segment:
            continue
        
        # 如果是分号分隔符，直接输出
        if segment in ['；', ';']:
            html += '<span class="cb-sep">；</span>'
            continue
        
        # 检查这个 segment 是否包含复选框
        if '□' in segment or '☑' in segment:
            # 包含复选框，需要解析
            # 先检查是否有前缀标签（如"经营状态："）
            prefix_match = re.match(r'^([^□☑]+[：:])(.*)$', segment)
            if prefix_match:
                prefix = prefix_match.group(1)
                checkbox_part = prefix_match.group(2)
                # 前缀标签也可编辑
                html += f'<input type="text" class="cb-prefix-input" value="{escape_attr(prefix)}" data-idx="prefix_{row_idx}_{text_idx}">'
                text_idx += 1
            else:
                checkbox_part = segment
            
            # 解析复选框部分
            parts = re.split(r'([□☑])', checkbox_part)
            i = 0
            while i < len(parts):
                part = parts[i]
                if part in ['□', '☑']:
                    checked = 'checked' if part == '☑' else ''
                    # 获取复选框后面的选项名
                    label = ''
                    remaining = ''
                    if i + 1 < len(parts):
                        next_part = parts[i + 1]
                        # 匹配选项名称
                        match = re.match(r'^([^\s（(]+)', next_part.strip())
                        if match:
                            label = match.group(1)
                            remaining = next_part[next_part.find(label) + len(label):] if label else next_part
                    
                    cb_id = f'cb_{row_idx}_{cb_idx}'
                    html += f'<label class="cb-label"><input type="checkbox" id="{cb_id}" {checked}><span class="cb-text">{escape_html(label)}</span></label>'
                    
                    # 处理括号内容 - 都改成可编辑
                    if remaining.strip():
                        bracket_match = re.match(r'^[（(]([^）)]*)[）)](.*)$', remaining.strip())
                        if bracket_match:
                            bracket_content = bracket_match.group(1)
                            after_bracket = bracket_match.group(2)
                            # 括号内容都可编辑
                            placeholder = '请输入' if ('待补充' in bracket_content or '___' in bracket_content or not bracket_content.strip()) else ''
                            value = '' if ('待补充' in bracket_content or '___' in bracket_content) else bracket_content
                            html += f'<span class="cb-sep">（</span><input type="text" class="cb-input" value="{escape_attr(value)}" placeholder="{escape_attr(placeholder)}"><span class="cb-sep">）</span>'
                            if after_bracket.strip():
                                # 后缀也可编辑
                                html += f'<input type="text" class="cb-suffix-input" value="{escape_attr(after_bracket.strip())}" data-idx="suffix_{row_idx}_{text_idx}">'
                                text_idx += 1
                        else:
                            # 普通后缀文本 - 可编辑
                            html += f'<input type="text" class="cb-suffix-input" value="{escape_attr(remaining.strip())}" data-idx="suffix_{row_idx}_{text_idx}">'
                            text_idx += 1
                    
                    if i + 1 < len(parts):
                        parts[i + 1] = ''
                    cb_idx += 1
                elif part.strip():
                    # 非复选框文本 - 可编辑
                    html += f'<input type="text" class="cb-text-input" value="{escape_attr(part.strip())}" data-idx="text_{row_idx}_{text_idx}">'
                    text_idx += 1
                i += 1
        else:
            # 不包含复选框的纯文本（如"成立时间：2004年01月"）- 可编辑
            html += f'<input type="text" class="cb-plain-input" value="{escape_attr(segment)}" data-idx="plain_{row_idx}_{text_idx}">'
            text_idx += 1
    
    html += '</div>'
    return html


def escape_html(s: str) -> str:
    """转义 HTML 特殊字符"""
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def escape_attr(s: str) -> str:
    """转义 HTML 属性值"""
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace('\n', '&#10;')


def escape_js(s: str) -> str:
    """转义 JavaScript 字符串"""
    return s.replace('\\', '\\\\').replace("'", "\\'").replace('\n', '\\n').replace('\r', '')


def build_page(title: str, content: str) -> str:
    """生成完整的 HTML 页面
    
    Args:
        title: 页面标题
        content: 页面内容（表格 HTML）
        
    Returns:
        str: 完整的 HTML 页面
    """
    css = '''
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:"PingFang SC","Microsoft YaHei",sans-serif;background:#f5f7fa;padding:20px;color:#333}
.container{width:100%;margin:0 auto;background:#fff;border-radius:8px;box-shadow:0 2px 12px rgba(0,0,0,.08)}
.header{padding:24px;border-bottom:1px solid #eee;display:flex;justify-content:space-between;align-items:center}
.header h1{font-size:20px;font-weight:600}
.header-btns{display:flex;gap:10px}
.content{padding:24px}
.section{margin-bottom:32px}
.section-title{font-size:16px;font-weight:600;color:#1890ff;margin-bottom:16px;padding:8px 12px;background:#f0f7ff;border-radius:4px;border-left:4px solid #1890ff}
.table-wrap{border:1px solid #e8e8e8;border-radius:4px;overflow:hidden}
.row{display:flex;border-bottom:1px solid #e8e8e8}
.row:last-child{border-bottom:none}
.col1{width:180px;min-width:180px;padding:12px 16px;background:#fafafa;font-size:14px;color:#666;border-right:1px solid #e8e8e8;display:flex;align-items:center}
.col2{flex:1;padding:8px}
.cell-input{width:100%;min-height:60px;padding:8px 12px;border:1px solid transparent;border-radius:4px;font-size:14px;line-height:1.6;resize:vertical;background:#fff;font-family:inherit}
.cell-input:hover{border-color:#d9d9d9}
.cell-input:focus{outline:none;border-color:#1890ff;box-shadow:0 0 0 2px rgba(24,144,255,.1)}
.checkbox-cell{padding:12px 16px}
.checkbox-wrap{display:flex;flex-wrap:wrap;align-items:center;gap:4px;line-height:1.8}
.cb-label{display:inline-flex;align-items:center;gap:4px;cursor:pointer;padding:2px 6px;border-radius:3px;transition:background .2s}
.cb-label:hover{background:#f0f7ff}
.cb-label input[type="checkbox"]{width:16px;height:16px;cursor:pointer;accent-color:#1890ff}
.cb-text{font-size:14px;color:#333}
.cb-sep{font-size:14px;color:#666;padding:0 2px}
.cb-input{width:80px;height:28px;padding:0 8px;border:1px solid #d9d9d9;border-radius:4px;font-size:13px}
.cb-input:focus{outline:none;border-color:#1890ff}
.cb-prefix-input,.cb-suffix-input,.cb-text-input,.cb-plain-input{height:28px;padding:0 8px;border:1px solid #e8e8e8;border-radius:4px;font-size:13px;background:#fff;min-width:60px}
.cb-plain-input{min-width:150px}
.cb-prefix-input:focus,.cb-suffix-input:focus,.cb-text-input:focus,.cb-plain-input:focus{outline:none;border-color:#1890ff;box-shadow:0 0 0 2px rgba(24,144,255,.1)}
.btn{padding:10px 20px;border:none;border-radius:4px;font-size:14px;cursor:pointer;transition:all .2s}
.btn-primary{background:#1890ff;color:#fff}
.btn-primary:hover{background:#40a9ff}
.btn-outline{background:#fff;color:#1890ff;border:1px solid #1890ff}
.btn-outline:hover{background:#e6f7ff}
.btn-success{background:#52c41a;color:#fff}
.btn-success:hover{background:#73d13d}
.footer{padding:16px 24px;border-top:1px solid #eee;display:flex;justify-content:flex-end;gap:12px}
.modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);justify-content:center;align-items:center;z-index:1000}
.modal.show{display:flex}
.modal-box{background:#fff;border-radius:8px;width:90%;max-width:700px;max-height:85vh;display:flex;flex-direction:column}
.modal-header{padding:16px 20px;border-bottom:1px solid #eee;display:flex;justify-content:space-between;align-items:center}
.modal-header h3{font-size:16px}
.modal-close{background:none;border:none;font-size:24px;cursor:pointer;color:#999;line-height:1}
.modal-close:hover{color:#333}
.modal-body{padding:20px;flex:1;overflow:auto}
.modal-body textarea{width:100%;height:300px;padding:12px;border:1px solid #d9d9d9;border-radius:4px;font-family:"Consolas","Monaco",monospace;font-size:13px;line-height:1.5;resize:none}
.modal-footer{padding:16px 20px;border-top:1px solid #eee;display:flex;justify-content:flex-end;gap:10px}
.toast{position:fixed;top:20px;left:50%;transform:translateX(-50%) translateY(-100px);background:#52c41a;color:#fff;padding:12px 24px;border-radius:4px;font-size:14px;transition:transform .3s,opacity .3s;opacity:0;z-index:2000}
.toast.show{transform:translateX(-50%) translateY(0);opacity:1}
@media(max-width:600px){
.col1{width:100px;min-width:100px;font-size:13px;padding:10px}
.cell-input{font-size:13px;min-height:50px}
.checkbox-wrap{gap:4px}
.cb-plain-input{min-width:100px}
}
'''.replace('\n', '')

    js = '''
function getMd(){
var md='# ''' + escape_js(title) + '''\\n\\n';
document.querySelectorAll('.section').forEach(function(sec){
var titleEl=sec.querySelector('.section-title');
if(titleEl){
var t=titleEl.getAttribute('data-original')||titleEl.textContent;
md+='## '+t+'\\n\\n';
}
md+='| 项目 | 填写内容 |\\n|------|----------|\\n';
sec.querySelectorAll('.row').forEach(function(row){
var col1=row.getAttribute('data-col1')||'';
var col2='';
var hasCheckbox=row.getAttribute('data-has-checkbox')==='true';
if(hasCheckbox){
var wrap=row.querySelector('.checkbox-wrap');
if(wrap){
var parts=[];
var children=wrap.children;
for(var i=0;i<children.length;i++){
var node=children[i];
if(node.classList.contains('cb-label')){
var cb=node.querySelector('input[type="checkbox"]');
var text=node.querySelector('.cb-text');
var symbol=cb&&cb.checked?'☑':'□';
var labelText=text?text.textContent:'';
parts.push(symbol+' '+labelText);
}else if(node.classList.contains('cb-input')){
var inputVal=node.value||'待补充';
parts.push(inputVal);
}else if(node.classList.contains('cb-sep')){
parts.push(node.textContent);
}else if(node.classList.contains('cb-prefix-input')||node.classList.contains('cb-suffix-input')||node.classList.contains('cb-text-input')||node.classList.contains('cb-plain-input')){
parts.push(node.value||'');
}
}
col2=parts.join('');
}
}else{
var textarea=row.querySelector('.cell-input');
col2=textarea?textarea.value:'';
}
md+='| '+col1+' | '+col2+' |\\n';
});
md+='\\n';
});
return md;
}
function showExport(){
document.getElementById('mdOutput').value=getMd();
document.getElementById('modal').classList.add('show');
}
function closeModal(){document.getElementById('modal').classList.remove('show');}
function copyMd(){
navigator.clipboard.writeText(document.getElementById('mdOutput').value).then(function(){toast('复制成功');});
}
function downloadMd(){
var md=getMd();
var blob=new Blob([md],{type:'text/markdown;charset=utf-8'});
var a=document.createElement('a');
a.href=URL.createObjectURL(blob);
a.download=\'''' + escape_js(title) + '''.md';
a.click();
toast('下载成功');
}
function resetForm(){
document.querySelectorAll('.cell-input').forEach(function(el){
el.value=el.getAttribute('data-original')||'';
});
document.querySelectorAll('.checkbox-wrap').forEach(function(wrap){
var original=wrap.getAttribute('data-original')||'';
var checkboxes=wrap.querySelectorAll('input[type="checkbox"]');
var idx=0;
var matches=original.match(/[□☑]/g)||[];
checkboxes.forEach(function(cb,i){
cb.checked=matches[i]==='☑';
});
});
document.querySelectorAll('.cb-input').forEach(function(el){
el.value='';
});
toast('已重置');
}
function toast(msg){
var t=document.getElementById('toast');
t.textContent=msg;
t.classList.add('show');
setTimeout(function(){t.classList.remove('show');},2000);
}
document.getElementById('modal').onclick=function(e){if(e.target===this)closeModal();};
'''

    html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{escape_html(title)}</title>
<style>{css}</style>
</head>
<body>
<div class="container">
<div class="header">
<h1>{escape_html(title)}</h1>
<div class="header-btns">
<button class="btn btn-outline" onclick="resetForm()">重置</button>
</div>
</div>
<div class="content">{content}</div>
<div class="footer">
<button class="btn btn-outline" onclick="showExport()">导出Markdown</button>
<button class="btn btn-success" onclick="downloadMd()">下载文件</button>
</div>
</div>
<div class="modal" id="modal">
<div class="modal-box">
<div class="modal-header">
<h3>导出 Markdown</h3>
<button class="modal-close" onclick="closeModal()">&times;</button>
</div>
<div class="modal-body">
<textarea id="mdOutput" readonly></textarea>
</div>
<div class="modal-footer">
<button class="btn btn-outline" onclick="closeModal()">关闭</button>
<button class="btn btn-primary" onclick="copyMd()">复制内容</button>
</div>
</div>
</div>
<div class="toast" id="toast"></div>
<script>{js}</script>
</body>
</html>'''
    return html
