# 贷款申请助手 - 桌面版

基于 Streamlit 的贷款申请助手桌面应用，替代扣子智能体。

## 功能

- 📤 **上传资料提取**：支持 PDF、图片、Excel、Word，自动识别类型并提取信息
- 🔍 **方案匹配**：根据客户资料匹配贷款产品
- 📝 **申请表生成**：自动填充贷款申请表
- 📊 **客户列表**：查看飞书多维表格中的客户数据

## 安装

```bash
# 1. 创建虚拟环境
python -m venv venv
venv\Scripts\activate  # Windows

# 2. 安装依赖
pip install -r requirements.txt

# 3. 配置环境变量
copy .env.example .env
# 编辑 .env 填入你的 API Key
```

## 配置

编辑 `.env` 文件：

```
# DeepSeek API
DEEPSEEK_API_KEY=your_key
DEEPSEEK_BASE_URL=https://api.deepseek.com

# 飞书多维表格
FEISHU_APP_ID=your_app_id
FEISHU_APP_SECRET=your_app_secret
FEISHU_APP_TOKEN=your_bitable_app_token
FEISHU_TABLE_ID=your_table_id

# 百度 OCR
BAIDU_OCR_API_KEY=your_key
BAIDU_OCR_SECRET_KEY=your_secret
```

## 运行

```bash
# 方式1：双击 run.bat

# 方式2：命令行
streamlit run app.py
```

浏览器访问 http://localhost:8501

## 目录结构

```
desktop_app/
├── app.py              # 主入口
├── config.py           # 配置
├── services/           # 服务层
│   ├── ai_service.py       # DeepSeek AI
│   ├── ocr_service.py      # 百度 OCR
│   ├── feishu_service.py   # 飞书 API
│   └── file_service.py     # 文件处理
├── prompts/            # 提示词（复用项目现有）
├── requirements.txt
└── .env
```

## 与扣子工作流的对应关系

| 扣子工作流 | 桌面版功能 |
|-----------|-----------|
| Upload_Flying | 上传资料提取 |
| MatchingScheme | 方案匹配 |
| Applicationform | 申请表生成 |

## 优势

- ✅ 本地运行，稳定可控
- ✅ 无需依赖扣子平台
- ✅ 可自定义提示词和逻辑
- ✅ 数据仍存储在飞书多维表格
