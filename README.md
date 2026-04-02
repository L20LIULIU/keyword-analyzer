# Amazon 广告关键词分析看板

基于 Dash + Supabase 的 Amazon 广告优化分析工具。

## 功能

- **模式1: 关键词竞争分析** — 上传西柚找词数据，对比本品与竞品关键词差距
- **模式2: 广告监控看板** — 上传领星广告报告，生成费比/关键词/否定词分析

## 部署

### 本地运行
```bash
pip install -r requirements.txt
python app.py
```

### Render.com 部署
1. Fork 此仓库到你的 GitHub
2. 在 Render.com 创建 Web Service，连接 GitHub 仓库
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `gunicorn app:server --bind 0.0.0.0:$PORT --timeout 120`

## 技术栈

- Python Dash + Plotly
- Dash Bootstrap Components
- Pandas / NumPy / OpenPyXL
- Gunicorn (生产服务器)
