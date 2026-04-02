"""
pages.py — 使用指南页面 & 产品配置编辑器页面
用于 Amazon 广告关键词分析看板 Dash Web App
"""
import os
import json
import base64
from datetime import datetime

import dash
from dash import html, dcc, dash_table, Input, Output, State, callback
import dash_bootstrap_components as dbc


# ── Supabase 辅助 ─────────────────────────────────────────────────

def _get_sb():
    """独立的 Supabase 客户端，避免循环导入"""
    from supabase import create_client
    url = os.environ.get(
        "SUPABASE_URL", "https://yolbjbfcwducxgqvwxmr.supabase.co"
    )
    key = os.environ.get(
        "SUPABASE_KEY",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlvbGJqYmZjd2R1Y3hncXZ3eG1yIiwi"
        "cm9sZSI6ImFub24iLCJpYXQiOjE3NzUwODE1MzQsImV4cCI6MjA5MDY1NzUzNH0."
        "7c5M3yGkMBfF-mglXc5CMquzL8Jr0NMuwqKOTtrqG2M",
    )
    return create_client(url, key)


# ── 默认产品模板 ──────────────────────────────────────────────────

_EMPTY_PRODUCT = {
    "name": "",
    "asin": "",
    "price_normal": 0,
    "price_promo": 0,
    "target_qty": 0,
    "promo_days": 0,
    "tacos": 0.037,
    "role": "新品",
}

_DEFAULT_GLOBAL = {
    "month_days": 30,
    "tacos_target": 0.037,
    "focus_asin": "",
    "currency": "€",
    "market": "DE",
}


# =====================================================================
#  Page 1: 使用指南页面
# =====================================================================

def guide_page():
    """返回使用指南页面布局"""
    return dbc.Container([
        html.H2("📖 使用指南", className="text-center mt-4 mb-4"),
        html.Hr(),

        dbc.Accordion([
            # ── 1. 工具简介 ──────────────────────────────────────
            dbc.AccordionItem([
                dcc.Markdown("""
这是一款 **Amazon 广告关键词分析工具**，支持两种分析模式：

1. **模式1 — 关键词竞争分析**：对比本品与竞品的关键词数据，发现差距与机会。
2. **模式2 — 广告监控看板**：基于领星广告报告，生成费比监控和优化建议。

两种模式可独立使用，数据分析结果会自动保存到云端数据库，下次打开即可查看。
                """, style={"fontSize": "15px"}),
            ], title="工具简介", item_id="section-intro"),

            # ── 2. 模式1 — 关键词竞争分析 ────────────────────────
            dbc.AccordionItem([
                dcc.Markdown("""
### 功能说明

模式1 用于对比**本品**与**竞品**的关键词数据，帮助发现关键词布局差距和投放机会。
数据来源于**西柚找词 (Xiyou)** 平台导出的关键词报告。

### 输入文件

| 文件 | 格式 | 必需 | 说明 |
|------|------|------|------|
| 本品关键词文件 | `.xlsx` | ✅ 是 | 从西柚找词导出的本品关键词报告 |
| 竞品关键词文件 | `.xlsx` | ✅ 是 | 从西柚找词导出的竞品关键词报告（可多个） |
| products_config.json | `.json` | ❌ 否 | 产品预算规划配置文件 |

### 分析输出 — 10 个分析标签页

1. **执行摘要** — 整体分析结论和关键指标汇总
2. **竞品独有词** — 竞品有而本品没有的关键词，需要布局
3. **排名差距词** — 本品有但排名落后于竞品的关键词，需提升
4. **流量获取差距词** — 流量获取方面的差距分析
5. **本品高潜力词** — 本品表现较好、值得加大投放的关键词
6. **词根聚类** — 按词根聚类，规划 Campaign 结构
7. **综合优先投放** — 综合评分后的优先投放关键词列表
8. **产品预算规划** — 基于产品配置的预算分配建议
9. **流量来源对比** — 自然流量与广告流量的结构对比
10. **月度执行计划** — 月度广告投放执行计划

### 操作步骤

1. 进入「模式1: 关键词分析」页面
2. 点击「上传新数据更新分析」展开上传区域
3. 上传本品关键词文件和竞品关键词文件（支持多选）
4. （可选）上传 `products_config.json` 配置文件
5. 点击「开始分析」按钮
6. 等待分析完成，结果会自动显示在标签页中
7. 可点击各标签页查看详细分析结果
8. 每个表格支持排序、筛选和导出 Excel
                """, style={"fontSize": "14px"}),
            ], title="模式1 — 关键词竞争分析", item_id="section-mode1"),

            # ── 3. 模式2 — 广告监控看板 ──────────────────────────
            dbc.AccordionItem([
                dcc.Markdown("""
### 功能说明

模式2 用于分析广告投放效果，数据来源于**领星 (Lingxing)** 平台导出的广告报告。
生成费比监控看板和优化建议。

### 输入文件

| 文件 | 格式 | 必需 | 说明 |
|------|------|------|------|
| 广告活动报告 | `.xlsx` | ✅ 是 | 领星导出的广告活动（Campaign）报告 |
| 推广商品报告 | `.xlsx` | ✅ 是 | 领星导出的推广商品报告 |
| 搜索词报告 | `.xlsx` | ❌ 否 | 领星导出的搜索词报告 |
| 关键词报告 | `.xlsx` | ❌ 否 | 领星导出的关键词报告 |
| 广告配置.xlsx | `.xlsx` | ❌ 否 | 自定义的广告配置文件（品线费比目标等） |

### 分析输出 — 4 个分析标签页

1. **费比总览** — 各品线/产品的 ACoS/TACoS 费比总览
2. **Campaign明细** — 每个广告活动的详细表现数据
3. **关键词ROI分析** — 关键词级别的 ROI 分析
4. **建议否定词** — 建议否定的低效关键词/搜索词

### 操作步骤

1. 进入「模式2: 广告监控」页面
2. 点击「上传新数据更新分析」展开上传区域
3. 上传**广告活动报告**和**推广商品报告**（必需）
4. （可选）上传搜索词报告、关键词报告、广告配置文件
5. 点击「开始分析」按钮
6. 等待分析完成，结果会自动显示在标签页中
7. 查看各标签页的分析结果
8. 每个表格支持排序、筛选和导出 Excel
                """, style={"fontSize": "14px"}),
            ], title="模式2 — 广告监控看板", item_id="section-mode2"),

            # ── 4. 产品配置说明 ──────────────────────────────────
            dbc.AccordionItem([
                dcc.Markdown("""
### products_config.json 配置格式

该文件用于模式1的产品预算规划功能。如果不上传，将使用默认值。

#### 全局设置字段

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `month_days` | 整数 | 30 | 每月计算天数 |
| `tacos_target` | 小数 | 0.037 | 整体目标费比（TACoS），如 3.7% 填 0.037 |
| `focus_asin` | 字符串 | — | 焦点产品 ASIN |
| `currency` | 字符串 | € | 货币符号（€ / $ / £ / ¥） |
| `market` | 字符串 | DE | 市场代码（DE / US / UK / JP 等） |

#### 产品字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | 字符串 | 产品名称 |
| `asin` | 字符串 | Amazon ASIN 编号 |
| `price_normal` | 数值 | 平日售价 |
| `price_promo` | 数值 | 促销售价 |
| `target_qty` | 整数 | 月度目标销量 |
| `promo_days` | 整数 | 促销天数（0~30） |
| `tacos` | 小数 | 该产品的目标费比 |
| `role` | 字符串 | 产品角色 |

#### 产品角色 (role) 选项

- **核心套装** — 主力产品，预算占比最高
- **新品** — 新上市产品，需要较多广告投入
- **配件** — 配件产品，预算占比较低

#### 预算分配规则

不同角色的预算分配权重不同：
- 核心套装：根据销售额占比分配主要预算
- 新品：额外分配推广预算，支持新品成长
- 配件：按销售额比例分配较少预算

#### 配置文件示例

```json
{
  "month_days": 30,
  "tacos_target": 0.037,
  "focus_asin": "B0XXXXXXXX",
  "currency": "€",
  "market": "DE",
  "products": [
    {
      "name": "产品A - 核心套装",
      "asin": "B0AAAAAAAA",
      "price_normal": 39.99,
      "price_promo": 33.99,
      "target_qty": 300,
      "promo_days": 7,
      "tacos": 0.035,
      "role": "核心套装"
    }
  ]
}
```
                """, style={"fontSize": "14px"}),
            ], title="产品配置说明 (products_config.json)", item_id="section-config"),

            # ── 5. 广告配置说明 ──────────────────────────────────
            dbc.AccordionItem([
                dcc.Markdown("""
### 广告配置.xlsx 文件格式

该文件用于模式2的广告监控分析，提供品线费比目标和产品信息。
文件为 Excel 格式，包含两个 Sheet。

#### Sheet 1: 品线费比配置

配置各品线（产品线）的 ACoS 目标值，用于判断广告活动是否达标。

| 列名 | 说明 |
|------|------|
| 品线名称 | 产品线名称，需与广告活动中的品线标识一致 |
| 目标ACoS | 该品线的目标 ACoS 值（如 0.15 表示 15%） |
| 其他自定义列 | 可添加备注等附加信息 |

#### Sheet 2: ASIN产品信息

配置各 ASIN 的产品基本信息，用于关联广告数据与产品信息。

| 列名 | 说明 |
|------|------|
| ASIN | Amazon 产品编号 |
| 产品名称 | 产品名称或简称 |
| 品线 | 所属品线名称（与 Sheet 1 对应） |
| 售价 | 当前售价 |

> **提示**：如不上传广告配置文件，系统将使用默认费比目标进行分析。
                """, style={"fontSize": "14px"}),
            ], title="广告配置说明 (广告配置.xlsx)", item_id="section-adconfig"),

            # ── 6. FAQ ───────────────────────────────────────────
            dbc.AccordionItem([
                dbc.Card([
                    dbc.CardBody([
                        html.H6("Q: 上传文件有大小限制吗？", className="fw-bold text-primary"),
                        html.P("A: 建议单个文件不超过 50MB。过大的文件可能导致上传超时或分析缓慢。"),
                        html.Hr(),

                        html.H6("Q: 分析结果会保存吗？", className="fw-bold text-primary"),
                        html.P(
                            "A: 是的，每次分析结果会自动保存到云端数据库。"
                            "下次打开页面时会直接展示最近一次的分析结果，无需重新上传文件。"
                        ),
                        html.Hr(),

                        html.H6("Q: 支持哪些市场？", className="fw-bold text-primary"),
                        html.P(
                            "A: 目前主要支持 DE（德国）市场。"
                            "可通过 products_config.json 中的 market 字段切换到其他市场"
                            "（US / UK / JP / FR / IT / ES）。"
                        ),
                        html.Hr(),

                        html.H6("Q: 数据安全吗？", className="fw-bold text-primary"),
                        html.P(
                            "A: 数据存储在 Supabase 云数据库中，仅通过 API 密钥访问。"
                            "数据传输使用 HTTPS 加密。"
                        ),
                    ]),
                ], className="border-0"),
            ], title="常见问题 (FAQ)", item_id="section-faq"),

        ], active_item="section-intro", always_open=True, className="mb-4"),

    ], fluid=True, className="pb-5")


# =====================================================================
#  Page 2: 产品配置编辑器页面
# =====================================================================

def config_page():
    """返回产品配置编辑器页面布局"""
    return dbc.Container([
        html.H2("⚙️ 产品预算配置编辑器", className="text-center mt-4 mb-2"),
        html.P(
            "在线编辑产品预算配置，替代手动编辑 products_config.json 文件",
            className="text-center text-muted mb-4",
        ),

        # ── Store 组件 ────────────────────────────────────────
        dcc.Store(id="store-products-list", data=[]),
        dcc.Download(id="download-config-json"),

        # ── Section A: 全局设置 ──────────────────────────────
        dbc.Card([
            dbc.CardHeader(html.H5("🌐 全局设置", className="mb-0")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("每月天数", html_for="cfg-global-month-days"),
                        dbc.Input(
                            id="cfg-global-month-days",
                            type="number",
                            value=30,
                            min=1,
                            max=31,
                        ),
                    ], md=2),
                    dbc.Col([
                        dbc.Label(
                            "整体目标费比 (TACoS)",
                            html_for="cfg-global-tacos-target",
                        ),
                        dbc.Input(
                            id="cfg-global-tacos-target",
                            type="number",
                            value=0.037,
                            step=0.001,
                            min=0,
                            max=1,
                        ),
                        dbc.FormText("如 3.7% 填 0.037"),
                    ], md=2),
                    dbc.Col([
                        dbc.Label("焦点产品 ASIN", html_for="cfg-global-focus-asin"),
                        dbc.Input(
                            id="cfg-global-focus-asin",
                            type="text",
                            placeholder="B0XXXXXXXX",
                        ),
                    ], md=3),
                    dbc.Col([
                        dbc.Label("货币符号", html_for="cfg-global-currency"),
                        dbc.Select(
                            id="cfg-global-currency",
                            options=[
                                {"label": "€ 欧元", "value": "€"},
                                {"label": "$ 美元", "value": "$"},
                                {"label": "£ 英镑", "value": "£"},
                                {"label": "¥ 日元/人民币", "value": "¥"},
                            ],
                            value="€",
                        ),
                    ], md=2),
                    dbc.Col([
                        dbc.Label("市场代码", html_for="cfg-global-market"),
                        dbc.Select(
                            id="cfg-global-market",
                            options=[
                                {"label": "DE (德国)", "value": "DE"},
                                {"label": "US (美国)", "value": "US"},
                                {"label": "UK (英国)", "value": "UK"},
                                {"label": "JP (日本)", "value": "JP"},
                                {"label": "FR (法国)", "value": "FR"},
                                {"label": "IT (意大利)", "value": "IT"},
                                {"label": "ES (西班牙)", "value": "ES"},
                            ],
                            value="DE",
                        ),
                    ], md=3),
                ]),
            ]),
        ], className="mb-4"),

        # ── Section B: 产品列表 ──────────────────────────────
        dbc.Card([
            dbc.CardHeader(
                dbc.Row([
                    dbc.Col(html.H5("📦 产品列表", className="mb-0"), width="auto"),
                    dbc.Col(
                        dbc.Button(
                            "➕ 添加产品",
                            id="btn-add-product",
                            color="success",
                            size="sm",
                        ),
                        width="auto",
                        className="ms-auto",
                    ),
                ], align="center"),
            ),
            dbc.CardBody(
                html.Div(id="product-rows-container"),
            ),
        ], className="mb-4"),

        # ── Section C: 保存 / 导出 ──────────────────────────
        dbc.Card([
            dbc.CardHeader(html.H5("💾 保存 / 导入导出", className="mb-0")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Button(
                            "💾 保存配置到云端",
                            id="btn-save-config",
                            color="primary",
                            className="w-100",
                        ),
                    ], md=3),
                    dbc.Col([
                        dbc.Button(
                            "📥 导出 JSON",
                            id="btn-export-config",
                            color="info",
                            outline=True,
                            className="w-100",
                        ),
                    ], md=3),
                    dbc.Col([
                        dcc.Upload(
                            id="upload-import-config",
                            children=dbc.Button(
                                "📤 导入 JSON",
                                color="secondary",
                                outline=True,
                                className="w-100",
                            ),
                        ),
                    ], md=3),
                    dbc.Col([
                        html.Div(id="config-save-status", className="mt-2"),
                    ], md=3),
                ]),
            ]),
        ], className="mb-5"),

    ], fluid=True, className="pb-5")


# ── 辅助: 构建单个产品行 ─────────────────────────────────────────

def _build_product_row(idx, product):
    """为第 idx 个产品生成一组表单控件"""
    return dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    dbc.Label("产品名称", size="sm"),
                    dbc.Input(
                        id={"type": "prod-name", "index": idx},
                        type="text",
                        value=product.get("name", ""),
                        size="sm",
                    ),
                ], md=2),
                dbc.Col([
                    dbc.Label("ASIN", size="sm"),
                    dbc.Input(
                        id={"type": "prod-asin", "index": idx},
                        type="text",
                        value=product.get("asin", ""),
                        placeholder="B0XXXXXXXX",
                        size="sm",
                    ),
                ], md=2),
                dbc.Col([
                    dbc.Label("平日售价", size="sm"),
                    dbc.Input(
                        id={"type": "prod-price-normal", "index": idx},
                        type="number",
                        value=product.get("price_normal", 0),
                        step=0.01,
                        size="sm",
                    ),
                ], md=1),
                dbc.Col([
                    dbc.Label("促销售价", size="sm"),
                    dbc.Input(
                        id={"type": "prod-price-promo", "index": idx},
                        type="number",
                        value=product.get("price_promo", 0),
                        step=0.01,
                        size="sm",
                    ),
                ], md=1),
                dbc.Col([
                    dbc.Label("目标销量", size="sm"),
                    dbc.Input(
                        id={"type": "prod-target-qty", "index": idx},
                        type="number",
                        value=product.get("target_qty", 0),
                        min=0,
                        size="sm",
                    ),
                ], md=1),
                dbc.Col([
                    dbc.Label("促销天数", size="sm"),
                    dbc.Input(
                        id={"type": "prod-promo-days", "index": idx},
                        type="number",
                        value=product.get("promo_days", 0),
                        min=0,
                        max=30,
                        size="sm",
                    ),
                ], md=1),
                dbc.Col([
                    dbc.Label("目标费比", size="sm"),
                    dbc.Input(
                        id={"type": "prod-tacos", "index": idx},
                        type="number",
                        value=product.get("tacos", 0.037),
                        step=0.001,
                        min=0,
                        max=1,
                        size="sm",
                    ),
                ], md=1),
                dbc.Col([
                    dbc.Label("角色", size="sm"),
                    dbc.Select(
                        id={"type": "prod-role", "index": idx},
                        options=[
                            {"label": "核心套装", "value": "核心套装"},
                            {"label": "新品", "value": "新品"},
                            {"label": "配件", "value": "配件"},
                        ],
                        value=product.get("role", "新品"),
                    ),
                ], md=2),
                dbc.Col([
                    dbc.Label("\u00a0", size="sm"),  # spacer
                    html.Div(
                        dbc.Button(
                            "🗑️",
                            id={"type": "btn-del-product", "index": idx},
                            color="danger",
                            outline=True,
                            size="sm",
                            className="w-100",
                        ),
                    ),
                ], md=1),
            ]),
        ], className="py-2"),
    ], className="mb-2")


# =====================================================================
#  Callbacks
# =====================================================================

# ── 1. 添加产品行 ────────────────────────────────────────────────

@callback(
    Output("store-products-list", "data", allow_duplicate=True),
    Input("btn-add-product", "n_clicks"),
    State("store-products-list", "data"),
    prevent_initial_call=True,
)
def toggle_add_product(n_clicks, products):
    """添加一条空产品记录到 Store"""
    if not n_clicks:
        return products or []
    products = products or []
    products.append(dict(_EMPTY_PRODUCT))
    return products


# ── 2. 删除产品行 ────────────────────────────────────────────────

@callback(
    Output("store-products-list", "data", allow_duplicate=True),
    Input({"type": "btn-del-product", "index": dash.ALL}, "n_clicks"),
    State("store-products-list", "data"),
    prevent_initial_call=True,
)
def delete_product(n_clicks_list, products):
    """删除被点击的产品行"""
    if not products:
        return []
    # 找出哪个按钮被点击
    ctx = dash.callback_context
    if not ctx.triggered:
        return products
    triggered_id = ctx.triggered[0]["prop_id"]
    # triggered_id 形如 '{"index":2,"type":"btn-del-product"}.n_clicks'
    try:
        btn_info = json.loads(triggered_id.split(".")[0])
        idx = btn_info["index"]
    except (json.JSONDecodeError, KeyError):
        return products
    # 确保索引有效
    if 0 <= idx < len(products):
        products.pop(idx)
    return products


# ── 3. 渲染产品行（Store -> UI）──────────────────────────────────

@callback(
    Output("product-rows-container", "children"),
    Input("store-products-list", "data"),
)
def render_product_rows(products):
    """根据 Store 数据渲染产品行"""
    if not products:
        return dbc.Alert(
            "暂无产品。点击「添加产品」按钮新增。",
            color="light",
            className="text-center text-muted",
        )
    rows = []
    for i, prod in enumerate(products):
        rows.append(_build_product_row(i, prod))
    return rows


# ── 4. 导入 JSON ─────────────────────────────────────────────────

@callback(
    Output("store-products-list", "data", allow_duplicate=True),
    Output("cfg-global-month-days", "value"),
    Output("cfg-global-tacos-target", "value"),
    Output("cfg-global-focus-asin", "value"),
    Output("cfg-global-currency", "value"),
    Output("cfg-global-market", "value"),
    Output("config-save-status", "children", allow_duplicate=True),
    Input("upload-import-config", "contents"),
    State("upload-import-config", "filename"),
    prevent_initial_call=True,
)
def import_config(contents, filename):
    """从上传的 JSON 文件加载配置到表单"""
    if not contents:
        raise dash.exceptions.PreventUpdate

    try:
        _, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)
        cfg = json.loads(decoded.decode("utf-8"))
    except Exception as e:
        return (
            dash.no_update, dash.no_update, dash.no_update,
            dash.no_update, dash.no_update, dash.no_update,
            dbc.Alert(f"导入失败: {e}", color="danger", duration=5000),
        )

    products = cfg.get("products", [])
    month_days = cfg.get("month_days", _DEFAULT_GLOBAL["month_days"])
    tacos_target = cfg.get("tacos_target", _DEFAULT_GLOBAL["tacos_target"])
    focus_asin = cfg.get("focus_asin", _DEFAULT_GLOBAL["focus_asin"])
    currency = cfg.get("currency", _DEFAULT_GLOBAL["currency"])
    market = cfg.get("market", _DEFAULT_GLOBAL["market"])

    return (
        products,
        month_days,
        tacos_target,
        focus_asin,
        currency,
        market,
        dbc.Alert(
            f"✅ 已导入 {filename}，共 {len(products)} 个产品",
            color="success",
            duration=4000,
        ),
    )


# ── 5. 导出 JSON ─────────────────────────────────────────────────

@callback(
    Output("download-config-json", "data"),
    Input("btn-export-config", "n_clicks"),
    State("store-products-list", "data"),
    State("cfg-global-month-days", "value"),
    State("cfg-global-tacos-target", "value"),
    State("cfg-global-focus-asin", "value"),
    State("cfg-global-currency", "value"),
    State("cfg-global-market", "value"),
    prevent_initial_call=True,
)
def export_config(n_clicks, products, month_days, tacos_target,
                  focus_asin, currency, market):
    """将当前表单数据导出为 products_config.json"""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    cfg = {
        "month_days": month_days or 30,
        "tacos_target": tacos_target or 0.037,
        "focus_asin": focus_asin or "",
        "currency": currency or "€",
        "market": market or "DE",
        "products": products or [],
    }
    content = json.dumps(cfg, ensure_ascii=False, indent=2)
    return dict(content=content, filename="products_config.json")


# ── 6. 保存到 Supabase ───────────────────────────────────────────

@callback(
    Output("config-save-status", "children"),
    Input("btn-save-config", "n_clicks"),
    State("store-products-list", "data"),
    State("cfg-global-month-days", "value"),
    State("cfg-global-tacos-target", "value"),
    State("cfg-global-focus-asin", "value"),
    State("cfg-global-currency", "value"),
    State("cfg-global-market", "value"),
    prevent_initial_call=True,
)
def save_config(n_clicks, products, month_days, tacos_target,
                focus_asin, currency, market):
    """保存配置到 Supabase analysis_results 表"""
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    cfg = {
        "month_days": month_days or 30,
        "tacos_target": tacos_target or 0.037,
        "focus_asin": focus_asin or "",
        "currency": currency or "€",
        "market": market or "DE",
        "products": products or [],
    }

    try:
        sb = _get_sb()
        now = datetime.utcnow().isoformat()
        sb.table("analysis_results").insert({
            "mode": "config",
            "tab_name": "products_config",
            "data": cfg,
            "created_at": now,
        }).execute()
        return dbc.Alert(
            "✅ 配置已保存到云端",
            color="success",
            duration=4000,
        )
    except Exception as e:
        return dbc.Alert(
            f"❌ 保存失败: {e}",
            color="danger",
            duration=6000,
        )
