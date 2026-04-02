"""
Amazon 广告关键词分析看板
Dash Web Application — 固定 URL 版本
"""
import os
import io
import json
import base64
import traceback

import dash
from dash import html, dcc, dash_table, Input, Output, State, callback, ctx
import dash_bootstrap_components as dbc
import pandas as pd

# ── 初始化 Dash App ───────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    suppress_callback_exceptions=True,
    title="Amazon 广告关键词分析看板",
    update_title="分析中...",
)
server = app.server  # for gunicorn

# ── 颜色/样式常量 ────────────────────────────────────────────────
STATUS_COLORS = {
    "超费比": "#FFCCCC",
    "达标": "#D4EDDA",
    "未消耗": "#E0E0E0",
    "欠投放": "#CCE5FF",
    "高": "#FFCCCC",
    "中": "#FFF3CD",
    "低": "#D4EDDA",
    "优质": "#D4EDDA",
    "合理": "#CCE5FF",
    "偏高": "#FFF3CD",
    "高风险": "#FFCCCC",
}


def _style_by_col(df, col, keywords_colors):
    """为 DataTable 生成条件样式"""
    styles = []
    for kw, color in keywords_colors.items():
        styles.append({
            "if": {
                "filter_query": f'{{{col}}} contains "{kw}"',
                "column_id": col,
            },
            "backgroundColor": color,
            "fontWeight": "bold" if "超" in kw or "高" in kw else "normal",
        })
    return styles


def _make_table(df, table_id, page_size=25):
    """创建标准 DataTable"""
    if df is None or df.empty:
        return html.P("暂无数据", className="text-muted text-center my-4")
    return dash_table.DataTable(
        id=table_id,
        columns=[{"name": c, "id": c} for c in df.columns],
        data=df.to_dict("records"),
        page_size=page_size,
        sort_action="native",
        filter_action="native",
        export_format="xlsx",
        style_table={"overflowX": "auto"},
        style_header={
            "backgroundColor": "#2C3E50",
            "color": "white",
            "fontWeight": "bold",
            "textAlign": "center",
            "fontSize": "13px",
        },
        style_cell={
            "textAlign": "left",
            "padding": "8px 12px",
            "fontSize": "13px",
            "maxWidth": "300px",
            "overflow": "hidden",
            "textOverflow": "ellipsis",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#F8F9FA"},
        ],
    )


# ── 页面布局 ──────────────────────────────────────────────────────

# 导航栏
navbar = dbc.NavbarSimple(
    brand="📊 Amazon 广告关键词分析看板",
    brand_style={"fontSize": "1.2rem", "fontWeight": "bold"},
    color="dark",
    dark=True,
    className="mb-3",
    children=[
        dbc.NavItem(dbc.NavLink("模式1: 关键词分析", href="/mode1", id="nav-mode1")),
        dbc.NavItem(dbc.NavLink("模式2: 广告监控", href="/mode2", id="nav-mode2")),
    ],
)

# 模式2上传区
mode2_upload = dbc.Card([
    dbc.CardHeader(html.H5("📤 上传领星广告报告", className="mb-0")),
    dbc.CardBody([
        dbc.Row([
            dbc.Col([
                html.Label("广告活动报告 *", className="fw-bold"),
                dcc.Upload(
                    id="upload-campaign",
                    children=html.Div(["拖拽或 ", html.A("点击上传", className="text-primary")]),
                    style={
                        "borderWidth": "2px", "borderStyle": "dashed",
                        "borderColor": "#ccc", "borderRadius": "8px",
                        "padding": "20px", "textAlign": "center",
                        "cursor": "pointer", "backgroundColor": "#fafafa",
                    },
                ),
                html.Div(id="campaign-filename", className="text-success mt-1 small"),
            ], md=3),
            dbc.Col([
                html.Label("推广商品报告 *", className="fw-bold"),
                dcc.Upload(
                    id="upload-product",
                    children=html.Div(["拖拽或 ", html.A("点击上传", className="text-primary")]),
                    style={
                        "borderWidth": "2px", "borderStyle": "dashed",
                        "borderColor": "#ccc", "borderRadius": "8px",
                        "padding": "20px", "textAlign": "center",
                        "cursor": "pointer", "backgroundColor": "#fafafa",
                    },
                ),
                html.Div(id="product-filename", className="text-success mt-1 small"),
            ], md=3),
            dbc.Col([
                html.Label("搜索词报告（可选）", className="fw-bold"),
                dcc.Upload(
                    id="upload-searchterm",
                    children=html.Div(["拖拽或 ", html.A("点击上传", className="text-primary")]),
                    style={
                        "borderWidth": "2px", "borderStyle": "dashed",
                        "borderColor": "#ccc", "borderRadius": "8px",
                        "padding": "20px", "textAlign": "center",
                        "cursor": "pointer", "backgroundColor": "#fafafa",
                    },
                ),
                html.Div(id="searchterm-filename", className="text-success mt-1 small"),
            ], md=3),
            dbc.Col([
                html.Label("关键词报告（可选）", className="fw-bold"),
                dcc.Upload(
                    id="upload-keyword",
                    children=html.Div(["拖拽或 ", html.A("点击上传", className="text-primary")]),
                    style={
                        "borderWidth": "2px", "borderStyle": "dashed",
                        "borderColor": "#ccc", "borderRadius": "8px",
                        "padding": "20px", "textAlign": "center",
                        "cursor": "pointer", "backgroundColor": "#fafafa",
                    },
                ),
                html.Div(id="keyword-filename", className="text-success mt-1 small"),
            ], md=3),
        ]),
        html.Hr(),
        dbc.Row([
            dbc.Col([
                html.Label("广告配置文件（可选）"),
                dcc.Upload(
                    id="upload-adconfig",
                    children=html.Div(["上传 广告配置.xlsx"]),
                    style={
                        "borderWidth": "1px", "borderStyle": "dashed",
                        "borderColor": "#ccc", "borderRadius": "6px",
                        "padding": "10px", "textAlign": "center",
                        "cursor": "pointer", "fontSize": "12px",
                    },
                ),
            ], md=3),
            dbc.Col([
                dbc.Button(
                    "🚀 开始分析", id="btn-analyze-mode2",
                    color="primary", size="lg", className="mt-3",
                    style={"width": "100%"},
                ),
            ], md=3),
            dbc.Col([
                dbc.Spinner(
                    html.Div(id="mode2-status", className="mt-3"),
                    color="primary", size="sm",
                ),
            ], md=6),
        ]),
    ]),
], className="mb-3")

# 模式1上传区
mode1_upload = dbc.Card([
    dbc.CardHeader(html.H5("📤 上传西柚关键词数据", className="mb-0")),
    dbc.CardBody([
        dbc.Row([
            dbc.Col([
                html.Label("本品关键词文件 *", className="fw-bold"),
                dcc.Upload(
                    id="upload-own",
                    children=html.Div(["拖拽或 ", html.A("点击上传", className="text-primary")]),
                    style={
                        "borderWidth": "2px", "borderStyle": "dashed",
                        "borderColor": "#ccc", "borderRadius": "8px",
                        "padding": "20px", "textAlign": "center",
                        "cursor": "pointer", "backgroundColor": "#fafafa",
                    },
                ),
                html.Div(id="own-filename", className="text-success mt-1 small"),
            ], md=4),
            dbc.Col([
                html.Label("竞品关键词文件 *（可多选）", className="fw-bold"),
                dcc.Upload(
                    id="upload-comp",
                    children=html.Div(["拖拽或 ", html.A("点击上传", className="text-primary")]),
                    style={
                        "borderWidth": "2px", "borderStyle": "dashed",
                        "borderColor": "#ccc", "borderRadius": "8px",
                        "padding": "20px", "textAlign": "center",
                        "cursor": "pointer", "backgroundColor": "#fafafa",
                    },
                    multiple=True,
                ),
                html.Div(id="comp-filename", className="text-success mt-1 small"),
            ], md=4),
            dbc.Col([
                html.Label("产品配置 JSON（可选）"),
                dcc.Upload(
                    id="upload-config-json",
                    children=html.Div(["上传 products_config.json"]),
                    style={
                        "borderWidth": "1px", "borderStyle": "dashed",
                        "borderColor": "#ccc", "borderRadius": "6px",
                        "padding": "10px", "textAlign": "center",
                        "cursor": "pointer", "fontSize": "12px",
                    },
                ),
            ], md=2),
            dbc.Col([
                dbc.Button(
                    "🚀 开始分析", id="btn-analyze-mode1",
                    color="success", size="lg", className="mt-3",
                    style={"width": "100%"},
                ),
            ], md=2),
        ]),
        dbc.Spinner(
            html.Div(id="mode1-status", className="mt-2"),
            color="success", size="sm",
        ),
    ]),
], className="mb-3")

# 模式2结果区
mode2_results = html.Div(id="mode2-results")

# 模式1结果区
mode1_results = html.Div(id="mode1-results")

# 首页
home_page = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("欢迎使用 Amazon 广告关键词分析看板", className="text-center mt-5"),
            html.P("请从顶部导航选择分析模式", className="text-center text-muted"),
            html.Hr(),
            dbc.Row([
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.H4("📊 模式1: 关键词竞争分析", className="card-title"),
                        html.P("上传西柚找词数据，对比本品与竞品关键词差距"),
                        html.Ul([
                            html.Li("竞品独有词发现"),
                            html.Li("排名差距分析"),
                            html.Li("流量获取对比"),
                            html.Li("预算规划建议"),
                        ]),
                        dbc.Button("进入模式1", href="/mode1", color="success", outline=True),
                    ]),
                ]), md=6),
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.H4("📈 模式2: 广告监控看板", className="card-title"),
                        html.P("上传领星广告报告，生成费比监控看板"),
                        html.Ul([
                            html.Li("费比总览 (ACoS)"),
                            html.Li("Campaign 明细"),
                            html.Li("关键词 ROI 分析"),
                            html.Li("建议否定词"),
                        ]),
                        dbc.Button("进入模式2", href="/mode2", color="primary", outline=True),
                    ]),
                ]), md=6),
            ]),
        ], md=10, className="mx-auto"),
    ]),
], fluid=True)


# ── 主布局 ────────────────────────────────────────────────────────
app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    dcc.Store(id="store-mode2-data", storage_type="memory"),
    dcc.Store(id="store-mode1-data", storage_type="memory"),
    navbar,
    dbc.Container(id="page-content", fluid=True),
])


# ── 路由 ──────────────────────────────────────────────────────────
@callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    if pathname == "/mode2":
        return html.Div([mode2_upload, mode2_results])
    elif pathname == "/mode1":
        return html.Div([mode1_upload, mode1_results])
    return home_page


# ── 文件名回显 ────────────────────────────────────────────────────
def _get_filename(contents, filename):
    if contents:
        return f"✅ {filename}"
    return ""

for upload_id, display_id in [
    ("upload-campaign", "campaign-filename"),
    ("upload-product", "product-filename"),
    ("upload-searchterm", "searchterm-filename"),
    ("upload-keyword", "keyword-filename"),
    ("upload-own", "own-filename"),
]:
    @callback(Output(display_id, "children"),
              Input(upload_id, "filename"))
    def _show_name(fn, _did=display_id):
        return f"✅ {fn}" if fn else ""

@callback(Output("comp-filename", "children"),
          Input("upload-comp", "filename"))
def _show_comp_names(fns):
    if not fns:
        return ""
    if isinstance(fns, str):
        return f"✅ {fns}"
    return "✅ " + ", ".join(fns)


# ── 辅助: 解码上传文件 ───────────────────────────────────────────
def _decode_upload(contents):
    """将 Dash upload 的 base64 内容解码为 BytesIO"""
    if not contents:
        return None
    _, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    return io.BytesIO(decoded)


# ── 模式2: 分析回调 ──────────────────────────────────────────────
@callback(
    Output("mode2-results", "children"),
    Output("mode2-status", "children"),
    Input("btn-analyze-mode2", "n_clicks"),
    State("upload-campaign", "contents"),
    State("upload-product", "contents"),
    State("upload-searchterm", "contents"),
    State("upload-keyword", "contents"),
    State("upload-adconfig", "contents"),
    prevent_initial_call=True,
)
def run_mode2(n_clicks, campaign_c, product_c, st_c, kw_c, cfg_c):
    if not campaign_c or not product_c:
        return dash.no_update, dbc.Alert("请上传广告活动报告和推广商品报告", color="warning")

    try:
        from core.ad_monitor_core import load_and_process

        campaign_io = _decode_upload(campaign_c)
        product_io = _decode_upload(product_c)
        st_io = _decode_upload(st_c)
        kw_io = _decode_upload(kw_c)
        cfg_io = _decode_upload(cfg_c)

        results = load_and_process(
            campaign_file=campaign_io,
            product_file=product_io,
            search_term_file=st_io,
            keyword_file=kw_io,
            config_file=cfg_io,
        )

        tabs = []
        tab_data = [
            ("费比总览", results.get("overview")),
            ("Campaign明细", results.get("campaigns")),
            ("关键词ROI分析", results.get("keywords")),
            ("建议否定词", results.get("negatives")),
        ]

        for i, (label, df) in enumerate(tab_data):
            if df is not None and not df.empty:
                # 数值列格式化
                for col in df.columns:
                    if "ACoS" in col or "CVR" in col or "费比" in col:
                        df[col] = df[col].apply(
                            lambda x: f"{x:.1%}" if isinstance(x, (int, float)) and x < 10 else x
                        )
                    elif "€" in col or "花费" in col or "销售额" in col or "收入" in col or "出价" in col:
                        df[col] = df[col].apply(
                            lambda x: f"€{x:,.2f}" if isinstance(x, (int, float)) else x
                        )

                # 条件样式
                style_cond = [
                    {"if": {"row_index": "odd"}, "backgroundColor": "#F8F9FA"},
                ]
                for col in df.columns:
                    if "状态" in col or "动作" in col or "原因" in col:
                        for kw, color in STATUS_COLORS.items():
                            style_cond.append({
                                "if": {
                                    "filter_query": f'{{{col}}} contains "{kw}"',
                                    "column_id": col,
                                },
                                "backgroundColor": color,
                            })

                count_info = f"({len(df)} 条)"
                tabs.append(dbc.Tab(
                    label=f"{label} {count_info}",
                    children=[
                        dash_table.DataTable(
                            columns=[{"name": c, "id": c} for c in df.columns],
                            data=df.to_dict("records"),
                            page_size=30,
                            sort_action="native",
                            filter_action="native",
                            export_format="xlsx",
                            style_table={"overflowX": "auto"},
                            style_header={
                                "backgroundColor": "#2C3E50",
                                "color": "white",
                                "fontWeight": "bold",
                                "textAlign": "center",
                                "fontSize": "13px",
                            },
                            style_cell={
                                "textAlign": "left",
                                "padding": "8px 12px",
                                "fontSize": "13px",
                                "maxWidth": "300px",
                                "overflow": "hidden",
                                "textOverflow": "ellipsis",
                            },
                            style_data_conditional=style_cond,
                        ),
                    ],
                ))
            else:
                tabs.append(dbc.Tab(
                    label=label,
                    children=[html.P("无数据（未上传对应报告）", className="text-muted p-4")],
                ))

        result_card = dbc.Card([
            dbc.CardHeader(html.H5("📈 广告监控分析结果", className="mb-0")),
            dbc.CardBody(dbc.Tabs(tabs)),
        ])

        return result_card, dbc.Alert("✅ 分析完成!", color="success", duration=3000)

    except Exception as e:
        tb = traceback.format_exc()
        return dash.no_update, dbc.Alert(
            [html.Strong("分析失败: "), html.Br(), str(e), html.Pre(tb, style={"fontSize": "11px"})],
            color="danger",
        )


# ── 模式1: 分析回调 ──────────────────────────────────────────────
@callback(
    Output("mode1-results", "children"),
    Output("mode1-status", "children"),
    Input("btn-analyze-mode1", "n_clicks"),
    State("upload-own", "contents"),
    State("upload-comp", "contents"),
    State("upload-config-json", "contents"),
    prevent_initial_call=True,
)
def run_mode1(n_clicks, own_c, comp_c, cfg_c):
    if not own_c or not comp_c:
        return dash.no_update, dbc.Alert("请上传本品和竞品关键词文件", color="warning")

    try:
        from core.ad_pipeline_core import load_and_analyze

        own_io = _decode_upload(own_c)

        # comp 可能是单个或多个文件
        if isinstance(comp_c, str):
            comp_ios = [_decode_upload(comp_c)]
        else:
            comp_ios = [_decode_upload(c) for c in comp_c]

        config_json = None
        if cfg_c:
            cfg_io = _decode_upload(cfg_c)
            config_json = json.load(cfg_io)

        results = load_and_analyze(
            own_file=own_io,
            comp_files=comp_ios,
            config_json=config_json,
        )

        tabs = []
        tab_order = [
            "执行摘要", "竞品独有词(需布局)", "排名差距词(需提升)",
            "流量获取差距词", "本品高潜力词", "词根聚类Campaign规划",
            "综合关键词优先投放", "产品预算规划", "流量来源结构对比",
            "月度广告执行计划",
        ]

        for label in tab_order:
            df = results.get(label)
            if df is not None and not df.empty:
                # 数值格式化
                for col in df.columns:
                    if any(k in col for k in ["CPC", "出价", "价格", "预算", "花费", "收入"]):
                        df[col] = df[col].apply(
                            lambda x: f"€{x:,.2f}" if isinstance(x, (int, float)) else x
                        )
                    elif any(k in col for k in ["CVR", "ACoS", "TACoS", "占比"]):
                        df[col] = df[col].apply(
                            lambda x: f"{x:.1%}" if isinstance(x, (int, float)) and 0 < x < 10 else x
                        )

                count_info = f"({len(df)})" if len(df) > 1 else ""
                short_label = label[:6] + "..." if len(label) > 8 else label

                tabs.append(dbc.Tab(
                    label=f"{short_label} {count_info}",
                    children=[
                        html.H6(label, className="mt-2 mb-2"),
                        dash_table.DataTable(
                            columns=[{"name": c, "id": c} for c in df.columns],
                            data=df.to_dict("records"),
                            page_size=30,
                            sort_action="native",
                            filter_action="native",
                            export_format="xlsx",
                            style_table={"overflowX": "auto"},
                            style_header={
                                "backgroundColor": "#27AE60",
                                "color": "white",
                                "fontWeight": "bold",
                                "textAlign": "center",
                                "fontSize": "13px",
                            },
                            style_cell={
                                "textAlign": "left",
                                "padding": "8px 12px",
                                "fontSize": "13px",
                                "maxWidth": "300px",
                                "overflow": "hidden",
                                "textOverflow": "ellipsis",
                            },
                            style_data_conditional=[
                                {"if": {"row_index": "odd"}, "backgroundColor": "#F8F9FA"},
                            ],
                        ),
                    ],
                ))

        result_card = dbc.Card([
            dbc.CardHeader(html.H5("📊 关键词竞争分析结果", className="mb-0")),
            dbc.CardBody(dbc.Tabs(tabs) if tabs else html.P("分析完成但无数据")),
        ])

        return result_card, dbc.Alert("✅ 分析完成!", color="success", duration=3000)

    except Exception as e:
        tb = traceback.format_exc()
        return dash.no_update, dbc.Alert(
            [html.Strong("分析失败: "), html.Br(), str(e), html.Pre(tb, style={"fontSize": "11px"})],
            color="danger",
        )


# ── 入口 ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
