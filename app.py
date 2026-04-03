"""
Amazon 广告关键词分析看板
Dash Web Application — Supabase 集成版本
"""
import os
import io
import json
import base64
import traceback
from datetime import datetime

import dash
from dash import html, dcc, dash_table, Input, Output, State, callback, ctx
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

# ── Supabase 配置 ─────────────────────────────────────────────────
SUPABASE_URL = os.environ.get(
    "SUPABASE_URL", "https://yolbjbfcwducxgqvwxmr.supabase.co"
)
SUPABASE_KEY = os.environ.get(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlvbGJqYmZjd2R1Y3hncXZ3eG1yIiwi"
    "cm9sZSI6ImFub24iLCJpYXQiOjE3NzUwODE1MzQsImV4cCI6MjA5MDY1NzUzNH0."
    "7c5M3yGkMBfF-mglXc5CMquzL8Jr0NMuwqKOTtrqG2M",
)

_supabase_client = None


def _get_supabase():
    """返回 Supabase 客户端（懒初始化）"""
    global _supabase_client
    if _supabase_client is None:
        from supabase import create_client

        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase_client


def _load_latest(mode):
    """
    从 Supabase 加载某个 mode 的最新分析结果。
    按 tab_name 分组, 取每组最新一条。
    返回 dict {tab_name: DataFrame}
    """
    try:
        sb = _get_supabase()
        resp = (
            sb.table("analysis_results")
            .select("tab_name, data, created_at")
            .eq("mode", mode)
            .order("created_at", desc=True)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            return {}

        # 按 tab_name 分组, 取最新一条
        seen = {}
        for row in rows:
            tab = row["tab_name"]
            if tab not in seen:
                seen[tab] = row["data"]

        result = {}
        for tab_name, data in seen.items():
            if data:
                df = pd.DataFrame(data)
                if not df.empty:
                    result[tab_name] = df
        return result
    except Exception as e:
        print(f"[Supabase] _load_latest({mode}) 失败: {e}")
        return None


def _load_history_list(mode):
    """
    加载某个 mode 的历史记录列表（按时间倒序，去重）。
    返回 [{label: "2026-03-20 18:22", value: "2026-03-20T18:22:00"}]
    """
    try:
        sb = _get_supabase()
        resp = (
            sb.table("analysis_results")
            .select("created_at")
            .eq("mode", mode)
            .order("created_at", desc=True)
            .execute()
        )
        rows = resp.data or []
        seen = set()
        options = []
        for row in rows:
            ts = row["created_at"]
            # 取到分钟级别去重
            short = ts[:16].replace("T", " ")
            if short not in seen:
                seen.add(short)
                options.append({"label": short, "value": ts})
        return options
    except Exception:
        return []


def _load_by_timestamp(mode, timestamp):
    """加载指定时间戳的分析结果"""
    try:
        sb = _get_supabase()
        resp = (
            sb.table("analysis_results")
            .select("tab_name, data")
            .eq("mode", mode)
            .eq("created_at", timestamp)
            .execute()
        )
        rows = resp.data or []
        result = {}
        for row in rows:
            if row["data"]:
                df = pd.DataFrame(row["data"])
                if not df.empty:
                    result[row["tab_name"]] = df
        return result
    except Exception:
        return {}


def _save_results(mode, results_dict):
    """
    保存分析结果到 Supabase。
    对每个 (tab_name, df) 对: insert {mode, tab_name, data: df.to_dict("records")}
    """
    try:
        sb = _get_supabase()
        now = datetime.utcnow().isoformat()
        rows = []
        for tab_name, df in results_dict.items():
            if df is not None and not df.empty:
                rows.append({
                    "mode": mode,
                    "tab_name": tab_name,
                    "data": df.to_dict("records"),
                    "created_at": now,
                })
        if rows:
            sb.table("analysis_results").insert(rows).execute()
    except Exception as e:
        print(f"[Supabase] _save_results({mode}) 失败: {e}")


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


# ── 共享渲染函数 ──────────────────────────────────────────────────


def _format_df_columns(df, mode="mode2"):
    """
    对 DataFrame 列进行数值格式化（就地修改并返回）。
    mode2: 广告监控; mode1: 关键词分析
    """
    df = df.copy()
    for col in df.columns:
        if any(k in col for k in ["ACoS", "CVR", "费比"]):
            df[col] = df[col].apply(
                lambda x: f"{x:.1%}" if isinstance(x, (int, float)) and x < 10 else x
            )
        elif any(k in col for k in ["€", "花费", "销售额", "收入", "出价"]):
            df[col] = df[col].apply(
                lambda x: f"€{x:,.2f}" if isinstance(x, (int, float)) else x
            )
        if mode == "mode1":
            if any(k in col for k in ["CPC", "价格", "预算"]):
                df[col] = df[col].apply(
                    lambda x: f"€{x:,.2f}" if isinstance(x, (int, float)) else x
                )
            elif any(k in col for k in ["TACoS", "占比"]):
                df[col] = df[col].apply(
                    lambda x: f"{x:.1%}"
                    if isinstance(x, (int, float)) and 0 < x < 10
                    else x
                )
    return df


def _build_style_conditional(df, header_color="#2C3E50"):
    """为 DataTable 构建 style_data_conditional"""
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
    return style_cond


def _render_datatable(df, header_color="#2C3E50", page_size=30, freeze_cols=0):
    """渲染单个 DataTable 组件，支持固定表头和冻结左侧列"""
    if df is None or df.empty:
        return html.P("暂无数据", className="text-muted text-center my-4")

    columns = [{"name": c, "id": c} for c in df.columns]

    fixed_cols = {}
    if freeze_cols > 0:
        fixed_cols = {"headers": True, "data": freeze_cols}

    return dash_table.DataTable(
        columns=columns,
        data=df.to_dict("records"),
        page_size=page_size,
        sort_action="native",
        filter_action="native",
        export_format="xlsx",
        fixed_rows={"headers": True},
        fixed_columns=fixed_cols if fixed_cols else {},
        style_table={
            "overflowX": "auto",
            "overflowY": "auto",
            "maxHeight": "600px",
            "minWidth": "100%",
        },
        style_header={
            "backgroundColor": header_color,
            "color": "white",
            "fontWeight": "bold",
            "textAlign": "center",
            "fontSize": "13px",
            "position": "sticky",
            "top": 0,
            "zIndex": 1,
        },
        style_cell={
            "textAlign": "left",
            "padding": "8px 12px",
            "fontSize": "13px",
            "minWidth": "100px",
            "maxWidth": "300px",
            "overflow": "hidden",
            "textOverflow": "ellipsis",
        },
        style_data_conditional=_build_style_conditional(df),
    )


def _build_mode2_tabs(results_dict, full_results=None):
    """
    从 results_dict {tab_name: DataFrame} 构建模式2 Tab 列表。
    tab_order 固定为 4 个标签页。
    """
    tabs = []

    # Dashboard tab (first!)
    if full_results:
        dashboard_content = _build_dashboard_section(full_results)
        tabs.append(dbc.Tab(
            label="📊 总览仪表盘",
            children=[dashboard_content],
        ))

    tab_order = ["费比总览", "Campaign明细", "关键词ROI分析", "建议否定词"]
    for label in tab_order:
        df = results_dict.get(label)
        if df is not None and not df.empty:
            df = _format_df_columns(df, mode="mode2")
            count_info = f"({len(df)} 条)"
            tabs.append(dbc.Tab(
                label=f"{label} {count_info}",
                children=[_render_datatable(df, header_color="#2C3E50")],
            ))
        else:
            tabs.append(dbc.Tab(
                label=label,
                children=[html.P("无数据", className="text-muted p-4")],
            ))
    return tabs


def _build_dashboard_section(results):
    """Build the comprehensive dashboard overview section."""
    dashboard = results.get("dashboard", {})
    priority_df = results.get("priority")
    pl_df = results.get("pl_suggestions")

    if not dashboard:
        return html.Div()

    sections = []

    # ── Section 1: KPI Cards ──
    metrics = dashboard.get("metrics", {})
    total_spend = metrics.get("total_spend", 0)
    total_sales = metrics.get("total_sales", 0)
    overall_acos = metrics.get("overall_acos", 0)
    total_orders = metrics.get("total_orders", 0)
    target_acos = metrics.get("target_acos", 0.04)
    acos_ok = overall_acos <= target_acos if overall_acos > 0 else True

    kpi_cards = dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("总花费", className="text-muted"),
                html.H3(f"€{total_spend:,.0f}", style={"color": "#2C3E50"}),
            ])
        ], className="text-center"), md=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("广告销售额", className="text-muted"),
                html.H3(f"€{total_sales:,.0f}", style={"color": "#27ae60"}),
            ])
        ], className="text-center"), md=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("综合ACoS", className="text-muted"),
                html.H3(f"{overall_acos*100:.1f}%",
                        style={"color": "#27ae60" if acos_ok else "#e74c3c"}),
                html.Small(f"目标: {target_acos*100:.1f}% {'✅' if acos_ok else '❌'}")
            ])
        ], className="text-center"), md=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("总订单", className="text-muted"),
                html.H3(f"{total_orders:,}", style={"color": "#2C3E50"}),
            ])
        ], className="text-center"), md=3),
    ], className="mb-3")
    sections.append(kpi_cards)

    # ── Section 2: Budget & Sales Progress (if budget_config provided) ──
    budget = dashboard.get("budget", {})
    prediction = dashboard.get("prediction", {})

    if budget.get("monthly_budget"):
        budget_pct = budget.get("budget_spent_pct", 0)
        sales_pct = budget.get("sales_completed_pct", 0)

        progress_section = dbc.Card([
            dbc.CardHeader(html.H5("📊 月度目标进度", className="mb-0")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.H6(f"广告预算: €{budget.get('monthly_budget',0):,.0f}"),
                        dbc.Progress(
                            value=min(budget_pct * 100, 100),
                            label=f"{budget_pct*100:.1f}%",
                            color="danger" if budget_pct > 0.9 else ("warning" if budget_pct > 0.7 else "success"),
                            className="mb-2", style={"height": "25px"}
                        ),
                        html.Small(f"已花费 €{total_spend:,.0f} | 剩余 €{budget.get('budget_remaining',0):,.0f}"),
                    ], md=6),
                    dbc.Col([
                        html.H6(f"销量目标: €{budget.get('monthly_sales_target',0):,.0f}"),
                        dbc.Progress(
                            value=min(sales_pct * 100, 100),
                            label=f"{sales_pct*100:.1f}%",
                            color="success" if sales_pct > 0.8 else ("warning" if sales_pct > 0.5 else "danger"),
                            className="mb-2", style={"height": "25px"}
                        ),
                        html.Small(f"已完成 €{total_sales:,.0f} | 缺口 €{budget.get('sales_remaining',0):,.0f}"),
                    ], md=6),
                ]),
                # Prediction section
                html.Hr(),
                dbc.Row([
                    dbc.Col([
                        html.H6("📈 月底预测"),
                        html.P([
                            f"按当前速度，预计月底花费 ",
                            html.B(f"€{prediction.get('predicted_month_end_spend', 0):,.0f}"),
                            " | 预计销售额 ",
                            html.B(f"€{prediction.get('predicted_month_end_sales', 0):,.0f}"),
                        ]),
                        html.P([
                            "预算状态: ",
                            html.Span("⚠️ 将超预算" if prediction.get("will_overspend") else "✅ 预算充足",
                                     style={"color": "#e74c3c" if prediction.get("will_overspend") else "#27ae60", "fontWeight":"bold"}),
                            " | 销量状态: ",
                            html.Span("✅ 预计达标" if prediction.get("will_meet_target") else "❌ 预计未达标",
                                     style={"color": "#27ae60" if prediction.get("will_meet_target") else "#e74c3c", "fontWeight":"bold"}),
                        ]),
                    ], md=6),
                    dbc.Col([
                        html.H6("🎯 每日建议目标"),
                        html.P(f"建议每日花费: €{budget.get('daily_budget_needed', 0):,.0f}"),
                        html.P(f"建议每日销售: €{budget.get('daily_sales_needed', 0):,.0f}"),
                    ], md=6),
                ]),
            ]),
        ], className="mb-3")
        sections.append(progress_section)

    # ── Section 3: Alerts ──
    alerts = dashboard.get("alerts", [])
    if alerts:
        alert_items = []
        for a in alerts[:10]:
            alert_items.append(html.Li([
                html.B(f"{a.get('品线','')} — {a.get('ASIN','')}"),
                f"  ACoS {a.get('acos',0)*100:.1f}%，花费 €{a.get('spend',0):,.0f}，销售额 €{a.get('sales',0):,.0f}",
            ]))
        sections.append(dbc.Alert([
            html.H5("🚨 异常警报：高ACoS产品（>15%）", className="alert-heading"),
            html.Ul(alert_items),
        ], color="danger", className="mb-3"))

    # ── Section 4: Priority Classification ──
    if priority_df is not None and not priority_df.empty:
        # Summary counts
        pri_counts = priority_df["优先级"].value_counts().to_dict()

        pri_summary = dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([
                html.H4(pri_counts.get("🔴 紧急处理", 0), style={"color":"#e74c3c"}),
                html.Small("紧急处理")
            ]), className="text-center border-danger"), md=3),
            dbc.Col(dbc.Card(dbc.CardBody([
                html.H4(pri_counts.get("🟡 观察调整", 0), style={"color":"#f39c12"}),
                html.Small("观察调整")
            ]), className="text-center border-warning"), md=3),
            dbc.Col(dbc.Card(dbc.CardBody([
                html.H4(pri_counts.get("🟢 增加投入", 0), style={"color":"#27ae60"}),
                html.Small("增加投入")
            ]), className="text-center border-success"), md=3),
            dbc.Col(dbc.Card(dbc.CardBody([
                html.H4(pri_counts.get("⚪ 低优先级", 0), style={"color":"#95a5a6"}),
                html.Small("低优先级")
            ]), className="text-center"), md=3),
        ], className="mb-3")
        sections.append(html.H5("🚦 产品优先级分类", className="mt-3 mb-2"))
        sections.append(pri_summary)

        # Priority table
        pri_formatted = _format_df_columns(priority_df.copy(), mode="mode2")
        sections.append(_render_datatable(pri_formatted, header_color="#2C3E50"))

    # ── Section 5: Product Line Suggestions ──
    if pl_df is not None and not pl_df.empty:
        sections.append(html.H5("📋 品线优化建议", className="mt-4 mb-2"))
        pl_formatted = _format_df_columns(pl_df.copy(), mode="mode2")
        sections.append(_render_datatable(pl_formatted, header_color="#2C3E50"))

    # ── Section 6: Charts ──
    charts = []

    def _to_float(v):
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).replace("%","").replace("€","").replace(",","").strip()
        try:
            return float(s)
        except Exception:
            return 0

    # Chart 1: ACoS vs Target bar chart (per ASIN)
    if priority_df is not None and not priority_df.empty:
        chart_df = priority_df[priority_df["ASIN"] != ""].head(20)
        if not chart_df.empty and "综合ACoS" in chart_df.columns and "目标ACoS" in chart_df.columns:
            _acos_vals = chart_df["综合ACoS"].apply(_to_float)
            _target_vals = chart_df["目标ACoS"].apply(_to_float)
            _labels = chart_df["ASIN"].astype(str)

            fig_acos = go.Figure()
            fig_acos.add_trace(go.Bar(
                x=_labels, y=_acos_vals, name="实际ACoS",
                marker_color=["#e74c3c" if a > t else "#27ae60" for a, t in zip(_acos_vals, _target_vals)],
            ))
            fig_acos.add_trace(go.Scatter(
                x=_labels, y=_target_vals, name="目标ACoS",
                mode="markers+lines", line=dict(color="#f39c12", dash="dash"),
            ))
            fig_acos.update_layout(
                title="ASIN ACoS vs 目标ACoS", height=350,
                xaxis_title="ASIN", yaxis_title="ACoS (%)",
                plot_bgcolor="white", paper_bgcolor="white",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            charts.append(dbc.Col(dcc.Graph(figure=fig_acos), md=12))

    # Chart 2: Spend vs Sales scatter (bubble size = ACoS)
    if priority_df is not None and not priority_df.empty:
        chart_df2 = priority_df[priority_df["ASIN"] != ""].copy()
        if not chart_df2.empty and "合计花费" in chart_df2.columns and "合计销售额" in chart_df2.columns:
            _spend = chart_df2["合计花费"].apply(_to_float)
            _sales = chart_df2["合计销售额"].apply(_to_float)
            _acos2 = chart_df2["综合ACoS"].apply(_to_float) if "综合ACoS" in chart_df2.columns else pd.Series([0]*len(chart_df2))
            _pri = chart_df2["优先级"].astype(str) if "优先级" in chart_df2.columns else pd.Series([""]*len(chart_df2))
            _color_map = {"🔴 紧急处理":"#e74c3c","🟡 观察调整":"#f39c12","🟢 增加投入":"#27ae60","⚪ 低优先级":"#bdc3c7"}
            _colors = [_color_map.get(p, "#95a5a6") for p in _pri]

            fig_scatter = go.Figure()
            fig_scatter.add_trace(go.Scatter(
                x=_spend, y=_sales,
                mode="markers",
                marker=dict(
                    size=[max(6, min(a * 3, 40)) for a in _acos2],
                    color=_colors, opacity=0.7,
                    line=dict(width=1, color="white"),
                ),
                text=[f"{a}<br>ACoS:{ac:.1f}%" for a, ac in zip(chart_df2["ASIN"], _acos2)],
                hovertemplate="%{text}<br>花费:€%{x:,.0f}<br>销售:€%{y:,.0f}<extra></extra>",
            ))
            fig_scatter.update_layout(
                title="花费 vs 销售额（气泡大小=ACoS）",
                xaxis_title="花费 (€)", yaxis_title="销售额 (€)",
                height=400, plot_bgcolor="white", paper_bgcolor="white",
            )
            charts.append(dbc.Col(dcc.Graph(figure=fig_scatter), md=12))

    # Chart 3: Product line pie chart
    if pl_df is not None and not pl_df.empty and "花费(€)" in pl_df.columns:
        _pl_spend = pl_df["花费(€)"].apply(_to_float)
        _pl_names = pl_df["品线"].astype(str)
        if _pl_spend.sum() > 0:
            fig_pie = go.Figure()
            fig_pie.add_trace(go.Pie(
                labels=_pl_names, values=_pl_spend,
                textinfo="label+percent", hole=0.4,
            ))
            fig_pie.update_layout(title="品线花费占比", height=350, paper_bgcolor="white")
            charts.append(dbc.Col(dcc.Graph(figure=fig_pie), md=6))

        # Sales pie
        if "销售额(€)" in pl_df.columns:
            _pl_sales = pl_df["销售额(€)"].apply(_to_float)
            if _pl_sales.sum() > 0:
                fig_pie2 = go.Figure()
                fig_pie2.add_trace(go.Pie(
                    labels=_pl_names, values=_pl_sales,
                    textinfo="label+percent", hole=0.4,
                ))
                fig_pie2.update_layout(title="品线销售额占比", height=350, paper_bgcolor="white")
                charts.append(dbc.Col(dcc.Graph(figure=fig_pie2), md=6))

    if charts:
        sections.append(html.H5("📈 可视化分析", className="mt-4 mb-2"))
        sections.append(dbc.Row(charts))

    return html.Div(sections)


def _build_mode1_tabs(results_dict):
    """
    从 results_dict {tab_name: DataFrame} 构建模式1 Tab 列表。
    关键词分析表格冻结第一列（关键词列）。
    """
    tab_order = [
        "执行摘要", "竞品独有词(需布局)", "排名差距词(需提升)",
        "流量获取差距词", "本品高潜力词", "词根聚类Campaign规划",
        "综合关键词优先投放", "产品预算规划", "流量来源结构对比",
        "月度广告执行计划",
    ]
    # 需要冻结第一列的 sheet
    freeze_sheets = {
        "竞品独有词(需布局)", "排名差距词(需提升)", "流量获取差距词",
        "本品高潜力词", "词根聚类Campaign规划", "综合关键词优先投放",
        "流量来源结构对比",
    }
    tabs = []
    for label in tab_order:
        df = results_dict.get(label)
        if df is not None and not df.empty:
            df = _format_df_columns(df, mode="mode1")
            count_info = f"({len(df)})" if len(df) > 1 else ""
            short_label = label[:6] + "..." if len(label) > 8 else label
            freeze = 1 if label in freeze_sheets else 0
            tabs.append(dbc.Tab(
                label=f"{short_label} {count_info}",
                children=[
                    html.H6(label, className="mt-2 mb-2"),
                    _render_datatable(df, header_color="#27AE60", freeze_cols=freeze),
                ],
            ))
    return tabs


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
        dbc.NavItem(dbc.NavLink("产品配置", href="/config", id="nav-config")),
        dbc.NavItem(dbc.NavLink("使用指南", href="/guide", id="nav-guide")),
    ],
)

# ── 模式2 上传区（可折叠）──────────────────────────────────────────
mode2_upload_collapse = dbc.Card([
    dbc.CardHeader(
        dbc.Button(
            "📤 上传新数据更新分析",
            id="btn-toggle-mode2-upload",
            color="link",
            className="text-decoration-none fw-bold w-100 text-start",
        ),
        className="p-0",
    ),
    dbc.Collapse(
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("广告活动报告 *", className="fw-bold"),
                    dcc.Upload(
                        id="upload-campaign",
                        children=html.Div([
                            "拖拽或 ", html.A("点击上传", className="text-primary"),
                        ]),
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
                        children=html.Div([
                            "拖拽或 ", html.A("点击上传", className="text-primary"),
                        ]),
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
                        children=html.Div([
                            "拖拽或 ", html.A("点击上传", className="text-primary"),
                        ]),
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
                        children=html.Div([
                            "拖拽或 ", html.A("点击上传", className="text-primary"),
                        ]),
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
        id="collapse-mode2-upload",
        is_open=False,
    ),
], className="mb-3")

# ── 模式1 上传区（可折叠）──────────────────────────────────────────
mode1_upload_collapse = dbc.Card([
    dbc.CardHeader(
        dbc.Button(
            "📤 上传新数据更新分析",
            id="btn-toggle-mode1-upload",
            color="link",
            className="text-decoration-none fw-bold w-100 text-start",
        ),
        className="p-0",
    ),
    dbc.Collapse(
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("本品关键词文件 *", className="fw-bold"),
                    dcc.Upload(
                        id="upload-own",
                        children=html.Div([
                            "拖拽或 ", html.A("点击上传", className="text-primary"),
                        ]),
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
                        children=html.Div([
                            "拖拽或 ", html.A("点击上传", className="text-primary"),
                        ]),
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
        id="collapse-mode1-upload",
        is_open=False,
    ),
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
            html.P(
                "数据已加载，选择模式查看分析结果",
                className="text-center text-muted",
            ),
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
        # 历史记录下拉
        history = _load_history_list("mode2")
        history_selector = dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(html.H5("📈 广告监控分析结果", className="mb-0 mt-1"), md=6),
                    dbc.Col([
                        dbc.InputGroup([
                            dbc.InputGroupText("历史记录"),
                            dbc.Select(
                                id="select-mode2-history",
                                options=[{"label": h["label"], "value": h["value"]} for h in history],
                                value=history[0]["value"] if history else None,
                                placeholder="选择历史版本...",
                            ),
                        ], size="sm"),
                    ], md=6),
                ]),
            ]),
        ], className="mb-2")

        # 加载最新数据
        loaded = _load_latest("mode2")
        if loaded is None:
            initial_results = dbc.Alert("无法连接数据库，请上传文件进行分析。", color="warning", className="mt-3")
        elif loaded:
            tabs = _build_mode2_tabs(loaded)
            initial_results = dbc.Card([dbc.CardBody(dbc.Tabs(tabs))])
        else:
            initial_results = dbc.Alert("暂无历史分析数据，请上传文件进行首次分析。", color="info", className="mt-3")

        return html.Div([
            history_selector,
            html.Div(id="mode2-history-results", children=initial_results),
            mode2_upload_collapse,
            mode2_results,
        ])

    elif pathname == "/mode1":
        # 历史记录下拉
        history = _load_history_list("mode1")
        history_selector = dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(html.H5("📊 关键词竞争分析结果", className="mb-0 mt-1"), md=6),
                    dbc.Col([
                        dbc.InputGroup([
                            dbc.InputGroupText("历史记录"),
                            dbc.Select(
                                id="select-mode1-history",
                                options=[{"label": h["label"], "value": h["value"]} for h in history],
                                value=history[0]["value"] if history else None,
                                placeholder="选择历史版本...",
                            ),
                        ], size="sm"),
                    ], md=6),
                ]),
            ]),
        ], className="mb-2")

        loaded = _load_latest("mode1")
        if loaded is None:
            initial_results = dbc.Alert("无法连接数据库，请上传文件进行分析。", color="warning", className="mt-3")
        elif loaded:
            tabs = _build_mode1_tabs(loaded)
            initial_results = dbc.Card([
                dbc.CardBody(dbc.Tabs(tabs) if tabs else html.P("暂无有效数据")),
            ])
        else:
            initial_results = dbc.Alert("暂无历史分析数据，请上传文件进行首次分析。", color="info", className="mt-3")

        return html.Div([
            history_selector,
            html.Div(id="mode1-history-results", children=initial_results),
            mode1_upload_collapse,
            mode1_results,
        ])

    elif pathname == "/guide":
        from pages import guide_page
        return guide_page()

    elif pathname == "/config":
        from pages import config_page
        return config_page()

    return home_page


# ── 历史记录切换回调 ──────────────────────────────────────────────
@callback(
    Output("mode2-history-results", "children"),
    Input("select-mode2-history", "value"),
    prevent_initial_call=True,
)
def switch_mode2_history(timestamp):
    if not timestamp:
        return dash.no_update
    loaded = _load_by_timestamp("mode2", timestamp)
    if loaded:
        tabs = _build_mode2_tabs(loaded)
        return dbc.Card([dbc.CardBody(dbc.Tabs(tabs))])
    return dbc.Alert("该记录无数据", color="warning")


@callback(
    Output("mode1-history-results", "children"),
    Input("select-mode1-history", "value"),
    prevent_initial_call=True,
)
def switch_mode1_history(timestamp):
    if not timestamp:
        return dash.no_update
    loaded = _load_by_timestamp("mode1", timestamp)
    if loaded:
        tabs = _build_mode1_tabs(loaded)
        return dbc.Card([dbc.CardBody(dbc.Tabs(tabs) if tabs else html.P("无数据"))])
    return dbc.Alert("该记录无数据", color="warning")


# ── 折叠按钮回调 ──────────────────────────────────────────────────
@callback(
    Output("collapse-mode2-upload", "is_open"),
    Input("btn-toggle-mode2-upload", "n_clicks"),
    State("collapse-mode2-upload", "is_open"),
    prevent_initial_call=True,
)
def toggle_mode2_upload(n, is_open):
    return not is_open


@callback(
    Output("collapse-mode1-upload", "is_open"),
    Input("btn-toggle-mode1-upload", "n_clicks"),
    State("collapse-mode1-upload", "is_open"),
    prevent_initial_call=True,
)
def toggle_mode1_upload(n, is_open):
    return not is_open


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
        return dash.no_update, dbc.Alert(
            "请上传广告活动报告和推广商品报告", color="warning",
        )

    try:
        from core.ad_monitor_core import load_and_process

        campaign_io = _decode_upload(campaign_c)
        product_io = _decode_upload(product_c)
        st_io = _decode_upload(st_c)
        kw_io = _decode_upload(kw_c)
        cfg_io = _decode_upload(cfg_c)

        # Load budget config from Supabase (if saved in config page)
        budget_config = None
        try:
            sb = _get_supabase()
            cfg_resp = sb.table("analysis_results").select("data").eq("mode", "config").eq("tab_name", "budget_config").order("created_at", desc=True).limit(1).execute()
            if cfg_resp.data:
                budget_config = cfg_resp.data[0].get("data", {})
        except Exception:
            pass

        results = load_and_process(
            campaign_file=campaign_io,
            product_file=product_io,
            search_term_file=st_io,
            keyword_file=kw_io,
            config_file=cfg_io,
            budget_config=budget_config,
        )

        # 标准化 key 映射
        results_dict = {
            "费比总览": results.get("overview"),
            "Campaign明细": results.get("campaigns"),
            "关键词ROI分析": results.get("keywords"),
            "建议否定词": results.get("negatives"),
        }
        # 过滤掉 None
        results_dict = {k: v for k, v in results_dict.items() if v is not None}

        # 保存到 Supabase
        _save_results("mode2", results_dict)

        # 构建结果 Tab 显示
        tabs = _build_mode2_tabs(results_dict, full_results=results)
        result_card = dbc.Card([
            dbc.CardHeader(html.H5("📈 广告监控分析结果", className="mb-0")),
            dbc.CardBody(dbc.Tabs(tabs)),
        ])

        return result_card, dbc.Alert("✅ 分析完成!", color="success", duration=3000)

    except Exception as e:
        tb = traceback.format_exc()
        return dash.no_update, dbc.Alert(
            [html.Strong("分析失败: "), html.Br(), str(e),
             html.Pre(tb, style={"fontSize": "11px"})],
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
        return dash.no_update, dbc.Alert(
            "请上传本品和竞品关键词文件", color="warning",
        )

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

        # results 已经是 {tab_name: DataFrame} 格式
        results_dict = {k: v for k, v in results.items() if v is not None}

        # 保存到 Supabase
        _save_results("mode1", results_dict)

        # 构建结果 Tab 显示
        tabs = _build_mode1_tabs(results_dict)
        result_card = dbc.Card([
            dbc.CardHeader(html.H5("📊 关键词竞争分析结果", className="mb-0")),
            dbc.CardBody(
                dbc.Tabs(tabs) if tabs else html.P("分析完成但无数据")
            ),
        ])

        return result_card, dbc.Alert("✅ 分析完成!", color="success", duration=3000)

    except Exception as e:
        tb = traceback.format_exc()
        return dash.no_update, dbc.Alert(
            [html.Strong("分析失败: "), html.Br(), str(e),
             html.Pre(tb, style={"fontSize": "11px"})],
            color="danger",
        )


# ── 入口 ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
