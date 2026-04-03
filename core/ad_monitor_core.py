#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
广告监控看板核心模块 (Web版)
从 ad_monitor.py 重构，保留全部业务逻辑，移除 Excel/openpyxl 输出。
所有结果以 pandas DataFrame 返回，供 Web 前端渲染。

核心函数:
  - get_overview_df()    → Sheet 1 "费比总览"
  - get_campaigns_df()   → Sheet 2 "Campaign明细"
  - get_keywords_df()    → Sheet 3 "关键词ROI分析"
  - get_negatives_df()   → Sheet 4 "建议否定词"
  - load_and_process()   → 一站式加载+处理，返回 dict[str, DataFrame]
"""

import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
from io import BytesIO

VERSION = "1.1-web"

# ═══════════════════════════════════════════════════════════════════════════════
# 领星列名映射（实际导出列名 → 内部标准名）
# ═══════════════════════════════════════════════════════════════════════════════
LINGXING_COL_MAP = {
    # 通用维度列
    "广告活动":       "campaign",
    "广告组":         "ad_group",
    "有效状态":       "status",
    "类型":           "ad_type_raw",   # SP/SB/SBV/SD
    "竞价策略":       "bid_strategy",
    "匹配方式":       "match_type",
    "关键词":         "keyword",
    "用户搜索词":     "search_term",
    "投放":           "targeting",
    "日期":           "date",
    "ASIN":           "asin",
    "MSKU":           "sku",
    "广告名称":       "ad_name",
    "广告组投放类型": "group_type",
    # 数值列（领星用"-本币"后缀）
    "花费-本币":      "spend",
    "广告销售额-本币":"ad_sales",
    "直接销售额-本币":"direct_sales",
    "间接销售额-本币":"indirect_sales",
    "CPC-本币":       "cpc",
    "CPA-本币":       "cpa",
    "广告笔单价-本币":"ad_order_value",
    # 无后缀的数值列
    "曝光量":         "impressions",
    "点击":           "clicks",
    "CTR":            "ctr",
    "ACoS":           "acos",
    "ROAS":           "roas",
    "广告订单":       "orders",
    "直接订单":       "direct_orders",
    "间接订单":       "indirect_orders",
    "CVR":            "cvr",
    "广告销量":       "ad_units",
}

# DJI 品牌词根（用于否定词检测）
DJI_ROOTS = [
    "dji", "Dji", "DJI", "dJi", "DjI", "dJI", "djI",
    "di ji", "d ji", "dji ", " dji",
]


# ═══════════════════════════════════════════════════════════════════════════════
# 通用辅助函数
# ═══════════════════════════════════════════════════════════════════════════════

def _safe(val, default=0.0):
    try:
        v = float(str(val).replace("%", "").replace(",", ""))
        return default if np.isnan(v) else v
    except Exception:
        return default


def _pct(val):
    """把领星导出的百分比字符串或小数都统一转成小数"""
    s = str(val).strip()
    if s.endswith("%"):
        return _safe(s) / 100
    v = _safe(s)
    # 领星有时直接是小数，有时是百分比数值（>1则是百分比数值）
    return v / 100 if v > 1 else v


# ═══════════════════════════════════════════════════════════════════════════════
# 配置加载 — 读取 广告配置.xlsx
# ═══════════════════════════════════════════════════════════════════════════════

def load_config(config_source):
    """
    读取 广告配置.xlsx，返回统一的 cfg 字典。

    Parameters
    ----------
    config_source : str 或 BytesIO
        文件路径字符串，或上传的 BytesIO 对象。
        如果是字符串且为目录，则在该目录下查找 "广告配置.xlsx"。

    Returns
    -------
    dict
        {
          "product_lines": [ { "name", "keywords", "sp_acos", "sb_acos", "sbv_acos", "market" }, ... ],
          "asins":         { "B0XXXXX": { "name", "product_line", "acos_override" }, ... },
          "fallback_acos": 0.04,
        }
    """
    default_cfg = {"product_lines": [], "asins": {}, "fallback_acos": 0.04}

    # 确定读取源
    if isinstance(config_source, BytesIO):
        xlsx_input = config_source
    elif isinstance(config_source, str):
        if os.path.isdir(config_source):
            xlsx_path = os.path.join(config_source, "广告配置.xlsx")
        else:
            xlsx_path = config_source
        if not os.path.isfile(xlsx_path):
            return default_cfg
        xlsx_input = xlsx_path
    else:
        return default_cfg

    cfg = {"product_lines": [], "asins": {}, "fallback_acos": 0.04}

    try:
        # Sheet1：品线费比配置
        # 先找到真正的表头行（跳过说明行）
        raw = pd.read_excel(xlsx_input, sheet_name="品线费比配置",
                            header=None, dtype=str)
        header_row = None
        for i, row in raw.iterrows():
            if any("品线名称" in str(v) for v in row.values):
                header_row = i
                break

        # BytesIO 读完后需要重置位置
        if isinstance(xlsx_input, BytesIO):
            xlsx_input.seek(0)

        if header_row is not None:
            df1 = pd.read_excel(xlsx_input, sheet_name="品线费比配置",
                                header=header_row, dtype=str)
        else:
            if isinstance(xlsx_input, BytesIO):
                xlsx_input.seek(0)
            df1 = pd.read_excel(xlsx_input, sheet_name="品线费比配置", dtype=str)

        if isinstance(xlsx_input, BytesIO):
            xlsx_input.seek(0)

        for _, row in df1.iterrows():
            name = str(row.get("品线名称", "") or "").strip()
            # 跳过空行、表头重复行、说明文字行
            if not name or name in ("nan", "品线名称") or name.startswith("示例") or name.startswith("【"):
                continue
            kw_raw = str(row.get("活动名匹配词", "") or "").strip()
            keywords = [k.strip() for k in kw_raw.split(",") if k.strip()] if kw_raw and kw_raw != "nan" else [name]

            def _get_acos(col, _row=row):
                v = str(_row.get(col, "") or "").strip()
                if not v or v == "nan":
                    return None
                return _safe(v) / 100 if _safe(v) > 1 else _safe(v)

            sp  = _get_acos("SP 目标ACoS")
            sb  = _get_acos("SB 目标ACoS")
            sbv = _get_acos("SBV 目标ACoS")

            cfg["product_lines"].append({
                "name":     name,
                "keywords": keywords,
                "sp_acos":  sp  or 0.04,
                "sb_acos":  sb  or sp  or 0.04,
                "sbv_acos": sbv or sb  or sp or 0.04,
                "market":   str(row.get("站点", "DE") or "DE").strip(),
            })

        # Sheet2：ASIN产品信息
        if isinstance(xlsx_input, BytesIO):
            xlsx_input.seek(0)
        raw2 = pd.read_excel(xlsx_input, sheet_name="ASIN产品信息",
                             header=None, dtype=str)
        header_row2 = None
        for i, row in raw2.iterrows():
            if any("ASIN" in str(v) for v in row.values):
                header_row2 = i
                break

        if isinstance(xlsx_input, BytesIO):
            xlsx_input.seek(0)

        if header_row2 is not None:
            df2 = pd.read_excel(xlsx_input, sheet_name="ASIN产品信息",
                                header=header_row2, dtype=str)
            for _, row in df2.iterrows():
                asin = str(row.get("ASIN", "") or "").strip().upper()
                if not asin or asin in ("nan", "ASIN"):
                    continue
                override_raw = str(row.get("目标费比覆盖", "") or "").strip()
                override = None
                if override_raw and override_raw != "nan":
                    v = _safe(override_raw)
                    override = v / 100 if v > 1 else v

                cfg["asins"][asin] = {
                    "name":         str(row.get("产品名称（简称）", asin) or asin).strip(),
                    "product_line": str(row.get("品线", "") or "").strip(),
                    "acos_override": override,
                }

    except Exception:
        # 读取失败，返回默认配置
        pass

    return cfg


def get_target_acos(cfg, campaign_name, ad_type, asin=None):
    """
    按优先级返回目标 ACoS：
    1. ASIN 单独指定的 acos_override
    2. 从活动名第4段匹配品线，取对应广告类型费比
    3. 兜底 fallback_acos
    """
    # ASIN 单独覆盖
    if asin and cfg.get("asins"):
        asin_info = cfg["asins"].get(str(asin).upper())
        if asin_info and asin_info.get("acos_override"):
            return asin_info["acos_override"]

    # 从活动名第4段识别品线
    parts = str(campaign_name).split("_")
    segment4 = parts[3].strip() if len(parts) >= 4 else ""

    matched_line = None
    for pl in cfg.get("product_lines", []):
        if segment4 in pl["keywords"]:
            matched_line = pl
            break

    if matched_line:
        at = str(ad_type).upper()
        if "SBV" in at:   return matched_line["sbv_acos"]
        if "SB"  in at:   return matched_line["sb_acos"]
        return matched_line["sp_acos"]

    return cfg.get("fallback_acos", 0.04)


def get_product_line(cfg, campaign_name):
    """从活动名第4段返回品线名称，未匹配返回第4段原始值。"""
    parts = str(campaign_name).split("_")
    segment4 = parts[3].strip() if len(parts) >= 4 else "未知"
    for pl in cfg.get("product_lines", []):
        if segment4 in pl["keywords"]:
            return pl["name"]
    return segment4 if segment4 else "未知"


# ═══════════════════════════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════════════════════════

def _read_lingxing(source):
    """
    读取领星导出的Excel，自动找到真正的表头行（跳过前几行说明）。

    Parameters
    ----------
    source : str 或 BytesIO
        文件路径或上传的 BytesIO 对象。
    """
    for skip in range(0, 5):
        if isinstance(source, BytesIO):
            source.seek(0)
        df = pd.read_excel(source, sheet_name=0, skiprows=skip, dtype=str)
        cols = " ".join(str(c) for c in df.columns)
        if any(k in cols for k in ["广告活动", "花费", "ACoS", "曝光量", "关键词", "用户搜索词"]):
            # 应用列名映射
            df = df.rename(columns=LINGXING_COL_MAP)
            df.columns = [str(c).strip() for c in df.columns]
            return df
    if isinstance(source, BytesIO):
        source.seek(0)
    df = pd.read_excel(source, sheet_name=0, dtype=str)
    df = df.rename(columns=LINGXING_COL_MAP)
    return df


def _detect_type_from_col(df):
    """从「类型」列或活动名称推断广告类型 SP/SB/SBV。"""
    if "ad_type_raw" in df.columns:
        def _map(v):
            v = str(v).upper()
            if "SBV" in v or "VIDEO" in v:  return "SBV"
            if "SB"  in v or "BRAND" in v:  return "SB"
            if "SD"  in v or "DISPLAY" in v:return "SD"
            return "SP"
        df["ad_type"] = df["ad_type_raw"].apply(_map)
    elif "campaign" in df.columns:
        df["ad_type"] = df["campaign"].apply(detect_ad_type)
    else:
        df["ad_type"] = "SP"
    return df


def _numerify(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: _safe(x))
    return df


def _pctify(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(_pct)
    return df


def detect_ad_type(campaign_name):
    """从活动名称推断广告类型 SP/SB/SBV"""
    n = str(campaign_name).upper()
    if "SBV" in n or "VIDEO" in n or "视频" in n:
        return "SBV"
    if "SB" in n or "BRAND" in n or "品牌" in n:
        return "SB"
    return "SP"


def _clean_df(df, key_col="campaign"):
    """过滤空行，确保关键列存在"""
    if key_col in df.columns:
        df = df[df[key_col].notna()].copy()
        df = df[df[key_col].astype(str).str.strip().str.lower() != "nan"].copy()
        df = df[df[key_col].astype(str).str.strip() != ""].copy()
    return df.reset_index(drop=True)


def load_campaign_report(source):
    """
    加载领星广告活动报告。

    Parameters
    ----------
    source : str 或 BytesIO
    """
    df = _read_lingxing(source)
    df = _detect_type_from_col(df)
    df = _numerify(df, ["spend", "ad_sales", "impressions", "clicks", "orders", "cpc", "roas"])
    df = _pctify(df,  ["acos", "ctr", "cvr"])
    # 领星活动报告无预算列，budget_used_rate 留 0（从 config 取预算）
    df["budget"]           = 0.0
    df["budget_used_rate"] = 0.0
    df = _clean_df(df, "campaign")
    return df


def load_product_report(source):
    """
    加载领星推广商品报告。

    Parameters
    ----------
    source : str 或 BytesIO
    """
    df = _read_lingxing(source)
    df = _detect_type_from_col(df)
    df = _numerify(df, ["spend", "ad_sales", "impressions", "clicks", "orders", "cpc", "roas"])
    df = _pctify(df,  ["acos", "ctr", "cvr"])
    df = _clean_df(df, "campaign")
    return df


def load_search_term_report(source):
    """
    加载领星用户搜索词报告（也兼容关键词报告）。

    Parameters
    ----------
    source : str 或 BytesIO
    """
    df = _read_lingxing(source)
    df = _detect_type_from_col(df)
    df = _numerify(df, ["spend", "ad_sales", "impressions", "clicks", "orders", "cpc"])
    df = _pctify(df,  ["acos"])
    df = _clean_df(df, "campaign")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 费比判断
# ═══════════════════════════════════════════════════════════════════════════════

def calc_acos(spend, direct_sales, indirect_sales=0.0):
    """
    ACoS = 花费 ÷ (直接销售额 + 间接销售额)
    口径：广告总销售额（直接+间接），不使用报表自带 ACoS 列
    """
    total_sales = _safe(direct_sales) + _safe(indirect_sales)
    if total_sales <= 0:
        return 0.0
    return _safe(spend) / total_sales


def acos_status(acos_val, target_acos):
    """
    两档判断（无容忍区间）：
    超过目标（红）: acos > target
    低于目标（蓝）: acos <= target 且 acos > 0
    无数据（灰）  : acos == 0（无花费）

    Returns
    -------
    str
        状态标签文本
    """
    if acos_val <= 0:
        return "无花费"
    if acos_val > target_acos:
        return f"超费比 {acos_val*100:.1f}%"
    return f"达标 {acos_val*100:.1f}%"


def budget_status(used_rate):
    """返回预算状态标签。"""
    if used_rate <= 0:
        return "未消耗"
    if used_rate >= 0.90:
        return f"预算告急 {used_rate*100:.0f}%"
    if used_rate >= 0.50:
        return f"正常 {used_rate*100:.0f}%"
    return f"消耗偏低 {used_rate*100:.0f}%"


# ═══════════════════════════════════════════════════════════════════════════════
# DJI品牌词检测
# ═══════════════════════════════════════════════════════════════════════════════

def is_dji_misspell(term):
    """
    检测是否包含DJI的变体写法（错写、空格、错序等）
    """
    t = str(term).lower().strip()
    patterns = [
        r"\bdji\b",          # 正常写法（也收录，因为可能被错误加入Auto）
        r"\bd\s+ji\b",       # d ji
        r"\bdj\s+i\b",       # dj i
        r"\bd\s*j\s*i\b",    # d j i
        r"\bdjl\b",          # 错字
        r"\bdjii\b",         # 错字
        r"\bdjji\b",         # 错字
        r"\bjdi\b",          # 错序
        r"\bidj\b",          # 错序
    ]
    return any(re.search(p, t) for p in patterns)


# ═══════════════════════════════════════════════════════════════════════════════
# Sheet 1：费比总览 → get_overview_df
# ═══════════════════════════════════════════════════════════════════════════════

def get_overview_df(product_df, cfg):
    """
    生成费比总览 DataFrame（对应原 Sheet 1 "费比总览"）。

    Parameters
    ----------
    product_df : DataFrame
        推广商品报告，由 load_product_report() 返回。
    cfg : dict
        配置字典，由 load_config() 返回。

    Returns
    -------
    DataFrame
        列名与原 Excel 输出一致（中文），包含每个 ASIN × 广告类型的费比信息，
        以及全局汇总行（品线="全部品线合计"）。
    """
    if product_df is None or product_df.empty:
        return pd.DataFrame()

    cur = "€"

    if "asin" not in product_df.columns:
        return pd.DataFrame({"错误": ["推广商品报告缺少 ASIN 列，请确认导出格式"]})

    # 确保数值列存在且为 float
    product_df = product_df.copy()
    for col in ["spend", "direct_sales", "indirect_sales", "orders", "clicks"]:
        if col not in product_df.columns:
            product_df[col] = 0.0
        else:
            product_df[col] = product_df[col].apply(lambda x: _safe(x))

    # 从活动名给每条记录打上品线标签
    product_df["product_line"] = product_df["campaign"].apply(
        lambda c: get_product_line(cfg, c))

    # 聚合：ASIN × 广告类型
    grp = product_df.groupby(["asin", "ad_type"]).agg(
        campaign       = ("campaign",       "first"),
        product_line   = ("product_line",   "first"),
        spend          = ("spend",          "sum"),
        direct_sales   = ("direct_sales",   "sum"),
        indirect_sales = ("indirect_sales", "sum"),
        orders         = ("orders",         "sum"),
        clicks         = ("clicks",         "sum"),
    ).reset_index()

    # 用直接+间接口径重算 ACoS 和 ROAS
    grp["total_sales"] = grp["direct_sales"] + grp["indirect_sales"]
    grp["acos"] = grp.apply(
        lambda r: calc_acos(r["spend"], r["direct_sales"], r["indirect_sales"]), axis=1)
    grp["roas"] = grp.apply(
        lambda r: float(r["total_sales"]) / float(r["spend"]) if float(r["spend"]) > 0 else 0.0, axis=1)

    AD_TYPES = ["SP", "SB", "SBV"]
    asin_info = cfg.get("asins", {}) if cfg else {}

    # 先给每个 ASIN 计算综合信息
    asins = grp["asin"].unique()
    asin_summary = {}
    for asin in asins:
        d = grp[grp["asin"] == asin]
        ts = d["spend"].sum()
        sales = d["total_sales"].sum()
        ac = ts / sales if sales > 0 else 0
        pl = d["product_line"].iloc[0] if not d.empty else "未知"
        asin_summary[asin] = {"product_line": pl, "total_acos": ac, "total_spend": ts}

    # 按品线 → 超费比优先 排序
    def _sort_key(a):
        s = asin_summary[a]
        pl = s["product_line"]
        sample_camp = grp[grp["asin"] == a]["campaign"].iloc[0] if not grp[grp["asin"] == a].empty else ""
        tgt = get_target_acos(cfg, sample_camp, "SP", a)
        over = 1 if s["total_acos"] > tgt else 0
        return (pl, over * -1, -s["total_spend"])

    sorted_asins = sorted(asins, key=_sort_key)

    rows = []
    for asin in sorted_asins:
        asin_data = grp[grp["asin"] == asin]
        pl = asin_summary[asin]["product_line"]
        info = asin_info.get(str(asin).upper(), {})
        name = info.get("name", asin)

        sample_camp = asin_data["campaign"].iloc[0] if not asin_data.empty else ""
        row_data = {"品线": pl, "ASIN": asin, "产品名称": name}

        total_spend = total_sales_v = total_orders = 0.0

        for t in AD_TYPES:
            sub = asin_data[asin_data["ad_type"] == t]
            tgt = get_target_acos(cfg, sample_camp, t, asin)

            if sub.empty or sub["spend"].iloc[0] <= 0:
                row_data[f"{t} 花费({cur})"] = "-"
                row_data[f"{t} 销售额({cur})"] = "-"
                row_data[f"{t} ACoS"] = "-"
                row_data[f"{t} 目标ACoS"] = f"{tgt*100:.1f}%"
                row_data[f"{t} 状态"] = "-"
            else:
                sp = sub["spend"].iloc[0]
                sl = sub["total_sales"].iloc[0]
                ac = sub["acos"].iloc[0]
                label = acos_status(ac, tgt)
                total_spend += sp
                total_sales_v += sl
                total_orders += sub["orders"].iloc[0]
                row_data[f"{t} 花费({cur})"] = round(sp, 1)
                row_data[f"{t} 销售额({cur})"] = round(sl, 1)
                row_data[f"{t} ACoS"] = f"{ac*100:.1f}%"
                row_data[f"{t} 目标ACoS"] = f"{tgt*100:.1f}%"
                row_data[f"{t} 状态"] = label

        # 综合 ACoS
        overall_tgt = get_target_acos(cfg, sample_camp, "SP", asin)
        overall_acos = total_spend / total_sales_v if total_sales_v > 0 else 0
        overall_label = acos_status(overall_acos, overall_tgt)

        row_data["合计花费"] = round(total_spend, 1)
        row_data["合计销售额"] = round(total_sales_v, 1)
        row_data["综合ACoS"] = f"{overall_acos*100:.1f}%" if total_spend > 0 else "-"
        row_data["综合状态"] = overall_label
        row_data["合计订单"] = int(total_orders)

        rows.append(row_data)

    # 全局汇总行
    all_spend = grp["spend"].sum()
    all_sales = grp["total_sales"].sum()
    all_orders = grp["orders"].sum()
    all_acos = all_spend / all_sales if all_sales > 0 else 0

    summary_row = {"品线": "全部品线合计", "ASIN": "", "产品名称": ""}
    for t in AD_TYPES:
        summary_row[f"{t} 花费({cur})"] = ""
        summary_row[f"{t} 销售额({cur})"] = ""
        summary_row[f"{t} ACoS"] = ""
        summary_row[f"{t} 目标ACoS"] = ""
        summary_row[f"{t} 状态"] = ""
    summary_row["合计花费"] = round(all_spend, 1)
    summary_row["合计销售额"] = round(all_sales, 1)
    summary_row["综合ACoS"] = f"{all_acos*100:.1f}%"
    summary_row["综合状态"] = ""
    summary_row["合计订单"] = int(all_orders)
    rows.append(summary_row)

    # 定义列顺序
    col_order = ["品线", "ASIN", "产品名称"]
    for t in AD_TYPES:
        col_order += [f"{t} 花费({cur})", f"{t} 销售额({cur})", f"{t} ACoS", f"{t} 目标ACoS", f"{t} 状态"]
    col_order += ["合计花费", "合计销售额", "综合ACoS", "综合状态", "合计订单"]

    result_df = pd.DataFrame(rows, columns=col_order)
    return result_df


# ═══════════════════════════════════════════════════════════════════════════════
# Sheet 2：Campaign 明细 → get_campaigns_df
# ═══════════════════════════════════════════════════════════════════════════════

def get_campaigns_df(camp_df, cfg):
    """
    生成 Campaign 明细 DataFrame（对应原 Sheet 2 "Campaign明细"）。

    Parameters
    ----------
    camp_df : DataFrame
        广告活动报告，由 load_campaign_report() 返回。
    cfg : dict
        配置字典，由 load_config() 返回。

    Returns
    -------
    DataFrame
        列: 品线, 广告类型, 活动名称, 状态, 花费(€), 销售额(€), ACoS,
            目标ACoS, 费比状态, ROAS, 点击, 订单, CVR, 竞价策略
    """
    if camp_df is None or camp_df.empty:
        return pd.DataFrame()

    camp_df = camp_df.copy()
    camp_df["product_line"] = camp_df["campaign"].apply(lambda c: get_product_line(cfg, c))
    for col in ["direct_sales", "indirect_sales"]:
        if col not in camp_df.columns:
            camp_df[col] = 0.0

    camp_df["_acos_real"] = camp_df.apply(
        lambda r: calc_acos(r["spend"], r.get("direct_sales", 0), r.get("indirect_sales", 0)), axis=1)
    camp_df["_tgt"] = camp_df.apply(
        lambda r: get_target_acos(cfg, r["campaign"], r.get("ad_type", "SP")), axis=1)
    camp_df["_over"] = camp_df["_acos_real"] > camp_df["_tgt"]
    camp_df = camp_df.sort_values(
        ["product_line", "_over", "spend"],
        ascending=[True, False, False]
    ).reset_index(drop=True)

    rows = []
    for _, r in camp_df.iterrows():
        spend = _safe(r.get("spend", 0))
        cvr = _safe(r.get("cvr", 0))
        at = str(r.get("ad_type", "SP"))
        status = str(r.get("status", ""))
        bstrat = str(r.get("bid_strategy", ""))
        name = str(r.get("campaign", ""))
        pl = get_product_line(cfg, name)

        tgt = get_target_acos(cfg, name, at)
        ds = _safe(r.get("direct_sales", 0))
        ids_ = _safe(r.get("indirect_sales", 0))
        ac = calc_acos(spend, ds, ids_)
        total_s = ds + ids_

        a_label = acos_status(ac, tgt)

        rows.append({
            "品线":       pl,
            "广告类型":   at,
            "活动名称":   name,
            "状态":       status,
            "花费(€)":    round(spend, 1),
            "销售额(€)":  round(total_s, 1),
            "ACoS":       f"{ac*100:.1f}%" if spend > 0 else "-",
            "目标ACoS":   f"{tgt*100:.1f}%",
            "费比状态":   a_label,
            "ROAS":       round(total_s / spend, 2) if spend > 0 else "-",
            "点击":       int(_safe(r.get("clicks", 0))),
            "订单":       int(_safe(r.get("orders", 0))),
            "CVR":        f"{cvr*100:.1f}%" if cvr > 0 else "-",
            "竞价策略":   bstrat,
        })

    col_order = [
        "品线", "广告类型", "活动名称", "状态",
        "花费(€)", "销售额(€)", "ACoS", "目标ACoS", "费比状态",
        "ROAS", "点击", "订单", "CVR", "竞价策略",
    ]
    return pd.DataFrame(rows, columns=col_order)


# ═══════════════════════════════════════════════════════════════════════════════
# Sheet 3：关键词 ROI 分析 → get_keywords_df
# ═══════════════════════════════════════════════════════════════════════════════

def get_keywords_df(kw_df, cfg):
    """
    生成关键词 ROI 分析 DataFrame（对应原 Sheet 3 "关键词ROI分析"）。

    Parameters
    ----------
    kw_df : DataFrame
        关键词报告（或搜索词报告），由 load_search_term_report() 返回。
    cfg : dict
        配置字典，由 load_config() 返回。

    Returns
    -------
    DataFrame
        列: 关键词, 花费(€), 广告收入(€), ACoS, ROAS,
            点击, 转化, CVR, CPC(€), 建议出价, 建议动作
    """
    if kw_df is None or kw_df.empty:
        return pd.DataFrame()

    glb = cfg.get("global", {}) if cfg else {}
    cur = glb.get("currency", "€")
    target = glb.get("tacos_target", 0.037)

    # 聚合（同一个关键词可能出现在多个活动/组）
    kw_col = "keyword" if "keyword" in kw_df.columns else kw_df.columns[0]
    grp = kw_df.groupby([kw_col]).agg(
        spend       = ("spend",       "sum"),
        ad_sales    = ("ad_sales",    "sum"),
        clicks      = ("clicks",      "sum"),
        orders      = ("orders",      "sum"),
        impressions = ("impressions", "sum"),
    ).reset_index()
    grp.rename(columns={kw_col: "keyword"}, inplace=True)

    grp["acos"] = grp.apply(
        lambda r: r["spend"] / r["ad_sales"] if r["ad_sales"] > 0 else 0, axis=1)
    grp["roas"] = grp.apply(
        lambda r: r["ad_sales"] / r["spend"] if r["spend"] > 0 else 0, axis=1)
    grp["cpc"] = grp.apply(
        lambda r: r["spend"] / r["clicks"] if r["clicks"] > 0 else 0, axis=1)
    grp["cvr"] = grp.apply(
        lambda r: r["orders"] / r["clicks"] if r["clicks"] > 0 else 0, axis=1)

    # 建议出价逻辑
    def suggest_bid(r):
        cpc    = r["cpc"]
        acos   = r["acos"]
        spend  = r["spend"]
        orders = r["orders"]
        clicks = r["clicks"]

        if cpc <= 0:
            return "-"
        if clicks >= 10 and orders == 0:
            new = round(cpc * 0.5, 2)
            return f"{cur}{new:.2f} (无转化→降50%)"
        if acos <= 0:
            return f"{cur}{round(cpc, 2):.2f} (维持)"
        ratio = target / acos
        if ratio < 0.8:
            new = round(cpc * ratio, 2)
            return f"{cur}{new:.2f} (超费比→降价)"
        if ratio > 1.2:
            new = round(cpc * min(ratio, 1.3), 2)
            return f"{cur}{new:.2f} (低费比→加价)"
        return f"{cur}{round(cpc, 2):.2f} (维持)"

    grp["建议出价"] = grp.apply(suggest_bid, axis=1)

    # 状态标签
    def kw_action(r):
        ac = r["acos"]
        sp = r["spend"]
        cl = r["clicks"]
        od = r["orders"]
        if sp <= 0:
            return "未消耗"
        if cl >= 10 and od == 0:
            return "无转化高点击→考虑暂停或否定"
        if ac > target * 1.5:
            return "高费比→降出价"
        if ac > target * 1.0:
            return "略超费比→小幅降价"
        if ac < target * 0.5 and sp > 0:
            return "低费比→可加价扩量"
        return "正常"

    grp["建议动作"] = grp.apply(kw_action, axis=1)

    # 排序：问题优先
    def kw_sort(r):
        if r["clicks"] >= 10 and r["orders"] == 0:
            return 0
        if r["acos"] > target * 1.5:
            return 1
        if r["acos"] > target:
            return 2
        if r["acos"] < target * 0.5 and r["spend"] > 0:
            return 3
        return 4

    grp["_sort"] = grp.apply(kw_sort, axis=1)
    grp = grp.sort_values(["_sort", "spend"], ascending=[True, False]).reset_index(drop=True)

    # 构建输出行
    rows = []
    for _, r in grp.iterrows():
        ac = r["acos"]
        rows.append({
            "关键词":           str(r["keyword"]),
            f"花费({cur})":     round(r["spend"], 2),
            f"广告收入({cur})":  round(r["ad_sales"], 2),
            "ACoS":             f"{ac*100:.1f}%" if ac > 0 else "-",
            "ROAS":             round(r["roas"], 2) if r["roas"] > 0 else "-",
            "点击":             int(r["clicks"]),
            "转化":             int(r["orders"]),
            "CVR":              f"{r['cvr']*100:.1f}%" if r["cvr"] > 0 else "-",
            f"CPC({cur})":      round(r["cpc"], 2) if r["cpc"] > 0 else "-",
            "建议出价":         r["建议出价"],
            "建议动作":         r["建议动作"],
        })

    col_order = [
        "关键词",
        f"花费({cur})", f"广告收入({cur})", "ACoS", "ROAS",
        "点击", "转化", "CVR", f"CPC({cur})",
        "建议出价", "建议动作",
    ]
    return pd.DataFrame(rows, columns=col_order)


# ═══════════════════════════════════════════════════════════════════════════════
# Sheet 4：建议否定词 → get_negatives_df
# ═══════════════════════════════════════════════════════════════════════════════

def build_negatives(st_df, cfg):
    """
    从用户搜索词报告中提取建议否定词 DataFrame。

    条件1: ACoS > 100%（30天数据）
    条件2: 包含DJI变体写法（错写/空格/错序）

    Parameters
    ----------
    st_df : DataFrame
        搜索词报告，由 load_search_term_report() 返回。
    cfg : dict
        配置字典。

    Returns
    -------
    DataFrame
        列: 否定搜索词, 否定原因, 花费, 广告收入, ACoS, 点击, 订单, 建议否定类型, 操作
    """
    if st_df is None or st_df.empty:
        return pd.DataFrame()

    # 聚合
    st_col = "search_term" if "search_term" in st_df.columns else st_df.columns[0]
    grp = st_df.groupby([st_col]).agg(
        spend    = ("spend",    "sum"),
        ad_sales = ("ad_sales", "sum"),
        clicks   = ("clicks",   "sum"),
        orders   = ("orders",   "sum"),
    ).reset_index()
    grp.rename(columns={st_col: "search_term"}, inplace=True)

    grp["acos"] = grp.apply(
        lambda r: r["spend"] / r["ad_sales"] if r["ad_sales"] > 0 else 999, axis=1)

    results = []
    for _, r in grp.iterrows():
        term   = str(r["search_term"]).strip()
        acos   = r["acos"]
        spend  = r["spend"]
        orders = r["orders"]
        clicks = r["clicks"]

        reasons = []

        # 条件1: ACoS > 100% 且有实际花费
        if acos > 1.0 and spend > 0:
            reasons.append(f"ACoS {acos*100:.0f}%（>100%）")

        # 条件2: DJI变体写法
        if is_dji_misspell(term) and term.lower() != "dji":
            reasons.append("DJI错写/错序变体")

        if reasons:
            results.append({
                "否定搜索词":     term,
                "否定原因":       " + ".join(reasons),
                "花费":           round(spend, 2),
                "广告收入":       round(r["ad_sales"], 2),
                "ACoS":           f"{acos*100:.0f}%" if acos < 10 else ">1000%",
                "点击":           int(clicks),
                "订单":           int(orders),
                "建议否定类型":   "精准否定（Exact）",
                "操作":           "复制「否定搜索词」列到领星 → 批量添加否定关键词",
            })

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    # ACoS > 100% 优先，然后DJI变体
    df["_sort"] = df["否定原因"].apply(lambda x: 0 if "ACoS" in x else 1)
    df = df.sort_values(["_sort", "花费"], ascending=[True, False]).drop(columns=["_sort"])
    return df.reset_index(drop=True)


def get_negatives_df(search_term_df, cfg=None):
    """
    生成建议否定词 DataFrame（对应原 Sheet 4 "建议否定词"）。

    Parameters
    ----------
    search_term_df : DataFrame
        搜索词报告，由 load_search_term_report() 返回。
    cfg : dict, optional
        配置字典（当前版本 build_negatives 内部可选使用）。

    Returns
    -------
    DataFrame
        列: 否定搜索词, 否定原因, 花费, 广告收入, ACoS, 点击, 订单, 建议否定类型, 操作
    """
    if cfg is None:
        cfg = {"product_lines": [], "asins": {}, "fallback_acos": 0.04}
    return build_negatives(search_term_df, cfg)


# ═══════════════════════════════════════════════════════════════════════════════
# Dashboard 分析引擎
# ═══════════════════════════════════════════════════════════════════════════════

def classify_priority(asin_row, target_acos):
    """
    根据单个 ASIN 的费比/花费/销售情况，返回优先级分类。

    Parameters
    ----------
    asin_row : dict-like
        必须包含 '合计花费', '合计销售额', '综合ACoS' 字段。
        ACoS 可以是 "12.5%" 格式的字符串或 0.125 的浮点数。
    target_acos : float
        目标 ACoS（小数形式，0.04 = 4%）。

    Returns
    -------
    dict
        { "level", "emoji", "label", "reason", "action" }
    """
    spend = _safe(asin_row.get("合计花费", 0))
    sales = _safe(asin_row.get("合计销售额", 0))

    # 解析 ACoS：支持 "12.5%" 字符串或浮点数
    acos_raw = asin_row.get("综合ACoS", 0)
    if isinstance(acos_raw, str):
        acos_raw = acos_raw.strip()
        if acos_raw in ("-", "", "nan"):
            acos_val = 0.0
        elif acos_raw.endswith("%"):
            acos_val = _safe(acos_raw.replace("%", "")) / 100
        else:
            acos_val = _safe(acos_raw)
    else:
        acos_val = _safe(acos_raw)

    # ⚪ 低优先级：无花费或极低花费 (< 50€)
    if spend < 50:
        return {
            "level": "gray",
            "emoji": "⚪",
            "label": "低优先级",
            "reason": f"花费仅 {spend:.1f}€，数据量不足以判断" if spend > 0 else "无广告花费",
            "action": "暂不处理，积累更多数据后再评估",
        }

    # 🔴 紧急处理：ACoS > target*2.5 且花费 > 500
    if acos_val > target_acos * 2.5 and spend > 500:
        return {
            "level": "red",
            "emoji": "🔴",
            "label": "紧急处理",
            "reason": f"ACoS {acos_val*100:.1f}% 远超目标 {target_acos*100:.1f}%（>{target_acos*2.5*100:.0f}%），花费 {spend:.0f}€",
            "action": "立即暂停低效广告活动，检查关键词匹配和竞价策略",
        }

    # 🟡 观察调整：ACoS > target（含 ACoS 在 target ~ target*2.5 之间，或花费 <= 500 的高 ACoS）
    if acos_val > target_acos:
        return {
            "level": "yellow",
            "emoji": "🟡",
            "label": "观察调整",
            "reason": f"ACoS {acos_val*100:.1f}% 超过目标 {target_acos*100:.1f}%，花费 {spend:.0f}€",
            "action": "降低高ACoS关键词出价，优化广告活动结构",
        }

    # 🟢 增加投入：ACoS < target 且有销售
    if acos_val <= target_acos and sales > 0:
        return {
            "level": "green",
            "emoji": "🟢",
            "label": "增加投入",
            "reason": f"ACoS {acos_val*100:.1f}% 低于目标 {target_acos*100:.1f}%，利润空间充足",
            "action": "适当提高预算和出价，扩大广告规模抢占流量",
        }

    # 默认低优先级（有花费但无销售的边缘情况）
    return {
        "level": "gray",
        "emoji": "⚪",
        "label": "低优先级",
        "reason": f"花费 {spend:.0f}€ 但无销售，需检查产品页面和广告设置",
        "action": "检查 listing 质量、库存状态和广告投放设置",
    }


def get_product_line_suggestions(overview_df, cfg):
    """
    按品线汇总费比表现，生成每条品线的优化建议。

    Parameters
    ----------
    overview_df : DataFrame
        费比总览 DataFrame（由 get_overview_df() 返回）。
    cfg : dict
        配置字典。

    Returns
    -------
    DataFrame
        列: 品线, ACoS, 目标ACoS, 花费(€), 销售额(€), 花费占比, 销售贡献占比, 建议
    """
    if overview_df is None or overview_df.empty:
        return pd.DataFrame()

    # 排除汇总行
    df = overview_df[overview_df["品线"] != "全部品线合计"].copy()
    if df.empty:
        return pd.DataFrame()

    # 解析数值列
    df["_spend"] = df["合计花费"].apply(lambda x: _safe(x))
    df["_sales"] = df["合计销售额"].apply(lambda x: _safe(x))

    # 按品线聚合
    pl_grp = df.groupby("品线").agg(
        spend=("_spend", "sum"),
        sales=("_sales", "sum"),
    ).reset_index()

    total_spend = pl_grp["spend"].sum()
    total_sales = pl_grp["sales"].sum()

    # 为每个品线查找目标 ACoS
    def _get_pl_target(pl_name):
        for pl in cfg.get("product_lines", []):
            if pl["name"] == pl_name:
                return pl.get("sp_acos", cfg.get("fallback_acos", 0.04))
        return cfg.get("fallback_acos", 0.04)

    rows = []
    for _, r in pl_grp.iterrows():
        pl_name = r["品线"]
        spend = r["spend"]
        sales = r["sales"]
        acos = spend / sales if sales > 0 else 0
        target = _get_pl_target(pl_name)

        spend_pct = spend / total_spend if total_spend > 0 else 0
        sales_pct = sales / total_sales if total_sales > 0 else 0

        # 生成建议
        if acos > target * 2:
            suggestion = "紧急优化，暂停低效广告活动"
        elif acos > target:
            suggestion = "降低高ACoS产品的SP预算，向低ACoS产品倾斜"
        elif acos > 0:
            suggestion = "表现良好，可适当增加预算抢量"
        else:
            suggestion = "无广告数据，建议开启广告测试"

        rows.append({
            "品线": pl_name,
            "ACoS": f"{acos*100:.1f}%",
            "目标ACoS": f"{target*100:.1f}%",
            "花费(€)": round(spend, 1),
            "销售额(€)": round(sales, 1),
            "花费占比": f"{spend_pct*100:.1f}%",
            "销售贡献占比": f"{sales_pct*100:.1f}%",
            "建议": suggestion,
        })

    col_order = ["品线", "ACoS", "目标ACoS", "花费(€)", "销售额(€)",
                 "花费占比", "销售贡献占比", "建议"]
    return pd.DataFrame(rows, columns=col_order)


def get_dashboard_data(overview_df, campaigns_df, cfg, budget_config=None):
    """
    Generate comprehensive dashboard data from analysis results.

    Parameters
    ----------
    overview_df : DataFrame
        费比总览 DataFrame（由 get_overview_df() 返回）。
    campaigns_df : DataFrame
        Campaign 明细 DataFrame（由 get_campaigns_df() 返回）。
    cfg : dict
        配置字典。
    budget_config : dict, optional
        Budget/target configuration with optional keys:
        - monthly_budget: float (total ad budget for the month)
        - monthly_sales_target: float (total sales target)
        - current_day: int (current day of month)
        - month_days: int (total days in month, default 30)

    Returns
    -------
    dict
        Comprehensive dashboard data with total metrics, budget progress,
        sales progress, predictions, priority products, product line
        summaries, and alerts.
    """
    result = {}

    # ── Total metrics ────────────────────────────────────────────────────────
    # 从 overview_df 的汇总行获取全局数据
    total_spend = 0.0
    total_sales = 0.0
    overall_acos = 0.0
    total_orders = 0

    if overview_df is not None and not overview_df.empty:
        summary_mask = overview_df["品线"] == "全部品线合计"
        if summary_mask.any():
            summary_row = overview_df[summary_mask].iloc[0]
            total_spend = _safe(summary_row.get("合计花费", 0))
            total_sales = _safe(summary_row.get("合计销售额", 0))
            total_orders = int(_safe(summary_row.get("合计订单", 0)))
            acos_str = summary_row.get("综合ACoS", "0")
            if isinstance(acos_str, str) and acos_str.endswith("%"):
                overall_acos = _safe(acos_str.replace("%", "")) / 100
            else:
                overall_acos = _safe(acos_str)

    result["total_spend"] = round(total_spend, 2)
    result["total_sales"] = round(total_sales, 2)
    result["overall_acos"] = round(overall_acos, 4)
    result["total_orders"] = total_orders

    # ── Budget & Sales progress + Predictions ────────────────────────────────
    if budget_config is not None:
        monthly_budget = budget_config.get("monthly_budget", 0)
        monthly_sales_target = budget_config.get("monthly_sales_target", 0)
        current_day = budget_config.get("current_day", 1)
        month_days = budget_config.get("month_days", 30)
        remaining_days = max(month_days - current_day, 0)

        # Budget progress
        if monthly_budget > 0:
            budget_spent_pct = round(total_spend / monthly_budget, 4)
            budget_remaining = round(monthly_budget - total_spend, 2)
            daily_budget_needed = round(budget_remaining / remaining_days, 2) if remaining_days > 0 else 0.0
        else:
            budget_spent_pct = 0.0
            budget_remaining = 0.0
            daily_budget_needed = 0.0

        result["budget_spent_pct"] = budget_spent_pct
        result["budget_remaining"] = budget_remaining
        result["daily_budget_needed"] = daily_budget_needed

        # Sales progress
        if monthly_sales_target > 0:
            sales_completed_pct = round(total_sales / monthly_sales_target, 4)
            sales_remaining = round(monthly_sales_target - total_sales, 2)
            daily_sales_needed = round(sales_remaining / remaining_days, 2) if remaining_days > 0 else 0.0
        else:
            sales_completed_pct = 0.0
            sales_remaining = 0.0
            daily_sales_needed = 0.0

        result["sales_completed_pct"] = sales_completed_pct
        result["sales_remaining"] = sales_remaining
        result["daily_sales_needed"] = daily_sales_needed

        # Predictions (linear extrapolation from current pace)
        if current_day > 0:
            daily_spend_rate = total_spend / current_day
            daily_sales_rate = total_sales / current_day
        else:
            daily_spend_rate = 0.0
            daily_sales_rate = 0.0

        predicted_month_end_spend = round(daily_spend_rate * month_days, 2)
        predicted_month_end_sales = round(daily_sales_rate * month_days, 2)

        result["predicted_month_end_spend"] = predicted_month_end_spend
        result["predicted_month_end_sales"] = predicted_month_end_sales
        result["will_overspend"] = predicted_month_end_spend > monthly_budget if monthly_budget > 0 else False
        result["will_meet_target"] = predicted_month_end_sales >= monthly_sales_target if monthly_sales_target > 0 else False

        # Estimated day budget runs out
        if daily_spend_rate > 0 and monthly_budget > 0:
            budget_exhaustion_day = int(monthly_budget / daily_spend_rate)
            result["budget_exhaustion_day"] = min(budget_exhaustion_day, month_days)
        else:
            result["budget_exhaustion_day"] = month_days
    else:
        # No budget config — fill with None to indicate not applicable
        for key in ("budget_spent_pct", "budget_remaining", "daily_budget_needed",
                     "sales_completed_pct", "sales_remaining", "daily_sales_needed",
                     "predicted_month_end_spend", "predicted_month_end_sales",
                     "will_overspend", "will_meet_target", "budget_exhaustion_day"):
            result[key] = None

    # ── Priority products ────────────────────────────────────────────────────
    priority_list = []
    if overview_df is not None and not overview_df.empty:
        asin_df = overview_df[overview_df["品线"] != "全部品线合计"].copy()
        for _, row in asin_df.iterrows():
            asin = row.get("ASIN", "")
            if not asin or str(asin).strip() == "":
                continue
            # 获取该 ASIN 的目标 ACoS
            asin_info = cfg.get("asins", {}).get(str(asin).upper(), {})
            if asin_info and asin_info.get("acos_override"):
                target = asin_info["acos_override"]
            else:
                # 使用品线的 SP 目标 ACoS
                pl_name = row.get("品线", "")
                target = cfg.get("fallback_acos", 0.04)
                for pl in cfg.get("product_lines", []):
                    if pl["name"] == pl_name:
                        target = pl.get("sp_acos", cfg.get("fallback_acos", 0.04))
                        break

            priority = classify_priority(row, target)
            priority_list.append({
                "ASIN": asin,
                "产品名称": row.get("产品名称", asin),
                "品线": row.get("品线", ""),
                "合计花费": _safe(row.get("合计花费", 0)),
                "合计销售额": _safe(row.get("合计销售额", 0)),
                "综合ACoS": row.get("综合ACoS", "-"),
                "目标ACoS": f"{target*100:.1f}%",
                "priority_level": priority["level"],
                "priority_emoji": priority["emoji"],
                "priority_label": priority["label"],
                "priority_reason": priority["reason"],
                "priority_action": priority["action"],
            })

    result["priority_products"] = priority_list

    # ── Product line summaries ───────────────────────────────────────────────
    pl_suggestions = get_product_line_suggestions(overview_df, cfg)
    result["product_line_summaries"] = pl_suggestions.to_dict("records") if not pl_suggestions.empty else []

    # ── Alerts: products with ACoS > 15% ─────────────────────────────────────
    alerts = []
    for item in priority_list:
        acos_str = item.get("综合ACoS", "-")
        if isinstance(acos_str, str) and acos_str.endswith("%"):
            acos_val = _safe(acos_str.replace("%", "")) / 100
        else:
            acos_val = _safe(acos_str)
        if acos_val > 0.15:
            alerts.append({
                "ASIN": item["ASIN"],
                "产品名称": item["产品名称"],
                "品线": item["品线"],
                "ACoS": acos_str,
                "花费": item["合计花费"],
                "priority": item["priority_label"],
            })
    result["alerts"] = alerts

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 一站式入口
# ═══════════════════════════════════════════════════════════════════════════════

def load_and_process(campaign_file, product_file,
                     search_term_file=None, keyword_file=None,
                     config_file=None, budget_config=None):
    """
    一站式加载所有报告并生成全部分析 DataFrame 和 Dashboard 数据。

    Parameters
    ----------
    campaign_file : str 或 BytesIO
        领星广告活动报告。
    product_file : str 或 BytesIO
        领星推广商品报告。
    search_term_file : str 或 BytesIO, optional
        领星用户搜索词报告。
    keyword_file : str 或 BytesIO, optional
        领星关键词报告。
    config_file : str 或 BytesIO, optional
        广告配置.xlsx。如果是字符串且为目录，在其中查找 "广告配置.xlsx"。
        如果为 None，则使用 campaign_file 所在目录查找（仅当 campaign_file 是路径时）。
    budget_config : dict, optional
        Budget/target configuration with optional keys:
        - monthly_budget: float (total ad budget for the month)
        - monthly_sales_target: float (total sales target)
        - current_day: int (current day of month)
        - month_days: int (total days in month, default 30)

    Returns
    -------
    dict
        {
            "overview":       DataFrame,   # 费比总览
            "campaigns":      DataFrame,   # Campaign 明细
            "keywords":       DataFrame,   # 关键词 ROI 分析
            "negatives":      DataFrame,   # 建议否定词
            "dashboard":      dict,        # Dashboard 综合数据
            "priority":       DataFrame,   # ASIN 优先级分类
            "pl_suggestions": DataFrame,   # 品线优化建议
        }
    """
    # 加载配置
    if config_file is not None:
        cfg = load_config(config_file)
    elif isinstance(campaign_file, str):
        base_dir = os.path.dirname(os.path.abspath(campaign_file))
        cfg = load_config(base_dir)
    else:
        cfg = {"product_lines": [], "asins": {}, "fallback_acos": 0.04}

    # 加载报告
    camp_df = load_campaign_report(campaign_file)
    prod_df = load_product_report(product_file)

    st_df = None
    if search_term_file is not None:
        if isinstance(search_term_file, BytesIO) or (isinstance(search_term_file, str) and os.path.isfile(search_term_file)):
            st_df = load_search_term_report(search_term_file)

    kw_df = None
    if keyword_file is not None:
        if isinstance(keyword_file, BytesIO) or (isinstance(keyword_file, str) and os.path.isfile(keyword_file)):
            kw_df = load_search_term_report(keyword_file)  # 列结构相似

    # 生成各 DataFrame
    overview_df  = get_overview_df(prod_df, cfg)
    campaigns_df = get_campaigns_df(camp_df, cfg)
    keywords_df  = get_keywords_df(kw_df, cfg)
    negatives_df = get_negatives_df(st_df, cfg)

    # Dashboard 分析
    dashboard_data = get_dashboard_data(overview_df, campaigns_df, cfg, budget_config)

    # Priority DataFrame（从 dashboard 的 priority_products 列表构建）
    priority_list = dashboard_data.get("priority_products", [])
    if priority_list:
        priority_df = pd.DataFrame(priority_list)
        # 按优先级排序：red > yellow > green > gray
        level_order = {"red": 0, "yellow": 1, "green": 2, "gray": 3}
        priority_df["_sort"] = priority_df["priority_level"].map(level_order)
        priority_df = priority_df.sort_values(
            ["_sort", "合计花费"], ascending=[True, False]
        ).drop(columns=["_sort"]).reset_index(drop=True)
    else:
        priority_df = pd.DataFrame()

    # Product line suggestions DataFrame
    pl_suggestions_df = get_product_line_suggestions(overview_df, cfg)

    return {
        "overview":       overview_df,
        "campaigns":      campaigns_df,
        "keywords":       keywords_df,
        "negatives":      negatives_df,
        "dashboard":      dashboard_data,
        "priority":       priority_df,
        "pl_suggestions": pl_suggestions_df,
    }
