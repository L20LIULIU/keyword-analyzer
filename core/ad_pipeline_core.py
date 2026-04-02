#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ad_pipeline_core.py — Web-oriented keyword analysis engine

Refactored from ad_pipeline.py v2.0 for web use.
All business logic is preserved; Excel/openpyxl styling code is removed.
Every output function returns a pandas DataFrame with Chinese column names.

Main entry point:
    load_and_analyze(own_file, comp_files, config_json=None) -> dict[str, DataFrame]
"""

import pandas as pd
import numpy as np
import json
import re
import os
import io
from datetime import datetime

VERSION = "2.0-web"

# ═══════════════════════════════════════════════════════════════════════════════
# Column remapping (Xiyou export headers -> internal names)
# ═══════════════════════════════════════════════════════════════════════════════

REMAP = {
    "关键词 (数据来源于西柚找词)": "关键词",
    "周平均关键词排名":            "关键词排名",
    "关键词排名":                  "关键词排名",
    "周平均搜索量":                "周搜索量",
    "周平均竞争难度":              "竞争难度",
    "Top3周平均点击份额":          "Top3点击份额",
    "Top3 点击份额":               "Top3点击份额",
    "Top3周平均转化份额":          "Top3转化份额",
    "Top3 转化份额":               "Top3转化份额",
}

# ═══════════════════════════════════════════════════════════════════════════════
# Difficulty scoring
# ═══════════════════════════════════════════════════════════════════════════════

DIFF_SCORE = {"简单": 30, "中等": 20, "困难": 10, "极难": 5}

# ═══════════════════════════════════════════════════════════════════════════════
# Stop words (German + English) for root clustering
# ═══════════════════════════════════════════════════════════════════════════════

_STOP = {
    "für", "mit", "und", "oder", "von", "zum", "zur", "beim", "im", "am",
    "die", "der", "das", "ein", "eine", "des", "dem", "den",
    "for", "with", "and", "or", "to", "the", "a", "an",
    "in", "on", "at", "of", "by", "is", "are", "my", "your",
}

# ═══════════════════════════════════════════════════════════════════════════════
# Utility functions
# ═══════════════════════════════════════════════════════════════════════════════


def safe_float(val, default=0.0):
    try:
        v = float(val)
        return default if np.isnan(v) else v
    except Exception:
        return default


def priority_label(score):
    if score >= 65:
        return "高"
    if score >= 35:
        return "中"
    return "低"


def extract_asin(filepath):
    """Extract ASIN from filename. Accepts file path strings or BytesIO with .name."""
    if isinstance(filepath, (io.BytesIO, io.BufferedIOBase)):
        name = getattr(filepath, "name", "unknown")
    else:
        name = os.path.basename(str(filepath))
    for part in name.replace("~", "_").split("_"):
        p = part.split(".")[0]
        if len(p) == 10 and p.upper().startswith("B0"):
            return p.upper()
    return os.path.splitext(name)[0]


# ═══════════════════════════════════════════════════════════════════════════════
# Configuration loading
# ═══════════════════════════════════════════════════════════════════════════════


def load_config(config_source=None):
    """
    Load product configuration.

    Parameters
    ----------
    config_source : str, dict, BytesIO, or None
        - str: file path to products_config.json
        - dict: already-parsed config
        - BytesIO: JSON content as bytes
        - None: attempt auto-discovery in script directory, else return None
    """
    if config_source is None:
        # Try auto-discovery next to this module
        auto_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "products_config.json"
        )
        if os.path.isfile(auto_path):
            with open(auto_path, encoding="utf-8") as f:
                return json.load(f)
        return None

    if isinstance(config_source, dict):
        return config_source

    if isinstance(config_source, (io.BytesIO, io.BufferedIOBase)):
        config_source.seek(0)
        return json.load(config_source)

    if isinstance(config_source, str) and os.path.isfile(config_source):
        with open(config_source, encoding="utf-8") as f:
            return json.load(f)

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1 — Data loading (accepts file paths OR BytesIO)
# ═══════════════════════════════════════════════════════════════════════════════


def load_file(source):
    """
    Load a Xiyou keyword export Excel file.

    Parameters
    ----------
    source : str or BytesIO
        File path or in-memory bytes object from a web upload.
    """
    try:
        df = pd.read_excel(source, sheet_name="关键词反查结果")
    except Exception:
        if isinstance(source, (io.BytesIO, io.BufferedIOBase)):
            source.seek(0)
        df = pd.read_excel(source, sheet_name=0)

    df = df.rename(columns=REMAP)

    if "关键词" not in df.columns:
        for col in list(df.columns):
            if "关键词" in col and col != "关键词":
                df = df.rename(columns={col: "关键词"})
                break

    df["关键词"] = df["关键词"].astype(str).str.strip().str.lower()
    df = df[df["关键词"].notna() & (df["关键词"] != "nan") & (df["关键词"] != "")]
    return df.drop_duplicates(subset=["关键词"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Step 2 — Keyword analysis modules
# ═══════════════════════════════════════════════════════════════════════════════

# --- Module A: Listing coverage ---


def listing_coverage(rank):
    r = safe_float(rank, 999)
    if r >= 999:
        return "未覆盖"
    if r > 48:
        return "弱覆盖(P3+)"
    if r > 16:
        return "中等覆盖(P2)"
    return "强覆盖(P1)"


# --- Module B: Bid suggestion ---


def suggest_bid(cpc, sp_rank=None):
    cpc_val = safe_float(cpc, 0)
    if cpc_val <= 0:
        return "-"
    sp = safe_float(sp_rank, 999)
    if sp >= 999:
        mult, note = 1.3, "新进入×1.3"
    elif sp > 8:
        mult, note = 1.2, "提价×1.2"
    elif sp > 4:
        mult, note = 1.1, "微调×1.1"
    else:
        mult, note = 1.0, "维持"
    return f"€{cpc_val * mult:.2f}({note})"


# --- Module C: Traffic source structure ---


def parse_traffic_sources(share_str):
    if not share_str or (isinstance(share_str, float) and np.isnan(share_str)):
        return {}
    result = {}
    for part in str(share_str).split(","):
        m = re.match(r"\s*([^:]+):\s*([\d.]+)%", part.strip())
        if m:
            result[m.group(1).strip()] = float(m.group(2))
    return result


def traffic_source_label(share_str):
    src = parse_traffic_sources(share_str)
    if not src:
        return "-"
    parts = sorted(src.items(), key=lambda x: x[1], reverse=True)
    return "  ".join(f"{s}:{p:.0f}%" for s, p in parts if p >= 5)


def comp_source_insight(own_share_str, comp_share_str):
    own = parse_traffic_sources(own_share_str)
    comp = parse_traffic_sources(comp_share_str)
    if not comp:
        return "-"
    insights = []

    comp_sb = comp.get("SB", 0) + comp.get("SBV", 0)
    own_sb = own.get("SB", 0) + own.get("SBV", 0)
    if comp_sb >= 15 and own_sb < 5:
        insights.append(f"竞品SB/SBV占{comp_sb:.0f}%而本品几乎没有→开通SB/SBV广告")
    elif comp_sb >= 10 and comp_sb > own_sb + 8:
        insights.append(f"竞品SB占比比本品高{comp_sb - own_sb:.0f}%→加大SB投入")

    comp_sp = comp.get("SP", 0)
    own_sp = own.get("SP", 0)
    if comp_sp >= 20 and comp_sp > own_sp + 10:
        insights.append(f"竞品SP占{comp_sp:.0f}%而本品仅{own_sp:.0f}%→提高SP出价/预算")

    comp_nat = comp.get("自然", 0)
    own_nat = own.get("自然", 0)
    if comp_nat > own_nat + 20:
        insights.append(f"竞品自然流量比本品高{comp_nat - own_nat:.0f}%→提升Listing权重")

    comp_ac = comp.get("AC", 0)
    own_ac = own.get("AC", 0)
    if comp_ac >= 10 and own_ac < 3:
        insights.append(f"竞品AC占{comp_ac:.0f}%→优化Listing类目节点相关性")

    return " | ".join(insights) if insights else "流量结构相近，维持现有策略"


# --- Module D: Match type suggestion ---


def suggest_match_type(keyword, vol, diff):
    tokens = str(keyword).strip().split()
    n = len(tokens)
    vol_v = safe_float(vol)
    if diff in ("困难", "极难"):
        return "精准匹配（竞争激烈→控成本）"
    if n == 1:
        return "广泛+词组 独立Campaign" if vol_v >= 5000 else "词组+精准匹配"
    if n == 2:
        return "词组匹配 独立AdGroup" if vol_v >= 3000 else "精准匹配"
    if n == 3:
        return "精准匹配"
    return "精准+Auto广告（长尾词）"


# --- Module E: ROI index ---


def _roi_raw(cvr, cpc, vol):
    cvr_v = safe_float(cvr)
    cpc_v = safe_float(cpc)
    vol_v = safe_float(vol)
    if cvr_v <= 0 or cpc_v <= 0 or vol_v <= 0:
        return 0.0
    return (cvr_v / cpc_v) * np.log1p(vol_v)


def _global_roi_max(*dfs):
    max_val = 0.0
    for df in dfs:
        if df is None or df.empty or "点击转化率(均值)" not in df.columns:
            continue
        vc = next(
            (c for c in ["周搜索量_本品", "周搜索量"] if c in df.columns), None
        )
        if not vc:
            continue
        vals = df.apply(
            lambda r: _roi_raw(
                r.get("点击转化率(均值)"), r.get("CPC建议竞价(€)"), r.get(vc)
            ),
            axis=1,
        )
        max_val = max(max_val, vals.max())
    return max_val if max_val > 0 else 1.0


def add_roi_columns(df, vol_col, global_max):
    if (
        df is None
        or df.empty
        or "点击转化率(均值)" not in df.columns
        or "CPC建议竞价(€)" not in df.columns
    ):
        return df
    vc = vol_col if vol_col in df.columns else "周搜索量"
    df = df.copy()
    raws = df.apply(
        lambda r: _roi_raw(
            r.get("点击转化率(均值)"), r.get("CPC建议竞价(€)"), r.get(vc)
        ),
        axis=1,
    )
    df["ROI指数"] = (raws / global_max * 100).clip(0, 100).round(1)
    df["性价比"] = df["ROI指数"].apply(
        lambda x: "高性价比" if x >= 65 else ("中性价比" if x >= 35 else "低性价比")
    )
    return df


# --- Module F: Root clustering ---


def extract_root(keyword, n=2):
    tokens = str(keyword).lower().strip().split()
    filtered = [t for t in tokens if t not in _STOP and len(t) > 1]
    return (
        " ".join(filtered[:n])
        if filtered
        else (tokens[0] if tokens else "other")
    )


def _campaign_advice(row):
    max_v = safe_float(row.get("最大搜索量"))
    count = int(row.get("关键词数量", 1))
    total = safe_float(row.get("总周搜索量"))
    if max_v >= 10000:
        return "独立Campaign + 精准/词组双AdGroup（核心高量词族）"
    if max_v >= 3000 and count >= 3:
        return "独立AdGroup（词组匹配为主）"
    if total >= 2000:
        return "合并AdGroup（精准匹配，控成本）"
    return "加入Auto广告组低成本探索"


# --- Opportunity scoring & action suggestions ---


def _vol_score(vol, max_vol, weight=40):
    return (min(vol, max_vol) / max_vol * weight) if max_vol > 0 else 0


def score_comp_only(row, max_vol):
    vol = safe_float(row.get("周搜索量"))
    diff = str(row.get("竞争难度档位", "") or "")
    traf = safe_float(row.get("流量"))
    return round(
        min(
            100,
            _vol_score(vol, max_vol, 40)
            + DIFF_SCORE.get(diff, 18)
            + min(30, traf / 200),
        ),
        1,
    )


def score_rank_gap(row, max_vol):
    vol = safe_float(row.get("周搜索量_本品", row.get("周搜索量")))
    gap = safe_float(row.get("排名差距"))
    diff = str(
        row.get("竞争难度档位_本品", row.get("竞争难度档位", "")) or ""
    )
    return round(
        min(
            100,
            _vol_score(vol, max_vol, 40)
            + min(35, gap * 1.2)
            + DIFF_SCORE.get(diff, 12),
        ),
        1,
    )


def score_traffic_gap(row, max_vol):
    vol = safe_float(row.get("周搜索量_本品", row.get("周搜索量")))
    gap = safe_float(row.get("流量获得率差距"))
    diff = str(
        row.get("竞争难度档位_本品", row.get("竞争难度档位", "")) or ""
    )
    return round(
        min(
            100,
            _vol_score(vol, max_vol, 40)
            + min(35, gap * 150)
            + DIFF_SCORE.get(diff, 12),
        ),
        1,
    )


def score_own_potential(row, max_vol):
    vol = safe_float(row.get("周搜索量"))
    rank = safe_float(row.get("自然排名"), 999)
    diff = str(row.get("竞争难度档位", "") or "")
    return round(
        min(
            100,
            _vol_score(vol, max_vol, 40)
            + min(35, max(0, (rank - 16) / 2))
            + DIFF_SCORE.get(diff, 12),
        ),
        1,
    )


def get_action(row, analysis_type):
    actions = []
    vol = safe_float(row.get("周搜索量", row.get("周搜索量_本品")))
    diff = str(
        row.get("竞争难度档位", row.get("竞争难度档位_本品", "")) or ""
    )
    listing = str(row.get("Listing覆盖", "") or "")

    if analysis_type == "comp_only":
        actions.append("将关键词补充至Listing标题/五点")
        if diff in ("简单", "中等", "", "nan"):
            actions.append("Listing更新后新建SP精准广告")
            if vol >= 5000:
                actions.append("同步布局SB品牌广告")
        else:
            actions.append("小预算测试SP广告（竞争激烈，控制风险）")

    elif analysis_type == "rank_gap":
        own_sp = row.get("SP广告排名_本品", row.get("SP广告排名"))
        gap = safe_float(row.get("排名差距"))
        has_sp = not (pd.isna(own_sp) or safe_float(own_sp, 999) >= 999)
        if "未覆盖" in listing or "弱覆盖" in listing:
            actions.append("【先做】优化Listing关键词布局以建立A9相关性")
        elif "中等覆盖" in listing:
            actions.append("优化Listing标题/五点关键词密度")
        if not has_sp:
            actions.append("新建SP广告（精准匹配）")
        elif safe_float(own_sp, 999) > 8:
            actions.append("提高SP广告出价（当前排位靠后）")
        else:
            actions.append("维持SP出价，持续积累排名权重")
        if gap > 30:
            actions.append("全面优化Listing相关性")
        if vol >= 5000:
            actions.append("加投SB广告提升品牌曝光")

    elif analysis_type == "traffic_gap":
        own_rate = safe_float(
            row.get("流量获得率_本品", row.get("流量获得率"))
        )
        comp_rate = safe_float(row.get("流量获得率_竞品"))
        own_sp = row.get("SP广告排名_本品", row.get("SP广告排名"))
        has_sp = not (pd.isna(own_sp) or safe_float(own_sp, 999) >= 999)
        if "未覆盖" in listing or "弱覆盖" in listing:
            actions.append("【先做】优化Listing覆盖度（当前流量来源不足）")
        if not has_sp:
            actions.append("新建SP广告")
        else:
            actions.append("提高SP广告出价/预算")
        if comp_rate > own_rate * 2:
            actions.append("全面检查Listing相关性与主图CTR")
        elif comp_rate > own_rate * 1.5:
            actions.append("优化广告创意提升点击率")
        if vol >= 5000:
            actions.append("布局SBV视频广告增加曝光渠道")

    elif analysis_type == "own_potential":
        own_rank = safe_float(row.get("自然排名"), 999)
        own_sp = row.get("SP广告排名")
        has_sp = not (pd.isna(own_sp) or safe_float(own_sp, 999) >= 999)
        if "未覆盖" in listing or "弱覆盖" in listing:
            actions.append("【先做】强化Listing关键词布局")
        if not has_sp:
            actions.append("新建SP广告（精准匹配）")
        elif safe_float(own_sp, 999) > 8:
            actions.append("提高SP广告出价")
        if own_rank > 48:
            actions.append("重点突破（当前第3页+，广告拉动自然排名）")
        elif own_rank > 16:
            actions.append("持续SP投入积累自然权重（当前第2页）")
        if vol >= 10000 and diff in ("简单", "中等"):
            actions.append("高价值突破机会（高量+低竞争）")

    return " | ".join(actions) if actions else "持续监控"


# --- Internal helpers ---


def _pick_cols(df, cols):
    return [c for c in cols if c in df.columns]


def _agg_comp(comp_raw):
    def join_asins(x):
        return ", ".join(x.dropna().astype(str).unique())

    numeric_max = [
        "流量", "自然流量", "广告流量",
        "流量获得率", "自然流量获得率", "广告流量获得率",
        "周搜索量", "Top3点击份额", "Top3转化份额",
    ]
    rank_min = ["自然排名", "SP广告排名"]
    num_mean = ["CPC建议竞价(€)", "点击转化率(均值)", "竞争难度"]
    str_first = ["翻译", "词标签", "竞争难度档位", "展示位置", "展示位流量份额"]

    agg = {}
    for col in comp_raw.columns:
        if col == "关键词":
            continue
        if col == "来源ASIN":
            agg[col] = join_asins
        elif col in rank_min:
            agg[col] = "min"
        elif col in numeric_max:
            agg[col] = "max"
        elif col in num_mean:
            agg[col] = "mean"
        elif col in str_first:
            agg[col] = "first"
        elif comp_raw[col].dtype in (np.float64, np.int64, float, int):
            agg[col] = "max"
        else:
            agg[col] = "first"

    return comp_raw.groupby("关键词").agg(agg).reset_index()


def _get_stats(df):
    if df is None or df.empty or "优先级" not in df.columns:
        return {"total": 0, "high": 0, "medium": 0, "low": 0, "top_action": "-"}
    vc = df["优先级"].value_counts()
    top = df.iloc[0]["建议动作"].split(" | ")[0] if len(df) > 0 else "-"
    return {
        "total": len(df),
        "high": int(vc.get("高", 0)),
        "medium": int(vc.get("中", 0)),
        "low": int(vc.get("低", 0)),
        "top_action": top,
    }


def _round_df(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round(3)
    return df


def _score_and_sort(df, score_fn, atype, vol_col):
    if df is None or df.empty:
        return df
    max_vol = df[vol_col].fillna(0).max() if vol_col in df.columns else 1
    if max_vol == 0:
        max_vol = 1
    df["机会评分"] = df.apply(lambda r: score_fn(r, max_vol), axis=1)
    df["优先级"] = df["机会评分"].apply(priority_label)
    df["建议动作"] = df.apply(lambda r: get_action(r, atype), axis=1)
    return df.sort_values("机会评分", ascending=False).reset_index(drop=True)


def _add_match_type(df, vol_col, diff_col):
    if df is None or df.empty:
        return df
    vc = vol_col if vol_col in df.columns else "周搜索量"
    dc = diff_col if diff_col in df.columns else "竞争难度档位"
    df["建议匹配方式"] = df.apply(
        lambda r: suggest_match_type(
            r["关键词"], r.get(vc, 0), str(r.get(dc, "") or "")
        ),
        axis=1,
    )
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# Step 2 — Main analysis flow
# ═══════════════════════════════════════════════════════════════════════════════


def run_keyword_analysis(own_file, competitor_files):
    """
    Run the full keyword gap analysis.

    Parameters
    ----------
    own_file : str or BytesIO
    competitor_files : list of str or BytesIO

    Returns
    -------
    dict with keys: own_asin, comp_asins, own_df, comp_agg,
                    comp_only, rank_gap, traf_gap, own_pot,
                    vol_col_rg, vol_col_tg
    """
    own_asin = extract_asin(own_file)
    comp_asins = [extract_asin(f) for f in competitor_files]

    own_df = load_file(own_file)

    comp_parts = []
    for f, asin in zip(competitor_files, comp_asins):
        cdf = load_file(f)
        cdf["来源ASIN"] = asin
        comp_parts.append(cdf)

    comp_raw = pd.concat(comp_parts, ignore_index=True)
    comp_agg = _agg_comp(comp_raw)

    own_set = set(own_df["关键词"])
    comp_set = set(comp_agg["关键词"])

    # Base fields
    if "自然排名" in own_df.columns:
        own_df["Listing覆盖"] = own_df["自然排名"].apply(listing_coverage)
    if "展示位流量份额" in own_df.columns:
        own_df["本品流量来源"] = own_df["展示位流量份额"].apply(
            traffic_source_label
        )
    if "展示位流量份额" in comp_agg.columns:
        comp_agg["竞品流量来源"] = comp_agg["展示位流量份额"].apply(
            traffic_source_label
        )

    # Analysis 1: Competitor-only keywords
    comp_only = comp_agg[~comp_agg["关键词"].isin(list(own_set))].copy()
    comp_only["Listing覆盖"] = "未覆盖"
    comp_only["建议出价(€)"] = comp_only.apply(
        lambda r: suggest_bid(r.get("CPC建议竞价(€)")), axis=1
    )
    comp_only = _add_match_type(comp_only, "周搜索量", "竞争难度档位")
    comp_only = _score_and_sort(
        comp_only, score_comp_only, "comp_only", "周搜索量"
    )

    # Shared keywords merge
    own_common = own_df[own_df["关键词"].isin(list(comp_set))].copy()
    comp_common = comp_agg[comp_agg["关键词"].isin(list(own_set))].copy()

    merge_cols = _pick_cols(
        comp_common,
        [
            "关键词", "自然排名", "SP广告排名", "流量获得率",
            "周搜索量", "竞争难度档位", "展示位流量份额",
        ],
    )
    merged = own_common.merge(
        comp_common[merge_cols],
        on="关键词",
        suffixes=("_本品", "_竞品"),
        how="inner",
    )

    own_rank_col = (
        "自然排名_本品" if "自然排名_本品" in merged.columns else "自然排名"
    )
    comp_rank_col = (
        "自然排名_竞品" if "自然排名_竞品" in merged.columns else None
    )
    own_rate_col = (
        "流量获得率_本品" if "流量获得率_本品" in merged.columns else "流量获得率"
    )
    comp_rate_col = (
        "流量获得率_竞品" if "流量获得率_竞品" in merged.columns else None
    )
    own_sp_col = (
        "SP广告排名_本品" if "SP广告排名_本品" in merged.columns else "SP广告排名"
    )

    merged["排名差距"] = (
        merged[own_rank_col].fillna(999) - merged[comp_rank_col].fillna(999)
        if comp_rank_col and comp_rank_col in merged.columns
        else 0
    )
    merged["流量获得率差距"] = (
        merged[comp_rate_col].fillna(0) - merged[own_rate_col].fillna(0)
        if comp_rate_col and comp_rate_col in merged.columns
        else 0
    )
    merged["Listing覆盖"] = merged[own_rank_col].apply(listing_coverage)

    own_share_col = (
        "展示位流量份额_本品"
        if "展示位流量份额_本品" in merged.columns
        else "展示位流量份额"
    )
    comp_share_col = (
        "展示位流量份额_竞品"
        if "展示位流量份额_竞品" in merged.columns
        else None
    )

    if own_share_col in merged.columns:
        merged["本品流量来源"] = merged[own_share_col].apply(
            traffic_source_label
        )
    if comp_share_col and comp_share_col in merged.columns:
        merged["竞品流量来源"] = merged[comp_share_col].apply(
            traffic_source_label
        )
        merged["流量来源差距"] = merged.apply(
            lambda r: comp_source_insight(
                r.get(own_share_col), r.get(comp_share_col)
            ),
            axis=1,
        )

    merged["建议出价(€)"] = merged.apply(
        lambda r: suggest_bid(r.get("CPC建议竞价(€)"), r.get(own_sp_col)),
        axis=1,
    )

    _vol4m = (
        "周搜索量_本品" if "周搜索量_本品" in merged.columns else "周搜索量"
    )
    _diff4m = (
        "竞争难度档位_本品"
        if "竞争难度档位_本品" in merged.columns
        else "竞争难度档位"
    )
    merged = _add_match_type(merged, _vol4m, _diff4m)

    # Analysis 2: Rank gap keywords
    rank_gap = merged[merged["排名差距"] > 8].copy()
    vol_col_rg = (
        "周搜索量_本品" if "周搜索量_本品" in rank_gap.columns else "周搜索量"
    )
    rank_gap = _score_and_sort(rank_gap, score_rank_gap, "rank_gap", vol_col_rg)

    # Analysis 3: Traffic gap keywords
    traf_gap = merged[merged["流量获得率差距"] > 0.1].copy()
    vol_col_tg = (
        "周搜索量_本品" if "周搜索量_本品" in traf_gap.columns else "周搜索量"
    )
    traf_gap = _score_and_sort(
        traf_gap, score_traffic_gap, "traffic_gap", vol_col_tg
    )

    # Analysis 4: Own high-potential keywords
    median_vol = (
        own_df["周搜索量"].fillna(0).median()
        if "周搜索量" in own_df.columns
        else 0
    )
    own_pot = own_df[
        (own_df["自然排名"].fillna(999) > 16)
        & (own_df["周搜索量"].fillna(0) > median_vol)
    ].copy()
    own_pot["建议出价(€)"] = own_pot.apply(
        lambda r: suggest_bid(r.get("CPC建议竞价(€)"), r.get("SP广告排名")),
        axis=1,
    )
    own_pot = _add_match_type(own_pot, "周搜索量", "竞争难度档位")
    own_pot = _score_and_sort(
        own_pot, score_own_potential, "own_potential", "周搜索量"
    )

    # ROI global normalization
    _roi_max = _global_roi_max(comp_only, rank_gap, traf_gap, own_pot)
    comp_only = add_roi_columns(comp_only, "周搜索量", _roi_max)
    rank_gap = add_roi_columns(rank_gap, vol_col_rg, _roi_max)
    traf_gap = add_roi_columns(traf_gap, vol_col_tg, _roi_max)
    own_pot = add_roi_columns(own_pot, "周搜索量", _roi_max)

    return {
        "own_asin": own_asin,
        "comp_asins": comp_asins,
        "own_df": own_df,
        "comp_agg": comp_agg,
        "comp_only": comp_only,
        "rank_gap": rank_gap,
        "traf_gap": traf_gap,
        "own_pot": own_pot,
        "vol_col_rg": vol_col_rg,
        "vol_col_tg": vol_col_tg,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Step 3 — Product budget planning
# ═══════════════════════════════════════════════════════════════════════════════


def calc_products(cfg):
    """
    Calculate per-product budget planning.

    Returns
    -------
    (product_rows: list[dict], total_rev: float, total_bud: float)
    """
    if not cfg or "products" not in cfg:
        return [], 0, 0

    glb = cfg.get("global", {})
    month_days = glb.get("month_days", 30)
    split_by_role = cfg.get(
        "budget_split_by_role",
        {
            "核心套装": {"SP": 0.55, "SB": 0.35, "SBV": 0.10},
            "新品": {"SP": 0.75, "SB": 0.20, "SBV": 0.05},
            "配件": {"SP": 0.70, "SB": 0.20, "SBV": 0.10},
        },
    )

    rows = []
    for p in cfg["products"]:
        name = p["name"]
        asin = p["asin"]
        pn = p["price_normal"]
        pp = p["price_promo"]
        target = p["target_qty"]
        pd_days = p["promo_days"]
        tacos = p["tacos"]
        role = p["role"]

        nd = month_days - pd_days
        x = target / (pd_days * 1.6 + nd)
        xp = x * 1.6
        qty_promo = pd_days * xp
        qty_normal = nd * x
        rev_promo = qty_promo * pp
        rev_normal = qty_normal * pn
        rev_total = rev_promo + rev_normal
        ad_budget = rev_total * tacos
        roas = 1 / tacos
        disc_pct = (pn - pp) / pn * 100

        sp_r = split_by_role.get(role, {}).get("SP", 0.60)
        sb_r = split_by_role.get(role, {}).get("SB", 0.30)
        sbv_r = split_by_role.get(role, {}).get("SBV", 0.10)

        rows.append(
            {
                "产品名称": name,
                "ASIN": asin,
                "产品角色": role,
                "平日RRP(€)": pn,
                "促销RRP(€)": pp,
                "折扣幅度": round(disc_pct, 1),
                "平日日销量": round(x, 1),
                "促销日销量": round(xp, 1),
                "月总销量": round(target, 0),
                "月总销售额(€)": round(rev_total, 0),
                "收入占比": 0,
                "建议费比": round(tacos * 100, 1),
                "月广告预算(€)": round(ad_budget, 0),
                "目标ROAS": round(roas, 1),
                "SP预算(€)": round(ad_budget * sp_r, 0),
                "SB预算(€)": round(ad_budget * sb_r, 0),
                "SBV预算(€)": round(ad_budget * sbv_r, 0),
            }
        )

    total_rev = sum(r["月总销售额(€)"] for r in rows)
    total_bud = sum(r["月广告预算(€)"] for r in rows)
    for r in rows:
        r["收入占比"] = (
            round(r["月总销售额(€)"] / total_rev * 100, 1) if total_rev > 0 else 0
        )

    return rows, total_rev, total_bud


# ═══════════════════════════════════════════════════════════════════════════════
# Step 4 — Keyword priority matrix (ROI x Traffic x Opportunity Score)
# ═══════════════════════════════════════════════════════════════════════════════


def build_kw_priority_matrix(kw_data, cfg, focus_asin=None):
    """
    Combine all 4 analysis dimensions into a single priority-ranked DataFrame.
    """
    relevant_terms = cfg.get("relevant_terms", []) if cfg else []

    def is_relevant(kw):
        kw_l = str(kw).lower()
        return not relevant_terms or any(t in kw_l for t in relevant_terms)

    frames = []

    comp_only = kw_data["comp_only"]
    if comp_only is not None and not comp_only.empty:
        df1 = comp_only.copy()
        df1["来源维度"] = "竞品独有词"
        frames.append(df1)

    rank_gap = kw_data["rank_gap"]
    if rank_gap is not None and not rank_gap.empty:
        df2 = rank_gap.copy()
        df2["来源维度"] = "排名差距词"
        vc = (
            "周搜索量_本品" if "周搜索量_本品" in df2.columns else "周搜索量"
        )
        df2 = df2.rename(columns={vc: "周搜索量"}) if vc != "周搜索量" else df2
        frames.append(df2)

    traf_gap = kw_data["traf_gap"]
    if traf_gap is not None and not traf_gap.empty:
        df3 = traf_gap.copy()
        df3["来源维度"] = "流量获取差距"
        vc = (
            "周搜索量_本品" if "周搜索量_本品" in df3.columns else "周搜索量"
        )
        df3 = df3.rename(columns={vc: "周搜索量"}) if vc != "周搜索量" else df3
        frames.append(df3)

    own_pot = kw_data["own_pot"]
    if own_pot is not None and not own_pot.empty:
        df4 = own_pot[own_pot["关键词"].apply(is_relevant)].copy()
        df4["来源维度"] = "本品高潜力词"
        frames.append(df4)

    if not frames:
        return pd.DataFrame()

    all_kw = pd.concat(frames, ignore_index=True)
    all_kw = all_kw.drop_duplicates(subset=["关键词"])

    for col in [
        "周搜索量", "机会评分", "ROI指数", "CPC建议竞价(€)", "点击转化率(均值)",
    ]:
        if col in all_kw.columns:
            all_kw[col] = pd.to_numeric(all_kw[col], errors="coerce")

    max_vol = all_kw["周搜索量"].fillna(0).max() or 1
    all_kw["流量得分"] = (
        all_kw["周搜索量"].fillna(0) / max_vol * 100
    ).round(1)
    all_kw["综合优先分"] = (
        all_kw["机会评分"].fillna(0) * 0.50
        + all_kw["ROI指数"].fillna(0) * 0.30
        + all_kw["流量得分"].fillna(0) * 0.20
    ).round(1)

    # Per-keyword TACOS estimate
    glb = cfg.get("global", {}) if cfg else {}
    focus_asin = focus_asin or glb.get("focus_asin", "")
    promo_price = 246  # default
    if cfg and focus_asin:
        for p in cfg.get("products", []):
            if p["asin"] == focus_asin:
                promo_price = p["price_promo"]
                break

    def tacos_risk(row):
        cpc = float(row.get("CPC建议竞价(€)", 0) or 0)
        cvr = float(row.get("点击转化率(均值)", 0) or 0)
        if cvr <= 0 or cpc <= 0:
            return "数据不足"
        kw_tacos = cpc / (cvr * promo_price)
        if kw_tacos <= 0.04:
            return f"优质({kw_tacos*100:.1f}%)"
        elif kw_tacos <= 0.08:
            return f"合理({kw_tacos*100:.1f}%)"
        elif kw_tacos <= 0.15:
            return f"偏高({kw_tacos*100:.1f}%)"
        else:
            return f"高风险({kw_tacos*100:.1f}%)"

    all_kw["词级费比估算"] = all_kw.apply(tacos_risk, axis=1)

    keep_cols = [
        c
        for c in [
            "关键词", "翻译", "来源维度",
            "周搜索量", "流量得分", "Listing覆盖",
            "自然排名", "自然排名_本品", "SP广告排名", "SP广告排名_本品",
            "竞争难度档位", "竞争难度档位_本品",
            "本品流量来源", "竞品流量来源", "流量来源差距",
            "CPC建议竞价(€)", "建议出价(€)", "点击转化率(均值)",
            "ROI指数", "性价比", "词级费比估算",
            "建议匹配方式", "机会评分", "综合优先分", "优先级", "建议动作",
        ]
        if c in all_kw.columns
    ]

    return (
        all_kw[keep_cols]
        .sort_values("综合优先分", ascending=False)
        .reset_index(drop=True)
    )


# ═══════════════════════════════════════════════════════════════════════════════
# DataFrame output functions (replacing Excel sheet writers)
# ═══════════════════════════════════════════════════════════════════════════════


def get_summary_df(kw_data, stats, product_rows=None, total_rev=0, total_bud=0):
    """
    Sheet 1: Executive summary as a DataFrame.

    Returns a DataFrame with columns:
        分析维度, 关键词总数, 高优先级, 中优先级, 低优先级, 首要行动建议
    """
    own_asin = kw_data["own_asin"]
    comp_asins = kw_data["comp_asins"]

    rows = []
    for label, key in [
        ("竞品独有词（需布局）", "comp_only"),
        ("排名差距词（需提升）", "rank_gap"),
        ("流量获取差距词", "traffic_gap"),
        ("本品高潜力词", "own_potential"),
    ]:
        d = stats.get(key, {})
        rows.append(
            {
                "分析维度": label,
                "关键词总数": d.get("total", 0),
                "高优先级": d.get("high", 0),
                "中优先级": d.get("medium", 0),
                "低优先级": d.get("low", 0),
                "首要行动建议": d.get("top_action", "-"),
            }
        )

    df = pd.DataFrame(rows)
    # Attach metadata as DataFrame attrs for callers that need them
    df.attrs["own_asin"] = own_asin
    df.attrs["comp_asins"] = comp_asins
    df.attrs["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    df.attrs["version"] = VERSION
    if product_rows:
        df.attrs["total_rev"] = total_rev
        df.attrs["total_bud"] = total_bud
    return df


def get_comp_only_df(comp_only):
    """Sheet 2: Competitor-only keywords (竞品独有词)."""
    if comp_only is None or comp_only.empty:
        return pd.DataFrame()
    cols = _pick_cols(
        comp_only,
        [
            "关键词", "翻译", "词标签", "来源ASIN",
            "周搜索量", "流量", "自然排名", "SP广告排名",
            "竞争难度档位", "CPC建议竞价(€)", "点击转化率(均值)",
            "Listing覆盖", "建议出价(€)", "建议匹配方式",
            "ROI指数", "性价比", "竞品流量来源",
            "机会评分", "优先级", "建议动作",
        ],
    )
    return _round_df(
        comp_only[cols].copy(),
        ["机会评分", "CPC建议竞价(€)", "点击转化率(均值)", "ROI指数"],
    )


def get_rank_gap_df(rank_gap):
    """Sheet 3: Rank gap keywords (排名差距词)."""
    if rank_gap is None or rank_gap.empty:
        return pd.DataFrame()
    cols = _pick_cols(
        rank_gap,
        [
            "关键词", "翻译", "周搜索量_本品", "周搜索量",
            "Listing覆盖",
            "自然排名_本品", "自然排名_竞品", "排名差距",
            "SP广告排名_本品", "SP广告排名_竞品",
            "竞争难度档位_本品", "竞争难度档位",
            "本品流量来源", "竞品流量来源", "流量来源差距",
            "建议出价(€)", "建议匹配方式",
            "ROI指数", "性价比",
            "机会评分", "优先级", "建议动作",
        ],
    )
    return _round_df(
        rank_gap[cols].copy(), ["机会评分", "排名差距", "ROI指数"]
    )


def get_traffic_gap_df(traf_gap):
    """Sheet 4: Traffic gap keywords (流量获取差距词)."""
    if traf_gap is None or traf_gap.empty:
        return pd.DataFrame()
    cols = _pick_cols(
        traf_gap,
        [
            "关键词", "翻译", "周搜索量_本品", "周搜索量",
            "Listing覆盖",
            "流量获得率_本品", "流量获得率_竞品", "流量获得率差距",
            "自然排名_本品", "SP广告排名_本品",
            "竞争难度档位_本品",
            "本品流量来源", "竞品流量来源", "流量来源差距",
            "建议出价(€)", "建议匹配方式",
            "ROI指数", "性价比",
            "机会评分", "优先级", "建议动作",
        ],
    )
    return _round_df(
        traf_gap[cols].copy(),
        [
            "机会评分", "流量获得率_本品", "流量获得率_竞品",
            "流量获得率差距", "ROI指数",
        ],
    )


def get_own_potential_df(own_pot):
    """Sheet 5: Own high-potential keywords (本品高潜力词)."""
    if own_pot is None or own_pot.empty:
        return pd.DataFrame()
    cols = _pick_cols(
        own_pot,
        [
            "关键词", "翻译", "词标签", "周搜索量",
            "Listing覆盖",
            "自然排名", "SP广告排名",
            "流量获得率", "竞争难度档位",
            "CPC建议竞价(€)", "点击转化率(均值)",
            "本品流量来源",
            "建议出价(€)", "建议匹配方式",
            "ROI指数", "性价比",
            "机会评分", "优先级", "建议动作",
        ],
    )
    return _round_df(
        own_pot[cols].copy(),
        [
            "机会评分", "流量获得率", "CPC建议竞价(€)",
            "点击转化率(均值)", "ROI指数",
        ],
    )


def get_root_cluster_df(own_pot, comp_only, rank_gap, traf_gap):
    """Sheet 6: Root clustering for Campaign planning (词根聚类)."""
    frames = []

    def _collect(df, source, vol_col):
        if df is None or df.empty:
            return
        keep = [
            c
            for c in [
                "关键词", vol_col, "机会评分",
                "CPC建议竞价(€)", "点击转化率(均值)", "竞争难度档位",
            ]
            if c in df.columns
        ]
        tmp = df[keep].copy().rename(columns={vol_col: "周搜索量"})
        tmp["来源维度"] = source
        frames.append(tmp)

    _collect(own_pot, "本品高潜力词", "周搜索量")
    _collect(comp_only, "竞品独有词", "周搜索量")
    _collect(
        rank_gap,
        "排名差距词",
        (
            "周搜索量_本品"
            if rank_gap is not None and "周搜索量_本品" in rank_gap.columns
            else "周搜索量"
        ),
    )
    _collect(
        traf_gap,
        "流量获取差距词",
        (
            "周搜索量_本品"
            if traf_gap is not None and "周搜索量_本品" in traf_gap.columns
            else "周搜索量"
        ),
    )

    if not frames:
        return pd.DataFrame()

    all_kws = pd.concat(frames, ignore_index=True)
    all_kws = all_kws.sort_values("机会评分", ascending=False).drop_duplicates(
        subset=["关键词"]
    )
    all_kws["词根"] = all_kws["关键词"].apply(extract_root)

    def join_dims(x):
        return " + ".join(sorted(x.dropna().astype(str).unique()))

    clusters = (
        all_kws.groupby("词根")
        .agg(
            关键词数量=("关键词", "count"),
            总周搜索量=("周搜索量", "sum"),
            最大搜索量=("周搜索量", "max"),
            平均搜索量=("周搜索量", "mean"),
            最高机会评分=("机会评分", "max"),
            平均CPC=("CPC建议竞价(€)", "mean"),
            平均CVR=("点击转化率(均值)", "mean"),
            涉及分析维度=("来源维度", join_dims),
            代表关键词=("关键词", lambda x: " / ".join(list(x)[:3])),
            完整词族=("关键词", lambda x: " | ".join(list(x))),
        )
        .reset_index()
    )

    clusters["平均搜索量"] = clusters["平均搜索量"].round(0).astype(int)
    clusters["平均CPC"] = clusters["平均CPC"].round(3)
    clusters["平均CVR"] = clusters["平均CVR"].round(4)
    clusters["词族ROI均值"] = clusters.apply(
        lambda r: round(
            _roi_raw(r["平均CVR"], r["平均CPC"], r["最大搜索量"]), 4
        ),
        axis=1,
    )
    clusters["建议Campaign结构"] = clusters.apply(_campaign_advice, axis=1)
    clusters["建议匹配方式"] = clusters.apply(
        lambda r: suggest_match_type(r["词根"], r["最大搜索量"], ""), axis=1
    )
    clusters = clusters.sort_values(
        "总周搜索量", ascending=False
    ).reset_index(drop=True)

    OUT_COLS = [
        "词根", "关键词数量", "总周搜索量", "最大搜索量", "平均搜索量",
        "最高机会评分", "平均CPC", "平均CVR", "词族ROI均值",
        "建议Campaign结构", "建议匹配方式",
        "涉及分析维度", "代表关键词", "完整词族",
    ]
    return clusters[[c for c in OUT_COLS if c in clusters.columns]]


def get_priority_matrix_df(kw_data, cfg, focus_asin=None):
    """Sheet 7: Combined priority matrix (综合优先投放)."""
    return build_kw_priority_matrix(kw_data, cfg, focus_asin)


def get_budget_df(product_rows, total_rev, total_bud, cfg=None):
    """Sheet 8: Product budget planning (产品预算规划)."""
    if not product_rows:
        return pd.DataFrame()

    df = pd.DataFrame(product_rows)

    # Append totals row
    total_row = {
        "产品名称": "合计",
        "ASIN": "",
        "产品角色": "",
        "平日RRP(€)": np.nan,
        "促销RRP(€)": np.nan,
        "折扣幅度": np.nan,
        "平日日销量": np.nan,
        "促销日销量": np.nan,
        "月总销量": round(sum(r["月总销量"] for r in product_rows), 0),
        "月总销售额(€)": round(total_rev, 0),
        "收入占比": 100.0,
        "建议费比": (
            round(total_bud / total_rev * 100, 2) if total_rev > 0 else 0
        ),
        "月广告预算(€)": round(total_bud, 0),
        "目标ROAS": (
            round(total_rev / total_bud, 1) if total_bud > 0 else 0
        ),
        "SP预算(€)": round(
            sum(r["SP预算(€)"] for r in product_rows), 0
        ),
        "SB预算(€)": round(
            sum(r["SB预算(€)"] for r in product_rows), 0
        ),
        "SBV预算(€)": round(
            sum(r["SBV预算(€)"] for r in product_rows), 0
        ),
    }
    df_all = pd.concat(
        [df, pd.DataFrame([total_row])], ignore_index=True
    )
    return df_all


def get_traffic_source_df(traf_gap):
    """Sheet 9: Traffic source comparison (流量来源对比)."""
    if traf_gap is None or traf_gap.empty:
        return pd.DataFrame()

    keep = [
        c
        for c in [
            "关键词", "翻译", "周搜索量_本品", "周搜索量",
            "Listing覆盖",
            "本品流量来源", "竞品流量来源",
            "流量获得率_本品", "流量获得率_竞品",
            "流量来源差距",
            "建议出价(€)", "优先级", "建议动作",
        ]
        if c in traf_gap.columns
    ]

    df = traf_gap[keep].copy()
    vc = "周搜索量_本品" if "周搜索量_本品" in df.columns else "周搜索量"
    return df.sort_values(vc, ascending=False).reset_index(drop=True)


def get_monthly_plan_df(product_rows, cfg):
    """
    Sheet 10: Monthly execution plan (月度执行计划).

    Returns a dict of DataFrames:
        {
            "预算分配": DataFrame,
            "投放节奏": DataFrame,
            "费比预警": DataFrame,
            "meta": dict with focus product info
        }
    """
    if not product_rows or not cfg:
        return {
            "预算分配": pd.DataFrame(),
            "投放节奏": pd.DataFrame(),
            "费比预警": pd.DataFrame(),
            "meta": {},
        }

    glb = cfg.get("global", {})
    focus_asin = glb.get("focus_asin", "")
    cur = glb.get("currency", "€")

    p = next(
        (r for r in product_rows if r["ASIN"] == focus_asin),
        product_rows[0],
    )

    total_bud = p["月广告预算(€)"]

    # Budget allocation table
    budget_rows = []
    for dim, key, pct_denom, desc in [
        ("SP 精准/词组广告", "SP预算(€)", total_bud, "核心关键词精准投放，保排名"),
        ("SB 品牌广告", "SB预算(€)", total_bud, "品牌词+类目词，提升曝光与品牌认知"),
        ("SBV 视频广告", "SBV预算(€)", total_bud, "视觉差异化，拦截竞品搜索"),
    ]:
        val = p[key]
        budget_rows.append(
            {
                "维度": dim,
                "月预算": val,
                "日均预算": round(val / 30, 0),
                "占比": (
                    f"{val / pct_denom * 100:.0f}%"
                    if pct_denom
                    else "-"
                ),
                "说明": desc,
            }
        )
    budget_rows.append(
        {
            "维度": "合计",
            "月预算": total_bud,
            "日均预算": round(total_bud / 30, 0),
            "占比": "100%",
            "说明": f"费比{p['建议费比']}%  目标ROAS {p['目标ROAS']}x",
        }
    )
    budget_df = pd.DataFrame(budget_rows)

    # Phased strategy table
    phases = [
        {
            "阶段": "促销预热",
            "时间": "促销前3天",
            "出价策略": "出价×1.1，提前抢位",
            "预算分配": "加大10%",
            "重点动作": "确保核心词SP排名进前5，SB素材上线",
        },
        {
            "阶段": "促销高峰",
            "时间": "促销期",
            "出价策略": "出价×1.2，防预算耗尽熄火",
            "预算分配": f"日均{cur}{round(total_bud * 0.6 / 30, 0):.0f}",
            "重点动作": "每日监控费比，ROI高词加预算",
        },
        {
            "阶段": "平日维持",
            "时间": "剩余天数",
            "出价策略": "恢复正常出价",
            "预算分配": f"日均{cur}{round(total_bud * 0.4 / 30, 0):.0f}",
            "重点动作": "持续积累自然排名权重，减少纯广告依赖",
        },
    ]
    phase_df = pd.DataFrame(phases)

    # TACOS alert rules
    alerts = [
        {
            "条件": "日费比 < 3.5%",
            "级别": "正常",
            "建议动作": "预算有余量，对高ROI词加价或扩量",
        },
        {
            "条件": "日费比 3.5%~4.5%",
            "级别": "警戒",
            "建议动作": "在目标区间内，维持现有投放",
        },
        {
            "条件": "日费比 > 4.5%",
            "级别": "超标",
            "建议动作": "超预警：暂停低ROI词，或降低Auto广告出价",
        },
    ]
    alert_df = pd.DataFrame(alerts)

    meta = {
        "asin": p["ASIN"],
        "product_name": p["产品名称"],
        "promo_price": p["促销RRP(€)"],
        "target_qty": int(p["月总销量"]),
        "total_budget": total_bud,
        "tacos_pct": p["建议费比"],
        "target_roas": p["目标ROAS"],
        "currency": cur,
    }

    return {
        "预算分配": budget_df,
        "投放节奏": phase_df,
        "费比预警": alert_df,
        "meta": meta,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Main entry point for web use
# ═══════════════════════════════════════════════════════════════════════════════


def load_and_analyze(own_file, comp_files, config_json=None):
    """
    Run the full analysis pipeline and return all results as DataFrames.

    Parameters
    ----------
    own_file : str or BytesIO
        Own product keyword export (Xiyou format).
    comp_files : list of str or BytesIO
        One or more competitor keyword exports.
    config_json : str, dict, BytesIO, or None
        Product configuration (products_config.json).

    Returns
    -------
    dict keyed by Chinese sheet name, each value a DataFrame (or dict for
    月度执行计划). Also includes '_meta' with raw analysis artifacts.

    Keys:
        "执行摘要"           -> DataFrame
        "竞品独有词"         -> DataFrame
        "排名差距词"         -> DataFrame
        "流量获取差距词"     -> DataFrame
        "本品高潜力词"       -> DataFrame
        "词根聚类"           -> DataFrame
        "综合优先投放"       -> DataFrame
        "产品预算规划"       -> DataFrame
        "流量来源对比"       -> DataFrame
        "月度执行计划"       -> dict with sub-DataFrames
        "_meta"              -> dict with raw kw_data, stats, etc.
    """
    # Load config
    cfg = load_config(config_json)

    # Step 2: Keyword analysis
    kw_data = run_keyword_analysis(own_file, comp_files)

    comp_only = kw_data["comp_only"]
    rank_gap = kw_data["rank_gap"]
    traf_gap = kw_data["traf_gap"]
    own_pot = kw_data["own_pot"]

    # Step 3: Product budget planning
    product_rows, total_rev, total_bud = calc_products(cfg)

    # Step 4: Priority matrix
    kw_matrix = build_kw_priority_matrix(kw_data, cfg)

    # Compute stats
    stats = {
        "comp_only": _get_stats(comp_only),
        "rank_gap": _get_stats(rank_gap),
        "traffic_gap": _get_stats(traf_gap),
        "own_potential": _get_stats(own_pot),
    }

    # Build all output DataFrames
    result = {
        "执行摘要": get_summary_df(
            kw_data, stats, product_rows, total_rev, total_bud
        ),
        "竞品独有词": get_comp_only_df(comp_only),
        "排名差距词": get_rank_gap_df(rank_gap),
        "流量获取差距词": get_traffic_gap_df(traf_gap),
        "本品高潜力词": get_own_potential_df(own_pot),
        "词根聚类": get_root_cluster_df(
            own_pot, comp_only, rank_gap, traf_gap
        ),
        "综合优先投放": kw_matrix,
        "产品预算规划": get_budget_df(
            product_rows, total_rev, total_bud, cfg
        ),
        "流量来源对比": get_traffic_source_df(traf_gap),
        "月度执行计划": get_monthly_plan_df(product_rows, cfg),
        "_meta": {
            "kw_data": kw_data,
            "stats": stats,
            "product_rows": product_rows,
            "total_rev": total_rev,
            "total_bud": total_bud,
            "cfg": cfg,
        },
    }

    return result
