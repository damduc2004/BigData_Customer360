import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# ── CONFIG ────────────────────────────────────────────────────────────────────
MYSQL_URI = "mysql+mysqlconnector://root:123456@localhost:3306/bigdata"

st.set_page_config(page_title="FPT Play — Customer Analytics", page_icon="📊", layout="wide")
st.title("📊 FPT Play — Customer Behavior Analytics")


# ── DATA LOADING ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_rfm():
    engine = create_engine(MYSQL_URI)
    return pd.read_sql("SELECT * FROM customer_rfm_stats", engine)


@st.cache_data(ttl=300)
def load_content():
    engine = create_engine(MYSQL_URI)
    return pd.read_sql("SELECT * FROM customer_content_stats", engine)


@st.cache_data(ttl=300)
def load_search():
    engine = create_engine(MYSQL_URI)
    df = pd.read_sql("SELECT * FROM customer_search_stats", engine)
    col_map = {
        "most_searched_t6": "Most_Searched_T6",
        "most_searched_t7": "Most_Searched_T7",
        "genre_t6": "Genre_T6",
        "genre_t7": "Genre_T7",
        "changing": "Changing",
    }
    df.rename(columns=col_map, inplace=True)
    for c in df.select_dtypes(include="object").columns:
        df[c] = df[c].str.strip()
    return df


df_rfm = load_rfm()
df_content = load_content()
df_search = load_search()


# ── TABS ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs([
    "💰 RFM Segments",
    "🎬 Content Behavior",
    "🔍 Search Trends",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — RFM SEGMENTS
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.header("💰 Phân khúc khách hàng (RFM)")

    # ── Filters (inside tab) ─────────────────────────────────────────────────
    with st.expander("🎛️ Bộ lọc RFM", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            seg_opts = sorted(df_rfm["Segment"].dropna().unique())
            seg_sel = st.multiselect("Segment", options=seg_opts, default=seg_opts, key="rfm_seg")

    df_rfm_f = df_rfm[
        df_rfm["Segment"].isin(seg_sel)
    ]

    # ── KPIs ─────────────────────────────────────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("👥 Khách hàng", f"{len(df_rfm_f):,}")
    k2.metric("📅 Avg Recency", f"{df_rfm_f['Recency'].mean():.1f} ngày")
    k3.metric("🔁 Avg Frequency", f"{df_rfm_f['Frequency'].mean():.2f}")
    k4.metric("💰 Avg Monetary", f"{df_rfm_f['Monetary'].mean():,.0f}")
    k5.metric("🧾 Avg AOV", f"{df_rfm_f['AOV'].mean():,.0f}")

    st.markdown("---")

    # ── Row 1: Segment distribution + Revenue ────────────────────────────────
    col_l, col_r = st.columns(2)

    seg_counts = df_rfm_f["Segment"].value_counts().reset_index()
    seg_counts.columns = ["Segment", "Count"]

    with col_l:
        st.subheader("Phân bổ Segment")
        fig = px.bar(seg_counts, x="Segment", y="Count", color="Segment", text_auto=True,
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(showlegend=False, xaxis_tickangle=-35)
        st.plotly_chart(fig, key="rfm_seg_bar", width="stretch")

    with col_r:
        st.subheader("Revenue by Segment")
        rev = df_rfm_f.groupby("Segment")["Monetary"].sum().reset_index()
        rev.columns = ["Segment", "Revenue"]
        rev = rev.sort_values("Revenue", ascending=False)
        fig2 = px.bar(rev, x="Segment", y="Revenue", color="Segment", text_auto=",.0f",
                      color_discrete_sequence=px.colors.qualitative.Pastel)
        fig2.update_layout(showlegend=False, xaxis_tickangle=-35)
        st.plotly_chart(fig2, key="rfm_rev_bar", width="stretch")

    # ── Row 2: Treemap + Pie ─────────────────────────────────────────────────
    col_l2, col_r2 = st.columns(2)

    with col_l2:
        st.subheader("Treemap — Tỉ trọng khách hàng")
        fig_tree = px.treemap(seg_counts, path=["Segment"], values="Count",
                              color_discrete_sequence=px.colors.qualitative.Set3)
        st.plotly_chart(fig_tree, key="rfm_treemap", width="stretch")

    with col_r2:
        st.subheader("Pie — Tỉ lệ phần trăm Segment")
        fig_pie = px.pie(seg_counts, names="Segment", values="Count", hole=0.35)
        st.plotly_chart(fig_pie, key="rfm_pie", width="stretch")

    st.markdown("---")

    # ── Row 3: Top RFM Scores + Stats Table ──────────────────────────────────
    col_l4, col_r4 = st.columns(2)

    with col_l4:
        st.subheader("Top 20 RFM Score phổ biến")
        rfm_top = df_rfm_f["RFM_Score"].value_counts().nlargest(20).reset_index()
        rfm_top.columns = ["RFM_Score", "Count"]
        fig3 = px.bar(rfm_top, x="RFM_Score", y="Count", text_auto=True,
                      color_discrete_sequence=["#9b59b6"])
        fig3.update_layout(xaxis_type="category")
        st.plotly_chart(fig3, key="rfm_top20", width="stretch")

    with col_r4:
        st.subheader("📋 Thống kê tổng hợp theo Segment")
        seg_stats = df_rfm_f.groupby("Segment").agg(
            Customers=("CustomerID", "count"),
            Avg_Recency=("Recency", "mean"),
            Avg_Frequency=("Frequency", "mean"),
            Avg_Monetary=("Monetary", "mean"),
            Avg_AOV=("AOV", "mean"),
            Total_Revenue=("Monetary", "sum"),
        ).round(1).sort_values("Customers", ascending=False)
        st.dataframe(seg_stats, height=400, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — CONTENT BEHAVIOR
# ══════════════════════════════════════════════════════════════════════════════
CONTENT_COLS = [
    "Total_Truyen_Hinh", "Total_Phim_Truyen",
    "Total_Giai_Tri", "Total_The_Thao", "Total_Thieu_Nhi",
]
CONTENT_LABELS = ["Truyền Hình", "Phim Truyện", "Giải Trí", "Thể Thao", "Thiếu Nhi"]

with tab2:
    st.header("🎬 Hành vi xem nội dung")

    # ── Filters ──────────────────────────────────────────────────────────────
    with st.expander("🎛️ Bộ lọc Content", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            mw_opts = sorted(df_content["MostWatch"].dropna().unique())
            mw_sel = st.multiselect("MostWatch", options=mw_opts, default=mw_opts, key="ct_mw")
        with fc2:
            act_opts = sorted(df_content["Active"].dropna().unique())
            act_sel = st.multiselect("Active", options=act_opts, default=act_opts, key="ct_act")

    df_ct_f = df_content[
        df_content["MostWatch"].isin(mw_sel)
        & df_content["Active"].isin(act_sel)
    ]

    # ── KPIs ─────────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("👥 Tổng user", f"{len(df_ct_f):,}")
    high_pct = (df_ct_f["Active"] == "High").mean() * 100 if len(df_ct_f) > 0 else 0
    k2.metric("🟢 Active = High", f"{high_pct:.1f}%")
    k3.metric("📱 Avg Devices", f"{df_ct_f['Total_Devices'].mean():.1f}" if len(df_ct_f) > 0 else "0")
    total_dur = df_ct_f[CONTENT_COLS].sum().sum()
    k4.metric("⏱️ Tổng thời lượng", f"{total_dur:,.0f}")

    st.markdown("---")

    # ── Row 1: Duration pie + MostWatch bar ──────────────────────────────────
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Tổng thời lượng theo thể loại")
        dur = df_ct_f[CONTENT_COLS].sum().reset_index()
        dur.columns = ["Category", "Duration"]
        dur["Category"] = CONTENT_LABELS
        fig = px.pie(dur, names="Category", values="Duration", hole=0.4,
                     color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig, key="ct_dur_pie", width="stretch")

    with col_r:
        st.subheader("MostWatch — Thể loại xem nhiều nhất")
        mw = df_ct_f["MostWatch"].value_counts().reset_index()
        mw.columns = ["MostWatch", "Count"]
        fig2 = px.bar(mw, x="MostWatch", y="Count", color="MostWatch", text_auto=True,
                      color_discrete_sequence=px.colors.qualitative.Vivid)
        fig2.update_layout(showlegend=False)
        st.plotly_chart(fig2, key="ct_mw_bar", width="stretch")

    st.markdown("---")

    # ── Row 2: Active pie + Taste top ────────────────────────────────────────
    col_l2, col_r2 = st.columns(2)

    with col_l2:
        st.subheader("Tỷ lệ Active")
        ac = df_ct_f["Active"].value_counts().reset_index()
        ac.columns = ["Active", "Count"]
        fig3 = px.pie(ac, names="Active", values="Count", hole=0.4,
                      color_discrete_map={"High": "#2ecc71", "Low": "#e74c3c"})
        st.plotly_chart(fig3, key="ct_active_pie", width="stretch")

    with col_r2:
        st.subheader("Top 15 Taste (sở thích đa thể loại)")
        taste = df_ct_f["Taste"].value_counts().nlargest(15).reset_index()
        taste.columns = ["Taste", "Count"]
        fig4 = px.bar(taste, x="Count", y="Taste", orientation="h", text_auto=True,
                      color_discrete_sequence=["#3498db"])
        fig4.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig4, key="ct_taste_bar", width="stretch")

    st.markdown("---")

    # ── Row 3: Radar + Device distribution ───────────────────────────────────
    col_l3, col_r3 = st.columns(2)

    with col_l3:
        st.subheader("Radar — Thời lượng trung bình")
        avg_dur = df_ct_f[CONTENT_COLS].mean()
        fig_radar = go.Figure()
        fig_radar.add_trace(go.Scatterpolar(
            r=avg_dur.values.tolist() + [avg_dur.values[0]],
            theta=CONTENT_LABELS + [CONTENT_LABELS[0]],
            fill="toself", name="Avg Duration",
        ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True)),
            showlegend=False, height=400,
        )
        st.plotly_chart(fig_radar, key="ct_radar", width="stretch")

    with col_r3:
        st.subheader("Phân phối số thiết bị")
        dev_clip = df_ct_f["Total_Devices"].clip(
            upper=int(df_ct_f["Total_Devices"].quantile(0.95)))
        fig_dev = px.histogram(dev_clip, nbins=40,
                               color_discrete_sequence=["#e67e22"])
        fig_dev.update_layout(xaxis_title="Số thiết bị", yaxis_title="Số user",
                              showlegend=False)
        st.plotly_chart(fig_dev, key="ct_dev_hist", width="stretch")

    st.markdown("---")

    # ── Row 4: MostWatch × Active + Treemap Taste ────────────────────────────
    col_l4, col_r4 = st.columns(2)

    with col_l4:
        st.subheader("MostWatch × Active")
        mw_act = df_ct_f.groupby(["MostWatch", "Active"]).size().reset_index(name="Count")
        fig_stack = px.bar(mw_act, x="MostWatch", y="Count", color="Active",
                           barmode="group", text_auto=True,
                           color_discrete_map={"High": "#2ecc71", "Low": "#e74c3c"})
        fig_stack.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig_stack, key="ct_mw_act", width="stretch")

    with col_r4:
        st.subheader("Treemap — Taste")
        taste_tree = df_ct_f["Taste"].value_counts().nlargest(20).reset_index()
        taste_tree.columns = ["Taste", "Count"]
        fig_tree = px.treemap(taste_tree, path=["Taste"], values="Count",
                              color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_tree, key="ct_taste_tree", width="stretch")

    st.markdown("---")

    # ── Row 5: Stats table ───────────────────────────────────────────────────
    st.subheader("📋 Thống kê theo MostWatch")
    mw_stats = df_ct_f.groupby("MostWatch").agg(
        Users=("Contract", "count"),
        Avg_Devices=("Total_Devices", "mean"),
        Avg_Truyen_Hinh=("Total_Truyen_Hinh", "mean"),
        Avg_Phim_Truyen=("Total_Phim_Truyen", "mean"),
        Avg_Giai_Tri=("Total_Giai_Tri", "mean"),
        Avg_The_Thao=("Total_The_Thao", "mean"),
        Avg_Thieu_Nhi=("Total_Thieu_Nhi", "mean"),
        High_Pct=("Active", lambda x: (x == "High").mean() * 100),
    ).round(1).sort_values("Users", ascending=False)
    st.dataframe(mw_stats, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — SEARCH TRENDS
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.header("🔍 Xu hướng tìm kiếm (T6 vs T7 / 2022)")

    search_cols = list(df_search.columns)
    has_changing = "Changing" in search_cols
    has_genre = "Genre_T6" in search_cols and "Genre_T7" in search_cols

    if df_search.empty:
        st.warning("Bảng customer_search_stats rỗng hoặc chưa có dữ liệu.")
    elif not has_genre:
        st.error(f"Thiếu cột Genre. Các cột hiện có: {search_cols}")
        st.info("Hãy chạy lại `etl_search_stats.py` (uncomment `import_to_mysql`).")
    else:
        # ── Filters ──────────────────────────────────────────────────────────
        with st.expander("🎛️ Bộ lọc Search", expanded=True):
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                g6_opts = sorted(df_search["Genre_T6"].dropna().unique())
                g6_sel = st.multiselect("Genre T6", options=g6_opts, default=g6_opts,
                                        key="sr_g6")
            with fc2:
                g7_opts = sorted(df_search["Genre_T7"].dropna().unique())
                g7_sel = st.multiselect("Genre T7", options=g7_opts, default=g7_opts,
                                        key="sr_g7")
            with fc3:
                if has_changing:
                    chg_type = st.radio(
                        "Trạng thái thay đổi",
                        ["Tất cả", "Không đổi", "Thay đổi"],
                        horizontal=True, key="sr_chg",
                    )
                else:
                    chg_type = "Tất cả"

        df_sr_f = df_search[
            df_search["Genre_T6"].isin(g6_sel) & df_search["Genre_T7"].isin(g7_sel)
        ]
        if has_changing and chg_type == "Không đổi":
            df_sr_f = df_sr_f[df_sr_f["Changing"] == "No Change"]
        elif has_changing and chg_type == "Thay đổi":
            df_sr_f = df_sr_f[df_sr_f["Changing"] != "No Change"]

        # ── KPIs ─────────────────────────────────────────────────────────────
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("👥 Users", f"{len(df_sr_f):,}")
        k2.metric("🎭 Genre T6 (loại)", f"{df_sr_f['Genre_T6'].nunique()}")
        k3.metric("🎭 Genre T7 (loại)", f"{df_sr_f['Genre_T7'].nunique()}")
        if has_changing:
            no_chg = (df_sr_f["Changing"] == "No Change").mean() * 100 if len(df_sr_f) > 0 else 0
            k4.metric("🔄 Không đổi sở thích", f"{no_chg:.1f}%")

        st.markdown("---")

        # ── Row 1: Genre T6 vs T7 ───────────────────────────────────────────
        col_l, col_r = st.columns(2)

        with col_l:
            st.subheader("Genre phổ biến — Tháng 6")
            g6 = df_sr_f["Genre_T6"].value_counts().reset_index()
            g6.columns = ["Genre", "Count"]
            fig = px.bar(g6, x="Genre", y="Count", color="Genre", text_auto=True,
                         color_discrete_sequence=px.colors.qualitative.Vivid)
            fig.update_layout(showlegend=False, xaxis_tickangle=-35)
            st.plotly_chart(fig, key="sr_g6_bar", width="stretch")

        with col_r:
            st.subheader("Genre phổ biến — Tháng 7")
            g7 = df_sr_f["Genre_T7"].value_counts().reset_index()
            g7.columns = ["Genre", "Count"]
            fig2 = px.bar(g7, x="Genre", y="Count", color="Genre", text_auto=True,
                          color_discrete_sequence=px.colors.qualitative.Vivid)
            fig2.update_layout(showlegend=False, xaxis_tickangle=-35)
            st.plotly_chart(fig2, key="sr_g7_bar", width="stretch")

        st.markdown("---")

        # ── Row 2: Grouped comparison + Pie thay đổi ────────────────────────
        col_l2, col_r2 = st.columns(2)

        with col_l2:
            st.subheader("So sánh Genre: T6 vs T7")
            g6_vc = df_sr_f["Genre_T6"].value_counts().reset_index()
            g6_vc.columns = ["Genre", "T6"]
            g7_vc = df_sr_f["Genre_T7"].value_counts().reset_index()
            g7_vc.columns = ["Genre", "T7"]
            merged = pd.merge(g6_vc, g7_vc, on="Genre", how="outer").fillna(0)
            merged = merged.sort_values("T6", ascending=False)
            fig_comp = go.Figure()
            fig_comp.add_trace(go.Bar(name="Tháng 6", x=merged["Genre"],
                                      y=merged["T6"], marker_color="#3498db"))
            fig_comp.add_trace(go.Bar(name="Tháng 7", x=merged["Genre"],
                                      y=merged["T7"], marker_color="#e74c3c"))
            fig_comp.update_layout(barmode="group", xaxis_tickangle=-35)
            st.plotly_chart(fig_comp, key="sr_compare", width="stretch")

        with col_r2:
            if has_changing:
                st.subheader("Tỷ lệ thay đổi vs không đổi")
                df_ch = df_sr_f.copy()
                df_ch["Status"] = df_ch["Changing"].apply(
                    lambda x: "Không đổi" if x == "No Change" else "Thay đổi"
                )
                ch_pie = df_ch["Status"].value_counts().reset_index()
                ch_pie.columns = ["Status", "Count"]
                fig4 = px.pie(ch_pie, names="Status", values="Count", hole=0.4,
                              color_discrete_map={"Không đổi": "#2ecc71",
                                                  "Thay đổi": "#e67e22"})
                st.plotly_chart(fig4, key="sr_chg_pie", width="stretch")

        st.markdown("---")

        # ── Row 3: Top changing + Top keywords T6 ───────────────────────────
        if has_changing:
            col_l3, col_r3 = st.columns(2)

            with col_l3:
                st.subheader("Top 15 xu hướng thay đổi")
                ch = df_sr_f["Changing"].value_counts().nlargest(15).reset_index()
                ch.columns = ["Changing", "Count"]
                fig3 = px.bar(ch, x="Count", y="Changing", orientation="h",
                              text_auto=True, color_discrete_sequence=["#3498db"])
                fig3.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig3, key="sr_top_chg", width="stretch")

            with col_r3:
                st.subheader("Top 15 từ khóa — Tháng 6")
                kw6 = df_sr_f["Most_Searched_T6"].value_counts().nlargest(15).reset_index()
                kw6.columns = ["Keyword", "Count"]
                fig_kw6 = px.bar(kw6, x="Count", y="Keyword", orientation="h",
                                 text_auto=True, color_discrete_sequence=["#9b59b6"])
                fig_kw6.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_kw6, key="sr_kw6", width="stretch")

            st.markdown("---")

        # ── Row 4: Top keywords T7 + Heatmap ────────────────────────────────
        col_l4, col_r4 = st.columns(2)

        with col_l4:
            st.subheader("Top 15 từ khóa — Tháng 7")
            kw7 = df_sr_f["Most_Searched_T7"].value_counts().nlargest(15).reset_index()
            kw7.columns = ["Keyword", "Count"]
            fig_kw7 = px.bar(kw7, x="Count", y="Keyword", orientation="h",
                             text_auto=True, color_discrete_sequence=["#e67e22"])
            fig_kw7.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_kw7, key="sr_kw7", width="stretch")

        with col_r4:
            st.subheader("Heatmap — Genre T6 × T7")
            cross = df_sr_f.groupby(["Genre_T6", "Genre_T7"]).size().reset_index(name="Count")
            cross_pv = cross.pivot(index="Genre_T6", columns="Genre_T7",
                                   values="Count").fillna(0)
            fig_hm = px.imshow(
                cross_pv, text_auto=True,
                labels=dict(x="Genre T7", y="Genre T6", color="Users"),
                color_continuous_scale="YlOrRd", aspect="auto",
            )
            fig_hm.update_layout(height=500)
            st.plotly_chart(fig_hm, key="sr_heatmap", width="stretch")

        st.markdown("---")

        # ── Row 5: Sankey ────────────────────────────────────────────────────
        st.subheader("Sankey: Dòng chảy Genre T6 → T7")
        flow = df_sr_f.groupby(["Genre_T6", "Genre_T7"]).size().reset_index(name="count")
        flow = flow.nlargest(20, "count")

        labels_t6 = [f"{g} (T6)" for g in flow["Genre_T6"]]
        labels_t7 = [f"{g} (T7)" for g in flow["Genre_T7"]]
        all_labels = list(dict.fromkeys(labels_t6 + labels_t7))
        label_map = {lb: i for i, lb in enumerate(all_labels)}

        fig_sankey = go.Figure(go.Sankey(
            node=dict(label=all_labels, pad=15, thickness=20),
            link=dict(
                source=[label_map[f"{g} (T6)"] for g in flow["Genre_T6"]],
                target=[label_map[f"{g} (T7)"] for g in flow["Genre_T7"]],
                value=flow["count"].tolist(),
            ),
        ))
        fig_sankey.update_layout(height=500)
        st.plotly_chart(fig_sankey, key="sr_sankey", width="stretch")