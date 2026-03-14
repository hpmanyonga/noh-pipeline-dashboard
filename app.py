# NOH Growth Engine — Pipeline Dashboard
# Run: streamlit run app.py
#
# Requirements (add to requirements.txt if not present):
#   streamlit>=1.50.0
#   plotly>=6.5.0
#   pandas>=2.0.0
#   supabase>=2.0.0
#
# Data source: Supabase `leads` table with pipeline extension columns.
# Falls back to synthetic demo data when Supabase is unavailable.
# Refresh cadence: real-time (on page load / manual refresh).

import os
import random
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="NOH Growth Engine — Pipeline",
    page_icon=None,
    layout="wide",
)

# ---------------------------------------------------------------------------
# Brand constants
# ---------------------------------------------------------------------------
TEAL = "#40887d"
DUSTY_PINK = "#f3bdc4"
DARK_TEAL = "#2e6b68"
LIGHT_TEAL = "#a8d5d3"
WARM_GOLD = "#e8c07a"
SOFT_PINK = "#d4878f"

STATUS_COLORS = {
    "NEW": "#e74c3c",
    "TRIAGED": "#e67e22",
    "BOOKED": "#27ae60",
    "ATTENDED": "#2980b9",
    "CLOSED_WON": TEAL,
    "CLOSED_LOST": "#95a5a6",
}

# ---------------------------------------------------------------------------
# Custom CSS — brand font + metric card styling
# ---------------------------------------------------------------------------
st.markdown(
    f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
        html, body, [class*="css"] {{
            font-family: Arial, Helvetica, sans-serif;
        }}
        /* Metric card styling */
        div[data-testid="stMetric"] {{
            background: #f8f9fa;
            border-left: 4px solid {TEAL};
            padding: 12px 16px;
            border-radius: 6px;
        }}
        div[data-testid="stMetric"] label {{
            font-size: 0.85rem;
            color: #555;
        }}
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {{
            font-size: 1.8rem;
            font-weight: 700;
            color: {DARK_TEAL};
        }}
        /* Stale-lead highlight class (applied via pandas styler) */
        .stale-row {{
            background-color: #fff3cd !important;
        }}
        /* Header accent bar */
        .header-bar {{
            background: linear-gradient(90deg, {TEAL}, {DUSTY_PINK});
            height: 4px;
            border-radius: 2px;
            margin-bottom: 1rem;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================================
# DATA LAYER
# ============================================================================

# ---------------------------------------------------------------------------
# Supabase connection helper
# ---------------------------------------------------------------------------
def _get_supabase_client():
    """Return a Supabase client or None if credentials are missing."""
    try:
        from supabase import create_client
    except ImportError:
        return None

    url = os.environ.get("SUPABASE_URL", "https://zcjodsewjugovlmocgrz.supabase.co")
    key = os.environ.get("SUPABASE_KEY")
    if not key:
        return None
    try:
        return create_client(url, key)
    except Exception:
        return None


def _fetch_from_supabase() -> pd.DataFrame | None:
    """Attempt to load leads from Supabase. Returns None on failure."""
    client = _get_supabase_client()
    if client is None:
        return None
    try:
        resp = client.table("leads").select("*").execute()
        if resp.data:
            return pd.DataFrame(resp.data)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Synthetic data generator — used when Supabase is unavailable
# ---------------------------------------------------------------------------
def _generate_synthetic_leads(n: int = 80) -> pd.DataFrame:
    """
    Generate realistic synthetic pipeline data.

    Distributions based on actual NOH patient source data:
      - Google search: 47%, Other: 29%, Word of mouth: 8%,
        TikTok: 7%, Dr referral: 6%, Discovery: 3%

    Status distribution (realistic funnel):
      - NEW: 10%, TRIAGED: 15%, BOOKED: 25%, ATTENDED: 30%,
        CLOSED_WON: 15%, CLOSED_LOST: 5%
    """
    random.seed(42)  # reproducible demo data

    sources = (
        ["Google Search"] * 47
        + ["Other"] * 29
        + ["Word of Mouth"] * 8
        + ["TikTok"] * 7
        + ["Dr Referral"] * 6
        + ["Discovery"] * 3
    )

    statuses_pool = (
        ["NEW"] * 10
        + ["TRIAGED"] * 15
        + ["BOOKED"] * 25
        + ["ATTENDED"] * 30
        + ["CLOSED_WON"] * 15
        + ["CLOSED_LOST"] * 5
    )

    sites = ["Pretoria", "Johannesburg", "Rustenburg"]
    site_weights = [0.50, 0.35, 0.15]

    lead_types = [
        "NEW_MATERNITY",
        "NEW_GYNAE",
        "FOLLOW_UP",
        "SECOND_OPINION",
    ]
    type_weights = [0.60, 0.20, 0.12, 0.08]

    first_names = [
        "Thandi", "Nomsa", "Lerato", "Zanele", "Precious", "Nompumelelo",
        "Refilwe", "Naledi", "Bongiwe", "Ayanda", "Palesa", "Mpho",
        "Lindiwe", "Dineo", "Khanyi", "Zinhle", "Nthabiseng", "Tumelo",
        "Nonhlanhla", "Sibongile", "Masego", "Boitumelo", "Karabo",
        "Lethabo", "Thandeka", "Nandi", "Fikile", "Nomvula", "Busisiwe",
        "Thato",
    ]
    last_names = [
        "Molefe", "Dlamini", "Nkosi", "Zulu", "Mthembu", "Khumalo",
        "Ndlovu", "Mokoena", "Makhanya", "Sithole", "Radebe", "Cele",
        "Ngcobo", "Baloyi", "Mahlangu", "Tshabalala", "Maseko", "Vilakazi",
        "Mahomed", "Govender", "Pillay", "Van Wyk", "Botha", "Pretorius",
        "Fourie", "Motaung", "Mokwena", "Selepe", "Phiri", "Banda",
    ]

    admins = ["Nurse Refilwe", "Nurse Naledi", "Nurse Palesa", "Sister Dineo"]

    now = datetime.now()
    rows = []
    for i in range(n):
        # Random creation date within last 90 days
        created = now - timedelta(days=random.randint(0, 90), hours=random.randint(0, 23))
        status = random.choice(statuses_pool)

        # Timestamps progress through the funnel
        triaged_at = None
        booked_at = None
        attended_at = None

        if status in ("TRIAGED", "BOOKED", "ATTENDED", "CLOSED_WON", "CLOSED_LOST"):
            triaged_at = created + timedelta(days=random.randint(0, 3), hours=random.randint(1, 12))
        if status in ("BOOKED", "ATTENDED", "CLOSED_WON", "CLOSED_LOST"):
            booked_at = (triaged_at or created) + timedelta(days=random.randint(1, 7))
        if status in ("ATTENDED", "CLOSED_WON", "CLOSED_LOST"):
            attended_at = (booked_at or created) + timedelta(days=random.randint(1, 14))

        site = random.choices(sites, weights=site_weights, k=1)[0]
        lead_type = random.choices(lead_types, weights=type_weights, k=1)[0]

        rows.append(
            {
                "name": f"{random.choice(first_names)} {random.choice(last_names)}",
                "email": f"patient{i+1:03d}@example.com",
                "phone": f"+2771{random.randint(1000000,9999999)}",
                "province": random.choice(["Gauteng", "North West", "Limpopo"]),
                "gestational_weeks": random.randint(6, 38) if lead_type == "NEW_MATERNITY" else None,
                "delivery_preference": random.choice(["NVD", "CS", "Undecided"]) if lead_type == "NEW_MATERNITY" else None,
                "risk_level": random.choice(["Low", "Medium", "High"]),
                "lead_source": random.choice(sources),
                "lead_type": lead_type,
                "status": status,
                "site": site,
                "created_at": created.isoformat(),
                "triaged_at": triaged_at.isoformat() if triaged_at else None,
                "booked_at": booked_at.isoformat() if booked_at else None,
                "attended_at": attended_at.isoformat() if attended_at else None,
                "admin_assigned": random.choice(admins),
            }
        )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Load data (Supabase first, then synthetic fallback)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_leads() -> tuple[pd.DataFrame, bool]:
    """Return (dataframe, is_live). Tries Supabase first, falls back to synthetic."""
    df = _fetch_from_supabase()
    if df is not None and not df.empty:
        # Ensure pipeline columns exist — fill missing with defaults
        for col in ["lead_source", "lead_type", "status", "site", "triaged_at", "booked_at", "attended_at", "admin_assigned"]:
            if col not in df.columns:
                df[col] = None
        return df, True
    return _generate_synthetic_leads(80), False


# ============================================================================
# LOAD & PREPARE DATA
# ============================================================================

df_all, is_live = load_leads()

# Parse dates
df_all["created_at"] = pd.to_datetime(df_all["created_at"], errors="coerce")
for ts_col in ["triaged_at", "booked_at", "attended_at"]:
    if ts_col in df_all.columns:
        df_all[ts_col] = pd.to_datetime(df_all[ts_col], errors="coerce")

# Compute days in pipeline
df_all["days_in_pipeline"] = (pd.Timestamp.now() - df_all["created_at"]).dt.days

# Flag stale leads: >7 days still in NEW or TRIAGED
df_all["is_stale"] = (
    (df_all["status"].isin(["NEW", "TRIAGED"]))
    & (df_all["days_in_pipeline"] > 7)
)


# ============================================================================
# HEADER
# ============================================================================

st.markdown('<div class="header-bar"></div>', unsafe_allow_html=True)
st.title("NOH Growth Engine \u2014 Pipeline Dashboard")

data_badge = "\u2705 Live (Supabase)" if is_live else "\u26a0\ufe0f Demo data (Supabase unavailable)"
st.caption(f"Data source: {data_badge}")

# Date range selector + refresh
header_cols = st.columns([2, 2, 1])
with header_cols[0]:
    date_range = st.date_input(
        "Date range",
        value=(
            (datetime.now() - timedelta(days=30)).date(),
            datetime.now().date(),
        ),
        max_value=datetime.now().date(),
    )
with header_cols[2]:
    if st.button("Refresh", type="primary"):
        st.cache_data.clear()
        st.rerun()

# Filter by date range
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    mask = (df_all["created_at"].dt.date >= start_date) & (df_all["created_at"].dt.date <= end_date)
    df = df_all[mask].copy()
else:
    df = df_all.copy()

st.divider()


# ============================================================================
# SECTION 6: WEEKLY PULSE (sidebar)
# ============================================================================

st.sidebar.markdown(f"### Weekly Pulse")
st.sidebar.markdown("---")

# Time boundaries for this week
week_start = (datetime.now() - timedelta(days=datetime.now().weekday())).replace(
    hour=0, minute=0, second=0, microsecond=0
)
df_week = df_all[df_all["created_at"] >= week_start]

# Synthetic content performance data for the pulse
social_sources = ["TikTok", "Instagram", "Facebook"]
email_sources = ["Email Campaign"]

leads_from_social = len(df_week[df_week["lead_source"].isin(social_sources)]) if "lead_source" in df_week.columns else 0
leads_from_email = len(df_week[df_week["lead_source"].isin(email_sources)]) if "lead_source" in df_week.columns else 0

stale_count = df_all["is_stale"].sum()
no_shows = len(
    df_week[
        (df_week["status"] == "BOOKED")
        & (df_week["booked_at"].notna())
        & (df_week["attended_at"].isna())
        & ((pd.Timestamp.now() - df_week["booked_at"]).dt.days > 1)
    ]
) if "booked_at" in df_week.columns else 0

# Synthetic posts data
posts_this_week = random.randint(3, 7)
top_post = random.choice([
    "Birth story reel (TikTok) - 12.4k views",
    "Cost comparison carousel (Instagram) - 8.2k reach",
    "Midwife Q&A live (Facebook) - 3.1k viewers",
    "Patient testimonial (TikTok) - 9.8k views",
])

st.sidebar.metric("Posts published this week", posts_this_week)
st.sidebar.metric("Top performing post", "")
st.sidebar.caption(top_post)
st.sidebar.metric("Leads from social", leads_from_social)
st.sidebar.metric("Leads from email", leads_from_email)

if stale_count > 0:
    st.sidebar.error(f"Stale leads alert: {stale_count} leads untriaged for >7 days")
else:
    st.sidebar.success("No stale leads")

st.sidebar.metric("No-shows this week", no_shows)


# ============================================================================
# SECTION 2: TOP-LEVEL KPIs
# ============================================================================

st.subheader("Pipeline KPIs")

total_leads = len(df)
triaged = len(df[df["status"].isin(["TRIAGED", "BOOKED", "ATTENDED", "CLOSED_WON", "CLOSED_LOST"])])
booked = len(df[df["status"].isin(["BOOKED", "ATTENDED", "CLOSED_WON", "CLOSED_LOST"])])
attended = len(df[df["status"].isin(["ATTENDED", "CLOSED_WON", "CLOSED_LOST"])])

kpi_cols = st.columns(4)

with kpi_cols[0]:
    st.metric("New Leads", total_leads)

with kpi_cols[1]:
    pct_triaged = f"{triaged / total_leads * 100:.0f}%" if total_leads > 0 else "0%"
    st.metric("Triaged", triaged, delta=pct_triaged)

with kpi_cols[2]:
    pct_booked = f"{booked / triaged * 100:.0f}% of triaged" if triaged > 0 else "0%"
    st.metric("Booked", booked, delta=pct_booked)

with kpi_cols[3]:
    pct_attended = f"{attended / booked * 100:.0f}% of booked" if booked > 0 else "0%"
    st.metric("Attended", attended, delta=pct_attended)

st.divider()


# ============================================================================
# SECTION 3: FUNNEL VISUALIZATION
# ============================================================================

st.subheader("Lead-to-Enrolled Funnel")

enrolled = len(df[df["status"] == "CLOSED_WON"])

funnel_stages = ["Leads", "Triaged", "Booked", "Attended", "Enrolled"]
funnel_values = [total_leads, triaged, booked, attended, enrolled]

# Compute inter-stage conversion rates
conversions = []
for i in range(len(funnel_values) - 1):
    if funnel_values[i] > 0:
        rate = funnel_values[i + 1] / funnel_values[i] * 100
        conversions.append(f"{rate:.0f}%")
    else:
        conversions.append("--")

# Build funnel chart
fig_funnel = go.Figure(
    go.Funnel(
        y=funnel_stages,
        x=funnel_values,
        textinfo="value+percent initial",
        textposition="inside",
        marker=dict(
            color=[TEAL, DARK_TEAL, DUSTY_PINK, SOFT_PINK, WARM_GOLD],
            line=dict(width=1, color="white"),
        ),
        connector=dict(line=dict(color=LIGHT_TEAL, width=1)),
    )
)
fig_funnel.update_layout(
    height=350,
    margin=dict(t=20, b=20, l=20, r=20),
    font=dict(family="Arial", size=13),
)

# Conversion rate annotation
funnel_col, conv_col = st.columns([3, 1])

with funnel_col:
    st.plotly_chart(fig_funnel, use_container_width=True)

with conv_col:
    st.markdown("**Stage Conversion Rates**")
    for i, label in enumerate(["Leads to Triaged", "Triaged to Booked", "Booked to Attended", "Attended to Enrolled"]):
        st.markdown(f"- {label}: **{conversions[i]}**")
    if total_leads > 0:
        overall = enrolled / total_leads * 100
        st.markdown(f"- Overall (Lead to Enrolled): **{overall:.1f}%**")

st.divider()


# ============================================================================
# SECTION 4: LEAD SOURCE BREAKDOWN
# ============================================================================

st.subheader("Lead Source Breakdown")

if "lead_source" in df.columns:
    source_counts = df["lead_source"].value_counts().reset_index()
    source_counts.columns = ["Source", "Count"]
    source_counts = source_counts.sort_values("Count", ascending=True)

    fig_source = go.Figure(
        go.Bar(
            x=source_counts["Count"],
            y=source_counts["Source"],
            orientation="h",
            marker_color=TEAL,
            text=source_counts["Count"],
            textposition="outside",
        )
    )
    fig_source.update_layout(
        height=max(300, len(source_counts) * 40),
        margin=dict(t=10, b=10, l=140, r=40),
        xaxis_title="Number of leads",
        yaxis_title="",
        font=dict(family="Arial", size=12),
    )
    st.plotly_chart(fig_source, use_container_width=True)
else:
    st.info("Lead source data not available.")

st.divider()


# ============================================================================
# SECTION 5: PIPELINE STATUS TABLE
# ============================================================================

st.subheader("Pipeline Status Table")

# Filters
filter_cols = st.columns(4)
with filter_cols[0]:
    status_filter = st.multiselect(
        "Filter by status",
        options=sorted(df["status"].dropna().unique()),
        default=sorted(df["status"].dropna().unique()),
    )
with filter_cols[1]:
    site_filter = st.multiselect(
        "Filter by site",
        options=sorted(df["site"].dropna().unique()),
        default=sorted(df["site"].dropna().unique()),
    )
with filter_cols[2]:
    source_filter = st.multiselect(
        "Filter by source",
        options=sorted(df["lead_source"].dropna().unique()) if "lead_source" in df.columns else [],
        default=sorted(df["lead_source"].dropna().unique()) if "lead_source" in df.columns else [],
    )
with filter_cols[3]:
    stale_only = st.checkbox("Show stale leads only (>7 days in NEW/TRIAGED)")

# Apply filters
df_table = df.copy()
df_table = df_table[df_table["status"].isin(status_filter)]
df_table = df_table[df_table["site"].isin(site_filter)]
if "lead_source" in df_table.columns and source_filter:
    df_table = df_table[df_table["lead_source"].isin(source_filter)]
if stale_only:
    df_table = df_table[df_table["is_stale"]]

# Prepare display columns
display_cols = [
    "name", "created_at", "lead_source", "lead_type", "site",
    "status", "days_in_pipeline", "admin_assigned", "is_stale",
]
available_cols = [c for c in display_cols if c in df_table.columns]
df_display = df_table[available_cols].copy()
df_display = df_display.sort_values("created_at", ascending=False)

# Format created_at for display
if "created_at" in df_display.columns:
    df_display["created_at"] = df_display["created_at"].dt.strftime("%Y-%m-%d %H:%M")

# Rename columns for readability
col_rename = {
    "name": "Name",
    "created_at": "Date",
    "lead_source": "Source",
    "lead_type": "Type",
    "site": "Site",
    "status": "Status",
    "days_in_pipeline": "Days in Pipeline",
    "admin_assigned": "Admin Assigned",
    "is_stale": "Stale",
}
df_display = df_display.rename(columns=col_rename)


def _color_status(val):
    """Return CSS background color for pipeline status."""
    color_map = {
        "NEW": "background-color: #fce4e4; color: #c0392b;",
        "TRIAGED": "background-color: #fef3e2; color: #e67e22;",
        "BOOKED": "background-color: #e8f8f0; color: #27ae60;",
        "ATTENDED": "background-color: #e1edf7; color: #2980b9;",
        "CLOSED_WON": f"background-color: #d9edeb; color: {DARK_TEAL};",
        "CLOSED_LOST": "background-color: #ecf0f1; color: #7f8c8d;",
    }
    return color_map.get(val, "")


def _highlight_stale(row):
    """Highlight entire row if stale."""
    if row.get("Stale", False):
        return ["background-color: #fff3cd;"] * len(row)
    return [""] * len(row)


styled = (
    df_display.style
    .map(_color_status, subset=["Status"] if "Status" in df_display.columns else [])
    .apply(_highlight_stale, axis=1)
)

st.dataframe(
    styled,
    use_container_width=True,
    height=min(600, max(200, len(df_display) * 35 + 40)),
    hide_index=True,
)

st.caption(
    f"Showing {len(df_display)} of {len(df)} leads. "
    f"Denominator: all leads created between {date_range[0]} and {date_range[1]}. "
    f"Data source: {'Supabase (live)' if is_live else 'Synthetic demo data'}."
)

st.divider()


# ============================================================================
# SECTION 7: CONTENT PERFORMANCE
# ============================================================================

st.subheader("Content Performance")

# Synthetic content data — in production, this would come from a content_posts table
content_data = [
    {"Post Date": "2026-03-11", "Channel": "TikTok", "Topic": "Birth story — NVD at NOH Pretoria", "Engagements": 12_400, "Leads Attributed": 4},
    {"Post Date": "2026-03-10", "Channel": "Instagram", "Topic": "Cost comparison: NOH vs fee-for-service", "Engagements": 8_200, "Leads Attributed": 3},
    {"Post Date": "2026-03-09", "Channel": "Facebook", "Topic": "Midwife Q&A live session", "Engagements": 3_100, "Leads Attributed": 1},
    {"Post Date": "2026-03-08", "Channel": "TikTok", "Topic": "Patient testimonial — antenatal journey", "Engagements": 9_800, "Leads Attributed": 3},
    {"Post Date": "2026-03-07", "Channel": "Instagram", "Topic": "Meet our Pretoria midwifery team", "Engagements": 5_600, "Leads Attributed": 2},
    {"Post Date": "2026-03-06", "Channel": "WhatsApp", "Topic": "Weekly maternity tips broadcast", "Engagements": 1_200, "Leads Attributed": 1},
    {"Post Date": "2026-03-05", "Channel": "TikTok", "Topic": "What to pack for your hospital bag", "Engagements": 15_300, "Leads Attributed": 5},
    {"Post Date": "2026-03-04", "Channel": "Facebook", "Topic": "NOH Rustenburg site launch update", "Engagements": 2_800, "Leads Attributed": 2},
    {"Post Date": "2026-03-03", "Channel": "Instagram", "Topic": "Postnatal recovery tips carousel", "Engagements": 6_400, "Leads Attributed": 1},
    {"Post Date": "2026-03-02", "Channel": "Email", "Topic": "Monthly newsletter — March edition", "Engagements": 4_500, "Leads Attributed": 3},
]

df_content = pd.DataFrame(content_data)

st.dataframe(
    df_content.style.format({"Engagements": "{:,.0f}"}),
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "Content performance data is illustrative. "
    "Connect a content management system or social analytics API for live tracking."
)

st.divider()


# ============================================================================
# FOOTER
# ============================================================================

st.markdown(
    f"""
    <div style="text-align: center; color: #999; font-size: 0.8rem; padding: 1rem 0;">
        Network One Health | Pipeline Dashboard | Built with Streamlit
        <br>Data refreshed on page load. Last loaded: {datetime.now().strftime("%Y-%m-%d %H:%M")}
    </div>
    """,
    unsafe_allow_html=True,
)
