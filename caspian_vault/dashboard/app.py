import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text

# -------------------------
# Postgres connection
# -------------------------
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "airflow")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASS = os.getenv("PG_PASS", "airflow")

ENGINE = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

st.set_page_config(page_title="Caspian Seismic Analytics", layout="wide")
st.title("CaspianPetro — Seismic Analytics Dashboard")

def qdf(sql: str, params: dict | None = None) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

# -------------------------
# Safe format helpers
# -------------------------
def safe_int(x, default=0):
    try:
        if x is None or pd.isna(x):
            return default
        return int(x)
    except Exception:
        return default

def safe_float(x):
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None

def fmt_pct(x):
    v = safe_float(x)
    return "N/A" if v is None else f"{v*100:.2f}%"

def fmt_float(x, digits=4):
    v = safe_float(x)
    return "N/A" if v is None else f"{v:.{digits}f}"


# ============================================================
# Sidebar filters (Wells + Source + Survey type + Depth + Quality)
# ============================================================
st.sidebar.header("Filters")

# Pull filter options (safe even if empty)
well_opts = qdf("select distinct well_id from public.dim_well order by well_id")["well_id"].tolist()
sf_opts   = qdf("select distinct source_format from public.fct_readings order by source_format")["source_format"].tolist() if not qdf("select 1 from public.fct_readings limit 1").empty else []
st_opts   = qdf("""
    select distinct s.survey_type_id
    from public.fct_readings f
    join public.dim_survey_type s on s.hk_survey_type = f.hk_survey_type
    order by 1
""")["survey_type_id"].tolist() if not qdf("select 1 from public.fct_readings limit 1").empty else []

selected_wells = st.sidebar.multiselect(
    "Select wells (empty = all)",
    options=well_opts,
    default=[]
)

selected_sources = st.sidebar.multiselect(
    "Select source_format (empty = all)",
    options=sf_opts,
    default=[]
)

selected_survey_types = st.sidebar.multiselect(
    "Select survey_type_id (empty = all)",
    options=st_opts,
    default=[]
)

quality_only = st.sidebar.checkbox("Only quality_flag = 1", value=False)

# Depth range slider (computed from data if available)
depth_minmax = qdf("select min(depth_ft) as mn, max(depth_ft) as mx from public.fct_readings")
mn = safe_float(depth_minmax["mn"].iloc[0] if len(depth_minmax) else None)
mx = safe_float(depth_minmax["mx"].iloc[0] if len(depth_minmax) else None)
if mn is None or mx is None:
    depth_range = None
    st.sidebar.caption("Depth filter disabled (no readings).")
else:
    depth_range = st.sidebar.slider("Depth range (ft)", float(mn), float(mx), (float(mn), float(mx)))

# A small helper to build WHERE clause + params
def build_filters(alias_w="w", alias_f="f", alias_s="s"):
    where = []
    params = {}

    if selected_wells:
        where.append(f"{alias_w}.well_id = ANY(:wells)")
        params["wells"] = selected_wells

    if selected_sources:
        where.append(f"{alias_f}.source_format = ANY(:sources)")
        params["sources"] = selected_sources

    if selected_survey_types:
        where.append(f"{alias_s}.survey_type_id = ANY(:survey_types)")
        params["survey_types"] = selected_survey_types

    if quality_only:
        where.append(f"{alias_f}.quality_flag = 1")

    if depth_range is not None:
        where.append(f"{alias_f}.depth_ft between :dmin and :dmax")
        params["dmin"] = depth_range[0]
        params["dmax"] = depth_range[1]

    where_sql = (" where " + " and ".join(where)) if where else ""
    return where_sql, params


st.divider()

# ============================================================
# KPIs (filtered)
# ============================================================
where_sql, params = build_filters(alias_w="w", alias_f="f", alias_s="s")

kpi = qdf(f"""
select
  count(distinct w.well_id) as wells,
  count(*)                 as readings,
  avg(case when f.quality_flag=1 then 1 else 0 end) as quality_rate,
  avg(f.amplitude)         as avg_amp
from public.fct_readings f
join public.dim_well w using (hk_well)
join public.dim_survey_type s on s.hk_survey_type = f.hk_survey_type
{where_sql}
""", params=params)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Wells (filtered)", safe_int(kpi["wells"].iloc[0] if len(kpi) else 0))
c2.metric("Readings (filtered)", safe_int(kpi["readings"].iloc[0] if len(kpi) else 0))
c3.metric("Quality rate", fmt_pct(kpi["quality_rate"].iloc[0] if (len(kpi) and "quality_rate" in kpi.columns) else None))
c4.metric("Avg amplitude", fmt_float(kpi["avg_amp"].iloc[0] if (len(kpi) and "avg_amp" in kpi.columns) else None, digits=4))

st.divider()

# ============================================================
# Wells map (filtered wells)
# ============================================================
st.subheader("Wells map (filtered)")
wells_sql_where, wells_params = build_filters(alias_w="w", alias_f="f", alias_s="s")

# If you selected wells, show those wells even if fct_readings is empty.
if selected_wells:
    wells = qdf("""
        select well_id, lat, lon
        from public.dim_well
        where well_id = ANY(:wells)
    """, params={"wells": selected_wells})
else:
    wells = qdf("select well_id, lat, lon from public.dim_well")

if wells.empty:
    st.info("No wells found (dim_well is empty).")
else:
    fig_map = px.scatter_mapbox(
        wells, lat="lat", lon="lon", hover_name="well_id", zoom=4, height=420
    )
    fig_map.update_layout(mapbox_style="open-street-map", margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig_map, use_container_width=True)

st.divider()

# ============================================================
# Amplitude distribution (filtered)
# ============================================================
st.subheader("Amplitude distribution (filtered)")
where_sql, params = build_filters(alias_w="w", alias_f="f", alias_s="s")

amp = qdf(f"""
select f.amplitude, f.quality_flag
from public.fct_readings f
join public.dim_well w using (hk_well)
join public.dim_survey_type s on s.hk_survey_type = f.hk_survey_type
{where_sql}
""", params=params)

if amp.empty:
    st.info("No readings for current filters.")
else:
    fig_hist = px.histogram(amp, x="amplitude", color="quality_flag", nbins=60)
    st.plotly_chart(fig_hist, use_container_width=True)

st.divider()

# ============================================================
# Anomaly heatmap (filtered)
# ============================================================
st.subheader("Anomaly heatmap (depth bucket × well) — filtered")
where_sql, params = build_filters(alias_w="w", alias_f="f", alias_s="s")

heat = qdf(f"""
with filtered as (
  select f.*
  from public.fct_readings f
  join public.dim_well w using (hk_well)
  join public.dim_survey_type s on s.hk_survey_type = f.hk_survey_type
  {where_sql}
),
stats as (
  select hk_well, avg(amplitude) as mu, stddev_samp(amplitude) as sigma
  from filtered
  group by 1
),
scored as (
  select
    f.hk_well,
    cast(floor(f.depth_ft/50)*50 as int) as depth_bucket,
    case
      when s.sigma is null or s.sigma = 0 then 0
      else abs(f.amplitude - s.mu) / s.sigma
    end as z
  from filtered f
  join stats s using (hk_well)
)
select
  w.well_id,
  depth_bucket,
  avg(z) as avg_z
from scored
join public.dim_well w using (hk_well)
group by 1,2
order by 2,1
""", params=params)

if heat.empty:
    st.info("No readings available for anomaly heatmap with current filters.")
else:
    pivot = heat.pivot(index="depth_bucket", columns="well_id", values="avg_z").fillna(0)
    if pivot.empty:
        st.info("Heatmap has no values to display yet.")
    else:
        fig_hm = px.imshow(pivot, aspect="auto")
        st.plotly_chart(fig_hm, use_container_width=True)

st.divider()

# ============================================================
# Marts (filtered by wells / source_format / survey_type if possible)
# ============================================================
st.subheader("mart_well_performance (filtered)")
# This mart already has source_format and hk_well, so we can filter by wells + source_format.
where = []
params = {}
if selected_wells:
    where.append("w.well_id = ANY(:wells)")
    params["wells"] = selected_wells
if selected_sources:
    where.append("m.source_format = ANY(:sources)")
    params["sources"] = selected_sources
where_sql = (" where " + " and ".join(where)) if where else ""

m1 = qdf(f"""
select w.well_id, m.source_format, m.total_readings, m.avg_amplitude, m.data_quality_rate
from public.mart_well_performance m
join public.dim_well w using (hk_well)
{where_sql}
order by total_readings desc
""", params=params)

st.dataframe(m1, use_container_width=True)

st.subheader("mart_sensor_analysis (survey_type as sensor type) — filtered")
# Filter by selected_survey_types if user picks them
where = []
params = {}
if selected_survey_types:
    where.append("s.survey_type_id = ANY(:survey_types)")
    params["survey_types"] = selected_survey_types
where_sql = (" where " + " and ".join(where)) if where else ""

m2 = qdf(f"""
select s.survey_type_id as sensor_type_id, m.total_readings, m.data_quality_rate, m.avg_amplitude
from public.mart_sensor_analysis m
join public.dim_survey_type s on s.hk_survey_type = m.hk_sensor_type
{where_sql}
order by total_readings desc
""", params=params)

st.dataframe(m2, use_container_width=True)

st.subheader("mart_survey_summary — filtered")
# Filter by survey_type_id + source_format
where = []
params = {}
if selected_sources:
    where.append("m.source_format = ANY(:sources)")
    params["sources"] = selected_sources
if selected_survey_types:
    where.append("s.survey_type_id = ANY(:survey_types)")
    params["survey_types"] = selected_survey_types
where_sql = (" where " + " and ".join(where)) if where else ""

m3 = qdf(f"""
select s.survey_type_id, m.source_format, m.wells_surveyed, m.total_readings, m.avg_amplitude,
       m.first_ingest_ts, m.last_ingest_ts
from public.mart_survey_summary m
join public.dim_survey_type s using (hk_survey_type)
{where_sql}
order by total_readings desc
""", params=params)

st.dataframe(m3, use_container_width=True)