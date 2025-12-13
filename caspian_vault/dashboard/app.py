import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px

DB = "/home/hackathon/warehouse/caspian.duckdb"

st.set_page_config(page_title="Caspian Seismic Analytics", layout="wide")
st.title("CaspianPetro — Seismic Analytics Dashboard")

con = duckdb.connect(DB, read_only=True)

# ---- KPIs ----
kpi = con.execute("""
select
  (select count(*) from dim_well) as wells,
  (select count(*) from fct_readings) as readings,
  (select avg(case when quality_flag=1 then 1 else 0 end) from fct_readings) as quality_rate,
  (select avg(amplitude) from fct_readings) as avg_amp
""").df()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Wells", int(kpi["wells"][0]))
c2.metric("Total readings", int(kpi["readings"][0]))
c3.metric("Data quality rate", f"{float(kpi['quality_rate'][0])*100:.2f}%")
c4.metric("Avg amplitude", f"{float(kpi['avg_amp'][0]):.4f}")

st.divider()

# ---- Map of wells ----
st.subheader("Wells map")
wells = con.execute("select well_id, lat, lon from dim_well").df()
fig_map = px.scatter_mapbox(
    wells, lat="lat", lon="lon", hover_name="well_id", zoom=4, height=420
)
fig_map.update_layout(mapbox_style="open-street-map", margin=dict(l=0,r=0,t=0,b=0))
st.plotly_chart(fig_map, use_container_width=True)

st.divider()

# ---- Amplitude distribution ----
st.subheader("Amplitude distribution")
amp = con.execute("select amplitude, quality_flag from fct_readings").df()
fig_hist = px.histogram(amp, x="amplitude", color="quality_flag", nbins=60)
st.plotly_chart(fig_hist, use_container_width=True)

# ---- Anomaly heatmap (simple) ----
# anomaly score = |amplitude - mean(well)| / std(well)
st.subheader("Anomaly heatmap (depth bucket × well)")
heat = con.execute("""
with stats as (
  select hk_well, avg(amplitude) as mu, stddev_samp(amplitude) as sigma
  from fct_readings
  group by 1
),
scored as (
  select
    f.hk_well,
    cast(floor(depth_ft/50)*50 as int) as depth_bucket,
    case
      when s.sigma is null or s.sigma = 0 then 0
      else abs(f.amplitude - s.mu) / s.sigma
    end as z
  from fct_readings f
  join stats s using (hk_well)
)
select
  w.well_id,
  depth_bucket,
  avg(z) as avg_z
from scored
join dim_well w using (hk_well)
group by 1,2
""").df()

# pivot for heatmap
pivot = heat.pivot(index="depth_bucket", columns="well_id", values="avg_z").fillna(0)
fig_hm = px.imshow(pivot, aspect="auto")
st.plotly_chart(fig_hm, use_container_width=True)

st.divider()

# ---- Marts ----
st.subheader("mart_well_performance")
m1 = con.execute("""
select w.well_id, m.source_format, m.total_readings, m.avg_amplitude, m.data_quality_rate
from mart_well_performance m
join dim_well w using (hk_well)
order by total_readings desc
""").df()
st.dataframe(m1, use_container_width=True)

st.subheader("mart_sensor_analysis (survey_type as sensor type)")
m2 = con.execute("""
select s.survey_type_id as sensor_type_id, m.total_readings, m.data_quality_rate, m.avg_amplitude
from mart_sensor_analysis m
join dim_survey_type s on s.hk_survey_type = m.hk_sensor_type
order by total_readings desc
""").df()
st.dataframe(m2, use_container_width=True)

st.subheader("mart_survey_summary")
m3 = con.execute("""
select s.survey_type_id, m.source_format, m.wells_surveyed, m.total_readings, m.avg_amplitude,
       m.first_ingest_ts, m.last_ingest_ts
from mart_survey_summary m
join dim_survey_type s using (hk_survey_type)
order by total_readings desc
""").df()
st.dataframe(m3, use_container_width=True)
