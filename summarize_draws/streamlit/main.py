import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import numpy as np

# ----------------------
# Load data and geojson
# ----------------------

@st.cache_data
def load_data():
    file_path = "MSM - aggregated.xlsx"
    return pd.read_excel(file_path)

@st.cache_data
def load_geojson():
    # US State boundaries GeoJSON
    url = "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json"
    return requests.get(url).json()

st.set_page_config(layout="wide")

# Load data
df = load_data()
df.columns = ["Demographic", "Demographic Level", "Division", "Type", "Geography", "mean", "median", "q025", "q975"]

# Replace NaN demographics with "All"
df.loc[df["Demographic Level"].isnull(), "Demographic Level"] = "All"
df.loc[df["Demographic"].isnull(), "Demographic"] = "All"

# Mapping dictionaries for demographic levels
AGE_MAP = {1: "18–24", 2: "25–34", 3: "35–44", 4: "45–54", 5: "55+"}
INCOME_MAP = {1: "< $20k", 2: "$20k–$40k", 3: "$40k–$90k", 4: "$90k+"}
EDUC_MAP = {1: "College", 2: "Graduate", 3: "HS/GED", 4: "Less HS"}
RACE_MAP = {1: "Black", 2: "Other", 3: "White"}

# ----------------------
# Streamlit Sidebar
# ----------------------

st.sidebar.title("MSM Prevalence Explorer")

# Demographic filter
demo_var = st.sidebar.selectbox(
    "Select Demographic Variable",
    sorted(df["Demographic"].unique().tolist())
)

# Geographic level filter
geo_level = st.sidebar.radio(
    "Select Geography Level",
    ["state", "county"]
)

# Statistic type filter
stat_type = st.sidebar.radio(
    "Select Statistic Type",
    ["rate", "count"]
)

# Base filtering
filtered_df = df[
    (df["Demographic"] == demo_var) &
    (df["Type"] == stat_type)
]
filtered_df = filtered_df[filtered_df["Division"] == geo_level]

# Optional geography filter
if geo_level in ["state", "county"]:
    geo_options = st.sidebar.multiselect(
        f"Select {geo_level}(s)",
        filtered_df["Geography"].unique()
    )
    if geo_options:
        filtered_df = filtered_df[filtered_df["Geography"].isin(geo_options)]

# Special handling: "All" should only include rows where both fields are "All"
if demo_var == "All":
    filtered_df = filtered_df[
        (filtered_df["Demographic Level"] == "All")
    ]

# Demographic-specific filters
if demo_var == "age":
    filtered_df["Demographic Level"] = filtered_df["Demographic Level"].replace(AGE_MAP)
    levels = filtered_df["Demographic Level"].unique().tolist()
    selection = st.sidebar.multiselect("Select Age Ranges", levels, default=levels)
elif demo_var == "income":
    filtered_df["Demographic Level"] = filtered_df["Demographic Level"].replace(INCOME_MAP)
    levels = filtered_df["Demographic Level"].unique().tolist()
    selection = st.sidebar.multiselect("Select Income Ranges", levels, default=levels)
elif demo_var == "educ":
    filtered_df["Demographic Level"] = filtered_df["Demographic Level"].replace(EDUC_MAP)
    levels = filtered_df["Demographic Level"].unique().tolist()
    selection = st.sidebar.multiselect("Select Education Levels", levels, default=levels)
elif demo_var == "race":
    filtered_df["Demographic Level"] = filtered_df["Demographic Level"].replace(RACE_MAP)
    levels = filtered_df["Demographic Level"].unique().tolist()
    selection = st.sidebar.multiselect("Select Race Categories", levels, default=levels)
else:
    levels = []
    selection = None

# Apply subgroup level selection (if relevant)
if selection:
    filtered_df = filtered_df[filtered_df["Demographic Level"].isin(selection)]

# ----------------------
# Main Panel
# ----------------------

st.title("U.S. MSM Prevalence Dashboard")
st.write(f"### Analysis of: **{demo_var}** — {geo_level} Level ({stat_type})")

# ----------------------
# Helper Function
# ----------------------

def make_adaptive_bins(values, max_bins=6):
    """
    Create adaptive, human-readable percent bins based on value range.
    values: pandas Series of floats (e.g., 0.032 = 3.2%)
    """
    lo, hi = values.min(), values.max()
    if lo == hi:
        lo -= 0.005
        hi += 0.005
    lo = max(0, lo - 0.001)
    hi = min(1, hi + 0.001)

    span = hi - lo
    n_bins = min(max_bins, max(3, int(span / 0.015)))

    raw_bins = np.linspace(lo, hi, n_bins + 1)
    pct_bins = (raw_bins * 100).round(1)
    labels = [f"{pct_bins[i]}–{pct_bins[i+1]}%" for i in range(len(pct_bins) - 1)]
    return raw_bins, labels

# ----------------------
# Map Visualization
# ----------------------

if geo_level.lower() == "state" and stat_type == "rate":
    st.write("#### MSM Prevalence (Rate) by State")

    # state_df = filtered_df[filtered_df["Division"].str.lower() == "state"].copy()

    state_df = (
        filtered_df[filtered_df["Division"].str.lower() == "state"]
        .groupby("Geography", as_index=False)
        .agg({"median": "median"})
    )

    bins, labels = make_adaptive_bins(state_df["median"])
    state_df["rate_bin"] = pd.cut(state_df["median"], bins=bins, labels=labels, include_lowest=True, right=False)
    state_df = state_df[state_df["rate_bin"].notna()].copy()

    if state_df.empty:
        st.warning("No data available for this combination.")
        st.stop()

    state_df["rate_bin"] = state_df["rate_bin"].astype(str)
    valid_labels = sorted(state_df["rate_bin"].unique().tolist(), key=lambda x: float(x.split("–")[0].strip("%")))

    palette = px.colors.sequential.Plasma[-len(valid_labels):]


    geojson = load_geojson()

    state_df["rate_pct"] = (state_df["median"] * 100).round(2)

    fig = px.choropleth(
        state_df,
        geojson=geojson,
        locations="Geography",
        featureidkey="properties.name",
        color="rate_bin",
        category_orders={"rate_bin": valid_labels},
        color_discrete_sequence=palette,
        locationmode="USA-states",
        scope="usa",
        hover_name="Geography",  # State name or code in hover
        hover_data={
            "rate_pct": True,     # show percentage
            "median": False,      # hide raw median
            "Geography": False,   # already shown via hover_name
        },
        labels={"rate_bin": "MSM (%)"},
        title=f"MSM Proportion by State — {demo_var}"
    )

    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>MSM Rate (Median): %{customdata[0]}%<extra></extra>"
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(height=700, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

elif geo_level == "county":
    st.write("#### County-level mapping is not available yet.")
    st.warning("Showing data in tabular format only below...")

# ----------------------
# Filtered Data Table
# ----------------------

if st.checkbox("Show Filtered Data Table"):
    display_df = filtered_df.copy()
    display_df[["mean", "median", "q025", "q975"]] = display_df[["mean", "median", "q025", "q975"]].round(3)

    if demo_var == "All":
        filtered_df = (
        filtered_df
        .groupby(["Geography", "Division", "Type"], as_index=False)
        .agg({"median": "median", "q025": "median", "q975": "median"})
    )
        display_df = display_df[["Geography", "median", "q025", "q975"]]
    else:
        display_df = display_df[["Demographic Level", "Geography", "median", "q025", "q975"]]

    st.write("#### Filtered Data:")
    st.dataframe(display_df.sort_values(by="median", ascending=False), use_container_width=True)

    # Add CSV download button
    csv = display_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download filtered data as CSV",
        data=csv,
        file_name=f"filtered_{demo_var}_{geo_level}_{stat_type}.csv",
        mime="text/csv"
    )