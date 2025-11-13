import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import numpy as np
import os

# ----------------------
# Load data and geojson
# ----------------------
@st.cache_data
def load_data():
    # Adjusted to your repo structure on Streamlit Cloud
    file_path = "MSM - aggregated.xlsx"
    return pd.read_excel(file_path)

@st.cache_data
def load_geojson():
    # US State boundaries GeoJSON (two-letter "name" codes)
    url = "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json"
    return requests.get(url).json()

st.set_page_config(layout="wide")

# ----------------------
# Helpers
# ----------------------
STATE_ABBR = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA","Colorado":"CO","Connecticut":"CT",
    "Delaware":"DE","District of Columbia":"DC","Florida":"FL","Georgia":"GA","Hawaii":"HI","Idaho":"ID","Illinois":"IL",
    "Indiana":"IN","Iowa":"IA","Kansas":"KS","Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD","Massachusetts":"MA",
    "Michigan":"MI","Minnesota":"MN","Mississippi":"MS","Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV",
    "New Hampshire":"NH","New Jersey":"NJ","New Mexico":"NM","New York":"NY","North Carolina":"NC","North Dakota":"ND",
    "Ohio":"OH","Oklahoma":"OK","Oregon":"OR","Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD",
    "Tennessee":"TN","Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA","Washington":"WA","West Virginia":"WV",
    "Wisconsin":"WI","Wyoming":"WY"
}

def short_county_label(full_label: str) -> str:
    """
    Convert 'Autauga County, Alabama' -> 'Autauga, AL'.
    Leaves unknowns unchanged. Special-case DC.
    """
    if not isinstance(full_label, str):
        return full_label
    s = full_label.strip()
    # Special-case DC variants
    if s.lower() in {"district of columbia, district of columbia", "washington, district of columbia"}:
        return "Washington, DC"
    # Split "County, State"
    if "," in s:
        left, state_full = [t.strip() for t in s.split(",", 1)]
        # Remove trailing "County" or "Parish" or "City and County"
        for suffix in [" County", " Parish", " City and County", " Borough", " Census Area"]:
            if left.endswith(suffix):
                left = left[: -len(suffix)]
        abbr = STATE_ABBR.get(state_full, None)
        return f"{left}, {abbr}" if abbr else s
    return s

def make_adaptive_bins(values, max_bins=6):
    lo, hi = values.min(), values.max()
    if lo == hi:
        lo -= 0.005; hi += 0.005
    lo = max(0, lo - 0.001); hi = min(1, hi + 0.001)
    span = hi - lo
    n_bins = min(max_bins, max(3, int(span / 0.015)))
    raw_bins = np.linspace(lo, hi, n_bins + 1)
    pct_bins = (raw_bins * 100).round(1)
    labels = [f"{pct_bins[i]}–{pct_bins[i+1]}%" for i in range(len(pct_bins) - 1)]
    return raw_bins, labels

def make_ci_scatter(ci_df, title, x_label="Percent (%)", y_label="", top_n=None, shorten_labels=False):
    """
    Horizontal CI scatter: y = label, x = median %, error bars = 95% CI.
    Expects columns: label, median, q025, q975 (0–1 scale).
    """
    df = ci_df.copy()
    if shorten_labels:
        df["label"] = df["label"].apply(short_county_label)

    # Sort by median high -> low
    df = df.sort_values("median", ascending=False)
    if top_n is not None:
        df = df.head(top_n)

    # Set ordered categorical for correct plotting
    df["label"] = pd.Categorical(df["label"], categories=df["label"].tolist(), ordered=True)

    # Percent scale + symmetric error-bars in percentage points
    df["median_pct"] = (df["median"] * 100.0).round(2)
    df["err_plus"]  = (df["q975"] - df["median"]) * 100.0
    df["err_minus"] = (df["median"] - df["q025"]) * 100.0

    fig = px.scatter(
        df,
        x="median_pct",
        y="label",
        error_x="err_plus",
        error_x_minus="err_minus",
        labels={"median_pct": x_label, "label": y_label},
        title=title,
    )
    fig.update_traces(marker=dict(size=7))
    
    # Dynamic height so labels never overlap
    row_height = 14
    fig.update_layout(
        title=title,
        xaxis_title=x_label,
        yaxis_title=y_label,
        height=max(500, int(len(df) * row_height)),
        margin=dict(l=0, r=0, t=60, b=0),
        yaxis=dict(
            categoryorder="array",
            categoryarray=df["label"].tolist(),
            automargin=True,
            tickmode="linear",     # Fix: don't skip labels
            dtick=1
        ),
        showlegend=False
    )
    return fig

# ----------------------
# Load + prepare data
# ----------------------
df = load_data()
df.columns = ["Demographic", "Demographic Level", "Division", "Type", "Geography", "mean", "median", "q025", "q975"]

# Normalize missing demo labels
df.loc[df["Demographic Level"].isnull(), "Demographic Level"] = "All"
df.loc[df["Demographic"].isnull(), "Demographic"] = "All"

# Mapping dictionaries for demographic levels (numeric -> human labels)
AGE_MAP   = {1: "18–24", 2: "25–34", 3: "35–44", 4: "45–54", 5: "55+"}
INCOME_MAP= {1: "< $20k", 2: "$20k–$40k", 3: "$40k–$90k", 4: "$90k+"}
EDUC_MAP  = {1: "College", 2: "Graduate", 3: "HS/GED", 4: "Less HS"}
RACE_MAP  = {1: "Black", 2: "Other", 3: "White"}

# ----------------------
# Sidebar
# ----------------------
st.sidebar.title("MSM Prevalence Explorer")

demo_var = st.sidebar.selectbox(
    "Select Demographic Variable",
    sorted(df["Demographic"].unique().tolist())
)

geo_level = st.sidebar.radio("Select Geography Level", ["state", "county"])
stat_type = st.sidebar.radio("Select Statistic Type", ["rate", "count"])

# Base filtering
filtered_df = df[(df["Demographic"] == demo_var) & (df["Type"] == stat_type)]
filtered_df = filtered_df[filtered_df["Division"] == geo_level]

# Optional geography filter
geo_options = st.sidebar.multiselect(
    f"Select {geo_level}(s)",
    filtered_df["Geography"].unique()
)
if geo_options:
    filtered_df = filtered_df[filtered_df["Geography"].isin(geo_options)]

# "All" should only include rows where demographic level is "All"
if demo_var == "All":
    filtered_df = filtered_df[(filtered_df["Demographic Level"] == "All")]

# Demographic-specific subgroup filters
selection = None
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

if selection:
    filtered_df = filtered_df[filtered_df["Demographic Level"].isin(selection)]

# ----------------------
# Main title
# ----------------------
st.title("U.S. MSM Prevalence Dashboard")
st.write(f"### Analysis of: **{demo_var}** — {geo_level} Level ({stat_type})")

# ----------------------
# Map Visualization (state + median bins; CI in hover)
# ----------------------
if geo_level.lower() == "state" and stat_type == "rate":
    st.write("#### MSM Prevalence (Rate) by State")

    # One row per state; keep median + CI for hover
    state_df = (
        filtered_df[filtered_df["Division"].str.lower() == "state"]
        .groupby("Geography", as_index=False)
        .agg({"median": "median", "q025": "median", "q975": "median"})
    )

    if state_df.empty:
        st.warning("No data available for this combination.")
    else:
        bins, labels = make_adaptive_bins(state_df["median"])
        state_df["rate_bin"] = pd.cut(state_df["median"], bins=bins, labels=labels, include_lowest=True, right=False)
        state_df = state_df[state_df["rate_bin"].notna()].copy()

        state_df["rate_bin"] = state_df["rate_bin"].astype(str)
        valid_labels = sorted(state_df["rate_bin"].unique().tolist(),
                              key=lambda x: float(x.split("–")[0].strip("%")))
        palette = px.colors.sequential.Plasma[-len(valid_labels):]

        geojson = load_geojson()

        # Percent values for hover (median and CI)
        state_df["median_pct"] = (state_df["median"] * 100).round(2)
        state_df["q025_pct"]   = (state_df["q025"]   * 100).round(2)
        state_df["q975_pct"]   = (state_df["q975"]   * 100).round(2)

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
            hover_name="Geography",
            hover_data={
                "median_pct": True,
                "q025_pct": True,
                "q975_pct": True,
                "rate_bin": False,
                "Geography": False
            },
            labels={"rate_bin": "MSM (%)"},
            title=f"MSM Proportion by State — {demo_var}"
        )

        fig.update_traces(
            hovertemplate="<b>%{hovertext}</b><br>"
                          "Median: %{customdata[0]}%<br>"
                          "95% CI: %{customdata[1]}–%{customdata[2]}%<extra></extra>"
        )
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(height=700, margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)

elif geo_level == "county":
    st.write("#### County-level mapping is not available yet.")
    st.warning("Showing data in tabular/CI format only below...")

# ----------------------
# Filtered Data Table
# ----------------------
if st.checkbox("Show Filtered Data Table"):
    display_df = filtered_df.copy()
    display_df[["mean", "median", "q025", "q975"]] = display_df[["mean", "median", "q025", "q975"]].round(3)

    if demo_var == "All":
        display_df = display_df[["Geography", "median", "q025", "q975"]]
    else:
        display_df = display_df[["Demographic Level", "Geography", "median", "q025", "q975"]]

    st.write("#### Filtered Data:")
    st.dataframe(display_df.sort_values(by="median", ascending=False), use_container_width=True)

# ----------------------
# Confidence Interval Plots (NEW)
# ----------------------
st.markdown("---")
show_ci = st.checkbox("Show Confidence Interval Chart", value=False)

x_axis_label = "Percent (%)" if stat_type == "rate" else "Count"

if show_ci:
    if geo_level.lower() == "state":
        # All states (no slider), sorted by median high->low
        ci_df = (
            filtered_df[filtered_df["Division"].str.lower() == "state"]
            .groupby("Geography", as_index=False)
            .agg({"median": "median", "q025": "median", "q975": "median"})
            .rename(columns={"Geography": "label"})
        )

        if ci_df.empty:
            st.info("No state-level data available for this selection.")
        else:
            fig_ci_state = make_ci_scatter(
                ci_df,
                title=f"State MSM Estimate Confidence Intervals — {demo_var}",
                x_label=x_axis_label,
                y_label="State",
                top_n=None,               # all states + DC
                shorten_labels=True       # still shortens (e.g., 'Washington, DC')
            )
            st.plotly_chart(fig_ci_state, use_container_width=True)

    elif geo_level.lower() == "county":
        # Sidebar slider to choose Top N counties by median rate
        max_n = st.sidebar.slider("Number of counties to display:", 10, 200, 40, step=5)

        ci_df = (
            filtered_df[filtered_df["Division"].str.lower() == "county"]
            .groupby("Geography", as_index=False)
            .agg({"median": "median", "q025": "median", "q975": "median"})
            .rename(columns={"Geography": "label"})
        )

        if ci_df.empty:
            st.info("No county-level data available for this selection.")
        else:
            fig_ci_county = make_ci_scatter(
                ci_df,
                title=f"Top {max_n} Counties by MSM Prevalence — {demo_var}",
                x_label="Percent (%)",
                y_label="County",
                top_n=max_n,
                shorten_labels=True
            )
            st.plotly_chart(fig_ci_county, use_container_width=True)