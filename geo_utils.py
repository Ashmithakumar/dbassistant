import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

GEO_COLUMNS = ['city', 'region', 'state', 'country', 'location', 'province', 'territory']

def detect_geo_column(df: pd.DataFrame):
    for col in df.columns:
        if col.lower() in GEO_COLUMNS:
            return col
    return None

@st.cache_data
def geocode_locations(locations):
    geolocator = Nominatim(user_agent="geo_heatmap_app")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    latitudes = []
    longitudes = []
    for loc in locations:
        location = geocode(loc)
        if location:
            latitudes.append(location.latitude)
            longitudes.append(location.longitude)
        else:
            latitudes.append(None)
            longitudes.append(None)
    return latitudes, longitudes

def scale_bubble_sizes(series, min_size=5, max_size=40):
    if series.max() == series.min():
        return pd.Series([min_size + (max_size - min_size)/2] * len(series))
    normalized = (series - series.min()) / (series.max() - series.min())
    return normalized * (max_size - min_size) + min_size

def filter_india_data(df, lat_col, lon_col):
    india_bounds = {
        'min_lat': 6.5,
        'max_lat': 37.5,
        'min_lon': 68.0,
        'max_lon': 97.5
    }
    
    india_data = df[(df[lat_col] >= india_bounds['min_lat']) & 
                    (df[lat_col] <= india_bounds['max_lat']) & 
                    (df[lon_col] >= india_bounds['min_lon']) & 
                    (df[lon_col] <= india_bounds['max_lon'])]
    
    return india_data

def detect_currency_format(metric_col):
    if any(x in metric_col.lower() for x in ["dollar", "dollars", "$", "usd"]):
        return "$"
    return "₹"

def format_currency(value, currency_symbol):
    try:
        val = float(value)
    except:
        return value

    if currency_symbol == "$":
        return f"${val:,.0f}"
    else:
        iv = int(val)
        s = str(iv)[::-1]
        groups = [s[:3]] + [s[i:i+2] for i in range(3, len(s), 2)]
        return "₹" + ",".join(group[::-1] for group in groups[::-1])

def apply_currency_format(df, metric_col):
    currency_symbol = detect_currency_format(metric_col)
    df[metric_col] = df[metric_col].apply(lambda x: format_currency(x, currency_symbol))

def show_geo_heatmap(df: pd.DataFrame, geo_col: str, metric_col: str):
    st.write("🧪 Detected metric column:", metric_col)
    df = df.copy()
    df[geo_col] = df[geo_col].astype(str)
    apply_currency_format(df, metric_col)

    map_template = "plotly_white"
    land_color = "beige"
    ocean_color = "lightblue"
    font_color = "black"
    paper_bgcolor = "white"
    plot_bgcolor = "white"

    st.info(f"Geocoding {geo_col} locations...")
    latitudes, longitudes = geocode_locations(df[geo_col])
    df["latitude"] = latitudes
    df["longitude"] = longitudes
    df = df.dropna(subset=["latitude", "longitude", metric_col])

    india_df = filter_india_data(df, "latitude", "longitude")
    if len(india_df) == 0:
        st.warning("No data points found within India. Showing available data points.")
        india_df = df

    india_df["scaled_metric"] = scale_bubble_sizes(india_df[metric_col].str.replace(r"[^\d]", "", regex=True).astype(float))

    center_lat, center_lon = 22.5, 78.9
    zoom = 4

    fig = px.scatter_geo(
        india_df,
        lat="latitude",
        lon="longitude",
        size="scaled_metric",
        color=india_df["scaled_metric"],
        color_continuous_scale="Viridis",
        projection="natural earth",
        hover_data={
            geo_col: True,
            metric_col: True,
            "latitude": False,
            "longitude": False,
            "scaled_metric": False
        }
    )

    fig.update_layout(
        template=map_template,
        title=dict(
            text=f"India Geographic Heatmap of {metric_col} by {geo_col}",
            font=dict(size=20, color=font_color)
        ),
        font=dict(color=font_color),
        paper_bgcolor=paper_bgcolor,
        plot_bgcolor=plot_bgcolor,
        hoverlabel=dict(
            bgcolor="#264653",
            font_size=14,
            font_color="#f1faee",
            font_family="Arial Black",
            bordercolor="#f1faee"
        ),
        coloraxis_colorbar=dict(
            title="Currency",
            tickprefix="₹" if "₹" in df[metric_col].iloc[0] else "$",
            ticks="outside"
        )
    )

    fig.update_geos(
        visible=False,
        resolution=50,
        showcountries=True,
        countrycolor="gray",
        showland=True,
        landcolor=land_color,
        showocean=True,
        oceancolor=ocean_color,
        center={"lat": center_lat, "lon": center_lon},
        projection_type="natural earth",
        lonaxis=dict(range=[68.0, 97.5]),
        lataxis=dict(range=[6.5, 37.5])
    )

    fig.add_trace(go.Scattergeo(
        lon=[center_lon],
        lat=[center_lat],
        mode='markers',
        marker=dict(size=0),
        hoverinfo='skip',
        showlegend=False
    ))

    st.plotly_chart(fig, use_container_width=True)