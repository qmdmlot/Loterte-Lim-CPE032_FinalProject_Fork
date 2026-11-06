"""Streamlit dashboard: real-time Kafka view + historical MongoDB Atlas view.

Layout follows the course template:
    setup_sidebar()            connection settings and refresh controls
    consume_kafka_data()       real-time processing (Kafka -> rolling buffers)
    query_historical_data()    historical retrieval (MongoDB Atlas)
    display_real_time_view()   "Real-Time" tab
    display_historical_view()  "Historical" tab (filters, aggregation, export)
    main()

Run with:  streamlit run app.py
"""
import io
import json
import logging
import os
import random
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from math import sin, pi
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Weather Stream Dashboard", page_icon="⛅", layout="wide")
load_dotenv()
logger = logging.getLogger("dashboard")

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "streaming-data")
MONGODB_URI = os.getenv("MONGODB_URI", "")
MONGODB_DB = os.getenv("MONGODB_DB", "streaming_db")
DISPLAY_TZ = os.getenv("DISPLAY_TZ", "Asia/Manila")
TZ = ZoneInfo(DISPLAY_TZ)
BUFFER_SIZE = 200
SEED_WINDOW_SECONDS = 600  # on first connect, replay the last 10 minutes so charts are not empty

METRICS = {
    "temperature_c": {"label": "Temperature", "unit": "°C"},
    "feelslike_c": {"label": "Feels like", "unit": "°C"},
    "humidity_pct": {"label": "Humidity", "unit": "%"},
    "wind_kph": {"label": "Wind speed", "unit": "kph"},
    "pressure_mb": {"label": "Pressure", "unit": "mb"},
    "uv_index": {"label": "UV index", "unit": ""},
}
PALETTE = ["#4f8cff", "#34d399", "#fbbf24", "#f472b6", "#a78bfa", "#fb923c", "#22d3ee", "#a3e635"]


def _slug(city: str) -> str:
    return "_".join("".join(ch if ch.isalnum() else " " for ch in city.lower()).split())


SENSORS = [_slug(c) for c in os.getenv("CITIES", "Quezon City,Manila,Cebu,Davao").split(",") if c.strip()]
EMPTY_COLUMNS = ["timestamp", "value", "metric_type", "sensor_id"]


def pretty(sensor_id: str) -> str:
    return sensor_id.replace("_", " ").title()


def color_for(sensor_id: str) -> str:
    if sensor_id in SENSORS:
        return PALETTE[SENSORS.index(sensor_id) % len(PALETTE)]
    return PALETTE[sum(map(ord, sensor_id)) % len(PALETTE)]


def metric_label(metric: str) -> str:
    m = METRICS.get(metric, {"label": metric, "unit": ""})
    return f"{m['label']} ({m['unit']})" if m["unit"] else m["label"]


def hex_to_rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i : i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


CSS = """
<style>
.block-container {padding-top: 1.6rem;}
div[data-testid="stMetric"] {
    background: rgba(128,128,128,.08); border: 1px solid rgba(128,128,128,.25);
    padding: 14px 16px; border-radius: 14px;
}
.live-badge {
    display:inline-flex; align-items:center; gap:8px; padding:5px 12px; border-radius:999px;
    font-weight:700; font-size:.8rem; color:#f87171; background:rgba(248,113,113,.12);
    border:1px solid rgba(248,113,113,.4);
}
.live-badge.paused {color:#fbbf24; background:rgba(251,191,36,.12); border-color:rgba(251,191,36,.4);}
.live-dot {width:9px; height:9px; border-radius:50%; background:currentColor; animation:pulse 1.4s infinite;}
@keyframes pulse {0%{box-shadow:0 0 0 0 currentColor;} 70%{box-shadow:0 0 0 8px transparent;} 100%{box-shadow:0 0 0 0 transparent;}}
</style>
"""


# ----------------------------------------------------------------------------
# Sidebar
# ----------------------------------------------------------------------------
def setup_sidebar() -> dict:
    """Render connection settings and refresh controls; return the chosen settings."""
    with st.sidebar:
        st.title("⛅ Weather Stream")
        st.caption("Kafka · Spark · MongoDB Atlas")

        st.subheader("Kafka")
        bootstrap = st.text_input("Bootstrap servers", value=DEFAULT_BOOTSTRAP, key="kafka_bootstrap")
        topic = st.text_input("Topic", value=DEFAULT_TOPIC, key="kafka_topic")

        st.subheader("Storage")
        st.text_input("Storage backend", value="MongoDB Atlas", disabled=True, key="storage_backend")

        st.subheader("Refresh")
        auto_refresh = st.toggle("Auto-refresh", value=True, key="auto_refresh")
        refresh_seconds = st.select_slider("Interval (seconds)", options=[5, 10, 15, 30, 60], value=15, key="refresh_seconds")
        paused = st.toggle("Pause live stream", key="paused", help="Stops polling Kafka and auto-refresh.")
        st.button("🔄 Refresh now")

        with st.expander("Developer options"):
            demo_mode = st.checkbox(
                "Demo data (no Kafka)", key="demo_mode",
                help="Shows clearly labelled synthetic readings for UI work when Kafka is not running.",
            )
    return {
        "bootstrap": bootstrap.strip(),
        "topic": topic.strip(),
        "auto_refresh": auto_refresh,
        "refresh_seconds": refresh_seconds,
        "paused": paused,
        "demo_mode": demo_mode,
    }


def render_pipeline_status(settings: dict) -> None:
    store = get_live_store()
    ok, _ = check_mongo() if MONGODB_URI else (False, "")
    with st.sidebar:
        st.divider()
        st.caption("PIPELINE STATUS")
        if settings["demo_mode"]:
            st.info("Kafka: demo mode")
        elif store["error"]:
            st.error("Kafka: offline")
        elif store["consumer"] is not None:
            st.success("Kafka: connected")
        else:
            st.info("Kafka: idle")
        if ok:
            st.success("MongoDB Atlas: connected")
        else:
            st.warning("MongoDB Atlas: not connected")
        if store["last_msg"]:
            st.caption(f"Last message: {store['last_msg']:%H:%M:%S}  |  received: {store['total']:,}")


# ----------------------------------------------------------------------------
# Real-time processing: Kafka
# ----------------------------------------------------------------------------
@st.cache_resource
def get_live_store() -> dict:
    """Shared across reruns: consumer, rolling buffers, status."""
    return {
        "lock": threading.Lock(),
        "config": None,
        "consumer": None,
        "buffers": {},  # (sensor_id, metric_type) -> deque of records
        "error": None,
        "last_msg": None,
        "total": 0,
    }


def _make_consumer(bootstrap: str, topic: str) -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap.split(","),
        group_id=None,
        enable_auto_commit=False,
        api_version_auto_timeout_ms=3000,
        request_timeout_ms=15000,
    )
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        raise RuntimeError(f"Topic '{topic}' not found. Create it and start producer.py.")
    tps = [TopicPartition(topic, p) for p in sorted(partitions)]
    consumer.assign(tps)
    since_ms = int((time.time() - SEED_WINDOW_SECONDS) * 1000)
    offsets = consumer.offsets_for_times({tp: since_ms for tp in tps})
    for tp in tps:
        found = offsets.get(tp)
        if found is None:
            consumer.seek_to_end(tp)
        else:
            consumer.seek(tp, found.offset)
    return consumer


def _parse_message(raw: bytes):
    """Validate one Kafka message. Returns a record dict, or None if malformed."""
    try:
        rec = json.loads(raw.decode("utf-8"))
        value = rec["value"]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        ts = pd.Timestamp(rec["timestamp"])
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        return {
            "timestamp": ts,
            "value": float(value),
            "metric_type": str(rec["metric_type"]),
            "sensor_id": str(rec["sensor_id"]),
        }
    except (ValueError, KeyError, TypeError, UnicodeDecodeError, AttributeError):
        return None


def generate_sample_data(n_points: int = 30) -> pd.DataFrame:
    """Synthetic readings for UI development only. Used when 'Demo data' is ticked."""
    now = datetime.now(timezone.utc)
    rows = []
    for city in SENSORS:
        offset = (sum(map(ord, city)) % 7) / 4
        for i in range(n_points):
            ts = now - timedelta(seconds=15 * (n_points - i))
            hour = ts.astimezone(TZ).hour + ts.minute / 60
            base = {"temperature_c": 30, "feelslike_c": 34, "humidity_pct": 74, "wind_kph": 12, "pressure_mb": 1010, "uv_index": 5}
            amp = {"temperature_c": 3, "feelslike_c": 4, "humidity_pct": -9, "wind_kph": 4, "pressure_mb": 3, "uv_index": 4}
            for metric in METRICS:
                v = base[metric] + amp[metric] * sin((hour - 9) / 24 * 2 * pi) + offset + random.uniform(-0.4, 0.4)
                rows.append({"timestamp": ts, "value": round(max(v, 0), 2), "metric_type": metric, "sensor_id": city})
    df = pd.DataFrame(rows, columns=EMPTY_COLUMNS)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(TZ)
    return df.sort_values("timestamp").reset_index(drop=True)


def consume_kafka_data(bootstrap: str, topic: str, poll: bool = True):
    """Drain new Kafka messages into rolling buffers.

    Returns (DataFrame of buffered records in the display timezone, error message or None).
    Connection errors are captured so the page keeps running and retries on the next refresh.
    """
    store = get_live_store()
    with store["lock"]:
        if store["config"] != (bootstrap, topic):  # settings changed: start clean
            if store["consumer"] is not None:
                try:
                    store["consumer"].close()
                except Exception:
                    pass
            store.update(config=(bootstrap, topic), consumer=None, buffers={}, error=None, last_msg=None, total=0)
        if poll:
            try:
                if store["consumer"] is None:
                    store["consumer"] = _make_consumer(bootstrap, topic)
                batches = store["consumer"].poll(timeout_ms=1000, max_records=1000)
                for msgs in batches.values():
                    for msg in msgs:
                        rec = _parse_message(msg.value)
                        if rec is None:
                            continue  # skip malformed messages
                        key = (rec["sensor_id"], rec["metric_type"])
                        store["buffers"].setdefault(key, deque(maxlen=BUFFER_SIZE)).append(rec)
                        store["total"] += 1
                        store["last_msg"] = datetime.now(TZ)
                store["error"] = None
            except Exception as exc:  # broker down, topic missing, etc.
                store["error"] = f"{type(exc).__name__}: {exc}"
                logger.warning("Kafka error: %s", store["error"])
                if store["consumer"] is not None:
                    try:
                        store["consumer"].close()
                    except Exception:
                        pass
                    store["consumer"] = None
        records = [r for buf in store["buffers"].values() for r in buf]
        error = store["error"]
    df = pd.DataFrame(records, columns=EMPTY_COLUMNS)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(TZ)
        df = df.sort_values("timestamp").reset_index(drop=True)
    return df, error


# ----------------------------------------------------------------------------
# Historical retrieval: MongoDB Atlas
# ----------------------------------------------------------------------------
@st.cache_resource
def get_mongo_client() -> MongoClient:
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_URI is not set. Copy .env.example to .env and fill it in.")
    return MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000, tz_aware=True)


@st.cache_data(ttl=15, show_spinner=False)
def check_mongo():
    try:
        get_mongo_client().admin.command("ping")
        return True, "Connected"
    except Exception as exc:
        return False, type(exc).__name__


@st.cache_data(ttl=30, show_spinner=False)
def query_historical_data(sensors: tuple, metric: str, start: datetime, end: datetime, granularity: str) -> pd.DataFrame:
    """Fetch history from MongoDB Atlas.

    granularity: 'Raw'; '1 minute' / '5 minutes' (windowed aggregates written by Spark);
                 '1 hour' (resampled with pandas from raw readings).
    Returns columns: timestamp (UTC), sensor_id, metric_type, value [, min, max, count].
    """
    db = get_mongo_client()[MONGODB_DB]
    if granularity in ("1 minute", "5 minutes"):
        minutes = 1 if granularity == "1 minute" else 5
        cursor = db["readings_agg"].find(
            {
                "metric_type": metric,
                "sensor_id": {"$in": list(sensors)},
                "window_minutes": minutes,
                "window_start": {"$gte": start, "$lte": end},
            },
            {"_id": 0, "window_end": 0, "window_minutes": 0},
        ).sort("window_start", 1)
        df = pd.DataFrame(list(cursor))
        if df.empty:
            return pd.DataFrame(columns=["timestamp", "sensor_id", "metric_type", "value", "min", "max", "count"])
        df = df.rename(columns={"window_start": "timestamp", "avg": "value"})
    else:
        cursor = db["readings"].find(
            {
                "metric_type": metric,
                "sensor_id": {"$in": list(sensors)},
                "timestamp": {"$gte": start, "$lte": end},
            },
            {"_id": 0},
        ).sort("timestamp", 1)
        df = pd.DataFrame(list(cursor))
        if df.empty:
            return pd.DataFrame(columns=["timestamp", "sensor_id", "metric_type", "value"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        if granularity == "1 hour":
            df = (
                df.set_index("timestamp")
                .groupby("sensor_id")["value"]
                .resample("1h")
                .agg(["mean", "min", "max", "count"])
                .reset_index()
                .rename(columns={"mean": "value"})
                .dropna(subset=["value"])
            )
            df["metric_type"] = metric
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    cols = ["timestamp", "sensor_id", "metric_type", "value"] + [c for c in ("min", "max", "count") if c in df.columns]
    return df[cols].sort_values("timestamp").reset_index(drop=True)


# ----------------------------------------------------------------------------
# Export helpers
# ----------------------------------------------------------------------------
def summarize(df: pd.DataFrame) -> pd.DataFrame:
    s = df.groupby("sensor_id")["value"].agg(["count", "min", "mean", "max", "std"]).round(2).reset_index()
    s["sensor_id"] = s["sensor_id"].map(pretty)
    return s.rename(columns={"sensor_id": "city"})


@st.cache_data(show_spinner=False)
def build_xlsx(df: pd.DataFrame, summary: pd.DataFrame, filters_text: str) -> bytes:
    data = df.copy()
    data["timestamp"] = data["timestamp"].dt.tz_localize(None)  # Excel has no timezone support
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        data.to_excel(writer, sheet_name="Data", index=False)
        summary.to_excel(writer, sheet_name="Summary", index=False, startrow=2)
        writer.sheets["Summary"]["A1"] = filters_text
        for ws in writer.sheets.values():
            for col in ws.columns:
                width = max(len(str(c.value)) if c.value is not None else 0 for c in col[:200])
                ws.column_dimensions[col[0].column_letter].width = min(max(width + 2, 10), 60)
        writer.sheets["Data"].freeze_panes = "A2"
    return buf.getvalue()


@st.cache_data(show_spinner=False)
def build_chart_png(df: pd.DataFrame, metric: str, granularity: str) -> bytes:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 4.6), dpi=130)
    for sensor, g in df.groupby("sensor_id"):
        g = g.sort_values("timestamp")
        g = g.iloc[:: max(1, len(g) // 3000)]
        ax.plot(g["timestamp"].dt.tz_localize(None), g["value"], label=pretty(sensor), color=color_for(sensor), linewidth=1.6)
    ax.set_title(f"{metric_label(metric)} - {granularity}")
    ax.set_ylabel(metric_label(metric))
    ax.grid(alpha=0.3)
    ax.legend(loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()


def _ascii(text) -> str:
    """The built-in PDF fonts only cover latin-1; replace anything else."""
    return str(text).encode("latin-1", "replace").decode("latin-1")


@st.cache_data(show_spinner=False)
def build_pdf(df: pd.DataFrame, metric: str, filters_text: str, chart_png: bytes, summary: pd.DataFrame) -> bytes:
    from fpdf import FPDF

    def table(pdf, headers, rows, widths):
        pdf.set_font("Helvetica", "B", 9)
        for h, w in zip(headers, widths):
            pdf.cell(w, 7, _ascii(h), border=1)
        pdf.ln()
        pdf.set_font("Helvetica", "", 9)
        for row in rows:
            for val, w in zip(row, widths):
                pdf.cell(w, 6, _ascii(val), border=1)
            pdf.ln()

    pdf = FPDF()
    pdf.set_auto_page_break(True, 15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 10, "Weather Stream - History Report", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(
        0, 6,
        _ascii(f"Generated: {datetime.now(TZ):%Y-%m-%d %H:%M} ({DISPLAY_TZ})\nMetric: {METRICS[metric]['label']}\n{filters_text}"),
        new_x="LMARGIN", new_y="NEXT",
    )
    pdf.ln(2)
    pdf.image(io.BytesIO(chart_png), w=190)
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Summary by city", new_x="LMARGIN", new_y="NEXT")
    table(pdf, ["City", "Count", "Min", "Mean", "Max", "Std"], summary.fillna("").values.tolist(), [50, 25, 25, 25, 25, 25])
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Latest 25 rows", new_x="LMARGIN", new_y="NEXT")
    recent = df.sort_values("timestamp", ascending=False).head(25)
    rows = [[r.timestamp.strftime("%Y-%m-%d %H:%M:%S"), pretty(r.sensor_id), f"{r.value:.2f}"] for r in recent.itertuples()]
    table(pdf, ["Timestamp", "City", "Value"], rows, [70, 60, 40])
    return bytes(pdf.output())


# ----------------------------------------------------------------------------
# Charts
# ----------------------------------------------------------------------------
def live_line_chart(df: pd.DataFrame, metric: str) -> go.Figure:
    fig = go.Figure()
    for sensor, g in df.groupby("sensor_id"):
        fig.add_trace(
            go.Scatter(
                x=g["timestamp"], y=g["value"], mode="lines+markers", name=pretty(sensor),
                line=dict(color=color_for(sensor), width=2), marker=dict(size=4),
            )
        )
    fig.update_layout(
        height=380, margin=dict(l=10, r=10, t=30, b=10), hovermode="x unified",
        yaxis_title=metric_label(metric), legend=dict(orientation="h", y=-0.2),
    )
    return fig


def history_chart(df: pd.DataFrame, metric: str) -> go.Figure:
    fig = go.Figure()
    has_band = {"min", "max"}.issubset(df.columns) and df["min"].notna().any()
    for sensor, g in df.groupby("sensor_id"):
        color = color_for(sensor)
        if has_band:
            fig.add_trace(go.Scatter(x=g["timestamp"], y=g["max"], mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
            fig.add_trace(
                go.Scatter(
                    x=g["timestamp"], y=g["min"], mode="lines", line=dict(width=0), fill="tonexty",
                    fillcolor=hex_to_rgba(color, 0.15), showlegend=False, hoverinfo="skip",
                )
            )
        fig.add_trace(go.Scatter(x=g["timestamp"], y=g["value"], mode="lines", name=pretty(sensor), line=dict(color=color, width=2)))
    fig.update_layout(
        height=430, margin=dict(l=10, r=10, t=30, b=10), hovermode="x unified",
        yaxis_title=metric_label(metric), legend=dict(orientation="h", y=-0.3),
    )
    fig.update_xaxes(rangeslider_visible=True)
    return fig


# ----------------------------------------------------------------------------
# Views
# ----------------------------------------------------------------------------
def display_real_time_view(df: pd.DataFrame, error: str, settings: dict) -> None:
    badge = (
        '<span class="live-badge paused">⏸ PAUSED</span>'
        if settings["paused"]
        else '<span class="live-badge"><span class="live-dot"></span>LIVE</span>'
    )
    left, right = st.columns([4, 1])
    left.subheader("Real-Time Streaming View")
    right.markdown(f"<div style='text-align:right'>{badge}</div>", unsafe_allow_html=True)

    store = get_live_store()
    last = store["last_msg"].strftime("%H:%M:%S") if store["last_msg"] else "none yet"
    refresh_note = f"auto-refresh every {settings['refresh_seconds']} s" if settings["auto_refresh"] and not settings["paused"] else "auto-refresh off"
    st.caption(f"{refresh_note} · last message at {last} ({DISPLAY_TZ})")

    if settings["demo_mode"]:
        st.warning("DEMO DATA: these readings are synthetic and are not coming from Kafka.")
    elif error:
        st.error(f"Cannot read from Kafka ({settings['bootstrap']}, topic '{settings['topic']}'): {error}")
        st.caption("Is the broker running and is producer.py publishing? The page retries on every refresh.")
    if df.empty:
        st.info("Waiting for data… is producer.py running?")
        return

    c_metric, c_cities = st.columns([1, 2])
    metric = c_metric.selectbox("Metric", list(METRICS), format_func=metric_label, key="live_metric")
    available = sorted(df["sensor_id"].unique())
    cities = c_cities.multiselect("Cities", available, default=available, format_func=pretty, key="live_cities")
    if not cities:
        st.warning("Select at least one city.")
        return

    view = df[(df["metric_type"] == metric) & (df["sensor_id"].isin(cities))]
    if view.empty:
        st.info("No readings for this metric and city selection yet.")
        return

    unit = METRICS[metric]["unit"]
    cols = st.columns(min(4, len(cities)))
    for i, sensor in enumerate(cities):
        series = view[view["sensor_id"] == sensor]["value"].tolist()
        with cols[i % len(cols)]:
            if series:
                delta = f"{series[-1] - series[-2]:+.2f} {unit}".strip() if len(series) > 1 else None
                st.metric(pretty(sensor), f"{series[-1]:.1f} {unit}".strip(), delta)
            else:
                st.metric(pretty(sensor), "–")

    c1, c2 = st.columns([2, 1])
    with c1:
        st.markdown(f"**{metric_label(metric)} · rolling stream**")
        st.plotly_chart(live_line_chart(view, metric), key="live_line")
    with c2:
        st.markdown("**City comparison (latest)**")
        latest = view.sort_values("timestamp").groupby("sensor_id").tail(1)
        bar = go.Figure(
            go.Bar(
                x=[pretty(s) for s in latest["sensor_id"]], y=latest["value"],
                marker_color=[color_for(s) for s in latest["sensor_id"]],
            )
        )
        bar.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10), yaxis_title=metric_label(metric))
        st.plotly_chart(bar, key="live_bar")

    st.markdown("**Recent messages**")
    recent = df[df["sensor_id"].isin(cities)].sort_values("timestamp", ascending=False).head(10).copy()
    recent["timestamp"] = recent["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    st.dataframe(recent[EMPTY_COLUMNS], hide_index=True)


def display_historical_view() -> None:
    st.subheader("Historical Data View")
    st.caption(f"Stored in MongoDB Atlas · times shown in {DISPLAY_TZ}")

    f1, f2, f3 = st.columns([1, 2, 1])
    metric = f1.selectbox("Metric", list(METRICS), format_func=metric_label, key="h_metric")
    sensors = f2.multiselect("Cities", SENSORS, default=SENSORS, format_func=pretty, key="h_sensors")
    granularity = f3.radio(
        "Granularity", ["Raw", "1 minute", "5 minutes", "1 hour"], key="h_gran",
        help="1 and 5 minute values are windowed aggregates computed by Spark. 1 hour is computed from raw data.",
    )
    g1, g2, g3 = st.columns([1, 2, 2])
    all_data = g1.checkbox("All collected data", value=True, key="h_all")
    date_range = None
    if not all_data:
        today = datetime.now(TZ).date()
        date_range = g2.date_input("Date range", value=(today - timedelta(days=1), today), key="h_dates")
    hours = g3.slider("Time of day (hour)", 0, 24, (0, 24), key="h_hours")

    if not sensors:
        st.warning("Select at least one city.")
        return

    if all_data:
        start = datetime(2000, 1, 1, tzinfo=timezone.utc)
        end = datetime.now(timezone.utc) + timedelta(days=1)
    else:
        if not isinstance(date_range, (tuple, list)) or len(date_range) != 2:
            st.info("Pick both a start and an end date.")
            return
        start = datetime.combine(date_range[0], datetime.min.time(), tzinfo=TZ).astimezone(timezone.utc)
        end = datetime.combine(date_range[1], datetime.max.time(), tzinfo=TZ).astimezone(timezone.utc)

    try:
        with st.spinner("Querying MongoDB Atlas…"):
            df = query_historical_data(tuple(sorted(sensors)), metric, start, end, granularity)
    except Exception as exc:
        st.error(f"Could not query MongoDB Atlas: {type(exc).__name__}: {exc}")
        st.caption("Check MONGODB_URI in .env and that your IP is allowed in Atlas Network Access.")
        return

    if df.empty:
        st.warning("No data for these filters. Widen the date range, pick other cities, or make sure the Spark job is running.")
        return

    df = df.copy()
    df["timestamp"] = df["timestamp"].dt.tz_convert(TZ)
    if hours != (0, 24):
        df = df[(df["timestamp"].dt.hour >= hours[0]) & (df["timestamp"].dt.hour < hours[1])]
        if df.empty:
            st.warning("No data in that time-of-day range.")
            return

    unit = METRICS[metric]["unit"]
    k = st.columns(5)
    k[0].metric("Records", f"{len(df):,}")
    k[1].metric("Minimum", f"{df['value'].min():.2f} {unit}".strip())
    k[2].metric("Maximum", f"{df['value'].max():.2f} {unit}".strip())
    k[3].metric("Average", f"{df['value'].mean():.2f} {unit}".strip())
    k[4].metric("Span", f"{df['timestamp'].min():%b %d} → {df['timestamp'].max():%b %d}")

    tab_chart, tab_table, tab_stats = st.tabs(["Chart", "Table", "Statistics"])
    with tab_chart:
        st.plotly_chart(history_chart(df, metric), key="hist_chart")
        if "min" in df.columns:
            st.caption("Shaded band shows the min–max range inside each window.")
    with tab_table:
        shown = df.copy()
        shown["sensor_id"] = shown["sensor_id"].map(pretty)
        st.dataframe(
            shown.sort_values("timestamp", ascending=False), hide_index=True,
            column_config={"timestamp": st.column_config.DatetimeColumn("timestamp", format="YYYY-MM-DD HH:mm:ss")},
        )
        st.caption(f"{len(shown):,} rows")
    with tab_stats:
        summary = summarize(df)
        st.dataframe(summary, hide_index=True)
        s1, s2 = st.columns(2)
        with s1:
            st.markdown("**Average by city**")
            means = df.groupby("sensor_id")["value"].mean().reset_index()
            fig = go.Figure(go.Bar(x=means["sensor_id"].map(pretty), y=means["value"], marker_color=[color_for(s) for s in means["sensor_id"]]))
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10), yaxis_title=metric_label(metric))
            st.plotly_chart(fig, key="stats_avg")
        with s2:
            st.markdown("**Average by hour of day**")
            fig = go.Figure()
            for sensor, g in df.groupby("sensor_id"):
                by_hour = g.groupby(g["timestamp"].dt.hour)["value"].mean()
                fig.add_trace(go.Scatter(x=by_hour.index, y=by_hour.values, mode="lines+markers", name=pretty(sensor), line=dict(color=color_for(sensor))))
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10), xaxis_title="Hour", yaxis_title=metric_label(metric), legend=dict(orientation="h", y=-0.3))
            st.plotly_chart(fig, key="stats_hour")

    st.markdown("**Export**")
    summary = summarize(df)
    range_text = "All collected data" if all_data else f"{date_range[0]} to {date_range[1]}"
    filters_text = (
        f"Cities: {', '.join(pretty(s) for s in sorted(sensors))} | Range: {range_text} | "
        f"Hours: {hours[0]}-{hours[1]} | Granularity: {granularity}"
    )
    stamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    e1, e2, e3, e4 = st.columns(4)
    try:
        chart_png = build_chart_png(df, metric, granularity)
        e1.download_button("⬇ CSV", df.to_csv(index=False).encode("utf-8"), f"weather_history_{stamp}.csv", "text/csv")
        e2.download_button(
            "⬇ Excel", build_xlsx(df, summary, filters_text), f"weather_history_{stamp}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        e3.download_button("⬇ PDF report", build_pdf(df, metric, filters_text, chart_png, summary), f"weather_report_{stamp}.pdf", "application/pdf")
        e4.download_button("⬇ Chart (PNG)", chart_png, f"weather_chart_{stamp}.png", "image/png")
    except Exception as exc:
        st.error(f"Could not prepare exports: {type(exc).__name__}: {exc}")


def display_about() -> None:
    st.subheader("About this project")
    st.markdown("#### 🌐 API used")
    st.write(
        "Data comes from **WeatherAPI.com** (`current.json` endpoint). Each call returns the current "
        "conditions for one city. `producer.py` splits every response into one Kafka message per metric: "
        "temperature, feels-like, humidity, wind speed, pressure and UV index."
    )
    st.markdown("#### 🔧 Pipeline implemented")
    st.graphviz_chart(
        """digraph G {
        rankdir=LR; node [shape=box, style="rounded,filled", fillcolor="#e8f0ff", fontname="Helvetica"];
        API [label="WeatherAPI.com"]; P [label="producer.py"]; K [label="Kafka topic"];
        S [label="Spark Structured\\nStreaming"]; M [label="MongoDB Atlas"];
        L [label="Real-Time tab"]; H [label="Historical tab"];
        API -> P -> K; K -> L; K -> S -> M -> H; }"""
    )
    st.markdown(
        "- **Kafka** carries live readings. The Real-Time tab consumes them directly and refreshes every 15 seconds.\n"
        "- **Spark** parses and validates each message, writes raw readings, and computes 1-minute and 5-minute "
        "windowed averages, minimums and maximums.\n"
        "- **MongoDB Atlas** stores raw and aggregated data for the Historical tab, which supports filtering and export."
    )
    st.markdown("#### 🧾 Message schema")
    st.table(
        pd.DataFrame(
            [
                ["timestamp", "ISO 8601 string", "2026-11-01T06:14:00+00:00"],
                ["value", "number", "29.4"],
                ["metric_type", "string", "temperature_c"],
                ["sensor_id", "string", "quezon_city"],
            ],
            columns=["Field", "Type", "Example"],
        )
    )
    st.markdown("#### 🎯 Significance of the dashboard")
    st.write(
        "The dashboard turns raw readings into information people can act on: spotting heat spikes, comparing "
        "cities, and seeing how conditions trend over hours or days. Separating the live view from the stored "
        "history shows how one streaming system serves both immediate monitoring and long-term analysis, a pattern "
        "that applies to any sensor or IoT workload."
    )


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> None:
    st.markdown(CSS, unsafe_allow_html=True)
    settings = setup_sidebar()

    if settings["auto_refresh"] and not settings["paused"]:
        st_autorefresh(interval=settings["refresh_seconds"] * 1000, key="auto_refresh_tick")

    st.title("⛅ Weather Streaming Dashboard")

    if settings["demo_mode"]:
        df, error = generate_sample_data(), None
    else:
        df, error = consume_kafka_data(settings["bootstrap"], settings["topic"], poll=not settings["paused"])

    render_pipeline_status(settings)

    tab_live, tab_history, tab_about = st.tabs(["🔴 Real-Time", "📊 Historical", "ℹ️ About"])
    with tab_live:
        display_real_time_view(df, error, settings)
    with tab_history:
        display_historical_view()
    with tab_about:
        display_about()


if __name__ == "__main__":
    main()
