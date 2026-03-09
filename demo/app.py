"""
Hybrid Multi-Agent Orchestration — Live Architecture Demo
=========================================================
Streamlit UI that compares all 3 routing modes side by side.

Run with:
    streamlit run demo/app.py

Requires the API server to be running:
    python run.py   (or: uvicorn api.main:app --reload)
"""

import requests
import streamlit as st
import plotly.graph_objects as go

API_BASE = "http://localhost:8000"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Multi-Agent Orchestration Demo",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom styles ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
.mode-header { font-size: 1.1rem; font-weight: 700; margin-bottom: 0.2rem; }
.hop-request  { background: #1e3a5f !important; border-left: 4px solid #60a5fa; padding: 6px 10px; margin: 3px 0; border-radius: 4px; font-size: 0.82rem; color: #e2f0ff !important; }
.hop-response { background: #14432a !important; border-left: 4px solid #4ade80; padding: 6px 10px; margin: 3px 0; border-radius: 4px; font-size: 0.82rem; color: #dcfce7 !important; }
.hop-peer     { background: #4a3000 !important; border-left: 4px solid #fbbf24; padding: 6px 10px; margin: 3px 0; border-radius: 4px; font-size: 0.82rem; color: #fef3c7 !important; }
.hop-request b, .hop-response b, .hop-peer b { color: inherit !important; }
.hop-request i, .hop-response i, .hop-peer i { opacity: 0.85; }
.summary-box  { background: #1a1f2e !important; border: 1px solid #334155; border-radius: 8px; padding: 1rem; margin-top: 1rem; color: #e2e8f0 !important; }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("Hybrid Multi-Agent Orchestration")
st.markdown(
    "**Live architecture comparison** — same request, three different orchestration strategies."
)

st.markdown("""
| Mode | Bus | Routing | Execution |
|------|-----|---------|-----------|
| **Mode 1** | None | Direct calls | Sequential (bottleneck) |
| **Mode 2** | Redis Pub/Sub | Keyword matching | Parallel (fast) |
| **Mode 3** | Redis Pub/Sub | OpenRouter / DeepSeek v3.2 | Parallel + LLM |
""")

st.divider()

# ── Input ─────────────────────────────────────────────────────────────────────
example_inputs = [
    "Set up utilities and broadband at 123 Main Street, London",
    "I need electricity and internet connection at my new flat",
    "Can you arrange gas, electricity and fibre broadband for me?",
    "Help me move into my new home — sort out all utilities",
]

col_input, col_btn = st.columns([4, 1])
with col_input:
    user_input = st.text_input(
        "Your request (edit freely)",
        value=example_inputs[0],
        placeholder="e.g. Set up gas and WiFi at 42 Oak Lane",
        help="Type anything — the address is extracted automatically from what you type.",
    )
    if not user_input.strip():
        user_input = example_inputs[0]

with col_btn:
    st.markdown("<br>", unsafe_allow_html=True)
    run_btn = st.button("Run All 3 Modes", type="primary", use_container_width=True)

# ── Run comparison ────────────────────────────────────────────────────────────
if run_btn:
    with st.spinner("Running all 3 modes against the same request… (this takes ~10s)"):
        try:
            resp = requests.post(
                f"{API_BASE}/compare",
                json={"user_input": user_input},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.ConnectionError:
            st.error(
                "Cannot connect to API server. "
                "Start it first with: `python run.py`"
            )
            st.stop()
        except Exception as e:
            st.error(f"API error: {e}")
            st.stop()

    # Show what address was extracted from the input
    extracted_address = data.get("summary", {}).get("address_used", "")
    st.success(f"Comparison complete!  |  Address used: **{extracted_address or 'extracted from input'}**")

    m1 = data["mode1"]
    m2 = data["mode2"]
    m3 = data["mode3"]
    summary = data["summary"]

    # ── Timing comparison bar chart ───────────────────────────────────────────
    st.subheader("Response Time Comparison")

    colors = ["#ef4444", "#22c55e", "#3b82f6"]   # red, green, blue
    fig = go.Figure(go.Bar(
        x=["Mode 1\nNo Bus (Sequential)", "Mode 2\nRedis + Keywords", "Mode 3\nRedis + OpenRouter"],
        y=[m1["total_time"], m2["total_time"], m3["total_time"]],
        marker_color=colors,
        text=[f"{t:.2f}s" for t in [m1["total_time"], m2["total_time"], m3["total_time"]]],
        textposition="outside",
        width=0.5,
    ))
    fig.update_layout(
        yaxis_title="Response Time (seconds)",
        yaxis=dict(range=[0, max(m1["total_time"], m2["total_time"], m3["total_time"]) * 1.3]),
        plot_bgcolor="white",
        height=350,
        margin=dict(t=20, b=20),
        showlegend=False,
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="#f0f0f0")
    st.plotly_chart(fig, use_container_width=True)

    # ── Key metrics ───────────────────────────────────────────────────────────
    mc1, mc2, mc3, mc4 = st.columns(4)
    with mc1:
        st.metric("Mode 1 time", f"{m1['total_time']:.2f}s", help="Sequential, no bus")
    with mc2:
        saved = m1["total_time"] - m2["total_time"]
        st.metric("Mode 2 time", f"{m2['total_time']:.2f}s",
                  delta=f"-{saved:.2f}s vs Mode 1", delta_color="inverse")
    with mc3:
        saved3 = m1["total_time"] - m3["total_time"]
        st.metric("Mode 3 time", f"{m3['total_time']:.2f}s",
                  delta=f"-{saved3:.2f}s vs Mode 1", delta_color="inverse")
    with mc4:
        st.metric("Speedup (M2 vs M1)", summary.get("speedup_mode2_vs_mode1", "—"))

    st.divider()

    # ── Side-by-side mode details ─────────────────────────────────────────────
    st.subheader("Mode Details")
    col1, col2, col3 = st.columns(3)

    def render_mode(col, mode_data: dict, color: str, badge: str):
        with col:
            st.markdown(
                f"<div class='mode-header' style='color:{color}'>{badge}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(f"**Routing:** {mode_data['routing']}")
            st.markdown(f"**Parallelism:** {mode_data['parallelism']}")

            if mode_data.get("error"):
                st.error(f"Error: {mode_data['error']}")
                return

            st.markdown(f"**Total time:** `{mode_data['total_time']:.3f}s`")
            st.markdown(f"**Hops:** `{mode_data['hop_count']}`")

            st.markdown("**Message flow:**")
            for hop in mode_data.get("hops", []):
                typ = hop.get("type", "request")
                css = "hop-peer" if typ == "peer" else ("hop-response" if typ == "response" else "hop-request")
                arrow = "←" if typ == "response" else ("⇢" if typ == "peer" else "→")
                dur_str = f" ({hop['duration_ms']:.0f}ms)" if hop.get("duration_ms") else ""
                st.markdown(
                    f"<div class='{css}'>"
                    f"<b>{hop['number']}.</b> {hop['source']} {arrow} {hop['target']}: "
                    f"<i>{hop['message']}</i>{dur_str}"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            with st.expander("Task results"):
                results = mode_data.get("results", {})
                if results:
                    for task, res in results.items():
                        st.markdown(f"**{task}**")
                        st.json(res, expanded=False)
                else:
                    st.caption("No results captured")

    render_mode(col1, m1, "#ef4444", "Mode 1 — No Bus (Sequential)")
    render_mode(col2, m2, "#16a34a", "Mode 2 — Redis Bus + Keywords")
    render_mode(col3, m3, "#2563eb", "Mode 3 — Redis Bus + OpenRouter")

    # ── Summary explanation ───────────────────────────────────────────────────
    st.divider()
    st.subheader("Why is Mode 2 faster than Mode 1?")
    st.markdown(f"""
<div class='summary-box'>

**Mode 1 (No Bus)** — Main Agent routes every task sequentially:

```
validate_address (0.8s) → setup_electricity (1.2s) → setup_gas (1.0s)
                         → check_availability (0.9s) → setup_internet (1.1s)
Total = 0.8 + 1.2 + 1.0 + 0.9 + 1.1 = 5.0s  ← SUM of all tasks
```

**Mode 2 (Redis Bus)** — Main Agent publishes both agents in parallel, fire-and-forget:

```
Utilities:  validate_address (0.8s) → setup_electricity (1.2s) → setup_gas (1.0s) = 3.0s
                                ↘ AddressValidated (peer-to-peer, no Main Agent) ↗
Broadband:                         check_availability (0.9s) → setup_internet (1.1s) = 2.0s
Total = max(3.0, 0.8 + 2.0) = 3.0s  ← MAX of parallel pipelines
```

**The message bus eliminates the bottleneck** by enabling concurrent execution.
Main Agent load drops from 100% → ~10% (it only monitors, never blocks).

</div>
""", unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Architecture: Control Plane (Main Agent) / Data Plane (Redis Pub/Sub) separation · "
    "Peer-to-peer: Utilities → Broadband via `agent.broadband.address_validated` · "
    "Retry: exponential backoff + Dead Letter Queue"
)
