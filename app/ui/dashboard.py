
import streamlit as st

st.set_page_config(layout="wide")

st.title("Alpha Engine v3.0")
st.caption("Recursive • Self-learning • Dual Track")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.subheader("Champion")
    st.metric("Sentiment", "sent_v3", "0.92")

with col2:
    st.subheader("Challenger")
    st.metric("Quant", "quant_v3.1", "testing")

with col3:
    st.subheader("Regime")
    st.metric("Market", "HIGH VOL", "trend strong")

with col4:
    st.subheader("Consensus")
    st.metric("P_final", "0.84", "agreement high")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Track Stability")
    st.write("Sentiment")
    st.progress(0.92)

    st.write("Quant")
    st.progress(0.74)

with right:
    st.subheader("Rollback Guardrails")
    st.write("Stability Monitor")
    st.progress(0.88)

    st.write("Promotion Gate")
    st.progress(0.66)

st.divider()

st.subheader("Recent Signals")

signals = [
    ("NVDA", "↑", 0.82),
    ("AMD", "↓", 0.61),
    ("TSLA", "↑", 0.73)
]

for t, d, c in signals:
    st.write(f"{t} {d} {c}")
