
import streamlit as st

def Bar(value, label=None):
    v = max(0,min(1,value))
    st.progress(v)
    if label:
        st.caption(label)
