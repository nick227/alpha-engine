
import streamlit as st

def card(title, value, subtitle=None):
    st.subheader(title)
    st.metric(title, value, subtitle)
