
import streamlit as st
from app.ui.tokens import Type, Space

def Section(title, body):
    st.markdown(f"### {title}")
    st.write("")
    body()
    st.write("")
