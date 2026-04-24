
import streamlit as st

def Section(title, body):
    st.markdown(f"### {title}")
    st.write("")
    body()
    st.write("")
