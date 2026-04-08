
import streamlit as st
from app.ui.components.bar import Bar

def List(items):
    for i in items:
        st.write(i["label"])
        Bar(i.get("score",0))
