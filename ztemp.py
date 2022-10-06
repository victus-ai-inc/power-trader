import streamlit as st
import time

placeholder = st.empty()
for seconds in range(100000):
    with placeholder.container():
        st.title(seconds)
        time.sleep(1)