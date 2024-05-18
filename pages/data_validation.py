import streamlit as st
from forms.data_source_form import data_source_form

# Navigation within main content
st.title("Data Quality")
page_names = ["Data Validation", "Data Reconciliation"]
page = st.selectbox("Select ", page_names)

data_source_form(check_type=page)
st.empty()

sidebar_style = """
[data-testid="stAppViewBlockContainer"] {
    max-width: 98%; 
}
"""

st.markdown(f"<style>{sidebar_style}</style>", unsafe_allow_html=True)
