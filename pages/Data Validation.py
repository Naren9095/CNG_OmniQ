import streamlit as st
from forms.data_source_form import data_source_form

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

page_bg = """
    <style>
         [data-testid="stSidebarContent"] {
           background: rgb(238,174,202);
           background: linear-gradient(164deg, rgba(238,174,202,0.22875087535014005) 29%, rgba(148,187,233,0.7077424719887955) 100%);
        }

       /* [data-testid="stAppViewBlockContainer"] {
            display: flex;
            justify-content: center;
        } */
    </style>
"""

st.markdown(page_bg, unsafe_allow_html=True)