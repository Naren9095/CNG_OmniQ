import streamlit as st
import json;
from streamlit_lottie import st_lottie 

st.set_page_config(layout='wide')

page_bg = """
    <style>
         [data-testid="stSidebarContent"] {
           background: rgb(238,174,202);
           background: linear-gradient(164deg, rgba(238,174,202,0.22875087535014005) 29%, rgba(148,187,233,0.7077424719887955) 100%);
        }

        [data-testid="stPageLink-NavLink"] {
            border: 1px solid #0000001a;
            min-height: 80px;
            width: 250px;
            padding: 20px;
            margin: 10px;
        }

        .st-emotion-cache-vgwloc p 
            font-size: 30px;
        }

    </style>
"""

st.markdown(page_bg, unsafe_allow_html=True)

st.image('logo.png', width=800)

col3, col4,col5 = st.columns(3)
with col3:
    st.page_link("Home.py", label="Home", icon="🏠")
with col4:
    st.page_link("pages/Connections.py", label="Connections", icon="🤝")
with col5:
    st.page_link("pages/Data Validation.py", label="Data Validation", icon="⚾️")