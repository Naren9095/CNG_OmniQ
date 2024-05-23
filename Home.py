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

st.image('logo.png', width=800);

# url = requests.get( 
#     "https://lottie.host/873217e0-3225-4fdc-b05c-2c59df812741/FsQyPza8B5.lottie") 
# Creating a blank dictionary to store JSON file, 
# as their structure is similar to Python Dictionary 
# url_json = dict() 

# if url.status_code == 200: 
#     url_json = url.json() 
# else: 
#     print("Error in the URL") 

# def load_lottiefile(filepath: str):
#      with(filepath, "r") as f:
#           return json.load(f);

# lottieCoding = load_lottiefile("lottieFiles/anim.json");

# st.lottie(lottieCoding);

# col1, col2 = st.columns(2);

# with col1:
#     st.page_link("Home.py", label="Home", icon="🏠")
#     st.page_link("pages/Connections.py", label="Connections", icon="🤝")

# with col2:
#     st.page_link("pages/Data Validation.py", label="Data Validation", icon="⚾️")
#     st.page_link("pages/Row to Row Reconciliation.py", label="Row to Row Reconciliation", icon="🪀");

# st.page_link("Home.py", label="Home", icon="🏠")
# st.page_link("pages/Connections.py", label="Connections", icon="🤝")
# st.page_link("pages/Data Validation.py", label="Data Validation", icon="⚾️")
# st.page_link("pages/Row to Row Reconciliation.py", label="Row to Row Reconciliation", icon="🪀");

col1, col2 = st.columns(2);

with col1:
    col3, col4 = st.columns(2);
    with col3:
        st.page_link("Home.py", label="Home", icon="🏠")
        st.page_link("pages/Connections.py", label="Connections", icon="🤝")
    with col4:
        st.page_link("pages/Data Validation.py", label="Data Validation", icon="⚾️")
        st.page_link("pages/Row to Row Reconciliation.py", label="Row to Row Reconciliation", icon="🪀");