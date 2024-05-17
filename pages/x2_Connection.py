import streamlit as st
from sql_cred import sql_Server_Cred,snow_Cred

st.set_page_config(layout="wide")
hide_menu_style = """
        <style>
        #MainMenu {visibility: hidden;}
        body { background-color: #f9f9f9}
        </style>
        """
st.markdown(hide_menu_style, unsafe_allow_html=True)

top_container = st.container()

with top_container:
    st.title('OmniQSuite - Compare your data in a single platform')
    st.markdown("### Create a Connection")

    #Selecting a Connection
    DataSource = st.selectbox('select a DataSource',['SqlServer','MySQL','Snowflake'])
    st.markdown('####')
    st.markdown('####')

    #Entering the connection Details
    if DataSource == 'SqlServer':
        sql_Server_Cred('','','','','')
    elif DataSource == 'Snowflake':
        snow_Cred('','','')
    else:
        pass

