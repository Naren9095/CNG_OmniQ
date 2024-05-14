import streamlit as st
from sql_cred import sql_Server_Cred

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
         sql_Server_Cred('','','','')
    elif DataSource == 'Snowflake':
        pass

