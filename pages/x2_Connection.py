import streamlit as st
from sql_cred import sql_Server_Cred,snow_Cred
import json

st.set_page_config(layout="wide")
hide_menu_style = """
        <style>
        #MainMenu {visibility: hidden;}
        body { background-color: #f9f9f9}
        </style>
        """
st.markdown(hide_menu_style, unsafe_allow_html=True)

def newConn():
    st.markdown("### Create a Connection")

    #Selecting a Connection
    DataSource = st.selectbox('select a DataSource',['SqlServer','MySQL','Snowflake'])
    st.markdown('####')
    st.markdown('####')

    #Entering the connection Details
    if DataSource == 'SqlServer':
        sql_Server_Cred('','','','','','new_connection')
    elif DataSource == 'Snowflake':
        snow_Cred('','','','','new_connection')
    else:
        pass

top_container = st.container()

with top_container:
    st.title('OmniQSuite - Compare your data in a single platform')


    col1,col2 = st.columns(2,gap='small')
    newconnbutton = col1.radio('',['New Connection','Existing Connections'],horizontal=True)
    # st.write(newconnbutton)
    

    if newconnbutton=='New Connection':
        newConn()

    if newconnbutton=='Existing Connections':
        with open(r'/Users/naren/Documents/cred_cng/cred.json','r') as openfile:
            cred2 = json.load(openfile)
            # st.write(cred2)
            # st.write(cred2['username1']['connections'].keys())
            
        st.markdown("### Existing Connections")
        conn = st.selectbox('Select a connection to edit',list(cred2['username1']['connections'].keys()))
        st.markdown('####')
        st.markdown('####')

        # st.write(cred2['username1']['connections'][conn])
        connection_name = cred2['username1']['connections'][conn]
        if connection_name['type'] == 'SqlServer':
            sql_Server_Cred(user_name=connection_name['user_name'],password=connection_name['password'],server_name=connection_name['server_name'],database_name=connection_name['database_name'],connection_name=conn,connection_from='existing_connection')
        if connection_name['type'] == 'Snowflake':
            snow_Cred(account=connection_name['account'],user_name=connection_name['user_name'],password=connection_name['password'],connection_name=conn,connection_from='existing_connection')
