import streamlit as st
from sql_cred import sql_Server_Cred,snow_Cred
import json,os

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
    DataSource = st.selectbox('select a DataSource',['SQLSERVER','SNOWFLAKE'])
    st.markdown('####')
    st.markdown('####')

    #Entering the connection Details
    if DataSource == 'SQLSERVER':
        sql_Server_Cred('','','','','','new_connection')
    elif DataSource == 'SNOWFLAKE':
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
        if os.path.exists('./cred.json'):
            with open(r'./cred.json','r') as openfile:
                cred2 = json.load(openfile)
                # st.write(cred2)
                # st.write(cred2['username1']['connections'].keys())
                
            st.markdown("### Existing Connections")
            conn = st.selectbox('Select a connection to edit',list(cred2['username1']['connections'].keys()))
            st.markdown('####')
            st.markdown('####')

            # st.write(cred2['username1']['connections'][conn])
            if cred2['username1'].get('connections') and cred2['username1']['connections'].get(conn):
                connection_name = cred2['username1']['connections'][conn]
                if connection_name['type'] == 'AZURE_SQL_SERVER':
                    sql_Server_Cred(user_name=connection_name['username'],password=connection_name['password'],server_name=connection_name['server'],database_name=connection_name['database'],connection_name=conn,connection_from='existing_connection')
                if connection_name['type'] == 'SNOWFLAKE':
                    snow_Cred(account=connection_name['account'],user_name=connection_name['username'],password=connection_name['password'],connection_name=conn,connection_from='existing_connection')
        else:
            st.error('No Existing Connections. Create a Connection first')


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
