import streamlit as st
from sql_connection import sql_Server_Conn

def sql_Server_Cred(user_name, password, server_name, database_name):
        col1,col2 = st.columns(2,gap="large")

        with col1.form('sql_server_form',border=True,clear_on_submit=False):
            user_name = st.text_input("User Name")
            password = st.text_input("Password")
            server_name = st.text_input("Server Name")
            database_name = st.text_input("Database Name")

            submitted = st.form_submit_button("Submit")
            if submitted:
                succ_error = sql_Server_Conn(user_name=user_name,password = password,server_name=server_name,database_name=database_name)

# server_name = "omniqsuiteserver.database.windows.net"
# database_name = "testOmniQSuite"
# user_name = "OmniQSuite"
# password_name = "DataQuality@#123"
