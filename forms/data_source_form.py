import streamlit as st
import sys
import json
import os
from dotenv import load_dotenv
sys.path.append('../')
from dqFunc import *
from checks import checks_list

# st.session_state['source_validation'] = {}  # Store validation results for source
# st.session_state['source_reconciliation'] = {}  # Store validation results for source
# st.session_state['target_reconciliation'] = {}  # Store validation results for target
data_connections = []
load_dotenv()
CREDENTIALS_FILE_PATH = os.getenv('CREDENTIALS_FILE_PATH')
TYPE_SNOWFLAKE = os.getenv("TYPE_SNOWFLAKE")
TYPE_AZURE_SQL_SERVER = os.getenv("TYPE_AZURE_SQL_SERVER")
USERNAME = os.getenv("USERNAME")
if os.path.exists(f"{CREDENTIALS_FILE_PATH}"):
    with open(f"{CREDENTIALS_FILE_PATH}",'r') as openfile:
        st.write('file opened')
        connection_credentials = json.load(openfile)
        data_connections = connection_credentials[USERNAME]['connections'].keys()
else:
    st.error("No Existing Connections Found!")

def data_source_form(check_type):

    source_connection=source_database=source_schema=source_table=None

    st.header(check_type)
    if check_type == "Data Validation":
        st.header("Select Source")
        source_connection = st.selectbox('Select Connection',['']+list(data_connections),key='source_connection_selectbox_'+check_type)
        source_database_options = (getDatabaseList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection]) if source_connection!='' else ['']) if source_connection!='' and connection_credentials[USERNAME]['connections'][source_connection]['type']!=TYPE_AZURE_SQL_SERVER else ( [connection_credentials[USERNAME]['connections'][source_connection]['database']] if source_connection!=''  else '')
        source_database = st.selectbox('Select Database',source_database_options,key='source_database_'+check_type)
        if source_database:
            source_schema_options = getSchemaList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection],database=source_database) if source_connection!='' and source_database!='' and connection_credentials[USERNAME]['connections'][source_connection]['type']!=TYPE_AZURE_SQL_SERVER else getSchemaList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection],database=connection_credentials[USERNAME]['connections'][source_connection]['database'])
            source_schema = st.selectbox('Select Schema',source_schema_options,key='source_schema_button_'+check_type)
            if source_schema:
                source_table_options = (getTableList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection],database=source_database,schema=source_schema)) if source_connection!='' and source_database!='' and source_schema!='' and connection_credentials[USERNAME]['connections'][source_connection]['type']!=TYPE_AZURE_SQL_SERVER else (getTableList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection],database=connection_credentials[USERNAME]['connections'][source_connection]['database'],schema=source_schema))
                source_table = st.selectbox('Select Table',source_table_options,key='source_table_button_'+check_type)
        # submit_button = st.button("Validate",key='source_validate_button_'+check_type)
        # if submit_button:
        #     st.session_state['source_validation'][check_type] = 'validation_result_1'
        #     st.success("Data validated 'validation_result_1'!")
        # if 'source_validation' in st.session_state and check_type in st.session_state['source_validation']:
        #     st.write(f"Source validation result for {check_type}: {st.session_state['source_validation'][check_type]}")
        if source_connection and source_database and source_schema and source_table:
            checks_list(check_type=check_type,source_connection_details=connection_credentials[USERNAME]['connections'][source_connection],source_database=source_database,source_schema=source_schema,source_table=source_table)
    else:
        source_connection=source_database=source_schema=source_table=None
        target_connection=target_database=target_schema=target_table=None
        col1, col2 = st.columns(2)
        with col1:
            st.header("Select Source")
            source_connection = st.selectbox('Select Connection',['']+list(data_connections),key='source_connection_selectbox_'+check_type)
            source_database_options = (getDatabaseList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection]) if source_connection!='' else ['']) if source_connection!='' and connection_credentials[USERNAME]['connections'][source_connection]['type']!=TYPE_AZURE_SQL_SERVER else ( [connection_credentials[USERNAME]['connections'][source_connection]['database']] if source_connection!=''  else '')
            source_database = st.selectbox('Select Database',(getDatabaseList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection]) if source_connection!='' else '') if source_connection!='' and connection_credentials[USERNAME]['connections'][source_connection]['type']!=TYPE_AZURE_SQL_SERVER else ( [connection_credentials[USERNAME]['connections'][source_connection]['database']] if source_connection!=''  else ''),key='source_database_'+check_type)
            if source_database:
                source_schema_options = getSchemaList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection],database=source_database) if source_connection!='' and source_database!='' and connection_credentials[USERNAME]['connections'][source_connection]['type']!=TYPE_AZURE_SQL_SERVER else getSchemaList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection],database=connection_credentials[USERNAME]['connections'][source_connection]['database'])
                source_schema = st.selectbox('Select Schema',source_schema_options,key='source_schema_button_'+check_type)
                if source_schema:
                    source_table_options = (getTableList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection],database=source_database,schema=source_schema)) if source_connection!='' and source_database!='' and source_schema!='' and connection_credentials[USERNAME]['connections'][source_connection]['type']!=TYPE_AZURE_SQL_SERVER else (getTableList(connectionDetails=connection_credentials[USERNAME]['connections'][source_connection],database=connection_credentials[USERNAME]['connections'][source_connection]['database'],schema=source_schema))
                    source_table = st.selectbox('Select Table',source_table_options,key='source_table_button_'+check_type)
            # source_submit_button = st.button("Validate",key='source_validate_button_'+check_type)
            # if source_submit_button:
            #     st.session_state['source_reconciliation'][check_type] = 'reconciliation_result_1'
            #     st.success("Data validated 'reconciliation_result_1'!")    
        with col2:
            st.header("Select Target")
            target_connection = st.selectbox('Select Connection',['']+list(data_connections),key='target_connection_selectbox_'+check_type)
            target_database_options = (getDatabaseList(connectionDetails=connection_credentials[USERNAME]['connections'][target_connection]) if target_connection!='' else ['']) if target_connection!='' and connection_credentials[USERNAME]['connections'][target_connection]['type']!=TYPE_AZURE_SQL_SERVER else ( [connection_credentials[USERNAME]['connections'][target_connection]['database']] if target_connection!=''  else '')
            target_database = st.selectbox('Select Database',(getDatabaseList(connectionDetails=connection_credentials[USERNAME]['connections'][target_connection]) if target_connection!='' else '') if target_connection!='' and connection_credentials[USERNAME]['connections'][target_connection]['type']!=TYPE_AZURE_SQL_SERVER else ( [connection_credentials[USERNAME]['connections'][target_connection]['database']] if target_connection!=''  else ''),key='target_database_'+check_type)
            if target_database:
                target_schema_options = getSchemaList(connectionDetails=connection_credentials[USERNAME]['connections'][target_connection],database=target_database) if target_connection!='' and target_database!='' and connection_credentials[USERNAME]['connections'][target_connection]['type']!=TYPE_AZURE_SQL_SERVER else getSchemaList(connectionDetails=connection_credentials[USERNAME]['connections'][target_connection],database=connection_credentials[USERNAME]['connections'][target_connection]['database'])
                target_schema = st.selectbox('Select Schema',target_schema_options,key='target_schema_button_'+check_type)
                if target_schema:
                    target_table_options = (getTableList(connectionDetails=connection_credentials[USERNAME]['connections'][target_connection],database=target_database,schema=target_schema)) if target_connection!='' and target_database!='' and target_schema!='' and connection_credentials[USERNAME]['connections'][target_connection]['type']!=TYPE_AZURE_SQL_SERVER else (getTableList(connectionDetails=connection_credentials[USERNAME]['connections'][target_connection],database=connection_credentials[USERNAME]['connections'][target_connection]['database'],schema=target_schema))
                    target_table = st.selectbox('Select Table',target_table_options,key='target_table_button_'+check_type)
                    
        #     target_submit_button = st.button("Validate",key='target_validate_button_'+check_type)
            
        #     if target_submit_button:
        #         st.session_state['target_reconciliation'][check_type] = 'reconciliation_result_2'
        #         st.success("Data validated 'reconciliation_result_2'!")       

        # if 'source_reconciliation' in st.session_state and check_type in st.session_state['source_reconciliation']:
        #     col1.write(f"Source validation result for {check_type}: {st.session_state['source_reconciliation'][check_type]}")

        # if 'target_reconciliation' in st.session_state and check_type in st.session_state['target_reconciliation']:
        #     col2.write(f"Target validation result for {check_type}: {st.session_state['target_reconciliation'][check_type]}")
        
        if source_connection and source_database and source_schema and source_table and target_connection and target_database and target_schema and target_table:
            checks_list(check_type=check_type,source_connection_details=connection_credentials[USERNAME]['connections'][source_connection],source_database=source_database,source_schema=source_schema,source_table=source_table,target_connection_details=connection_credentials[USERNAME]['connections'][target_connection],target_database=target_database,target_schema=target_schema,target_table=target_table)