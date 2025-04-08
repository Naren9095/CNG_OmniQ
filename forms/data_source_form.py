import streamlit as st
import sys
import json
import os
from checks import checks_list
from st_aggrid import AgGrid,GridOptionsBuilder, GridUpdateMode

from dotenv import load_dotenv
sys.path.append('../')
from dqFunc import *
from row_to_row import row_to_row_recon

data_connections = []
load_dotenv()
CREDENTIALS_FILE_PATH = os.getenv('CREDENTIALS_FILE_PATH')
TYPE_SNOWFLAKE = os.getenv("TYPE_SNOWFLAKE")
TYPE_AZURE_SQL_SERVER = os.getenv("TYPE_AZURE_SQL_SERVER")
USERNAME = os.getenv("USERNAME")

css_content =''
with open('./ag-grid-theme-builder.css','r+') as f:
    css_content = f.read()

if os.path.exists(f"{CREDENTIALS_FILE_PATH}"):
    with open(f"{CREDENTIALS_FILE_PATH}",'r') as openfile:
        connection_credentials = json.load(openfile)
        data_connections = connection_credentials[USERNAME]['connections'].keys()
else:
    st.error("No Existing Connections Found!")

def data_source_form(check_type):
    source_connection=source_database=source_schema=source_table=None
    st.divider()
    st.header(check_type)
    if check_type == "Data Validation":
        st_source_connection = f"{check_type}_source_connection"
        st_source_database = f"{check_type}_source_database"
        st_source_schema = f"{check_type}_source_schema"
        st_source_table = f"{check_type}_source_table"
        st_preview_data = f"{check_type}_preview_data"

        st.header("Select Source")
        source_connection = st.selectbox(
            'Select Connection',
            [''] + list(data_connections),
            key='source_connection_selectbox_' + check_type
        )
        st.session_state[st_source_connection] = source_connection

        if st.session_state[st_source_connection]:
            conn_details = connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]]
            is_azure = conn_details['type'] == TYPE_AZURE_SQL_SERVER

            if not is_azure:
                try:
                    source_database_options = [''] + getDatabaseList(connectionDetails=conn_details)
                except Exception as e:
                    st.error(f"Unable to fetch Databases list: {e}")
            else:
                source_database_options = [conn_details['database']]

            source_database = st.selectbox(
                'Select Database',
                source_database_options,
                key='source_database_' + check_type
            )
            st.session_state[st_source_database] = source_database

            if st.session_state[st_source_database]:
                if not is_azure:
                    source_schema_options = getSchemaList(
                        connectionDetails=conn_details,
                        database=st.session_state[st_source_database]
                    )
                else:
                    source_schema_options = getSchemaList(
                        connectionDetails=conn_details,
                        database=conn_details['database']
                    )

                source_schema = st.selectbox(
                    'Select Schema',
                    source_schema_options,
                    key='source_schema_button_' + check_type
                )
                st.session_state[st_source_schema] = source_schema

                if st.session_state[st_source_schema]:
                    source_table_or_custom_query = st.radio(
                        '   ',
                        ['Table', 'Custom Query'],
                        horizontal=True,
                        key=f'radio_data_source_{check_type}',label_visibility='collapsed'
                    )

                    if source_table_or_custom_query == 'Table':
                        if not is_azure:
                            source_table_options = [''] + getTableList(
                                connectionDetails=conn_details,
                                database=st.session_state[st_source_database],
                                schema=st.session_state[st_source_schema]
                            )
                        else:
                            source_table_options = getTableList(
                                connectionDetails=conn_details,
                                database=conn_details['database'],
                                schema=st.session_state[st_source_schema]
                            )

                        source_table = st.selectbox(
                            'Select Table',
                            source_table_options,
                            key='source_table_button_' + check_type
                        )
                        st.session_state[st_source_table] = source_table

                        if st.session_state[st_source_table]:
                            preview_data_btn = st.button('Preview Data', key=f'Preview_data_source_{check_type}')

                            if preview_data_btn:
                                try:
                                    st.session_state[st_preview_data] = getDataPreview(
                                        connectionDetails=conn_details,
                                        database=st.session_state[st_source_database],
                                        schema=st.session_state[st_source_schema],
                                        table=st.session_state[st_source_table]
                                    )
                                except Exception as e:
                                    st.error(f"Unable to Preview Data: {e}")

                            if st_preview_data in st.session_state:
                                st.write('Data Preview')

                                gb = GridOptionsBuilder.from_dataframe(st.session_state[st_preview_data])
                                gb.configure_pagination()
                                gb.configure_default_column(editable=True, groupable=True)
                                grid_options = gb.build()

                                AgGrid(
                                    st.session_state[st_preview_data],
                                    gridOptions=grid_options,
                                    update_mode=GridUpdateMode.VALUE_CHANGED,
                                    allow_unsafe_jscode=True,
                                    theme='streamlit'
                                )
                    else:
                        st.text_area('Enter Custom Query', key=f'source_data_custom_query_{check_type}')

        if (
            st.session_state.get(st_source_connection)
            and st.session_state.get(st_source_database)
            and st.session_state.get(st_source_schema)
            and st.session_state.get(st_source_table)
        ):
            checks_list(
                check_type=check_type,
                source_connection_details=connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]],
                source_database=st.session_state[st_source_database],
                source_schema=st.session_state[st_source_schema],
                source_table=st.session_state[st_source_table]
            )
    else:
        st_source_connection = 'source_connection'
        st_source_database = 'source_database'
        st_source_schema = 'source_schema'
        st_source_table = 'source_table'
        st_target_connection = 'target_connection'
        st_target_database = 'target_database'
        st_target_schema = 'target_schema'
        st_target_table = 'target_table'
        st_preview_source_data = f'preview_source_data_{check_type}'
        st_preview_target_data = f'preview_target_data_{check_type}'

        for key in [st_source_connection, st_source_database, st_source_schema, st_source_table,
                    st_target_connection, st_target_database, st_target_schema, st_target_table]:
            if key not in st.session_state:
                st.session_state[key] = None

        if st_preview_source_data not in st.session_state:
            st.session_state[st_preview_source_data] = None
        if st_preview_target_data not in st.session_state:
            st.session_state[st_preview_target_data] = None

        col1, col2 = st.columns(2)
        with col1:
            st.header("Select Source")
            source_connection = st.selectbox('Select Connection', [''] + list(data_connections),
                                            key='source_connection_selectbox_' + check_type,
                                            on_change=lambda: st.session_state.update({st_preview_source_data: None}))
            st.session_state[st_source_connection] = source_connection

            try:
                source_database_options = (getDatabaseList(connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]]) if st.session_state[st_source_connection] != '' else ['']) if st.session_state[st_source_connection] != '' and connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]]['type'] != TYPE_AZURE_SQL_SERVER else ([connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]]['database']] if st.session_state[st_source_connection] != '' else '')
            except Exception as e:
                st.error(f"Unable to fetch Databases list: {e}")
            source_database = st.selectbox('Select Database', source_database_options,
                                            key='source_database_' + check_type,
                                            on_change=lambda: st.session_state.update({st_preview_source_data: None}))
            st.session_state[st_source_database] = source_database

            if st.session_state[st_source_database]:
                source_schema_options = getSchemaList(connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]], database=st.session_state[st_source_database]) if st.session_state[st_source_connection] != '' and st.session_state[st_source_database] != '' and connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]]['type'] != TYPE_AZURE_SQL_SERVER else getSchemaList(connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]], database=connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]]['database'])
                source_schema = st.selectbox('Select Schema', source_schema_options,
                                            key='source_schema_button_' + check_type,
                                            on_change=lambda: st.session_state.update({st_preview_source_data: None}))
                st.session_state[st_source_schema] = source_schema

                if st.session_state[st_source_schema]:
                    source_table_or_custom_query = st.radio('  ', ['Table', 'Custom Query'], horizontal=True,
                                                            key=f'radio_data_source_{check_type}',
                                                            label_visibility='collapsed')
                    if source_table_or_custom_query == 'Table':
                        source_table_options = (getTableList(connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]], database=st.session_state[st_source_database], schema=st.session_state[st_source_schema])) if st.session_state[st_source_connection] != '' and st.session_state[st_source_database] != '' and st.session_state[st_source_schema] != '' and connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]]['type'] != TYPE_AZURE_SQL_SERVER else (getTableList(connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]], database=connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]]['database'], schema=st.session_state[st_source_schema]))
                        source_table = st.selectbox('Select Table', source_table_options,
                                                    key='source_table_button_' + check_type,
                                                    on_change=lambda: st.session_state.update({st_preview_source_data: None}))
                        st.session_state[st_source_table] = source_table

                        if st.session_state[st_source_table]:
                            preview_source_data_btn = st.button('Preview Data', key=f'Preview_data_source_{check_type}')
                            if preview_source_data_btn:
                                try:
                                    st.session_state[st_preview_source_data] = getDataPreview(
                                        connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]],
                                        database=st.session_state[st_source_database],
                                        schema=st.session_state[st_source_schema],
                                        table=st.session_state[st_source_table]
                                    )
                                except Exception as e:
                                    st.error(f"Unable to Preview Data: {e}")
                            if st.session_state[st_preview_source_data] is not None:
                                st.write('Source Data Preview')
                                AgGrid(st.session_state[st_preview_source_data])
                    else:
                        st.text_area('Enter Custom Query', key=f'source_data_custom_query_{check_type}')

        with col2:
            st.header("Select Target")
            target_connection = st.selectbox('Select Connection', [''] + list(data_connections),
                                            key='target_connection_selectbox_' + check_type,
                                            on_change=lambda: st.session_state.update({st_preview_target_data: None}))
            st.session_state[st_target_connection] = target_connection
            try:
                target_database_options = ([''] + getDatabaseList(connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]]) if st.session_state[st_target_connection] != '' else ['']) if st.session_state[st_target_connection] != '' and connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]]['type'] != TYPE_AZURE_SQL_SERVER else ([connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]]['database']] if st.session_state[st_target_connection] != '' else '')
            except Exception as e:
                st.error(f"Unable to fetch Databases list: {e}")
            target_database = st.selectbox('Select Database', target_database_options,
                                            key='target_database_' + check_type,
                                            on_change=lambda: st.session_state.update({st_preview_target_data: None}))
            st.session_state[st_target_database] = target_database

            if st.session_state[st_target_database]:
                target_schema_options = [''] + getSchemaList(connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]], database=st.session_state[st_target_database]) if st.session_state[st_target_connection] != '' and st.session_state[st_target_database] != '' and connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]]['type'] != TYPE_AZURE_SQL_SERVER else getSchemaList(connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]], database=connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]]['database'])
                target_schema = st.selectbox('Select Schema', target_schema_options,
                                            key='target_schema_button_' + check_type,
                                            on_change=lambda: st.session_state.update({st_preview_target_data: None}))
                st.session_state[st_target_schema] = target_schema

                if st.session_state[st_target_schema]:
                    target_table_or_custom_query = st.radio('   ', ['Table', 'Custom Query'], horizontal=True,
                                                            key=f'radio_data_target_{check_type}',
                                                            label_visibility='collapsed')
                    if target_table_or_custom_query == 'Table':
                        target_table_options = ([''] + getTableList(connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]], database=st.session_state[st_target_database], schema=st.session_state[st_target_schema])) if st.session_state[st_target_connection] != '' and st.session_state[st_target_database] != '' and st.session_state[st_target_schema] != '' and connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]]['type'] != TYPE_AZURE_SQL_SERVER else (getTableList(connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]], database=connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]]['database'], schema=st.session_state[st_target_schema]))
                        target_table = st.selectbox('Select Table', target_table_options,
                                                    key='target_table_button_' + check_type,
                                                    on_change=lambda: st.session_state.update({st_preview_target_data: None}))
                        st.session_state[st_target_table] = target_table

                        if st.session_state[st_target_table]:
                            preview_target_data_btn = st.button('Preview Data', key=f'Preview_data_target_{check_type}')
                            if preview_target_data_btn:
                                try:
                                    st.session_state[st_preview_target_data] = getDataPreview(
                                        connectionDetails=connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]],
                                        database=st.session_state[st_target_database],
                                        schema=st.session_state[st_target_schema],
                                        table=st.session_state[st_target_table]
                                    )
                                except Exception as e:
                                    st.error(f"Unable to Preview Data: {e}")
                            if st.session_state[st_preview_target_data] is not None:
                                st.write('Target Data Preview')
                                AgGrid(st.session_state[st_preview_target_data])
                    else:
                        st.text_area('Enter Custom Query', key=f'target_data_custom_query_{check_type}')

        if st.session_state[st_source_connection] and st.session_state[st_source_database] and st.session_state[st_source_schema] and st.session_state[st_source_table] and st.session_state[st_target_connection] and st.session_state[st_target_database] and st.session_state[st_target_schema] and st.session_state[st_target_table]:
            agg_row = st.radio('Choose One', ['Agg Reconciliation', 'Row to Row Reconciliation'])
            if agg_row == 'Agg Reconciliation':
                try:
                    checks_list(
                        check_type=check_type,
                        source_connection_details=connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]],
                        source_database=st.session_state[st_source_database],
                        source_schema=st.session_state[st_source_schema],
                        source_table=st.session_state[st_source_table],
                        target_connection_details=connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]],
                        target_database=st.session_state[st_target_database],
                        target_schema=st.session_state[st_target_schema],
                        target_table=st.session_state[st_target_table]
                    )
                except Exception as e:
                    st.error(f"Error: {e}")
            elif agg_row == 'Row to Row Reconciliation':
                source_details = {
                    "database": st.session_state[st_source_database],
                    "schema": st.session_state[st_source_schema],
                    "table": st.session_state[st_source_table]
                }
                target_details = {
                    "database": st.session_state[st_target_database],
                    "schema": st.session_state[st_target_schema],
                    "table": st.session_state[st_target_table]
                }
                source_connection_obj = connection_credentials[USERNAME]['connections'][st.session_state[st_source_connection]]
                target_connection_obj = connection_credentials[USERNAME]['connections'][st.session_state[st_target_connection]]
                try:
                    row_to_row_recon(source_connection=source_connection_obj,target_connection=target_connection_obj,source_details=source_details,target_details=target_details)
                except Exception as e:
                    st.error(f"Error: {e}")