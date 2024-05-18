import streamlit as st

st.session_state['source_validation'] = {}  # Store validation results for source
st.session_state['source_reconciliation'] = {}  # Store validation results for source
st.session_state['target_reconciliation'] = {}  # Store validation results for target


def data_source_form(type):
    st.header("Select Data Source")
    if type == "Data Validation":
        with st.form(key='source_'+type):
            st.header("Select Data Source")
            source_connection = st.selectbox('Select Connection',['Snowflake','SQL','Azure SQL'])
            submit_button = st.form_submit_button("Validate")
            if submit_button:
                st.session_state['source_validation'][type] = 'validation_result_1'
                st.success("Data validated 'validation_result_1'!")

        if 'source_validation' in st.session_state and type in st.session_state['source_validation']:
            st.write(f"Source validation result for {type}: {st.session_state['source_validation'][type]}")
    else:
        col1, col2 = st.columns(2)
        with col1.form(key='source_'+type):
            st.header("Select Data Source")
            source_connection = st.selectbox('Select Connection',['Snowflake','SQL','Azure SQL'],key='source_'+type)
            submit_button_1 = st.form_submit_button("Validate")
            if submit_button_1:
                st.session_state['source_reconciliation'][type] = 'reconciliation_result_1'
                st.success("Data validated 'reconciliation_result_1'!")       
        with col2.form(key='target_'+type):
            st.header("Select Data Source")
            target_connection= st.selectbox('Select Connection',['Snowflake','SQL','Azure SQL'],key='target_'+type)
            submit_button_2 = st.form_submit_button("ValidateNow")
            if submit_button_2:
                st.session_state['target_reconciliation'][type] = 'reconciliation_result_2'
                st.success("Data validated 'reconciliation_result_2'!")       

        if 'source_reconciliation' in st.session_state and type in st.session_state['source_reconciliation']:
            col1.write(f"Source validation result for {type}: {st.session_state['source_reconciliation'][type]}")

        if 'target_reconciliation' in st.session_state and type in st.session_state['target_reconciliation']:
            col2.write(f"Target validation result for {type}: {st.session_state['target_reconciliation'][type]}")
        