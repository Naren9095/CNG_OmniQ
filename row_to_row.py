import streamlit as st
from dqFunc import getColumnList,executeQuery
import pandas as pd
from st_aggrid import AgGrid

def row_to_row_recon(source_connection,target_connection,source_details,target_details):
  check_type = 'Data Reconciliation'

  source_connection_details =  source_connection
  source_database = source_details['database']
  source_schema = source_details['schema']
  source_table = source_details['table']

  target_connection_details =  target_connection
  target_database = target_details['database']
  target_schema = target_details['schema']
  target_table = target_details['table']


  st.header('Row To Row Reconciliation')
  src_list = list(getColumnList(source_connection_details,source_database,source_schema,source_table))
  trg_list = list(getColumnList(target_connection_details,target_database,target_schema,target_table))

  col1,col2 = st.columns(2,gap='medium')



  ls_src = []
  ls_trg = []
  ls_prmkey_src=[]
  ls_prmkey_trg=[]
  df_src=df_trg=None

  with col1:
    src_list = st.multiselect('Pick your source columns',src_list,key='col1')
    ls_prmkey_src.append(st.selectbox('Primary Key',src_list,key='src_prmkey'))
    if ls_prmkey_src:
      ls_prmkey_src = [f"{key.lower()}_left" for key in ls_prmkey_src if key]  #primary key for target
      for i in range(3):
        st.write('')
      st.markdown("<h4>Map the neccessary source columns</h4>", unsafe_allow_html=True)
      for i in range(len(src_list) if len(src_list) < len(trg_list) else len(trg_list)):
        if i == 0:
          ls_src.append(st.selectbox('Source_columns',list(src_list),key='col1'+str(i)))
        else:
          ls_src.append(st.selectbox('Source_columns',list(src_list),key='col1'+str(i),label_visibility='hidden')) #ls_src is the list of source columns to reconcile
   
  with col2:
    trg_list = st.multiselect('Pick your target columns',trg_list,key='col2')
    ls_prmkey_trg.append(st.selectbox('Primary Key',trg_list,key='trg_prmkey'))
    if ls_prmkey_trg:
      ls_prmkey_trg = [f"{key.lower()}_right" for key in ls_prmkey_trg if key]  #primary key for target
      for i in range(3):
        st.write('')
      st.markdown("<h4>Map the neccessary target columns</h4>", unsafe_allow_html=True)
      for i in range(len(src_list) if len(src_list) < len(trg_list) else len(trg_list)):
        if i == 0:
          ls_trg.append(st.selectbox('Target_columns',list(trg_list),key='col2'+str(i)))
        else:
          ls_trg.append(st.selectbox('Target_columns',list(trg_list),key='col2'+str(i),label_visibility='hidden')) #ls_trg is the list of source columns to reconcile

  if((len(ls_src) == len(set(ls_src)) == len(src_list)) and (len(ls_trg) == len(set(ls_trg)) == len(trg_list))):
    columns_src = ','.join(ls_src)
    columns_trg = ','.join(ls_trg)

    src_query = f'''
                        select {columns_src}
                        from {source_database}.{source_schema}.{source_table}
                  '''
    if columns_src and source_database and source_schema and source_table and source_connection_details:
      df_src = executeQuery(dbProvider=source_connection_details['type'],connectionDetails=source_connection_details,query=src_query)
      df_src.columns = map(str.lower, df_src.columns)
      df_src = df_src.add_suffix('_left')

    trg_query = f'''select {columns_trg}
                    from {target_database}.{target_schema}.{target_table}
                  '''
    if columns_trg and target_database and target_schema and target_table and target_connection_details:
      df_trg = executeQuery(dbProvider=target_connection_details['type'],connectionDetails=target_connection_details,query=trg_query)
      df_trg.columns = map(str.lower, df_trg.columns) #converting column names to lower case
      df_trg = df_trg.add_suffix('_right')

    if ls_prmkey_src and ls_prmkey_trg:
      merged_df = pd.merge(left=df_src,right=df_trg,how='outer',left_on=ls_prmkey_src,right_on=ls_prmkey_trg,suffixes=('_left','_right'))


      no_of_Columns = len(ls_src)


      left_table = merged_df.iloc[:, :no_of_Columns]
      right_table = merged_df.iloc[:, no_of_Columns:]


      change_names = dict(zip(list(right_table.columns),list(left_table.columns)))

      left_table.set_index(ls_prmkey_src,inplace=True)
      right_table.rename(columns=change_names, inplace=True)
      right_table.set_index(ls_prmkey_src,inplace=True)


      c_df = left_table.compare(right_table)
      final_df = pd.DataFrame(list(c_df.index),columns=ls_prmkey_src)
      for i in range(3):
        st.write('')
      st.write(f"Mismatched {ls_prmkey_src[0].replace('_left','')}s'")
      AgGrid(final_df)