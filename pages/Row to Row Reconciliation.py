import streamlit as st
from dqFunc import getColumnList,executeQuery
import pandas as pd
from st_aggrid import AgGrid


st.set_page_config(layout='wide')
check_type = 'Data Reconciliation'

source_connection_details =  {
        "username": "OmniQSuite",
        "password": "DataQuality@#123",
        "server": "omniqsuiteserver.database.windows.net",
        "database": "testOmniQSuite",
        "type": "AZURE_SQL_SERVER"
      }
source_database = 'testOmniQSuite'
source_schema = 'naren'
source_table = 'youtube_2005_2010'

target_connection_details =  {
        "account": "fvrfaye-rl30738",
        "username": "cngdata",
        "password": "Cngdata@#123",
        "connection": "snow_new_connection_newest_newest",
        "type": "SNOWFLAKE"
      }
target_database = 'omniq'
target_schema = 'naren'
target_table = 'youtube_2005_2010'

# def row_to_row_reconciliation(check_type,source_connection_details,source_database,source_schema,source_table,target_connection_details,target_database,target_schema,target_table):

st.header('Row To Row Reconciliation')
src_list = list(getColumnList(source_connection_details,source_database,source_schema,source_table))
trg_list = list(getColumnList(target_connection_details,target_database,target_schema,target_table))




col1,col2,col3 = st.columns(3,gap='small')

# st.markdown("""
#     <style>
#     [data-testid=column]:nth-of-type(1) [data-testid=stVerticalBlock]{
#         gap: 0rem;
#     }
#     </style>
#     """,unsafe_allow_html=True)

ls_src = []
ls_trg = []
ls_prmkey_src=[]
ls_prmkey_trg=[]
df_src=df_trg=None

with col1:
  src_list = st.multiselect('Pick your source columns',src_list,key='col1')
  ls_prmkey_src.append(st.selectbox('Primary Key',src_list,key='src_prmkey'))
  if ls_prmkey_src:
    ls_prmkey_src = [f"{ls_prmkey_src[i].lower()}_source_left_{i}" for i in range(len(ls_prmkey_src)) if ls_prmkey_src[i]]

    for i in range(3):
      st.write('')
    st.markdown("<h4>Map the source and target columns</h4>", unsafe_allow_html=True)
    for i in range(len(src_list) if len(src_list) < len(trg_list) else len(trg_list)):
      if i == 0:
        ls_src.append(st.selectbox('Source_columns',list(src_list),key='col1'+str(i)))
      else:
        ls_src.append(st.selectbox('Source_columns',list(src_list),key='col1'+str(i),label_visibility='hidden')) #ls_src is the list of source columns to reconcile
  # st.write(ls_src) 


# with col2:
#   st.markdown("<h4 style='text-align: left;'>Map the source and target columns</h4>", unsafe_allow_html=True)

with col3:
  trg_list = st.multiselect('Pick your target columns',trg_list,key='col3')
  ls_prmkey_trg.append(st.selectbox('Primary Key',trg_list,key='trg_prmkey'))
  if ls_prmkey_trg:
    ls_prmkey_trg = [f"{ls_prmkey_trg[i].lower()}_target_right_{i}" for i in range(len(ls_prmkey_trg)) if ls_prmkey_trg[i]]
    # ls_prmkey_trg = [f"{key.lower()}_right" for key in ls_prmkey_trg if key]  #primary key for target
    for i in range(6):
      st.write('')
    # st.markdown("<h4> M</h4>", unsafe_allow_html=True)
    for i in range(len(src_list) if len(src_list) < len(trg_list) else len(trg_list)):
      if i == 0:
        ls_trg.append(st.selectbox('Target_columns',list(trg_list),key='col3'+str(i)))
      else:
        ls_trg.append(st.selectbox('Target_columns',list(trg_list),key='col3'+str(i),label_visibility='hidden')) #ls_trg is the list of source columns to reconcile
    # st.write(ls_trg) 


#-------------------------------------------------------------- converting the list of columns to pass in the SQL query
columns_src = ','.join(ls_src)
columns_trg = ','.join(ls_trg)

#--------------------------------------------------------------Query for source
src_query = f'''
                    select {columns_src}
                    from {source_database}.{source_schema}.{source_table}
              '''
if columns_src and source_database and source_schema and source_table and source_connection_details:
  df_src = executeQuery(dbProvider=source_connection_details['type'],connectionDetails=source_connection_details,query=src_query)
  df_src.columns = map(str.lower, df_src.columns)
  df_src = df_src.add_suffix('_left')
  # st.write(df_src)

#--------------------------------------------------------------Query for target
trg_query = f'''select {columns_trg}
                from {target_database}.{target_schema}.{target_table}
              '''
if columns_trg and target_database and target_schema and target_table and target_connection_details:
  df_trg = executeQuery(dbProvider=target_connection_details['type'],connectionDetails=target_connection_details,query=trg_query)
  df_trg.columns = map(str.lower, df_trg.columns) #converting column names to lower case
  df_trg = df_trg.add_suffix('_right')
  # st.write(df_trg)

#--------------------------------------------------------------merging the source and the target
# st.write(ls_prmkey_src,ls_prmkey_trg)
if ls_prmkey_src and ls_prmkey_trg:
  merged_df = pd.merge(left=df_src,right=df_trg,how='outer',left_on=ls_prmkey_src,right_on=ls_prmkey_trg,suffixes=('_left','_right'))
  # st.write(merged_df)

  #--------------------------------------------------------------Checking the memory size
  # st.write(merged_df.info(verbose=True,memory_usage=True))

  no_of_Columns = len(ls_src)

  # print(merged_df.iloc[:, :no_of_Columns])
  # print(merged_df.iloc[:, no_of_Columns:])

  #-------------------------------------------------------------- dividing the data frame into two dataframes for usage in the COMPARE function.
  left_table = merged_df.iloc[:, :no_of_Columns]
  right_table = merged_df.iloc[:, no_of_Columns:]
  # st.write(left_table)
  # st.write(right_table)


  ##--------------------------------------------------------------Changing the names of the right table as same as the left table to use in the COMPARE function. Compare func needs same column names and same index
  change_names = dict(zip(list(right_table.columns),list(left_table.columns)))
  # print(change_names)

  left_table.set_index(ls_prmkey_src,inplace=True)
  right_table.rename(columns=change_names, inplace=True)
  right_table.set_index(ls_prmkey_src,inplace=True)


  c_df = left_table.compare(right_table)
  # st.write(c_df)
  # st.write(c_df.columns)
  final_df = pd.DataFrame(list(c_df.index),columns=ls_prmkey_src)
  st.write()
  AgGrid(final_df)





