import pandas as pd
pd.set_option('display.max_columns',None)

from snow import session
from connections import engine
import streamlit as st

# --------------------------------------------------------------from UI
ls_src = ['channel_id','channel_name','keywords']
ls_trg = ['channel_id','channel_name','keywords']

ls_prmkey_src = ['channel_id']
ls_prmkey_src = [f"{key}_left" for key in ls_prmkey_src]

ls_prmkey_trg = ['channel_id']
ls_prmkey_trg = [f"{key}_right" for key in ls_prmkey_trg]

#-------------------------------------------------------------- converting the list of columns to pass in the SQL query
columns_src = ','.join(ls_src)
columns_trg = ','.join(ls_trg)


#--------------------------------------------------------------Query for SQL SERVER source
q_user_schemas = f'''
                    select {columns_src}
                    from raw.youtube--naren.youtube_2005_2010 
                    where join_date between '2005-05-02' and '2015-06-30'
                  '''

with engine.begin() as conn:
    df_src = pd.read_sql_query(q_user_schemas, conn)
    df_src.columns = map(str.lower, df_src.columns)
    df_src = df_src.add_suffix('_left')
print(df_src)




#--------------------------------------------------------------Query for SNOWFLAKE target
sql_query = session.sql(f'''select {columns_trg}
from omniq.gold.youtube--youtube_2005_2010 
where join_date between '2005-05-02' and '2015-06-30' 
                         ''')
df_trg = sql_query.toPandas()
df_trg.columns = map(str.lower, df_trg.columns) #converting column names to lower case
df_trg = df_trg.add_suffix('_right')
print(df_trg)



#--------------------------------------------------------------merging the source and the target
merged_df = pd.merge(left=df_src,right=df_trg,how='outer',left_on=ls_prmkey_src,right_on=ls_prmkey_trg,suffixes=('_left','_right'))
print(merged_df)

#--------------------------------------------------------------Checking the memory size
print()
print(merged_df.info(verbose=True,memory_usage=True))

no_of_Columns = len(ls_src)

# print(merged_df.iloc[:, :no_of_Columns])
# print(merged_df.iloc[:, no_of_Columns:])

#-------------------------------------------------------------- dividing the data frame into two dataframes for usage in the COMPARE function.
left_table = merged_df.iloc[:, :no_of_Columns]
right_table = merged_df.iloc[:, no_of_Columns:]
print(left_table)
print(right_table)


##--------------------------------------------------------------Changing the names of the right table as same as the left table to use in the COMPARE function. Compare func needs same column names and same index
change_names = dict(zip(list(right_table.columns),list(left_table.columns)))
# print(change_names)

print(left_table.set_index(ls_prmkey_src,inplace=True))
right_table.rename(columns=change_names, inplace=True)
print(right_table.set_index(ls_prmkey_src,inplace=True))


# print()
# print()
c_df = left_table.compare(right_table)
print(c_df)
print(c_df.columns)
final_df = pd.DataFrame(list(c_df.index),columns=ls_prmkey_src)
print(final_df)