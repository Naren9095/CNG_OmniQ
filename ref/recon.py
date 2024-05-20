from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
from snow import snow_df
from snow import session



server_name = "omniqsuiteserver.database.windows.net"
database_name = "testOmniQSuite"
user_name = "OmniQSuite"
password = "DataQuality@#123"
driver_name = "{ODBC Driver 18 for SQL Server}"

connection_string = f"DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password}"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = create_engine(connection_url)

q_user_schemas = '''
                    select channel_id as id_sql,channel_name as name_sql,keywords as key_sql--,join_date,mean_views_last_30_videos 
                    from naren.youtube_2005_2010 
                    where join_date between '2005-05-02' and '2006-01-01' 
                    order by subscriber_count;'''
with engine.begin() as conn:
    df = pd.read_sql_query(q_user_schemas, conn)
# print(df)





sql_query = session.sql('''select channel_id as id_snow,channel_name as name_snow,keywords as key_snow--,join_date,mean_views_last_30_videos 
from youtube_2005_2010 
where join_date between '2005-05-02' and '2006-01-01'
order by subscriber_count
                           ''')
snow_df = sql_query.toPandas()
# print(snow_df)

merged_df = pd.merge(left=df,right=snow_df,how='outer',left_on='id_sql',right_on='ID_SNOW',suffixes=('_left','_right'))
print(merged_df)
print()
print(merged_df.info(verbose=True,memory_usage=True))

not_matchrec_df = merged_df.loc[merged_df.name_sql != merged_df.NAME_SNOW] ['id_sql']
# print(not_matchrec_df)
ls = ['id_sql','name_sql']

print(merged_df[ls])

left_table = merged_df[['id_sql','name_sql','key_sql']]
right_table = merged_df[['ID_SNOW','NAME_SNOW','KEY_SNOW']]

# print(left_table.set_index('id_sql',inplace=True))
right_table.rename(columns={'ID_SNOW': 'id_sql', 'NAME_SNOW': 'name_sql','KEY_SNOW':'key_sql'}, inplace=True)
# print(right_table.set_index('id_sql',inplace=True))

# df.compare(snow_df,)

print()
print()
c_df = left_table.compare(right_table)
print(c_df)