from snowflake.snowpark.session import Session
from snowflake.snowpark import version
from snow_cred import snowflake_conn_prop

#creating the session in snowflake
session = Session.builder.configs(snowflake_conn_prop).create()

rolename = "ACCOUNTADMIN"
dbname = "OMNIQ"
schemaname = "NAREN"
warehouse = "COMPUTE_WH"

session.sql(f"USE ROLE {rolename}").collect()
session.sql(f"USE WAREHOUSE {warehouse}").collect()
session.sql(f"USE SCHEMA {dbname}.{schemaname}").collect()

sql_query = session.sql('''select channel_id as id_snow,channel_name as name_snow,keywords as key_snow--,join_date,mean_views_last_30_videos 
from youtube_2005_2010 
where join_date between '2005-05-02' and '2015-06-30' 
order by subscriber_count
                           ''')
snow_df = sql_query.toPandas()
# print(snow_df)