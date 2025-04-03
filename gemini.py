import pathlib
import textwrap
import os
from dotenv import load_dotenv
import google.generativeai as genai
import re

load_dotenv()

# GOOGLE_API_KEY=os.environ.get('GOOGLE_API_KEY')
# "AIzaSyD0qnU1FuorrzB5EbTGXDV3xoTsGnrg87o,AIzaSyDZF3UGdSYCoAJSeveC1ks-cE7le_ZBYhc,AIzaSyA1qnV4xPBsm9fJo-wXCYhxU205KoI7DuI,AIzaSyA5Um1rv76I6ixa0QqqpA8UJ49os6FHdzQ,AIzaSyBlp8Hb1iwDE6RLW4HEyvB_i_4eTi36xxQ"
GOOGLE_API_KEY = 'AIzaSyAFk5aDF2uOYkwyC8NDdSYOzusT2UEhCBY'
genai.configure(api_key=GOOGLE_API_KEY)

# List all available models
# for m in genai.list_models():
#     if 'generateContent' in m.supported_generation_methods:
#         print(m.name)

# model = genai.GenerativeModel('gemini-pro')

model = genai.GenerativeModel('gemini-1.5-pro-latest')

def get_query(query):
    response = model.generate_content(f"{query}. Just give me only the query and no other explanation or text.")
    # Use regular expression to extract the SQL statement
    match = re.search(r"`sql\n(.*?)\n`", response.text, re.DOTALL)
    if match:
        return match.group(1).strip()  # Extract group 1 (the SQL statement) and remove leading/trailing whitespace
    else:
        # Handle cases where the response doesn't contain the expected format
        return "Unable to extract query from response."
    

# print(get_query('''Give me null check validation for the following snowflake table. YOUTUBE (
# 	CHANNEL_ID VARCHAR(25),
# 	CHANNEL_LINK VARCHAR(35),
# 	CHANNEL_NAME VARCHAR(250),
# 	SUBSCRIBER_COUNT NUMBER(38,0),
# 	BANNER_LINK VARCHAR(310),
# 	DESCRIPTION VARCHAR(2501),
# 	KEYWORDS VARCHAR(750),
# 	AVATAR VARCHAR(165),
# 	COUNTRY VARCHAR(40),
# 	TOTAL_VIEWS NUMBER(38,0),
# 	TOTAL_VIDEOS NUMBER(38,0),
# 	JOIN_DATE DATE,
# 	MEAN_VIEWS_LAST_30_VIDEOS NUMBER(20,10),
# 	MEDIAN_VIEWS_LAST_30_VIDEOS NUMBER(20,10),
# 	STD_VIEWS_LAST_30_VIDEOS NUMBER(20,10),
# 	VIDEOS_PER_WEEK NUMBER(4,2)
# );'''))


# print(get_query('''Give me total count validation for the following snowflake table. YOUTUBE (
# 	CHANNEL_ID VARCHAR(25),
# 	CHANNEL_LINK VARCHAR(35),
# 	CHANNEL_NAME VARCHAR(250),
# 	SUBSCRIBER_COUNT NUMBER(38,0),
# 	BANNER_LINK VARCHAR(310),
# 	DESCRIPTION VARCHAR(2501),
# 	KEYWORDS VARCHAR(750),
# 	AVATAR VARCHAR(165),
# 	COUNTRY VARCHAR(40),
# 	TOTAL_VIEWS NUMBER(38,0),
# 	TOTAL_VIDEOS NUMBER(38,0),
# 	JOIN_DATE DATE,
# 	MEAN_VIEWS_LAST_30_VIDEOS NUMBER(20,10),
# 	MEDIAN_VIEWS_LAST_30_VIDEOS NUMBER(20,10),
# 	STD_VIEWS_LAST_30_VIDEOS NUMBER(20,10),
# 	VIDEOS_PER_WEEK NUMBER(4,2)
# );'''))

my_schema = ''' YOUTUBE (
	CHANNEL_ID VARCHAR(25),
	CHANNEL_LINK VARCHAR(35),
	CHANNEL_NAME VARCHAR(250),
	SUBSCRIBER_COUNT NUMBER(38,0),
	BANNER_LINK VARCHAR(310),
	DESCRIPTION VARCHAR(2501),
	KEYWORDS VARCHAR(750),
	AVATAR VARCHAR(165),
	COUNTRY VARCHAR(40),
	TOTAL_VIEWS NUMBER(38,0),
	TOTAL_VIDEOS NUMBER(38,0),
	JOIN_DATE DATE,
	MEAN_VIEWS_LAST_30_VIDEOS NUMBER(20,10),
	MEDIAN_VIEWS_LAST_30_VIDEOS NUMBER(20,10),
	STD_VIEWS_LAST_30_VIDEOS NUMBER(20,10),
	VIDEOS_PER_WEEK NUMBER(4,2)
);'''

print(get_query(f"Give me {os.environ.get('TOTAL_COUNT_VALIDATION')} for the following {os.environ.get('SNOWFLAKE')} table.{my_schema}"))
print(get_query(f"Give me {os.environ.get('COUNT_VALIDATION')} for the following {os.environ.get('SNOWFLAKE')} table.{my_schema}"))

# print(get_query('''
#     Convert the following query into aggregation query for snowflake, select s.total_views,d.total_views from youtube s inner join youtube_copy d on s.channel_id = d.channel_id;
# '''))
