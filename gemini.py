import os
import random
from dotenv import load_dotenv
import google.generativeai as genai
import re

load_dotenv()

GOOGLE_API_KEY=random.choice(os.environ.get('GOOGLE_API_KEYS').split(','))


GOOGLE_API_KEY = 'AIzaSyBSjshPI1sWZliEH1C-hAAJTKLKfBzDt5o'
genai.configure(api_key=GOOGLE_API_KEY)


model = genai.GenerativeModel('gemini-1.5-pro-latest')

def get_query(query):
    print("QUERY IS \n", query)
    response = model.generate_content(f"{query}. Just give me ONLY the SQL query, with no other explanation or text. If you must use a code block, use ```sql ... ```")
    print("RESPONSE IS \n", response.text)

    # Attempt to extract from code block first.
    match = re.search(r"```sql\s*(.*?)\s*```", response.text, re.DOTALL)
    if match:
        return match.group(1).strip()

    # If no code block, try to get the first sentence that looks like a query.
    lines = response.text.split('\n')
    for line in lines:
        line = line.strip()
        if line.lower().startswith(('select', 'insert', 'update', 'delete', 'create', 'drop', 'alter')):
            return line

    # If all else fails, return an error message.
    raise Exception(f"Error: Could not extract SQL query from response: {response.text}")

    

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

# print(get_query(f"Give me {os.environ.get('COUNT_CHECK')} for the following {os.environ.get('SNOWFLAKE')} table.{my_schema}"))
