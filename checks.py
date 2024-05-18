import streamlit as st
from dotenv import load_dotenv
import sys
import os
sys.path.append('../')
from dqFunc import getAzureSQLColumns,getSnowflakeColumns,validate
load_dotenv()

TYPE_SNOWFLAKE = os.getenv("TYPE_SNOWFLAKE")
TYPE_AZURE_SQL_SERVER = os.getenv("TYPE_AZURE_SQL_SERVER")

def checks_list(source_connection_details=None,source_database=None,source_schema=None,source_table=None,check_type=None,target_connection_details=None,target_database=None,target_schema=None,target_table=None):
    st.title(check_type)
    styles = """
        <style>
            [data-testid="stHorizontalBlock"], [data-testid="stMultiSelect"] {
                margin-left: 30px;
            }
        </style>
    """

    listOfChecks = {
        "Count check": False,
        "Null check": False,
        "Schema check": False,
        "Duplicate check": False,
        "Aggregation check": False,
        "Data type check": False,
        "Trailing/Leading spaces check": False,
        "Pattern check": False,
        "Length/size of column check": False,
        "In set check": False
    }
    
    listOfTableColumns = {
        "SourceColumns": getSnowflakeColumns(connectionDetails=source_connection_details,database=source_database,schema=source_schema,table=source_table) if source_connection_details['type'] != TYPE_AZURE_SQL_SERVER else getAzureSQLColumns(connectionDetails=source_connection_details,schema=source_schema,table=source_table),
        "TargetColumns": ["Super", "Bat", "Wonder", "knk"]
    }

    listOfChecksColumnsList = {}
    mappedColumns = {}

    st.markdown(styles, unsafe_allow_html=True)
    st.header("Perform Checks")
    qualityCheckType = check_type
    mappedSourceToTargetColumns = {}
    checks_where_columns_not_needed = ("Count check", "Schema check")
    checks_where_columns_mapping_not_needed = ("Aggregation check")
    st.header("Select the Checks to perform")
    for check in listOfChecks.keys():
        listOfChecks[check] = st.checkbox(check)
        if(check not in checks_where_columns_not_needed):
            mappedSourceColumns = []
            mappedTargetColumns = []
            match qualityCheckType:
                case "Data Validation":
                    if(listOfChecks[check]):
                        options = st.multiselect("Choose the necessary columns to perform checks", listOfTableColumns["SourceColumns"], key=check)
                        listOfChecksColumnsList[check] = options
                case "Data Reconcilation":
                    if(listOfChecks[check]):
                        if(check not in checks_where_columns_mapping_not_needed):
                            minColumnListNum = min(len(listOfTableColumns["SourceColumns"]), len(listOfTableColumns["TargetColumns"]))
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write("Map necessary Source Columns")
                                for i in range(0, minColumnListNum):
                                    sourceColumnName = st.selectbox("Choose the Source column name", listOfTableColumns["SourceColumns"],
                                                                    index=None, key=check+"col1"+"_"+str(i), label_visibility="collapsed", 
                                                                    placeholder="Choose Source Column")
                                    mappedSourceColumns.append(sourceColumnName)

                            with col2:
                                st.write("Map necessary Target Columns")
                                for i in range(0, minColumnListNum):
                                    targetColumnName = st.selectbox("Choose the Target column name", listOfTableColumns["TargetColumns"],
                                                                    index=None, key=check+"col2""_"+str(i), label_visibility="collapsed", 
                                                                    placeholder="Choose Target Column")
                                    mappedTargetColumns.append(targetColumnName)
                            
                            mappedSourceToTargetColumns[check] = {"SourceColumns": mappedSourceColumns, "TargetColumns": mappedTargetColumns}
                        else:
                            col1, col2 = st.columns(2)
                            with col1:                       
                                sourceColumns = st.multiselect("Choose necessary Source Columns", listOfTableColumns["SourceColumns"],
                                                                    key=check+"col1", placeholder="Choose Source Columns")
                                
                            with col2:  
                                targetColumns = st.multiselect("Choose necessary Target Columns", listOfTableColumns["TargetColumns"],
                                                                    key=check+"col", placeholder="Choose Target Columns")
                                
                            mappedSourceToTargetColumns[check] = {"SourceColumns": sourceColumns, "TargetColumns": targetColumns}


    st.header("Results:")
    st.write(listOfChecksColumnsList)

    st.write(mappedSourceToTargetColumns)