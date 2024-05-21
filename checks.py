import streamlit as st
from dotenv import load_dotenv
import sys
import os
import pandas as pd
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
        "TargetColumns": [] if check_type == "Data Validation" else getSnowflakeColumns(connectionDetails=target_connection_details,database=target_database,schema=target_schema,table=target_table) if target_connection_details['type'] != TYPE_AZURE_SQL_SERVER else getAzureSQLColumns(connectionDetails=target_connection_details,schema=target_schema,table=target_table)
    }

    listOfChecksColumnsList = {}
    mappedColumns = {}

    st.markdown(styles, unsafe_allow_html=True)
    st.header("Perform Checks")
    qualityCheckType = check_type
    mappedSourceToTargetColumns = {}
    checks_where_columns_not_needed = ("Count check", "Schema check")
    checks_where_columns_mapping_not_needed = ("Aggregation check")
    # data_df = None;
    st.header("Select the Checks to perform")

    def storeMappedValues(data_df, mappedSourceToTargetColumns):
        sourceColumns = data_df["Map Source Columns"].tolist();
        targetColumns = data_df["Map Target Columns"].tolist();

        # mappedSourceToTargetColumns[check] = {"SourceColumns": sourceColumns, "TargetColumns": targetColumns}
        print("sourceColumns: ", sourceColumns);
        print("targetColumns: ", targetColumns);
    
    def upperAndReplace(checkStr):
        return checkStr.upper().replace(' ', '_');

    for check in listOfChecks.keys():
        # check = check.upper().replace(' ', '_');
        listOfChecks[check] = st.checkbox(check)
        if(check not in checks_where_columns_not_needed):
             # check = check.upper().replace(' ', '_');
            mappedSourceColumns = []
            mappedTargetColumns = []
            match qualityCheckType:
                case "Data Validation":
                    if(listOfChecks[check]):
                        options = st.multiselect("Choose the necessary columns to perform checks", listOfTableColumns["SourceColumns"], key=check)
                        listOfChecksColumnsList[upperAndReplace(check)] = options
                case "Data Reconciliation":
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

                            mappedColumns = dict(zip(mappedSourceColumns, mappedTargetColumns));
                            filteredMappedColumns = {k: v for k, v in mappedColumns.items() if v is not None}
                            mappedSourceToTargetColumns[upperAndReplace(check)] = filteredMappedColumns
                        else:
                            col1, col2 = st.columns(2)
                            with col1:                       
                                sourceColumns = st.multiselect("Choose necessary Source Columns", listOfTableColumns["SourceColumns"],
                                                                    key=check+"col1", placeholder="Choose Source Columns")
                                
                            with col2:  
                                targetColumns = st.multiselect("Choose necessary Target Columns", listOfTableColumns["TargetColumns"],
                                                                    key=check+"col", placeholder="Choose Target Columns")
                                
                            mappedSourceToTargetColumns[upperAndReplace(check)] = {"SourceColumns": sourceColumns, "TargetColumns": targetColumns}


                    # if(listOfChecks[check]):
                    #     data_df = pd.DataFrame(
                    #         {
                    #             "Map Source Columns": [],
                    #             "Map Target Columns": []

                    #         }
                    #     )

                    #     mappedColumns = st.data_editor(
                    #         data_df,
                    #         column_config={
                    #            "Map Source Columns": st.column_config.SelectboxColumn(
                    #                 "Map Source Columns",
                    #                 width="medium",
                    #                 options=listOfTableColumns["SourceColumns"],
                    #                 required=True,
                    #             ),
                    #              "Map Target Columns": st.column_config.SelectboxColumn(
                    #                 "Map Target Columns",
                    #                 width="medium",
                    #                 options=listOfTableColumns["TargetColumns"],
                    #                 required=True,
                    #             )
                    #         },
                    #         hide_index=True,
                    #         num_rows="dynamic",
                    #         key=check,
                    #         width=1000,
                    #         on_change = storeMappedValues(data_df, mappedSourceToTargetColumns)
                    #     )

                    #     st.write(mappedColumns);

                        # sourceColumns = data_df["Map Source Columns"].tolist();
                        # targetColumns = data_df["Map Target Columns"].tolist();

                        # mappedSourceToTargetColumns[check] = {"SourceColumns": sourceColumns, "TargetColumns": targetColumns}
        else:
            match qualityCheckType:
                case "Data Validation":
                    if(listOfChecks[check]):
                        listOfChecksColumnsList[upperAndReplace(check)] = [];
                case "Data Reconciliation":
                    if(listOfChecks[check]):
                        mappedSourceToTargetColumns[upperAndReplace(check)] = {};

    st.header("Results:")
    st.write(listOfChecksColumnsList)
    st.write(mappedSourceToTargetColumns);


    st.write(mappedSourceToTargetColumns)
    print(mappedSourceToTargetColumns)