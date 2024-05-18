import streamlit as st;

def checks_list(connectionDetails=None,database=None,schema=None,table=None,type=None):
    st.title(type)
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
        "SourceColumns": ["Spider", "Captain", "Black"],
        "TargetColumns": ["Super", "Bat", "Wonder", "knk"]
    }

    listOfChecksColumnsList = {}
    mappedColumns = {}

    st.markdown(styles, unsafe_allow_html=True)
    st.header("Perform Checks")
    qualityCheckType = st.selectbox("Choose your Quality Check selection Type", ("Data Validation", "Data Reconcilation"))
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