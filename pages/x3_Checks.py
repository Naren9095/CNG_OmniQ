import streamlit as st;
# from constants import listOfChecks;

# listOfChecks = {
#     "COUNT_CHECK": "Count check",
#     "NULL_CHECK": "Null check",
#     "SCHEMA_CHECK": "Schema check",
#     "DUPLICATE_CHECK": "Duplicate check",
#     "AGGREGATION_CHECK": "Aggregation check",
#     "DATA_TYPE_CHECK": "Data type check",
#     "TRAILING_SPACES_CHECK": "Trailing/Leading spaces check",
#     "PATTERN_CHECK": "Pattern check",
#     "SIZE_OF_COLUMN_CHECK": "Length/size of column check",
#     "IN_SET_CHECK": "In set check"
# }

styles = """
    <style>
        [data-testid="stMultiSelect"], [data-testid="stHorizontalBlock"] {
            margin-left: 30px;
        }
    </style>
"""

# qualityCheckType = "Data Reconcilation";

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

listOfChecksColumnsList = {};
mappedColumns = {};

st.markdown(styles, unsafe_allow_html=True);
st.title('OmniQSuite - Compare your data in a single platform');
st.header("Perform Checks");

qualityCheckType = st.selectbox("Choose your Quality Check selection Type", ("Data Validation", "Data Reconcilation"));



mappedSourceToTargetColumns = {}

st.header("Select the Checks to perform");
for check in listOfChecks.keys():
    listOfChecks[check] = st.checkbox(check);
    mappedSourceColumns = [];
    mappedTargetColumns = [];
    match qualityCheckType:
        case "Data Validation":
            if(listOfChecks[check]):
                options = st.multiselect("Choose the necessary columns to perform checks", listOfTableColumns["SourceColumns"], key=check);
                listOfChecksColumnsList[check] = options;
        case "Data Reconcilation":
            if(listOfChecks[check]):
                minColumnListNum = min(len(listOfTableColumns["SourceColumns"]), len(listOfTableColumns["TargetColumns"]))
                col1, col2 = st.columns(2);
                with col1:
                    st.write("Map necessary Source Columns");
                    for i in range(0, minColumnListNum):
                        sourceColumnName = st.selectbox("Choose the Source column name", listOfTableColumns["SourceColumns"],
                                                        index=None, key=check+"col1"+"_"+str(i), label_visibility="collapsed", 
                                                        placeholder="Choose Source Column");
                        mappedSourceColumns.append(sourceColumnName);

                with col2:
                    st.write("Map necessary Target Columns");
                    for i in range(0, minColumnListNum):
                        targetColumnName = st.selectbox("Choose the Target column name", listOfTableColumns["TargetColumns"],
                                                        index=None, key=check+"col2""_"+str(i), label_visibility="collapsed", 
                                                        placeholder="Choose Target Column");
                        mappedTargetColumns.append(targetColumnName);
                
                mappedSourceToTargetColumns[check] = {"SourceColumns": mappedSourceColumns, "TargetColumns": mappedTargetColumns}



st.header("Values");
# st.write(listOfChecks);
st.write(listOfChecksColumnsList);

st.write(mappedSourceToTargetColumns);