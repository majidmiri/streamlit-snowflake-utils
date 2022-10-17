import streamlit as st
from snowflake.snowpark.session import Session
import json
import pandas as pd
import graphviz as graphviz
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode, JsCode

with open("creds.json") as f:
        connection_parameters = json.load(f)    
session = Session.builder.configs(connection_parameters).create()


@st.experimental_memo
def get_tables(database_id):
    return pd.DataFrame({'<Select>'},columns=['OBJECT_NAME']).append(session.sql("""
    SELECT 
        table_schema||'.'||table_name||' ('||table_id::string||')' object_name
    FROM 
        snowflake.account_usage.tables
    WHERE 
        table_catalog_id={0} 
        AND deleted IS NULL
    ORDER BY 1
    """.format(database_id)).toPandas())
    


@st.experimental_memo
def get_queries(query_window,object_id,access_type):
    return session.sql("""
        WITH cte_access_history AS
        (
        SELECT
            ah.query_id,
            src.value:columns source_columns,
            src.value:objectDomain::varchar source_object_type,
            src.value:objectId::varchar source_object_id,
            src.value:objectName::varchar source_object_name,
            tgt.value:objectDomain::varchar target_object_type,
            tgt.value:objectId::varchar target_object_id,
            tgt.value:objectName::varchar target_object_name,
            tgt.value:columns target_columns
        FROM 
            snowflake.account_usage.access_history ah,
        LATERAL FLATTEN 
            (input => ah.{access_type}_OBJECTS_ACCESSED) as src,
        LATERAL FLATTEN 
            (input => ah.OBJECTS_MODIFIED) as tgt
        WHERE 
            query_start_time>=dateadd(day,{query_window},current_timestamp) 
            AND target_object_id = {object_id}
        )
        SELECT 
            qh.query_text,
            qh.start_time,
            qh.end_time,
            qh.execution_status,
            qh.query_type,
            ah.* 
        FROM 
            cte_access_history ah 
        INNER JOIN 
            snowflake.account_usage.query_history qh 
        ON ah.query_id = qh.query_id
        """.format(object_id=object_id,query_window=query_window,access_type=access_type)).to_pandas()


#st.set_page_config(layout="wide")

st.header('Access History')
st.markdown("***")      

df_databases = pd.DataFrame({'<Select>'},columns=['DATABASE_NAME']).append(session.sql("""
    SELECT 
        database_name||' ('||database_id||')' database_name
    FROM 
        snowflake.account_usage.databases
    WHERE 
        deleted IS NULL
    ORDER BY 1
""").toPandas())

sb_database = st.selectbox('Database:',df_databases['DATABASE_NAME'],key='select_database')
if not '<Select>' in sb_database:
    database_id = sb_database.split(' ')[1].replace('(','').replace(')','')

    df_tables = get_tables(database_id)

    sb_object = st.selectbox('Table/View:',df_tables['OBJECT_NAME'],key='select_object')

    sb_window = st.slider('Lookup Window (days):',min_value=1,max_value=365,value=10)

    sb_access_type = st.radio("Object Access Type?",('Base','Direct'))

    if not '<Select>' in sb_object:
        object_id = sb_object.split(' ')[1].replace('(','').replace(')','')            
        
        df_dependencies = get_queries(-1*sb_window,object_id,sb_access_type)

        df_dependencies_unique_queries = df_dependencies[["QUERY_ID","QUERY_TYPE","START_TIME","END_TIME"]].drop_duplicates().sort_values(by=['START_TIME'], ascending=False)

        gb = GridOptionsBuilder.from_dataframe(df_dependencies_unique_queries)
        gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc='sum', editable=True)
        cellsytle_jscode = JsCode("""
        function(params) {
            if (params.value == 'A') {
                return {
                    'color': 'white',
                    'backgroundColor': 'darkred'
                }
            } else {
                return {
                    'color': 'black',
                    'backgroundColor': 'white'
                }
            }
        };
        """)
        gb.configure_selection('single', use_checkbox=True, rowMultiSelectWithClick=False, suppressRowDeselection=True)
        gb.configure_grid_options(domLayout='normal')
        gridOptions = gb.build()

        grid_response = AgGrid(
            df_dependencies_unique_queries, 
            gridOptions=gridOptions,
            autoHeight=True,
            width='100%',
            data_return_mode='FILTERED', 
            update_mode='MODEL_CHANGED',
            allow_unsafe_jscode=True,
            enable_enterprise_modules=True,
            fit_columns_on_grid_load=True
            )

        df_dependencies_unique_queries = grid_response['data']
        selected = grid_response['selected_rows']
        selected_df = pd.DataFrame(selected)

        if len(selected_df.index)==1:
            query_id = str(selected_df["QUERY_ID"].values[0])
            df_dependencies_filtered = df_dependencies[df_dependencies["QUERY_ID"]==query_id]
            
            st.markdown("***")
            st.subheader('Query')
            st.markdown(str(df_dependencies_filtered.iloc[0]["QUERY_TEXT"]))

            sources = ""
            target = ""
            relationships = ""
            index = 0

            for row in df_dependencies_filtered.iterrows():
                source_columns = ""
                index += 1

                cols = json.loads(row[1]["SOURCE_COLUMNS"])
                for col in cols:
                    source_columns += """<tr><td align="left">{0}</td></tr>\n""".format(col["columnName"])

                sources += """
                source{index} [label=<\n
                    <table border="0" cellborder="1" cellspacing="0" cellpadding="2">\n
                        <tr><td bgcolor="#CCCCCC">{source_name}</td></tr>\n
                        {source_columns}
                    </table>\n
                >]\n
                """.format(index=index,source_name=str(row[1]["SOURCE_OBJECT_NAME"]),source_columns=source_columns)

                relationships += "source{index}->target; \n\n".format(index=index)

            cols = json.loads(row[1]["TARGET_COLUMNS"])
            target_columns = ""
            for col in cols:
                target_columns += """<tr><td align="left">{0}</td></tr>\n""".format(col["columnName"])


            target = """
                target [label=<
                    <table border="0" cellborder="1" cellspacing="0" cellpadding="2">
                        <tr><td bgcolor="#cccccc">{target_name}</td></tr>
                        {target_columns}
                    </table>
                >]  
            """.format(target_name=str(df_dependencies_filtered.iloc[0]["TARGET_OBJECT_NAME"]),target_columns=target_columns)

            graph = """
            digraph G {{

                node [shape=none, margin=0]
                edge [arrowtail=none, dir=both]
            
            {0}
            {1}
            {2}

            }} 
            """.format(sources,target,relationships)
            
            st.subheader('Diagram')
            st.graphviz_chart(graph,use_container_width=True)

