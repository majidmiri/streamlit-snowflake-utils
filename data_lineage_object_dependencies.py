import streamlit as st
from snowflake.snowpark.session import Session
import json
import pandas as pd
import graphviz as graphviz

with open("creds.json") as f:
    connection_parameters = json.load(f)    
session = Session.builder.configs(connection_parameters).create()

st.header('Object Dependencies')
st.markdown("***")

df_databases = pd.DataFrame({'<Select>'},columns=['DATABASE_NAME']).append(session.sql("""
    SELECT 
        database_name||' ('||database_id||')' DATABASE_NAME
    FROM 
        snowflake.account_usage.databases
    WHERE 
        deleted IS NULL
    ORDER BY 1
""").toPandas())

sb_database = st.selectbox('Database:',df_databases['DATABASE_NAME'],key='select_database')
if not '<Select>' in sb_database:
    database_id = sb_database.split(' ')[1].replace('(','').replace(')','')
    df_tables = pd.DataFrame({'<Select>'},columns=['OBJECT_NAME']).append(session.sql("""
    SELECT 
        table_schema||'.'||table_name||' ('||table_id::string||')' OBJECT_NAME
    FROM 
        snowflake.account_usage.tables
    WHERE 
        table_catalog_id={0} 
        AND deleted IS NULL
    ORDER BY 1
    """.format(database_id)).toPandas())
    sb_object = st.selectbox('Table/View:',df_tables['OBJECT_NAME'],key='select_object')

    if not '<Select>' in sb_object:
        object_id = sb_object.split(' ')[1].replace('(','').replace(')','')            
        
        df_dependencies = session.sql("""
        WITH cte_dependencies AS
        (
            SELECT 
                referenced_object_id source_object_id,
                referencing_object_id target_object_id,
                '"'||referenced_database||'"."'||referenced_schema||'"."'||referenced_object_name||'"' source_object_name,                
                '"'||referencing_database||'"."'||referencing_schema||'"."'||referencing_object_name||'"' target_object_name 
            FROM                 
                snowflake.account_usage.object_dependencies
                -- If the view is slow, physicalize the view above and use the persisted table instead, for example: 
                -- snowflake_archive.account_usage.object_dependencies
        ),
        cte_parents AS 
        (
            SELECT 
                source_object_id,
                source_object_name,
                target_object_id,
                target_object_name 
            FROM 
                cte_dependencies 
            WHERE target_object_id={object_id}
            UNION ALL 
            SELECT 
                cte.source_object_id,
                cte.source_object_name,
                cte.target_object_id,
                cte.target_object_name 
            FROM 
                cte_dependencies cte
            INNER JOIN 
                cte_parents parent 
            ON cte.target_object_id=parent.source_object_id
        ),
        cte_children AS 
        (
            SELECT 
                source_object_id,
                source_object_name,
                target_object_id,
                target_object_name 
            FROM 
                cte_dependencies 
            WHERE source_object_id={object_id}
            UNION ALL 
            SELECT 
                cte.source_object_id,
                cte.source_object_name,
                cte.target_object_id,
                cte.target_object_name
            FROM 
                cte_dependencies cte
            INNER JOIN 
                cte_children child 
            ON cte.source_object_id=child.target_object_id
        )
        SELECT 
            source_object_id,
            REPLACE(source_object_name,'"','') source_object, 
            target_object_id,
            REPLACE(target_object_name,'"','') target_object 
        FROM cte_parents
        UNION ALL 
        SELECT 
            source_object_id,
            REPLACE(source_object_name,'"','') source_object, 
            target_object_id,
            REPLACE(target_object_name,'"','') target_object 
        FROM cte_children
        """.format(object_id=object_id)).to_pandas()

        rows = ""
        row_style = ""
        graph = graphviz.Digraph()
        for index, row in df_dependencies.iterrows():                   
            graph.node(str(row["SOURCE_OBJECT"]),shape='rectangle',style='rounded')
            graph.node(str(row["TARGET_OBJECT"]),shape='rectangle',style='rounded')
            if str(row["SOURCE_OBJECT_ID"])==object_id:
                graph.node(str(row["SOURCE_OBJECT"]),shape='rectangle',style='filled')
            if str(row["TARGET_OBJECT_ID"])==object_id:
                graph.node(str(row["TARGET_OBJECT"]),shape='rectangle',style='filled')
            graph.edge(str(row["SOURCE_OBJECT"]),str(row["TARGET_OBJECT"]))
        
        st.markdown("***")
        st.subheader('Diagram')
        st.graphviz_chart(graph,use_container_width=True)
    