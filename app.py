#from multiprocessing.sharedctypes import Value
import streamlit as st
import os

st.set_page_config(layout="wide")
root = os.path.join(os.path.dirname(__file__))

dashboards = {
    "Access History": os.path.join(root, "data_lineage_access_history.py"),
    "Object Dependencies": os.path.join(root, "data_lineage_object_dependencies.py")
}

choice_from_url = query_params = st.experimental_get_query_params().get("Access History", ["Access History"])[0]
index = list(dashboards.keys()).index(choice_from_url)

st.sidebar.header("Data Lineage App")
choice = st.sidebar.radio("Choose:", list(dashboards.keys()), index=index)

path = dashboards[choice]

with open(path, encoding="utf-8") as code:
    c = code.read()
    exec(c, globals())
