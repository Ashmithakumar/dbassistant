import streamlit as st
import requests
import os
from db_utils import current_db_config, get_db_config, get_db_connection

def update_mysql_config(host, user, password, database, port=3306):
    global current_db_config
    payload = {
        "data_source": "mysql",
        "parameters": {
            "host": host,
            "username": user,
            "password": password,
            "database": database,
            "port": port
        }
    }
    
    try:
        response = requests.post("http://127.0.0.1:8000/connect", json=payload)
        if response.status_code == 200:
            current_db_config["type"] = "mysql"
            current_db_config["mysql"] = {
                "host": host,
                "user": user,
                "password": password,
                "database": database,
                "port": port
            }
            st.session_state.db_connected = True
            st.session_state.db_config = current_db_config
            return True, "Connected to MySQL successfully!"
        else:
            return False, f"Connection failed: {response.json().get('detail', 'Unknown error')}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def update_excel_config(file_path):
    global current_db_config
    payload = {
        "data_source": "excel",
        "parameters": {
            "file_path": file_path
        }
    }
    
    try:
        response = requests.post("http://127.0.0.1:8000/connect", json=payload)
        if response.status_code == 200:
            current_db_config["type"] = "excel"
            current_db_config["excel"] = {"file_path": file_path}
            st.session_state.db_connected = True
            st.session_state.db_config = current_db_config
            return True, "Connected to Excel file successfully!"
        else:
            return False, f"Connection failed: {response.json().get('detail', 'Unknown error')}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def is_excel_updated(file_path):
    if not os.path.exists(file_path):
        return False
    current_time = os.path.getmtime(file_path)
    last_seen_time = st.session_state.get("last_excel_update")
    if last_seen_time is None or current_time > last_seen_time:
        st.session_state["last_excel_update"] = current_time
        return True
    return False

def is_any_mysql_table_updated():
    try:
        from schema_utils import get_database_schema
        schema = get_database_schema()
        conn = get_db_connection()
        if not schema or not conn:
            return False
        
        updated = False
        if "mysql_table_counts" not in st.session_state:
            st.session_state["mysql_table_counts"] = {}
        
        with conn.cursor() as cursor:
            for table in schema.keys():
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                count = cursor.fetchone()[0]
                last_count = st.session_state["mysql_table_counts"].get(table)
                if last_count is None or last_count != count:
                    st.session_state["mysql_table_counts"][table] = count
                    updated = True
        
        return updated
    except Exception as e:
        st.warning(f"MySQL update check failed: {e}")
        return False