import streamlit as st
import pymysql
import os

current_db_config = {
    "type": "mysql",
    "mysql": {
        "host": "",
        "user": "",
        "password": "",
        "database": "",
        "port": 3306
    },
    "excel": {
        "file_path": ""
    }
}

def get_db_config():
    if "db_connected" in st.session_state and st.session_state.db_connected:
        return st.session_state.db_config
    return current_db_config

def get_db_connection():
    db_config = get_db_config()
    db_type = db_config.get("type", "mysql")
    
    if db_type == "mysql":
        config = db_config.get("mysql", {})
        try:
            return pymysql.connect(
                host=config.get("host"),
                user=config.get("user"),
                password=config.get("password"),
                database=config.get("database"),
                port=config.get("port", 3306),
                autocommit=True
            )
        except Exception as e:
            st.error(f"MySQL connection error: {str(e)}")
            return None
    elif db_type == "excel":
        config = db_config.get("excel", {})
        file_path = config.get("file_path").replace("\\", "/")
        if os.path.exists(file_path):
            return file_path
        st.error(f"Excel file not found: {file_path}")
        return None