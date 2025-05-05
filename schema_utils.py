import os
import json
import pandas as pd
import pymysql
import streamlit as st
import google.generativeai as genai
from db_utils import get_db_connection, get_db_config

def get_database_schema(db_source=None):
    schema_file = "schema.json"
    db_config = get_db_config()
    db_type = db_source if db_source else db_config.get("type", "mysql")
    
    if db_type == "mysql":
        current_db = db_config.get("mysql", {}).get("database", "")
    else:
        current_db = os.path.basename(db_config.get("excel", {}).get("file_path", ""))
    
    if os.path.exists(schema_file):
        try:
            with open(schema_file, "r") as file:
                schema_data = json.load(file)
                if schema_data.get("database") == current_db and schema_data.get("db_type") == db_type:
                    return schema_data.get("schema", {})
        except Exception:
            pass
    
    schema = {}
    
    if db_type == "mysql":
        cfg = get_db_config()["mysql"]
        try:
            connection = pymysql.connect(
                host=cfg["host"],
                user=cfg["user"],
                password=cfg["password"],
                database=cfg["database"],
                port=cfg.get("port", 3306),
                autocommit=True
            )
            with connection.cursor() as cursor:
                cursor.execute("SHOW TABLES;")
                for (table_name,) in cursor.fetchall():
                    cursor.execute(f"DESCRIBE `{table_name}`;")
                    schema[table_name] = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            st.error(f"Error fetching MySQL schema: {e}")
        finally:
            if "connection" in locals() and connection:
                connection.close()
    else:
        try:
            excel_path = get_db_connection()
            if excel_path:
                excel_data = pd.read_excel(excel_path, sheet_name=None)
                for sheet_name, df in excel_data.items():
                    schema[sheet_name] = df.columns.tolist()
        except Exception as e:
            st.error(f"Error fetching Excel schema: {str(e)}")
    
    with open(schema_file, "w") as file:
        json.dump({
            "database": current_db,
            "db_type": db_type,
            "schema": schema
        }, file, indent=4)
    
    return schema

def get_combined_schema():
    schema = {}
    selected_type = st.session_state.get("selected_db_type", "mysql")
    
    if selected_type == "combined":
        schema["mysql"] = get_database_schema("mysql")
        schema["excel"] = get_database_schema("excel")
    else:
        schema = get_database_schema(selected_type)
    
    return schema

def save_combined_schema(schema):
    with open("combined_schema.json", "w") as file:
        json.dump(schema, file, indent=4)

def describe_schema_and_suggest_queries(schema, db_type):
    if not schema:
        return "No schema available. Please connect to a database first."
    
    description = []
    for table_name, columns in schema.items():
        description.append(f"### {table_name}")
        description.append("Columns:")
        for col in columns:
            description.append(f"- {col}")
        description.append("")
    
    prompt = f"""
    You are a database expert assistant. Analyze the following database schema and provide:
    1. A clear description of the database structure
    2. 5 relevant natural language queries that could be asked about this data
    3. For each query, explain what information it would provide

    Database Type: {db_type}
    
    Schema:
    {chr(10).join(description)}

    Format your response in markdown with clear sections.
    """
    
    try:
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        return f"Error generating schema description: {str(e)}"