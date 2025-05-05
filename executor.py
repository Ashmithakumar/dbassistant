import pandas as pd
import pymysql
import streamlit as st
from db_config import get_db_config, get_db_connection
from decimal import Decimal
from datetime import date, datetime

def decimal_to_serializable(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (date, datetime)):
        return obj.strftime("%Y-%m-%d")
    return obj

def execute_sql_query(query):
    db_config = get_db_config()
    db_type = db_config.get("type", "mysql")

    if db_type == "mysql":
        try:
            connection = get_db_connection()
            if not connection:
                return {"error": "Failed to establish MySQL connection"}

            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                for stmt in query.strip().split(";"):
                    if stmt.strip():
                        cursor.execute(stmt)
                results = cursor.fetchall()

            results = [{key: decimal_to_serializable(value) for key, value in row.items()} for row in results]
            return results
        except Exception as e:
            return {"error": f"SQL Execution Error: {str(e)}"}
        finally:
            if connection:
                connection.close()

    elif db_type == "excel":
        try:
            excel_path = get_db_connection()
            if not excel_path:
                return {"error": "Failed to get Excel file path"}
            
            excel_data = pd.read_excel(excel_path, sheet_name=None)
            local_namespace = {}
            
            for sheet_name, df in excel_data.items():
                safe_name = sheet_name.replace(" ", "_").replace("-", "_")
                local_namespace[safe_name] = df
                for i, (name, dataframe) in enumerate(excel_data.items()):
                    local_namespace[f"df_{i}"] = dataframe
            
            local_namespace["pd"] = pd
            local_namespace["excel_data"] = excel_data
            local_namespace["result"] = None
            local_namespace["print_output"] = []
            
            def custom_print(*args, **kwargs):
                output = " ".join(str(arg) for arg in args)
                local_namespace["print_output"].append(output)
            
            local_namespace["print"] = custom_print
            exec(query, globals(), local_namespace)
            
            result_df = local_namespace.get("result")
            
            if result_df is not None:
                if isinstance(result_df, pd.DataFrame):
                    return result_df.to_dict(orient="records")
                elif isinstance(result_df, (int, float, str, bool)):
                    return [{"result": decimal_to_serializable(result_df)}]
                else:
                    try:
                        return [{"result": str(result_df)}]
                    except:
                        return [{"result": "Result of type " + str(type(result_df).__name__)}]
            elif local_namespace["print_output"]:
                return [{"output": line} for line in local_namespace["print_output"]]
            else:
                return [{"message": "Query executed but no results returned"}]
                
        except Exception as e:
            return {"error": f"Excel Query Execution Error: {str(e)}"}

    elif st.session_state.get("selected_db_type") == "combined":
        try:
            mysql_conn = get_db_connection("mysql")
            excel_path = get_db_connection("excel")
            if not mysql_conn or not excel_path:
                return {"error": "Both MySQL and Excel connections are required for combined execution."}

            excel_data = pd.read_excel(excel_path, sheet_name=None)
            local_namespace = {
                "pd": pd,
                "conn": mysql_conn,
                "excel_data": excel_data,
                "result": None,
                "print_output": []
            }

            def custom_print(*args, **kwargs):
                output = " ".join(str(arg) for arg in args)
                local_namespace["print_output"].append(output)

            local_namespace["print"] = custom_print
            exec(query, globals(), local_namespace)
            result = local_namespace.get("result")

            if isinstance(result, pd.DataFrame):
                return result.to_dict(orient="records")
            elif isinstance(result, (int, float, str, bool)):
                return [{"result": decimal_to_serializable(result)}]
            elif local_namespace["print_output"]:
                return [{"output": line} for line in local_namespace["print_output"]]
            else:
                return [{"message": "Query executed but no results returned"}]

        except Exception as e:
            return {"error": f"Combined query execution error: {e}"}