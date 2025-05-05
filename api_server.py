from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import threading
import pymysql
import pandas as pd
import os
import pyodbc
import gspread
import pymongo
from urllib.parse import urlparse
from oauth2client.service_account import ServiceAccountCredentials

# Create FastAPI app
api_app = FastAPI(title="DB Connection API")

# Define request models
class MySQLConnectionRequest(BaseModel):
    host: str
    username: str
    password: str
    database: str
    port: int = 3306

class ExcelConnectionRequest(BaseModel):
    file_path: str

class ConnectionRequest(BaseModel):
    data_source: str
    parameters: dict

# Supported Data Sources
SUPPORTED_DATA_SOURCES = ["mysql", "sqlserver", "mongodb", "excel", "csv", "google_sheets"]

# API endpoints
@api_app.post("/connect/mysql")
def connect_mysql_endpoint(request: MySQLConnectionRequest):
    try:
        connection = pymysql.connect(
            host=request.host,
            user=request.username,
            password=request.password,
            database=request.database,
            port=request.port
        )
        if connection.open:
            connection.close()
            return {"status": "success", "message": "Connected to MySQL successfully!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_app.post("/connect/excel")
def connect_excel_endpoint(request: ExcelConnectionRequest):
    try:
        if not os.path.exists(request.file_path):
            raise HTTPException(status_code=404, detail=f"Excel file not found: {request.file_path}")
        pd.read_excel(request.file_path, sheet_name=None)
        return {"status": "success", "message": "Connected to Excel file successfully!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api_app.post("/connect")
def connect_database(request: ConnectionRequest):
    """
    Connect to different data sources based on the user's selection.
    Currently focusing on MySQL and Excel connections.
    """
    if request.data_source not in SUPPORTED_DATA_SOURCES:
        raise HTTPException(status_code=400, detail=f"Unsupported data source: {request.data_source}")

    try:
        if request.data_source == "mysql":
            return connect_mysql(request.parameters)
        elif request.data_source == "sqlserver":
            return connect_sqlserver(request.parameters)
        elif request.data_source == "mongodb":
            return connect_mongodb(request.parameters)
        elif request.data_source == "excel":
            return connect_excel(request.parameters)
        elif request.data_source == "csv":
            return connect_csv(request.parameters)
        elif request.data_source == "google_sheets":
            return connect_google_sheets(request.parameters)
        else:
            # For other data sources, return a message indicating they're not currently supported
            return {
                "status": "info",
                "message": f"Connection to {request.data_source} is not currently implemented."
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")

# Connection handlers
def connect_mysql(params):
    try:
        # ✅ Check for missing parameters
        required_keys = ["host", "username", "password", "database"]
        missing_params = [key for key in required_keys if key not in params or not params[key]]
        if missing_params:
            return {
                "status": "error",
                "message": f"Missing required parameters: {', '.join(missing_params)}"
            }

        # ✅ Check for empty values
        empty_params = [key for key in required_keys if params[key] == ""]
        if empty_params:
            return {
                "status": "error",
                "message": f"Empty values provided for: {', '.join(empty_params)}"
            }

        # ✅ Establish MySQL connection
        connection = pymysql.connect(
            host=params["host"],
            user=params["username"],
            password=params["password"],
            database=params["database"],
            port=params.get("port", 3306)
        )

        if connection.open:
            connection.close()  # ✅ Close connection after checking
            return {
                "status": "success",
                "message": "Connected to MySQL successfully!"
            }

    except pymysql.err.OperationalError as e:
        if "Access denied" in str(e):
            return {
                "status": "error",
                "message": "Access denied. Please check your username and password."
            }
        return {
            "status": "error",
            "message": f"MySQL Connection Issue: {str(e)}"
        }
    except pymysql.err.DatabaseError as e:
        return {
            "status": "error",
            "message": f"MySQL Database Error: {str(e)}"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Unexpected MySQL Error: {str(e)}"
        }

    return {
        "status": "error",
        "message": "Failed to connect to MySQL."
    }

def connect_sqlserver(params):
    try:
        # ✅ Check for required parameters
        required_keys = ["server", "database"]
        for key in required_keys:
            if key not in params:
                return {"status": "error", "message": f"Missing required parameter: {key}"}, 400

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={params['server']};"
            f"DATABASE={params['database']};"
            f"Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_str)
        conn.close()
        return {"status": "success", "message": "Connected to SQL Server successfully!"}

    except pyodbc.InterfaceError as e:
        # 👇 Split database error from login/interface error
        if "Cannot open database" in str(e):
            return {"status": "error", "message": f"SQL Server Database Error: {str(e)}"}, 500
        return {"status": "error", "message": f"SQL Server Interface Error: {str(e)}"}, 400
    except pyodbc.OperationalError as e:
        return {"status": "error", "message": f"SQL Server Operational Error: {str(e)}"}, 500
    except pyodbc.DatabaseError as e:
        return {"status": "error", "message": f"SQL Server Database Error: {str(e)}"}, 500
    except pyodbc.Error as e:
        return {"status": "error", "message": f"SQL Server General Error: {str(e)}"}, 500
    except KeyError as e:
        return {"status": "error", "message": f"Missing required parameter: {str(e)}"}, 400
    except Exception as e:
        return {"status": "error", "message": f"Unexpected SQL Server Error: {str(e)}"}, 500

    return {"status": "error", "message": "Failed to connect to SQL Server."}, 500

def connect_mongodb(params):
    try:
        # ✅ Check for required parameters
        required_keys = ["mongo_url", "database"]
        for key in required_keys:
            if key not in params:
                return {"status": "error", "message": f"Missing required parameter: {key}"}, 400

        mongo_url = params["mongo_url"]
        database = params["database"]

        # ✅ Validate MongoDB URL format
        parsed_url = urlparse(mongo_url)
        if not parsed_url.scheme.startswith("mongodb"):
            return {"status": "error", "message": "Invalid MongoDB URL format."}, 400

        # ✅ Attempt connection
        client = pymongo.MongoClient(mongo_url, serverSelectionTimeoutMS=5000)

        # ✅ Check if the database exists
        if database not in client.list_database_names():
            return {"status": "error", "message": f"Database '{database}' not found."}, 400

        # ✅ Connection successful
        client.close()
        return {"status": "success", "message": f"Connected to MongoDB database '{database}' successfully!"}

    except pymongo.errors.ServerSelectionTimeoutError:
        return {"status": "error", "message": "MongoDB Server unreachable. Please check the connection URL."}, 500
    except pymongo.errors.OperationFailure as e:
        return {"status": "error", "message": f"MongoDB Authentication failed: {str(e)}"}, 401
    except pymongo.errors.ConnectionFailure as e:
        return {"status": "error", "message": f"MongoDB Connection failed: {str(e)}"}, 500
    except pymongo.errors.InvalidURI as e:
        return {"status": "error", "message": f"Invalid MongoDB URI: {str(e)}"}, 400
    except KeyError as e:
        return {"status": "error", "message": f"Missing required parameter: {str(e)}"}, 400
    except Exception as e:
        return {"status": "error", "message": f"Unexpected MongoDB Error: {str(e)}"}, 500

    return {"status": "error", "message": "Failed to connect to MongoDB."}, 500

def connect_excel(params):
    try:
        # ✅ Check for missing file path
        if "file_path" not in params or not params["file_path"]:
            return {
                "status": "error",
                "message": "Missing required parameter: file_path"
            }

        file_path = params["file_path"]

        # ✅ Check for empty file path
        if file_path == "":
            return {
                "status": "error",
                "message": "Empty file path provided"
            }

        # ✅ Check if the path exists and is a file
        if not os.path.exists(file_path):
            return {
                "status": "error",
                "message": "File not found"
            }
        if os.path.isdir(file_path):
            return {
                "status": "error",
                "message": "Provided path is a directory, not a file"
            }

        # ✅ Check for valid Excel extensions
        if not file_path.endswith(('.xlsx', '.xls')):
            return {
                "status": "error",
                "message": "Unsupported file format. Please use .xlsx or .xls"
            }

        # ✅ Try reading the Excel file
        try:
            df = pd.read_excel(file_path)
            return {
                "status": "success",
                "message": f"Excel file loaded successfully with {len(df)} rows!",
                "columns": list(df.columns)
            }

        except pd.errors.ParserError:
            return {
                "status": "error",
                "message": "Failed to parse the Excel file"
            }
        except ValueError as ve:
            return {
                "status": "error",
                "message": f"ValueError while reading Excel: {str(ve)}"
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error reading the Excel file: {str(e)}"
            }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Excel Error: {str(e)}"
        }

def connect_csv(params):
    try:
        file_path = params.get("file_path")

        # ✅ Check if file path is missing
        if not file_path:
            return {"status": "error", "message": "Missing file path."}, 400

        # ✅ Check if the path exists and is a file
        if not os.path.exists(file_path):
            return {"status": "error", "message": "File not found."}, 400
        if os.path.isdir(file_path):
            return {"status": "error", "message": "Provided path is a directory, not a file."}, 400

        # ✅ Check for valid CSV extensions
        if not file_path.endswith('.csv'):
            return {"status": "error", "message": "Unsupported file format. Please use .csv"}, 400

        # ✅ Try reading the CSV file
        try:
            df = pd.read_csv(file_path)
            return {
                "status": "success",
                "message": f"CSV file loaded successfully with {len(df)} rows!",
                "columns": list(df.columns)
            }

        except pd.errors.EmptyDataError:
            return {"status": "error", "message": "CSV file is empty."}, 400
        except pd.errors.ParserError:
            return {"status": "error", "message": "Failed to parse the CSV file."}, 400
        except ValueError as ve:
            return {"status": "error", "message": f"ValueError while reading CSV: {str(ve)}"}, 400
        except Exception as e:
            return {"status": "error", "message": f"Error reading the CSV file: {str(e)}"}, 500

    except FileNotFoundError:
        return {"status": "error", "message": "CSV file not found."}, 400
    except Exception as e:
        return {"status": "error", "message": f"CSV Error: {str(e)}"}, 500

def connect_google_sheets(params):
    try:
        # ✅ Check for missing parameters
        required_keys = ["credentials_file", "sheet_id"]
        for key in required_keys:
            if key not in params:
                return {"status": "error", "message": f"Missing required parameter: {key}"}

        credentials_file = params["credentials_file"]
        sheet_id = params["sheet_id"]

        # ✅ Check if credentials file exists
        if not os.path.exists(credentials_file):
            return {"status": "error", "message": f"Credentials file not found: {credentials_file}"}

        # ✅ Authenticate with Google Sheets
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
            client = gspread.authorize(credentials)
        except ValueError:
            return {"status": "error", "message": "Invalid credentials file format. Please check the JSON structure."}
        except Exception as e:
            return {"status": "error", "message": f"Google Sheets authentication failed: {str(e)}"}

        # ✅ Open the Google Sheets document
        try:
            sheet = client.open_by_key(sheet_id)
        except gspread.exceptions.SpreadsheetNotFound:
            return {"status": "error", "message": "Google Sheets document not found. Please check the sheet_id."}
        except gspread.exceptions.APIError as e:
            if "403" in str(e):
                return {"status": "error", "message": "Permission denied. Ensure the credentials file has access to the specified Google Sheet."}
            return {"status": "error", "message": f"Google Sheets API Error: {str(e)}"}
        except Exception as e:
            return {"status": "error", "message": f"Unexpected error while accessing Google Sheets: {str(e)}"}

        # ✅ Connection successful
        return {"status": "success", "message": "Connected to Google Sheets successfully!"}

    except Exception as e:
        return {"status": "error", "message": f"Unexpected Google Sheets Error: {str(e)}"}

def start_api_server():
    def run_server():
        uvicorn.run(api_app, host="127.0.0.1", port=8000)
    
    api_thread = threading.Thread(target=run_server)
    api_thread.daemon = True
    api_thread.start()

if __name__ == "__main__":
    start_api_server()