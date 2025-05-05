import os
import json
import streamlit as st
import pandas as pd
import google.generativeai as genai
from db_utils import get_db_connection

def get_gemini_api_key():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        api_key = "AIzaSyDeAxlIRDKa1QS9dzNXsbo1Cwx4Clobe7k"
    return api_key

genai.configure(api_key=get_gemini_api_key())

def clean_query_output(query):
    # Remove markdown code blocks
    query = query.replace("```sql", "").replace("```python", "").replace("```", "")
    # Remove any comments
    query = "\n".join(line for line in query.split("\n") if not line.strip().startswith("#"))
    # Remove any markdown formatting
    query = query.replace("**", "").replace("*", "").replace("`", "")
    # Remove any leading/trailing whitespace
    query = query.strip()
    return query

def generate_sql_query(user_query, schema, db_type):
    schema_description = "\n".join(
        [f"Table {table}: {', '.join(schema[table])}" for table in schema]
    )

    if db_type == "mysql":
        prompt = f"""
        You are an expert SQL assistant. Convert the following natural language query into a valid MySQL query.

        ### Database Schema:
        {schema_description}

        ### CRITICAL OUTPUT FORMAT RULES:
        1. Return ONLY the SQL query, nothing else
        2. NO explanations, NO comments, NO markdown formatting
        3. NO code blocks (```sql or ```)
        4. NO step-by-step instructions
        5. NO text before or after the query
        6. Query must be executable as-is

        ### Important Considerations:
        1. Ensure the query executes correctly without syntax errors.
        2. Use GROUP_CONCAT and conditional aggregation for pivot-like transformations.
        3. Do not generate multi-line SQL queries—return a single-line version.

        ### Critical Query Rules:
        1️⃣ **Basic SQL Handling**
           - Use SUM(), COUNT(), and AVG() for total, count, or average calculations.
           - Enclose table and column names in backticks (`) if they contain MySQL reserved keywords.
           - Limit results to **5 rows** unless specified otherwise.

        2️⃣ **Pivot Handling**: 
           - If the query involves pivoting, generate the SQL query that uses `GROUP_CONCAT(DISTINCT ...)` to dynamically create the pivoted column names based on values in a specific column (e.g., `ColumnName`).
           - Use `SUM(IF(...))` to calculate sums for each distinct value in the pivoted column (e.g., `Invoice_Qty`).
           - The pivot will group by certain categories (e.g., `Category1`, `Category2`).

        3️⃣ **Advanced Filtering & Joins**
           - If multiple tables are involved, **automatically determine the correct JOIN conditions**.
           - If date filtering is needed, assume the **date column is named something like `created_at` or `date`** unless specified.
           - If the query involves fetching **latest or historical data**, use **ORDER BY DESC with LIMIT 1**.

        4️⃣ **Dynamic Columns & Transposition**
           - If the user asks for a **pivot-like transformation**, apply **GROUP_CONCAT()** or **conditional CASE statements** to restructure the output.
           - Ensure to **avoid subqueries** where possible and use **efficient joins** instead.

        ### Examples:
        1. Example for Pivoting a Column or crosstab reports:
           SET @sql = '';
           SELECT
               GROUP_CONCAT(DISTINCT
               CONCAT(
                   'SUM(IF(`ColumnName` = ''',
                   `ColumnName`,
                   ''', Invoice_Qty, 0)) AS ',
                   CONCAT('`', `ColumnName`, '`')
               )
               ) INTO @sql
           FROM table_name;

           SET @sql = CONCAT('SELECT Category1, Category2, ', @sql, ' 
                               FROM table_name 
                               GROUP BY Category1, Category2');

           PREPARE stmt FROM @sql;
           EXECUTE stmt;
           DEALLOCATE PREPARE stmt;
           
        2. Example of pivoting with `CASE WHEN`:
           SELECT 
               `GroupByColumn1`,
               `GroupByColumn2`,
               SUM(CASE WHEN `PivotColumn` = 'Value1' THEN `ValueColumn` ELSE 0 END) AS `Value1`,
               SUM(CASE WHEN `PivotColumn` = 'Value2' THEN `ValueColumn` ELSE 0 END) AS `Value2`,
               SUM(CASE WHEN `PivotColumn` = 'Value3' THEN `ValueColumn` ELSE 0 END) AS `Value3`
           FROM 
               table_name
           WHERE 
               `ConditionColumn` = 'SomeCondition'
           GROUP BY 
               `GroupByColumn1`, `GroupByColumn2`;

        ### User Query:
        {user_query}

        Return ONLY the SQL query, nothing else.
        """
    else:  # Excel
        excel_path = get_db_connection()
        excel_data = pd.read_excel(excel_path, sheet_name=None)
        sheet_names = list(excel_data.keys())
        
        prompt = f"""
        You are an expert data analyst. Convert the following natural language query into executable pandas code for Excel data.

        ### Excel Data Structure:
        - The Excel file has {len(sheet_names)} sheets: {', '.join([f"'{name}'" for name in sheet_names])}
        - Schema details: {schema_description}

        ### CRITICAL OUTPUT FORMAT RULES:
        1. Return ONLY executable pandas code, nothing else
        2. NO explanations, NO comments, NO markdown formatting
        3. NO code blocks (```python or ```)
        4. NO step-by-step instructions
        5. NO text before or after the code
        6. Code must be executable as-is

        ### Important Instructions:
        1. The Excel data is already loaded as follows:
           import pandas as pd
           excel_data = pd.read_excel('{excel_path}', sheet_name=None)
           {'; '.join([f"df_{i} = excel_data['{name}']" for i, name in enumerate(sheet_names)])}
        
        2. Use ONLY the variable names provided above (df_0, df_1, etc.) in your code
        3. DO NOT assume variables like 'Backlog' or 'Revenue' exist - use the numbered df variables
        4. If calculating totals or doing analysis, store the result in a variable called 'result'
        5. Return complete, executable pandas code that would solve the query

        ### Query Rules:
        - For "total" or "sum" queries by a category (like 'City' or 'Region'), use `.groupby()` followed by `.sum()`.
        - Always include the category (e.g., 'City') in the result so we know what the aggregation is grouped by.
        - For filtering, use `.query()` or boolean indexing.
        - Always handle potential missing data with `.dropna(subset=...)` when necessary.
        - Always include .reset_index() after groupby operations so that grouped columns are visible in the output.

        ### User Query:
        {user_query}

        Return ONLY the pandas code, nothing else.
        """

    try:
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        query = response.text.strip()
        query = clean_query_output(query)
        st.write("Generated Query:")
        st.text_area("Generated Query", value=query, height=300)
        return query
    except Exception as e:
        return f"Error generating query: {e}"

def generate_combined_query(user_query, schema):
    mysql_schema = schema.get("mysql", {})
    excel_schema = schema.get("excel", {})

    mysql_description = "\n".join([f"Table `{t}`: {', '.join(cols)}" for t, cols in mysql_schema.items()])
    excel_description = "\n".join([f"Sheet `{s}`: {', '.join(cols)}" for s, cols in excel_schema.items()])

    prompt = f"""
    You are a smart data assistant.

    You have access to two data sources:

    📊 MySQL tables:
    {mysql_description}

    📄 Excel sheets:
    {excel_description}

    ### CRITICAL OUTPUT FORMAT RULES:
    1. Return ONLY executable Python code, nothing else
    2. NO explanations, NO comments, NO markdown formatting
    3. NO code blocks (```python or ```)
    4. NO step-by-step instructions
    5. NO text before or after the code
    6. Code must be executable as-is

    📦 Database & File Info:
    - All SQL queries target a MySQL database.
    - Use `pd.read_sql("SELECT ...", conn)` for querying MySQL.
    - Assume conn is a valid mysql connection
    - DO NOT use `sqlite3.connect(...)`, `sqlite3`, or any SQLite-related functions.
    - You can assume `conn` is a valid MySQL connection created with `pymysql.connect(...)`.

    ⚠️ Rules:
    - ONLY use the tables, sheets, and column names given above in the schema.
    - Always include .reset_index() after groupby operations so that grouped columns are visible in the output
    - DO NOT invent new tables, columns, or sheet names.
    - DO NOT use SHOW TABLES, DESCRIBE, or dynamic schema discovery.
    - Use `pd.read_sql("...", conn)` for MySQL queries.
    - assume conn as a valid mysq connection
    - Use `excel_data = pd.read_excel(excel_file, sheet_name=None)` to access all Excel sheets.
    - To access a specific Excel sheet, use `excel_data['SheetName']`.

    🔧 Instructions:
    1. Identify all columns mentioned in the user query.
    2. Find which source (MySQL or Excel) each column comes from.
    3. If both sources are involved:
       - First, look for a shared column (e.g., product_id) to perform a merge.
       - If no common column exists, use `pd.concat`, `merge(..., how='cross')`, or join logically by calculating independently and combining results into one final DataFrame.
    4. Perform all filtering, aggregation, or transformation as needed.
    5. Always assign your final result to a DataFrame called `result_df`.
    6. Always return `result_df`.

    ### User Query:
    {user_query}

    Return ONLY the Python code, nothing else.
    """

    try:
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        query = response.text.strip()
        query = clean_query_output(query)
        st.write("Generated Combined Query:")
        st.text_area("Generated Query", value=query, height=300)
        return query
    except Exception as e:
        return f"Error generating combined query: {e}"