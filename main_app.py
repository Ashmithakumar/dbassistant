import streamlit as st
import pandas as pd
import os
import plotly.express as px
from api_server import start_api_server
from db_config import (
    update_mysql_config,
    update_excel_config,
    get_db_config,
    get_db_connection,
    is_excel_updated,
    is_any_mysql_table_updated
)
from schema_utils import (
    get_database_schema,
    get_combined_schema,
    describe_schema_and_suggest_queries
)
from query_engine import (
    generate_sql_query,
    generate_combined_query
)
from executor import execute_sql_query
from geo_utils import detect_geo_column, show_geo_heatmap

# Start the API server
start_api_server()

# Apply Custom CSS for UI Styling
st.markdown(
    """
    <style>
        .title {
            text-align: center;
            font-size: 32px;
            font-weight: bold;
            color: #ffffff;
            background: linear-gradient(90deg, #ff7e5f, #feb47b);
            padding: 10px;
            border-radius: 10px;
        }
        .stTextArea>label { 
            font-size: 18px; 
            font-weight: bold;
            color: #333333;
        }
        .stButton>button {
            width: 100%;
            background-color: #ff7e5f;
            color: white;
            font-size: 16px;
            font-weight: bold;
            border-radius: 8px;
        }
        .connection-form {
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
        }
        .db-type-selector {
            margin-bottom: 20px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

def display_dashboard(df):
    """Display interactive dashboard for the query results"""
    tab1, tab2, tab3 = st.tabs(["📄 Table View", "📊 Charts", "🗺️ Map View"])

    # ✅ Tab 1: Table View
    with tab1:
        st.subheader("📄 Table View")
        st.dataframe(df)

    # ✅ Tab 2: Charts
    with tab2:
        st.subheader("📊 Auto Charts")

        numeric_cols = df.select_dtypes(include=['number']).columns
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        datetime_cols = df.select_dtypes(include=['datetime64', 'datetime']).columns

        # Try convert possible date columns
        for col in df.columns:
            if 'date' in col.lower() or 'month' in col.lower() or 'year' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
                    pass
        datetime_cols = df.select_dtypes(include=['datetime64[ns]', 'datetime']).columns

        # 📊 Bar Chart
        if len(categorical_cols) > 0 and len(numeric_cols) > 0:
            group_col = categorical_cols[0]
            value_col = numeric_cols[0]
            chart_data = df.groupby(group_col)[value_col].sum().reset_index()
            st.subheader("📊 Bar Chart")
            st.plotly_chart(px.bar(chart_data, x=group_col, y=value_col, title=f"{value_col} by {group_col}"))

        # 📈 Line Chart
        if len(datetime_cols) > 0 and len(numeric_cols) > 0:
            time_col = datetime_cols[0]
            metric = numeric_cols[0]
            time_data = df[[time_col, metric]].dropna().sort_values(by=time_col)
            st.subheader("📈 Line Chart")
            st.plotly_chart(px.line(time_data, x=time_col, y=metric, title=f"{metric} over Time"))

        # 🧁 Pie Chart
        if len(categorical_cols) > 0 and len(numeric_cols) > 0 and df[categorical_cols[0]].nunique() <= 10:
            cat_col = categorical_cols[0]
            val_col = numeric_cols[0]
            pie_data = df.groupby(cat_col)[val_col].sum().reset_index()
            st.subheader("🧁 Pie Chart")
            st.plotly_chart(px.pie(pie_data, names=cat_col, values=val_col, title=f"{val_col} by {cat_col}"))

        # 🔥 Correlation Heatmap
        if len(numeric_cols) >= 2:
            corr = df[numeric_cols].corr()
            st.subheader("🔥 Correlation Heatmap")
            fig = px.imshow(corr, text_auto=True, title="Correlation Heatmap")
            st.plotly_chart(fig)

    # ✅ Tab 3: Map View
    with tab3:
        st.subheader("🗺️ Map View")
        geo_col = detect_geo_column(df)
        if geo_col and len(df.select_dtypes(include=['number']).columns) > 0:
            metric_col = df.select_dtypes(include=['number']).columns[0]
            show_geo_heatmap(df, geo_col, metric_col)
        else:
            st.info("No geographic data found to display map.")


def main():
    st.markdown('<div class="title">Database Assistant (Powered by Gemini 1.5 Pro)</div>', unsafe_allow_html=True)
    
    if "db_connected" not in st.session_state:
        st.session_state.db_connected = False
    
    with st.expander("🔌 Database Connection", expanded=not st.session_state.db_connected):
        st.markdown('<div class="connection-form">', unsafe_allow_html=True)
        
        st.markdown('<div class="db-type-selector">', unsafe_allow_html=True)
        db_type = st.radio("Select Database Type", ["MySQL", "Excel", "Combined"], horizontal=True)
        st.markdown('</div>', unsafe_allow_html=True)
        st.session_state["selected_db_type"] = db_type.lower()
        
        if db_type == "MySQL":
            col1, col2 = st.columns(2)
            with col1:
                host = st.text_input("Host")
                user = st.text_input("Username")
                database = st.text_input("Database")
            with col2:
                port = st.number_input("Port", value=3306)
                password = st.text_input("Password", type="password")

            if st.button("Connect to MySQL Database"):
                success, message = update_mysql_config(host, user, password, database, port)
                if success:
                    st.success(message)
                    st.session_state.mysql_connected = True
                else:
                    st.error(message)

        elif db_type == "Excel":
            file_path = st.text_input("Excel File Path")
            if st.button("Connect to Excel File"):
                success, message = update_excel_config(file_path)
                if success:
                    st.success(message)
                    st.session_state.excel_connected = True
                else:
                    st.error(message)

        elif db_type == "Combined":
            st.subheader("🔌 MySQL Configuration")
            col1, col2 = st.columns(2)
            with col1:
                host = st.text_input("MySQL Host")
                user = st.text_input("MySQL Username")
                database = st.text_input("MySQL Database")
            with col2:
                port = st.number_input("MySQL Port", value=3306)
                password = st.text_input("MySQL Password", type="password")

            file_path = st.text_input("Excel File Path")

            if st.button("Connect to Both MySQL & Excel"):
                mysql_success, mysql_msg = update_mysql_config(host, user, password, database, port)
                excel_success, excel_msg = update_excel_config(file_path)
                if mysql_success and excel_success:
                    st.success("✅ Connected to both MySQL and Excel!")
                    st.session_state.mysql_connected = True
                    st.session_state.excel_connected = True
                    st.session_state.db_connected = True
                else:
                    st.error("❌ Failed to connect to both. Check details.")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    if st.session_state.db_connected:
        db_config = get_db_config()
        db_type = db_config.get("type", "mysql")

        if db_type == "mysql":
            if is_any_mysql_table_updated():
                st.info("🔄 MySQL database was updated.")
            st.success(f"✅ Connected to MySQL database: {db_config['mysql']['database']} on {db_config['mysql']['host']}")
        elif db_type == "excel":
            file_path = db_config["excel"]["file_path"]
            if is_excel_updated(file_path):
                st.info("🔄 Excel file was updated.")
            st.success(f"✅ Connected to Excel file: {file_path}")
        elif db_type == "combined":
            st.success("✅ Connected to both MySQL and Excel!")

        with st.expander("🔍 Explore Database Schema & Suggestions", expanded=False):
            selected_type = st.session_state.get("selected_db_type", "mysql")
            
            if selected_type == "combined":
                schema = get_combined_schema()
                mysql_schema = schema.get("mysql", {})
                excel_schema = schema.get("excel", {})

                mysql_info = describe_schema_and_suggest_queries(mysql_schema, "mysql")
                excel_info = describe_schema_and_suggest_queries(excel_schema, "excel")

                st.markdown("### 🗃️ MySQL Schema")
                st.markdown(mysql_info)
                st.markdown("---")
                st.markdown("### 📄 Excel Schema")
                st.markdown(excel_info)
            else:
                schema = get_database_schema()
                schema_info = describe_schema_and_suggest_queries(schema, selected_type)
                st.markdown(schema_info)

        user_input = st.text_area("Enter your query:")

        if st.button("Generate & Execute Query"):
            schema = get_combined_schema() if selected_type == "combined" else get_database_schema()

            if not schema:
                st.error("Cannot generate query without a database schema. Please connect to a database first.")
            else:
                if selected_type == "combined":
                    query = generate_combined_query(user_input, schema)
                else:
                    query = generate_sql_query(user_input, schema, selected_type)

                if "Error" in query:
                    st.error(query)
                else:
                    if selected_type == "mysql":
                        st.code(query, language="sql")
                    elif selected_type == "excel":
                        st.code(query, language="python")
                    else:
                        st.code(query, language="python")

                    results = execute_sql_query(query)
                    if isinstance(results, dict) and "error" in results:
                        st.error(results["error"])
                    else:
                        st.write("### Query Results")
                        df = pd.DataFrame(results)
                        
                        # Show dashboard toggle
                        show_dashboard = st.checkbox("Show Interactive Dashboard", value=True)
                        if show_dashboard:
                            display_dashboard(df)
                        else:
                            # Show the original view with geo heatmap
                            st.dataframe(df)
                            geo_col = detect_geo_column(df)
                            if geo_col and len(df.select_dtypes(include=['number']).columns) > 0:
                                metric_col = df.select_dtypes(include=['number']).columns[0]
                                show_geo_heatmap(df, geo_col, metric_col)

    else:
        st.warning("Please connect to a database first to use the assistant.")

if __name__ == "__main__":
    main()