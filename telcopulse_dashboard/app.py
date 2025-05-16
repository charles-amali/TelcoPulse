import streamlit as st
import pandas as pd
import plotly.express as px
from s3_utils import S3Connector
import os
from dotenv import load_dotenv
import time

load_dotenv()

# Set page config
st.set_page_config(
    page_title="Dashboard",
    page_icon="📈",
    layout="wide"
)

# Initialize S3 connector
@st.cache_resource
def get_s3_connector():
    return S3Connector()

# Add auto-refresh functionality
def auto_refresh():
    time.sleep(300)  # Sleep for 5 minutes
    st.rerun()

# Get configuration from environment variables
bucket_name = os.getenv('S3_BUCKET_NAME',"telco-pulse-135")
prefix = os.getenv('S3_PREFIX', "processed-data/")

if not bucket_name:
    st.error("S3_BUCKET_NAME environment variable is not set!")

# Main content
st.markdown("<h1 style='text-align: center;'>TelcoPulse: Real-Time Network Metrics</h1>", unsafe_allow_html=True)

# st.title("TelcoPulse: Real-Time Network Metrics")

# Load data
if bucket_name:
    with st.spinner("Loading data from S3..."):
        s3_connector = get_s3_connector()
        
        # Step 1: List all subfolder prefixes inside the main prefix
        try:
            base_prefix = prefix if prefix.endswith('/') else f"{prefix}/"
            folders = s3_connector.fs.ls(f"{bucket_name}/{base_prefix}", detail=True)
            table_prefixes = [
                f"{base_prefix}{os.path.basename(f['Key'])}/"
                for f in folders if f['StorageClass'] != 'GLACIER'
            ]
        except Exception as e:
            st.error(f"Failed to list folders: {e}")
            st.stop()

        # Step 2: Let user select a table to view
        table_name_to_prefix = {
            p.rstrip('/').split('/')[-1]: p for p in table_prefixes
        }

        selected_table = st.selectbox("Select a table to load", list(table_name_to_prefix.keys()))
        selected_prefix = table_name_to_prefix[selected_table]

        # Step 3: Load selected table
        df = s3_connector.read_all_parquet(selected_prefix)
        
        if df is not None:
            # st.success(f"Successfully loaded {len(df)} rows of data!")
            
            # Display basic information
            st.subheader("Dataset Overview")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", len(df))
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
            
            # Display data preview
            st.subheader("Data Preview")
            st.dataframe(df.tail())
            
            # Column selection for visualization
            st.subheader("Data Visualization")
            col1, col2 = st.columns(2)
            
            with col1:
                x_col = st.selectbox("Select X-axis", df.columns)
            with col2:
                y_col = st.selectbox("Select Y-axis", df.columns)
            
            # Create visualization
            if x_col and y_col:
                try:
                    # Create two columns for visualizations
                    viz_col1, viz_col2 = st.columns(2)
                    
                    # Bar plot in first column
                    with viz_col1:
                        bar_fig = px.bar(df, x=x_col, y=y_col, 
                                       title=f"Bar: {y_col} vs {x_col}")
                        st.plotly_chart(bar_fig, use_container_width=True)
                    
                    # Line plot in second column 
                    with viz_col2:
                        line_fig = px.box(df, x=x_col, y=y_col,
                                         title=f"Box Plot: {y_col} vs {x_col}")
                        st.plotly_chart(line_fig, use_container_width=True)
                        
                except Exception as e:
                    st.error(f"Error creating visualization: {str(e)}")
            
            # Display column statistics
            st.subheader("Column Statistics")
            st.dataframe(df.describe())
            
        else:
            st.error("No data found in the specified location. Please check your bucket name and prefix.")
else:
    st.info("Please enter an S3 bucket name in the sidebar to begin.")

# Start auto-refresh in background
if bucket_name:
    auto_refresh() 