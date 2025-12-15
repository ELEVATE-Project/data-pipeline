import streamlit as st
import boto3
import configparser
import os
from botocore.exceptions import NoCredentialsError, ClientError

# --- Configuration ---
CONFIG_FILE = 'config.ini'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

# Helper function to get config safely
def get_config(section, key, default=None):
    try:
        return config.get(section, key)
    except (configparser.NoSectionError, configparser.NoOptionError):
        return default

# --- Constants ---
CSV_TYPES = ["chaupal or chavadi data", "mi stories data"]
INDIAN_STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
    "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram",
    "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu",
    "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal",
    "Andaman and Nicobar Islands", "Chandigarh", "Dadra and Nagar Haveli and Daman and Diu",
    "Delhi", "Jammu and Kashmir", "Ladakh", "Lakshadweep", "Puducherry"
]

# --- Page Setup ---
st.set_page_config(
    page_title="Shikshagraha Data Upload",
    page_icon="https://dashboard.shikshagraha.org/favicon.ico",
    layout="centered"
)

# Custom CSS for specific styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Roboto', sans-serif;
    }
    
    /* Background for the whole app */
    .stApp {
        background-color: #f0f2f5;
    }
    
    /* The main content area acts as the 'Card' */
    .block-container {
        background-color: white;
        padding: 2rem 3rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-top: 2rem;
        max-width: 900px;
    }

    h1 {
        color: #1a73e8; /* Brand Blue */
        font-weight: 700;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    
    .upload-header {
        text-align: center;
        color: #5f6368;
        margin-bottom: 3rem;
        font-size: 1.1rem;
    }
    
    /* Button Styling */
    .stButton>button {
        background-color: #1a73e8;
        color: white;
        border: none;
        border-radius: 4px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        width: 100%;
        transition: background-color 0.3s;
    }
    .stButton>button:hover {
        background-color: #1557b0; /* Darker Blue */
        color: white;
    }
    
    /* Selectbox styling */
    .stSelectbox label {
        color: #202124;
        font-weight: 500;
    }

    /* File Uploader styling */
    .stFileUploader {
        border: 1px dashed #dadce0;
        border-radius: 8px;
        padding: 1rem;
    }
    .stFileUploader label {
         color: #202124;
         font-weight: 500;
    }
    
    /* Logo container styling */
    .logo-container {
        display: flex;
        justify-content: center;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# --- Main Application ---

import pandas as pd
import logging
import io
import datetime

def main():
    # Logo Handling (Centered via HTML/CSS)
    logo_url = "https://shikshagraha.org/wp-content/themes/twentytwentythree-child/images/Group-22x.png"
    st.markdown(
        f"""
        <div class="logo-container">
            <img src="{logo_url}" width="200" alt="Shikshagraha Logo">
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("<h1>Data Upload Portal</h1>", unsafe_allow_html=True)
    st.markdown("<p class='upload-header'>Upload your Excel data securely to the Shikshagraha repository.</p>", unsafe_allow_html=True)

    with st.container():
        # Input Section
        col1, col2 = st.columns(2)
        
        with col1:
            csv_type = st.selectbox(
                "Select Data Type *", 
                options=["Select..."] + CSV_TYPES,
                index=0,
                help="Choose the category of data you are uploading."
            )
            
        with col2:
            state_name = st.selectbox(
                "Select State *", 
                options=["Select..."] + INDIAN_STATES,
                index=0,
                help="Select the state this data belongs to."
            )

        # File Uploader
        uploaded_file = st.file_uploader("Upload Excel File *", type=['xlsx'])

        # Submit Button
        if st.button("Submit Upload"):
            validate_and_process(csv_type, state_name, uploaded_file)

def validate_and_process(csv_type, state_name, uploaded_file):
    # Setup Logging
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger('upload_logger')
    logger.setLevel(logging.INFO)
    logger.handlers = [handler]

    # 1. Validation
    errors = []
    if csv_type == "Select...":
        errors.append("Please select a valid Data Type.")
    if state_name == "Select...":
        errors.append("Please select a valid State.")
    if uploaded_file is None:
        errors.append("Please upload an Excel file.")
    
    if errors:
        for error in errors:
            st.error(error, icon="🚨")
        return

    # Schema Validation
    try:
        xls = pd.ExcelFile(uploaded_file)
        
        # Determine required columns based on type
        required_cols = []
        if csv_type == "chaupal or chavadi data":
            required_cols = ['id', 'Title', 'Challenges', 'District', 'Date of Discussion']
        elif csv_type == "mi stories data":
            required_cols = ['id', 'Title', 'action_steps', 'impact', 'detected_district', 'detected_state', 'designation', 'Pdf', 'content', 'Images']

        found_valid_sheet = False
        valid_sheet_name = None
        
        for sheet in xls.sheet_names:
            df = pd.read_excel(uploaded_file, sheet_name=sheet)
            columns = list(df.columns)
            missing = [col for col in required_cols if col not in columns]
            
            if not missing:
                found_valid_sheet = True
                valid_sheet_name = sheet
                break
        
        if not found_valid_sheet:
            st.error(f"Validation Failed: Could not find a sheet with the required columns for '{csv_type}'.", icon="❌")
            st.error(f"Required columns: {', '.join(required_cols)}")
            return

    except Exception as e:
        st.error(f"Error reading Excel file: {e}", icon="❌")
        return

    logger.info(f"Params: Type={csv_type}, State={state_name}, File={uploaded_file.name}")
    logger.info(f"Validated Sheet: {valid_sheet_name}")

    with st.spinner("Uploading to S3..."):
        success = upload_to_s3(uploaded_file, csv_type, state_name, logger)
    

    if success:
        st.success(f"File uploaded successfully!", icon="✅")
        st.balloons()
        logger.info("Upload completed successfully.")
    else:
        logger.error("Upload failed.")

def upload_to_s3(file, csv_type, state_name, logger):
    # AWS Config
    bucket_name = get_config('AWS', 'bucket_name')
    region = get_config('AWS', 'region_name')
    aws_access_key = get_config('AWS', 'access_key_id')
    aws_secret_key = get_config('AWS', 'secret_access_key')

    logger.info(f"Connecting to bucket: {bucket_name} in region: {region}")

    if not bucket_name or bucket_name == 'your_bucket_name':
        msg = "AWS Bucket Name is not configured properly."
        st.error(msg, icon="❌")
        logger.error(msg)
        return False

    # Construct S3 Key
    safe_state = state_name.replace(" ", "_").lower()
    
    new_filename = ""
    if csv_type == "chaupal or chavadi data":
        new_filename = f"{safe_state}_chaupal.xlsx"
    elif csv_type == "mi stories data":
        new_filename = f"{safe_state}_mi_stories.xlsx"
    else:
        # Fallback
        safe_type = csv_type.replace(" ", "_").lower()
        new_filename = f"{safe_state}_{safe_type}.xlsx"

    s3_key = f"{new_filename}"

    logger.info(f"Generated S3 Key: {s3_key}")

    try:
        if aws_access_key and aws_secret_key and aws_access_key != 'your_access_key':
            logger.info("Using provided AWS credentials.")
            s3 = boto3.client(
                's3',
                region_name=region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
        else:
            logger.warning("No explicit credentials found in config, attempting environment/role logs.")
            s3 = boto3.client('s3', region_name=region)

        file.seek(0)
        
        logger.info(f"Initiating upload of {file.size} bytes as {new_filename}...")
        s3.upload_fileobj(file, bucket_name, s3_key)
        logger.info("Upload operation returned successfully.")
        return True

    except NoCredentialsError:
        msg = "AWS Credentials not found."
        st.error(msg, icon="❌")
        logger.critical(msg)
        return False
    except ClientError as e:
        msg = f"AWS S3 Error: {e}"
        st.error(msg, icon="❌")
        logger.error(msg)
        return False
    except Exception as e:
        msg = f"An unexpected error occurred: {e}"
        st.error(msg, icon="❌")
        logger.error(msg, exc_info=True)
        return False

if __name__ == "__main__":
    main()
