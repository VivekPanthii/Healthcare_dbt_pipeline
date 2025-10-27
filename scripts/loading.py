import boto3
import pandas as pd
from datetime import datetime
import snowflake.connector
import io
from dotenv import load_dotenv
import os
from botocore.exceptions import ClientError 
import logging
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# S3 setup
bucket_name = "healthcare-etl-pipeline"
s3= boto3.client('s3',region_name='ap-south-1')

# Snowflake connection

def snow_configuratin():
    try:
        logger.info("Connecting to Snowflake")
        conn = snowflake.connector.connect(
            user = os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        logger.info("Connected Successfully")
        return conn
    except Exception as e:
        logger.error(f"Something wrong: {e}")
        return

def load_folder(folder_name,folder_prefix,conn):
    logger.info(f"Processing Folder: {folder_name}")

    #Use paginator for more than 1000 files better for the future requirements
    paginator=s3.get_paginator('list_objects_v2')
    page_iterate=paginator.paginate(Bucket=bucket_name,Prefix=folder_prefix)

    all_files=[]

    for page in page_iterate:
        for obj in page.get('Contents',[]):
            if obj['Key'].endswith(".csv"):
                all_files.append(obj['Key'])

    if not all_files:
        logger.info(f"No CSV files found in {folder_name}")
        return

    # Load existing metadata for folder
    try:
        metadata_obj = s3.get_object(Bucket=bucket_name, Key=f"{folder_name}/metadata/{folder_name}.parquet")
        metadata = pd.read_parquet(io.BytesIO(metadata_obj['Body'].read()), engine="pyarrow")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            metadata = pd.DataFrame({
                    "s3_path": pd.Series(dtype="string"),
                    "loaded_time": pd.Series(dtype="datetime64[ns]"),
                    "status": pd.Series(dtype="string"),
                    "row_count": pd.Series(dtype="int")
            })
        else:
            raise



    # Filter unprocessed files
    processed_files=metadata['s3_path'].tolist()
    new_files = [f for f in all_files if f not in processed_files]
    if new_files !=[]:
        for file_path in new_files:
            file=os.path.basename(file_path)
            logger.info(file)
            try:
                # COPY INTO Snowflake
                sql = f"""
                        COPY INTO {folder_name} FROM '@raw_datasets/{file_path}' FILE_FORMAT=(TYPE= CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
                    """
                conn.cursor().execute(sql)
                status = "success"
                row_count = 0 #pending count rows if needed
            except Exception as e:
                status = "failed"
                row_count = 0

            # Update metadata
            metadata = pd.concat([metadata, pd.DataFrame([{
                "s3_path": file_path,
                "loaded_time": datetime.now(),
                "status": status,
                "row_count": row_count
            }])], ignore_index=True)

        # Save metadata back to S3
        out_buffer = io.BytesIO()
        metadata.to_parquet(out_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=f"{folder_name}/metadata/{folder_name}.parquet", Body=out_buffer.getvalue())
        logger.info(f"Metadata updated for {folder_name}")
        logger.info("All folder Processed")
    else:
        logger.info(f"No NewFiles are available in {folder_name}")



def main(conn):
    logger.info("Starting S3 Loading to Snowflake Loader.......")
    folder_list= s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
    for prefix in folder_list.get("CommonPrefixes",[]):
        folder_name=prefix["Prefix"].strip('/')
        load_folder(folder_name,f"{folder_name}/data/",conn)
    

if __name__ == "__main__":
    conn=snow_configuratin()
    main(conn)
    