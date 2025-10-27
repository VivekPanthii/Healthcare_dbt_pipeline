import os
import json
import time
import logging
import boto3
from botocore.exceptions import ClientError

bucket_name="healthcare-etl-pipeline"
s3= boto3.client('s3',region_name='ap-south-1')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


data_folder='data_source'


def source_to_s3_extraction(local_file_path,s3_key):
    try:
        s3.upload_file(local_file_path,bucket_name,s3_key)
        logger.info(f"Extraction of file {s3_key}, COMPLETED!!!!!")
    except ClientError as e:
        logger.error(f"Opss!!!, SOMETHING WENT WRONG: {e}")

def s3_object_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise


def extraction():
        try:
            # scan all folders
            for folder in os.listdir(data_folder):
                # dynamic metapath and datapath
                meta_path=f"{data_folder}/{folder}/metadata/{folder}.json"
                data_path=f"{data_folder}/{folder}/data"
                
                # load existing metadata
                if os.path.exists(meta_path):
                    with open(meta_path, 'r') as f:
                        metadata = json.load(f)
                else:
                    metadata = {}
                
                

                # scan data_sources for files
                file_list = [f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]
                for file_name in file_list:
                    if file_name not in metadata or metadata[file_name]["status"] != "processed":
                        s3_key=f"{folder}/data/{file_name}"
                        file_path = os.path.join(data_path,file_name)

                    
                    # --- Simulate reading/processing the file ---
                        logger.info(f"Processing file: {file_name}")



                        # must send the file_name from here to s3 bucket
                        if not s3_object_exists(bucket_name, s3_key):
                            source_to_s3_extraction(file_path, s3_key)

                        
                        # --- Update metadata ---
                        metadata[file_name] = {
                            "status": "processed",
                            "last_modified": os.path.getctime(file_path),
                            "processed_time": time.time()
                        }


                os.makedirs(os.path.dirname(meta_path), exist_ok=True)
                with open(meta_path, 'w') as f:
                    json.dump(metadata, f, indent=4)
            logger.info("All new files processed and metadata updated.")
        except Exception as e:
            logger.error(f"Opps, Something went wrong {e}")



if __name__ == "__main__":
    print("Testing extraction function...")
    extraction()

    



