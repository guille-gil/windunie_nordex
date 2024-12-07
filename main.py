from scripts.data_retrieval_nordex_vib import retrieve_data
from utils.s3_helpers import upload_to_s3
from datetime import datetime

# Nordex API
# directly configured in data_retrieval_nordex_vib.py

# Retrieval configuration
START_DATE = datetime(2024, 11, 29)
END_DATE = datetime(2024, 11, 30)

""" END_DATE = datetime.now()
    START_DATES = end - datetime.timedelta(days=1) """

# S3 Bucket configuration
S3_KEY = "data/all_data.parquet"
REGION_NAME = "eu-central-1"
ACCESS_KEY_ID = "accesspoint-github-ptt7hytm6rgx8qbuq7ey4fsjacnmneuc1a-s3alias"
SECRET_ACCESS_KEY = "uFdotTBIT8Sq22puPEgo7BkkHgEn2UvjZbRgKDp9"  # Replace with env variable for security
BUCKET_NAME = "windunie.nordex"
LOCAL_FILE_PATH = "/Users/guillermogildeavallebellido/Desktop/Windunie/project_git/data/raw/all_data.parquet"


def main():
    # Step 1: Retrieve Data
    print("Starting data retrieval...")
    dataframes = retrieve_data(START_DATE, END_DATE)

    # Step 2: Save Data Locally
    if not dataframes.empty:
        print("Data retrieved. Saving to Parquet file...")
        dataframes.to_parquet(LOCAL_FILE_PATH)

        # Step 3: Upload to S3
        # Bear in mind its uploading everything in the local folder. Manually removed for now.
        print("Uploading Parquet file to S3...")
        upload_to_s3(
            local_file_path=LOCAL_FILE_PATH,
            bucket_name=BUCKET_NAME,
            s3_key=S3_KEY,
            region_name=REGION_NAME,
            access_key_id=ACCESS_KEY_ID,
            secret_access_key=SECRET_ACCESS_KEY,
        )
        print(f"File successfully uploaded to S3: {BUCKET_NAME}/{S3_KEY}")
    else:
        print("No data retrieved. Exiting.")

if __name__ == "__main__":
    main()