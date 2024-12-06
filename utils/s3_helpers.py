import boto3


def get_s3_client(region_name, access_key_id, secret_access_key):
    """
    Initialize and return an S3 client.
    """
    return boto3.resource(
        "s3",
        region_name=region_name,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )


def upload_to_s3(local_file_path, bucket_name, s3_key, region_name, access_key_id, secret_access_key):
    """
    Upload a file to S3 with a specified S3 key (name).
    """
    try:
        # Initialize S3 client
        s3_client = get_s3_client(region_name, access_key_id, secret_access_key)
        bucket = s3_client.Bucket(bucket_name)

        # Open and upload the file
        with open(local_file_path, "rb") as file_data:
            bucket.put_object(Key=s3_key, Body=file_data)

        print(f"File '{local_file_path}' uploaded to bucket '{bucket_name}' as '{s3_key}'.")

    except FileNotFoundError:
        print(f"Error: The file '{local_file_path}' was not found.")
    except Exception as e:
        print("An error occurred during upload:", e)
        raise


def download_from_s3(bucket_name, s3_key, local_file_path, region_name, access_key_id, secret_access_key):
    """
    Download a file from S3 to a specified local path.
    """
    try:
        # Initialize S3 client
        s3_client = get_s3_client(region_name, access_key_id, secret_access_key)
        bucket = s3_client.Bucket(bucket_name)

        # Download the file
        bucket.download_file(Key=s3_key, Filename=local_file_path)
        print(f"File '{s3_key}' downloaded from bucket '{bucket_name}' to '{local_file_path}'.")

    except FileNotFoundError:
        print(f"Error: The file '{s3_key}' was not found in bucket '{bucket_name}'.")
    except Exception as e:
        print("An error occurred during download:", e)
        raise