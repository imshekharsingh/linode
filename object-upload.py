import os
import time
import boto3
import mimetypes
import traceback
import requests
from botocore.exceptions import ClientError


LINODE_ACCESS_KEY = "_______________"
LINODE_SECRET_KEY = "______________"
BUCKET_NAME = "bucket_name"
ENDPOINT_URL = "https://<region_name>.linodeobjects.com"
REGION = "<region_name>"

# Define local and bucket folders (source and destination)
DIRECTORY_PATH = "../Desktop/images/"
BUCKET_BASE_PATH = "images/new-uploads"


TIME_THRESHOLD = 300

def generate_presigned_url(s3_client, bucket, key, content_type):
    try:
        return s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket,
                'Key': key,
                'ContentType': content_type,
                'ACL': 'public-read',
            },
            ExpiresIn=300,
            HttpMethod='PUT'
        )
    except ClientError as e:
        print(f"Failed to generate presigned URL for {key}: {e}")
        return None

def upload_with_presigned_url(presigned_url, file_path, content_type):
    with open(file_path, "rb") as f:
        file_bytes = f.read()

    headers = {
        'Content-Type': content_type,
        # 'x-amz-acl': 'public-read'
        'x-amz-acl': 'private'
    }

    try:
        resp = requests.put(presigned_url, data=file_bytes, headers=headers)
        if resp.status_code == 200:
            print(f"Uploaded successfully via pre-signed URL: {file_path}")
        else:
            print(f"Failed to upload {file_path}: Status {resp.status_code} | {resp.text}")
    except Exception as e:
        print(f"Error during requests.put: {e}")
        traceback.print_exc()

def upload_files():
    s3_client = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=LINODE_ACCESS_KEY,
        aws_secret_access_key=LINODE_SECRET_KEY,
        endpoint_url=ENDPOINT_URL
    )

    current_time = time.time()

    for root, _, files in os.walk(DIRECTORY_PATH):
        for file in files:
            if file.startswith("."):
                print(f"Skipping hidden file: {file}")
                continue

            file_path = os.path.join(root, file)

            # Files updated in last 5 mins
            last_modified_time = os.path.getmtime(file_path)
            if current_time - last_modified_time > TIME_THRESHOLD:
                print(f"Skipping old file: {file_path}")
                continue

            relative_path = os.path.relpath(file_path, DIRECTORY_PATH)
            object_key = f"{BUCKET_BASE_PATH}/{relative_path}".replace("\\", "/")
            content_type, _ = mimetypes.guess_type(file_path)
            content_type = content_type or "application/octet-stream"

            print(f"📁 Preparing to upload: {file_path} → {object_key}")

            url = generate_presigned_url(s3_client, BUCKET_NAME, object_key, content_type)
            if url:
                upload_with_presigned_url(url, file_path, content_type)

if __name__ == "__main__":
    upload_files()