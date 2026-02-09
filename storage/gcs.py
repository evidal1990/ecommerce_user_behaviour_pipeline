import os
from google.cloud import storage
from google.auth import default

credentials, project = default()


def get_blobs(bucket):
    blobs_list = []
    client = storage.Client()
    blobs = client.bucket(bucket).list_blobs()
    for blob in blobs:
        blobs_list.append(blob.name)
    return blobs_list


def add_file(bucket, blob, file_path):
    client = storage.Client()
    bucket = client.bucket(bucket)

    file_name = os.path.basename(file_path)
    blob_path = f"{blob}/{file_name}"
    bucket.blob(blob_path).upload_from_filename(file_path)

    return bucket.blob(blob_path).exists()
