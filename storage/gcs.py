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


def add_blob(bucket, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket)
    bucket.blob(f"{blob_name}/").upload_from_string("")


def del_blob(bucket, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket)
    bucket.blob(f"{blob_name}/").delete()
