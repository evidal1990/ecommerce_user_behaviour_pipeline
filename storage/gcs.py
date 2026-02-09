from google.cloud import storage
from google.auth import default

credentials, project = default()


def get_blobs(bucket_name):
    blobs_list = []
    client = storage.Client()
    blobs = client.bucket(bucket_name).list_blobs()
    for blob in blobs:
        blobs_list.append(blob.name)
    return blobs_list
