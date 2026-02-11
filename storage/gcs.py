import os
from google.cloud import storage
from google.auth import default

credentials, project = default()


class GoogleCloudStorage:
    def __init__(self, bucket_name) -> None:
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def get_blobs(self) -> list:
        blobs_list = []
        blobs = self.bucket.list_blobs()
        for blob in blobs:
            blobs_list.append(blob.name)
        return blobs_list

    def add_file(self, blob, file_path) -> bool:
        file_name = os.path.basename(file_path)
        blob_path = f"{blob}/{file_name}"
        self.bucket.blob(blob_path).upload_from_filename(file_path)

        return self.bucket.blob(blob_path).exists()
