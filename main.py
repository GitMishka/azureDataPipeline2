from azure.storage.blob import BlobServiceClient, BlobClient
import os
import config

connection_string = config.connection_string

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

container_name = "testcontainer"
local_dir = r"C:\Users\Misha\Desktop\GitHub\azureDataPipeline2"
blobs_to_download = ["sample.csv"]

def download_blobs(blob_service_client, container_name, local_dir, blobs_to_download=None):
    os.makedirs(local_dir, exist_ok=True)

    container_client = blob_service_client.get_container_client(container_name)

    if not blobs_to_download:
        blobs_to_download = [blob.name for blob in container_client.list_blobs()]

    for blob_name in blobs_to_download:
        download_path = os.path.join(local_dir, blob_name)
        print(f"Downloading blob: {blob_name}")

        blob_client = container_client.get_blob_client(blob_name)
        with open(download_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

download_blobs(blob_service_client, container_name, local_dir, blobs_to_download)
