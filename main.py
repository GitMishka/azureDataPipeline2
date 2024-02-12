import os
from azure.storage.blob import BlockBlobService
import config


account_name = config.account_name
account_key = config.account_key

blob_service = BlockBlobService(account_name=account_name, account_key=account_key)


container_name = "testcontainer"

local_dir = r"C:\Users\Misha\Desktop\GitHub\azureDataPipeline2"


blobs_to_download = ["sample.csv"]  

def download_blobs(blob_service, container_name, local_dir, blobs_to_download=None):

    os.makedirs(local_dir, exist_ok=True)

    if not blobs_to_download:
        blobs_to_download = [blob.name for blob in blob_service.list_blobs(container_name)]

    for blob_name in blobs_to_download:
        download_path = os.path.join(local_dir, blob_name)
        print(f"Downloading blob: {blob_name}")
        blob_service.get_blob_to_path(container_name, blob_name, download_path)

download_blobs(blob_service, container_name, local_dir, blobs_to_download)
