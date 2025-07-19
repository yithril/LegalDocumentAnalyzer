import os
import logging
from azure.storage.blob import BlobServiceClient
from config import AZURE_STORAGE_CONNECTION_STRING

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class BlobService:
    def __init__(self):
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        except Exception as e:
            logger.exception(f"Error initializing BlobServiceClient: {e}")
            raise e

    def upload_file(self, file_path, tenant_id, project_id, blob_name=None):
        if blob_name is None:
            blob_name = os.path.basename(file_path)
        
        try:
            container_client = self._get_or_create_container(tenant_id, project_id)
            with open(file_path, "rb") as data:
                container_client.upload_blob(blob_name, data, overwrite=True)
            logger.info(f"File {blob_name} uploaded successfully to {container_client.url}")
        except Exception as e:
            logger.exception(f"Error uploading file {blob_name}: {e}")

    def download_file(self, tenant_id, project_id, blob_name):
        """Download a file from blob storage and return it as a stream.
        Returns the blob stream that can be read in chunks."""
        try:
            container_client = self._get_or_create_container(tenant_id, project_id)
            blob_client = container_client.get_blob_client(blob=blob_name)
            stream = blob_client.download_blob()
            logger.info(f"File {blob_name} stream created successfully from {container_client.url}")
            return stream
        except Exception as e:
            logger.exception(f"Error downloading file {blob_name}: {e}")
            raise e

    def delete_file(self, tenant_id, project_id, blob_name):
        try:
            container_client = self._get_or_create_container(tenant_id, project_id)
            blob_client = container_client.get_blob_client(blob=blob_name)
            blob_client.delete_blob()
            logger.info(f"File {blob_name} deleted successfully from {container_client.url}")
        except Exception as e:
            logger.exception(f"Error deleting file {blob_name}: {e}")
            raise e

    def _validate_tenant_and_project(self, tenant_id, project_id):
        """Validate tenant_id and project_id parameters."""
        if tenant_id is None or project_id is None:
            raise ValueError("tenant_id and project_id cannot be None")
        
        if not isinstance(tenant_id, str) or not isinstance(project_id, str):
            raise TypeError("tenant_id and project_id must be strings")
        
        if not tenant_id.strip() or not project_id.strip():
            raise ValueError("tenant_id and project_id cannot be empty strings")

    def _get_or_create_container(self, tenant_id, project_id):
        self._validate_tenant_and_project(tenant_id, project_id)
        
        container_name = f"{tenant_id}-{project_id}".lower()
        container_client = self.blob_service_client.get_container_client(container_name)
        try:
            container_client.create_container()
        except Exception:
            logger.exception(f"Error creating container {container_name}")
        return container_client
