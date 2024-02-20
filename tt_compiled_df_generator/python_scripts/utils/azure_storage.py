from azure.storage.blob import ContainerClient


class AzureStorage:

    def __init__(self):
        pass
        
    def delete_blob(self, **kwargs):
        print(f"    Deleting '{kwargs['blob_name']}' blob from '{kwargs['container_name']}' container...", end=" ")
        azure_storage_container_client = self._get_container_client(container_name=kwargs['container_name'])
        azure_storage_container_client.delete_blob(kwargs['blob_name'])
        print("done!")

    def download_csv_blob_as_dataframe(self, **kwargs):
        print(f"    Downloading '{kwargs['blob_name']}' blob from '{kwargs['container_name']}' container...", end=" ")
        azure_storage_container_client = self._get_container_client(container_name=kwargs['container_name'])
        csv_blob = azure_storage_container_client.download_blob(kwargs['blob_name'])
        print("done!")
        return csv_blob

    def upload_dataframe_as_csv_blob(self, **kwargs):
        print(f"    Uploading '{kwargs['blob_name']}' blob to '{kwargs['container_name']}' container...", end=" ")
        azure_storage_container_client = self._get_container_client(container_name=kwargs['container_name'])
        azure_storage_container_client.upload_blob(
            name=kwargs['blob_name'], data=kwargs['dataframe'].to_csv(index=False, encoding='utf-8'), overwrite=True,
            tags=dict(projectName="TTDFGenerator"))
        print("done!")

    def get_blob_names(self, **kwargs):
        azure_storage_container_client = self._get_container_client(container_name=kwargs['container_name'])
        blob_names_iterator = azure_storage_container_client.list_blob_names(
            name_starts_with="job_application_dataframe_{}".format(kwargs['country'].lower().replace(' ', '_')))
        return [blob_name for blob_name in blob_names_iterator]
            
    @staticmethod
    def _get_container_client(**kwargs):
        return ContainerClient.from_connection_string(
            conn_str="DefaultEndpointsProtocol=https;AccountName=hrdataframes;AccountKey=U7XjV+xhssVoFSQ1UiFU9StpCtyz9p9KY+/nwzQr1e7D+L50pxyPR1IOa5FEPxJ7UjFXAnENuBhY+AStZ8YdAQ==;EndpointSuffix=core.windows.net",
            container_name=kwargs['container_name'])
