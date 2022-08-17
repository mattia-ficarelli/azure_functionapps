from datetime import datetime
import os

from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    req_body = req.get_json()
    folder_path = "/"+ req_body.get("sourcePath")
    file_system= req_body.get("fileSystem_prod")
    connection_string = os.getenv("DATALAKE_CONNECTION_STRING_PROD")
    service_client = DataLakeServiceClient.from_connection_string(connection_string)
    #Start latestFolder script
    try:
        file_system_client = service_client.get_file_system_client(file_system=file_system)
        pathlist = list(file_system_client.get_paths(folder_path))
        folders = []
        for path in pathlist:
            folders.append(path.name.replace(folder_path.strip("/"), "").lstrip("/").rsplit("/", 1)[0])
            folders.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"), reverse=True)
            latestFolder = folders[0]
    except Exception as e:
        latestFolder = 'failure'
        print(e)
    #End Start latestFolder script
    return func.HttpResponse(f"{latestFolder}", status_code=200)