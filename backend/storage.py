import os
import boto3
from abc import ABC, abstractmethod

class BaseStorageClient(ABC):
    @abstractmethod
    def list_files(self, folder_path: str):
        pass

    @abstractmethod
    def download_file(self, file_path: str) -> bytes:
        pass

    @abstractmethod
    def archive_file(self, file_path: str, archive_path: str):
        pass

    @abstractmethod
    def upload_bytes(self, file_bytes: bytes, target_path: str):
        pass

class S3Client(BaseStorageClient):
    def __init__(self):
        self.access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.region = os.getenv("AWS_REGION", "eu-north-1")
        
        if not all([self.access_key, self.secret_key, self.bucket_name]):
            raise ValueError("AWS credentials or bucket name missing from .env")
            
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region
        )

    def list_files(self, folder_path: str):
        # In S3, folder_path acts as a prefix
        prefix = folder_path if folder_path.endswith('/') else f"{folder_path}/"
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                # Return the full S3 Key so we can download it accurately
                if not obj['Key'].endswith('/'):  # Ignore folder objects
                    files.append(obj['Key'])
        return files

    def download_file(self, file_path: str) -> bytes:
        response = self.s3.get_object(Bucket=self.bucket_name, Key=file_path)
        return response['Body'].read()

    def archive_file(self, file_path: str, archive_path: str):
        # S3 does not have a native "move" command. We copy then delete.
        copy_source = {'Bucket': self.bucket_name, 'Key': file_path}
        self.s3.copy_object(CopySource=copy_source, Bucket=self.bucket_name, Key=archive_path)
        self.s3.delete_object(Bucket=self.bucket_name, Key=file_path)

    def upload_bytes(self, file_bytes: bytes, target_path: str):
        """Uploads raw bytes to a specific S3 path."""
        self.s3.put_object(Bucket=self.bucket_name, Key=target_path, Body=file_bytes)
