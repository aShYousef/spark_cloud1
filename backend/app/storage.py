import os
import uuid
import tempfile
from abc import ABC, abstractmethod
from typing import Optional
import aiofiles
from .config import settings


# =====================================================
# Abstract Storage Backend
# =====================================================
class StorageBackend(ABC):

    @abstractmethod
    async def save_file(self, file_content: bytes, filename: str, folder: str = "") -> str:
        pass

    @abstractmethod
    async def get_file(self, file_path: str) -> bytes:
        pass

    @abstractmethod
    async def delete_file(self, file_path: str) -> bool:
        pass

    @abstractmethod
    async def list_files(self, folder: str = "") -> list:
        pass

    @abstractmethod
    def get_download_url(self, file_path: str) -> str:
        pass

    @abstractmethod
    async def download_to_temp(self, file_path: str) -> str:
        """
        Download file to local temp path (required for Spark).
        Returns absolute local file path.
        """
        pass


# =====================================================
# Local Storage Implementation
# =====================================================
class LocalStorage(StorageBackend):

    def __init__(self, base_path: str = "storage"):
        self.base_path = base_path
        os.makedirs(os.path.join(base_path, "uploads"), exist_ok=True)
        os.makedirs(os.path.join(base_path, "results"), exist_ok=True)

    async def save_file(self, file_content: bytes, filename: str, folder: str = "uploads") -> str:
        file_id = str(uuid.uuid4())
        ext = os.path.splitext(filename)[1]
        safe_filename = f"{file_id}{ext}"

        folder_path = os.path.join(self.base_path, folder)
        os.makedirs(folder_path, exist_ok=True)

        file_path = os.path.join(folder_path, safe_filename)
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(file_content)

        return os.path.join(folder, safe_filename)

    async def get_file(self, file_path: str) -> bytes:
        full_path = os.path.join(self.base_path, file_path)
        async with aiofiles.open(full_path, "rb") as f:
            return await f.read()

    async def delete_file(self, file_path: str) -> bool:
        full_path = os.path.join(self.base_path, file_path)
        if os.path.exists(full_path):
            os.remove(full_path)
            return True
        return False

    async def list_files(self, folder: str = "") -> list:
        folder_path = os.path.join(self.base_path, folder)
        if not os.path.exists(folder_path):
            return []
        return os.listdir(folder_path)

    def get_download_url(self, file_path: str) -> str:
        return f"/api/files/download/{file_path}"

    async def download_to_temp(self, file_path: str) -> str:
        """
        For Spark: return real local file path
        """
        full_path = os.path.join(self.base_path, file_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(file_path)
        return os.path.abspath(full_path)


# =====================================================
# S3 Storage Implementation
# =====================================================
class S3Storage(StorageBackend):

    def __init__(self):
        import boto3
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION,
        )
        self.bucket = settings.S3_BUCKET

    async def save_file(self, file_content: bytes, filename: str, folder: str = "uploads") -> str:
        file_id = str(uuid.uuid4())
        ext = os.path.splitext(filename)[1]
        key = f"{folder}/{file_id}{ext}"

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=file_content
        )
        return key

    async def get_file(self, file_path: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=file_path)
        return obj["Body"].read()

    async def delete_file(self, file_path: str) -> bool:
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=file_path)
            return True
        except Exception:
            return False

    async def list_files(self, folder: str = "") -> list:
        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=folder)
        return [obj["Key"] for obj in resp.get("Contents", [])]

    def get_download_url(self, file_path: str) -> str:
        return self.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": file_path},
            ExpiresIn=3600,
        )

    async def download_to_temp(self, file_path: str) -> str:
        """
        Download S3 object to /tmp for Spark processing
        """
        suffix = os.path.splitext(file_path)[1]
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        self.s3.download_fileobj(self.bucket, file_path, tmp_file)
        tmp_file.close()
        return tmp_file.name


# =====================================================
# Factory
# =====================================================
def get_storage() -> StorageBackend:
    if settings.STORAGE_BACKEND == "s3" and settings.AWS_ACCESS_KEY_ID:
        return S3Storage()
    return LocalStorage()
