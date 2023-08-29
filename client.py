from io import BytesIO
from logging import exception

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from config import getLogger, settings
from fastapi import HTTPException, status

logger = getLogger("status_logger")


class S3Client:
    config = Config(
        read_timeout=settings.s3_client_read_timeout_s, connect_timeout=settings.s3_client_connect_timeout_s
    )

    def __init__(self):
        logger.debug("Creating minio client")
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=self.config,
            region_name=settings.s3_region,
        )
        self.bucket = settings.s3_bucket

    async def _send_request(self, command, request_kwargs):
        try:
            request_command = getattr(self.client, command)
        except AttributeError:
            logger.exception(f"{command} AttributeError")
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Unexpected behavior",
            )
        logger.debug(f"Sending request {command} with kwargs {request_kwargs}")
        try:
            response = request_command(
                Bucket=self.bucket,
                **request_kwargs,
            )
        except ClientError as e:
            error = e.response.get("Error", {})
            try:
                code = e.response["ResponseMetadata"]['HTTPStatusCode']
            except KeyError:
                code = status.HTTP_500_INTERNAL_SERVER_ERROR
            raise HTTPException(
                status_code=code,
                detail=error.get("Message", "Images storage communication problem"),
            )
        logger.debug(f"S3 Client returning response {request_kwargs=}")
        return response

    async def put_binary_file(self, object_name, data, content_type):
        request_kwargs = {
            "Key": object_name,
            "Body": data,
            "ContentType": content_type,
        }
        result = await self._send_request("put_object", request_kwargs)
        return result

    async def get_binary_file(self, object_name):
        buf = BytesIO()
        request_kwargs = {"Key": object_name, "Fileobj": buf}
        response = await self._send_request("download_fileobj", request_kwargs)
        data = buf.getvalue()
        if not isinstance(data, bytes) or not data:
            message = f"Unexpected data content or empty {object_name=}"
            logger.error(message)
            raise HTTPException(status_code=response.status, detail=message)
        return data

    async def get_file_metadata(self, object_name):
        request_kwargs = {"Key": object_name}
        response = await self._send_request("head_object", request_kwargs)
        return response

    async def remove_file(self, object_name):
        request_kwargs = {"Key": object_name}
        response = await self._send_request("delete_object", request_kwargs)
        return response


class S3ClientHolder:
    def __init__(self):
        self._client = None

    @property
    def client(self):
        if not self._client:
            self._client = S3Client()
        return self._client


client_holder = S3ClientHolder()


def get_s3_client() -> S3Client:
    return client_holder.client
