import logging
from io import BufferedReader
from typing import Any, Literal

from aiobotocore.client import AioBaseClient
from aiobotocore.response import StreamingBody
from botocore.exceptions import ClientError
from fastapi import status

from some_module.config import settings

logger = logging.getLogger(__name__)

class SomeStuffRepositoryError(RepositoryError):
    pass

class SomeStuffRepository:

    def __init__(self, s3_client: AioBaseClient, bucket: str = settings.S3_BUCKET) -> None:
        logger.debug('Creating minio client')
        self.s3_client = s3_client
        self.bucket = bucket

    async def _send_request(self, command: str, request_kwargs: dict[str, Any]) -> tuple[dict, int]:
        try:
            request_command = getattr(self.s3_client, command)
            logger.debug('Sending request %s with kwargs %s', command, request_kwargs)
            response = await request_command(
                Bucket=self.bucket,
                **request_kwargs,
            )
            logger.debug('S3 Client returning response command=%s request_kwargs=%s', command, request_kwargs)
            http_status_code = response['ResponseMetadata']['HTTPStatusCode']
            return response, http_status_code
        except AttributeError as e:
            raise SomeStuffRepositoryError(f'{command=} not found') from e
        except ClientError as e:
            error = e.response.get('Error', {})
            try:
                code = e.response['ResponseMetadata']['HTTPStatusCode']
                raise SomeStuffRepositoryError(f'{request_kwargs=}, {command=}. Error {error=}, {code=}') from e
            except KeyError as e:
                raise SomeStuffRepositoryError(f'Images storage communication problem {request_kwargs=}, {command=}') from e
        except KeyError as e:
            raise SomeStuffRepositoryError(f'Unexpected response for {request_kwargs=}, {command=}') from e
        except Exception as e:
            raise SomeStuffRepositoryError('Unexpected error') from e

    async def put_some_stuff(self, object_name: str, data: BufferedReader, content_type: str) -> Literal[True]:
        request_kwargs = {
            'Key': object_name,
            'Body': data,
            'ContentType': content_type,
        }
        _, http_status_code = await self._send_request('put_object', request_kwargs)

        if http_status_code != status.HTTP_200_OK:
            raise SomeStuffRepositoryError(f'Upload some_stuff {object_name=} failed. Response code: {http_status_code}')

        return True

    async def get_some_stuff(self, object_name: str) -> StreamingBody:
        request_kwargs = {'Key': object_name}
        response, http_status_code = await self._send_request('get_object', request_kwargs)

        if http_status_code != status.HTTP_200_OK:
            raise SomeStuffRepositoryError(f'Get some_stuff {object_name=} failed. Response code: {http_status_code}')

        data = response['Body']
        return data

    async def get_some_stuff_metadata(self, object_name: str) -> dict:
        request_kwargs = {'Key': object_name}
        response, http_status_code = await self._send_request('head_object', request_kwargs)
        if http_status_code != status.HTTP_200_OK:
            raise SomeStuffRepositoryError(f'Get some_stuff metadata {object_name=} failed. Response code: {http_status_code}')
        return response

    async def remove_some_stuff(self, object_name: str) -> Literal[True]:
        request_kwargs = {'Key': object_name}
        _, http_status_code = await self._send_request('delete_object', request_kwargs)
        if http_status_code != status.HTTP_204_NO_CONTENT:
            raise SomeStuffRepositoryError(f'Deletion of some_stuff {object_name=} failed. Response code: {http_status_code}')
        return True
