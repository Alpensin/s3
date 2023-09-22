from typing import AsyncGenerator

from aiobotocore.client import AioBaseClient
from aiobotocore.session import get_session
from botocore.client import Config

from some_stuff.config import settings

CONFIG = Config(
    read_timeout=settings.S3_CLIENT_READ_TIMEOUT_S,
    connect_timeout=settings.S3_CLIENT_CONNECT_TIMEOUT_S,
    max_pool_connections=settings.S3_HTTP_POOL_MAX_SIZE,
    region_name=settings.S3_REGION
)


async def get_async_client() -> AsyncGenerator[AioBaseClient, None]:
    session = get_session()
    async with session.create_client('s3',
                                     endpoint_url=settings.S3_ENDPOINT,
                                     aws_access_key_id=settings.S3_ACCESS_KEY,
                                     aws_secret_access_key=settings.S3_SECRET_KEY,
                                     config=CONFIG,
                                     ) as client:
        yield client
