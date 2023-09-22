import asyncio

import pytest
from aiobotocore.client import AioBaseClient
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from some_stuff.adapters.postgres.db.sessions import async_session_factory
from some_stuff.adapters.s3.repositories.SomeStuff_repository import SomeStuffRepository
from some_stuff.apps.api import app


@pytest.fixture
async def http_client():
    with TestClient(app) as client:
        yield client


@pytest.fixture
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture()
async def db_session() -> AsyncSession:
    async with async_session_factory() as session:
        yield session
        await session.flush()
        await session.rollback()


@pytest.fixture
def s3_client_mock(mocker):
    return mocker.Mock(spec=AioBaseClient)


@pytest.fixture
async def SomeStuff_repository(s3_client_mock) -> SomeStuffRepository:
    return SomeStuffRepository(s3_client_mock)


@pytest.fixture
async def s3_response_mock(mocker):
    async def return_response(*args, **kwargs):
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}
    return mocker.Mock(wraps=return_response)
