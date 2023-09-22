import asyncio

from aiobotocore.session import get_session

from some_stuff.adapters.s3.repositories.some_stuffs_repository import some_stuffsRepository
from some_stuff.adapters.s3.storage.sessions import CONFIG
from some_stuff.config import settings
from some_stuff.containers.some_stuff_api_container import SomeStuffApiContainer


async def get_rep_from_container() -> some_stuffsRepository:
    container = SomeStuffApiContainer()
    rep: asyncio.Future[some_stuffsRepository] = container.some_stuffs_repository()
    r: some_stuffsRepository = await rep
    return r

async def main2() -> None:
    session = get_session()
    async with session.create_client('s3',
                                     endpoint_url=settings.S3_ENDPOINT,
                                     aws_access_key_id=settings.S3_ACCESS_KEY,
                                     aws_secret_access_key=settings.S3_SECRET_KEY,
                                     config=CONFIG,
                                     ) as client:
        r = some_stuffsRepository(client)
        with open('/Users/a.mardanov/Downloads/Untitled 2.csv', 'rb') as file:
            await r.put_some_stuff(object_name='Untitled 2.csv', data=file, content_type='text/csv')
        stream = await r.get_some_stuff('Untitled 2.csv')
        with open('res.csv', 'wb') as file:
            async for chunk in stream.iter_any():
                file.write(chunk)

        res = await r.get_some_stuff_metadata('Untitled 2.csv')
        res = await r.remove_some_stuff('Untitled 2.csv')
        a = res

async def main() -> None:
    r = await get_rep_from_container()
    with open('/Users/a.mardanov/Downloads/Untitled 2.csv', 'rb') as file:
        await r.put_some_stuff(object_name='Untitled 2.csv', data=file, content_type='text/csv')
    stream = await r.get_some_stuff('Untitled 2.csv')
    with open('res.csv', 'wb') as file:
        async for chunk in stream.iter_any():
            file.write(chunk)

    res = await r.get_some_stuff_metadata('Untitled 2.csv')
    res = await r.remove_some_stuff('Untitled 2.csv')
    a = res

if __name__ == "__main__":
    asyncio.run(main2())
