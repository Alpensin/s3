from dependency_injector import containers, providers

from some_stuff.adapters.s3.repositories.SomeStuff_repository import SomeStuffRepository
from some_stuff.adapters.s3.storage.sessions import get_async_client


class ColorSchemeApiContainer(containers.DeclarativeContainer):
    s3_client = providers.Resource(get_async_client)
    SomeStuff_repository = providers.Factory(
        SomeStuffRepository,
        s3_client=s3_client,
    )
